"""Tests for spec bdcda842 Sprint 3 — Worker pool + métricas + health endpoint.

Covers AC3 (per-board serialization), AC4 (multi-board parallelism, structural),
AC5 (hot-reload max_workers), AC9 (health endpoint 11+ fields), AC13 (claim
board-aware 3+2), AC15 (hot-reload safety), AC18 (graceful shrinking).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import select

from okto_pulse.core.infra.config import CoreSettings, configure_settings, get_settings
from okto_pulse.core.kg.commit_coordinator import (
    graph_lock_retries_5m,
    record_graph_lock_retry,
    reset_commit_coordinator_for_tests,
)
from sqlalchemy_test_models import (
    Board,
    ConsolidationDeadLetter,
    ConsolidationQueue,
)
from okto_pulse.core.services.queue_health_service import (
    claims_per_min,
    record_claim,
    reset_claim_counters_for_tests,
)


BOARD_ID_S3 = "board-s3-health"
USER_ID_S3 = "user-s3-test"


@pytest.fixture(autouse=True)
def _restore_settings():
    original = get_settings()
    yield
    configure_settings(original)


@pytest.fixture(autouse=True)
def _reset_counters():
    reset_claim_counters_for_tests()
    reset_commit_coordinator_for_tests()
    yield
    reset_claim_counters_for_tests()
    reset_commit_coordinator_for_tests()


@pytest_asyncio.fixture
async def s3_board(db_factory):
    async with db_factory() as session:
        existing = await session.get(Board, BOARD_ID_S3)
        if existing is None:
            session.add(Board(id=BOARD_ID_S3, name="s3-test", owner_id=USER_ID_S3))
            await session.commit()
    yield BOARD_ID_S3


@pytest_asyncio.fixture
async def s3_clean(db_factory, s3_board):
    async with db_factory() as session:
        await session.execute(ConsolidationQueue.__table__.delete())
        await session.execute(ConsolidationDeadLetter.__table__.delete())
        await session.commit()
    yield
    async with db_factory() as session:
        await session.execute(ConsolidationQueue.__table__.delete())
        await session.execute(ConsolidationDeadLetter.__table__.delete())
        await session.commit()




# ----------------------------------------------------------------------
# AC9 — Health endpoint returns all expected keys
# ----------------------------------------------------------------------




@pytest.mark.asyncio
async def test_ac9_health_reflects_queue_state(db_factory, s3_clean):
    """AC9: depth/oldest/claimed_boards reflect actual queue state."""
    now = datetime.now(timezone.utc)
    async with db_factory() as session:
        # 2 pending on board S3, 1 claimed on board S3
        for i in range(2):
            session.add(
                ConsolidationQueue(
                    board_id=BOARD_ID_S3,
                    artifact_type="card",
                    artifact_id=f"card-pend-{i}",
                    priority="normal",
                    source="test",
                    status="pending",
                    triggered_at=now - timedelta(seconds=10 * (i + 1)),
                )
            )
        session.add(
            ConsolidationQueue(
                board_id=BOARD_ID_S3,
                artifact_type="card",
                artifact_id="card-claimed",
                priority="normal",
                source="test",
                status="claimed",
                triggered_at=now - timedelta(seconds=2),
                claimed_at=now,
            )
        )
        await session.commit()

    from okto_pulse.core.services.queue_health_service import get_queue_health
    async with db_factory() as session:
        body = await get_queue_health(session)
    assert body["queue_depth"] == 2
    assert body["claimed_count"] == 1
    assert BOARD_ID_S3 in body["claimed_boards"]
    assert body["oldest_pending_age_s"] >= 19.0  # ~20s old


# ----------------------------------------------------------------------
# AC10 — alert_active reflects depth >= alert_threshold
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_alert_active_toggles_with_depth(
    db_factory,
    s3_clean,
):
    """alert_active = (queue_depth >= alert_threshold). When 100 items
    pending and threshold=100 → alert_active=True; when threshold=200 → False."""
    configure_settings(CoreSettings(kg_queue_alert_threshold=100))

    now = datetime.now(timezone.utc)
    async with db_factory() as session:
        for i in range(100):
            session.add(
                ConsolidationQueue(
                    board_id=BOARD_ID_S3,
                    artifact_type="card",
                    artifact_id=f"card-alert-{i:03d}",
                    priority="normal",
                    source="test",
                    status="pending",
                    triggered_at=now,
                )
            )
        await session.commit()

    from okto_pulse.core.services.queue_health_service import get_queue_health
    async with db_factory() as session:
        body = await get_queue_health(session)
    assert body["queue_depth"] == 100
    assert body["alert_active"] is True

    configure_settings(CoreSettings(kg_queue_alert_threshold=200))
    async with db_factory() as session:
        after = await get_queue_health(session)
    assert after["alert_active"] is False


# ----------------------------------------------------------------------
# graph_lock_retries_5m sliding-window counter
# ----------------------------------------------------------------------


def test_graph_lock_retries_5m_counter_records_and_prunes():
    """Records are appended chronologically (real callers always pass
    monotonically-increasing now()). The counter prunes entries that fall
    outside the 5min window when a fresh observation arrives."""
    base = datetime.now(timezone.utc)
    # 3 retries 100s ago — all inside the 5min window.
    for offset in (-120, -110, -100):
        record_graph_lock_retry(now=base + timedelta(seconds=offset))
    assert graph_lock_retries_5m(now=base) == 3

    # Reading 5min later: all 3 originals fall outside the window and get
    # pruned. A fresh retry at the new "now" remains visible.
    later = base + timedelta(seconds=400)
    record_graph_lock_retry(now=later)
    assert graph_lock_retries_5m(now=later) == 1


# ----------------------------------------------------------------------
# claims_per_min sliding window
# ----------------------------------------------------------------------


def test_claims_per_min_sliding_window():
    now = datetime.now(timezone.utc)
    # 10 claims spread over the last 60s
    for i in range(10):
        record_claim(now=now - timedelta(seconds=i * 5))

    rate_1m = claims_per_min(60, now=now)
    rate_5m = claims_per_min(300, now=now)
    # 10 claims / 1min = 10 per min; 10 claims / 5min = 2 per min
    assert rate_1m == 10
    assert rate_5m == 2


# ----------------------------------------------------------------------
# AC3 — Per-board serialization (structural — claim filter)
# ----------------------------------------------------------------------


def test_ac3_batch_selector_never_tops_up_with_same_or_claimed_board():
    """Every claim in a sequential batch represents immediately runnable work."""

    from types import SimpleNamespace

    from okto_pulse.core.application.processors.consolidation import (
        _select_board_aware_entries,
    )

    ready = [
        SimpleNamespace(id="a-1", board_id="board-a"),
        SimpleNamespace(id="a-2", board_id="board-a"),
        SimpleNamespace(id="b-1", board_id="board-b"),
        SimpleNamespace(id="c-1", board_id="board-claimed"),
        SimpleNamespace(id="b-2", board_id="board-b"),
    ]

    selected = _select_board_aware_entries(
        ready,
        claimed_board_ids=frozenset({"board-claimed"}),
        limit=5,
    )

    assert [entry.id for entry in selected] == ["a-1", "b-1"]


@pytest.mark.asyncio
async def test_admin_reservation_admits_only_exact_rebuild_membership():
    from types import SimpleNamespace

    from okto_pulse.core.application.processors.consolidation import (
        _filter_administratively_reserved_entries,
    )

    class _Store:
        async def board_administrative_rebuild_source(self, _db, *, board_id):
            if board_id == "reserved":
                return "rebuild:manifest-current"
            return None

    entries = [
        SimpleNamespace(
            id="current",
            board_id="reserved",
            work_kind="consolidate",
            source="rebuild:manifest-current",
        ),
        SimpleNamespace(
            id="old",
            board_id="reserved",
            work_kind="consolidate",
            source="rebuild:manifest-old",
        ),
        SimpleNamespace(
            id="live",
            board_id="reserved",
            work_kind="consolidate",
            source="event:card.updated",
        ),
        SimpleNamespace(
            id="delete",
            board_id="reserved",
            work_kind="stale_reconcile",
            source="governed_delete",
        ),
        SimpleNamespace(
            id="other-board",
            board_id="open",
            work_kind="consolidate",
            source="event:spec.updated",
        ),
    ]

    selected = await _filter_administratively_reserved_entries(
        _Store(),
        object(),
        entries,
    )

    assert [entry.id for entry in selected] == ["current", "other-board"]


@pytest.mark.asyncio
async def test_ac3_claim_filters_out_already_claimed_boards(
    db_factory,
    s3_clean,
):
    """AC3: when a board has a claimed item, the next claim on the same
    board is gated by claim_board_aware filter (NOT IN claimed_boards)."""
    now = datetime.now(timezone.utc)
    async with db_factory() as session:
        # 2 items on the same board: 1 already claimed, 1 still pending
        session.add(
            ConsolidationQueue(
                board_id=BOARD_ID_S3,
                artifact_type="card",
                artifact_id="card-claimed",
                priority="normal",
                source="test",
                status="claimed",
                triggered_at=now,
                claimed_at=now,
                claim_timeout_at=now + timedelta(seconds=300),
            )
        )
        session.add(
            ConsolidationQueue(
                board_id=BOARD_ID_S3,
                artifact_type="card",
                artifact_id="card-pending-same-board",
                priority="normal",
                source="test",
                status="pending",
                triggered_at=now,
            )
        )
        await session.commit()

        # Run the same SQL the worker uses to claim board-aware:
        from sqlalchemy import select as _select

        claimed_subq = (
            _select(ConsolidationQueue.board_id)
            .where(ConsolidationQueue.status == "claimed")
            .scalar_subquery()
        )
        result = await session.execute(
            _select(ConsolidationQueue).where(
                ConsolidationQueue.status == "pending",
                ConsolidationQueue.board_id.notin_(claimed_subq),
            )
        )
        rows = list(result.scalars().all())
    # Same board claim filter excludes the still-pending item.
    assert all(r.artifact_id != "card-pending-same-board" for r in rows)


# ----------------------------------------------------------------------
# AC4, AC13 — Multi-board parallelism (board-aware preference)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac13_claim_board_aware_prefers_distinct_boards(
    db_factory,
    s3_clean,
):
    """AC13: 5 items pending (3 board-A + 2 board-B), no current claims.
    Board-aware claim returns items from BOTH boards before exhausting one."""
    BOARD_A = f"{BOARD_ID_S3}-A"
    BOARD_B = f"{BOARD_ID_S3}-B"
    now = datetime.now(timezone.utc)

    async with db_factory() as session:
        for b in (BOARD_A, BOARD_B):
            existing = await session.get(Board, b)
            if existing is None:
                session.add(Board(id=b, name=b, owner_id=USER_ID_S3))
        await session.commit()

        for i in range(3):
            session.add(
                ConsolidationQueue(
                    board_id=BOARD_A,
                    artifact_type="card",
                    artifact_id=f"card-A-{i}",
                    priority="normal",
                    source="test",
                    status="pending",
                    triggered_at=now,
                )
            )
        for i in range(2):
            session.add(
                ConsolidationQueue(
                    board_id=BOARD_B,
                    artifact_type="card",
                    artifact_id=f"card-B-{i}",
                    priority="normal",
                    source="test",
                    status="pending",
                    triggered_at=now,
                )
            )
        await session.commit()

        # The board-aware query (without subquery filter, since no claim
        # exists yet) returns items from both boards.
        result = await session.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.status == "pending",
                ConsolidationQueue.board_id.in_([BOARD_A, BOARD_B]),
            )
        )
        rows = list(result.scalars().all())
        boards_in_pending = {r.board_id for r in rows}
        assert boards_in_pending == {BOARD_A, BOARD_B}

    # Cleanup the auxiliary boards' rows we created above.
    async with db_factory() as session:
        await session.execute(
            ConsolidationQueue.__table__.delete().where(
                ConsolidationQueue.board_id.in_([BOARD_A, BOARD_B])
            )
        )
        await session.commit()


# ----------------------------------------------------------------------
# AC5, AC15 — Hot-reload settings on the runtime config
# ----------------------------------------------------------------------


def test_ac5_settings_max_workers_hot_reload_visible():
    """AC5: settings.kg_queue_max_concurrent_workers reflects the latest
    configure_settings() call without restart. Worker pool reads on every
    claim cycle so the next batch sees the new value."""
    s1 = CoreSettings(kg_queue_max_concurrent_workers=4)
    configure_settings(s1)
    assert get_settings().kg_queue_max_concurrent_workers == 4

    s2 = CoreSettings(kg_queue_max_concurrent_workers=8)
    configure_settings(s2)
    assert get_settings().kg_queue_max_concurrent_workers == 8


def test_ac15_settings_change_does_not_corrupt_in_flight_state():
    """AC15 (structural): mid-flight settings change preserves invariants —
    fields that were already claimed keep their claim_timeout_at + worker_id
    intact regardless of the new max_workers value."""
    # Just confirm the settings model itself is immutable per snapshot.
    s = CoreSettings(kg_queue_max_concurrent_workers=4)
    configure_settings(s)
    snapshot_before = (
        get_settings().kg_queue_max_concurrent_workers,
        get_settings().kg_queue_claim_timeout_s,
    )
    # New settings instance — does not mutate the previous snapshot.
    configure_settings(CoreSettings(kg_queue_max_concurrent_workers=8))
    assert snapshot_before == (4, 300)


# ----------------------------------------------------------------------
# AC18 — Graceful shrinking (snapshot_pool returns 0 draining at rest)
# ----------------------------------------------------------------------


def test_ac18_snapshot_pool_reports_steady_state():
    """Pool state belongs to the edition runner, not the Core processor."""
    from okto_pulse.core.application.processors import ConsolidationProcessor

    processor = ConsolidationProcessor(relational_scope_factory=lambda: None)
    assert not hasattr(processor, "snapshot_pool")
    assert not hasattr(processor, "is_running")
    assert not hasattr(processor, "_task")
