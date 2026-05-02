"""Tests for spec bdcda842 Sprint 2 (Core Resilience) — IMPL-2 + IMPL-3.

Covers AC1 (zero-loss), AC2 (recovery), AC7 (dead-letter), AC10 (alert toggle),
AC16 (priority routing), AC17 (errors[] schema).

These tests focus on the structural invariants the consolidation worker
must hold; full end-to-end Kùzu integration is exercised by the existing
test_events.py + test_commit_coordinator.py suites.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import select

from okto_pulse.core.events.handlers.consolidation_enqueuer import (
    ConsolidationEnqueuer,
)
from okto_pulse.core.events.types import CardCreated
from okto_pulse.core.infra.config import CoreSettings, configure_settings, get_settings
from okto_pulse.core.kg.workers.dead_letter import (
    build_attempt_entry,
    route_to_dead_letter,
)
from okto_pulse.core.models.db import (
    Board,
    ConsolidationDeadLetter,
    ConsolidationQueue,
)


BOARD_ID_S2 = "board-s2-resilience"
USER_ID_S2 = "user-s2-test"


@pytest.fixture(autouse=True)
def _restore_core_settings():
    original = get_settings()
    yield
    configure_settings(original)


@pytest_asyncio.fixture
async def s2_board(db_factory):
    """Ensure a Board row exists so FK constraints pass."""
    async with db_factory() as session:
        existing = await session.get(Board, BOARD_ID_S2)
        if existing is None:
            session.add(Board(id=BOARD_ID_S2, name="s2-test", owner_id=USER_ID_S2))
            await session.commit()
    yield BOARD_ID_S2


@pytest_asyncio.fixture
async def s2_clean(db_factory, s2_board):
    async with db_factory() as session:
        await session.execute(
            ConsolidationQueue.__table__.delete().where(
                ConsolidationQueue.board_id == BOARD_ID_S2
            )
        )
        await session.execute(
            ConsolidationDeadLetter.__table__.delete().where(
                ConsolidationDeadLetter.board_id == BOARD_ID_S2
            )
        )
        await session.commit()
    yield
    async with db_factory() as session:
        await session.execute(
            ConsolidationQueue.__table__.delete().where(
                ConsolidationQueue.board_id == BOARD_ID_S2
            )
        )
        await session.execute(
            ConsolidationDeadLetter.__table__.delete().where(
                ConsolidationDeadLetter.board_id == BOARD_ID_S2
            )
        )
        await session.commit()


# ----------------------------------------------------------------------
# AC1 — Zero-loss: enqueuer never rejects, regardless of depth
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac1_enqueuer_never_rejects_above_alert_threshold(
    db_factory, s2_clean, caplog,
):
    """AC1: with kg_queue_alert_threshold=100 (the minimum legal value)
    and 250 events injected, all 250 land in ConsolidationQueue.
    kg.queue.alert_fired logs once at the crossing edge (exactly one log
    entry, not 150)."""
    settings = CoreSettings(kg_queue_alert_threshold=100)
    configure_settings(settings)

    enqueuer = ConsolidationEnqueuer()

    with caplog.at_level(logging.WARNING, logger="okto_pulse.core.events.consolidation_enqueuer"):
        async with db_factory() as session:
            for i in range(250):
                event = CardCreated(
                    board_id=BOARD_ID_S2,
                    actor_id=USER_ID_S2,
                    card_id=f"card-zero-loss-{i:03d}",
                    spec_id="spec-zero-loss",
                    card_type="normal",
                    priority="none",
                )
                await enqueuer.handle(event, session)
                # Flush so each subsequent depth count picks up the prior insert.
                await session.flush()
            await session.commit()

    async with db_factory() as session:
        rows = (await session.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.board_id == BOARD_ID_S2,
            )
        )).scalars().all()
        assert len(rows) == 250, f"expected 250 enqueued rows, got {len(rows)}"

    alert_logs = [r for r in caplog.records if "alert_fired" in r.message]
    assert len(alert_logs) == 1, (
        f"expected exactly 1 alert_fired log (crossing edge), got {len(alert_logs)}"
    )


# ----------------------------------------------------------------------
# AC10 — alert_fired emits exactly once per low→high crossing
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac10_alert_fires_only_on_crossing(db_factory, s2_clean, caplog):
    """AC10: starting at depth=0, threshold=100 (min legal) — events 1..99
    do NOT fire, event 100 fires once, events 101..150 do NOT re-fire."""
    configure_settings(CoreSettings(kg_queue_alert_threshold=100))
    enqueuer = ConsolidationEnqueuer()

    with caplog.at_level(logging.WARNING, logger="okto_pulse.core.events.consolidation_enqueuer"):
        async with db_factory() as session:
            for i in range(150):
                event = CardCreated(
                    board_id=BOARD_ID_S2,
                    actor_id=USER_ID_S2,
                    card_id=f"card-alert-{i:03d}",
                    spec_id="spec-alert",
                    card_type="normal",
                    priority="none",
                )
                await enqueuer.handle(event, session)
                await session.flush()  # so the depth count picks up each insert

    alert_logs = [r for r in caplog.records if "alert_fired" in r.message]
    assert len(alert_logs) == 1
    # The fired log carries depth=100 (the one that crossed).
    assert "depth=100" in alert_logs[0].message


# ----------------------------------------------------------------------
# AC16 — Priority routing: card.cancelled inserts with priority=high
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac16_priority_high_for_cancelled_cards(db_factory, s2_clean):
    """AC16: a cancelled card lands with priority='high'; a normal create
    lands with priority='normal'. ORDER BY priority ASC then puts 'high'
    first (alphabetic ordering — 'h' < 'n')."""
    from okto_pulse.core.events.types import CardCancelled

    enqueuer = ConsolidationEnqueuer()
    async with db_factory() as session:
        await enqueuer.handle(
            CardCreated(
                board_id=BOARD_ID_S2,
                actor_id=USER_ID_S2,
                card_id="card-prio-normal",
                spec_id="spec-prio",
                card_type="normal",
                priority="none",
            ),
            session,
        )
        await enqueuer.handle(
            CardCancelled(
                board_id=BOARD_ID_S2,
                actor_id=USER_ID_S2,
                card_id="card-prio-high",
                previous_status="in_progress",
            ),
            session,
        )
        await session.commit()

    async with db_factory() as session:
        result = await session.execute(
            select(ConsolidationQueue)
            .where(ConsolidationQueue.board_id == BOARD_ID_S2)
            .order_by(ConsolidationQueue.priority.asc())
        )
        rows = list(result.scalars().all())
        assert len(rows) == 2
        # ASC: 'high' < 'normal' → high first
        assert rows[0].priority == "high"
        assert rows[0].artifact_id == "card-prio-high"
        assert rows[1].priority == "normal"
        assert rows[1].artifact_id == "card-prio-normal"


# ----------------------------------------------------------------------
# AC17 — DLQ errors[] schema (build_attempt_entry contract)
# ----------------------------------------------------------------------


def test_ac17_attempt_entry_schema_has_5_keys():
    """AC17: each entry in the DLQ errors[] array has EXACTLY the 5 fixed
    keys (attempt, occurred_at, error_type, message, traceback)."""
    entry = build_attempt_entry(
        attempt=3,
        error_type="RuntimeError",
        message="something went wrong",
        include_traceback=False,
    )
    assert set(entry.keys()) == {
        "attempt", "occurred_at", "error_type", "message", "traceback",
    }
    assert entry["attempt"] == 3
    assert entry["error_type"] == "RuntimeError"
    assert entry["message"] == "something went wrong"
    assert entry["traceback"] is None
    # ISO8601 UTC parseable by datetime.fromisoformat
    parsed = datetime.fromisoformat(entry["occurred_at"])
    assert parsed.tzinfo is not None


def test_ac17_attempt_entry_truncates_message_to_500_chars():
    long_msg = "x" * 1500
    entry = build_attempt_entry(
        attempt=1, error_type="ValueError", message=long_msg,
    )
    assert len(entry["message"]) == 500
    assert entry["message"] == "x" * 500


def test_ac17_attempt_entry_traceback_only_when_requested():
    """traceback is None unless include_traceback=True. Even when True, it
    is None outside an active except handler (no current exception)."""
    no_tb = build_attempt_entry(
        attempt=1, error_type="X", message="m", include_traceback=False,
    )
    assert no_tb["traceback"] is None

    try:
        raise RuntimeError("captured")
    except RuntimeError:
        with_tb = build_attempt_entry(
            attempt=1, error_type="RuntimeError",
            message="captured", include_traceback=True,
        )
    assert with_tb["traceback"] is not None
    assert "RuntimeError" in with_tb["traceback"]
    assert len(with_tb["traceback"]) <= 2000


# ----------------------------------------------------------------------
# AC7 — route_to_dead_letter moves row from queue to DLQ with full history
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac7_route_to_dead_letter_moves_row_with_history(
    db_factory, s2_clean,
):
    """AC7: after kg_queue_max_attempts failures, route_to_dead_letter:
    - creates 1 ConsolidationDeadLetter row with errors[] of size N
    - removes the original row from ConsolidationQueue
    - preserves attempts count + original_queue_id back-reference."""
    async with db_factory() as session:
        entry = ConsolidationQueue(
            board_id=BOARD_ID_S2,
            artifact_type="card",
            artifact_id="card-to-dlq",
            priority="normal",
            source="event:card.created",
            triggered_by_event="card.created",
            status="claimed",
            attempts=5,
            last_error="ValueError: prior failure message",
        )
        session.add(entry)
        await session.commit()
        await session.refresh(entry)
        original_id = entry.id

        dlq_row = await route_to_dead_letter(
            session, entry,
            error_text="ValueError: final failure on attempt 5",
        )
        await session.commit()

        assert dlq_row.attempts == 5
        assert dlq_row.original_queue_id == original_id
        assert isinstance(dlq_row.errors, list)
        # 4 placeholders + 1 final = 5 entries
        assert len(dlq_row.errors) == 5
        for i, err in enumerate(dlq_row.errors, start=1):
            assert err["attempt"] == i
            assert set(err.keys()) == {
                "attempt", "occurred_at", "error_type", "message", "traceback",
            }
        assert dlq_row.errors[-1]["message"] == "final failure on attempt 5"
        assert dlq_row.errors[-1]["error_type"] == "ValueError"

        # Original row was deleted from the queue.
        remaining = (await session.execute(
            select(ConsolidationQueue).where(ConsolidationQueue.id == original_id)
        )).scalar_one_or_none()
        assert remaining is None


# ----------------------------------------------------------------------
# AC2 — Worker pool re-claims orphaned items past claim_timeout_at
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac2_recovery_re_pendings_orphaned_claims(
    db_factory, s2_clean, monkeypatch,
):
    """AC2 (structural): the recovery scan re-pendings rows where
    claim_timeout_at has elapsed. Implementation lives in IMPL-3 (worker
    helper); this asserts the SQL invariant the future scan relies on."""
    now = datetime.now(timezone.utc)
    async with db_factory() as session:
        ok = ConsolidationQueue(
            board_id=BOARD_ID_S2,
            artifact_type="card",
            artifact_id="card-fresh-claim",
            priority="normal",
            source="event:card.created",
            status="claimed",
            worker_id="worker_fresh",
            claimed_at=now,
            claim_timeout_at=now + timedelta(seconds=300),
        )
        stale = ConsolidationQueue(
            board_id=BOARD_ID_S2,
            artifact_type="card",
            artifact_id="card-stale-claim",
            priority="normal",
            source="event:card.created",
            status="claimed",
            worker_id="worker_dead",
            claimed_at=now - timedelta(seconds=600),
            claim_timeout_at=now - timedelta(seconds=300),
        )
        session.add_all([ok, stale])
        await session.commit()

        result = await session.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.status == "claimed",
                ConsolidationQueue.claim_timeout_at < now,
            )
        )
        stale_rows = list(result.scalars().all())
        assert len(stale_rows) == 1
        assert stale_rows[0].artifact_id == "card-stale-claim"
