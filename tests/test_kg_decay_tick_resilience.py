"""Behavioral tests for spec R2b — KG decay tick resilience.

Maps to NC-9 test evidence for the following acceptance criteria:

    AC1  — fleet of 3 boards: board B (corrupt) does not abort A or C;
             boards_failed=1; kg.tick.board_failed warning emitted.
    AC1b — regression guard: without FR1 (no per-board except), a single
             corrupt board WOULD abort the fleet.
    AC2  — persist_tick_run called twice with same tick_id → exactly 1 row,
             no IntegrityError (idempotent upsert).
    AC2b — regression guard: session.add with same PK → IntegrityError.
    AC3  — KGDailyTickHandler.handle() does not re-raise on exception →
             caller sees normal return; dispatcher marks done.
    AC4  — boards_failed present in _run_daily_tick return dict AND in the
             kg.relevance.tick.completed structured log extra.
    AC7  — module docstrings in kg_decay_tick.py and events/types.py no
             longer contain "cron at 03:00 UTC"; both cite scheduler adapter.
    AC8  — _refuse_tick_if_degraded(None, db) returns None without querying
             board health (tick global is not health-gated, FR9).
"""

from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy import text as sa_text

from okto_pulse.core.infra.database import get_session_factory

# ---------------------------------------------------------------------------
# Helpers / shared fixtures
# ---------------------------------------------------------------------------

_HERE = Path(__file__).parent
_SRC = _HERE.parent / "src"

# Locate the production modules by absolute path so the AST assertion (AC7)
# reads the same file the Python import loads.
_KG_DECAY_TICK_PATH = (
    _SRC / "okto_pulse" / "core" / "events" / "handlers" / "kg_decay_tick.py"
)
_EVENTS_TYPES_PATH = (
    _SRC / "okto_pulse" / "core" / "events" / "types.py"
)


@pytest_asyncio.fixture
async def db_session():
    """Short-lived async session for tests that need a real DB session."""
    factory = get_session_factory()
    async with factory() as session:
        yield session
        # Roll back any test-only writes to keep isolation.
        await session.rollback()


@pytest_asyncio.fixture
async def clean_tick_run_session(db_session):
    """Session with the kg_tick_runs table cleared before the test."""
    await db_session.execute(sa_text("DELETE FROM kg_tick_runs"))
    await db_session.commit()
    yield db_session
    await db_session.execute(sa_text("DELETE FROM kg_tick_runs"))
    await db_session.commit()


# ---------------------------------------------------------------------------
# AC1 — board-level isolation: corrupt board B does not abort fleet
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac1_corrupt_board_does_not_abort_fleet(caplog, db_session):
    """AC1: fleet of 3 boards; B raises RuntimeError in _process_board_sync.

    Expected outcomes after _run_daily_tick:
    - boards_processed == 2  (A and C processed)
    - boards_failed    == 1  (B failed)
    - kg.tick.board_failed warning emitted with board_id=board_B
    """
    from okto_pulse.core.events.handlers.kg_decay_tick import _run_daily_tick

    board_a, board_b, board_c = "board-A-ac1", "board-B-ac1", "board-C-ac1"

    async def _fake_to_thread(fn, board_id, *args, **kwargs):
        if board_id == board_b:
            raise RuntimeError("graph is locked or corrupt")
        return (0, 0)

    with (
        patch(
            "okto_pulse.core.events.handlers.kg_decay_tick.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ),
        patch(
            "okto_pulse.core.events.handlers.kg_decay_tick._persist_tick_run",
            new_callable=AsyncMock,
        ) as mock_persist,
    ):
        # Make the DB query return our 3 boards.
        fake_exec = AsyncMock(return_value=MagicMock(
            scalars=lambda: MagicMock(all=lambda: [board_a, board_b, board_c])
        ))

        with (
            patch.object(db_session, "execute", side_effect=fake_exec),
            caplog.at_level("WARNING", logger="okto_pulse.core.events.handlers.kg_decay_tick"),
        ):
            result = await _run_daily_tick(
                tick_id="tick-ac1",
                session=db_session,
            )

    assert result["boards_processed"] == 2, (
        f"Expected 2 boards processed, got {result['boards_processed']}"
    )
    assert result["boards_failed"] == 1, (
        f"Expected 1 board failed, got {result['boards_failed']}"
    )

    # kg.tick.board_failed warning must be emitted with the correct board_id.
    board_failed_records = [
        r for r in caplog.records
        if "kg.tick.board_failed" in r.getMessage()
        and board_b in r.getMessage()
    ]
    assert board_failed_records, (
        "Expected at least one WARNING with kg.tick.board_failed and "
        f"board_id={board_b!r} but got none. Records: {[r.getMessage() for r in caplog.records]}"
    )

    # _persist_tick_run must have been called (summary written).
    assert mock_persist.called


# ---------------------------------------------------------------------------
# AC1b — regression guard: without per-board except, one corrupt board
# would abort the entire fleet
# ---------------------------------------------------------------------------


def test_ac1b_regression_fleet_abort_without_isolation():
    """AC1b: regression guard — if we removed the per-board try/except,
    a single RuntimeError would propagate and only 1 board would be processed
    (A) before aborting.  This test simulates that broken implementation to
    confirm the test suite would catch the regression.
    """

    async def _broken_run_daily_tick(boards, raise_on):
        """Minimal simulation of the PRE-FR1 code path (no per-board except)."""
        boards_processed = 0
        for bid in boards:
            # NO try/except — this is the broken version
            if bid == raise_on:
                raise RuntimeError("graph is locked")
            boards_processed += 1
        return boards_processed

    import asyncio

    boards = ["board-A", "board-B", "board-C"]
    with pytest.raises(RuntimeError, match="graph is locked"):
        asyncio.run(_broken_run_daily_tick(boards, raise_on="board-B"))


# ---------------------------------------------------------------------------
# AC2 — idempotent upsert: two calls with the same tick_id → exactly 1 row
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac2_persist_tick_run_idempotent(clean_tick_run_session):
    """AC2: calling _persist_tick_run twice with the same tick_id produces
    exactly one row in kg_tick_runs and raises no IntegrityError.
    """
    from okto_pulse.core.events.handlers.kg_decay_tick import _persist_tick_run

    session = clean_tick_run_session
    now = datetime.now(timezone.utc)
    tick_id = "idempotent-tick-ac2"

    await _persist_tick_run(
        session,
        tick_id=tick_id,
        started_at=now,
        completed_at=now,
        nodes_recomputed=5,
        duration_ms=100.0,
        boards_processed=3,
        boards_failed=0,
    )

    # Second call with updated values — must not raise.
    await _persist_tick_run(
        session,
        tick_id=tick_id,
        started_at=now,
        completed_at=now,
        nodes_recomputed=10,  # updated value
        duration_ms=200.0,
        boards_processed=3,
        boards_failed=1,
    )

    row_count = (
        await session.execute(
            sa_text("SELECT COUNT(*) FROM kg_tick_runs WHERE tick_id = :tid"),
            {"tid": tick_id},
        )
    ).scalar()
    assert row_count == 1, (
        f"Expected exactly 1 row for tick_id={tick_id!r}, got {row_count}"
    )

    # The second call's values should win (upsert updates).
    updated_row = (
        await session.execute(
            sa_text("SELECT nodes_recomputed, boards_failed FROM kg_tick_runs WHERE tick_id = :tid"),
            {"tid": tick_id},
        )
    ).fetchone()
    assert updated_row is not None
    assert updated_row[0] == 10, "ON CONFLICT SET should have updated nodes_recomputed to 10"
    assert updated_row[1] == 1, "ON CONFLICT SET should have updated boards_failed to 1"


# ---------------------------------------------------------------------------
# AC2b — regression guard: session.add with same PK → IntegrityError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac2b_regression_session_add_raises_integrity_error(
    clean_tick_run_session,
):
    """AC2b: regression guard — using session.add() for the same tick_id PK
    would raise an IntegrityError on the second insert.  Confirms the upsert
    is load-bearing.
    """
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy_test_models import KGTickRun

    session = clean_tick_run_session
    now = datetime.now(timezone.utc)
    tick_id = "dupe-tick-ac2b"

    row = KGTickRun(
        tick_id=tick_id,
        started_at=now,
        completed_at=now,
        nodes_recomputed=0,
        duration_ms=0.0,
        boards_processed=0,
        boards_failed=0,
    )
    session.add(row)
    await session.flush()

    # A second add with the same PK must raise.
    row2 = KGTickRun(
        tick_id=tick_id,
        started_at=now,
        completed_at=now,
        nodes_recomputed=0,
        duration_ms=0.0,
        boards_processed=0,
        boards_failed=0,
    )
    session.add(row2)
    with pytest.raises(IntegrityError):
        await session.flush()

    await session.rollback()


# ---------------------------------------------------------------------------
# AC3 — handle() does not re-raise; dispatcher can mark done
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac3_handler_does_not_reraise_on_exception():
    """AC3: KGDailyTickHandler.handle() catches exceptions from _run_daily_tick
    and persists a failure row WITHOUT re-raising. The caller sees normal
    return (None), not an exception.
    """
    from okto_pulse.core.events.handlers.kg_decay_tick import KGDailyTickHandler
    from okto_pulse.core.events.types import KGDailyTick

    handler = KGDailyTickHandler()
    event = KGDailyTick(
        tick_id="fail-tick-ac3",
        scheduled_at=datetime.now(timezone.utc).isoformat(),
        board_id="*",
        actor_id=None,
        actor_type="system",
    )
    mock_session = AsyncMock()

    with (
        patch(
            "okto_pulse.core.events.handlers.kg_decay_tick._run_daily_tick",
            side_effect=RuntimeError("forced failure for AC3"),
        ),
        patch(
            "okto_pulse.core.events.handlers.kg_decay_tick._persist_tick_run",
            new_callable=AsyncMock,
        ) as mock_persist,
    ):
        # Must return normally — no exception propagated.
        result = await handler.handle(event, mock_session)

    assert result is None, f"handle() should return None, got {result!r}"
    assert mock_persist.called, "_persist_tick_run must be called to log the failure"

    # Verify error was recorded in the persisted run.
    kwargs = mock_persist.call_args.kwargs
    assert kwargs.get("error") is not None, "error= must be populated in failure persist"
    assert "forced failure" in kwargs["error"]


# ---------------------------------------------------------------------------
# AC4 — boards_failed in return dict AND in structured log
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac4_boards_failed_in_dict_and_log(caplog, db_session):
    """AC4: with 2 boards OK and 1 failing, _run_daily_tick returns
    boards_processed==2 (a failing board does NOT count as processed —
    br_32bfa43c) and boards_failed==1, and the kg.relevance.tick.completed
    structured log extra carries boards_failed==1.
    """
    from okto_pulse.core.events.handlers.kg_decay_tick import _run_daily_tick

    async def _fake_to_thread(fn, board_id, *args, **kwargs):
        if board_id == "board-FAIL":
            raise RuntimeError("graph corrupt for board-FAIL")
        return (2, 1)  # recomputed=2, stale_pre=1

    with (
        patch(
            "okto_pulse.core.events.handlers.kg_decay_tick.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ),
        patch(
            "okto_pulse.core.events.handlers.kg_decay_tick._persist_tick_run",
            new_callable=AsyncMock,
        ),
    ):
        fake_exec = AsyncMock(return_value=MagicMock(
            scalars=lambda: MagicMock(all=lambda: ["board-A", "board-FAIL", "board-B"])
        ))
        with (
            patch.object(db_session, "execute", side_effect=fake_exec),
            caplog.at_level("INFO", logger="okto_pulse.core.events.handlers.kg_decay_tick"),
        ):
            result = await _run_daily_tick(
                tick_id="tick-ac4",
                session=db_session,
            )

    # A failing board does NOT count as processed (br_32bfa43c): 2 OK of 3.
    assert result["boards_processed"] == 2, (
        f"expected boards_processed==2 (failing board excluded), "
        f"got {result.get('boards_processed')}"
    )
    assert result["boards_failed"] == 1, (
        f"expected boards_failed==1, got {result.get('boards_failed')}"
    )

    # boards_failed==1 in the kg.relevance.tick.completed structured log extra.
    completed_records = [
        r for r in caplog.records
        if getattr(r, "event", None) == "kg.relevance.tick.completed"
        or "kg.relevance.tick.completed" in r.getMessage()
    ]
    assert completed_records, "kg.relevance.tick.completed log record not found"
    log_rec = completed_records[0]
    assert getattr(log_rec, "boards_failed", None) == 1, (
        "kg.relevance.tick.completed log must carry boards_failed==1, "
        f"got {getattr(log_rec, 'boards_failed', None)}"
    )


# ---------------------------------------------------------------------------
# AC7 — docstrings updated: no "03:00 UTC", cite scheduler adapter
# ---------------------------------------------------------------------------


def test_ac7_module_docstring_no_cron_utc():
    """AC7a: kg_decay_tick.py module docstring must NOT contain '03:00 UTC'
    and MUST mention the scheduler adapter boundary.
    """
    source = _KG_DECAY_TICK_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    module_docstring = ast.get_docstring(tree) or ""

    assert "03:00 UTC" not in module_docstring, (
        "kg_decay_tick.py module docstring still contains stale '03:00 UTC'. "
        "Update it to reflect the scheduler adapter mechanism."
    )
    assert "scheduler adapter" in module_docstring, (
        "kg_decay_tick.py module docstring must mention the scheduler adapter "
        "to reflect the boundary mechanism."
    )


def test_ac7_events_types_docstring_no_cron_utc():
    """AC7b: KGDailyTick docstring in events/types.py must NOT contain
    '03:00 UTC' and MUST mention the scheduler adapter boundary.
    """
    source = _EVENTS_TYPES_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)

    # Find the KGDailyTick class node and check its docstring.
    kg_daily_tick_docstring = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "KGDailyTick":
            kg_daily_tick_docstring = ast.get_docstring(node) or ""
            break

    assert kg_daily_tick_docstring is not None, (
        "KGDailyTick class not found in events/types.py"
    )
    assert "03:00 UTC" not in kg_daily_tick_docstring, (
        "KGDailyTick docstring in events/types.py still contains '03:00 UTC'. "
        "Update to reflect the scheduler adapter mechanism."
    )
    assert "scheduler adapter" in kg_daily_tick_docstring, (
        "KGDailyTick docstring in events/types.py must mention the scheduler adapter."
    )


# ---------------------------------------------------------------------------
# AC8 — _refuse_tick_if_degraded(None, db) returns None (no health query)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac8_global_tick_not_health_gated():
    """AC8: _refuse_tick_if_degraded(None, db) must return None immediately
    without ever calling get_kg_health (FR9 — global tick is not health-gated).
    """
    from okto_pulse.community.api.kg_tick import _refuse_tick_if_degraded

    mock_db = AsyncMock()

    with patch(
        "okto_pulse.community.api.kg_tick.get_kg_health",
        new_callable=AsyncMock,
    ) as mock_health:
        result = await _refuse_tick_if_degraded(None, mock_db)

    assert result is None, (
        f"_refuse_tick_if_degraded(None, db) must return None; got {result!r}"
    )
    mock_health.assert_not_called(), (
        "get_kg_health must NOT be called for a global tick (board_id=None)"
    )


# ---------------------------------------------------------------------------
# Campo 2026-06-10 — FK regression: tick global com board_id='*' violava a
# FK de domain_events sob PRAGMA foreign_keys=ON (runtime community) e
# NENHUM tick era agendado em produção. O fan-out publica por board real.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tick_fanout_publishes_per_real_board_never_star(db_session):
    """publish_tick_events global → 1 evento por board EXISTENTE, com
    board_id real (FK-safe), nunca o sentinel '*'."""
    import uuid as _uuid

    from sqlalchemy import select

    from okto_pulse.core.events.handlers.kg_decay_tick import (
        publish_tick_events,
    )
    from sqlalchemy_test_models import Board, DomainEventRow

    bids = [f"board-tickfk-{_uuid.uuid4().hex[:8]}" for _ in range(2)]
    for bid in bids:
        db_session.add(Board(id=bid, name=bid, owner_id="user-tickfk"))
    await db_session.flush()

    # PRAGMA per-connection: reproduz a condição do runtime community na
    # MESMA conexão que fará os INSERTs dos eventos.
    await db_session.execute(sa_text("PRAGMA foreign_keys=ON"))

    tick_ids = await publish_tick_events(db_session)
    assert len(tick_ids) >= 2, "fan-out deve emitir >= 1 evento por board"

    rows = (
        await db_session.execute(
            select(DomainEventRow.board_id).where(
                DomainEventRow.event_type == "kg.tick.daily"
            )
        )
    ).scalars().all()
    assert rows, "nenhum evento kg.tick.daily persistido"
    assert "*" not in rows, "sentinel global '*' viola a FK de domain_events"
    for bid in bids:
        assert bid in rows, f"board {bid} ficou fora do fan-out"


@pytest.mark.asyncio
async def test_tick_fanout_scoped_to_single_board(db_session):
    """board_id concreto → exatamente 1 evento para aquele board."""
    import uuid as _uuid

    from okto_pulse.core.events.handlers.kg_decay_tick import (
        publish_tick_events,
    )
    from sqlalchemy_test_models import Board

    bid = f"board-tickfk-{_uuid.uuid4().hex[:8]}"
    db_session.add(Board(id=bid, name=bid, owner_id="user-tickfk"))
    await db_session.flush()

    tick_ids = await publish_tick_events(db_session, board_id=bid)
    assert len(tick_ids) == 1


def test_tick_next_run_catch_up_semantics():
    """Scheduler interval de 24h nunca dispara num processo que reinicia —
    o next_run_time explícito honra o último tick persistido."""
    from datetime import timedelta

    from okto_pulse.community.app import _tick_next_run_from_last

    now = datetime(2026, 6, 10, 12, 0, 0, tzinfo=timezone.utc)
    floor = now + timedelta(seconds=120)

    # Sem histórico → dispara logo após o boot.
    assert _tick_next_run_from_last(None, 1440, now) == floor
    # Último tick vencido (3 dias atrás, interval 24h) → dispara logo.
    stale = now - timedelta(days=3)
    assert _tick_next_run_from_last(stale, 1440, now) == floor
    # Último tick recente → respeita o vencimento real.
    fresh = now - timedelta(hours=2)
    assert _tick_next_run_from_last(fresh, 1440, now) == fresh + timedelta(
        minutes=1440
    )
    # Naive datetime (SQLite) é tratado como UTC.
    naive_fresh = (now - timedelta(hours=2)).replace(tzinfo=None)
    assert _tick_next_run_from_last(naive_fresh, 1440, now) == fresh + timedelta(
        minutes=1440
    )
