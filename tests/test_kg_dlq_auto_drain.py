"""Behavioral tests for the DLQ auto-drain feature (Spec R2c, FR5/FR6).

Coverage map (NC-9 evidence):
  test_auto_drain_disabled_skips_reprocess      -> AC9
  test_auto_drain_enabled_calls_reprocess       -> AC10
  test_auto_drain_backoff_suppresses_second_run -> AC11
  test_health_has_drain_fields                  -> AC12
  test_poison_pill_excluded_and_warned          -> AC13
"""

from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
import pytest_asyncio

from okto_pulse.core.application.processors.consolidation import ConsolidationProcessor
from sqlalchemy_test_models import Board
from okto_pulse.core.services.kg_health_service import get_kg_health

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BOARD_ID_DRAIN = "board-dlq-drain-test"
_USER_ID_DRAIN = "user-dlq-drain-test"


# ---------------------------------------------------------------------------
# Fake board / DLQ row helpers
# ---------------------------------------------------------------------------

def _fake_board(board_id: str, *, dlq_auto_drain_enabled: bool) -> SimpleNamespace:
    return SimpleNamespace(
        id=board_id,
        name=f"test-{board_id}",
        owner_id="test-owner",
        settings={"dlq_auto_drain_enabled": dlq_auto_drain_enabled},
    )


def _fake_dlq_row(board_id: str, *, attempts: int = 0) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4().hex,
        board_id=board_id,
        artifact_type="card",
        artifact_id=uuid.uuid4().hex,
        attempts=attempts,
        errors=[],
        dead_lettered_at=datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# Helpers to build per-session fake DBs using a queue of session responses
# ---------------------------------------------------------------------------

class _ScalarsProxy:
    def __init__(self, items):
        self._items = items

    def scalars(self):
        return self

    def all(self):
        return list(self._items)


class _SessionDB:
    """One fake async session. Tracks deletions and commits."""

    def __init__(
        self,
        *,
        board_rows: list | None = None,
        dlq_rows: list | None = None,
        scalar_value: int = 0,
    ):
        self._boards = board_rows or []
        self._dlq_rows = list(dlq_rows or [])
        self._scalar_value = scalar_value
        self.deleted: list[Any] = []
        self.committed = False
        self._execute_call = 0

    async def execute(self, stmt):
        self._execute_call += 1
        if self._execute_call == 1 and self._boards:
            return _ScalarsProxy(self._boards)
        # Subsequent calls return DLQ rows
        return _ScalarsProxy(self._dlq_rows)

    async def scalar(self, stmt):
        return self._scalar_value

    async def delete(self, row):
        self.deleted.append(row)
        if row in self._dlq_rows:
            self._dlq_rows.remove(row)

    async def commit(self):
        self.committed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


def _make_session_sequence(sessions: list[_SessionDB]):
    """Factory that yields sessions from ``sessions`` in order.

    _run_dlq_auto_drain opens up to 4 session contexts per board:
      1. load all boards
      2. count DLQ rows (scalar)
      3. load + delete poison rows
      4. call reprocess (handled by mocked function)
    """
    index = [0]

    @asynccontextmanager
    async def _factory():
        i = index[0]
        db = sessions[i % len(sessions)]  # wrap around if needed
        index[0] += 1
        yield db

    return _factory


def _make_worker(relational_scope_factory) -> ConsolidationProcessor:
    return ConsolidationProcessor(
        relational_scope_factory=relational_scope_factory,
        heartbeat_seconds=30,
        batch_size=1,
    )


# ---------------------------------------------------------------------------
# Patch helper for reprocess_dead_letter_rows
# ---------------------------------------------------------------------------

class _ReprocessPatcher:
    """Context manager that patches reprocess_dead_letter_rows at the
    dead_letter_inspector_service module level.

    _run_dlq_auto_drain does ``from okto_pulse.core.services.dead_letter_inspector_service
    import reprocess_dead_letter_rows`` inside its body, so we patch the
    function on the service module and it is picked up on the next import.
    """

    def __init__(self, calls: list):
        self._calls = calls
        self._orig = None
        self._module = None

    def __enter__(self):
        import okto_pulse.core.services.dead_letter_inspector_service as svc
        self._module = svc
        self._orig = svc.reprocess_dead_letter_rows

        calls = self._calls

        async def _fake(session, board_id, **kwargs):
            calls.append(board_id)
            return {"requeued_count": 1, "already_queued_count": 0}

        svc.reprocess_dead_letter_rows = _fake
        return self

    def __exit__(self, *args):
        if self._module and self._orig:
            self._module.reprocess_dead_letter_rows = self._orig


# ---------------------------------------------------------------------------
# AC9: dlq_auto_drain_enabled=False -> reprocess NOT called
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_auto_drain_disabled_skips_reprocess():
    """AC9: when board.settings.dlq_auto_drain_enabled is False, the
    reprocess_dead_letter_rows function must NOT be called.
    """
    board_id = "board-ac9"
    boards_db = _SessionDB(board_rows=[_fake_board(board_id, dlq_auto_drain_enabled=False)])
    factory = _make_session_sequence([boards_db])
    worker = _make_worker(factory)

    calls: list[str] = []
    with _ReprocessPatcher(calls):
        await worker.run_dlq_auto_drain()

    assert calls == [], (
        f"reprocess must NOT be called for a disabled board; got calls={calls}"
    )


# ---------------------------------------------------------------------------
# AC10: enabled=True + dlq>0 + no backoff -> reprocess called 1x + INFO log
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_auto_drain_enabled_calls_reprocess(caplog):
    """AC10: board enabled + DLQ rows present + no backoff in effect ->
    reprocess called exactly once; kg.dlq.auto_drain INFO logged.
    """
    board_id = "board-ac10"
    dlq_row = _fake_dlq_row(board_id, attempts=0)

    # Session 1: load boards
    s1 = _SessionDB(board_rows=[_fake_board(board_id, dlq_auto_drain_enabled=True)])
    # Session 2: count DLQ rows
    s2 = _SessionDB(scalar_value=1)
    # Session 3: load + delete poison rows (none here since attempts=0 < max_attempts=3)
    s3 = _SessionDB(dlq_rows=[dlq_row])
    # Session 4: reprocess (handled by the patched function)
    s4 = _SessionDB()

    factory = _make_session_sequence([s1, s2, s3, s4])
    worker = _make_worker(factory)

    calls: list[str] = []
    with _ReprocessPatcher(calls):
        with caplog.at_level(logging.INFO, logger="okto_pulse.kg.consolidation_worker"):
            await worker.run_dlq_auto_drain()

    assert len(calls) == 1, f"Expected 1 reprocess call; got {calls}"
    assert calls[0] == board_id

    # FR6: kg.dlq.auto_drain INFO line must be present
    drain_logs = [r for r in caplog.records if "kg.dlq.auto_drain" in r.message]
    assert len(drain_logs) >= 1, (
        f"Expected kg.dlq.auto_drain log; got: {[r.message for r in caplog.records]}"
    )

    # Worker must have recorded last_run_at
    stats = worker.get_dlq_drain_stats(board_id)
    assert stats["last_run_at"] is not None
    assert isinstance(stats["requeued_count"], int)


# ---------------------------------------------------------------------------
# AC11: within backoff window -> second call suppressed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_auto_drain_backoff_suppresses_second_run():
    """AC11: after one successful run, a second call within the backoff window
    (default 300 s) must NOT trigger reprocess.
    """
    board_id = "board-ac11"
    dlq_row = _fake_dlq_row(board_id, attempts=0)

    def _make_sessions():
        s1 = _SessionDB(board_rows=[_fake_board(board_id, dlq_auto_drain_enabled=True)])
        s2 = _SessionDB(scalar_value=1)
        s3 = _SessionDB(dlq_rows=[dlq_row])
        s4 = _SessionDB()
        return [s1, s2, s3, s4]

    factory = _make_session_sequence(_make_sessions())
    worker = _make_worker(factory)

    calls: list[str] = []
    with _ReprocessPatcher(calls):
        # First run — should fire
        await worker.run_dlq_auto_drain()
        assert len(calls) == 1, "First run should have called reprocess"

        # Second run immediately — within backoff
        await worker.run_dlq_auto_drain()

    assert len(calls) == 1, (
        f"Backoff must suppress second call; total reprocess calls={len(calls)}"
    )


# ---------------------------------------------------------------------------
# AC12: health response has dlq_auto_drain_last_run_at + dlq_auto_drain_requeued_count
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def drain_health_board(db_factory):
    """Idempotent Board row for drain-health-fields test."""
    async with db_factory() as session:
        existing = await session.get(Board, _BOARD_ID_DRAIN)
        if existing is None:
            session.add(Board(
                id=_BOARD_ID_DRAIN,
                name="dlq-drain-test",
                owner_id=_USER_ID_DRAIN,
            ))
            await session.commit()
    yield _BOARD_ID_DRAIN


@pytest.mark.asyncio
async def test_health_has_drain_fields(db_factory, drain_health_board):
    """AC12: get_kg_health always returns dlq_auto_drain_last_run_at (ISO 8601
    string or None) and dlq_auto_drain_requeued_count (int >= 0).
    """
    async with db_factory() as session:
        result = await get_kg_health(drain_health_board, session)

    assert "dlq_auto_drain_last_run_at" in result, (
        "Health response must carry dlq_auto_drain_last_run_at"
    )
    assert "dlq_auto_drain_requeued_count" in result, (
        "Health response must carry dlq_auto_drain_requeued_count"
    )

    val_ts = result["dlq_auto_drain_last_run_at"]
    assert val_ts is None or isinstance(val_ts, str), (
        f"dlq_auto_drain_last_run_at must be None or str, got {type(val_ts)}"
    )
    if val_ts is not None:
        datetime.fromisoformat(val_ts)  # must be valid ISO 8601

    val_count = result["dlq_auto_drain_requeued_count"]
    assert isinstance(val_count, int) and val_count >= 0, (
        f"dlq_auto_drain_requeued_count must be int >= 0, got {val_count!r}"
    )


# ---------------------------------------------------------------------------
# AC13: poison-pill (attempts >= max_requeue_attempts) -> deleted + WARN logged
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_poison_pill_excluded_and_warned(caplog):
    """AC13: DLQ rows whose attempts >= max_requeue_attempts are permanently
    deleted (not requeued) and a WARN with 'poison_pill_excluded' is logged.
    """
    board_id = "board-ac13"
    max_att = 3

    poison_row = _fake_dlq_row(board_id, attempts=max_att)

    # Session 1: load boards
    s1 = _SessionDB(board_rows=[_fake_board(board_id, dlq_auto_drain_enabled=True)])
    # Session 2: count DLQ rows — 1 row exists
    s2 = _SessionDB(scalar_value=1)
    # Session 3: load poison rows (attempts >= max_att) — returns the poison row
    s3 = _SessionDB(dlq_rows=[poison_row])
    # Session 4: reprocess (patched, should not be called for poison row)
    s4 = _SessionDB()

    factory = _make_session_sequence([s1, s2, s3, s4])
    worker = _make_worker(factory)

    class _FakeSettings:
        kg_queue_dlq_auto_drain_backoff_s = 0
        kg_queue_dlq_auto_drain_max_requeue_attempts = max_att

    calls: list[str] = []
    with _ReprocessPatcher(calls):
        with caplog.at_level(logging.WARNING, logger="okto_pulse.kg.consolidation_worker"):
            with patch(
                "okto_pulse.core.infra.config.get_settings",
                return_value=_FakeSettings(),
            ):
                await worker.run_dlq_auto_drain()

    # The poison-pill row must have been sent to db.delete
    assert poison_row in s3.deleted, (
        f"Poison-pill row must be deleted; s3.deleted={s3.deleted}"
    )

    # WARN with poison_pill_excluded must appear in log
    warn_logs = [r for r in caplog.records if "poison_pill_excluded" in r.message]
    assert len(warn_logs) >= 1, (
        f"Expected poison_pill_excluded WARN; got: {[r.message for r in caplog.records]}"
    )
