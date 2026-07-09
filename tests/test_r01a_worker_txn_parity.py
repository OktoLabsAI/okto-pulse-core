"""Spec R01A IMP6 — worker/background transaction parity (session factory
outside the HTTP cycle), ac_e7064abb / ac_2eb75d75 (AC3).

The two background workers own a session via the registered ``session_factory``
OUTSIDE any HTTP request. This suite proves their commit/rollback is preserved:

- ``ConsolidationWorker`` (per-entry session): a mid-flow failure rolls back ALL
  of that entry's writes (no partial persistence) and the entry is re-pended /
  dead-lettered — never silently acked; the success path commits + DELETE-on-acks.
- ``OutboxWorker`` (witness): a per-event apply failure leaves the event
  unprocessed (``processed_at`` None) with ``retry_count`` incremented — the
  outside-HTTP commit never falsely marks a failed event done.
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from sqlalchemy import delete, select

from okto_pulse.core.kg.global_discovery.outbox_worker import OutboxWorker
from okto_pulse.core.kg.workers import consolidation as consolidation_mod
from okto_pulse.core.kg.workers.consolidation import ConsolidationWorker


def _board_id() -> str:
    return f"r01a-imp6-{uuid.uuid4().hex[:8]}"


class _TestClaimRepository:
    async def claim_global_outbox(self, session, *, limit: int):
        from okto_pulse.core.models.db import GlobalUpdateOutbox

        rows = (
            await session.execute(
                select(GlobalUpdateOutbox)
                .where(
                    GlobalUpdateOutbox.processed_at.is_(None),
                    GlobalUpdateOutbox.retry_count >= 0,
                )
                .order_by(GlobalUpdateOutbox.created_at.asc())
                .limit(limit)
            )
        ).scalars().all()
        return list(rows)

    async def claim_domain_event_executions(self, session, *, limit: int, now):
        return []

    async def claim_consolidation_queue(
        self,
        session,
        *,
        board_id: str | None,
        limit: int,
    ):
        return []


async def _seed_queue_entry(factory, board_id: str) -> str:
    from okto_pulse.core.models.db import Board, ConsolidationQueue

    entry_id = f"cq-{uuid.uuid4().hex[:10]}"
    async with factory() as db:
        # Hermetic seed: process_batch's claim is board-AGNOSTIC with
        # limit=batch_size ordered by priority/triggered_at asc, and the test
        # DB is session-scoped/shared — leftover queue rows from earlier tests
        # crowd this entry out of the batch AND get fed to the monkeypatched
        # per-entry callbacks (foreign acks / duplicate-marker IntegrityError).
        # Drain the queue so the worker under test sees exactly one entry.
        await db.execute(delete(ConsolidationQueue))
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="imp6", owner_id="imp6-owner"))
        db.add(
            ConsolidationQueue(
                id=entry_id,
                board_id=board_id,
                artifact_type="spec",
                artifact_id=f"a-{uuid.uuid4().hex[:6]}",
                status="pending",
            )
        )
        await db.commit()
    return entry_id


@pytest.mark.asyncio
async def test_consolidation_rolls_back_partial_writes_on_mid_flow_failure(monkeypatch) -> None:
    """AC3/ac_e7064abb: a failure in the middle of the per-entry flow persists NO
    partial data; the entry is failure-handled (not acked)."""
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.models.db import ConsolidationDeadLetter, ConsolidationQueue

    factory = get_session_factory()
    board_id = _board_id()
    entry_id = await _seed_queue_entry(factory, board_id)
    marker_id = f"cq-marker-{uuid.uuid4().hex[:8]}"

    async def _failing(db, entry):
        # Multi-step flow: write a partial side-effect, then fail mid-way.
        db.add(
            ConsolidationQueue(
                id=marker_id,
                board_id=board_id,
                artifact_type="marker",
                artifact_id="partial",
            )
        )
        await db.flush()
        raise RuntimeError("mid-flow failure (R01A IMP6)")

    monkeypatch.setattr(consolidation_mod, "_process_queue_entry_serialized", _failing)
    worker = ConsolidationWorker(session_factory=factory)
    await worker.process_batch()

    async with factory() as db:
        # The partial write rolled back — no partial persistence (the core proof).
        assert await db.get(ConsolidationQueue, marker_id) is None
        # The entry was failure-handled, not silently acked-as-success.
        fresh = await db.get(ConsolidationQueue, entry_id)
        if fresh is not None:
            assert fresh.attempts >= 1
            assert fresh.last_error and "mid-flow failure" in fresh.last_error
        else:
            dlq = (
                await db.execute(
                    select(ConsolidationDeadLetter).where(
                        ConsolidationDeadLetter.board_id == board_id
                    )
                )
            ).scalars().all()
            assert dlq, "entry neither re-pended nor dead-lettered"


@pytest.mark.asyncio
async def test_consolidation_commits_and_acks_on_success(monkeypatch) -> None:
    """Commit parity: a successful per-entry flow commits its writes and the
    processed entry is removed (DELETE-on-ack)."""
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.models.db import ConsolidationQueue

    factory = get_session_factory()
    board_id = _board_id()
    entry_id = await _seed_queue_entry(factory, board_id)
    marker_id = f"cq-ok-{uuid.uuid4().hex[:8]}"

    async def _ok(db, entry):
        db.add(
            ConsolidationQueue(
                id=marker_id,
                board_id=board_id,
                artifact_type="marker_ok",
                artifact_id="committed",
            )
        )
        await db.flush()
        return True

    monkeypatch.setattr(consolidation_mod, "_process_queue_entry_serialized", _ok)
    worker = ConsolidationWorker(session_factory=factory)
    await worker.process_batch()

    async with factory() as db:
        assert await db.get(ConsolidationQueue, marker_id) is not None  # committed
        assert await db.get(ConsolidationQueue, entry_id) is None  # DELETE-on-ack


@pytest.mark.asyncio
async def test_outbox_worker_failure_does_not_falsely_mark_processed(monkeypatch) -> None:
    """OutboxWorker witness (session factory outside HTTP): a per-event apply
    failure leaves the event unprocessed with retry_count incremented — the batch
    commit never falsely marks a failed event done."""
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.models.db import GlobalUpdateOutbox

    factory = get_session_factory()
    board_id = _board_id()
    event_id = f"evt-{uuid.uuid4().hex[:10]}"
    async with factory() as db:
        db.add(
            GlobalUpdateOutbox(
                event_id=event_id,
                board_id=board_id,
                session_id="imp6-sess",
                event_type="node_upsert",
                payload={},
            )
        )
        await db.commit()

    worker = OutboxWorker(factory, claim_repository=_TestClaimRepository())

    async def _failing_apply(event, db):
        raise RuntimeError("outbox apply failure (R01A IMP6)")

    monkeypatch.setattr(worker, "_apply_event", _failing_apply)
    await worker.process_once()

    async with factory() as db:
        row = (
            await db.execute(
                select(GlobalUpdateOutbox).where(GlobalUpdateOutbox.event_id == event_id)
            )
        ).scalar_one()
        assert row.processed_at is None  # NOT falsely marked done
        assert row.retry_count >= 1  # failure recorded
        assert row.last_error and "outbox apply failure" in row.last_error


def test_background_workers_own_session_factory_outside_http() -> None:
    """Both workers take a ``session_factory`` (not the request-scoped get_db) —
    they run outside the HTTP cycle."""
    assert "session_factory" in inspect.signature(ConsolidationWorker.__init__).parameters
    assert "session_factory" in inspect.signature(OutboxWorker.__init__).parameters
