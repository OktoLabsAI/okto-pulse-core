"""Spec R01A IMP6 — worker/background transaction parity (session factory
outside the HTTP cycle), ac_e7064abb / ac_2eb75d75 (AC3).

The two background workers own a session via the registered ``session_factory``
OUTSIDE any HTTP request. This suite proves their commit/rollback is preserved:

- ``ConsolidationProcessor`` (per-entry session): a mid-flow failure rolls back ALL
  of that entry's writes (no partial persistence) and the entry is re-pended /
  dead-lettered — never silently acked; the success path commits + DELETE-on-acks.
- ``GlobalOutboxProcessor`` (witness): a per-event apply failure leaves the event
  unprocessed (``processed_at`` None) with ``retry_count`` incremented — the
  outside-HTTP commit never falsely marks a failed event done.
"""

from __future__ import annotations

import asyncio
import inspect
import uuid

import pytest
from sqlalchemy import delete, select

from okto_pulse.core.application.processors.global_outbox import GlobalOutboxProcessor
from okto_pulse.core.application.processors import consolidation as consolidation_mod
from okto_pulse.core.application.processors.consolidation import ConsolidationProcessor


def _board_id() -> str:
    return f"r01a-imp6-{uuid.uuid4().hex[:8]}"


class _TestClaimRepository:
    async def claim_global_outbox(self, session, *, limit: int):
        from sqlalchemy_test_models import GlobalUpdateOutbox

        rows = (
            (
                await session.execute(
                    select(GlobalUpdateOutbox)
                    .where(
                        GlobalUpdateOutbox.processed_at.is_(None),
                        GlobalUpdateOutbox.retry_count >= 0,
                    )
                    .order_by(GlobalUpdateOutbox.created_at.asc())
                    .limit(limit)
                )
            )
            .scalars()
            .all()
        )
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
    from sqlalchemy_test_models import Board, ConsolidationQueue

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
async def test_consolidation_rolls_back_partial_writes_on_mid_flow_failure(
    monkeypatch,
) -> None:
    """AC3/ac_e7064abb: a failure in the middle of the per-entry flow persists NO
    partial data; the entry is failure-handled (not acked)."""
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import ConsolidationDeadLetter, ConsolidationQueue

    factory = get_session_factory()
    board_id = _board_id()
    entry_id = await _seed_queue_entry(factory, board_id)
    marker_id = f"cq-marker-{uuid.uuid4().hex[:8]}"

    async def _failing(db, entry, **_kwargs):
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
    worker = ConsolidationProcessor(relational_scope_factory=factory)
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
                (
                    await db.execute(
                        select(ConsolidationDeadLetter).where(
                            ConsolidationDeadLetter.board_id == board_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert dlq, "entry neither re-pended nor dead-lettered"


@pytest.mark.asyncio
async def test_consolidation_commits_and_acks_on_success(monkeypatch) -> None:
    """Commit parity: a successful per-entry flow commits its writes and the
    processed entry is removed (DELETE-on-ack)."""
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import ConsolidationQueue

    factory = get_session_factory()
    board_id = _board_id()
    entry_id = await _seed_queue_entry(factory, board_id)
    marker_id = f"cq-ok-{uuid.uuid4().hex[:8]}"

    async def _ok(db, entry, **_kwargs):
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
    worker = ConsolidationProcessor(relational_scope_factory=factory)
    await worker.process_batch()

    async with factory() as db:
        assert await db.get(ConsolidationQueue, marker_id) is not None  # committed
        assert await db.get(ConsolidationQueue, entry_id) is None  # DELETE-on-ack


@pytest.mark.asyncio
async def test_consolidation_finalizes_graph_session_only_after_main_uow_commit(
    monkeypatch,
) -> None:
    """The graph session remains compensatable until ledger+audit+ACK commit."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    board_id = _board_id()
    await _seed_queue_entry(factory, board_id)
    delegate = consolidation_mod.get_consolidation_persistence_port()
    events: list[str] = []

    class _RecordingStore:
        def __getattr__(self, name):
            return getattr(delegate, name)

        async def commit(self, db):
            await delegate.commit(db)
            events.append(f"commit:{sum(e.startswith('commit:') for e in events) + 1}")

    async def _process(_db, _entry, **kwargs):
        kwargs["deferred_session_ids"].append("kgses-worker-deferred")
        events.append("process")
        return True

    async def _finalize(session_id: str, *, agent_id: str) -> None:
        assert session_id == "kgses-worker-deferred"
        assert agent_id == consolidation_mod.AGENT_ID
        events.append("finalize")

    async def _abort(*_args, **_kwargs) -> None:
        pytest.fail("successful relational commit must not compensate the graph")

    monkeypatch.setattr(
        consolidation_mod,
        "get_consolidation_persistence_port",
        lambda: _RecordingStore(),
    )
    monkeypatch.setattr(consolidation_mod, "_process_queue_entry_serialized", _process)
    monkeypatch.setattr(consolidation_mod, "finalize_deferred_consolidation", _finalize)
    monkeypatch.setattr(consolidation_mod, "abort_deferred_consolidation", _abort)
    monkeypatch.setattr(
        consolidation_mod,
        "_run_post_commit_maintenance",
        lambda *_args, **_kwargs: asyncio.sleep(0),
    )

    worker = ConsolidationProcessor(relational_scope_factory=factory)
    assert await worker.process_batch() == 1

    assert events.index("commit:2") < events.index("finalize")
    assert events.index("finalize") < events.index("commit:3")


@pytest.mark.asyncio
async def test_consolidation_commit_failure_compensates_before_queue_retry(
    monkeypatch,
) -> None:
    """A failed main SQLite commit cannot strand graph-ahead state."""
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import ConsolidationQueue

    factory = get_session_factory()
    board_id = _board_id()
    entry_id = await _seed_queue_entry(factory, board_id)
    delegate = consolidation_mod.get_consolidation_persistence_port()
    events: list[str] = []
    commit_calls = 0

    class _FailingMainCommitStore:
        def __getattr__(self, name):
            return getattr(delegate, name)

        async def commit(self, db):
            nonlocal commit_calls
            commit_calls += 1
            if commit_calls == 2:
                events.append("main_commit_failed")
                raise RuntimeError("simulated SQLite commit failure")
            await delegate.commit(db)
            events.append(f"commit:{commit_calls}")

    async def _process(_db, _entry, **kwargs):
        kwargs["deferred_session_ids"].append("kgses-worker-rollback")
        return True

    async def _finalize(*_args, **_kwargs) -> None:
        pytest.fail("failed relational commit must not finalize the graph session")

    async def _abort(session_id: str, *, agent_id: str, blocking_execution) -> None:
        assert session_id == "kgses-worker-rollback"
        assert agent_id == consolidation_mod.AGENT_ID
        assert blocking_execution is not None
        events.append("abort")

    monkeypatch.setattr(
        consolidation_mod,
        "get_consolidation_persistence_port",
        lambda: _FailingMainCommitStore(),
    )
    monkeypatch.setattr(consolidation_mod, "_process_queue_entry_serialized", _process)
    monkeypatch.setattr(consolidation_mod, "finalize_deferred_consolidation", _finalize)
    monkeypatch.setattr(consolidation_mod, "abort_deferred_consolidation", _abort)

    worker = ConsolidationProcessor(relational_scope_factory=factory)
    assert await worker.process_batch() == 0

    assert events.index("main_commit_failed") < events.index("abort")
    async with factory() as db:
        fresh = await db.get(ConsolidationQueue, entry_id)
        assert fresh is not None
        assert fresh.attempts >= 1
        assert "simulated SQLite commit failure" in (fresh.last_error or "")


@pytest.mark.asyncio
async def test_semantic_event_invalidating_claim_compensates_stale_graph_commit(
    monkeypatch,
) -> None:
    """A cancel/archive event that wins the ACK race keeps its follow-up work.

    The graph pipeline is represented as already committed-but-deferred when
    the event transaction changes the queue row back to pending. The stale
    worker must lose its exact ACK CAS and compensate that graph mutation,
    while the pending successor remains durable for authoritative re-read.
    """

    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import ConsolidationQueue

    factory = get_session_factory()
    board_id = _board_id()
    entry_id = await _seed_queue_entry(factory, board_id)
    events: list[str] = []

    async def _process(_db, entry, **kwargs):
        kwargs["deferred_session_ids"].append("kgses-stale-snapshot")
        async with factory() as event_db:
            current = await event_db.get(ConsolidationQueue, entry.id)
            assert current is not None
            assert current.status == "claimed"
            current.status = "pending"
            current.claim_token = None
            current.claimed_by_session_id = None
            current.claimed_at = None
            current.worker_id = None
            current.claim_timeout_at = None
            current.triggered_by_event = "card.cancelled"
            await event_db.commit()
        events.append("event_invalidated_claim")
        return True

    async def _finalize(*_args, **_kwargs) -> None:
        pytest.fail("a stale graph snapshot must never be finalized")

    async def _abort(session_id: str, *, agent_id: str, blocking_execution) -> None:
        assert session_id == "kgses-stale-snapshot"
        assert agent_id == consolidation_mod.AGENT_ID
        assert blocking_execution is not None
        events.append("graph_compensated")

    monkeypatch.setattr(consolidation_mod, "_process_queue_entry_serialized", _process)
    monkeypatch.setattr(consolidation_mod, "finalize_deferred_consolidation", _finalize)
    monkeypatch.setattr(consolidation_mod, "abort_deferred_consolidation", _abort)

    worker = ConsolidationProcessor(relational_scope_factory=factory)
    assert await worker.process_batch() == 0

    assert events == ["event_invalidated_claim", "graph_compensated"]
    async with factory() as db:
        successor = await db.get(ConsolidationQueue, entry_id)
    assert successor is not None
    assert successor.status == "pending"
    assert successor.claim_token is None
    assert successor.triggered_by_event == "card.cancelled"


@pytest.mark.asyncio
async def test_outbox_worker_failure_does_not_falsely_mark_processed(
    monkeypatch,
) -> None:
    """GlobalOutboxProcessor witness (session factory outside HTTP): a per-event apply
    failure leaves the event unprocessed with retry_count incremented — the batch
    commit never falsely marks a failed event done."""
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import GlobalUpdateOutbox

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

    worker = GlobalOutboxProcessor(factory, claim_repository=_TestClaimRepository())

    async def _failing_apply(event, db):
        raise RuntimeError("outbox apply failure (R01A IMP6)")

    monkeypatch.setattr(worker, "_apply_event", _failing_apply)
    await worker.process_once()

    async with factory() as db:
        row = (
            await db.execute(
                select(GlobalUpdateOutbox).where(
                    GlobalUpdateOutbox.event_id == event_id
                )
            )
        ).scalar_one()
        assert row.processed_at is None  # NOT falsely marked done
        assert row.retry_count >= 1  # failure recorded
        assert row.last_error and "outbox apply failure" in row.last_error


def test_background_workers_receive_relational_scopes_outside_http() -> None:
    """Workers receive opaque relational scopes instead of HTTP dependencies."""
    for worker in (ConsolidationProcessor, GlobalOutboxProcessor):
        parameters = inspect.signature(worker.__init__).parameters
        assert "relational_scope_factory" in parameters
        assert "session_factory" not in parameters
