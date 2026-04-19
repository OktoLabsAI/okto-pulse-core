"""Tests for the internal event bus (core/events/).

Covers the core invariants of the outbox pattern:

- publish() is atomic with the caller's tx (rollback undoes event rows)
- dispatcher drains pending → status='done' and fires the handler
- ConsolidationEnqueuer: dedup bloqueia duplicatas + priority mapping
- retry with exponential backoff; DLQ after MAX_ATTEMPTS
- startup recovery resets orphaned 'processing' rows
- registry is populated with the 12 MVP event types
- FIFO ordering by (occurred_at, id)
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import select, update

from okto_pulse.core.events import EVENT_TYPES, EventBus, publish, register_handler
from okto_pulse.core.events.dispatcher import (
    BACKOFF_BASE,
    DRAIN_BATCH_SIZE,
    EventDispatcher,
    MAX_ATTEMPTS,
)
from okto_pulse.core.events.handlers.cancellation_decay import (
    DECAY_PENALTY,
    REVOCATION_REASON,
    CancellationDecayHandler,
    CancellationRestoreHandler,
)
from okto_pulse.core.events.handlers.consolidation_enqueuer import (
    ConsolidationEnqueuer,
)
from okto_pulse.core.events.types import (
    CardCancelled,
    CardCreated,
    CardMoved,
    CardRestored,
    SpecVersionBumped,
)
from okto_pulse.core.kg.schema import (
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)
from okto_pulse.core.models.db import (
    Board,
    ConsolidationQueue,
    DomainEventHandlerExecution,
    DomainEventRow,
)


BOARD_ID = "board-events-test"
USER_ID = "user-events-test"


@pytest_asyncio.fixture
async def event_board(db_factory):
    """Create a Board row so FK constraints on domain_events.board_id pass."""
    async with db_factory() as session:
        # Idempotent: reuse if already present from a prior test.
        existing = await session.get(Board, BOARD_ID)
        if existing is None:
            session.add(Board(id=BOARD_ID, name="events-test", owner_id=USER_ID))
            await session.commit()
    yield BOARD_ID


@pytest_asyncio.fixture
async def clean_tables(db_factory, event_board):
    """Wipe events / executions / queue rows before each test."""
    async with db_factory() as session:
        await session.execute(
            DomainEventHandlerExecution.__table__.delete()
        )
        await session.execute(DomainEventRow.__table__.delete())
        await session.execute(
            ConsolidationQueue.__table__.delete().where(
                ConsolidationQueue.board_id == BOARD_ID
            )
        )
        await session.commit()
    yield


# --- AC1: publish is atomic with caller tx ---


@pytest.mark.asyncio
async def test_publish_rolled_back_does_not_persist(db_factory, clean_tables):
    """Rollback after publish() leaves no event/execution rows behind."""
    async with db_factory() as session:
        await publish(
            CardCreated(
                board_id=BOARD_ID,
                actor_id=USER_ID,
                card_id="card-rollback",
                spec_id="spec-x",
                card_type="normal",
                priority="none",
            ),
            session=session,
        )
        # Simulate a caller-level failure: rollback instead of commit.
        await session.rollback()

    async with db_factory() as session:
        events = (await session.execute(select(DomainEventRow))).scalars().all()
        execs = (await session.execute(select(DomainEventHandlerExecution))).scalars().all()
        assert events == []
        assert execs == []


@pytest.mark.asyncio
async def test_publish_committed_inserts_event_and_execution(db_factory, clean_tables):
    async with db_factory() as session:
        await publish(
            CardCreated(
                board_id=BOARD_ID,
                actor_id=USER_ID,
                card_id="card-ok",
                spec_id="spec-ok",
                card_type="normal",
                priority="none",
            ),
            session=session,
        )
        await session.commit()

    async with db_factory() as session:
        events = (await session.execute(select(DomainEventRow))).scalars().all()
        assert len(events) == 1
        assert events[0].event_type == "card.created"
        assert events[0].board_id == BOARD_ID

        execs = (await session.execute(select(DomainEventHandlerExecution))).scalars().all()
        assert len(execs) == 1
        assert execs[0].handler_name == "ConsolidationEnqueuer"
        assert execs[0].status == "pending"


# --- AC14: registry has the 12 MVP events ---


def test_registry_has_twelve_events():
    for et in EVENT_TYPES:
        assert et in EventBus._registry, f"{et} not registered"
        assert ConsolidationEnqueuer in EventBus._registry[et]


# --- AC2: dispatcher drains event → ConsolidationQueue row appears ---


@pytest.mark.asyncio
async def test_dispatcher_drain_creates_consolidation_queue_row(
    db_factory, clean_tables
):
    async with db_factory() as session:
        await publish(
            CardCreated(
                board_id=BOARD_ID,
                actor_id=USER_ID,
                card_id="card-drain",
                spec_id="spec-drain",
                card_type="normal",
                priority="none",
            ),
            session=session,
        )
        await session.commit()

    dispatcher = EventDispatcher(db_factory)
    try:
        await dispatcher.start()
        # Wake + drain happens via the loop itself; give it a beat.
        await asyncio.sleep(0.5)
    finally:
        await dispatcher.stop(timeout=2.0)

    async with db_factory() as session:
        execs = (await session.execute(select(DomainEventHandlerExecution))).scalars().all()
        assert len(execs) == 1
        assert execs[0].status == "done"
        assert execs[0].processed_at is not None

        queue_rows = (await session.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.board_id == BOARD_ID,
                ConsolidationQueue.artifact_id == "card-drain",
            )
        )).scalars().all()
        assert len(queue_rows) == 1
        assert queue_rows[0].artifact_type == "card"
        assert queue_rows[0].source == "event:card.created"
        assert queue_rows[0].priority == "normal"


# --- AC4: dedup blocks duplicates on the same entity ---


@pytest.mark.asyncio
async def test_dedup_same_entity_only_one_queue_row(db_factory, clean_tables):
    card_id = "card-dedup"
    async with db_factory() as session:
        # Pre-seed a pending ConsolidationQueue row for this entity.
        session.add(ConsolidationQueue(
            board_id=BOARD_ID,
            artifact_type="card",
            artifact_id=card_id,
            priority="normal",
            source="seed",
            status="pending",
        ))
        await session.commit()

    async with db_factory() as session:
        await publish(
            CardMoved(
                board_id=BOARD_ID,
                actor_id=USER_ID,
                card_id=card_id,
                from_status="not_started",
                to_status="in_progress",
            ),
            session=session,
        )
        await session.commit()

    dispatcher = EventDispatcher(db_factory)
    try:
        await dispatcher.start()
        await asyncio.sleep(0.5)
    finally:
        await dispatcher.stop(timeout=2.0)

    async with db_factory() as session:
        queue_rows = (await session.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.board_id == BOARD_ID,
                ConsolidationQueue.artifact_id == card_id,
            )
        )).scalars().all()
        # Seed + dedup-no-op → still only 1 row
        assert len(queue_rows) == 1


# --- AC3: dedup does NOT block distinct entities ---


@pytest.mark.asyncio
async def test_dedup_allows_distinct_entities(db_factory, clean_tables):
    async with db_factory() as session:
        for cid in ("card-a", "card-b"):
            await publish(
                CardCreated(
                    board_id=BOARD_ID,
                    actor_id=USER_ID,
                    card_id=cid,
                    spec_id="spec-distinct",
                    card_type="normal",
                    priority="none",
                ),
                session=session,
            )
        await session.commit()

    dispatcher = EventDispatcher(db_factory)
    try:
        await dispatcher.start()
        await asyncio.sleep(0.8)
    finally:
        await dispatcher.stop(timeout=2.0)

    async with db_factory() as session:
        queue_rows = (await session.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.board_id == BOARD_ID,
                ConsolidationQueue.artifact_type == "card",
            )
        )).scalars().all()
        ids = sorted(r.artifact_id for r in queue_rows)
        assert ids == ["card-a", "card-b"]


# --- AC5: card.cancelled → priority high ---


@pytest.mark.asyncio
async def test_card_cancelled_gets_priority_high(db_factory, clean_tables):
    async with db_factory() as session:
        await publish(
            CardCancelled(
                board_id=BOARD_ID,
                actor_id=USER_ID,
                card_id="card-cancel",
                previous_status="in_progress",
            ),
            session=session,
        )
        await session.commit()

    dispatcher = EventDispatcher(db_factory)
    try:
        await dispatcher.start()
        await asyncio.sleep(0.5)
    finally:
        await dispatcher.stop(timeout=2.0)

    async with db_factory() as session:
        queue_rows = (await session.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.board_id == BOARD_ID,
                ConsolidationQueue.artifact_id == "card-cancel",
            )
        )).scalars().all()
        assert len(queue_rows) == 1
        assert queue_rows[0].priority == "high"
        assert queue_rows[0].source == "event:card.cancelled"


# --- AC6: spec.version_bumped → priority high ---


@pytest.mark.asyncio
async def test_spec_version_bumped_gets_priority_high(db_factory, clean_tables):
    async with db_factory() as session:
        await publish(
            SpecVersionBumped(
                board_id=BOARD_ID,
                actor_id=USER_ID,
                spec_id="spec-bumped",
                old_version=1,
                new_version=2,
                changed_fields=["functional_requirements"],
            ),
            session=session,
        )
        await session.commit()

    dispatcher = EventDispatcher(db_factory)
    try:
        await dispatcher.start()
        await asyncio.sleep(0.5)
    finally:
        await dispatcher.stop(timeout=2.0)

    async with db_factory() as session:
        queue_rows = (await session.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.board_id == BOARD_ID,
                ConsolidationQueue.artifact_id == "spec-bumped",
            )
        )).scalars().all()
        assert len(queue_rows) == 1
        assert queue_rows[0].priority == "high"


# --- AC9: startup recovery resets orphan 'processing' rows ---


@pytest.mark.asyncio
async def test_startup_recovery_resets_processing_to_pending(db_factory, clean_tables):
    async with db_factory() as session:
        # Seed an orphan: event + execution stuck in 'processing'.
        event = DomainEventRow(
            id="evt-recover",
            event_type="card.created",
            board_id=BOARD_ID,
            actor_id=USER_ID,
            actor_type="user",
            payload_json={
                "card_id": "card-recover",
                "spec_id": "spec-recover",
                "card_type": "normal",
                "priority": "none",
            },
            occurred_at=datetime.now(timezone.utc),
        )
        session.add(event)
        session.add(DomainEventHandlerExecution(
            event_id="evt-recover",
            handler_name="ConsolidationEnqueuer",
            status="processing",
            attempts=1,
        ))
        await session.commit()

    dispatcher = EventDispatcher(db_factory)
    try:
        await dispatcher.start()
        # Wait for drain to pick up the now-'pending' recovered row.
        await asyncio.sleep(0.8)
    finally:
        await dispatcher.stop(timeout=2.0)

    async with db_factory() as session:
        exec_row = (await session.execute(
            select(DomainEventHandlerExecution).where(
                DomainEventHandlerExecution.event_id == "evt-recover"
            )
        )).scalar_one()
        assert exec_row.status == "done"


# --- AC7/AC8: retry with backoff → DLQ after MAX_ATTEMPTS ---


class _FailingEventHandler:
    """Always raises; registered dynamically for these tests."""

    async def handle(self, event, session):  # noqa: ARG002
        raise RuntimeError("simulated failure")


@pytest.mark.asyncio
async def test_retry_then_dlq_after_max_attempts(db_factory, clean_tables):
    """Register a failing handler on a REAL event type and exhaust retries.

    Uses card.created so _event_from_row can reconstruct successfully; the
    failing handler itself is what triggers retries → DLQ.
    """
    event_type = "card.created"

    # Inject failing handler into the registry alongside ConsolidationEnqueuer.
    EventBus._registry.setdefault(event_type, []).append(_FailingEventHandler)
    try:
        async with db_factory() as session:
            event = DomainEventRow(
                id="evt-dlq",
                event_type=event_type,
                board_id=BOARD_ID,
                actor_id=USER_ID,
                actor_type="user",
                payload_json={
                    "card_id": "card-dlq",
                    "spec_id": "spec-dlq",
                    "card_type": "normal",
                    "priority": "none",
                },
                occurred_at=datetime.now(timezone.utc),
            )
            session.add(event)
            session.add(DomainEventHandlerExecution(
                event_id="evt-dlq",
                handler_name="_FailingEventHandler",
                status="pending",
                attempts=0,
            ))
            await session.commit()

        dispatcher = EventDispatcher(db_factory)
        try:
            await dispatcher.start()
            # Fast-forward: after each failed attempt, reset next_attempt_at
            # so the next drain picks it up immediately. Loop until the row
            # either lands in DLQ or we hit a safety cap.
            for _ in range(MAX_ATTEMPTS * 3):
                async with db_factory() as s:
                    await s.execute(
                        update(DomainEventHandlerExecution)
                        .where(DomainEventHandlerExecution.event_id == "evt-dlq")
                        .where(DomainEventHandlerExecution.status == "pending")
                        .values(next_attempt_at=None)
                    )
                    await s.commit()
                    row = (await s.execute(
                        select(DomainEventHandlerExecution).where(
                            DomainEventHandlerExecution.event_id == "evt-dlq",
                            DomainEventHandlerExecution.handler_name == "_FailingEventHandler",
                        )
                    )).scalar_one()
                    if row.status == "dlq":
                        break
                dispatcher.notify()
                await asyncio.sleep(0.25)
        finally:
            await dispatcher.stop(timeout=2.0)

        async with db_factory() as session:
            exec_row = (await session.execute(
                select(DomainEventHandlerExecution).where(
                    DomainEventHandlerExecution.event_id == "evt-dlq",
                    DomainEventHandlerExecution.handler_name == "_FailingEventHandler",
                )
            )).scalar_one()
            assert exec_row.status == "dlq"
            assert exec_row.attempts >= MAX_ATTEMPTS
            assert exec_row.last_error is not None
            assert "simulated failure" in exec_row.last_error
    finally:
        # Undo dynamic registration so other tests keep a clean registry.
        if _FailingEventHandler in EventBus._registry.get(event_type, []):
            EventBus._registry[event_type].remove(_FailingEventHandler)


# --- AC10: publish latency stays low ---


@pytest.mark.asyncio
async def test_publish_latency_under_15ms(db_factory, clean_tables):
    """Sanity check: publish() is not a hot path."""
    import time

    async with db_factory() as session:
        start = time.perf_counter()
        await publish(
            CardCreated(
                board_id=BOARD_ID,
                actor_id=USER_ID,
                card_id="card-latency",
                spec_id="spec-latency",
                card_type="normal",
                priority="none",
            ),
            session=session,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        await session.commit()

    # 15ms is generous for SQLite in-memory; real-world is ≪1ms.
    assert elapsed_ms < 15.0, f"publish took {elapsed_ms:.2f}ms"


# --- AC11: dispatcher start + stop are observable ---


@pytest.mark.asyncio
async def test_dispatcher_start_and_stop(db_factory, clean_tables, caplog):
    """start()/stop() log the expected messages and leave task in done state."""
    import logging

    caplog.set_level(logging.INFO, logger="okto_pulse.core.events.dispatcher")

    dispatcher = EventDispatcher(db_factory)
    await dispatcher.start()
    assert dispatcher._task is not None
    assert not dispatcher._task.done()

    await dispatcher.stop(timeout=2.0)
    assert dispatcher._task.done()

    log_text = " ".join(r.message for r in caplog.records)
    assert "EventDispatcher started" in log_text
    assert "EventDispatcher stopped" in log_text


# --- Sanity: payload_for_storage excludes top-level fields ---


def test_payload_for_storage_excludes_top_level_fields():
    event = CardCreated(
        board_id=BOARD_ID,
        actor_id=USER_ID,
        card_id="card-payload",
        spec_id="spec-payload",
        card_type="normal",
        priority="none",
    )
    payload = event.payload_for_storage()
    assert "event_id" not in payload
    assert "board_id" not in payload
    assert "actor_id" not in payload
    assert "actor_type" not in payload
    assert "occurred_at" not in payload
    assert payload["card_id"] == "card-payload"
    assert payload["spec_id"] == "spec-payload"


# ---------------------------------------------------------------------------
# KG integrity pack — Fase 1 tests
# CancellationDecayHandler + CancellationRestoreHandler (spec 2c4d500b)
# ---------------------------------------------------------------------------


def _seed_kuzu_node(
    board_id: str,
    node_type: str,
    node_id: str,
    card_id: str,
    relevance_score: float,
    revocation_reason: str | None = None,
) -> None:
    """Insert one Kùzu node derived from a card (source_artifact_ref='card:{id}').

    Uses the deterministic stub embedding (384 zeros) so we don't depend on
    sentence-transformers in unit tests. Explicitly releases the Python-side
    kuzu handles + runs gc so the handler running inside the dispatcher can
    acquire the Windows exclusive file lock.
    """
    import gc as _gc

    bootstrap_board_graph(board_id)
    _gc.collect()
    bc = open_board_connection(board_id)
    try:
        bc.conn.execute(
            f"CREATE (n:{node_type} "
            "{id: $id, title: $title, content: '', context: '', "
            "justification: '', source_artifact_ref: $ref, source_session_id: '', "
            "created_at: timestamp($now), created_by_agent: 'test', "
            "source_confidence: 0.8, relevance_score: $score, "
            "query_hits: 0, last_queried_at: NULL, "
            "superseded_by: NULL, superseded_at: NULL, "
            "revocation_reason: $reason, embedding: $emb})",
            {
                "id": node_id,
                "title": f"{node_type} for {card_id}",
                "ref": f"card:{card_id}",
                "now": datetime.now(timezone.utc).isoformat(),
                "score": relevance_score,
                "reason": revocation_reason,
                "emb": [0.0] * 384,
            },
        )
    finally:
        bc.close()
        del bc
        _gc.collect()


def _fetch_node_fields(
    board_id: str, node_type: str, node_id: str
) -> dict:
    """Read a node's mutable fields — returns empty dict if node missing."""
    import gc as _gc

    bc = open_board_connection(board_id)
    try:
        result = bc.conn.execute(
            f"MATCH (n:{node_type}) WHERE n.id = $id "
            "RETURN n.relevance_score, n.revocation_reason, n.superseded_at",
            {"id": node_id},
        )
        if not result.has_next():
            fields: dict = {}
        else:
            row = result.get_next()
            fields = {
                "relevance_score": row[0],
                "revocation_reason": row[1],
                "superseded_at": row[2],
            }
    finally:
        bc.close()
        del bc
        _gc.collect()
    return fields


@pytest_asyncio.fixture
async def decay_board(event_board):
    """Ensure the Kùzu graph exists and is cleaned between tests."""
    import gc as _gc

    bootstrap_board_graph(event_board)
    _gc.collect()
    bc = open_board_connection(event_board)
    try:
        for nt in ("Entity", "Decision"):
            try:
                bc.conn.execute(f"MATCH (n:{nt}) DELETE n")
            except Exception:  # noqa: BLE001 — best-effort cleanup
                pass
    finally:
        bc.close()
        del bc
        _gc.collect()
    yield event_board
    close_all_connections(event_board)
    _gc.collect()


def test_decay_handlers_registered():
    """Both new handlers appear in the bus registry next to the enqueuer."""
    assert CancellationDecayHandler in EventBus._registry.get("card.cancelled", [])
    assert CancellationRestoreHandler in EventBus._registry.get("card.restored", [])
    # And ConsolidationEnqueuer is STILL there for the same events.
    assert ConsolidationEnqueuer in EventBus._registry.get("card.cancelled", [])
    assert ConsolidationEnqueuer in EventBus._registry.get("card.restored", [])


@pytest.mark.asyncio
async def test_decay_applied(db_factory, clean_tables, decay_board, caplog):
    """Decay drops relevance_score by 0.5 and marks source_cancelled."""
    import logging

    caplog.set_level(logging.INFO, logger="okto_pulse.core.events.handlers.cancellation_decay")
    card_id = "card-decay-1"
    _seed_kuzu_node(decay_board, "Entity", "node-e1", card_id, 0.8)
    _seed_kuzu_node(decay_board, "Decision", "node-d1", card_id, 0.8)

    async with db_factory() as session:
        await publish(
            CardCancelled(
                board_id=decay_board,
                actor_id=USER_ID,
                card_id=card_id,
                previous_status="in_progress",
            ),
            session=session,
        )
        await session.commit()

    dispatcher = EventDispatcher(db_factory)
    try:
        await dispatcher.start()
        await asyncio.sleep(1.0)
    finally:
        await dispatcher.stop(timeout=2.0)

    entity = _fetch_node_fields(decay_board, "Entity", "node-e1")
    decision = _fetch_node_fields(decay_board, "Decision", "node-d1")
    assert abs(entity["relevance_score"] - 0.3) < 1e-6
    assert abs(decision["relevance_score"] - 0.3) < 1e-6
    assert entity["revocation_reason"] == REVOCATION_REASON
    assert decision["revocation_reason"] == REVOCATION_REASON
    assert entity["superseded_at"] is not None
    assert decision["superseded_at"] is not None

    # At least one log line carries the structured event marker.
    events = [r for r in caplog.records if getattr(r, "event", "") == "kg.cancellation_decay.applied"]
    assert events, "expected kg.cancellation_decay.applied log"
    # The last `applied` record covers both nodes.
    assert any(r.nodes_affected == 2 for r in events)


@pytest.mark.asyncio
async def test_decay_clamp_floor(db_factory, clean_tables, decay_board):
    """Score cannot go below 0 even if penalty exceeds current value."""
    card_id = "card-decay-floor"
    _seed_kuzu_node(decay_board, "Entity", "node-floor", card_id, 0.3)

    async with db_factory() as session:
        await publish(
            CardCancelled(
                board_id=decay_board,
                actor_id=USER_ID,
                card_id=card_id,
                previous_status="in_progress",
            ),
            session=session,
        )
        await session.commit()

    dispatcher = EventDispatcher(db_factory)
    try:
        await dispatcher.start()
        await asyncio.sleep(1.0)
    finally:
        await dispatcher.stop(timeout=2.0)

    node = _fetch_node_fields(decay_board, "Entity", "node-floor")
    assert node["relevance_score"] == 0.0
    assert node["revocation_reason"] == REVOCATION_REASON


@pytest.mark.asyncio
async def test_decay_idempotent(db_factory, clean_tables, decay_board):
    """Second CardCancelled for the same card does not re-apply penalty."""
    card_id = "card-decay-idem"
    _seed_kuzu_node(decay_board, "Entity", "node-idem", card_id, 0.9)

    async def _publish_and_drain():
        async with db_factory() as session:
            await publish(
                CardCancelled(
                    board_id=decay_board,
                    actor_id=USER_ID,
                    card_id=card_id,
                    previous_status="in_progress",
                ),
                session=session,
            )
            await session.commit()
        dispatcher = EventDispatcher(db_factory)
        try:
            await dispatcher.start()
            await asyncio.sleep(0.8)
        finally:
            await dispatcher.stop(timeout=2.0)

    await _publish_and_drain()
    first = _fetch_node_fields(decay_board, "Entity", "node-idem")
    assert abs(first["relevance_score"] - 0.4) < 1e-6  # 0.9 - 0.5

    # Wipe the previous exec row so clean_tables doesn't block the second run
    # from landing a fresh execution on the already-cancelled card.
    async with db_factory() as session:
        await session.execute(DomainEventHandlerExecution.__table__.delete())
        await session.execute(DomainEventRow.__table__.delete())
        await session.commit()

    await _publish_and_drain()
    second = _fetch_node_fields(decay_board, "Entity", "node-idem")
    # Score must be identical — filter kicked in on revocation_reason check.
    assert abs(second["relevance_score"] - first["relevance_score"]) < 1e-6


@pytest.mark.asyncio
async def test_decay_reverted(db_factory, clean_tables, decay_board, caplog):
    """CardRestored adds the penalty back and clears the markers."""
    import logging

    caplog.set_level(logging.INFO, logger="okto_pulse.core.events.handlers.cancellation_decay")
    card_id = "card-decay-revert"
    # Start in a decayed state to exercise only the restore leg.
    _seed_kuzu_node(
        decay_board, "Entity", "node-revert", card_id, 0.3,
        revocation_reason=REVOCATION_REASON,
    )

    async with db_factory() as session:
        await publish(
            CardRestored(
                board_id=decay_board,
                actor_id=USER_ID,
                card_id=card_id,
                from_status="cancelled",
                to_status="in_progress",
            ),
            session=session,
        )
        await session.commit()

    dispatcher = EventDispatcher(db_factory)
    try:
        await dispatcher.start()
        await asyncio.sleep(1.0)
    finally:
        await dispatcher.stop(timeout=2.0)

    node = _fetch_node_fields(decay_board, "Entity", "node-revert")
    assert abs(node["relevance_score"] - 0.8) < 1e-6  # 0.3 + 0.5
    assert node["revocation_reason"] is None
    assert node["superseded_at"] is None
    events = [r for r in caplog.records if getattr(r, "event", "") == "kg.cancellation_decay.reverted"]
    assert any(r.nodes_affected == 1 for r in events)


@pytest.mark.asyncio
async def test_restore_selective_by_reason(db_factory, clean_tables, decay_board):
    """Restore leaves nodes marked with other reasons untouched."""
    card_id = "card-selective"
    _seed_kuzu_node(
        decay_board, "Entity", "node-match", card_id, 0.3,
        revocation_reason=REVOCATION_REASON,
    )
    _seed_kuzu_node(
        decay_board, "Entity", "node-other", card_id, 0.3,
        revocation_reason="auto_superseded",
    )

    async with db_factory() as session:
        await publish(
            CardRestored(
                board_id=decay_board,
                actor_id=USER_ID,
                card_id=card_id,
                from_status="cancelled",
                to_status="in_progress",
            ),
            session=session,
        )
        await session.commit()

    dispatcher = EventDispatcher(db_factory)
    try:
        await dispatcher.start()
        await asyncio.sleep(1.0)
    finally:
        await dispatcher.stop(timeout=2.0)

    match = _fetch_node_fields(decay_board, "Entity", "node-match")
    other = _fetch_node_fields(decay_board, "Entity", "node-other")
    assert abs(match["relevance_score"] - 0.8) < 1e-6
    assert match["revocation_reason"] is None
    # Unmatched node stayed in its prior state.
    assert abs(other["relevance_score"] - 0.3) < 1e-6
    assert other["revocation_reason"] == "auto_superseded"


@pytest.mark.asyncio
async def test_decay_zero_nodes(db_factory, clean_tables, decay_board, caplog):
    """Cancelling a card with no derived nodes completes cleanly."""
    import logging

    caplog.set_level(logging.INFO, logger="okto_pulse.core.events.handlers.cancellation_decay")
    card_id = "card-no-nodes"

    async with db_factory() as session:
        await publish(
            CardCancelled(
                board_id=decay_board,
                actor_id=USER_ID,
                card_id=card_id,
                previous_status="in_progress",
            ),
            session=session,
        )
        await session.commit()

    dispatcher = EventDispatcher(db_factory)
    try:
        await dispatcher.start()
        await asyncio.sleep(0.8)
    finally:
        await dispatcher.stop(timeout=2.0)

    async with db_factory() as session:
        exec_rows = (await session.execute(
            select(DomainEventHandlerExecution).where(
                DomainEventHandlerExecution.handler_name == "CancellationDecayHandler",
            )
        )).scalars().all()
        assert len(exec_rows) == 1
        assert exec_rows[0].status == "done"

    events = [r for r in caplog.records if getattr(r, "event", "") == "kg.cancellation_decay.applied"]
    assert any(r.nodes_affected == 0 for r in events)


@pytest.mark.asyncio
async def test_handler_isolation_on_failure(db_factory, clean_tables, decay_board, monkeypatch):
    """Enqueuer still runs to completion when the decay handler raises."""
    async def _boom(self, event, session):  # noqa: ARG001
        raise RuntimeError("decay boom")

    monkeypatch.setattr(CancellationDecayHandler, "handle", _boom)

    card_id = "card-iso"
    async with db_factory() as session:
        await publish(
            CardCancelled(
                board_id=decay_board,
                actor_id=USER_ID,
                card_id=card_id,
                previous_status="in_progress",
            ),
            session=session,
        )
        await session.commit()

    dispatcher = EventDispatcher(db_factory)
    try:
        await dispatcher.start()
        await asyncio.sleep(1.0)
    finally:
        await dispatcher.stop(timeout=2.0)

    async with db_factory() as session:
        queue_rows = (await session.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.board_id == decay_board,
                ConsolidationQueue.artifact_id == card_id,
            )
        )).scalars().all()
        # Enqueuer committed its own transaction despite decay's failure.
        assert len(queue_rows) == 1
        assert queue_rows[0].priority == "high"

        exec_rows = (await session.execute(
            select(DomainEventHandlerExecution).where(
                DomainEventHandlerExecution.handler_name == "CancellationDecayHandler",
            )
        )).scalars().all()
        assert len(exec_rows) == 1
        # Either still retrying ('pending' with backoff) or already DLQ.
        assert exec_rows[0].status in ("pending", "dlq")
        assert exec_rows[0].last_error is not None
        assert "decay boom" in exec_rows[0].last_error
