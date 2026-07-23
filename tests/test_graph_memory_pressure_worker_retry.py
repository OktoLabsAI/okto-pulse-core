"""Retry-budget policy for typed graph allocation pressure."""

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.application.processors import consolidation, global_outbox
from okto_pulse.core.application.processors.consolidation import (
    ConsolidationProcessor,
)
from okto_pulse.core.application.processors.global_outbox import (
    GlobalOutboxProcessor,
)
from okto_pulse.core.kg.interfaces.graph_errors import (
    GRAPH_MEMORY_PRESSURE_CODE,
    GraphUnavailable,
    graph_memory_pressure_retry_after_seconds,
)
from okto_pulse.core.ports.consolidation import ConsolidationQueueRecord
from okto_pulse.core.ports.global_outbox import GlobalOutboxEventRecord


class _GraphMemoryPressure(GraphUnavailable):
    code = GRAPH_MEMORY_PRESSURE_CODE
    retryable = True


class _Clock:
    def __init__(self, current: datetime) -> None:
        self.current = current

    def now(self) -> datetime:
        return self.current


class _ConsolidationStore:
    def __init__(self) -> None:
        self.saved: list[ConsolidationQueueRecord] = []

    async def save_queue_entries(self, _context, entries) -> None:
        self.saved.extend(entries)


class _ClaimRepository:
    def __init__(self, events: list[GlobalOutboxEventRecord]) -> None:
        self.events = events

    async def claim_global_outbox(self, _context, *, limit: int):
        return tuple(
            event
            for event in self.events
            if event.processed_at is None and 0 <= event.retry_count < 5
        )[:limit]


class _GlobalOutboxStore:
    def __init__(self, events: list[GlobalOutboxEventRecord]) -> None:
        self.events = events
        self.commit_calls = 0

    async def materialize_claimed(self, _context, claimed):
        return tuple(claimed)

    async def save_events(self, _context, events) -> None:
        self.events = list(events)

    async def commit(self, _context) -> None:
        self.commit_calls += 1


@asynccontextmanager
async def _relational_scope():
    yield object()


def _outbox_event(event_id: str, *, board_id: str) -> GlobalOutboxEventRecord:
    return GlobalOutboxEventRecord(
        id=event_id,
        event_id=event_id,
        board_id=board_id,
        session_id=None,
        payload={},
        retry_count=0,
        last_error=None,
        processed_at=None,
        created_at=datetime(2026, 7, 23, tzinfo=timezone.utc),
    )


def test_typed_memory_pressure_uses_bounded_retry_after_contract() -> None:
    wrapped = RuntimeError("primitive failed")
    wrapped.__cause__ = _GraphMemoryPressure(
        "cooling",
        details={"retry_after_ms": 5_001},
    )

    assert (
        graph_memory_pressure_retry_after_seconds(
            _GraphMemoryPressure(
                "cooling",
                details={"retry_after_ms": 12_001},
            )
        )
        == 13
    )
    assert (
        graph_memory_pressure_retry_after_seconds(
            _GraphMemoryPressure(
                "cooling",
                details={"retry_after_ms": 999_999},
            )
        )
        == 300
    )
    assert graph_memory_pressure_retry_after_seconds(wrapped) == 6
    assert (
        graph_memory_pressure_retry_after_seconds(
            "graph_memory_pressure: legacy persisted failure"
        )
        == 60
    )
    assert (
        graph_memory_pressure_retry_after_seconds(
            GraphUnavailable("ordinary graph outage")
        )
        is None
    )


@pytest.mark.asyncio
async def test_consolidation_memory_pressure_re_pends_without_spending_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)
    entry = ConsolidationQueueRecord(
        id="queue-1",
        board_id="board-1",
        artifact_type="spec",
        artifact_id="spec-1",
        status="claimed",
        attempts=4,
        last_error=None,
        next_retry_at=None,
        claimed_at=now,
        claim_timeout_at=now + timedelta(seconds=30),
        worker_id="worker-1",
        claimed_by_session_id="worker-1",
        triggered_at=now,
        priority="normal",
        claim_token=None,
    )
    store = _ConsolidationStore()
    canonical_debt_calls = 0
    dead_letter_calls = 0

    async def forbidden_canonical_debt(*_args, **_kwargs):
        nonlocal canonical_debt_calls
        canonical_debt_calls += 1
        raise AssertionError("transient pressure must not create canonical debt")

    async def forbidden_dead_letter(*_args, **_kwargs):
        nonlocal dead_letter_calls
        dead_letter_calls += 1
        raise AssertionError("transient pressure must not enter the DLQ")

    monkeypatch.setattr(
        consolidation,
        "get_consolidation_persistence_port",
        lambda: store,
    )
    monkeypatch.setattr(
        consolidation,
        "upsert_canonical_debt",
        forbidden_canonical_debt,
    )
    monkeypatch.setattr(
        consolidation,
        "route_to_dead_letter",
        forbidden_dead_letter,
    )
    processor = ConsolidationProcessor(
        relational_scope_factory=lambda: None,
        clock=_Clock(now),
    )

    await processor._mark_failed(
        object(),
        entry,
        error_text="graph_memory_pressure: allocator cooldown",
        max_attempts=5,
    )

    assert entry.attempts == 4
    assert entry.status == "pending"
    assert entry.next_retry_at == now + timedelta(seconds=60)
    assert entry.claimed_at is None
    assert entry.claim_timeout_at is None
    assert entry.worker_id is None
    assert entry.claimed_by_session_id is None
    assert entry.claim_token is None
    assert store.saved == [entry]
    assert canonical_debt_calls == 0
    assert dead_letter_calls == 0


def _outbox_processor(
    monkeypatch: pytest.MonkeyPatch,
    events: list[GlobalOutboxEventRecord],
) -> tuple[GlobalOutboxProcessor, _GlobalOutboxStore]:
    store = _GlobalOutboxStore(events)
    monkeypatch.setattr(global_outbox, "get_global_outbox_store", lambda: store)
    processor = GlobalOutboxProcessor(
        _relational_scope,
        claim_repository=_ClaimRepository(events),
    )

    async def direct_graph_io(operation):
        return operation()

    monkeypatch.setattr(processor, "_run_graph_io", direct_graph_io)
    return processor, store


@pytest.mark.asyncio
async def test_outbox_apply_pressure_stops_batch_without_spending_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _outbox_event("event-1", board_id="board-1")
    second = _outbox_event("event-2", board_id="board-2")
    processor, store = _outbox_processor(monkeypatch, [first, second])
    applied: list[str] = []

    async def fail_with_pressure(event, _context):
        applied.append(event.event_id)
        raise _GraphMemoryPressure(
            "allocator cooldown",
            details={"retry_after_ms": 60_000},
        )

    monkeypatch.setattr(processor, "_apply_event", fail_with_pressure)

    for _ in range(6):
        assert await processor._process_once_under_writer() == 0

    assert applied == ["event-1"] * 6
    assert first.retry_count == 0
    assert first.last_error == "graph_memory_pressure:allocator cooldown"
    assert second.retry_count == 0
    assert second.last_error is None
    assert store.commit_calls == 6


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_site", ["flush", "verification"])
async def test_outbox_post_write_pressure_does_not_spend_retry(
    monkeypatch: pytest.MonkeyPatch,
    failure_site: str,
) -> None:
    event = _outbox_event("event-1", board_id="board-1")
    processor, _store = _outbox_processor(monkeypatch, [event])
    pressure = _GraphMemoryPressure(
        "allocator cooldown",
        details={"retry_after_ms": 60_000},
    )

    async def apply_event(_event, _context):
        return {}

    monkeypatch.setattr(processor, "_apply_event", apply_event)
    if failure_site == "flush":
        monkeypatch.setattr(
            processor,
            "_verify_processed_batch",
            lambda _events: ({}, pressure),
        )
    else:
        monkeypatch.setattr(
            processor,
            "_verify_processed_batch",
            lambda _events: ({event.board_id: pressure}, None),
        )

    for _ in range(6):
        assert await processor._process_once_under_writer() == 0

    assert event.processed_at is None
    assert event.retry_count == 0
    assert GRAPH_MEMORY_PRESSURE_CODE in (event.last_error or "")


def test_outbox_verification_pressure_stops_before_unverified_boards_are_acked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _outbox_event("event-1", board_id="board-1")
    second = _outbox_event("event-2", board_id="board-2")
    processor, _store = _outbox_processor(monkeypatch, [first, second])
    pressure = _GraphMemoryPressure(
        "allocator cooldown",
        details={"retry_after_ms": 60_000},
    )
    inspected: list[str] = []

    class _Runtime:
        @contextmanager
        def post_write_verification_scope(self):
            yield

        def close(self) -> None:
            return None

    def assert_source_inventory(board_id, _expected):
        inspected.append(board_id)
        raise pressure

    monkeypatch.setattr(global_outbox, "_global_discovery_runtime", _Runtime)
    monkeypatch.setattr(
        processor,
        "_flush_global_discovery_storage_after_batch",
        lambda: None,
    )
    monkeypatch.setattr(
        processor,
        "_assert_source_inventory_unchanged",
        assert_source_inventory,
    )

    verification_errors, batch_error = processor._verify_processed_batch(
        [(first, {}), (second, {})]
    )

    assert verification_errors == {}
    assert batch_error is pressure
    assert inspected == ["board-1"]
