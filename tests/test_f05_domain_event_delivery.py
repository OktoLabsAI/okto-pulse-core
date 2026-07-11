from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from okto_pulse.core.application.domain_event_delivery import (
    DomainEventDeliveryProcessor,
)
from okto_pulse.core.events.types import CardCreated
from okto_pulse.core.ports.domain_event_delivery import (
    DomainEventExecution,
    DomainEventFailure,
    StoredDomainEvent,
)


NOW = datetime(2026, 7, 11, tzinfo=timezone.utc)


class FakeStore:
    def __init__(self) -> None:
        self.execution = DomainEventExecution("exec-1", "event-1", "Handler", 1)
        self.event = StoredDomainEvent(
            event_id="event-1",
            event_type=CardCreated.event_type,
            board_id="board-1",
            actor_id="agent-1",
            actor_type="agent",
            occurred_at=NOW,
            payload={"card_id": "card-1", "spec_id": "spec-1"},
        )
        self.handler_error: Exception | None = None
        self.completed_at = None
        self.missing_at = None
        self.failure: DomainEventFailure | None = None
        self.invocations = 0

    async def recover_orphans(self) -> int:
        return 2

    async def claim_ready(self, *, limit: int, now: datetime):  # noqa: ANN201
        assert limit == 50
        assert now == NOW
        return [(self.execution.execution_id, self.execution.event_id)]

    async def begin_attempt(self, execution_id: str):  # noqa: ANN201
        return self.execution if execution_id == self.execution.execution_id else None

    async def load_event(self, event_id: str):  # noqa: ANN201
        if self.event is None:
            return None
        return self.event if event_id == self.event.event_id else None

    async def invoke_handler(
        self, execution_id, handler, event, *, processed_at
    ):  # noqa: ANN001
        del execution_id, handler
        assert event.card_id == "card-1"
        self.invocations += 1
        if self.handler_error is not None:
            raise self.handler_error
        self.completed_at = processed_at

    async def mark_event_missing(self, execution_id, *, processed_at):  # noqa: ANN001
        del execution_id
        self.missing_at = processed_at

    async def mark_failed(self, execution_id, failure):  # noqa: ANN001
        del execution_id
        self.failure = failure


def _processor(store: FakeStore) -> DomainEventDeliveryProcessor:
    return DomainEventDeliveryProcessor(
        store,
        handler_resolver=lambda _handler, _event: object,
        clock=lambda: NOW,
    )


@pytest.mark.asyncio
async def test_f05_event_delivery_success_and_recovery_use_only_the_port() -> None:
    store = FakeStore()
    processor = _processor(store)

    assert await processor.recover_orphans() == 2
    assert await processor.process_batch() == 1
    assert store.invocations == 1
    assert store.completed_at == NOW
    assert store.failure is None


@pytest.mark.asyncio
async def test_f05_event_delivery_retry_policy_is_core_owned() -> None:
    store = FakeStore()
    store.handler_error = RuntimeError("temporary")

    await _processor(store).process_batch()

    assert store.failure == DomainEventFailure(
        error="temporary",
        terminal=False,
        next_attempt_at=NOW.replace(second=2),
    )


@pytest.mark.asyncio
async def test_f05_event_delivery_dlq_threshold_is_core_owned() -> None:
    store = FakeStore()
    store.execution = replace(store.execution, attempts=5)
    store.handler_error = RuntimeError("permanent")

    await _processor(store).process_batch()

    assert store.failure == DomainEventFailure(
        error="permanent",
        terminal=True,
        processed_at=NOW,
    )


@pytest.mark.asyncio
async def test_f05_event_delivery_missing_event_is_terminal_without_handler() -> None:
    store = FakeStore()
    store.event = None

    await _processor(store).process_batch()

    assert store.missing_at == NOW
    assert store.invocations == 0
