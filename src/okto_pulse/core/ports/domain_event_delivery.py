"""Persistence port for deterministic domain-event delivery."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from okto_pulse.core.events.types import DomainEvent


@dataclass(frozen=True, slots=True)
class StoredDomainEvent:
    event_id: str
    event_type: str
    board_id: str
    actor_id: str | None
    actor_type: str
    occurred_at: datetime
    payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class DomainEventExecution:
    execution_id: str
    event_id: str
    handler_name: str
    attempts: int


@dataclass(frozen=True, slots=True)
class DomainEventFailure:
    error: str
    terminal: bool
    processed_at: datetime | None = None
    next_attempt_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class CardBoostFacts:
    card_type: str | None
    priority: str | None
    severity: str | None


@dataclass(frozen=True, slots=True)
class CognitiveCardFacts:
    card_id: str
    spec_id: str | None
    card_type: str | None
    title: str | None
    action_plan: str | None
    content_hash: str | None = None


@dataclass(frozen=True, slots=True)
class CognitiveSpecFacts:
    spec_id: str
    context: str | None
    content_hash: str | None = None


class DomainEventDeliveryStore(Protocol):
    async def recover_orphans(self) -> int: ...

    async def claim_ready(
        self, *, limit: int, now: datetime
    ) -> Sequence[tuple[str, str]]: ...

    async def begin_attempt(
        self, execution_id: str
    ) -> DomainEventExecution | None: ...

    async def load_event(self, event_id: str) -> StoredDomainEvent | None: ...

    async def invoke_handler(
        self,
        execution_id: str,
        handler: type,
        event: DomainEvent,
        *,
        processed_at: datetime,
    ) -> None: ...

    async def mark_event_missing(
        self, execution_id: str, *, processed_at: datetime
    ) -> None: ...

    async def mark_failed(
        self, execution_id: str, failure: DomainEventFailure
    ) -> None: ...


class DomainEventPublisher(Protocol):
    async def publish(
        self,
        context: object,
        *,
        event: DomainEvent,
        handler_names: Sequence[str],
    ) -> None: ...


class DomainEventFactReader(Protocol):
    async def load_card_boost_facts(
        self, context: object, *, card_id: str
    ) -> CardBoostFacts | None: ...

    async def load_cognitive_card_facts(
        self, context: object, *, card_id: str
    ) -> CognitiveCardFacts | None: ...

    async def load_board_settings(
        self, context: object, *, board_id: str
    ) -> dict[str, object] | None: ...

    async def load_cognitive_spec_facts(
        self, context: object, *, spec_id: str
    ) -> CognitiveSpecFacts | None: ...


_PUBLISHER_KEY = "ports.domain_event_delivery.publisher"
_FACT_READER_KEY = "ports.domain_event_delivery.fact_reader"


def register_domain_event_publisher(publisher: DomainEventPublisher) -> None:
    register_runtime_value(_PUBLISHER_KEY, publisher)


def get_domain_event_publisher() -> DomainEventPublisher:
    return require_runtime_value(_PUBLISHER_KEY, "domain_event_publisher_not_configured")


def register_domain_event_fact_reader(reader: DomainEventFactReader) -> None:
    register_runtime_value(_FACT_READER_KEY, reader)


def get_domain_event_fact_reader() -> DomainEventFactReader:
    return require_runtime_value(_FACT_READER_KEY, "domain_event_fact_reader_not_configured")


def reset_domain_event_publisher_for_tests() -> None:
    reset_runtime_values(_PUBLISHER_KEY, _FACT_READER_KEY)


__all__ = [
    "CardBoostFacts",
    "CognitiveCardFacts",
    "CognitiveSpecFacts",
    "DomainEventDeliveryStore",
    "DomainEventExecution",
    "DomainEventFactReader",
    "DomainEventFailure",
    "DomainEventPublisher",
    "StoredDomainEvent",
    "get_domain_event_fact_reader",
    "get_domain_event_publisher",
    "register_domain_event_fact_reader",
    "register_domain_event_publisher",
    "reset_domain_event_publisher_for_tests",
]
