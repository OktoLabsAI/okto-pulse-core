"""Ports for the edition-owned source of KG live-event data.

The Core owns the event protocol and the SSE coordination policy.  An edition
owns the concrete outbox, queue snapshot and connection-lifecycle work needed
to feed that protocol.  This keeps Local First SQL polling out of the Core and
allows a SaaS edition to provide the same stream from another event store.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable


class KGEventsProviderMissing(RuntimeError):
    """Raised when an edition has not composed a KG events reader."""

    code = "kg_events_provider_missing"

    def __init__(self) -> None:
        super().__init__(
            "kg_events_provider_missing: the edition composition root must "
            "register a KG events reader before serving the live-event stream"
        )


HISTORICAL_PROGRESS_SETTINGS_KEY = "kg_historical_consolidation"


@dataclass(frozen=True, slots=True)
class KGOutboxEvent:
    """A transport-neutral event emitted by the KG transactional outbox."""

    event_id: str
    session_id: str | None
    event_type: str
    created_at: datetime | None
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class KGEventsPoll:
    """One atomic read of newly emitted events and the queue progress."""

    events: Sequence[KGOutboxEvent]
    progress: Mapping[str, int]


@runtime_checkable
class KGEventsReaderPort(Protocol):
    """Edition-owned source for the KG live-event coordination service."""

    async def poll(
        self,
        *,
        board_id: str,
        after: datetime,
        limit: int,
    ) -> KGEventsPoll:
        """Return events after ``after`` plus the current queue progress."""

    async def replay(
        self,
        *,
        board_id: str,
        after: datetime,
        limit: int,
    ) -> Sequence[KGOutboxEvent]:
        """Return ordered events for a reconnecting client."""


_kg_events_reader_port: KGEventsReaderPort | None = None


def register_kg_events_reader_port(reader: KGEventsReaderPort) -> None:
    """Register the concrete event source selected by the edition."""

    global _kg_events_reader_port
    _kg_events_reader_port = reader


def get_kg_events_reader_port() -> KGEventsReaderPort:
    """Resolve the configured event source, failing closed when absent."""

    if _kg_events_reader_port is None:
        raise KGEventsProviderMissing()
    return _kg_events_reader_port


def reset_kg_events_reader_port_for_tests() -> None:
    """Clear explicit test composition."""

    global _kg_events_reader_port
    _kg_events_reader_port = None


__all__ = [
    "HISTORICAL_PROGRESS_SETTINGS_KEY",
    "KGEventsPoll",
    "KGEventsProviderMissing",
    "KGEventsReaderPort",
    "KGOutboxEvent",
    "get_kg_events_reader_port",
    "register_kg_events_reader_port",
    "reset_kg_events_reader_port_for_tests",
]
