"""Ports for the edition-owned source of KG live-event data.

The Core owns the event protocol and the SSE coordination policy.  An edition
owns the concrete outbox, queue snapshot and connection-lifecycle work needed
to feed that protocol.  This keeps Local First SQL polling out of the Core and
allows a SaaS edition to provide the same stream from another event store.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value

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
        after_event_id: str | None = None,
        include_code_traceability: bool = True,
    ) -> KGEventsPoll:
        """Return events after the stable composite cursor.

        Providers must order events by ``(created_at, event_id)``.  When
        ``after_event_id`` is present, include rows whose timestamp is newer
        than ``after`` or whose timestamp is equal and event id is
        lexicographically greater.  ``None`` preserves the legacy strict
        timestamp boundary (``created_at > after``).
        """

    async def replay(
        self,
        *,
        board_id: str,
        after: datetime,
        limit: int,
        after_event_id: str | None = None,
        include_code_traceability: bool = True,
    ) -> Sequence[KGOutboxEvent]:
        """Return deterministically ordered events after the composite cursor."""


_RUNTIME_KEY = "ports.kg_events.reader"


def register_kg_events_reader_port(reader: KGEventsReaderPort) -> None:
    """Register the concrete event source selected by the edition."""

    register_runtime_value(_RUNTIME_KEY, reader)


def get_kg_events_reader_port() -> KGEventsReaderPort:
    """Resolve the configured event source, failing closed when absent."""

    reader = resolve_runtime_value(_RUNTIME_KEY)
    if reader is None:
        raise KGEventsProviderMissing()
    return reader


def reset_kg_events_reader_port_for_tests() -> None:
    """Clear explicit test composition."""

    reset_runtime_values(_RUNTIME_KEY)


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
