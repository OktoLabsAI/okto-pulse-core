"""RuntimeEventBusPort runtime port (spec #15, tr_a1d9cc89 / api_7b9b0e15).

Canonical contract for runtime/observability events. It is NOT a third generic
``EventBus`` — collision_policy: this port MUST NOT shadow
``okto_pulse.core.events.bus.EventBus`` nor
``okto_pulse.core.kg.interfaces.event_bus.EventBus``; those existing buses may
only be wrapped as concrete adapters keeping their own names.

Pure: Protocol + frozen DTO. ``AsyncSession`` is referenced as a TYPE_CHECKING
type hint only (the contract mandates it) — no runtime persistence import.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Mapping, Protocol, runtime_checkable

if TYPE_CHECKING:  # pragma: no cover - type hints only, never imported at runtime
    from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True)
class RuntimeEvent:
    """A runtime/observability event (distinct from a domain event or KGEvent)."""

    name: str
    occurred_at: datetime
    source: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    severity: str | None = None


@runtime_checkable
class RuntimeEventBusPort(Protocol):
    """Publishes :class:`RuntimeEvent`s; adapts to existing domain/KG buses.

    Implementations may delegate to the existing ``core.events`` DomainEvent bus
    or the KG event bus via explicit adapters, but the port itself never
    redefines those contracts (collision_policy).
    """

    async def publish_runtime_event(
        self, event: RuntimeEvent, *, session: AsyncSession | None = None
    ) -> None:
        """Publish a runtime event, optionally within the caller's session."""
        ...

    async def flush(self) -> None:
        """Flush any buffered runtime events."""
        ...
