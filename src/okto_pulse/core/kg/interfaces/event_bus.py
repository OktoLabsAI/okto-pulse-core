"""EventBus Protocol — async fire-and-forget event publishing."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Protocol, runtime_checkable


@dataclass
class KGEvent:
    event_type: str
    board_id: str
    session_id: str
    payload: dict[str, Any] = field(default_factory=dict)


EventHandler = Callable[[KGEvent], Coroutine[Any, Any, None]]


@runtime_checkable
class EventBus(Protocol):
    async def publish(self, event: KGEvent) -> str:
        """Fire-and-forget publish. Returns event_id immediately."""
        ...

    async def subscribe(self, event_type: str, handler: EventHandler) -> None:
        """Register an async handler for an event type."""
        ...

    async def start(self) -> None:
        """Start background processing (poll loop, consumer, etc.)."""
        ...

    async def stop(self) -> None:
        """Graceful shutdown."""
        ...
