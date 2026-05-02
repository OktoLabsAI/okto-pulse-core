"""InMemoryEventBus — satisfies EventBus Protocol for tests.

Fire-and-forget publish to in-memory list. Handlers called synchronously.
"""

from __future__ import annotations

import uuid

from okto_pulse.core.kg.interfaces.event_bus import EventHandler, KGEvent


class InMemoryEventBus:
    def __init__(self):
        self.events: list[tuple[str, KGEvent]] = []
        self._handlers: dict[str, list[EventHandler]] = {}
        self._running = False

    async def publish(self, event: KGEvent) -> str:
        event_id = f"evt_{uuid.uuid4().hex[:16]}"
        self.events.append((event_id, event))
        handlers = self._handlers.get(event.event_type, [])
        for handler in handlers:
            try:
                await handler(event)
            except Exception:
                pass
        return event_id

    async def subscribe(self, event_type: str, handler: EventHandler) -> None:
        self._handlers.setdefault(event_type, []).append(handler)

    async def start(self) -> None:
        self._running = True

    async def stop(self) -> None:
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    def clear(self) -> None:
        self.events.clear()
        self._handlers.clear()
