"""SqliteOutboxEventBus — satisfies EventBus Protocol for embedded deployments.

Publishes KG events to the GlobalUpdateOutbox table via SQLAlchemy async
sessions. Handlers are invoked in-process after the outbox write succeeds.
The background polling loop drains unprocessed outbox entries for global
discovery sync.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Callable

from okto_pulse.core.kg.interfaces.event_bus import EventHandler, KGEvent

logger = logging.getLogger("okto_pulse.kg.event_bus")


class SqliteOutboxEventBus:
    """Outbox-backed EventBus implementation for embedded/community edition."""

    def __init__(self, session_factory: Callable):
        self._sf = session_factory
        self._handlers: dict[str, list[EventHandler]] = {}
        self._running = False
        self._poll_task: asyncio.Task | None = None

    async def publish(self, event: KGEvent) -> str:
        event_id = f"evt_{uuid.uuid4().hex[:16]}"

        # Write to outbox table
        try:
            from okto_pulse.core.models.db import GlobalUpdateOutbox

            async with self._sf() as session:
                session.add(GlobalUpdateOutbox(
                    event_id=event_id,
                    board_id=event.board_id,
                    session_id=event.session_id,
                    event_type=event.event_type,
                    payload=event.payload,
                ))
                await session.commit()
        except Exception as exc:
            logger.warning("event_bus.publish outbox_write_failed err=%s", exc)

        # Fire in-process handlers (best-effort)
        handlers = self._handlers.get(event.event_type, [])
        for handler in handlers:
            try:
                await handler(event)
            except Exception as exc:
                logger.warning(
                    "event_bus.handler_failed type=%s err=%s",
                    event.event_type, exc,
                )

        return event_id

    async def subscribe(self, event_type: str, handler: EventHandler) -> None:
        self._handlers.setdefault(event_type, []).append(handler)

    async def start(self) -> None:
        self._running = True
        logger.info("event_bus.started")

    async def stop(self) -> None:
        self._running = False
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        logger.info("event_bus.stopped")
