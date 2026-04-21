"""EventBus — atomic publisher for DomainEvents (outbox pattern).

Publishers (services/main.py) call `await publish(event, session=self.db)`
within their existing transaction. The call inserts the event row plus one
row per subscribed handler in domain_event_handler_executions, then signals
the dispatcher worker via a module-level asyncio.Event.

Key invariants:
- publish() NEVER calls session.commit() or session.rollback(). Atomicity
  is delegated to the caller's transaction lifecycle.
- The handler registry is static — populated once at import time via
  the @register_handler decorator. Registration is idempotent.
- _wake_event is module-level so the dispatcher (in a separate task) can
  wait on it without explicit plumbing.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from typing import TypeVar

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.events.types import DomainEvent
from okto_pulse.core.models.db import DomainEventHandlerExecution, DomainEventRow

logger = logging.getLogger(__name__)


# Module-level registry: event_type → list of handler classes.
_registry: dict[str, list[type]] = {}


def _signal_dispatcher() -> None:
    """Wake the registered dispatcher, if one is running on this loop.

    The dispatcher owns its own asyncio.Event (bound to its loop). We look
    it up lazily here so the publisher doesn't need a direct reference.
    Silent no-op when no dispatcher is registered or the loop differs
    (poll fallback still drains within POLL_INTERVAL_SECONDS).
    """
    try:
        # Late import to avoid the bus→dispatcher cycle at module load.
        from okto_pulse.core.events.dispatcher import get_dispatcher

        dispatcher = get_dispatcher()
        if dispatcher is not None:
            dispatcher.notify()
    except Exception:
        pass


T = TypeVar("T", bound=type)


def register_handler(*event_types: str) -> Callable[[T], T]:
    """Decorator: register the class as a handler for the given event_types.

    Idempotent — registering the same handler twice for the same event_type
    is a silent no-op (prevents double-register when modules are imported
    more than once in tests).
    """

    def decorator(handler_cls: T) -> T:
        for et in event_types:
            bucket = _registry.setdefault(et, [])
            if handler_cls not in bucket:
                bucket.append(handler_cls)
        return handler_cls

    return decorator


async def publish(event: DomainEvent, session: AsyncSession) -> None:
    """Insert the event + pending handler executions in the caller's tx.

    Never commits. Signals the dispatcher wake_event on return so the
    next drain loop iteration picks up the new executions immediately.
    """
    row = DomainEventRow(
        id=event.event_id,
        event_type=event.event_type,
        board_id=event.board_id,
        actor_id=event.actor_id,
        actor_type=event.actor_type,
        payload_json=event.payload_for_storage(),
        occurred_at=event.occurred_at,
    )
    session.add(row)
    # Flush the event row first so handler-execution INSERTs (which FK to
    # domain_events.id) see the parent row even when the FK relationship
    # isn't materialized as a SQLAlchemy relationship() and the unit-of-work
    # topological sort doesn't reorder unrelated pending objects. Without
    # this, SQLite raises FOREIGN KEY constraint failed at flush time.
    await session.flush()

    handlers = _registry.get(event.event_type, [])
    for handler_cls in handlers:
        session.add(
            DomainEventHandlerExecution(
                event_id=event.event_id,
                handler_name=handler_cls.__name__,
                status="pending",
                attempts=0,
            )
        )

    # Flush so the rows are visible to anything that queries in the same
    # tx. Caller still owns commit/rollback.
    await session.flush()

    _signal_dispatcher()


class EventBus:
    """Namespace facade for the module-level functions.

    Kept so callers can `from okto_pulse.core.events import EventBus` and
    access everything through a single object. The underlying state
    remains module-level so tests can inspect _registry directly.
    """

    register_handler = staticmethod(register_handler)
    publish = staticmethod(publish)
    _registry = _registry

    @classmethod
    def wake_event(cls) -> asyncio.Event | None:
        """Return the wake Event of the currently-running dispatcher, if any.

        Returns None outside of app lifespan (tests without a dispatcher).
        """
        from okto_pulse.core.events.dispatcher import get_dispatcher

        dispatcher = get_dispatcher()
        return dispatcher._wake_event if dispatcher is not None else None


def clear_registry() -> None:
    """Test-only helper to reset the registry between runs."""
    _registry.clear()


__all__ = [
    "EventBus",
    "publish",
    "register_handler",
    "clear_registry",
]
