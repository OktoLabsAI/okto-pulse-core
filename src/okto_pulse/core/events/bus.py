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

import logging

from okto_pulse.core.events.registry import (
    _registry,
    clear_registry,
    register_handler,
    resolve_handler,
)
from okto_pulse.core.events.types import DomainEvent
from okto_pulse.core.ports.domain_event_delivery import (
    get_domain_event_publisher,
)

logger = logging.getLogger(__name__)


def _signal_dispatcher() -> None:
    """Wake the registered dispatcher, if one is running on this loop.

    The dispatcher owns its own asyncio.Event (bound to its loop). We look
    it up lazily here so the publisher doesn't need a direct reference.
    Silent no-op when no dispatcher is registered or the loop differs
    (poll fallback still drains within POLL_INTERVAL_SECONDS).
    """
    from okto_pulse.core.application.runtime_workers import signal_runtime_worker

    signal_runtime_worker("event_dispatcher")


async def publish(event: DomainEvent, session: object) -> None:
    """Insert the event + pending handler executions in the caller's tx.

    Never commits. Signals the dispatcher wake_event on return so the
    next drain loop iteration picks up the new executions immediately.
    """
    await get_domain_event_publisher().publish(
        session,
        event=event,
        handler_names=tuple(
            handler.__name__ for handler in _registry.get(event.event_type, ())
        ),
    )

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
    def wake_event(cls):
        """Return the wake Event of the currently-running dispatcher, if any.

        Returns None outside of app lifespan (tests without a dispatcher).
        """
        from okto_pulse.core.application.runtime_workers import (
            current_runtime_worker_registry,
        )

        registry = current_runtime_worker_registry()
        handle = registry.get_handle("event_dispatcher") if registry else None
        return getattr(handle, "wake_event", None)


__all__ = [
    "EventBus",
    "publish",
    "register_handler",
    "resolve_handler",
    "clear_registry",
]
