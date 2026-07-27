"""Pure registry for domain-event handler semantics."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from okto_pulse.core.runtime_context import runtime_state


_registry = runtime_state("events.registry", dict)
T = TypeVar("T", bound=type)


def register_handler(*event_types: str) -> Callable[[T], T]:
    """Register a handler class idempotently for each event type."""

    def decorator(handler_cls: T) -> T:
        for event_type in event_types:
            bucket = _registry.setdefault(event_type, [])
            if handler_cls not in bucket:
                bucket.append(handler_cls)
        return handler_cls

    return decorator


def registered_handlers(event_type: str) -> tuple[type, ...]:
    return tuple(_registry.get(event_type, ()))


def resolve_handler(handler_name: str, event_type: str) -> type:
    for handler in registered_handlers(event_type):
        if handler.__name__ == handler_name:
            return handler
    raise RuntimeError(
        f"Handler {handler_name} not registered for {event_type}"
    )


def clear_registry() -> None:
    """Reset registry state for isolated tests."""
    _registry.clear()


__all__ = [
    "clear_registry",
    "register_handler",
    "registered_handlers",
    "resolve_handler",
]
