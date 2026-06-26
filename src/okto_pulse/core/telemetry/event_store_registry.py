"""Telemetry EVENT-store factory registry (spec R10-B/R10-E fulfilled).

The composition root (the Community edition) registers a factory that builds a
:class:`~okto_pulse.core.ports.telemetry.TelemetryEventStore` for a given
``metrics_dir`` / ``retention_days``. The core telemetry runtime
(``TelemetryService``) obtains the store ONLY through
:func:`get_telemetry_event_store` — it never references the concrete
``LocalTelemetryStore`` (removed in R10-E Pass 2; Community owns it as
``community.adapters.telemetry_store.CommunityLocalTelemetryStore``).

R10-E Pass 2 fulfilled: the register-before-remove fallback that instantiated
``LocalTelemetryStore`` is REMOVED. The registry is now FAIL-CLOSED: if no
factory is registered the call raises ``RuntimeError`` with a structured signal —
a composed edition MUST register before any store access.

Deterministic + test-isolated: the factory is a single module global, reset via
:func:`reset_telemetry_event_store_factory_for_tests`.
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any, Callable

from okto_pulse.core.ports.telemetry import TelemetryEventStore

logger = logging.getLogger("okto_pulse.telemetry.event_store")

#: A factory: ``(metrics_dir: Path, retention_days: int) -> TelemetryEventStore``.
TelemetryEventStoreFactory = Callable[[Path, int], TelemetryEventStore]

_factory: TelemetryEventStoreFactory | None = None
_lock = threading.Lock()


def register_telemetry_event_store_factory(factory: TelemetryEventStoreFactory) -> None:
    """Register the edition's event-store factory (composition root). Idempotent
    overwrite; thread-safe."""
    global _factory
    with _lock:
        _factory = factory


def get_telemetry_event_store(
    metrics_dir: Any, retention_days: int = 30
) -> TelemetryEventStore:
    """Build the EVENT store via the registered factory (fail-closed: R10-E Pass 2).

    R10-E Pass 2: the register-before-remove fallback that built ``LocalTelemetryStore``
    is removed. The composition root MUST register a factory via
    :func:`register_telemetry_event_store_factory` before any store access.
    Calling without a registered factory raises ``RuntimeError`` and emits a
    structured signal (secret-free, bounded).
    """
    base = Path(metrics_dir)
    if _factory is not None:
        return _factory(base, retention_days)
    # R10-E Pass 2 fail-closed: no provider → structured error (never instantiates
    # a concrete class). The composition root (Community) registers before any use.
    logger.error(
        "telemetry event-store registry has no factory — composition root must "
        "register before use (R10-E Pass 2 fulfilled)",
        extra={
            "metric_name": "telemetry_event_store_no_provider_total",
            "component": "event_store_registry",
            "outcome": "fail_closed",
            "reason": "no_factory_registered",
        },
    )
    raise RuntimeError(
        "No TelemetryEventStore factory registered. "
        "Call register_telemetry_event_store_factory before any store access. "
        "(R10-E Pass 2: LocalTelemetryStore removed from core; Community owns the store.)"
    )


def reset_telemetry_event_store_factory_for_tests() -> None:
    """Drop the registered factory (tests only)."""
    global _factory
    with _lock:
        _factory = None


__all__ = [
    "TelemetryEventStoreFactory",
    "register_telemetry_event_store_factory",
    "get_telemetry_event_store",
    "reset_telemetry_event_store_factory_for_tests",
]
