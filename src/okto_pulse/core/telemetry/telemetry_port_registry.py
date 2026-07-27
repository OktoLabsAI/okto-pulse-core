"""TelemetryPort factory registry (spec R10-E, IMP01/IMP03 — FULFILLED Pass 2).

The composition root (the Community edition) registers a factory that builds a
:class:`~okto_pulse.core.ports.telemetry.TelemetryPort` for a given ``settings``.
The request/emitter surfaces obtain the telemetry facade through
:func:`get_telemetry_port` (call-sites migrated in R10-E Pass 2 — all direct
``TelemetryService(settings)`` constructions are removed).

R10-E Pass 2 fulfilled: the Stage-A additive fallback that constructed
``TelemetryService`` directly is REMOVED. The registry is now FAIL-CLOSED:
no factory registered → structured error, no direct facade construction. The
Community composition root registers before any use.

Deterministic + test-isolated: the factory is a single module global, reset via
:func:`reset_telemetry_port_factory_for_tests`.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from okto_pulse.core.ports.telemetry import TelemetryPort
from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value

logger = logging.getLogger("okto_pulse.telemetry.port_registry")

#: A factory: ``(settings) -> TelemetryPort``.
TelemetryPortFactory = Callable[[Any], TelemetryPort]

_RUNTIME_KEY = "telemetry.port.factory"


def register_telemetry_port_factory(factory: TelemetryPortFactory) -> None:
    """Register the edition's telemetry-port factory (composition root).
    Idempotent overwrite; thread-safe."""
    register_runtime_value(_RUNTIME_KEY, factory)


def get_telemetry_port(settings: Any) -> TelemetryPort:
    """Resolve the telemetry facade via the registered factory (fail-closed: R10-E Pass 2).

    R10-E Pass 2: the Stage-A additive fallback that built ``TelemetryService``
    directly is removed. The composition root MUST register a factory via
    :func:`register_telemetry_port_factory` before any port use. Calling without
    a registered factory raises ``RuntimeError`` and emits a structured signal
    (secret-free, bounded).
    """
    factory = resolve_runtime_value(_RUNTIME_KEY)
    if factory is not None:
        return factory(settings)
    # R10-E Pass 2 fail-closed: no provider → structured error. Community registers
    # its factory (register_community_telemetry_port) at the composition root.
    logger.error(
        "telemetry port registry has no factory — composition root must "
        "register before use (R10-E Pass 2 fulfilled)",
        extra={
            "metric_name": "telemetry_port_no_provider_total",
            "component": "telemetry_port_registry",
            "outcome": "fail_closed",
            "reason": "no_factory_registered",
        },
    )
    raise RuntimeError(
        "No TelemetryPort factory registered. "
        "Call register_telemetry_port_factory before any port use. "
        "(R10-E Pass 2: direct TelemetryService construction removed from call-sites; "
        "Community registers the facade.)"
    )


def reset_telemetry_port_factory_for_tests() -> None:
    """Drop the registered factory (tests only)."""
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "TelemetryPortFactory",
    "register_telemetry_port_factory",
    "get_telemetry_port",
    "reset_telemetry_port_factory_for_tests",
]
