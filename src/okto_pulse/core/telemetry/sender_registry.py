"""Telemetry SENDER factory registry (spec R10-C/R10-E fulfilled).

The composition root (the Community edition) registers a factory that builds a
:class:`~okto_pulse.core.ports.telemetry.TelemetrySink` for a given ``settings``.
The metrics beacon lifecycle (``community.main._metrics_beacon_loop``) obtains
the sender ONLY through :func:`get_telemetry_sender` — it never references the
concrete ``TelemetryBeaconSender`` (removed in R10-E Pass 2; Community owns it as
``community.adapters.telemetry_sender.CommunityTelemetryBeaconSender``).

R10-E Pass 2 fulfilled: the register-before-remove fallback that instantiated
``TelemetryBeaconSender`` is REMOVED (``_CoreTelemetrySenderShim`` also removed).
The registry is now FAIL-CLOSED: no factory registered → structured error, no
concrete instantiation. The Community edition registers its factory before the
beacon loop starts so the composed runtime never reaches this guard.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable

from okto_pulse.core.ports.telemetry import TelemetrySink

logger = logging.getLogger("okto_pulse.telemetry.sender_registry")

#: A factory: ``(settings) -> TelemetrySink``.
TelemetrySenderFactory = Callable[[Any], TelemetrySink]

_telemetry_sender_factory: TelemetrySenderFactory | None = None
_lock = threading.Lock()


def register_telemetry_sender_factory(factory: TelemetrySenderFactory) -> None:
    """Register the edition's telemetry-sender factory (composition root).
    Idempotent overwrite; thread-safe."""
    global _telemetry_sender_factory
    with _lock:
        _telemetry_sender_factory = factory


def get_telemetry_sender(settings: Any) -> TelemetrySink:
    """Build the telemetry sender via the registered factory (fail-closed: R10-E Pass 2).

    R10-E Pass 2: the register-before-remove fallback that built
    ``TelemetryBeaconSender`` is removed. The composition root MUST register a
    factory via :func:`register_telemetry_sender_factory` before any send call.
    Calling without a registered factory raises ``RuntimeError`` and emits a
    structured signal (secret-free, bounded).
    """
    if _telemetry_sender_factory is not None:
        return _telemetry_sender_factory(settings)
    # R10-E Pass 2 fail-closed: no provider → structured error. Community registers
    # its factory (register_community_telemetry_sender) before the beacon loop so
    # the composed runtime never reaches this guard.
    logger.error(
        "telemetry sender registry has no factory — composition root must "
        "register before use (R10-E Pass 2 fulfilled)",
        extra={
            "metric_name": "telemetry_sender_no_provider_total",
            "component": "sender_registry",
            "outcome": "fail_closed",
            "reason": "no_factory_registered",
        },
    )
    raise RuntimeError(
        "No TelemetrySink factory registered. "
        "Call register_telemetry_sender_factory before any send. "
        "(R10-E Pass 2: TelemetryBeaconSender removed from core; Community owns the sender.)"
    )


def reset_telemetry_sender_factory_for_tests() -> None:
    """Drop the registered factory (tests only)."""
    global _telemetry_sender_factory
    with _lock:
        _telemetry_sender_factory = None


__all__ = [
    "TelemetrySenderFactory",
    "register_telemetry_sender_factory",
    "get_telemetry_sender",
    "reset_telemetry_sender_factory_for_tests",
]
