"""Telemetry STATE carrier registry (R12).

The composition root registers the edition-owned full-dict carrier. Core uses
an opaque state reference and never knows the concrete persistence layout.
"""

from __future__ import annotations

import logging
from typing import Any

from okto_pulse.core.ports.telemetry import TelemetryStateCarrier
from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value

logger = logging.getLogger("okto_pulse.telemetry.state_registry")

_RUNTIME_KEY = "telemetry.state.carrier"


def register_telemetry_state_carrier(carrier: TelemetryStateCarrier) -> None:
    """Register the edition's full-dict telemetry state carrier."""
    register_runtime_value(_RUNTIME_KEY, carrier)


def load_telemetry_state(state_ref: str) -> dict[str, Any]:
    """Load the full telemetry state dict via the registered carrier."""
    carrier = resolve_runtime_value(_RUNTIME_KEY)
    if carrier is not None:
        state = carrier.load_state(str(state_ref))
        return dict(state) if isinstance(state, dict) else {}
    logger.error(
        "telemetry state registry has no carrier - composition root must register before use",
        extra={
            "metric_name": "telemetry_state_carrier_no_provider_total",
            "component": "telemetry_state_registry",
            "outcome": "fail_closed",
            "reason": "no_carrier_registered",
        },
    )
    raise RuntimeError(
        "No TelemetryStateCarrier registered. "
        "Call register_telemetry_state_carrier before telemetry state access. "
        "(R12: Community owns the full state.json carrier.)"
    )


def save_telemetry_state(state_ref: str, state: dict[str, Any]) -> None:
    """Persist the full telemetry state dict via the registered carrier."""
    carrier = resolve_runtime_value(_RUNTIME_KEY)
    if carrier is not None:
        carrier.save_state(str(state_ref), dict(state))
        return
    logger.error(
        "telemetry state registry has no carrier - composition root must register before use",
        extra={
            "metric_name": "telemetry_state_carrier_no_provider_total",
            "component": "telemetry_state_registry",
            "outcome": "fail_closed",
            "reason": "no_carrier_registered",
        },
    )
    raise RuntimeError(
        "No TelemetryStateCarrier registered. "
        "Call register_telemetry_state_carrier before telemetry state access. "
        "(R12: Community owns the full state.json carrier.)"
    )


def reset_telemetry_state_carrier_for_tests() -> None:
    """Drop the registered carrier (tests only)."""
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "register_telemetry_state_carrier",
    "load_telemetry_state",
    "save_telemetry_state",
    "reset_telemetry_state_carrier_for_tests",
]
