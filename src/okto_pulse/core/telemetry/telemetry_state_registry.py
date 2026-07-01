"""Telemetry STATE carrier registry (R12).

The composition root registers the edition-owned full-dict carrier for
``metrics_dir/state.json``. Core telemetry settings/service code uses this
registry only; it does not know the concrete file layout.
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any

from okto_pulse.core.ports.telemetry import TelemetryStateCarrier

logger = logging.getLogger("okto_pulse.telemetry.state_registry")

_carrier: TelemetryStateCarrier | None = None
_lock = threading.Lock()


def register_telemetry_state_carrier(carrier: TelemetryStateCarrier) -> None:
    """Register the edition's full-dict telemetry state carrier."""
    global _carrier
    with _lock:
        _carrier = carrier


def load_telemetry_state(metrics_dir: Any) -> dict[str, Any]:
    """Load the full telemetry state dict via the registered carrier."""
    if _carrier is not None:
        state = _carrier.load_state(Path(metrics_dir))
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


def save_telemetry_state(metrics_dir: Any, state: dict[str, Any]) -> None:
    """Persist the full telemetry state dict via the registered carrier."""
    if _carrier is not None:
        _carrier.save_state(Path(metrics_dir), dict(state))
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
    global _carrier
    with _lock:
        _carrier = None


__all__ = [
    "register_telemetry_state_carrier",
    "load_telemetry_state",
    "save_telemetry_state",
    "reset_telemetry_state_carrier_for_tests",
]
