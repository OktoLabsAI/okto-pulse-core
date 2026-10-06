"""Current telemetry mode and state admission contract."""
from typing import Any, Literal

TelemetryMode = Literal["disabled", "anonymous_beacon"]
VALID_TELEMETRY_MODES = frozenset({"disabled", "anonymous_beacon"})


def validate_telemetry_state(state: Any) -> None:
    """Refuse incompatible state without converting or mutating it."""
    if not isinstance(state, dict):
        raise ValueError("incompatible_telemetry_state")
    if any(key in state for key in ("normalized_from", "migration_notices", "migration_notice", "requested_mode")):
        raise ValueError("incompatible_telemetry_state")
    mode = state.get("mode")
    if mode is not None and (not isinstance(mode, str) or mode not in VALID_TELEMETRY_MODES):
        raise ValueError("incompatible_telemetry_mode")
    history = state.get("history", [])
    if not isinstance(history, list):
        raise ValueError("incompatible_telemetry_history")
    for entry in history:
        validate_telemetry_state(entry)
