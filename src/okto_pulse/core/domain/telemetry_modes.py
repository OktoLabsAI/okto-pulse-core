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
    validate_failure_state_carrier(state)
    validate_watermark_carrier(state)
    history = state.get("history", [])
    if not isinstance(history, list):
        raise ValueError("incompatible_telemetry_history")
    for entry in history:
        validate_telemetry_state(entry)


def _has_publish_history(state: dict[str, Any]) -> bool:
    sequence = state.get("next_batch_seq", 1)
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
        raise ValueError("incompatible_telemetry_sequence")
    return sequence > 1 or bool(state.get("last_send_at"))


def validate_failure_state_carrier(state: dict[str, Any]) -> None:
    """A previous publish outcome requires its native status block."""
    if "failure_state" in state and not isinstance(state["failure_state"], dict):
        raise ValueError("incompatible_telemetry_failure_state")
    if _has_publish_history(state) and "failure_state" not in state:
        raise ValueError("telemetry_failure_state_required")


def validate_watermark_carrier(state: dict[str, Any]) -> None:
    """An advanced sequence requires the explicitly recorded cursor pair."""
    if _has_publish_history(state) and not {"watermark", "watermark_event_id"} <= state.keys():
        raise ValueError("telemetry_watermark_required")
