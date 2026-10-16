"""Bounded scenario-type error envelopes shared by inbound transports."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.domain.test_scenarios import VALID_SCENARIO_TYPES

def invalid_scenario_type_envelope(value: Any) -> dict[str, Any]:
    """Return the frozen API17 scenario-type rejection shape."""

    allowed = list(VALID_SCENARIO_TYPES)
    return {
        "error": "invalid_scenario_type",
        "value": value,
        "allowed": allowed,
        "message": (
            f"Invalid scenario_type {value!r}. "
            f"Allowed values: {', '.join(allowed)}."
        ),
        "mutated": False,
    }


def canonical_scenario_type_error(
    errors: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Map only a Literal failure on ``scenario_type`` to API17."""

    for error in errors:
        location = error.get("loc") or ()
        if (
            location
            and str(location[-1]) == "scenario_type"
            and error.get("type") == "literal_error"
        ):
            return invalid_scenario_type_envelope(error.get("input"))
    return None


__all__ = [
    "canonical_scenario_type_error",
    "invalid_scenario_type_envelope",
]
