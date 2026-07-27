"""Canonical configuration-presence projection.

Presence is structural, not truthy: a missing value, an explicit ``null``, an
empty container, and a configured value are different states.  Callers supply
whether a usable baseline exists because an empty *template* is a valid
baseline while an empty board snapshot has no template to compare against.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


ConfigurationPresenceState = Literal["absent", "null", "empty", "configured"]
CONFIGURATION_ABSENT = object()


@dataclass(frozen=True, slots=True)
class ConfigurationPresenceProjection:
    state: ConfigurationPresenceState
    baseline_available: bool
    comparable: bool

    def to_dict(self) -> dict[str, bool | str]:
        return {
            "configuration_presence": self.state,
            "baseline_available": self.baseline_available,
            "comparable": self.comparable,
        }


def classify_configuration_presence(
    value: Any = CONFIGURATION_ABSENT,
) -> ConfigurationPresenceState:
    if value is CONFIGURATION_ABSENT:
        return "absent"
    if value is None:
        return "null"
    if isinstance(value, (dict, list, tuple, set, str, bytes)) and not value:
        return "empty"
    return "configured"


def project_configuration_presence(
    value: Any = CONFIGURATION_ABSENT,
    *,
    baseline_available: bool | None = None,
    comparable: bool | None = None,
) -> ConfigurationPresenceProjection:
    state = classify_configuration_presence(value)
    has_baseline = (
        state in {"empty", "configured"}
        if baseline_available is None
        else bool(baseline_available)
    )
    return ConfigurationPresenceProjection(
        state=state,
        baseline_available=has_baseline,
        comparable=has_baseline if comparable is None else bool(comparable),
    )


__all__ = [
    "CONFIGURATION_ABSENT",
    "ConfigurationPresenceProjection",
    "ConfigurationPresenceState",
    "classify_configuration_presence",
    "project_configuration_presence",
]
