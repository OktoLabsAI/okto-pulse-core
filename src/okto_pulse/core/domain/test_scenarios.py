"""Canonical test-scenario type vocabulary.

The write-facing API and MCP schemas import :data:`ScenarioType`, while
read-facing projections intentionally keep accepting arbitrary strings so
historical values remain inspectable.  Keeping both the static type and the
runtime tuple here prevents the transport schemas and service validation from
drifting.
"""

from __future__ import annotations

from typing import Literal, TypeAlias, get_args


ScenarioType: TypeAlias = Literal[
    "unit",
    "integration",
    "e2e",
    "manual",
    "negative",
]

VALID_SCENARIO_TYPES: tuple[str, ...] = get_args(ScenarioType)
DEFAULT_SCENARIO_TYPE: ScenarioType = "integration"
VerificationMethod: TypeAlias = Literal[
    "automated_test", "static_analysis", "inspection", "demonstration"
]
VALID_VERIFICATION_METHODS: tuple[str, ...] = get_args(VerificationMethod)
# Other authored methods remain pending until both their Core admission rules
# and an edition verifier exist. Merely declaring an enum cannot grant credit.
ADMITTED_VERIFICATION_METHODS = frozenset({"automated_test"})


def validate_verification_method(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or value not in VALID_VERIFICATION_METHODS:
        raise ValueError("verification_method_invalid")
    return value


SCENARIO_TYPE_DESCRIPTION = (
    "Scenario kind: unit, integration, e2e, manual, or negative. "
    "Use negative for invalid, forbidden, or denial paths that the product "
    "is expected to reject."
)


__all__ = [
    "DEFAULT_SCENARIO_TYPE",
    "SCENARIO_TYPE_DESCRIPTION",
    "ScenarioType",
    "VALID_SCENARIO_TYPES",
    "VerificationMethod",
    "VALID_VERIFICATION_METHODS",
    "ADMITTED_VERIFICATION_METHODS",
    "validate_verification_method",
]
