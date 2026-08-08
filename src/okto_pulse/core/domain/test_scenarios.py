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
]
