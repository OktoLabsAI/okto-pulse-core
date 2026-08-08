"""Application-facing schema facade.

The application use cases still depend on legacy Pydantic request/update DTOs.
Keep the concrete ``core.models.schemas`` dependency behind this transitional
service facade so the pure application layer does not import outbound models
directly while the DTO migration is completed.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Iterable

from okto_pulse.core.models.schemas import (
    ApiContract,
    ArchitectureDesignUpdate,
    ArchitectureDiagramPayloadResponse,
    CardUpdate,
    GuidelineCreate,
    SpecUpdate,
)


@dataclass(frozen=True, slots=True)
class PersistedTestScenarioSpecUpdate:
    """Internal read-modify-write carrier for persisted scenario projections.

    Public REST/MCP writes continue to use ``SpecUpdate`` and its closed
    scenario-type enum.  Internal workflows that necessarily carry the current
    raw list use this narrow carrier; ``SpecService.update_spec`` still applies
    the delta-aware scenario validator before mutation, so only an unchanged
    legacy type is grandfathered.
    """

    test_scenarios: tuple[Any, ...]

    @classmethod
    def from_iterable(
        cls,
        scenarios: Iterable[Any],
    ) -> "PersistedTestScenarioSpecUpdate":
        return cls(tuple(deepcopy(list(scenarios))))

    def model_dump(self, *, exclude_unset: bool = False) -> dict[str, Any]:
        del exclude_unset
        return {"test_scenarios": deepcopy(list(self.test_scenarios))}


__all__ = [
    "ApiContract",
    "ArchitectureDesignUpdate",
    "ArchitectureDiagramPayloadResponse",
    "CardUpdate",
    "GuidelineCreate",
    "PersistedTestScenarioSpecUpdate",
    "SpecUpdate",
]
