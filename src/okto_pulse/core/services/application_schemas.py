"""Application-facing schema facade.

The application use cases still depend on legacy Pydantic request/update DTOs.
Keep the concrete ``core.models.schemas`` dependency behind this transitional
service facade so the pure application layer does not import outbound models
directly while the DTO migration is completed.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


from okto_pulse.core.models.schemas import (
    ApiContract,
    ArchitectureDesignUpdate,
    ArchitectureDiagramPayloadResponse,
    CardUpdate,
    GuidelineCreate,
    SpecUpdate,
    TestScenarioWrite,
)


class ScenarioContentUpdate(BaseModel):
    """Internal content edit with the same closed scenario payload contract.

    The service uses this for evidence invalidation after a semantic edit.
    Public SpecUpdate cannot act as an alternate operational-status writer.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)
    test_scenarios: list[TestScenarioWrite]


__all__ = [
    "ApiContract",
    "ArchitectureDesignUpdate",
    "ArchitectureDiagramPayloadResponse",
    "CardUpdate",
    "GuidelineCreate",
    "SpecUpdate",
    "ScenarioContentUpdate",
]
