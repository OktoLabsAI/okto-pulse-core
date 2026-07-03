"""Application-facing schema facade.

The application use cases still depend on legacy Pydantic request/update DTOs.
Keep the concrete ``core.models.schemas`` dependency behind this transitional
service facade so the pure application layer does not import outbound models
directly while the DTO migration is completed.
"""

from __future__ import annotations

from okto_pulse.core.models.schemas import (
    ApiContract,
    ArchitectureDesignUpdate,
    ArchitectureDiagramPayloadResponse,
    CardUpdate,
    GuidelineCreate,
    SpecUpdate,
)

__all__ = [
    "ApiContract",
    "ArchitectureDesignUpdate",
    "ArchitectureDiagramPayloadResponse",
    "CardUpdate",
    "GuidelineCreate",
    "SpecUpdate",
]
