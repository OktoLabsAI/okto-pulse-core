"""Prospective architecture selection; existing Design snapshots own content.

Absence means legacy inheritance, never an inferred empty/exclusive selection.
Only an authorized creation/revision writes this scope. Readers do not repair it.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ArchitectureAdoptionScope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    contract_version: Literal["architecture-adoption/v1"] = "architecture-adoption/v1"
    board_id: str = Field(min_length=1, strict=True)
    spec_id: str = Field(min_length=1, strict=True)
    adopted_in_edition: int = Field(ge=1, strict=True)
    actor_id: str = Field(min_length=1, strict=True)
    inherited_resource_ids: tuple[str, ...]

    @field_validator("inherited_resource_ids")
    @classmethod
    def canonical_resources(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item.startswith("architecture:") or not item[13:].strip() for item in value):
            raise ValueError("architecture_adoption_invalid_resource")
        if len(set(value)) != len(value):
            raise ValueError("architecture_adoption_duplicate_resource")
        return tuple(sorted(value))

    def require_scope(self, *, board_id: str, spec_id: str, edition: int) -> None:
        if self.board_id != board_id or self.spec_id != spec_id or self.adopted_in_edition > edition:
            raise ValueError("architecture_adoption_scope_mismatch")
