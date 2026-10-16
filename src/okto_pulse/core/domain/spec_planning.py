"""Typed freshness token for a Spec planning conclusion, never an approval."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field


class SpecPlanningManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    board_id: Annotated[str, Field(min_length=1, max_length=255)]
    spec_id: Annotated[str, Field(min_length=1, max_length=255)]
    edition: Annotated[int, Field(ge=1)]
    spec_version: Annotated[int, Field(ge=1)]
    subject_incarnation: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    subject_revision: Annotated[int, Field(ge=1)]
    authority_incarnation: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    authority_revision: Annotated[int, Field(ge=1)]
