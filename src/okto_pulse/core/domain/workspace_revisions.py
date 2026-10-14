"""Bounded change tokens for visible workspaces; never evidence of approval."""
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _ClosedModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class WorkspaceSubjectRef(_ClosedModel):
    subject_type: Literal["spec", "card"]
    subject_id: str = Field(min_length=1, max_length=255, pattern=r"\S")


class WorkspaceRevisionToken(_ClosedModel):
    incarnation: str = Field(pattern=r"^[0-9a-f]{32}$")
    revision: int = Field(ge=1, strict=True)


class WorkspaceRevision(WorkspaceSubjectRef):
    # None means absent from the authorized Board. It never means unchanged.
    token: WorkspaceRevisionToken | None


class WorkspaceRevisionsQuery(_ClosedModel):
    board_id: str = Field(min_length=1, max_length=255, pattern=r"\S")
    subjects: tuple[WorkspaceSubjectRef, ...] = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def unique_subjects(self) -> Self:
        identities = [(item.subject_type, item.subject_id) for item in self.subjects]
        if len(set(identities)) != len(identities):
            raise ValueError("workspace_revision_subject_duplicate")
        return self


class WorkspaceRevisions(_ClosedModel):
    board_id: str = Field(min_length=1, max_length=255)
    authority: WorkspaceRevisionToken
    subjects: tuple[WorkspaceRevision, ...] = Field(min_length=1, max_length=50)
