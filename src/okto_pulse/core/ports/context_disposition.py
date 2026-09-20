"""Explicit migration input, not executor authority or a new approval workflow.

The privileged migration operator supplies the decision reference and rationale.
These are recorded as submitted evidence, never authenticated review claims.
Binding historical context neither copies it into a live approval nor grants
permission to read it. Readers must still authorize the original section.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ContextTarget(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    kind: Literal["spec", "card"]
    identity: str = Field(min_length=1, max_length=128)

    @model_validator(mode="after")
    def valid_identity(self):
        if self.identity.strip() != self.identity or not self.identity.strip():
            raise ValueError("context_target_identity_invalid")
        return self


class ContextDisposition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    candidate_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    action: Literal["bind_context", "retain_history"]
    rationale: str = Field(min_length=1, max_length=4000)
    targets: tuple[ContextTarget, ...] = Field(default=(), max_length=100)

    @model_validator(mode="after")
    def valid_disposition(self):
        if (not self.rationale.strip() or (self.action == "bind_context") != bool(self.targets)
                or len(set(self.targets)) != len(self.targets)):
            raise ValueError("context_disposition_invalid")
        return self


class ContextDispositionPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    format: Literal["historical-context-disposition/v1"] = "historical-context-disposition/v1"
    migration_id: str = Field(min_length=1, max_length=128)
    decision_reference: str = Field(min_length=1, max_length=512)
    decisions: tuple[ContextDisposition, ...] = Field(max_length=100_000)

    @model_validator(mode="after")
    def complete_unique_input(self):
        if (not self.migration_id.strip() or not self.decision_reference.strip()
                or len({decision.candidate_sha256 for decision in self.decisions}) != len(self.decisions)):
            raise ValueError("context_disposition_plan_invalid")
        return self


def require_context_target_scope(*, origin_board: str, target_board: str) -> None:
    if not origin_board or origin_board != target_board:
        raise ValueError("context_target_scope_mismatch")


__all__ = ["ContextTarget", "ContextDisposition", "ContextDispositionPlan", "require_context_target_scope"]
