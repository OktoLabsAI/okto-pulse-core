"""Declared work is durable context, never authenticated delivery evidence."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from okto_pulse.core.models.schemas import ImpactEvidence

ProgressIdentity = Annotated[
    str,
    Field(strict=True, min_length=1, max_length=255, pattern=r"^[A-Za-z0-9_:.@-]+$"),
]


class DeliveryProgressSource(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    source_ref: ProgressIdentity | None = None
    declared_revision: (
        Annotated[str, Field(min_length=1, max_length=512, pattern=r"\S")] | None
    ) = None
    workspace_state: Literal["unknown", "dirty", "clean"]
    recoverability: Literal["unknown", "external_workspace", "declared_commit"]

    @model_validator(mode="after")
    def bounded_claim(self):
        if self.declared_revision is not None and self.source_ref is None:
            raise ValueError("delivery_progress_source_required")
        if self.recoverability == "declared_commit" and (
            not self.declared_revision or self.workspace_state != "clean"
        ):
            raise ValueError("delivery_progress_commit_claim_inconsistent")
        return self


class DeliveryProgress(BaseModel):
    """One checkpoint, reusing the existing summary and impact vocabulary.

    No field declares completion, approval, receipt validity or recovery by a
    different actor. An external workspace may be inaccessible on resumption.
    """

    model_config = ConfigDict(extra="forbid", strict=True)
    contract_version: Literal["delivery-progress/v1"] = "delivery-progress/v1"
    source_state: DeliveryProgressSource
    target_ids: list[ProgressIdentity] = Field(default_factory=list, max_length=100)
    impact_delta: ImpactEvidence | None = None
    remaining: str = Field(min_length=1, max_length=8000, pattern=r"\S")

    @model_validator(mode="after")
    def unique_targets(self):
        if len(set(self.target_ids)) != len(self.target_ids):
            raise ValueError("delivery_progress_target_duplicate")
        return self


def require_delivery_progress_mutable(card: object) -> None:
    status = getattr(card, "status", None)
    card_type = getattr(card, "card_type", None)
    if (
        getattr(card, "archived", None) is not False
        or getattr(status, "value", status) not in {"started", "in_progress"}
        or getattr(card_type, "value", card_type) not in {"normal", "bug", "test"}
    ):
        raise ValueError("delivery_progress_execution_state_required")
