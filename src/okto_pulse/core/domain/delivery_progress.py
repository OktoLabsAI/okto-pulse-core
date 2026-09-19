"""Declared work is durable context, never authenticated delivery evidence."""

from datetime import datetime, timezone
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_serializer, model_validator

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
    contract_version: Literal["delivery-progress/v1", "delivery-progress/v2"] = "delivery-progress/v1"
    material_change: Literal["none", "targets", "source", "unknown"] | None = None
    source_state: DeliveryProgressSource
    target_ids: list[ProgressIdentity] = Field(default_factory=list, max_length=100)
    impact_delta: ImpactEvidence | None = None
    remaining: str = Field(min_length=1, max_length=8000, pattern=r"\S")

    @model_validator(mode="after")
    def unique_targets(self):
        if len(set(self.target_ids)) != len(self.target_ids):
            raise ValueError("delivery_progress_target_duplicate")
        if (self.contract_version == "delivery-progress/v2") != (self.material_change is not None):
            raise ValueError("delivery_progress_change_declaration_required")
        if self.material_change == "targets" and not self.target_ids:
            raise ValueError("delivery_progress_changed_targets_required")
        if self.material_change == "source" and (not self.source_state.source_ref or self.target_ids):
            raise ValueError("delivery_progress_changed_source_required")
        if self.material_change == "none" and self.has_material_delta:
            raise ValueError("delivery_progress_change_declaration_conflict")
        return self

    @property
    def has_material_delta(self) -> bool:
        delta = self.impact_delta
        return bool(delta and (delta.files or delta.symbols or delta.surfaces or delta.tests))

    @model_serializer(mode="wrap")
    def preserve_v1_digest(self, handler):
        result = handler(self)
        if self.material_change is None:
            result.pop("material_change", None)
        return result


def progress_change_scope(progress: DeliveryProgress) -> str:
    """v1 dirty/delta is ambiguous work, never an invented no-change assertion.

    A legacy context note without dirty state or material delta is not a code
    change merely because it arrived later. Its original payload stays intact.
    """
    if progress.material_change is not None:
        return progress.material_change
    if progress.source_state.workspace_state == "dirty" or progress.has_material_delta:
        return "targets" if progress.target_ids else "source" if progress.source_state.source_ref else "unknown"
    return "none"


def progress_blocks_execution(
    progress: DeliveryProgress, *, target_id: str, source_ref: str,
    checkpoint_received_at: datetime, execution_observed_at: datetime | None,
) -> bool:
    """A receipt must observe the affected work after its material checkpoint.

    Receipt submission time, a repeated binding, a clean note or a newer hash do
    not restore proof. Unknown scope narrows only by supplied target/source IDs.
    """
    if progress_change_scope(progress) == "none":
        return False
    if progress.target_ids:
        if target_id not in progress.target_ids:
            return False
    elif progress.source_state.source_ref and source_ref != progress.source_state.source_ref:
        return False
    if execution_observed_at is None:
        return True
    def utc(value: datetime) -> datetime:
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    return utc(execution_observed_at) <= utc(checkpoint_received_at)


def require_delivery_progress_mutable(card: object) -> None:
    status = getattr(card, "status", None)
    card_type = getattr(card, "card_type", None)
    if (
        getattr(card, "archived", None) is not False
        or getattr(status, "value", status) not in {"started", "in_progress"}
        or getattr(card_type, "value", card_type) not in {"normal", "bug", "test"}
    ):
        raise ValueError("delivery_progress_execution_state_required")
