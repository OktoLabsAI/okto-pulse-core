"""Closed inbound contracts; receipt validity and digests are server owned."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_serializer, model_validator
from pydantic_core import PydanticCustomError

from okto_pulse.core.domain.delivery_progress import DeliveryProgress

Identity = Annotated[str, Field(min_length=1, max_length=512, pattern=r"\S")]


class DeliveryEvidenceQuery(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    board_id: Identity
    spec_id: Identity


class DeliveryEvidenceInput(BaseModel):
    """Spec-scoped exceptions; implementation and test writes belong to cards.

    Historical proof payloads are read from the ledger, never through this
    inbound contract. Reject obsolete writers instead of accepting proof that
    the card-ledger rollup cannot consume.
    """

    model_config = ConfigDict(extra="forbid", strict=True)
    expected_edition: int = Field(ge=1)
    expected_version: int = Field(ge=1)
    idempotency_key: Identity
    kind: Literal["waiver", "revoke"]
    obligation_refs: list[Identity] = Field(default_factory=list, max_length=1000)
    phase: Literal["implementation", "test"] | None = None
    record_id: Identity | None = None
    justification: str = Field(min_length=1, max_length=20000, pattern=r"\S")

    @model_validator(mode="before")
    @classmethod
    def reject_spec_scoped_proof(cls, value):
        if isinstance(value, dict) and value.get("kind") in ("implementation", "test"):
            raise PydanticCustomError(
                "delivery_card_scope_required",
                "Implementation and test evidence must use the card-scoped "
                "delivery-evidence endpoint with expected_card_version and "
                "expected_spec_edition, or okto_pulse_record_delivery_evidence "
                "with card_id. Read okto_pulse_get_delivery_evidence for current scope.",
            )
        return value

    @model_validator(mode="after")
    def closed_shape(self):
        if len(set(self.obligation_refs)) != len(self.obligation_refs):
            raise ValueError("delivery_duplicate_obligation")
        supplied = {
            name for name in ("phase", "record_id") if getattr(self, name) is not None
        }
        required = {
            "waiver": {"phase"},
            "revoke": {"record_id"},
        }[self.kind]
        if supplied != required:
            raise ValueError("delivery_command_fields_invalid")
        if bool(self.obligation_refs) != (self.kind != "revoke"):
            raise ValueError("delivery_obligation_refs_required")
        return self


class DeliveryEvidenceCommand(DeliveryEvidenceQuery, DeliveryEvidenceInput):
    def require_exception_kind(self) -> None:
        """Defend service/store callers that bypass Pydantic construction."""
        if self.kind not in ("waiver", "revoke"):
            raise ValueError(
                "delivery_card_scope_required: use the card-scoped delivery "
                "writer with expected_card_version and expected_spec_edition"
            )


class CardDeliveryEvidenceInput(BaseModel):
    """Card-scoped recording contract: the task owns its delivery bindings.

    Waivers are deliberately absent — they remain a human-only, spec-rollup
    level surface (BR: exceptions never move to the task ledger).
    """

    model_config = ConfigDict(extra="forbid", strict=True)
    expected_card_version: int = Field(ge=1)
    expected_spec_edition: int = Field(ge=1)
    idempotency_key: Identity
    kind: Literal["implementation", "test", "revoke", "progress"]
    obligation_refs: list[Identity] = Field(default_factory=list, max_length=1000)
    execution_id: Identity | None = None
    scenario_id: Identity | None = None
    implementation_ids: list[Identity] = Field(default_factory=list, max_length=1000)
    record_id: Identity | None = None
    justification: str = Field(min_length=1, max_length=20000, pattern=r"\S")
    progress: DeliveryProgress | None = None

    @model_serializer(mode="wrap")
    def preserve_legacy_request_digest(self, handler):
        result = handler(self)
        if self.progress is None:
            result.pop("progress", None)
        return result

    @model_validator(mode="after")
    def closed_shape(self):
        if len(set(self.obligation_refs)) != len(self.obligation_refs):
            raise ValueError("delivery_duplicate_obligation")
        if len(set(self.implementation_ids)) != len(self.implementation_ids):
            raise ValueError("delivery_duplicate_implementation")
        supplied = {
            name
            for name in ("execution_id", "scenario_id", "record_id")
            if getattr(self, name) is not None
        }
        required = {
            "implementation": {"execution_id"},
            "test": {"scenario_id"},
            "revoke": {"record_id"},
            "progress": set(),
        }[self.kind]
        if supplied != required:
            raise ValueError("delivery_command_fields_invalid")
        if self.kind != "progress" and bool(self.obligation_refs) != (
            self.kind != "revoke"
        ):
            raise ValueError("delivery_obligation_refs_required")
        if bool(self.implementation_ids) != (self.kind == "test"):
            raise ValueError("delivery_test_implementation_binding_required")
        if (self.progress is not None) != (self.kind == "progress"):
            raise ValueError("delivery_progress_fields_invalid")
        if self.kind == "progress" and (
            len(self.obligation_refs) > 200
            or len(self.model_dump_json().encode("utf-8")) > 128 * 1024
        ):
            raise ValueError("delivery_progress_payload_limit")
        return self


class CardDeliveryEvidenceCommand(DeliveryEvidenceQuery, CardDeliveryEvidenceInput):
    card_id: Identity
