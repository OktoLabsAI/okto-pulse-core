"""Closed inbound contracts; receipt validity and digests are server owned."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

Identity = Annotated[str, Field(min_length=1, max_length=512, pattern=r"\S")]


class DeliveryEvidenceQuery(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    board_id: Identity
    spec_id: Identity


class DeliveryEvidenceInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    expected_edition: int = Field(ge=1)
    expected_version: int = Field(ge=1)
    idempotency_key: Identity
    kind: Literal["implementation", "test", "waiver", "revoke"]
    obligation_refs: list[Identity] = Field(default_factory=list, max_length=1000)
    card_id: Identity | None = None
    execution_id: Identity | None = None
    scenario_id: Identity | None = None
    implementation_ids: list[Identity] = Field(default_factory=list, max_length=1000)
    phase: Literal["implementation", "test"] | None = None
    record_id: Identity | None = None
    justification: str = Field(min_length=1, max_length=20000, pattern=r"\S")

    @model_validator(mode="after")
    def closed_shape(self):
        if len(set(self.obligation_refs)) != len(self.obligation_refs):
            raise ValueError("delivery_duplicate_obligation")
        if len(set(self.implementation_ids)) != len(self.implementation_ids):
            raise ValueError("delivery_duplicate_implementation")
        supplied = {
            name
            for name in ("card_id", "execution_id", "scenario_id", "phase", "record_id")
            if getattr(self, name) is not None
        }
        required = {
            "implementation": {"card_id", "execution_id"},
            "test": {"card_id", "scenario_id"},
            "waiver": {"phase"},
            "revoke": {"record_id"},
        }[self.kind]
        if supplied != required:
            raise ValueError("delivery_command_fields_invalid")
        if bool(self.obligation_refs) != (self.kind != "revoke"):
            raise ValueError("delivery_obligation_refs_required")
        if bool(self.implementation_ids) != (self.kind == "test"):
            raise ValueError("delivery_test_implementation_binding_required")
        return self


class DeliveryEvidenceCommand(DeliveryEvidenceQuery, DeliveryEvidenceInput):
    pass
