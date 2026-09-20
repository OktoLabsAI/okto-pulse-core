"""Closed selection request and server-owned report manifest."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_serializer, model_validator

Identity = Annotated[str, Field(min_length=1, max_length=512, pattern=r"\S")]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class DeliverySelectionInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    expected_card_version: int = Field(ge=1)
    expected_spec_edition: int = Field(ge=1)
    expected_delivery_revision: int = Field(ge=0)
    record_ids: list[Identity] = Field(max_length=200)
    reuse_impact: bool = False

    @model_validator(mode="after")
    def unique_records(self):
        if len(set(self.record_ids)) != len(self.record_ids):
            raise ValueError("delivery_selection_duplicate_record")
        return self


class DeliverySelectedRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    id: Identity
    kind: Literal["implementation", "test", "progress"]
    sha256: Digest


class DeliveryImpactBasis(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    source_ref: Identity
    source_identity_sha256: Digest
    base_revision: Annotated[str, Field(pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")]
    result_revision: Annotated[str, Field(pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")]
    observation_receipt_id: Identity
    record_ids: list[Identity] = Field(min_length=1, max_length=200)


class DeliverySelectionManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    contract_version: Literal["card-delivery-selection/v1", "card-delivery-selection/v2"] = "card-delivery-selection/v1"
    board_id: Identity
    card_id: Identity
    spec_id: Identity
    spec_edition: int = Field(ge=1)
    card_version: int = Field(ge=1)
    delivery_revision: int = Field(ge=0)
    scope_sha256: Digest
    impact_sha256: Digest
    records: list[DeliverySelectedRecord] = Field(max_length=200)
    impact_basis: list[DeliveryImpactBasis] | None = Field(default=None, min_length=1, max_length=200)
    sha256: Digest

    @model_validator(mode="after")
    def unique_records(self):
        if len({record.id for record in self.records}) != len(self.records):
            raise ValueError("delivery_selection_duplicate_record")
        if (self.contract_version == "card-delivery-selection/v2") != (self.impact_basis is not None):
            raise ValueError("delivery_selection_impact_basis_required")
        if self.impact_basis is not None:
            if len({row.source_ref for row in self.impact_basis}) != len(self.impact_basis):
                raise ValueError("delivery_selection_impact_source_duplicate")
            selected = {row.id for row in self.records}
            if any(len(set(row.record_ids)) != len(row.record_ids) or not set(row.record_ids) <= selected for row in self.impact_basis):
                raise ValueError("delivery_selection_impact_records_invalid")
        return self

    @model_serializer(mode="wrap")
    def preserve_v1_hash(self, handler):
        result = handler(self)
        if self.impact_basis is None:
            result.pop("impact_basis", None)
        return result
