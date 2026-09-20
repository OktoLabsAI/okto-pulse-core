"""Closed selection request and server-owned report manifest."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

Identity = Annotated[str, Field(min_length=1, max_length=512, pattern=r"\S")]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class DeliverySelectionInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    expected_card_version: int = Field(ge=1)
    expected_spec_edition: int = Field(ge=1)
    expected_delivery_revision: int = Field(ge=0)
    record_ids: list[Identity] = Field(max_length=200)

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


class DeliverySelectionManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    contract_version: Literal["card-delivery-selection/v1"] = "card-delivery-selection/v1"
    board_id: Identity
    card_id: Identity
    spec_id: Identity
    spec_edition: int = Field(ge=1)
    card_version: int = Field(ge=1)
    delivery_revision: int = Field(ge=0)
    scope_sha256: Digest
    impact_sha256: Digest
    records: list[DeliverySelectedRecord] = Field(max_length=200)
    sha256: Digest

    @model_validator(mode="after")
    def unique_records(self):
        if len({record.id for record in self.records}) != len(self.records):
            raise ValueError("delivery_selection_duplicate_record")
        return self
