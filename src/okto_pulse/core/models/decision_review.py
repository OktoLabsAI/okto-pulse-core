"""Authenticated observations of Decision adherence, independent of Test Cards."""

import json
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

from okto_pulse.core.domain.decision_verification import Reference, Text
from okto_pulse.core.domain.verification_report import Digest, VerificationOutcome, VersionedVerificationObject


class DecisionReviewEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    decision_id: Reference
    expected_scope_sha256: Digest
    observed: Text
    result: VerificationOutcome
    sources: list[VersionedVerificationObject] = Field(min_length=1, max_length=20)
    reconciles: list[Reference] = Field(default_factory=list, max_length=100)

    @model_validator(mode="after")
    def unique_references(self):
        if len({s.reference for s in self.sources}) != len(self.sources) or len(set(self.reconciles)) != len(self.reconciles):
            raise ValueError("decision_review_duplicate_reference")
        return self


class DecisionReviewInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    expected_version: Annotated[int, Field(strict=True, ge=1)]
    expected_edition: Annotated[int, Field(strict=True, ge=1)]
    expected_review_revision: Digest
    idempotency_key: Reference
    entries: list[DecisionReviewEntry] = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def bounded_unique_batch(self):
        if len({entry.decision_id for entry in self.entries}) != len(self.entries):
            raise ValueError("decision_review_duplicate_decision")
        if len(json.dumps(self.model_dump(mode="json"), ensure_ascii=False, allow_nan=False).encode()) > 128 * 1024:
            raise ValueError("decision_review_batch_limit")
        return self


class DecisionReviewCommand(DecisionReviewInput):
    board_id: Reference
    spec_id: Reference


class DecisionReviewQuery(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    board_id: Reference
    spec_id: Reference
