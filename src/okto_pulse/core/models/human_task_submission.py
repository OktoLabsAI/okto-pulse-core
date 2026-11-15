"""Human intent is a declaration, never an execution or a test receipt."""

from __future__ import annotations

import hashlib
import json
import unicodedata
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from okto_pulse.core.models.delivery_evidence import Identity


def canonical_submission_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def submission_digest(value: object) -> str:
    return hashlib.sha256(canonical_submission_json(value)).hexdigest()


class HumanTaskSubmissionInput(BaseModel):
    """Closed public body. Identity, authority, time and receipts are absent."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    contract_version: Literal["human-task-submission/v1"]
    action: Literal["progress", "delivery_report"]
    content: str = Field(min_length=1, max_length=20000)
    remaining: str | None = Field(default=None, max_length=8000)
    expected_card_version: int = Field(ge=1)
    expected_spec_edition: int = Field(ge=1)
    idempotency_key: str = Field(min_length=16, max_length=128, pattern=r"^[\x21-\x7e]+$")
    requested_status: Literal["validation", "done"] | None = None
    supersedes_submission_id: Identity | None = None

    @field_validator("content", "remaining", mode="before")
    @classmethod
    def normalize_text(cls, value):
        if isinstance(value, str):
            return unicodedata.normalize("NFC", value.replace("\r\n", "\n").replace("\r", "\n")).strip()
        return value

    @model_validator(mode="after")
    def validate_intent(self):
        if self.action == "progress" and "requested_status" in self.model_fields_set:
            raise ValueError("human_submission_progress_has_no_transition")
        if self.action == "delivery_report" and self.requested_status is None:
            raise ValueError("human_submission_report_status_required")
        if len(canonical_submission_json(self.model_dump(mode="json"))) > 128 * 1024:
            raise ValueError("human_submission_payload_too_large")
        return self

    def content_digest(self) -> str:
        return submission_digest({"content": self.content, "remaining": self.remaining})


class HumanTaskSubmissionRequest(HumanTaskSubmissionInput):
    """Route scope, before the application resolves the Card's parent Spec."""
    board_id: Identity
    card_id: Identity


class HumanTaskSubmissionCommand(HumanTaskSubmissionRequest):
    """Scope comes from the authorized route/Card, never from the public body."""
    spec_id: Identity

    def intent_digest(self) -> str:
        # Includes every fence and semantic field, so an old key cannot silently
        # acquire new meaning after an edit, transition, or Spec edition change.
        return submission_digest(self.model_dump(mode="json"))


class HumanSubmissionReceipt(BaseModel):
    """Server-only immutable audit record; excluded from technical proof types."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    contract_version: Literal["human-submission-receipt/v1"] = "human-submission-receipt/v1"
    kind: Literal["human_submission"] = "human_submission"
    authority: Literal["human_declaration"] = "human_declaration"
    origin: Literal["human_task_submission"] = "human_task_submission"
    id: Identity
    submission_id: Identity
    board_id: Identity
    card_id: Identity
    spec_id: Identity
    spec_edition: int = Field(ge=1)
    actor_id: Identity
    actor_kind: Literal["human", "user"]
    received_at: datetime
    action: Literal["progress", "delivery_report"]
    before_card_version: int = Field(ge=1)
    after_card_version: int = Field(ge=1)
    content_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    intent_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    supersedes_submission_id: Identity | None = None

    @field_validator("received_at")
    @classmethod
    def require_utc(cls, value):
        if value.utcoffset() is None or value.utcoffset().total_seconds() != 0:
            raise ValueError("human_submission_server_time_must_be_utc")
        return value


class HumanSubmissionConflict(ValueError):
    """Bounded, actionable rejection without a stack trace or accepted receipt."""

    code = "submission_conflict"

    def __init__(self, reason: str, message: str, *, next_action: str = "refresh_and_retry",
                 next_owner: str = "user"):
        super().__init__(message)
        self.reason = reason
        self.next_action = next_action
        self.next_owner = next_owner

    def to_error_dict(self):
        return {"code": self.code, "message": str(self), "details": {"reason": self.reason},
                "next_action": self.next_action, "next_owner": self.next_owner}
