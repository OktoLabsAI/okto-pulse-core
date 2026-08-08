"""Closed transport DTOs and projections for the Research Decision Ledger."""

from __future__ import annotations

import base64
import json
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from okto_pulse.core.domain.research_decision_ledger import (
    ResearchDecisionAnchor,
    ResearchDecisionAnchorType,
    ResearchDecisionCommitResult,
    ResearchDecisionContent,
    ResearchDecisionCursor,
    ResearchDecisionDerivationRef,
    ResearchDecisionEntry,
    ResearchDecisionFrozenHead,
    ResearchDecisionHead,
    ResearchDecisionOffsetPage,
    ResearchDecisionPage,
    ResearchDecisionStatus,
    research_decision_content_digest,
)
from okto_pulse.core.models.schemas import PageEnvelope


class ResearchDecisionAnchorInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    anchor_type: ResearchDecisionAnchorType
    anchor_ref: str = Field(min_length=1, max_length=512)

    def to_domain(self) -> ResearchDecisionAnchor:
        return ResearchDecisionAnchor(
            anchor_type=self.anchor_type,
            anchor_ref=self.anchor_ref,
        )


class ResearchDecisionContentInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    unknown: str = Field(min_length=1)
    status: ResearchDecisionStatus = ResearchDecisionStatus.OPEN
    anchor: ResearchDecisionAnchorInput
    evidence_refs: list[str] = Field(default_factory=list)
    alternatives: list[str] = Field(default_factory=list)
    decision: str | None = None
    rationale: str | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    evidence_absence_justification: str | None = None

    def to_domain(self) -> ResearchDecisionContent:
        return ResearchDecisionContent(
            unknown=self.unknown,
            status=self.status,
            anchor=self.anchor.to_domain(),
            evidence_refs=tuple(self.evidence_refs),
            alternatives=tuple(self.alternatives),
            decision=self.decision,
            rationale=self.rationale,
            confidence=self.confidence,
            evidence_absence_justification=self.evidence_absence_justification,
        )


class ResearchDecisionWriteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation: Literal["append", "supersede"] = "append"
    idempotency_key: str = Field(min_length=1, max_length=255)
    expected_head_revision: int = Field(default=0, ge=0)
    ledger_id: str | None = None
    supersedes_entry_id: str | None = None
    entry: ResearchDecisionContentInput

    @model_validator(mode="after")
    def validate_operation_fences(self) -> "ResearchDecisionWriteRequest":
        if self.operation == "append":
            if (
                self.expected_head_revision != 0
                or self.ledger_id is not None
                or self.supersedes_entry_id is not None
            ):
                raise ValueError("research_decision_append_fences_invalid")
            return self
        if (
            self.expected_head_revision < 1
            or not self.ledger_id
            or not self.supersedes_entry_id
        ):
            raise ValueError("research_decision_supersede_fences_required")
        return self


class ResearchDecisionAnchorView(BaseModel):
    anchor_type: ResearchDecisionAnchorType
    anchor_ref: str


class ResearchDecisionContentView(BaseModel):
    unknown: str
    status: ResearchDecisionStatus
    anchor: ResearchDecisionAnchorView
    evidence_refs: list[str]
    alternatives: list[str]
    decision: str | None
    rationale: str | None
    confidence: float | None
    evidence_absence_justification: str | None


class ResearchDecisionEntryView(BaseModel):
    id: str
    ledger_id: str
    board_id: str
    refinement_id: str
    refinement_version: int
    predecessor_entry_id: str | None
    content: ResearchDecisionContentView
    content_digest: str
    created_by: str
    created_at: datetime

    @classmethod
    def from_domain(
        cls,
        entry: ResearchDecisionEntry,
    ) -> "ResearchDecisionEntryView":
        return cls(
            id=entry.id,
            ledger_id=entry.ledger_id,
            board_id=entry.board_id,
            refinement_id=entry.refinement_id,
            refinement_version=entry.refinement_version,
            predecessor_entry_id=entry.predecessor_entry_id,
            content=ResearchDecisionContentView(
                unknown=entry.content.unknown,
                status=entry.content.status,
                anchor=ResearchDecisionAnchorView(
                    anchor_type=entry.content.anchor.anchor_type,
                    anchor_ref=entry.content.anchor.anchor_ref,
                ),
                evidence_refs=list(entry.content.evidence_refs),
                alternatives=list(entry.content.alternatives),
                decision=entry.content.decision,
                rationale=entry.content.rationale,
                confidence=entry.content.confidence,
                evidence_absence_justification=(
                    entry.content.evidence_absence_justification
                ),
            ),
            content_digest=research_decision_content_digest(entry.content),
            created_by=entry.created_by,
            created_at=entry.created_at,
        )


class ResearchDecisionFrozenHeadView(BaseModel):
    ledger_id: str
    entry_id: str
    head_revision: int
    head_refinement_version: int
    status: ResearchDecisionStatus
    content_digest: str

    @classmethod
    def from_domain(
        cls,
        head: ResearchDecisionFrozenHead,
    ) -> "ResearchDecisionFrozenHeadView":
        return cls(**{
            "ledger_id": head.ledger_id,
            "entry_id": head.entry_id,
            "head_revision": head.head_revision,
            "head_refinement_version": head.head_refinement_version,
            "status": head.status,
            "content_digest": head.content_digest,
        })


class ResearchDecisionDerivationRefView(BaseModel):
    source_snapshot_id: str
    source_refinement_id: str
    source_refinement_version: int
    ledger_id: str
    entry_id: str
    head_revision: int
    status: ResearchDecisionStatus
    content_digest: str

    @classmethod
    def from_domain(
        cls,
        reference: ResearchDecisionDerivationRef,
    ) -> "ResearchDecisionDerivationRefView":
        return cls(**{
            "source_snapshot_id": reference.source_snapshot_id,
            "source_refinement_id": reference.source_refinement_id,
            "source_refinement_version": (
                reference.source_refinement_version
            ),
            "ledger_id": reference.ledger_id,
            "entry_id": reference.entry_id,
            "head_revision": reference.head_revision,
            "status": reference.status,
            "content_digest": reference.content_digest,
        })


class ResearchDecisionWriteResponse(BaseModel):
    entry_id: str
    thread_id: str
    sequence: int
    head_revision: int
    refinement_version: int
    replayed: bool
    entry: ResearchDecisionEntryView

    @classmethod
    def from_domain(
        cls,
        result: ResearchDecisionCommitResult,
    ) -> "ResearchDecisionWriteResponse":
        return cls(
            entry_id=result.entry.id,
            thread_id=result.entry.ledger_id,
            sequence=result.head.revision,
            head_revision=result.head.revision,
            refinement_version=result.refinement_version,
            replayed=result.replayed,
            entry=ResearchDecisionEntryView.from_domain(result.entry),
        )


class ResearchDecisionHeadResponse(BaseModel):
    """Authoritative current head used by REST clients for supersede CAS."""

    entry: ResearchDecisionEntryView
    head_revision: int = Field(ge=1)

    @classmethod
    def from_domain(
        cls,
        entry: ResearchDecisionEntry,
        head: ResearchDecisionHead,
    ) -> "ResearchDecisionHeadResponse":
        if (
            head.ledger_id != entry.ledger_id
            or head.current_entry_id != entry.id
            or head.board_id != entry.board_id
            or head.refinement_id != entry.refinement_id
            or head.refinement_version != entry.refinement_version
            or head.status is not entry.status
        ):
            raise ValueError("research_decision_head_projection_mismatch")
        return cls(
            entry=ResearchDecisionEntryView.from_domain(entry),
            head_revision=head.revision,
        )


class ResearchDecisionListResponse(BaseModel):
    items: list[ResearchDecisionEntryView]
    limit: int
    next_cursor: str | None
    has_more: bool
    ordering: tuple[str, str]

    @classmethod
    def from_domain(
        cls,
        page: ResearchDecisionPage[ResearchDecisionEntry],
    ) -> "ResearchDecisionListResponse":
        return cls(
            items=[
                ResearchDecisionEntryView.from_domain(entry)
                for entry in page.items
            ],
            limit=page.limit,
            next_cursor=(
                encode_research_decision_cursor(page.next_cursor)
                if page.next_cursor is not None
                else None
            ),
            has_more=page.has_more,
            ordering=page.ordering,
        )


class ResearchDecisionOffsetListResponse(
    PageEnvelope[ResearchDecisionEntryView]
):
    @classmethod
    def from_domain(
        cls,
        page: ResearchDecisionOffsetPage[ResearchDecisionEntry],
    ) -> "ResearchDecisionOffsetListResponse":
        return cls(
            items=[
                ResearchDecisionEntryView.from_domain(entry)
                for entry in page.items
            ],
            offset=page.offset,
            limit=page.limit,
            total_filtered=page.total_filtered,
            total_overall=page.total_overall,
        )


def encode_research_decision_cursor(cursor: ResearchDecisionCursor) -> str:
    payload = json.dumps(
        {
            "created_at": cursor.created_at.isoformat(),
            "entry_id": cursor.entry_id,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_research_decision_cursor(value: str) -> ResearchDecisionCursor:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("research_decision_cursor_invalid")
    try:
        encoded = value.strip()
        encoded += "=" * (-len(encoded) % 4)
        payload = json.loads(base64.urlsafe_b64decode(encoded).decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError
        return ResearchDecisionCursor(
            created_at=datetime.fromisoformat(str(payload["created_at"])),
            entry_id=str(payload["entry_id"]),
        )
    except (KeyError, TypeError, ValueError, UnicodeError) as exc:
        raise ValueError("research_decision_cursor_invalid") from exc


__all__ = [
    "ResearchDecisionAnchorInput",
    "ResearchDecisionContentInput",
    "ResearchDecisionEntryView",
    "ResearchDecisionHeadResponse",
    "ResearchDecisionDerivationRefView",
    "ResearchDecisionFrozenHeadView",
    "ResearchDecisionListResponse",
    "ResearchDecisionOffsetListResponse",
    "ResearchDecisionWriteRequest",
    "ResearchDecisionWriteResponse",
    "decode_research_decision_cursor",
    "encode_research_decision_cursor",
]
