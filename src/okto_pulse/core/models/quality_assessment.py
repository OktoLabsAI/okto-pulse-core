"""Closed transport DTOs and projections for SK-A quality assessments."""

from __future__ import annotations

import base64
from collections.abc import Callable
from datetime import datetime
import json
from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from okto_pulse.core.domain.quality_assessment import (
    AssessmentCurrentness,
    AssessmentReceipt,
    AssessmentReceiptView,
    EvidenceRef,
    FindingAnchorType,
    FindingSeverity,
    ProposedQuestionDraft,
    QualityFinding,
    QualityPage,
    QualityPageCursor,
    UnboundFindingAnchor,
    UnboundQualityFindingDraft,
)
if TYPE_CHECKING:
    from okto_pulse.core.services.quality_assessment import (
        CurrentAssessmentView,
        QualityGatePreview,
    )


class QualityEvidenceRefInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_type: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    source_version: int = Field(ge=1)
    content_hash: str = Field(pattern=r"^[0-9a-fA-F]{64}$")

    def to_domain(self) -> EvidenceRef:
        return EvidenceRef(
            source_type=self.source_type,
            source_id=self.source_id,
            source_version=self.source_version,
            content_hash=self.content_hash,
        )


class QualityFindingAnchorInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    anchor_type: FindingAnchorType
    anchor_ref: str | None = None
    excerpt_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-fA-F]{64}$",
    )

    def to_domain(self) -> UnboundFindingAnchor:
        return UnboundFindingAnchor(
            anchor_type=self.anchor_type,
            anchor_ref=self.anchor_ref,
            excerpt_hash=self.excerpt_hash,
        )


class QualityFindingInput(BaseModel):
    """Caller-owned finding content; identity and blocking flags are absent."""

    model_config = ConfigDict(extra="forbid")

    finding_key: str = Field(min_length=1)
    category_code: str = Field(min_length=1)
    severity: FindingSeverity
    confidence: float = Field(ge=0.0, le=1.0)
    deterministic: bool
    title: str = Field(min_length=1)
    detail: str = Field(min_length=1)
    anchor: QualityFindingAnchorInput
    remediation: str | None = None
    rule_code: str | None = None
    evidence_refs: list[QualityEvidenceRefInput] = Field(default_factory=list)

    def to_domain(self) -> UnboundQualityFindingDraft:
        return UnboundQualityFindingDraft(
            finding_key=self.finding_key,
            category_code=self.category_code,
            severity=self.severity,
            confidence=self.confidence,
            deterministic=self.deterministic,
            title=self.title,
            detail=self.detail,
            anchor=self.anchor.to_domain(),
            remediation=self.remediation,
            rule_code=self.rule_code,
            evidence_refs=tuple(item.to_domain() for item in self.evidence_refs),
        )


class QualityProposedQuestionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    client_key: str = Field(min_length=1)
    question: str = Field(min_length=1)
    question_type: str = Field(min_length=1)
    choices: list[str] = Field(default_factory=list)
    allow_free_text: bool = True
    category_code: str | None = None
    finding_keys: list[str] = Field(default_factory=list)

    def to_domain(self) -> ProposedQuestionDraft:
        return ProposedQuestionDraft(
            client_key=self.client_key,
            question=self.question,
            question_type=self.question_type,
            choices=tuple(self.choices),
            allow_free_text=self.allow_free_text,
            category_code=self.category_code,
            finding_keys=tuple(self.finding_keys),
        )


def encode_quality_cursor(cursor: QualityPageCursor) -> str:
    payload = json.dumps(
        {
            "v": 1,
            "created_at": cursor.created_at.isoformat(),
            "item_id": cursor.item_id,
            "offset": cursor.offset,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_quality_cursor(value: str) -> QualityPageCursor:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("quality_cursor_invalid")
    try:
        encoded = value.strip()
        encoded += "=" * (-len(encoded) % 4)
        payload = json.loads(base64.urlsafe_b64decode(encoded).decode("utf-8"))
        if (
            not isinstance(payload, dict)
            or set(payload) != {"v", "created_at", "item_id", "offset"}
            or payload["v"] != 1
        ):
            raise ValueError
        return QualityPageCursor(
            created_at=datetime.fromisoformat(str(payload["created_at"])),
            item_id=str(payload["item_id"]),
            offset=payload["offset"],
        )
    except (KeyError, TypeError, ValueError, UnicodeError) as exc:
        raise ValueError("quality_cursor_invalid") from exc


def project_quality_currentness(value: AssessmentCurrentness) -> dict[str, Any]:
    return {
        "current": value.current,
        "state": "current" if value.current else "previous",
        # Compatibility/audit field. Product surfaces use only ``state``.
        "stale_reasons": [reason.value for reason in value.stale_reasons],
    }


def project_quality_gate_preview(
    value: "QualityGatePreview",
) -> dict[str, Any]:
    return {
        "applicable": value.applicable,
        "enabled": value.enabled,
        "allowed": value.allowed,
        "reason_code": value.reason_code,
        "threshold": value.threshold,
        "score": value.score,
        "skipped": value.skipped,
    }


def project_current_quality_assessment(
    view: "CurrentAssessmentView",
) -> dict[str, Any]:
    currentness = project_quality_currentness(view.currentness)
    return {
        "receipt": project_quality_receipt(view.receipt),
        "edition": view.receipt.subject.subject_edition,
        "lifecycle_state": currentness["state"],
        "head_revision": view.head.revision,
        "currentness": currentness["state"],
        "stale_reasons": currentness["stale_reasons"],
        "gate_preview": project_quality_gate_preview(view.gate_preview),
    }


def project_quality_receipt_currentness(
    receipt: AssessmentReceipt,
    currentness: AssessmentCurrentness,
) -> dict[str, Any]:
    projected = project_quality_currentness(currentness)
    return {
        "receipt": project_quality_receipt(receipt),
        "currentness": projected["state"],
        "stale_reasons": projected["stale_reasons"],
    }


def project_quality_receipt(receipt: AssessmentReceipt) -> dict[str, Any]:
    return {
        "id": receipt.id,
        "board_id": receipt.subject.board_id,
        "subject_type": receipt.subject.subject_type.value,
        "subject_id": receipt.subject.subject_id,
        "subject_version": receipt.subject.subject_version,
        "subject_edition": receipt.subject.subject_edition,
        "assessment_kind": receipt.assessment_kind.value,
        "origin": receipt.origin.value,
        "source": receipt.source.value,
        "channel": receipt.channel,
        "outcome": receipt.outcome.value,
        "scale": {
            "kind": receipt.scale.kind.value,
            "minimum": receipt.scale.minimum,
            "maximum": receipt.scale.maximum,
            "direction": receipt.scale.direction.value,
        },
        "score": receipt.score,
        "justification": receipt.justification,
        "digests": {
            "content_digest": receipt.digests.content_digest,
            "clarification_digest": receipt.digests.clarification_digest,
            "ruleset_digest": receipt.digests.ruleset_digest,
            "taxonomy_digest": receipt.digests.taxonomy_digest,
            "policy_digest": receipt.digests.policy_digest,
            "input_digest": receipt.digests.input_digest,
            "canonicalization_version": (
                receipt.digests.canonicalization_version
            ),
        },
        "versions": {
            "ruleset_version": receipt.versions.ruleset_version,
            "taxonomy_version": receipt.versions.taxonomy_version,
            "analyzer_version": receipt.versions.analyzer_version,
            "policy_version": receipt.versions.policy_version,
        },
        "run_identity_digest": receipt.run_identity_digest,
        "authority_digest": receipt.authority_digest,
        "idempotency_key": receipt.idempotency_key,
        "request_digest": receipt.request_digest,
        "created_by": receipt.created_by,
        "created_at": receipt.created_at.isoformat(),
        "predecessor_receipt_id": receipt.predecessor_receipt_id,
        "contract_version": receipt.contract_version,
    }


def project_quality_receipt_view(
    view: AssessmentReceiptView,
) -> dict[str, Any]:
    return {
        "receipt": project_quality_receipt(view.receipt),
        "is_head": view.is_head,
        "state": view.state.value,
        "currentness": project_quality_currentness(view.freshness),
    }


def project_quality_finding(finding: QualityFinding) -> dict[str, Any]:
    return {
        "id": finding.id,
        "receipt_id": finding.receipt_id,
        "assessment_kind": finding.assessment_kind.value,
        "finding_key": finding.finding_key,
        "category_code": finding.category_code,
        "taxonomy_version": finding.taxonomy_version,
        "severity": finding.severity.value,
        "confidence": finding.confidence,
        "deterministic": finding.deterministic,
        "blocking_eligible": finding.blocking_eligible,
        "title": finding.title,
        "detail": finding.detail,
        "anchor": {
            "board_id": finding.anchor.board_id,
            "subject_type": finding.anchor.subject_type.value,
            "subject_id": finding.anchor.subject_id,
            "subject_version": finding.anchor.subject_version,
            "input_digest": finding.anchor.input_digest,
            "anchor_type": finding.anchor.anchor_type.value,
            "anchor_ref": finding.anchor.anchor_ref,
            "excerpt_hash": finding.anchor.excerpt_hash,
        },
        "evidence_refs": [
            {
                "source_type": item.source_type,
                "source_id": item.source_id,
                "source_version": item.source_version,
                "content_hash": item.content_hash,
            }
            for item in finding.evidence_refs
        ],
        "lifecycle": finding.lifecycle.value,
        "created_at": finding.created_at.isoformat(),
        "remediation": finding.remediation,
        "rule_code": finding.rule_code,
    }


_ItemT = TypeVar("_ItemT")


def project_quality_keyset_page(
    page: QualityPage[_ItemT],
    *,
    projector: Callable[[_ItemT], dict[str, Any]],
    created_at: Callable[[_ItemT], datetime],
    item_id: Callable[[_ItemT], str],
) -> dict[str, Any]:
    has_more = page.offset + len(page.items) < page.total_filtered
    next_cursor = None
    if has_more and page.items:
        last = page.items[-1]
        next_cursor = encode_quality_cursor(
            QualityPageCursor(
                created_at=created_at(last),
                item_id=item_id(last),
                offset=page.offset + len(page.items),
            )
        )
    return {
        "items": [projector(item) for item in page.items],
        "limit": page.limit,
        "offset": page.offset,
        "total_filtered": page.total_filtered,
        "total_overall": page.total_overall,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "ordering": "created_at_desc_id_desc",
    }


__all__ = [
    "QualityEvidenceRefInput",
    "QualityFindingAnchorInput",
    "QualityFindingInput",
    "QualityProposedQuestionInput",
    "decode_quality_cursor",
    "encode_quality_cursor",
    "project_current_quality_assessment",
    "project_quality_currentness",
    "project_quality_finding",
    "project_quality_gate_preview",
    "project_quality_keyset_page",
    "project_quality_receipt",
    "project_quality_receipt_currentness",
    "project_quality_receipt_view",
]
