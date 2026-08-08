"""Deterministic findings projected from failed semantic metric results."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone

from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_BINDING_ID_MAX_LENGTH,
    GUIDELINE_ID_MAX_LENGTH,
    GUIDELINE_REVISION_ID_MAX_LENGTH,
    POLICY_METRIC_CODE_MAX_LENGTH,
    POLICY_METRIC_ID_MAX_LENGTH,
    POLICY_RECEIPT_ID_MAX_LENGTH,
    PolicySubjectRef,
    normalize_policy_bounded_text,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentContractError,
    SemanticAssessmentPinpoint,
    SemanticGuidelineAssessmentReceipt,
    SemanticMetricOutcome,
    SemanticMetricResult,
)
from okto_pulse.core.domain.quality_assessment import EvidenceRef
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256


SEMANTIC_METRIC_RESULT_DIGEST_VERSION = "semantic-metric-result/v1"
SEMANTIC_METRIC_FINDING_CONTRACT_VERSION = "semantic-metric-finding/v1"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _sha256(value: object, code: str) -> str:
    if not isinstance(value, str):
        raise SemanticAssessmentContractError(code)
    normalized = value.strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise SemanticAssessmentContractError(code)
    return normalized


def _aware_datetime(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise SemanticAssessmentContractError(code)
    return value.astimezone(timezone.utc)


def _evidence_payload(item: EvidenceRef) -> dict[str, object]:
    return {
        "source_type": item.source_type,
        "source_id": item.source_id,
        "source_version": item.source_version,
        "content_hash": item.content_hash,
    }


def _pinpoint_payload(item: SemanticAssessmentPinpoint) -> dict[str, object]:
    return {
        "subject": {
            "board_id": item.subject.board_id,
            "subject_type": item.subject.entity_type.value,
            "subject_id": item.subject.subject_id,
            "subject_version": item.subject.subject_version,
        },
        "input_digest": item.input_digest,
        "anchor_type": item.anchor_type.value,
        "anchor_ref": item.anchor_ref,
        "excerpt_hash": item.excerpt_hash,
    }


def semantic_metric_result_digest_v1(
    result: SemanticMetricResult,
) -> str:
    """Seal every immutable field of one admitted metric result."""

    if not isinstance(result, SemanticMetricResult):
        raise SemanticAssessmentContractError(
            "semantic_metric_result_invalid"
        )
    return canonical_sha256(
        {
            "contract": SEMANTIC_METRIC_RESULT_DIGEST_VERSION,
            "metric_result_id": result.metric_result_id,
            "receipt_id": result.receipt_id,
            "subject": {
                "board_id": result.subject.board_id,
                "subject_type": result.subject.entity_type.value,
                "subject_id": result.subject.subject_id,
                "subject_version": result.subject.subject_version,
            },
            "binding_id": result.binding_id,
            "guideline_id": result.guideline_id,
            "revision_id": result.revision_id,
            "metric_id": result.metric_id,
            "metric_code": result.metric_code,
            "metric_definition_digest": result.metric_definition_digest,
            "score": result.score,
            "direction": result.direction.value,
            "default_threshold": result.default_threshold,
            "effective_threshold": result.effective_threshold,
            "threshold_source": result.threshold_source.value,
            "outcome": result.outcome.value,
            "rationale": result.rationale,
            "evidence_refs": [
                _evidence_payload(item) for item in result.evidence_refs
            ],
            "pinpoints": [
                _pinpoint_payload(item) for item in result.pinpoints
            ],
        }
    )


@dataclass(frozen=True, slots=True)
class SemanticMetricFinding:
    """Lossless, independently addressable view of one failed metric result."""

    finding_id: str
    metric_result_id: str
    metric_result_digest: str
    receipt_id: str
    receipt_digest: str
    subject: PolicySubjectRef
    subject_content_digest: str
    guideline_id: str
    guideline_revision_id: str
    guideline_revision_digest: str
    binding_id: str
    binding_revision: int
    binding_configuration_digest: str
    metric_id: str
    metric_code: str
    rationale: str
    evidence_refs: tuple[EvidenceRef, ...]
    pinpoints: tuple[SemanticAssessmentPinpoint, ...]
    created_at: datetime
    finding_digest: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError(
                "semantic_metric_finding_subject_invalid"
            )
        for field_name, max_length in (
            ("finding_id", POLICY_RECEIPT_ID_MAX_LENGTH),
            ("metric_result_id", POLICY_RECEIPT_ID_MAX_LENGTH),
            ("receipt_id", POLICY_RECEIPT_ID_MAX_LENGTH),
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("guideline_revision_id", GUIDELINE_REVISION_ID_MAX_LENGTH),
            ("binding_id", GUIDELINE_BINDING_ID_MAX_LENGTH),
            ("metric_id", POLICY_METRIC_ID_MAX_LENGTH),
            ("metric_code", POLICY_METRIC_CODE_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=max_length,
                    code=f"semantic_metric_finding_{field_name}_required",
                ),
            )
        if (
            not isinstance(self.binding_revision, int)
            or isinstance(self.binding_revision, bool)
            or self.binding_revision < 1
        ):
            raise SemanticAssessmentContractError(
                "semantic_metric_finding_binding_revision_invalid"
            )
        for field_name in (
            "metric_result_digest",
            "receipt_digest",
            "subject_content_digest",
            "guideline_revision_digest",
            "binding_configuration_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"semantic_metric_finding_{field_name}_invalid",
                ),
            )
        if not isinstance(self.rationale, str) or not self.rationale.strip():
            raise SemanticAssessmentContractError(
                "semantic_metric_finding_rationale_required"
            )
        object.__setattr__(self, "rationale", self.rationale.strip())
        if (
            not isinstance(self.evidence_refs, tuple | list)
            or not self.evidence_refs
            or any(
                not isinstance(item, EvidenceRef)
                for item in self.evidence_refs
            )
        ):
            raise SemanticAssessmentContractError(
                "semantic_metric_finding_evidence_invalid"
            )
        if (
            not isinstance(self.pinpoints, tuple | list)
            or not self.pinpoints
            or any(
                not isinstance(item, SemanticAssessmentPinpoint)
                for item in self.pinpoints
            )
        ):
            raise SemanticAssessmentContractError(
                "semantic_metric_finding_pinpoints_invalid"
            )
        object.__setattr__(self, "evidence_refs", tuple(self.evidence_refs))
        object.__setattr__(self, "pinpoints", tuple(self.pinpoints))
        object.__setattr__(
            self,
            "created_at",
            _aware_datetime(
                self.created_at,
                "semantic_metric_finding_created_at_invalid",
            ),
        )
        expected_digest = semantic_metric_finding_digest_v1(self)
        if self.finding_digest is not None:
            supplied = _sha256(
                self.finding_digest,
                "semantic_metric_finding_digest_invalid",
            )
            if supplied != expected_digest:
                raise SemanticAssessmentContractError(
                    "semantic_metric_finding_digest_mismatch"
                )
        object.__setattr__(self, "finding_digest", expected_digest)


def semantic_metric_finding_digest_v1(
    finding: SemanticMetricFinding,
) -> str:
    """Seal one finding without recursively including its own digest."""

    return canonical_sha256(
        {
            "contract": SEMANTIC_METRIC_FINDING_CONTRACT_VERSION,
            "finding_id": finding.finding_id,
            "metric_result_id": finding.metric_result_id,
            "metric_result_digest": finding.metric_result_digest,
            "receipt_id": finding.receipt_id,
            "receipt_digest": finding.receipt_digest,
            "subject": {
                "board_id": finding.subject.board_id,
                "subject_type": finding.subject.entity_type.value,
                "subject_id": finding.subject.subject_id,
                "subject_version": finding.subject.subject_version,
                "content_digest": finding.subject_content_digest,
            },
            "guideline": {
                "guideline_id": finding.guideline_id,
                "revision_id": finding.guideline_revision_id,
                "revision_digest": finding.guideline_revision_digest,
            },
            "binding": {
                "binding_id": finding.binding_id,
                "binding_revision": finding.binding_revision,
                "configuration_digest": (
                    finding.binding_configuration_digest
                ),
            },
            "metric_id": finding.metric_id,
            "metric_code": finding.metric_code,
            "rationale": finding.rationale,
            "evidence_refs": [
                _evidence_payload(item) for item in finding.evidence_refs
            ],
            "pinpoints": [
                _pinpoint_payload(item) for item in finding.pinpoints
            ],
            "created_at": finding.created_at.isoformat(),
        }
    )


def project_semantic_metric_findings(
    receipt: SemanticGuidelineAssessmentReceipt,
) -> tuple[SemanticMetricFinding, ...]:
    """Project exactly one stable finding for every failed metric result."""

    if not isinstance(receipt, SemanticGuidelineAssessmentReceipt):
        raise SemanticAssessmentContractError(
            "semantic_metric_findings_receipt_invalid"
        )
    findings: list[SemanticMetricFinding] = []
    for result in receipt.metric_results:
        if result.outcome is not SemanticMetricOutcome.FAIL:
            continue
        result_digest = semantic_metric_result_digest_v1(result)
        finding_id = canonical_sha256(
            {
                "contract": SEMANTIC_METRIC_FINDING_CONTRACT_VERSION,
                "receipt_id": receipt.receipt_id,
                "metric_result_id": result.metric_result_id,
                "metric_result_digest": result_digest,
            }
        )
        findings.append(
            SemanticMetricFinding(
                finding_id=finding_id,
                metric_result_id=result.metric_result_id,
                metric_result_digest=result_digest,
                receipt_id=receipt.receipt_id,
                receipt_digest=receipt.receipt_digest,
                subject=receipt.subject,
                subject_content_digest=receipt.subject_content_digest,
                guideline_id=receipt.guideline_id,
                guideline_revision_id=receipt.guideline_revision_id,
                guideline_revision_digest=(
                    receipt.guideline_revision_digest
                ),
                binding_id=receipt.binding_id,
                binding_revision=receipt.binding_revision,
                binding_configuration_digest=(
                    receipt.binding_configuration_digest
                ),
                metric_id=result.metric_id,
                metric_code=result.metric_code,
                rationale=result.rationale,
                evidence_refs=result.evidence_refs,
                pinpoints=result.pinpoints,
                created_at=receipt.recorded_at,
            )
        )
    return tuple(findings)


__all__ = [
    "SEMANTIC_METRIC_FINDING_CONTRACT_VERSION",
    "SEMANTIC_METRIC_RESULT_DIGEST_VERSION",
    "SemanticMetricFinding",
    "project_semantic_metric_findings",
    "semantic_metric_finding_digest_v1",
    "semantic_metric_result_digest_v1",
]
