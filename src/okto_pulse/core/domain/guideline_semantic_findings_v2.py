"""Finding cardinality and immutable waiver fences for semantic v2."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Literal

from okto_pulse.core.domain.guideline_policy import (
    PolicySubjectRef,
    normalize_policy_bounded_text,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentContractError,
    SemanticMetricOutcome,
)
from okto_pulse.core.domain.guideline_semantic_v2 import (
    SemanticMetricResultV2,
    SemanticPinpointV2,
    semantic_finding_digest_v2,
    semantic_metric_result_digest_v2,
)
from okto_pulse.core.domain.quality_assessment import EvidenceRef
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256


SEMANTIC_METRIC_FINDING_ID_VERSION_V2 = "semantic-metric-finding-id/v2"
SEMANTIC_WAIVER_FINDING_IDENTITY_MISMATCH = "semantic_waiver_finding_identity_mismatch"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _sha256(value: object, code: str) -> str:
    if not isinstance(value, str):
        raise SemanticAssessmentContractError(code)
    normalized = value.strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise SemanticAssessmentContractError(code)
    return normalized


def _aware_utc(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise SemanticAssessmentContractError(code)
    return value.astimezone(timezone.utc)


def _required(value: object, code: str, max_length: int = 500) -> str:
    return normalize_policy_bounded_text(
        value,
        max_length=max_length,
        code=code,
    )


def _positive_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise SemanticAssessmentContractError(code)
    return value


def _evidence_payload(item: EvidenceRef) -> dict[str, object]:
    return {
        "source_type": item.source_type,
        "source_id": item.source_id,
        "source_version": item.source_version,
        "content_hash": item.content_hash,
    }


@dataclass(frozen=True, slots=True)
class SemanticAssessmentReceiptProjectionV2:
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
    assessment_assessor_id: str
    confidence: int
    metric_results: tuple[SemanticMetricResultV2, ...]
    recorded_at: datetime
    contract_version: Literal[2] = 2

    def __post_init__(self) -> None:
        if self.contract_version != 2:
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_v2_contract_version_invalid"
            )
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_subject_invalid"
            )
        for field_name in (
            "receipt_id",
            "guideline_id",
            "guideline_revision_id",
            "binding_id",
            "assessment_assessor_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required(
                    getattr(self, field_name),
                    f"semantic_assessment_receipt_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "binding_revision",
            _positive_int(
                self.binding_revision,
                "semantic_assessment_receipt_binding_revision_invalid",
            ),
        )
        if (
            not isinstance(self.confidence, int)
            or isinstance(self.confidence, bool)
            or not 0 <= self.confidence <= 100
        ):
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_confidence_invalid"
            )
        for field_name in (
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
                    f"semantic_assessment_receipt_{field_name}_invalid",
                ),
            )
        if (
            not isinstance(self.metric_results, tuple | list)
            or not self.metric_results
            or any(
                not isinstance(item, SemanticMetricResultV2)
                for item in self.metric_results
            )
        ):
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_metric_results_invalid"
            )
        results = tuple(
            sorted(self.metric_results, key=lambda item: item.metric_result_id)
        )
        if len({item.metric_result_id for item in results}) != len(results):
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_metric_result_duplicate"
            )
        if any(
            item.receipt_id != self.receipt_id or item.subject != self.subject
            for item in results
        ):
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_metric_result_scope_mismatch"
            )
        object.__setattr__(self, "metric_results", results)
        object.__setattr__(
            self,
            "recorded_at",
            _aware_utc(
                self.recorded_at,
                "semantic_assessment_receipt_recorded_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class SemanticMetricFindingV2:
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
    pinpoints: tuple[SemanticPinpointV2, ...]
    created_at: datetime
    finding_digest: str | None = None
    contract_version: Literal[2] = 2

    def __post_init__(self) -> None:
        if self.contract_version != 2:
            raise SemanticAssessmentContractError(
                "semantic_metric_finding_v2_contract_version_invalid"
            )
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError(
                "semantic_metric_finding_subject_invalid"
            )
        for field_name in (
            "finding_id",
            "metric_result_id",
            "receipt_id",
            "guideline_id",
            "guideline_revision_id",
            "binding_id",
            "metric_id",
            "metric_code",
        ):
            object.__setattr__(
                self,
                field_name,
                _required(
                    getattr(self, field_name),
                    f"semantic_metric_finding_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "binding_revision",
            _positive_int(
                self.binding_revision,
                "semantic_metric_finding_binding_revision_invalid",
            ),
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
        object.__setattr__(
            self,
            "rationale",
            _required(
                self.rationale,
                "semantic_metric_finding_rationale_required",
                20_000,
            ),
        )
        if (
            not isinstance(self.evidence_refs, tuple | list)
            or not self.evidence_refs
            or any(not isinstance(item, EvidenceRef) for item in self.evidence_refs)
        ):
            raise SemanticAssessmentContractError(
                "semantic_metric_finding_evidence_invalid"
            )
        if (
            not isinstance(self.pinpoints, tuple | list)
            or not self.pinpoints
            or any(not isinstance(item, SemanticPinpointV2) for item in self.pinpoints)
        ):
            raise SemanticAssessmentContractError(
                "semantic_metric_finding_pinpoints_invalid"
            )
        object.__setattr__(self, "evidence_refs", tuple(self.evidence_refs))
        object.__setattr__(
            self,
            "pinpoints",
            tuple(sorted(self.pinpoints, key=lambda item: item.pinpoint_key)),
        )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "semantic_metric_finding_created_at_invalid",
            ),
        )
        expected = semantic_metric_finding_digest_v2(self)
        if (
            self.finding_digest is not None
            and _sha256(
                self.finding_digest,
                "semantic_metric_finding_digest_invalid",
            )
            != expected
        ):
            raise SemanticAssessmentContractError(
                "semantic_metric_finding_digest_mismatch"
            )
        object.__setattr__(self, "finding_digest", expected)


def _finding_payload(finding: SemanticMetricFindingV2) -> dict[str, object]:
    return {
        "contract_version": finding.contract_version,
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
            **(
                {"subject_edition": finding.subject.subject_edition}
                if finding.subject.subject_edition is not None
                else {}
            ),
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
            "configuration_digest": finding.binding_configuration_digest,
        },
        "metric_id": finding.metric_id,
        "metric_code": finding.metric_code,
        "rationale": finding.rationale,
        "evidence_refs": [_evidence_payload(item) for item in finding.evidence_refs],
        "pinpoints": [
            item.digest_payload(outcome=SemanticMetricOutcome.FAIL)
            for item in finding.pinpoints
        ],
        "created_at": finding.created_at.isoformat(),
    }


def semantic_metric_finding_digest_v2(
    finding: SemanticMetricFindingV2,
) -> str:
    if not isinstance(finding, SemanticMetricFindingV2):
        raise SemanticAssessmentContractError("semantic_metric_finding_v2_invalid")
    return semantic_finding_digest_v2(_finding_payload(finding))


def project_semantic_metric_findings_v2(
    receipt: SemanticAssessmentReceiptProjectionV2,
) -> tuple[SemanticMetricFindingV2, ...]:
    if not isinstance(receipt, SemanticAssessmentReceiptProjectionV2):
        raise SemanticAssessmentContractError(
            "semantic_metric_findings_v2_receipt_invalid"
        )
    findings: list[SemanticMetricFindingV2] = []
    for result in receipt.metric_results:
        if result.outcome is not SemanticMetricOutcome.FAIL:
            continue
        result_digest = semantic_metric_result_digest_v2(result)
        finding_id = canonical_sha256(
            {
                "contract": SEMANTIC_METRIC_FINDING_ID_VERSION_V2,
                "receipt_id": receipt.receipt_id,
                "metric_code": result.metric_code,
                "metric_result_digest": result_digest,
            }
        )
        findings.append(
            SemanticMetricFindingV2(
                finding_id=finding_id,
                metric_result_id=result.metric_result_id,
                metric_result_digest=result_digest,
                receipt_id=receipt.receipt_id,
                receipt_digest=receipt.receipt_digest,
                subject=receipt.subject,
                subject_content_digest=receipt.subject_content_digest,
                guideline_id=receipt.guideline_id,
                guideline_revision_id=receipt.guideline_revision_id,
                guideline_revision_digest=receipt.guideline_revision_digest,
                binding_id=receipt.binding_id,
                binding_revision=receipt.binding_revision,
                binding_configuration_digest=(receipt.binding_configuration_digest),
                metric_id=result.metric_id,
                metric_code=result.metric_code,
                rationale=result.rationale,
                evidence_refs=result.evidence_refs,
                pinpoints=result.pinpoints,
                created_at=receipt.recorded_at,
            )
        )
    return tuple(findings)


@dataclass(frozen=True, slots=True)
class SemanticMetricWaiverAnchorV2:
    finding_id: str
    finding_digest: str
    metric_result_id: str
    metric_result_digest: str
    receipt_id: str
    receipt_digest: str
    subject: PolicySubjectRef
    contract_version: Literal[2] = 2

    def __post_init__(self) -> None:
        if self.contract_version != 2:
            raise SemanticAssessmentContractError(
                "semantic_waiver_anchor_v2_contract_version_invalid"
            )
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError(
                "semantic_waiver_anchor_subject_invalid"
            )
        for field_name in ("finding_id", "metric_result_id", "receipt_id"):
            object.__setattr__(
                self,
                field_name,
                _required(
                    getattr(self, field_name),
                    f"semantic_waiver_anchor_{field_name}_required",
                ),
            )
        for field_name in (
            "finding_digest",
            "metric_result_digest",
            "receipt_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"semantic_waiver_anchor_{field_name}_invalid",
                ),
            )

    @classmethod
    def from_finding(
        cls,
        finding: SemanticMetricFindingV2,
    ) -> SemanticMetricWaiverAnchorV2:
        if not isinstance(finding, SemanticMetricFindingV2):
            raise SemanticAssessmentContractError("semantic_waiver_finding_v2_invalid")
        return cls(
            finding_id=finding.finding_id,
            finding_digest=finding.finding_digest,
            metric_result_id=finding.metric_result_id,
            metric_result_digest=finding.metric_result_digest,
            receipt_id=finding.receipt_id,
            receipt_digest=finding.receipt_digest,
            subject=finding.subject,
        )


class SemanticWaiverApplicability(str, Enum):
    APPLICABLE = "applicable"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True, slots=True)
class SemanticWaiverApplicabilityResult:
    status: SemanticWaiverApplicability
    reason_code: str | None


def evaluate_semantic_waiver_applicability_v2(
    anchor: SemanticMetricWaiverAnchorV2,
    finding: SemanticMetricFindingV2,
) -> SemanticWaiverApplicabilityResult:
    if not isinstance(anchor, SemanticMetricWaiverAnchorV2):
        raise SemanticAssessmentContractError("semantic_waiver_anchor_v2_invalid")
    if not isinstance(finding, SemanticMetricFindingV2):
        raise SemanticAssessmentContractError("semantic_waiver_finding_v2_invalid")
    if anchor == SemanticMetricWaiverAnchorV2.from_finding(finding):
        return SemanticWaiverApplicabilityResult(
            status=SemanticWaiverApplicability.APPLICABLE,
            reason_code=None,
        )
    return SemanticWaiverApplicabilityResult(
        status=SemanticWaiverApplicability.NOT_APPLICABLE,
        reason_code=SEMANTIC_WAIVER_FINDING_IDENTITY_MISMATCH,
    )


__all__ = [
    "SEMANTIC_METRIC_FINDING_ID_VERSION_V2",
    "SEMANTIC_WAIVER_FINDING_IDENTITY_MISMATCH",
    "SemanticAssessmentReceiptProjectionV2",
    "SemanticMetricFindingV2",
    "SemanticMetricWaiverAnchorV2",
    "SemanticWaiverApplicability",
    "SemanticWaiverApplicabilityResult",
    "evaluate_semantic_waiver_applicability_v2",
    "project_semantic_metric_findings_v2",
    "semantic_metric_finding_digest_v2",
]
