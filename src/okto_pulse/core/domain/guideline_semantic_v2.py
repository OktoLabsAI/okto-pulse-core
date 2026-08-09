"""Actionable semantic guideline pinpoints and canonical v2 digests.

This module is deliberately additive.  The persisted v1 assessment, result,
finding and waiver contracts remain owned by their existing modules and are
never imported or rewritten here.  V2 adds human-readable, sealed pinpoint
snapshots under independent canonical namespaces.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from enum import Enum
from typing import Literal

from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_BINDING_ID_MAX_LENGTH,
    GUIDELINE_REVISION_ID_MAX_LENGTH,
    POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
    POLICY_METRIC_CODE_MAX_LENGTH,
    POLICY_METRIC_ID_MAX_LENGTH,
    POLICY_RECEIPT_ID_MAX_LENGTH,
    GuidelineMetricDirection,
    PolicySubjectRef,
    normalize_policy_bounded_text,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentAssessor,
    SemanticAssessmentContractError,
    SemanticMetricOutcome,
    SemanticThresholdSource,
)
from okto_pulse.core.domain.quality_assessment import (
    EvidenceRef,
    FindingSeverity,
    UnboundFindingAnchor,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256


SEMANTIC_ASSESSMENT_REQUEST_DIGEST_VERSION_V2 = (
    "semantic-guideline-assessment-request/v2"
)
SEMANTIC_ASSESSMENT_RECEIPT_DIGEST_VERSION_V2 = (
    "semantic-guideline-assessment-receipt/v2"
)
SEMANTIC_METRIC_RESULT_DIGEST_VERSION_V2 = "semantic-metric-result/v2"
SEMANTIC_METRIC_FINDING_DIGEST_VERSION_V2 = "semantic-metric-finding/v2"

SEMANTIC_PINPOINT_KEY_MAX_LENGTH = 200
SEMANTIC_PINPOINT_TITLE_MAX_LENGTH = 500
SEMANTIC_PINPOINT_DETAIL_MAX_LENGTH = 20_000
SEMANTIC_PINPOINT_REMEDIATION_MAX_LENGTH = 10_000
SEMANTIC_ANCHOR_LABEL_MAX_LENGTH = 500
SEMANTIC_ANCHOR_EXCERPT_MAX_LENGTH = 8_000
SEMANTIC_ANCHOR_SOURCE_VERSION_MAX_LENGTH = 200

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class SemanticPinpointKind(str, Enum):
    EVIDENCE = "evidence"
    ISSUE = "issue"


class SemanticAnchorAvailability(str, Enum):
    AVAILABLE = "available"
    REMOVED = "removed"
    INACCESSIBLE = "inaccessible"


def _nfc_text(value: object, *, max_length: int, code: str) -> str:
    if not isinstance(value, str):
        raise SemanticAssessmentContractError(code)
    normalized = unicodedata.normalize(
        "NFC", value.replace("\r\n", "\n").replace("\r", "\n")
    ).strip()
    if not normalized or len(normalized) > max_length:
        raise SemanticAssessmentContractError(code)
    return normalized


def _optional_nfc_text(
    value: object,
    *,
    max_length: int,
    code: str,
) -> str | None:
    if value is None:
        return None
    return _nfc_text(value, max_length=max_length, code=code)


def _sha256(value: object, code: str) -> str:
    if not isinstance(value, str):
        raise SemanticAssessmentContractError(code)
    normalized = value.strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise SemanticAssessmentContractError(code)
    return normalized


def _score(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= 100:
        raise SemanticAssessmentContractError(code)
    return value


def _positive_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise SemanticAssessmentContractError(code)
    return value


@dataclass(frozen=True, slots=True)
class AnchorSnapshot:
    """Human-safe anchor content sealed while the subject was authorized."""

    label: str
    source_version: str
    availability_at_seal: SemanticAnchorAvailability
    excerpt: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "label",
            _nfc_text(
                self.label,
                max_length=SEMANTIC_ANCHOR_LABEL_MAX_LENGTH,
                code="semantic_anchor_snapshot_label_required",
            ),
        )
        object.__setattr__(
            self,
            "source_version",
            _nfc_text(
                self.source_version,
                max_length=SEMANTIC_ANCHOR_SOURCE_VERSION_MAX_LENGTH,
                code="semantic_anchor_snapshot_source_version_required",
            ),
        )
        if not isinstance(self.availability_at_seal, SemanticAnchorAvailability):
            raise SemanticAssessmentContractError(
                "semantic_anchor_snapshot_availability_invalid"
            )
        object.__setattr__(
            self,
            "excerpt",
            _optional_nfc_text(
                self.excerpt,
                max_length=SEMANTIC_ANCHOR_EXCERPT_MAX_LENGTH,
                code="semantic_anchor_snapshot_excerpt_invalid",
            ),
        )
        if (
            self.availability_at_seal is SemanticAnchorAvailability.INACCESSIBLE
            and self.excerpt is not None
        ):
            raise SemanticAssessmentContractError(
                "semantic_anchor_snapshot_inaccessible_excerpt_forbidden"
            )

    def digest_payload(self) -> dict[str, object]:
        return {
            "label": self.label,
            "excerpt": self.excerpt,
            "source_version": self.source_version,
            "availability_at_seal": self.availability_at_seal.value,
        }


@dataclass(frozen=True, slots=True)
class SemanticPinpointV2:
    """Actionable semantic explanation bound to one technical anchor."""

    pinpoint_key: str
    kind: SemanticPinpointKind
    title: str
    detail: str
    anchor: UnboundFindingAnchor
    anchor_snapshot: AnchorSnapshot
    severity: FindingSeverity | None = None
    remediation: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "pinpoint_key",
            _nfc_text(
                self.pinpoint_key,
                max_length=SEMANTIC_PINPOINT_KEY_MAX_LENGTH,
                code="semantic_pinpoint_v2_key_required",
            ),
        )
        if not isinstance(self.kind, SemanticPinpointKind):
            raise SemanticAssessmentContractError("semantic_pinpoint_v2_kind_invalid")
        object.__setattr__(
            self,
            "title",
            _nfc_text(
                self.title,
                max_length=SEMANTIC_PINPOINT_TITLE_MAX_LENGTH,
                code="semantic_pinpoint_v2_title_required",
            ),
        )
        object.__setattr__(
            self,
            "detail",
            _nfc_text(
                self.detail,
                max_length=SEMANTIC_PINPOINT_DETAIL_MAX_LENGTH,
                code="semantic_pinpoint_v2_detail_required",
            ),
        )
        if not isinstance(self.anchor, UnboundFindingAnchor):
            raise SemanticAssessmentContractError("semantic_pinpoint_v2_anchor_invalid")
        if not isinstance(self.anchor_snapshot, AnchorSnapshot):
            raise SemanticAssessmentContractError(
                "semantic_pinpoint_v2_anchor_snapshot_invalid"
            )
        if self.severity is not None and not isinstance(self.severity, FindingSeverity):
            raise SemanticAssessmentContractError(
                "semantic_pinpoint_v2_severity_invalid"
            )
        if self.kind is SemanticPinpointKind.ISSUE and self.severity is None:
            raise SemanticAssessmentContractError(
                "semantic_pinpoint_v2_issue_severity_required"
            )
        object.__setattr__(
            self,
            "remediation",
            _optional_nfc_text(
                self.remediation,
                max_length=SEMANTIC_PINPOINT_REMEDIATION_MAX_LENGTH,
                code="semantic_pinpoint_v2_remediation_invalid",
            ),
        )

    def blocking_for(self, outcome: SemanticMetricOutcome) -> bool:
        if not isinstance(outcome, SemanticMetricOutcome):
            raise SemanticAssessmentContractError(
                "semantic_pinpoint_v2_outcome_invalid"
            )
        return (
            self.kind is SemanticPinpointKind.ISSUE
            and outcome is SemanticMetricOutcome.FAIL
        )

    def digest_payload(
        self,
        *,
        outcome: SemanticMetricOutcome | None = None,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "pinpoint_key": self.pinpoint_key,
            "kind": self.kind.value,
            "title": self.title,
            "detail": self.detail,
            "severity": self.severity.value if self.severity else None,
            "remediation": self.remediation,
            "anchor": {
                "anchor_type": self.anchor.anchor_type.value,
                "anchor_ref": self.anchor.anchor_ref,
                "excerpt_hash": self.anchor.excerpt_hash,
            },
            "anchor_snapshot": self.anchor_snapshot.digest_payload(),
        }
        if outcome is not None:
            payload["blocking"] = self.blocking_for(outcome)
        return payload


@dataclass(frozen=True, slots=True)
class SemanticPinpointDraftV2:
    """Client-authored semantic content before Core seals an anchor snapshot."""

    pinpoint_key: str
    kind: SemanticPinpointKind
    title: str
    detail: str
    anchor: UnboundFindingAnchor
    severity: FindingSeverity | None = None
    remediation: str | None = None
    contract_version: Literal[2] = 2

    def __post_init__(self) -> None:
        if self.contract_version != 2:
            raise SemanticAssessmentContractError(
                "semantic_pinpoint_v2_contract_version_invalid"
            )
        object.__setattr__(
            self,
            "pinpoint_key",
            _nfc_text(
                self.pinpoint_key,
                max_length=SEMANTIC_PINPOINT_KEY_MAX_LENGTH,
                code="semantic_pinpoint_v2_key_required",
            ),
        )
        if not isinstance(self.kind, SemanticPinpointKind):
            raise SemanticAssessmentContractError("semantic_pinpoint_v2_kind_invalid")
        object.__setattr__(
            self,
            "title",
            _nfc_text(
                self.title,
                max_length=SEMANTIC_PINPOINT_TITLE_MAX_LENGTH,
                code="semantic_pinpoint_v2_title_required",
            ),
        )
        object.__setattr__(
            self,
            "detail",
            _nfc_text(
                self.detail,
                max_length=SEMANTIC_PINPOINT_DETAIL_MAX_LENGTH,
                code="semantic_pinpoint_v2_detail_required",
            ),
        )
        if not isinstance(self.anchor, UnboundFindingAnchor):
            raise SemanticAssessmentContractError("semantic_pinpoint_v2_anchor_invalid")
        if self.severity is not None and not isinstance(self.severity, FindingSeverity):
            raise SemanticAssessmentContractError(
                "semantic_pinpoint_v2_severity_invalid"
            )
        if self.kind is SemanticPinpointKind.ISSUE and self.severity is None:
            raise SemanticAssessmentContractError(
                "semantic_pinpoint_v2_issue_severity_required"
            )
        object.__setattr__(
            self,
            "remediation",
            _optional_nfc_text(
                self.remediation,
                max_length=SEMANTIC_PINPOINT_REMEDIATION_MAX_LENGTH,
                code="semantic_pinpoint_v2_remediation_invalid",
            ),
        )

    def seal(self, snapshot: AnchorSnapshot) -> SemanticPinpointV2:
        return SemanticPinpointV2(
            pinpoint_key=self.pinpoint_key,
            kind=self.kind,
            title=self.title,
            detail=self.detail,
            severity=self.severity,
            remediation=self.remediation,
            anchor=self.anchor,
            anchor_snapshot=snapshot,
        )


def normalize_semantic_pinpoints_v2(
    value: object,
    *,
    outcome: SemanticMetricOutcome | None = None,
) -> tuple[SemanticPinpointV2, ...]:
    if not isinstance(value, tuple | list) or not value:
        raise SemanticAssessmentContractError("semantic_pinpoints_v2_invalid")
    pinpoints = tuple(value)
    if any(not isinstance(item, SemanticPinpointV2) for item in pinpoints):
        raise SemanticAssessmentContractError("semantic_pinpoints_v2_invalid")
    if len({item.pinpoint_key for item in pinpoints}) != len(pinpoints):
        raise SemanticAssessmentContractError("semantic_pinpoints_v2_key_duplicate")
    if outcome is not None:
        if not isinstance(outcome, SemanticMetricOutcome):
            raise SemanticAssessmentContractError(
                "semantic_pinpoints_v2_outcome_invalid"
            )
        if outcome is SemanticMetricOutcome.FAIL and not any(
            item.kind is SemanticPinpointKind.ISSUE for item in pinpoints
        ):
            raise SemanticAssessmentContractError(
                "semantic_pinpoints_v2_failed_issue_required"
            )
    return tuple(sorted(pinpoints, key=lambda item: item.pinpoint_key))


@dataclass(frozen=True, slots=True)
class SemanticMetricAssessmentV2:
    metric_id: str
    score: int
    rationale: str
    evidence_refs: tuple[EvidenceRef, ...]
    pinpoints: tuple[SemanticPinpointV2, ...]
    contract_version: Literal[2] = 2

    def __post_init__(self) -> None:
        if self.contract_version != 2:
            raise SemanticAssessmentContractError(
                "semantic_metric_assessment_v2_contract_version_invalid"
            )
        object.__setattr__(
            self,
            "metric_id",
            normalize_policy_bounded_text(
                self.metric_id,
                max_length=POLICY_METRIC_ID_MAX_LENGTH,
                code="semantic_metric_assessment_metric_id_required",
            ),
        )
        object.__setattr__(
            self,
            "score",
            _score(self.score, "semantic_metric_assessment_score_invalid"),
        )
        object.__setattr__(
            self,
            "rationale",
            _nfc_text(
                self.rationale,
                max_length=20_000,
                code="semantic_metric_assessment_rationale_required",
            ),
        )
        if (
            not isinstance(self.evidence_refs, tuple | list)
            or not self.evidence_refs
            or any(not isinstance(item, EvidenceRef) for item in self.evidence_refs)
        ):
            raise SemanticAssessmentContractError(
                "semantic_metric_assessment_evidence_refs_invalid"
            )
        evidence = tuple(
            sorted(
                self.evidence_refs,
                key=lambda item: (
                    item.source_type,
                    item.source_id,
                    item.source_version,
                    item.content_hash,
                ),
            )
        )
        if len(set(evidence)) != len(evidence):
            raise SemanticAssessmentContractError(
                "semantic_metric_assessment_evidence_refs_duplicate"
            )
        object.__setattr__(self, "evidence_refs", evidence)
        object.__setattr__(
            self,
            "pinpoints",
            normalize_semantic_pinpoints_v2(self.pinpoints),
        )


@dataclass(frozen=True, slots=True)
class SemanticMetricAssessmentDraftV2:
    metric_id: str
    score: int
    rationale: str
    evidence_refs: tuple[EvidenceRef, ...]
    pinpoints: tuple[SemanticPinpointDraftV2, ...]
    contract_version: Literal[2] = 2

    def __post_init__(self) -> None:
        if self.contract_version != 2:
            raise SemanticAssessmentContractError(
                "semantic_metric_assessment_v2_contract_version_invalid"
            )
        object.__setattr__(
            self,
            "metric_id",
            normalize_policy_bounded_text(
                self.metric_id,
                max_length=POLICY_METRIC_ID_MAX_LENGTH,
                code="semantic_metric_assessment_metric_id_required",
            ),
        )
        object.__setattr__(
            self,
            "score",
            _score(self.score, "semantic_metric_assessment_score_invalid"),
        )
        object.__setattr__(
            self,
            "rationale",
            _nfc_text(
                self.rationale,
                max_length=20_000,
                code="semantic_metric_assessment_rationale_required",
            ),
        )
        if (
            not isinstance(self.evidence_refs, tuple | list)
            or not self.evidence_refs
            or any(not isinstance(item, EvidenceRef) for item in self.evidence_refs)
        ):
            raise SemanticAssessmentContractError(
                "semantic_metric_assessment_evidence_refs_invalid"
            )
        object.__setattr__(
            self,
            "evidence_refs",
            tuple(
                sorted(
                    self.evidence_refs,
                    key=lambda item: (
                        item.source_type,
                        item.source_id,
                        item.source_version,
                        item.content_hash,
                    ),
                )
            ),
        )
        if (
            not isinstance(self.pinpoints, tuple | list)
            or not self.pinpoints
            or any(
                not isinstance(item, SemanticPinpointDraftV2) for item in self.pinpoints
            )
        ):
            raise SemanticAssessmentContractError("semantic_pinpoints_v2_invalid")
        pinpoints = tuple(sorted(self.pinpoints, key=lambda item: item.pinpoint_key))
        if len({item.pinpoint_key for item in pinpoints}) != len(pinpoints):
            raise SemanticAssessmentContractError("semantic_pinpoints_v2_key_duplicate")
        object.__setattr__(self, "pinpoints", pinpoints)


@dataclass(frozen=True, slots=True)
class SemanticAssessmentDraftV2:
    subject: PolicySubjectRef
    binding_id: str
    expected_binding_revision: int
    guideline_revision_id: str
    idempotency_key: str
    confidence: int
    assessor: SemanticAssessmentAssessor
    metric_results: tuple[SemanticMetricAssessmentDraftV2, ...]
    contract_version: Literal[2] = 2

    def __post_init__(self) -> None:
        if self.contract_version != 2:
            raise SemanticAssessmentContractError(
                "semantic_assessment_v2_contract_version_invalid"
            )
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError("semantic_assessment_subject_invalid")
        if not isinstance(self.assessor, SemanticAssessmentAssessor):
            raise SemanticAssessmentContractError(
                "semantic_assessment_assessor_invalid"
            )
        object.__setattr__(
            self,
            "binding_id",
            normalize_policy_bounded_text(
                self.binding_id,
                max_length=GUIDELINE_BINDING_ID_MAX_LENGTH,
                code="semantic_assessment_binding_id_required",
            ),
        )
        object.__setattr__(
            self,
            "expected_binding_revision",
            _positive_int(
                self.expected_binding_revision,
                "semantic_assessment_expected_binding_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "guideline_revision_id",
            normalize_policy_bounded_text(
                self.guideline_revision_id,
                max_length=GUIDELINE_REVISION_ID_MAX_LENGTH,
                code="semantic_assessment_guideline_revision_id_required",
            ),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            normalize_policy_bounded_text(
                self.idempotency_key,
                max_length=POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                code="semantic_assessment_idempotency_key_required",
            ),
        )
        object.__setattr__(
            self,
            "confidence",
            _score(self.confidence, "semantic_assessment_confidence_invalid"),
        )
        if (
            not isinstance(self.metric_results, tuple | list)
            or not self.metric_results
            or any(
                not isinstance(item, SemanticMetricAssessmentDraftV2)
                for item in self.metric_results
            )
        ):
            raise SemanticAssessmentContractError(
                "semantic_assessment_metric_results_invalid"
            )
        results = tuple(sorted(self.metric_results, key=lambda item: item.metric_id))
        if len({item.metric_id for item in results}) != len(results):
            raise SemanticAssessmentContractError(
                "semantic_assessment_metric_result_duplicate"
            )
        object.__setattr__(self, "metric_results", results)


@dataclass(frozen=True, slots=True)
class SemanticAssessmentRequestV2:
    subject: PolicySubjectRef
    binding_id: str
    expected_binding_revision: int
    guideline_revision_id: str
    idempotency_key: str
    confidence: int
    assessor: SemanticAssessmentAssessor
    metric_results: tuple[SemanticMetricAssessmentV2, ...]
    contract_version: Literal[2] = 2

    def __post_init__(self) -> None:
        if self.contract_version != 2:
            raise SemanticAssessmentContractError(
                "semantic_assessment_v2_contract_version_invalid"
            )
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError("semantic_assessment_subject_invalid")
        if not isinstance(self.assessor, SemanticAssessmentAssessor):
            raise SemanticAssessmentContractError(
                "semantic_assessment_assessor_invalid"
            )
        object.__setattr__(
            self,
            "binding_id",
            normalize_policy_bounded_text(
                self.binding_id,
                max_length=GUIDELINE_BINDING_ID_MAX_LENGTH,
                code="semantic_assessment_binding_id_required",
            ),
        )
        object.__setattr__(
            self,
            "expected_binding_revision",
            _positive_int(
                self.expected_binding_revision,
                "semantic_assessment_expected_binding_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "guideline_revision_id",
            normalize_policy_bounded_text(
                self.guideline_revision_id,
                max_length=GUIDELINE_REVISION_ID_MAX_LENGTH,
                code="semantic_assessment_guideline_revision_id_required",
            ),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            normalize_policy_bounded_text(
                self.idempotency_key,
                max_length=POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                code="semantic_assessment_idempotency_key_required",
            ),
        )
        object.__setattr__(
            self,
            "confidence",
            _score(self.confidence, "semantic_assessment_confidence_invalid"),
        )
        if (
            not isinstance(self.metric_results, tuple | list)
            or not self.metric_results
            or any(
                not isinstance(item, SemanticMetricAssessmentV2)
                for item in self.metric_results
            )
        ):
            raise SemanticAssessmentContractError(
                "semantic_assessment_metric_results_invalid"
            )
        results = tuple(sorted(self.metric_results, key=lambda item: item.metric_id))
        if len({item.metric_id for item in results}) != len(results):
            raise SemanticAssessmentContractError(
                "semantic_assessment_metric_result_duplicate"
            )
        object.__setattr__(self, "metric_results", results)


@dataclass(frozen=True, slots=True)
class SemanticMetricResultV2:
    metric_result_id: str
    receipt_id: str
    subject: PolicySubjectRef
    binding_id: str
    guideline_id: str
    revision_id: str
    metric_id: str
    metric_code: str
    metric_definition_digest: str
    score: int
    direction: GuidelineMetricDirection
    default_threshold: int
    effective_threshold: int
    threshold_source: SemanticThresholdSource
    outcome: SemanticMetricOutcome
    rationale: str
    evidence_refs: tuple[EvidenceRef, ...]
    pinpoints: tuple[SemanticPinpointV2, ...]
    contract_version: Literal[2] = 2

    def __post_init__(self) -> None:
        if self.contract_version != 2:
            raise SemanticAssessmentContractError(
                "semantic_metric_result_v2_contract_version_invalid"
            )
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError(
                "semantic_metric_result_subject_invalid"
            )
        for field_name, max_length in (
            ("metric_result_id", POLICY_RECEIPT_ID_MAX_LENGTH),
            ("receipt_id", POLICY_RECEIPT_ID_MAX_LENGTH),
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
                    code=f"semantic_metric_result_{field_name}_required",
                ),
            )
        for field_name in ("guideline_id", "revision_id"):
            object.__setattr__(
                self,
                field_name,
                _nfc_text(
                    getattr(self, field_name),
                    max_length=500,
                    code=f"semantic_metric_result_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "metric_definition_digest",
            _sha256(
                self.metric_definition_digest,
                "semantic_metric_result_definition_digest_invalid",
            ),
        )
        for field_name in ("score", "default_threshold", "effective_threshold"):
            object.__setattr__(
                self,
                field_name,
                _score(
                    getattr(self, field_name),
                    f"semantic_metric_result_{field_name}_invalid",
                ),
            )
        if not isinstance(self.direction, GuidelineMetricDirection):
            raise SemanticAssessmentContractError(
                "semantic_metric_result_direction_invalid"
            )
        if not isinstance(self.threshold_source, SemanticThresholdSource):
            raise SemanticAssessmentContractError(
                "semantic_metric_result_threshold_source_invalid"
            )
        if not isinstance(self.outcome, SemanticMetricOutcome):
            raise SemanticAssessmentContractError(
                "semantic_metric_result_outcome_invalid"
            )
        expected = (
            SemanticMetricOutcome.PASS
            if (
                self.score >= self.effective_threshold
                if self.direction is GuidelineMetricDirection.MINIMUM
                else self.score <= self.effective_threshold
            )
            else SemanticMetricOutcome.FAIL
        )
        if self.outcome is not expected:
            raise SemanticAssessmentContractError(
                "semantic_metric_result_outcome_inconsistent"
            )
        object.__setattr__(
            self,
            "rationale",
            _nfc_text(
                self.rationale,
                max_length=20_000,
                code="semantic_metric_result_rationale_required",
            ),
        )
        if (
            not isinstance(self.evidence_refs, tuple | list)
            or not self.evidence_refs
            or any(not isinstance(item, EvidenceRef) for item in self.evidence_refs)
        ):
            raise SemanticAssessmentContractError(
                "semantic_metric_result_evidence_refs_invalid"
            )
        object.__setattr__(
            self,
            "evidence_refs",
            tuple(
                sorted(
                    self.evidence_refs,
                    key=lambda item: (
                        item.source_type,
                        item.source_id,
                        item.source_version,
                        item.content_hash,
                    ),
                )
            ),
        )
        object.__setattr__(
            self,
            "pinpoints",
            normalize_semantic_pinpoints_v2(
                self.pinpoints,
                outcome=self.outcome,
            ),
        )


def _evidence_payload(item: EvidenceRef) -> dict[str, object]:
    return {
        "source_type": item.source_type,
        "source_id": item.source_id,
        "source_version": item.source_version,
        "content_hash": item.content_hash,
    }


def semantic_assessment_request_digest_v2(
    request: SemanticAssessmentRequestV2,
) -> str:
    if not isinstance(request, SemanticAssessmentRequestV2):
        raise SemanticAssessmentContractError("semantic_assessment_request_v2_invalid")
    return canonical_sha256(
        {
            "contract": SEMANTIC_ASSESSMENT_REQUEST_DIGEST_VERSION_V2,
            "contract_version": request.contract_version,
            "subject": {
                "board_id": request.subject.board_id,
                "subject_type": request.subject.entity_type.value,
                "subject_id": request.subject.subject_id,
                "subject_version": request.subject.subject_version,
            },
            "binding_id": request.binding_id,
            "expected_binding_revision": request.expected_binding_revision,
            "guideline_revision_id": request.guideline_revision_id,
            "idempotency_key": request.idempotency_key,
            "confidence": request.confidence,
            "assessor": {
                "agent_id": request.assessor.agent_id,
                "model_id": request.assessor.model_id,
            },
            "metric_results": [
                {
                    "contract_version": item.contract_version,
                    "metric_id": item.metric_id,
                    "score": item.score,
                    "rationale": item.rationale,
                    "evidence_refs": [
                        _evidence_payload(evidence) for evidence in item.evidence_refs
                    ],
                    "pinpoints": [
                        pinpoint.digest_payload() for pinpoint in item.pinpoints
                    ],
                }
                for item in request.metric_results
            ],
        }
    )


def semantic_metric_result_digest_v2(result: SemanticMetricResultV2) -> str:
    if not isinstance(result, SemanticMetricResultV2):
        raise SemanticAssessmentContractError("semantic_metric_result_v2_invalid")
    return canonical_sha256(
        {
            "contract": SEMANTIC_METRIC_RESULT_DIGEST_VERSION_V2,
            "contract_version": result.contract_version,
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
            "evidence_refs": [_evidence_payload(item) for item in result.evidence_refs],
            "pinpoints": [
                item.digest_payload(outcome=result.outcome) for item in result.pinpoints
            ],
        }
    )


def semantic_receipt_digest_v2(payload: dict[str, object]) -> str:
    """Seal an already validated receipt projection in the v2 namespace.

    The application layer constructs the exact projection; this leaf rejects
    recursive or cross-version payloads and canonicalizes the complete body.
    """

    if not isinstance(payload, dict) or payload.get("contract_version") != 2:
        raise SemanticAssessmentContractError("semantic_assessment_receipt_v2_invalid")
    if "receipt_digest" in payload:
        raise SemanticAssessmentContractError(
            "semantic_assessment_receipt_v2_digest_recursive"
        )
    return canonical_sha256(
        {
            "contract": SEMANTIC_ASSESSMENT_RECEIPT_DIGEST_VERSION_V2,
            **payload,
        }
    )


def semantic_finding_digest_v2(payload: dict[str, object]) -> str:
    """Seal one validated v2 finding without its own digest field."""

    if not isinstance(payload, dict) or payload.get("contract_version") != 2:
        raise SemanticAssessmentContractError("semantic_metric_finding_v2_invalid")
    if "finding_digest" in payload:
        raise SemanticAssessmentContractError(
            "semantic_metric_finding_v2_digest_recursive"
        )
    return canonical_sha256(
        {
            "contract": SEMANTIC_METRIC_FINDING_DIGEST_VERSION_V2,
            **payload,
        }
    )


__all__ = [
    "AnchorSnapshot",
    "SEMANTIC_ASSESSMENT_RECEIPT_DIGEST_VERSION_V2",
    "SEMANTIC_ASSESSMENT_REQUEST_DIGEST_VERSION_V2",
    "SEMANTIC_METRIC_FINDING_DIGEST_VERSION_V2",
    "SEMANTIC_METRIC_RESULT_DIGEST_VERSION_V2",
    "SemanticAnchorAvailability",
    "SemanticAssessmentRequestV2",
    "SemanticAssessmentDraftV2",
    "SemanticMetricAssessmentV2",
    "SemanticMetricAssessmentDraftV2",
    "SemanticMetricResultV2",
    "SemanticPinpointKind",
    "SemanticPinpointDraftV2",
    "SemanticPinpointV2",
    "normalize_semantic_pinpoints_v2",
    "semantic_assessment_request_digest_v2",
    "semantic_finding_digest_v2",
    "semantic_metric_result_digest_v2",
    "semantic_receipt_digest_v2",
]
