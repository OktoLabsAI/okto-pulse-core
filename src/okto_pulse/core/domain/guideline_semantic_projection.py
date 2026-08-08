"""Closed read projections and keyset contracts for semantic guidelines.

The immutable semantic evidence domain is intentionally lossless.  Public
reads, however, must be explicit about how much evidence they expose.  This
module therefore uses distinct dataclasses for ``summary``, ``detail`` and
``full`` instead of nullable evidence fields.  A summary instance cannot leak
rationales, evidence references, pinpoints, idempotency keys or sealed
digests, even when a generic dataclass serializer is used.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import ClassVar, Generic, TypeVar

from okto_pulse.core.domain.guideline_policy import (
    GuidelineEnforcement,
    GuidelineMetricDirection,
    GuidelinePolicyContractError,
    PolicyCurrentness,
    PolicyEntityType,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentPinpoint,
    SemanticAssessmentState,
    SemanticGuidelineAssessmentReceipt,
    SemanticMetricOutcome,
    SemanticMetricResult,
    SemanticThresholdSource,
)
from okto_pulse.core.domain.guideline_semantic_currentness import (
    SemanticAssessmentCurrentSnapshot,
    SemanticAssessmentCurrentness,
    SemanticAssessmentCurrentnessReason,
    assess_semantic_assessment_currentness,
)
from okto_pulse.core.domain.guideline_semantic_exceptions import (
    SemanticMetricWaiver,
    SemanticMetricWaiverEventType,
    SemanticMetricWaiverExpireReason,
    SemanticMetricWaiverRevalidationReason,
    SemanticMetricWaiverRevalidationStatus,
    SemanticMetricWaiverStatus,
    SemanticPolicySkip,
    SemanticPolicySkipEventType,
    SemanticPolicySkipStatus,
)
from okto_pulse.core.domain.guideline_semantic_findings import (
    SemanticMetricFinding,
)
from okto_pulse.core.domain.quality_assessment import EvidenceRef


SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION = "semantic-guideline-keyset/v1"
SEMANTIC_GUIDELINE_PAGE_LIMIT_DEFAULT = 50
SEMANTIC_GUIDELINE_PAGE_LIMIT_MAX = 200

SEMANTIC_ASSESSMENT_ORDERING: tuple[str, str] = (
    "recorded_at DESC",
    "receipt_id DESC",
)
SEMANTIC_FINDING_ORDERING: tuple[str, str] = (
    "created_at DESC",
    "finding_id DESC",
)
SEMANTIC_WAIVER_ORDERING: tuple[str, str] = (
    "requested_at DESC",
    "waiver_id DESC",
)
SEMANTIC_SKIP_ORDERING: tuple[str, str] = (
    "created_at DESC",
    "skip_id DESC",
)


class SemanticGuidelineProjection(str, Enum):
    SUMMARY = "summary"
    DETAIL = "detail"
    FULL = "full"


def _exact_projection(
    value: object,
    expected: SemanticGuidelineProjection,
    code: str,
) -> None:
    if value is not expected:
        raise GuidelinePolicyContractError(code)


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GuidelinePolicyContractError(code)
    return value.strip()


def _sha256(value: object, code: str) -> str:
    normalized = _required_text(value, code).lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise GuidelinePolicyContractError(code)
    return normalized


def _aware_utc(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise GuidelinePolicyContractError(code)
    return value.astimezone(timezone.utc)


def _page_limit(value: object) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= SEMANTIC_GUIDELINE_PAGE_LIMIT_MAX
    ):
        raise GuidelinePolicyContractError("semantic_guideline_page_limit_invalid")
    return value


@dataclass(frozen=True, slots=True)
class SemanticEvidenceProjection:
    source_type: str
    source_id: str
    source_version: int
    content_hash: str


@dataclass(frozen=True, slots=True)
class SemanticPinpointProjection:
    anchor_type: str
    anchor_ref: str | None
    excerpt_hash: str | None
    input_digest: str


@dataclass(frozen=True, slots=True)
class SemanticMetricResultDetail:
    metric_result_id: str
    metric_id: str
    metric_code: str
    score: int
    direction: GuidelineMetricDirection
    default_threshold: int
    effective_threshold: int
    threshold_source: SemanticThresholdSource
    outcome: SemanticMetricOutcome
    rationale: str
    evidence_refs: tuple[SemanticEvidenceProjection, ...]
    pinpoints: tuple[SemanticPinpointProjection, ...]


@dataclass(frozen=True, slots=True)
class SemanticMetricResultFull(SemanticMetricResultDetail):
    metric_definition_digest: str


@dataclass(frozen=True, slots=True)
class SemanticAssessmentSummary:
    projection: SemanticGuidelineProjection
    receipt_id: str
    board_id: str
    entity_type: PolicyEntityType
    subject_id: str
    subject_version: int
    binding_id: str
    guideline_id: str
    guideline_revision_id: str
    enforcement: GuidelineEnforcement
    state: SemanticAssessmentState
    currentness: PolicyCurrentness
    currentness_reasons: tuple[SemanticAssessmentCurrentnessReason, ...]
    confidence: int
    minimum_confidence: int
    metric_count: int
    failed_metric_count: int
    recorded_at: datetime

    def __post_init__(self) -> None:
        _exact_projection(
            self.projection,
            SemanticGuidelineProjection.SUMMARY,
            "semantic_assessment_summary_projection_invalid",
        )


@dataclass(frozen=True, slots=True)
class SemanticAssessmentDetail(SemanticAssessmentSummary):
    binding_revision: int
    assessor_agent_id: str
    assessor_model_id: str | None
    assessor_independent: bool
    confidence_admissible: bool
    metric_results: tuple[
        SemanticMetricResultDetail | SemanticMetricResultFull,
        ...,
    ]

    def __post_init__(self) -> None:
        _exact_projection(
            self.projection,
            SemanticGuidelineProjection.DETAIL,
            "semantic_assessment_detail_projection_invalid",
        )
        if any(
            type(item) is not SemanticMetricResultDetail for item in self.metric_results
        ):
            raise GuidelinePolicyContractError(
                "semantic_assessment_detail_metric_projection_invalid"
            )


@dataclass(frozen=True, slots=True)
class SemanticAssessmentFull(SemanticAssessmentDetail):
    subject_content_digest: str
    last_semantic_editor_id: str
    guideline_revision_digest: str
    binding_configuration_digest: str
    policy_set_digest: str
    binding_head_digest: str
    input_digest: str
    request_digest: str
    idempotency_key: str
    receipt_digest: str

    def __post_init__(self) -> None:
        _exact_projection(
            self.projection,
            SemanticGuidelineProjection.FULL,
            "semantic_assessment_full_projection_invalid",
        )
        if any(
            type(item) is not SemanticMetricResultFull for item in self.metric_results
        ):
            raise GuidelinePolicyContractError(
                "semantic_assessment_full_metric_projection_invalid"
            )


SemanticAssessmentProjection = (
    SemanticAssessmentSummary | SemanticAssessmentDetail | SemanticAssessmentFull
)


@dataclass(frozen=True, slots=True)
class SemanticFindingSummary:
    projection: SemanticGuidelineProjection
    finding_id: str
    receipt_id: str
    board_id: str
    entity_type: PolicyEntityType
    subject_id: str
    subject_version: int
    guideline_id: str
    guideline_revision_id: str
    binding_id: str
    metric_id: str
    metric_code: str
    currentness: PolicyCurrentness
    currentness_reasons: tuple[SemanticAssessmentCurrentnessReason, ...]
    created_at: datetime

    def __post_init__(self) -> None:
        _exact_projection(
            self.projection,
            SemanticGuidelineProjection.SUMMARY,
            "semantic_finding_summary_projection_invalid",
        )


@dataclass(frozen=True, slots=True)
class SemanticFindingDetail(SemanticFindingSummary):
    metric_result_id: str
    binding_revision: int
    rationale: str
    evidence_refs: tuple[SemanticEvidenceProjection, ...]
    pinpoints: tuple[SemanticPinpointProjection, ...]

    def __post_init__(self) -> None:
        _exact_projection(
            self.projection,
            SemanticGuidelineProjection.DETAIL,
            "semantic_finding_detail_projection_invalid",
        )


@dataclass(frozen=True, slots=True)
class SemanticFindingFull(SemanticFindingDetail):
    metric_result_digest: str
    receipt_digest: str
    subject_content_digest: str
    guideline_revision_digest: str
    binding_configuration_digest: str
    finding_digest: str

    def __post_init__(self) -> None:
        _exact_projection(
            self.projection,
            SemanticGuidelineProjection.FULL,
            "semantic_finding_full_projection_invalid",
        )


SemanticFindingProjection = (
    SemanticFindingSummary | SemanticFindingDetail | SemanticFindingFull
)


@dataclass(frozen=True, slots=True)
class SemanticWaiverSummary:
    projection: SemanticGuidelineProjection
    waiver_id: str
    board_id: str
    entity_type: PolicyEntityType
    subject_id: str
    subject_version: int
    finding_id: str
    receipt_id: str
    guideline_id: str
    guideline_revision_id: str
    binding_id: str
    metric_id: str
    metric_code: str
    status: SemanticMetricWaiverStatus
    waiver_revision: int
    currentness: PolicyCurrentness
    currentness_reasons: tuple[SemanticAssessmentCurrentnessReason, ...]
    requested_at: datetime
    expires_at: datetime | None
    last_event_type: SemanticMetricWaiverEventType
    last_event_at: datetime

    def __post_init__(self) -> None:
        _exact_projection(
            self.projection,
            SemanticGuidelineProjection.SUMMARY,
            "semantic_waiver_summary_projection_invalid",
        )


@dataclass(frozen=True, slots=True)
class SemanticWaiverDetail(SemanticWaiverSummary):
    justification: str
    requested_by: str
    original_expires_at: datetime | None
    reviewed_by: str | None
    reviewed_at: datetime | None
    review_reason: str | None
    revoked_by: str | None
    revoked_at: datetime | None
    expire_reason: SemanticMetricWaiverExpireReason | None
    evidence_refs: tuple[SemanticEvidenceProjection, ...]

    def __post_init__(self) -> None:
        _exact_projection(
            self.projection,
            SemanticGuidelineProjection.DETAIL,
            "semantic_waiver_detail_projection_invalid",
        )


@dataclass(frozen=True, slots=True)
class SemanticWaiverFull(SemanticWaiverDetail):
    metric_result_id: str
    metric_result_digest: str
    finding_digest: str
    receipt_digest: str
    subject_content_digest: str
    guideline_revision_digest: str
    binding_revision: int
    binding_configuration_digest: str
    scope_digest: str
    head_digest: str
    last_event_id: str
    last_event_idempotency_key: str
    assessment_assessor_id: str
    last_revalidation_status: SemanticMetricWaiverRevalidationStatus | None
    last_revalidation_current: bool | None
    last_revalidation_reason_code: SemanticMetricWaiverRevalidationReason | None
    last_revalidation_evaluated_at: datetime | None
    last_revalidation_currentness_reasons: tuple[
        SemanticAssessmentCurrentnessReason, ...
    ]
    last_revalidation_scheduled_expiry_observed: bool

    def __post_init__(self) -> None:
        _exact_projection(
            self.projection,
            SemanticGuidelineProjection.FULL,
            "semantic_waiver_full_projection_invalid",
        )
        _required_text(
            self.assessment_assessor_id,
            "semantic_waiver_full_assessment_assessor_id_required",
        )


SemanticWaiverProjection = (
    SemanticWaiverSummary | SemanticWaiverDetail | SemanticWaiverFull
)


@dataclass(frozen=True, slots=True)
class SemanticSkipSummary:
    projection: SemanticGuidelineProjection
    skip_id: str
    board_id: str
    entity_type: PolicyEntityType
    subject_id: str
    subject_version: int
    guideline_id: str
    guideline_revision_id: str
    binding_id: str
    status: SemanticPolicySkipStatus
    skip_revision: int
    currentness: PolicyCurrentness
    currentness_reasons: tuple[SemanticAssessmentCurrentnessReason, ...]
    created_at: datetime
    last_event_type: SemanticPolicySkipEventType
    last_event_at: datetime

    def __post_init__(self) -> None:
        _exact_projection(
            self.projection,
            SemanticGuidelineProjection.SUMMARY,
            "semantic_skip_summary_projection_invalid",
        )


@dataclass(frozen=True, slots=True)
class SemanticSkipDetail(SemanticSkipSummary):
    binding_revision: int
    reason: str
    created_by: str
    revoked_by: str | None
    revoked_at: datetime | None
    revocation_reason: str | None

    def __post_init__(self) -> None:
        _exact_projection(
            self.projection,
            SemanticGuidelineProjection.DETAIL,
            "semantic_skip_detail_projection_invalid",
        )


@dataclass(frozen=True, slots=True)
class SemanticSkipFull(SemanticSkipDetail):
    subject_content_digest: str
    guideline_revision_digest: str
    binding_configuration_digest: str
    scope_digest: str
    last_event_id: str
    idempotency_key: str
    request_digest: str
    skip_digest: str

    def __post_init__(self) -> None:
        _exact_projection(
            self.projection,
            SemanticGuidelineProjection.FULL,
            "semantic_skip_full_projection_invalid",
        )


SemanticSkipProjection = SemanticSkipSummary | SemanticSkipDetail | SemanticSkipFull


def _evidence_projection(value: EvidenceRef) -> SemanticEvidenceProjection:
    return SemanticEvidenceProjection(
        source_type=value.source_type,
        source_id=value.source_id,
        source_version=value.source_version,
        content_hash=value.content_hash,
    )


def _pinpoint_projection(
    value: SemanticAssessmentPinpoint,
) -> SemanticPinpointProjection:
    return SemanticPinpointProjection(
        anchor_type=value.anchor_type.value,
        anchor_ref=value.anchor_ref,
        excerpt_hash=value.excerpt_hash,
        input_digest=value.input_digest,
    )


def _metric_detail(
    value: SemanticMetricResult,
) -> SemanticMetricResultDetail:
    return SemanticMetricResultDetail(
        metric_result_id=value.metric_result_id,
        metric_id=value.metric_id,
        metric_code=value.metric_code,
        score=value.score,
        direction=value.direction,
        default_threshold=value.default_threshold,
        effective_threshold=value.effective_threshold,
        threshold_source=value.threshold_source,
        outcome=value.outcome,
        rationale=value.rationale,
        evidence_refs=tuple(_evidence_projection(item) for item in value.evidence_refs),
        pinpoints=tuple(_pinpoint_projection(item) for item in value.pinpoints),
    )


def _metric_full(value: SemanticMetricResult) -> SemanticMetricResultFull:
    detail = _metric_detail(value)
    return SemanticMetricResultFull(
        **{
            field_name: getattr(detail, field_name)
            for field_name in detail.__dataclass_fields__
        },
        metric_definition_digest=value.metric_definition_digest,
    )


def _currentness(
    *,
    receipt: SemanticGuidelineAssessmentReceipt,
    assessment: SemanticAssessmentCurrentness | None,
) -> SemanticAssessmentCurrentness:
    if assessment is not None and not isinstance(
        assessment,
        SemanticAssessmentCurrentness,
    ):
        raise GuidelinePolicyContractError("semantic_projection_currentness_invalid")
    resolved = (
        assess_semantic_assessment_currentness(receipt, None)
        if assessment is None
        else assessment
    )
    if resolved.receipt_id != receipt.receipt_id:
        raise GuidelinePolicyContractError(
            "semantic_projection_currentness_receipt_mismatch"
        )
    return resolved


def project_semantic_assessment(
    receipt: SemanticGuidelineAssessmentReceipt,
    *,
    currentness: SemanticAssessmentCurrentness | None,
    projection: SemanticGuidelineProjection,
) -> SemanticAssessmentProjection:
    if not isinstance(receipt, SemanticGuidelineAssessmentReceipt):
        raise GuidelinePolicyContractError(
            "semantic_assessment_projection_receipt_invalid"
        )
    if not isinstance(projection, SemanticGuidelineProjection):
        raise GuidelinePolicyContractError("semantic_assessment_projection_invalid")
    state = _currentness(receipt=receipt, assessment=currentness)
    summary = {
        "projection": projection,
        "receipt_id": receipt.receipt_id,
        "board_id": receipt.subject.board_id,
        "entity_type": receipt.subject.entity_type,
        "subject_id": receipt.subject.subject_id,
        "subject_version": receipt.subject.subject_version,
        "binding_id": receipt.binding_id,
        "guideline_id": receipt.guideline_id,
        "guideline_revision_id": receipt.guideline_revision_id,
        "enforcement": receipt.enforcement,
        "state": receipt.state,
        "currentness": state.currentness,
        "currentness_reasons": state.reasons,
        "confidence": receipt.confidence,
        "minimum_confidence": receipt.minimum_confidence,
        "metric_count": receipt.metric_count,
        "failed_metric_count": receipt.failed_metric_count,
        "recorded_at": receipt.recorded_at,
    }
    if projection is SemanticGuidelineProjection.SUMMARY:
        return SemanticAssessmentSummary(**summary)
    details = {
        **summary,
        "binding_revision": receipt.binding_revision,
        "assessor_agent_id": receipt.assessor.agent_id,
        "assessor_model_id": receipt.assessor.model_id,
        "assessor_independent": receipt.assessor_independent,
        "confidence_admissible": receipt.confidence_admissible,
        "metric_results": tuple(
            (
                _metric_full(item)
                if projection is SemanticGuidelineProjection.FULL
                else _metric_detail(item)
            )
            for item in receipt.metric_results
        ),
    }
    if projection is SemanticGuidelineProjection.DETAIL:
        return SemanticAssessmentDetail(**details)
    return SemanticAssessmentFull(
        **details,
        subject_content_digest=receipt.subject_content_digest,
        last_semantic_editor_id=receipt.last_semantic_editor_id,
        guideline_revision_digest=receipt.guideline_revision_digest,
        binding_configuration_digest=receipt.binding_configuration_digest,
        policy_set_digest=receipt.policy_set_digest,
        binding_head_digest=receipt.binding_head_digest,
        input_digest=receipt.input_digest,
        request_digest=receipt.request_digest,
        idempotency_key=receipt.idempotency_key,
        receipt_digest=receipt.receipt_digest,
    )


def project_semantic_finding(
    finding: SemanticMetricFinding,
    *,
    currentness: SemanticAssessmentCurrentness,
    projection: SemanticGuidelineProjection,
) -> SemanticFindingProjection:
    if not isinstance(finding, SemanticMetricFinding):
        raise GuidelinePolicyContractError("semantic_finding_projection_invalid")
    if not isinstance(projection, SemanticGuidelineProjection):
        raise GuidelinePolicyContractError("semantic_finding_projection_invalid")
    if not isinstance(currentness, SemanticAssessmentCurrentness):
        raise GuidelinePolicyContractError("semantic_finding_currentness_invalid")
    if currentness.receipt_id != finding.receipt_id:
        raise GuidelinePolicyContractError(
            "semantic_finding_currentness_receipt_mismatch"
        )
    summary = {
        "projection": projection,
        "finding_id": finding.finding_id,
        "receipt_id": finding.receipt_id,
        "board_id": finding.subject.board_id,
        "entity_type": finding.subject.entity_type,
        "subject_id": finding.subject.subject_id,
        "subject_version": finding.subject.subject_version,
        "guideline_id": finding.guideline_id,
        "guideline_revision_id": finding.guideline_revision_id,
        "binding_id": finding.binding_id,
        "metric_id": finding.metric_id,
        "metric_code": finding.metric_code,
        "currentness": currentness.currentness,
        "currentness_reasons": currentness.reasons,
        "created_at": finding.created_at,
    }
    if projection is SemanticGuidelineProjection.SUMMARY:
        return SemanticFindingSummary(**summary)
    details = {
        **summary,
        "metric_result_id": finding.metric_result_id,
        "binding_revision": finding.binding_revision,
        "rationale": finding.rationale,
        "evidence_refs": tuple(
            _evidence_projection(item) for item in finding.evidence_refs
        ),
        "pinpoints": tuple(_pinpoint_projection(item) for item in finding.pinpoints),
    }
    if projection is SemanticGuidelineProjection.DETAIL:
        return SemanticFindingDetail(**details)
    return SemanticFindingFull(
        **details,
        metric_result_digest=finding.metric_result_digest,
        receipt_digest=finding.receipt_digest,
        subject_content_digest=finding.subject_content_digest,
        guideline_revision_digest=finding.guideline_revision_digest,
        binding_configuration_digest=finding.binding_configuration_digest,
        finding_digest=finding.finding_digest,
    )


def project_semantic_waiver(
    waiver: SemanticMetricWaiver,
    *,
    currentness: SemanticAssessmentCurrentness,
    projection: SemanticGuidelineProjection,
    evaluated_at: datetime | None = None,
) -> SemanticWaiverProjection:
    if not isinstance(waiver, SemanticMetricWaiver):
        raise GuidelinePolicyContractError("semantic_waiver_projection_invalid")
    if not isinstance(projection, SemanticGuidelineProjection):
        raise GuidelinePolicyContractError("semantic_waiver_projection_invalid")
    if not isinstance(currentness, SemanticAssessmentCurrentness):
        raise GuidelinePolicyContractError("semantic_waiver_currentness_invalid")
    anchor = waiver.anchor
    if currentness.receipt_id != anchor.receipt_id:
        raise GuidelinePolicyContractError(
            "semantic_waiver_currentness_receipt_mismatch"
        )
    if evaluated_at is not None:
        evaluated_at = _aware_utc(
            evaluated_at,
            "semantic_waiver_projection_evaluated_at_invalid",
        )
    effective_status = waiver.status
    effective_expire_reason = waiver.expire_reason
    if (
        evaluated_at is not None
        and waiver.status is SemanticMetricWaiverStatus.APPROVED
        and waiver.expires_at is not None
        and waiver.expires_at <= evaluated_at
    ):
        effective_status = SemanticMetricWaiverStatus.EXPIRED
        effective_expire_reason = SemanticMetricWaiverExpireReason.SCHEDULED_EXPIRY
    summary = {
        "projection": projection,
        "waiver_id": waiver.waiver_id,
        "board_id": anchor.subject.board_id,
        "entity_type": anchor.subject.entity_type,
        "subject_id": anchor.subject.subject_id,
        "subject_version": anchor.subject.subject_version,
        "finding_id": anchor.finding_id,
        "receipt_id": anchor.receipt_id,
        "guideline_id": anchor.guideline_id,
        "guideline_revision_id": anchor.guideline_revision_id,
        "binding_id": anchor.binding_id,
        "metric_id": anchor.metric_id,
        "metric_code": anchor.metric_code,
        "status": effective_status,
        "waiver_revision": waiver.waiver_revision,
        "currentness": currentness.currentness,
        "currentness_reasons": currentness.reasons,
        "requested_at": waiver.requested_at,
        "expires_at": waiver.expires_at,
        "last_event_type": waiver.last_event_type,
        "last_event_at": waiver.last_event_at,
    }
    if projection is SemanticGuidelineProjection.SUMMARY:
        return SemanticWaiverSummary(**summary)
    details = {
        **summary,
        "justification": waiver.justification,
        "requested_by": waiver.requested_by,
        "original_expires_at": waiver.original_expires_at,
        "reviewed_by": waiver.reviewed_by,
        "reviewed_at": waiver.reviewed_at,
        "review_reason": waiver.review_reason,
        "revoked_by": waiver.revoked_by,
        "revoked_at": waiver.revoked_at,
        "expire_reason": effective_expire_reason,
        "evidence_refs": tuple(
            _evidence_projection(item) for item in waiver.evidence_refs
        ),
    }
    if projection is SemanticGuidelineProjection.DETAIL:
        return SemanticWaiverDetail(**details)
    return SemanticWaiverFull(
        **details,
        metric_result_id=anchor.metric_result_id,
        metric_result_digest=anchor.metric_result_digest,
        finding_digest=anchor.finding_digest,
        receipt_digest=anchor.receipt_digest,
        subject_content_digest=anchor.subject_content_digest,
        guideline_revision_digest=anchor.guideline_revision_digest,
        binding_revision=anchor.binding_revision,
        binding_configuration_digest=anchor.binding_configuration_digest,
        scope_digest=waiver.scope_digest,
        head_digest=waiver.head_digest,
        last_event_id=waiver.last_event_id,
        last_event_idempotency_key=(waiver.last_event_idempotency_key),
        assessment_assessor_id=anchor.assessment_assessor_id,
        last_revalidation_status=waiver.last_revalidation_status,
        last_revalidation_current=waiver.last_revalidation_current,
        last_revalidation_reason_code=(waiver.last_revalidation_reason_code),
        last_revalidation_evaluated_at=(waiver.last_revalidation_evaluated_at),
        last_revalidation_currentness_reasons=(
            waiver.last_revalidation_currentness_reasons
        ),
        last_revalidation_scheduled_expiry_observed=(
            waiver.last_revalidation_scheduled_expiry_observed
        ),
    )


def _skip_currentness(
    skip: SemanticPolicySkip,
    current: SemanticAssessmentCurrentSnapshot | None,
) -> tuple[
    PolicyCurrentness,
    tuple[SemanticAssessmentCurrentnessReason, ...],
]:
    if current is None:
        return (
            PolicyCurrentness.STALE,
            (SemanticAssessmentCurrentnessReason.CURRENT_SNAPSHOT_MISSING,),
        )
    scope = skip.scope
    if (
        scope.subject.board_id != current.subject.board_id
        or scope.subject.entity_type is not current.subject.entity_type
        or scope.subject.subject_id != current.subject.subject_id
        or scope.guideline_id != current.guideline_id
        or scope.binding_id != current.binding_id
    ):
        raise GuidelinePolicyContractError("semantic_skip_currentness_scope_mismatch")
    reasons: set[SemanticAssessmentCurrentnessReason] = set()
    if scope.subject.subject_version != current.subject.subject_version:
        reasons.add(SemanticAssessmentCurrentnessReason.SUBJECT_VERSION_CHANGED)
    if scope.subject_content_digest != current.subject_content_digest:
        reasons.add(SemanticAssessmentCurrentnessReason.SUBJECT_CONTENT_CHANGED)
    if scope.guideline_revision_id != current.guideline_revision_id:
        reasons.add(SemanticAssessmentCurrentnessReason.GUIDELINE_REVISION_CHANGED)
    if scope.guideline_revision_digest != current.guideline_revision_digest:
        reasons.add(
            SemanticAssessmentCurrentnessReason.GUIDELINE_REVISION_DIGEST_CHANGED
        )
    if scope.binding_revision != current.binding_revision:
        reasons.add(SemanticAssessmentCurrentnessReason.BINDING_REVISION_CHANGED)
    if scope.binding_configuration_digest != current.binding_configuration_digest:
        reasons.add(SemanticAssessmentCurrentnessReason.BINDING_CONFIGURATION_CHANGED)
    ordered = tuple(
        reason for reason in SemanticAssessmentCurrentnessReason if reason in reasons
    )
    return (
        PolicyCurrentness.STALE if ordered else PolicyCurrentness.CURRENT,
        ordered,
    )


def project_semantic_skip(
    skip: SemanticPolicySkip,
    *,
    current: SemanticAssessmentCurrentSnapshot | None,
    projection: SemanticGuidelineProjection,
) -> SemanticSkipProjection:
    if not isinstance(skip, SemanticPolicySkip):
        raise GuidelinePolicyContractError("semantic_skip_projection_invalid")
    if not isinstance(projection, SemanticGuidelineProjection):
        raise GuidelinePolicyContractError("semantic_skip_projection_invalid")
    currentness, currentness_reasons = _skip_currentness(skip, current)
    scope = skip.scope
    summary = {
        "projection": projection,
        "skip_id": skip.skip_id,
        "board_id": scope.subject.board_id,
        "entity_type": scope.subject.entity_type,
        "subject_id": scope.subject.subject_id,
        "subject_version": scope.subject.subject_version,
        "guideline_id": scope.guideline_id,
        "guideline_revision_id": scope.guideline_revision_id,
        "binding_id": scope.binding_id,
        "status": skip.status,
        "skip_revision": skip.skip_revision,
        "currentness": currentness,
        "currentness_reasons": currentness_reasons,
        "created_at": skip.created_at,
        "last_event_type": skip.last_event_type,
        "last_event_at": skip.last_event_at,
    }
    if projection is SemanticGuidelineProjection.SUMMARY:
        return SemanticSkipSummary(**summary)
    details = {
        **summary,
        "binding_revision": scope.binding_revision,
        "reason": skip.reason,
        "created_by": skip.created_by,
        "revoked_by": skip.revoked_by,
        "revoked_at": skip.revoked_at,
        "revocation_reason": skip.revocation_reason,
    }
    if projection is SemanticGuidelineProjection.DETAIL:
        return SemanticSkipDetail(**details)
    return SemanticSkipFull(
        **details,
        subject_content_digest=scope.subject_content_digest,
        guideline_revision_digest=scope.guideline_revision_digest,
        binding_configuration_digest=scope.binding_configuration_digest,
        scope_digest=skip.scope_digest,
        last_event_id=skip.last_event_id,
        idempotency_key=skip.idempotency_key,
        request_digest=skip.request_digest,
        skip_digest=skip.skip_digest,
    )


@dataclass(frozen=True, slots=True)
class _SemanticTimeCursor:
    at: datetime
    item_id: str
    filter_digest: str
    projection_digest: str
    schema_version: str
    ordering: tuple[str, str]

    expected_ordering: ClassVar[tuple[str, str]]

    def __post_init__(self) -> None:
        if self.schema_version != SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION:
            raise GuidelinePolicyContractError("semantic_cursor_schema_version_invalid")
        if tuple(self.ordering) != self.expected_ordering:
            raise GuidelinePolicyContractError("semantic_cursor_ordering_invalid")
        object.__setattr__(
            self,
            "at",
            _aware_utc(self.at, "semantic_cursor_time_invalid"),
        )
        object.__setattr__(
            self,
            "item_id",
            _required_text(self.item_id, "semantic_cursor_item_id_required"),
        )
        object.__setattr__(
            self,
            "filter_digest",
            _sha256(
                self.filter_digest,
                "semantic_cursor_filter_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "projection_digest",
            _sha256(
                self.projection_digest,
                "semantic_cursor_projection_digest_invalid",
            ),
        )
        object.__setattr__(self, "ordering", self.expected_ordering)


@dataclass(frozen=True, slots=True)
class SemanticAssessmentPageCursor(_SemanticTimeCursor):
    expected_ordering: ClassVar[tuple[str, str]] = SEMANTIC_ASSESSMENT_ORDERING
    schema_version: str = SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION
    ordering: tuple[str, str] = SEMANTIC_ASSESSMENT_ORDERING

    @property
    def recorded_at(self) -> datetime:
        return self.at


@dataclass(frozen=True, slots=True)
class SemanticFindingPageCursor(_SemanticTimeCursor):
    expected_ordering: ClassVar[tuple[str, str]] = SEMANTIC_FINDING_ORDERING
    schema_version: str = SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION
    ordering: tuple[str, str] = SEMANTIC_FINDING_ORDERING

    @property
    def created_at(self) -> datetime:
        return self.at


@dataclass(frozen=True, slots=True)
class SemanticWaiverPageCursor(_SemanticTimeCursor):
    expected_ordering: ClassVar[tuple[str, str]] = SEMANTIC_WAIVER_ORDERING
    schema_version: str = SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION
    ordering: tuple[str, str] = SEMANTIC_WAIVER_ORDERING

    @property
    def requested_at(self) -> datetime:
        return self.at


@dataclass(frozen=True, slots=True)
class SemanticSkipPageCursor(_SemanticTimeCursor):
    expected_ordering: ClassVar[tuple[str, str]] = SEMANTIC_SKIP_ORDERING
    schema_version: str = SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION
    ordering: tuple[str, str] = SEMANTIC_SKIP_ORDERING

    @property
    def created_at(self) -> datetime:
        return self.at


_ItemT = TypeVar("_ItemT")
_CursorT = TypeVar("_CursorT")


@dataclass(frozen=True, slots=True)
class SemanticKeysetPage(Generic[_ItemT, _CursorT]):
    items: tuple[_ItemT, ...]
    limit: int
    next_cursor: _CursorT | None
    has_more: bool
    projection: SemanticGuidelineProjection

    ordering: ClassVar[tuple[str, ...]] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple | list):
            raise GuidelinePolicyContractError("semantic_page_items_invalid")
        object.__setattr__(self, "items", tuple(self.items))
        object.__setattr__(self, "limit", _page_limit(self.limit))
        if len(self.items) > self.limit:
            raise GuidelinePolicyContractError("semantic_page_over_limit")
        if not isinstance(self.has_more, bool):
            raise GuidelinePolicyContractError("semantic_page_has_more_invalid")
        if self.has_more != (self.next_cursor is not None):
            raise GuidelinePolicyContractError("semantic_page_cursor_mismatch")
        if not isinstance(self.projection, SemanticGuidelineProjection):
            raise GuidelinePolicyContractError("semantic_page_projection_invalid")
        if any(
            getattr(item, "projection", None) is not self.projection
            for item in self.items
        ):
            raise GuidelinePolicyContractError("semantic_page_projection_mismatch")


@dataclass(frozen=True, slots=True)
class SemanticAssessmentPage(
    SemanticKeysetPage[
        SemanticAssessmentProjection,
        SemanticAssessmentPageCursor,
    ]
):
    ordering: ClassVar[tuple[str, ...]] = SEMANTIC_ASSESSMENT_ORDERING

    def __post_init__(self) -> None:
        SemanticKeysetPage.__post_init__(self)
        if any(
            not isinstance(
                item,
                (
                    SemanticAssessmentSummary,
                    SemanticAssessmentDetail,
                    SemanticAssessmentFull,
                ),
            )
            for item in self.items
        ) or (
            self.next_cursor is not None
            and not isinstance(
                self.next_cursor,
                SemanticAssessmentPageCursor,
            )
        ):
            raise GuidelinePolicyContractError("semantic_assessment_page_invalid")


@dataclass(frozen=True, slots=True)
class SemanticFindingPage(
    SemanticKeysetPage[
        SemanticFindingProjection,
        SemanticFindingPageCursor,
    ]
):
    ordering: ClassVar[tuple[str, ...]] = SEMANTIC_FINDING_ORDERING

    def __post_init__(self) -> None:
        SemanticKeysetPage.__post_init__(self)
        if any(
            not isinstance(
                item,
                (
                    SemanticFindingSummary,
                    SemanticFindingDetail,
                    SemanticFindingFull,
                ),
            )
            for item in self.items
        ) or (
            self.next_cursor is not None
            and not isinstance(self.next_cursor, SemanticFindingPageCursor)
        ):
            raise GuidelinePolicyContractError("semantic_finding_page_invalid")


@dataclass(frozen=True, slots=True)
class SemanticWaiverPage(
    SemanticKeysetPage[
        SemanticWaiverProjection,
        SemanticWaiverPageCursor,
    ]
):
    ordering: ClassVar[tuple[str, ...]] = SEMANTIC_WAIVER_ORDERING

    def __post_init__(self) -> None:
        SemanticKeysetPage.__post_init__(self)
        if any(
            not isinstance(
                item,
                (
                    SemanticWaiverSummary,
                    SemanticWaiverDetail,
                    SemanticWaiverFull,
                ),
            )
            for item in self.items
        ) or (
            self.next_cursor is not None
            and not isinstance(self.next_cursor, SemanticWaiverPageCursor)
        ):
            raise GuidelinePolicyContractError("semantic_waiver_page_invalid")


@dataclass(frozen=True, slots=True)
class SemanticSkipPage(
    SemanticKeysetPage[
        SemanticSkipProjection,
        SemanticSkipPageCursor,
    ]
):
    ordering: ClassVar[tuple[str, ...]] = SEMANTIC_SKIP_ORDERING

    def __post_init__(self) -> None:
        SemanticKeysetPage.__post_init__(self)
        if any(
            not isinstance(
                item,
                (
                    SemanticSkipSummary,
                    SemanticSkipDetail,
                    SemanticSkipFull,
                ),
            )
            for item in self.items
        ) or (
            self.next_cursor is not None
            and not isinstance(self.next_cursor, SemanticSkipPageCursor)
        ):
            raise GuidelinePolicyContractError("semantic_skip_page_invalid")


__all__ = [
    "SEMANTIC_ASSESSMENT_ORDERING",
    "SEMANTIC_FINDING_ORDERING",
    "SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION",
    "SEMANTIC_GUIDELINE_PAGE_LIMIT_DEFAULT",
    "SEMANTIC_GUIDELINE_PAGE_LIMIT_MAX",
    "SEMANTIC_SKIP_ORDERING",
    "SEMANTIC_WAIVER_ORDERING",
    "SemanticAssessmentDetail",
    "SemanticAssessmentFull",
    "SemanticAssessmentPage",
    "SemanticAssessmentPageCursor",
    "SemanticAssessmentProjection",
    "SemanticAssessmentSummary",
    "SemanticEvidenceProjection",
    "SemanticFindingDetail",
    "SemanticFindingFull",
    "SemanticFindingPage",
    "SemanticFindingPageCursor",
    "SemanticFindingProjection",
    "SemanticFindingSummary",
    "SemanticGuidelineProjection",
    "SemanticKeysetPage",
    "SemanticMetricResultDetail",
    "SemanticMetricResultFull",
    "SemanticPinpointProjection",
    "SemanticSkipDetail",
    "SemanticSkipFull",
    "SemanticSkipPage",
    "SemanticSkipPageCursor",
    "SemanticSkipProjection",
    "SemanticSkipSummary",
    "SemanticWaiverDetail",
    "SemanticWaiverFull",
    "SemanticWaiverPage",
    "SemanticWaiverPageCursor",
    "SemanticWaiverProjection",
    "SemanticWaiverSummary",
    "project_semantic_assessment",
    "project_semantic_finding",
    "project_semantic_skip",
    "project_semantic_waiver",
]
