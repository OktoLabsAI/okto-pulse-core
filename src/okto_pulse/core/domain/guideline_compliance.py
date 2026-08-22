"""Pure guideline projections plus deprecated policy/v1 read contracts.

``guideline-domain/v2`` freezes semantic revisions. Legacy receipt projections
remain temporarily importable while their consumers migrate. This module compares
immutable evidence with an explicit current snapshot and builds list
projections without mutating the receipt.  Missing current evidence is stale
by construction, so callers cannot accidentally turn an old receipt into a
false pass.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import ClassVar, Generic, TypeVar

from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_PAGE_LIMIT_MAX,
    POLICY_KEYSET_CONTRACT_VERSION,
    AdoptedGuidelineRevisionRef,
    GuidelineEnforcement,
    GuidelineImpactItem,
    GuidelinePolicyContractError,
    GuidelineRevision,
    GuidelineRevisionPageCursor,
    GuidelineMetric,
    PolicyComplianceFinding,
    PolicyComplianceReasonCode,
    PolicyComplianceReceipt,
    PolicyComplianceState,
    PolicyCurrentness,
    PolicyEntityType,
    PolicyEvaluationOutcome,
    PolicySubjectRef,
    PolicyWaiver,
    PolicyWaiverExpireReasonCode,
    PolicyWaiverStatus,
)
from okto_pulse.core.domain.guideline_semantic_projection import (
    SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION,
    SemanticAssessmentPageCursor,
    SemanticFindingPageCursor,
    SemanticSkipPageCursor,
    SemanticWaiverPageCursor,
)


POLICY_COMPLIANCE_CONTRACT_VERSION = "policy-compliance/v1"
POLICY_CURSOR_TOKEN_MAX_LENGTH = 8192


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


def _positive_limit(value: object) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= GUIDELINE_PAGE_LIMIT_MAX
    ):
        raise GuidelinePolicyContractError("policy_page_limit_invalid")
    return value


class PolicyProjection(str, Enum):
    """Closed list projection surface shared by REST and MCP adapters."""

    SUMMARY = "summary"
    DETAIL = "detail"


@dataclass(frozen=True, slots=True)
class GuidelineRevisionListItem:
    """Projection-safe immutable revision row shared by REST and MCP."""

    projection: PolicyProjection
    revision_id: str
    guideline_id: str
    revision_number: int
    semantic_version: str
    title: str
    created_by: str
    created_at: datetime
    parent_revision_id: str | None
    content: str | None = None
    revision_digest: str | None = None
    tags: tuple[str, ...] | None = None
    metrics: tuple[GuidelineMetric, ...] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.projection, PolicyProjection):
            raise GuidelinePolicyContractError(
                "guideline_revision_projection_invalid"
            )
        for field_name in (
            "revision_id",
            "guideline_id",
            "semantic_version",
            "title",
            "created_by",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"guideline_revision_projection_{field_name}_required",
                ),
            )
        if (
            not isinstance(self.revision_number, int)
            or isinstance(self.revision_number, bool)
            or self.revision_number < 1
        ):
            raise GuidelinePolicyContractError(
                "guideline_revision_projection_number_invalid"
            )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "guideline_revision_projection_created_at_invalid",
            ),
        )
        if self.projection is PolicyProjection.SUMMARY:
            if any(
                value is not None
                for value in (
                    self.content,
                    self.revision_digest,
                    self.tags,
                    self.metrics,
                )
            ):
                raise GuidelinePolicyContractError(
                    "guideline_revision_summary_not_slim"
                )
        elif (
            self.content is None
            or self.revision_digest is None
            or self.tags is None
            or self.metrics is None
        ):
            raise GuidelinePolicyContractError(
                "guideline_revision_detail_incomplete"
            )


@dataclass(frozen=True, slots=True)
class GuidelineRevisionProjectionPage:
    items: tuple[GuidelineRevisionListItem, ...]
    limit: int
    next_cursor: GuidelineRevisionPageCursor | None
    has_more: bool

    ordering: ClassVar[tuple[str, str]] = (
        "revision_number DESC",
        "revision_id DESC",
    )

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple | list) or any(
            not isinstance(item, GuidelineRevisionListItem) for item in self.items
        ):
            raise GuidelinePolicyContractError(
                "guideline_revision_projection_page_items_invalid"
            )
        object.__setattr__(self, "items", tuple(self.items))
        if (
            not isinstance(self.limit, int)
            or isinstance(self.limit, bool)
            or not 1 <= self.limit <= GUIDELINE_PAGE_LIMIT_MAX
        ):
            raise GuidelinePolicyContractError("guideline_page_limit_invalid")
        if len(self.items) > self.limit:
            raise GuidelinePolicyContractError(
                "guideline_revision_projection_page_limit_exceeded"
            )
        if self.next_cursor is not None and not isinstance(
            self.next_cursor,
            GuidelineRevisionPageCursor,
        ):
            raise GuidelinePolicyContractError(
                "guideline_revision_projection_page_cursor_invalid"
            )
        if not isinstance(self.has_more, bool) or self.has_more != (
            self.next_cursor is not None
        ):
            raise GuidelinePolicyContractError(
                "guideline_revision_projection_page_cursor_mismatch"
            )


def project_guideline_revision(
    revision: GuidelineRevision,
    *,
    projection: PolicyProjection,
) -> GuidelineRevisionListItem:
    if not isinstance(revision, GuidelineRevision):
        raise GuidelinePolicyContractError("guideline_revision_invalid")
    if not isinstance(projection, PolicyProjection):
        raise GuidelinePolicyContractError("guideline_revision_projection_invalid")
    detail = projection is PolicyProjection.DETAIL
    return GuidelineRevisionListItem(
        projection=projection,
        revision_id=revision.revision_id,
        guideline_id=revision.guideline_id,
        revision_number=revision.revision_number,
        semantic_version=revision.semantic_version,
        title=revision.title,
        created_by=revision.created_by,
        created_at=revision.created_at,
        parent_revision_id=revision.parent_revision_id,
        content=revision.content if detail else None,
        revision_digest=revision.revision_digest if detail else None,
        tags=revision.tags if detail else None,
        metrics=revision.metrics if detail else None,
    )


class PolicyCurrentnessReason(str, Enum):
    """Stable, independently testable reasons why a receipt is stale."""

    CURRENT_SNAPSHOT_MISSING = "current_snapshot_missing"
    SUBJECT_EDITION_CHANGED = "subject_edition_changed"
    SUBJECT_VERSION_CHANGED = "subject_version_changed"
    SUBJECT_CONTENT_CHANGED = "subject_content_changed"
    INPUT_DIGEST_CHANGED = "input_digest_changed"
    POLICY_SET_CHANGED = "policy_set_changed"
    CATALOG_VERSION_CHANGED = "catalog_version_changed"
    RULESET_VERSION_CHANGED = "ruleset_version_changed"
    BINDING_HEAD_CHANGED = "binding_head_changed"


_CURRENTNESS_REASON_ORDER: tuple[PolicyCurrentnessReason, ...] = (
    PolicyCurrentnessReason.CURRENT_SNAPSHOT_MISSING,
    PolicyCurrentnessReason.SUBJECT_EDITION_CHANGED,
    PolicyCurrentnessReason.SUBJECT_VERSION_CHANGED,
    PolicyCurrentnessReason.SUBJECT_CONTENT_CHANGED,
    PolicyCurrentnessReason.INPUT_DIGEST_CHANGED,
    PolicyCurrentnessReason.POLICY_SET_CHANGED,
    PolicyCurrentnessReason.CATALOG_VERSION_CHANGED,
    PolicyCurrentnessReason.RULESET_VERSION_CHANGED,
    PolicyCurrentnessReason.BINDING_HEAD_CHANGED,
)


@dataclass(frozen=True, slots=True)
class PolicyComplianceCurrentSnapshot:
    """All live fences required to prove a receipt current."""

    subject: PolicySubjectRef
    subject_content_digest: str
    input_digest: str
    policy_set_digest: str
    binding_head_digest: str
    catalog_version: str
    ruleset_version: str

    def __post_init__(self) -> None:
        if not isinstance(self.subject, PolicySubjectRef):
            raise GuidelinePolicyContractError(
                "policy_current_snapshot_subject_invalid"
            )
        for field_name in (
            "subject_content_digest",
            "input_digest",
            "policy_set_digest",
            "binding_head_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"policy_current_snapshot_{field_name}_invalid",
                ),
            )
        for field_name in ("catalog_version", "ruleset_version"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"policy_current_snapshot_{field_name}_required",
                ),
            )

    @property
    def identity(self) -> tuple[str, PolicyEntityType, str]:
        return (
            self.subject.board_id,
            self.subject.entity_type,
            self.subject.subject_id,
        )


@dataclass(frozen=True, slots=True)
class PolicyCurrentnessAssessment:
    currentness: PolicyCurrentness
    reasons: tuple[PolicyCurrentnessReason, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.currentness, PolicyCurrentness):
            raise GuidelinePolicyContractError(
                "policy_currentness_assessment_state_invalid"
            )
        if not isinstance(self.reasons, tuple | list) or any(
            not isinstance(reason, PolicyCurrentnessReason) for reason in self.reasons
        ):
            raise GuidelinePolicyContractError(
                "policy_currentness_assessment_reasons_invalid"
            )
        reasons = tuple(
            reason for reason in _CURRENTNESS_REASON_ORDER if reason in self.reasons
        )
        if len(reasons) != len(set(self.reasons)):
            raise GuidelinePolicyContractError(
                "policy_currentness_assessment_reasons_duplicate"
            )
        if (self.currentness is PolicyCurrentness.CURRENT and reasons) or (
            self.currentness is PolicyCurrentness.STALE and not reasons
        ):
            raise GuidelinePolicyContractError(
                "policy_currentness_assessment_shape_invalid"
            )
        object.__setattr__(self, "reasons", reasons)


def assess_policy_receipt_currentness(
    receipt: PolicyComplianceReceipt,
    current: PolicyComplianceCurrentSnapshot | None,
) -> PolicyCurrentnessAssessment:
    """Compare all policy-compliance/v1 fences without mutating evidence."""

    if not isinstance(receipt, PolicyComplianceReceipt):
        raise GuidelinePolicyContractError("policy_receipt_invalid")
    recorded = PolicyComplianceCurrentSnapshot(
        subject=receipt.subject,
        subject_content_digest=receipt.subject_content_digest,
        input_digest=receipt.input_digest,
        policy_set_digest=receipt.policy_set_digest,
        binding_head_digest=receipt.binding_head_digest,
        catalog_version=receipt.catalog_version,
        ruleset_version=receipt.ruleset_version,
    )
    return assess_policy_compliance_fences(recorded, current)


def assess_policy_compliance_fences(
    recorded: PolicyComplianceCurrentSnapshot,
    current: PolicyComplianceCurrentSnapshot | None,
) -> PolicyCurrentnessAssessment:
    """Compare a frozen fence bundle with server-owned live evidence."""

    if not isinstance(recorded, PolicyComplianceCurrentSnapshot):
        raise GuidelinePolicyContractError("policy_recorded_snapshot_invalid")
    if current is None:
        return PolicyCurrentnessAssessment(
            currentness=PolicyCurrentness.STALE,
            reasons=(PolicyCurrentnessReason.CURRENT_SNAPSHOT_MISSING,),
        )
    if not isinstance(current, PolicyComplianceCurrentSnapshot):
        raise GuidelinePolicyContractError("policy_current_snapshot_invalid")
    if current.identity != recorded.identity:
        raise GuidelinePolicyContractError("policy_current_snapshot_scope_mismatch")

    if current.subject.subject_edition is not None:
        reasons = (
            ()
            if recorded.subject.subject_edition == current.subject.subject_edition
            else (PolicyCurrentnessReason.SUBJECT_EDITION_CHANGED,)
        )
        return PolicyCurrentnessAssessment(
            currentness=(
                PolicyCurrentness.CURRENT
                if not reasons
                else PolicyCurrentness.STALE
            ),
            reasons=reasons,
        )

    reasons: list[PolicyCurrentnessReason] = []
    subject_version_changed = (
        recorded.subject.subject_version != current.subject.subject_version
    )
    if subject_version_changed:
        reasons.append(PolicyCurrentnessReason.SUBJECT_VERSION_CHANGED)
    if recorded.subject_content_digest != current.subject_content_digest:
        reasons.append(PolicyCurrentnessReason.SUBJECT_CONTENT_CHANGED)
    if recorded.policy_set_digest != current.policy_set_digest:
        reasons.append(PolicyCurrentnessReason.POLICY_SET_CHANGED)
    if recorded.binding_head_digest != current.binding_head_digest:
        reasons.append(PolicyCurrentnessReason.BINDING_HEAD_CHANGED)
    comparisons = (
        (
            recorded.catalog_version,
            current.catalog_version,
            PolicyCurrentnessReason.CATALOG_VERSION_CHANGED,
        ),
        (
            recorded.ruleset_version,
            current.ruleset_version,
            PolicyCurrentnessReason.RULESET_VERSION_CHANGED,
        ),
    )
    reasons.extend(reason for recorded, live, reason in comparisons if recorded != live)
    # ``input_digest`` is an integrity aggregate of every component above.
    # Report it only when all explainable fences still match, otherwise one
    # isolated semantic change would misleadingly produce two stale reasons.
    if not reasons and recorded.input_digest != current.input_digest:
        reasons.append(PolicyCurrentnessReason.INPUT_DIGEST_CHANGED)
    return PolicyCurrentnessAssessment(
        currentness=(
            PolicyCurrentness.CURRENT if not reasons else PolicyCurrentness.STALE
        ),
        reasons=tuple(reasons),
    )


def policy_finding_severity_rank(
    finding: PolicyComplianceFinding,
) -> int:
    """Stable descending rank for ``policy-keyset/v1`` finding pages."""

    if not isinstance(finding, PolicyComplianceFinding):
        raise GuidelinePolicyContractError("policy_finding_invalid")
    return policy_finding_severity_rank_values(
        outcome=finding.outcome,
        enforcement=finding.enforcement,
        waived=finding.waiver_id is not None,
    )


def policy_finding_severity_rank_values(
    *,
    outcome: PolicyEvaluationOutcome,
    enforcement: GuidelineEnforcement,
    waived: bool,
) -> int:
    if not isinstance(outcome, PolicyEvaluationOutcome):
        raise GuidelinePolicyContractError("policy_finding_outcome_invalid")
    if not isinstance(enforcement, GuidelineEnforcement):
        raise GuidelinePolicyContractError("policy_finding_enforcement_invalid")
    if not isinstance(waived, bool):
        raise GuidelinePolicyContractError("policy_finding_waived_invalid")
    blocking = enforcement is GuidelineEnforcement.BLOCKING
    if outcome is PolicyEvaluationOutcome.ERROR:
        return 60 if blocking else 30
    if outcome is PolicyEvaluationOutcome.FAIL:
        if blocking and not waived:
            return 50
        if blocking:
            return 40
        return 20
    if outcome is PolicyEvaluationOutcome.NOT_APPLICABLE:
        return 10
    return 0


@dataclass(frozen=True, slots=True)
class PolicyComplianceReceiptListItem:
    """Projection-safe receipt row; summary never carries heavy evidence."""

    projection: PolicyProjection
    receipt_id: str
    subject: PolicySubjectRef
    outcome: PolicyEvaluationOutcome
    state: PolicyComplianceState
    currentness: PolicyCurrentness
    currentness_reasons: tuple[PolicyCurrentnessReason, ...]
    evaluator_version: str
    evaluated_by: str
    evaluated_at: datetime
    finding_count: int
    rule_count: int
    failed_rule_count: int
    error_rule_count: int
    blocking_finding_count: int
    waived_finding_count: int
    reason_codes: tuple[PolicyComplianceReasonCode, ...]
    subject_content_digest: str | None = None
    input_digest: str | None = None
    policy_set_digest: str | None = None
    binding_head_digest: str | None = None
    catalog_version: str | None = None
    ruleset_version: str | None = None
    adopted_revisions: tuple[AdoptedGuidelineRevisionRef, ...] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.projection, PolicyProjection):
            raise GuidelinePolicyContractError("policy_receipt_projection_invalid")
        if not isinstance(self.subject, PolicySubjectRef):
            raise GuidelinePolicyContractError(
                "policy_receipt_projection_subject_invalid"
            )
        for field_name in ("receipt_id", "evaluator_version", "evaluated_by"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"policy_receipt_projection_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "evaluated_at",
            _aware_utc(
                self.evaluated_at,
                "policy_receipt_projection_evaluated_at_invalid",
            ),
        )
        for field_name in (
            "finding_count",
            "rule_count",
            "failed_rule_count",
            "error_rule_count",
            "blocking_finding_count",
            "waived_finding_count",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise GuidelinePolicyContractError(
                    f"policy_receipt_projection_{field_name}_invalid"
                )
        if self.blocking_finding_count > self.finding_count:
            raise GuidelinePolicyContractError(
                "policy_receipt_projection_blocking_count_invalid"
            )
        if self.waived_finding_count > self.finding_count:
            raise GuidelinePolicyContractError(
                "policy_receipt_projection_waived_count_invalid"
            )
        if self.failed_rule_count + self.error_rule_count > self.rule_count:
            raise GuidelinePolicyContractError(
                "policy_receipt_projection_rule_counts_invalid"
            )
        if not isinstance(self.reason_codes, tuple | list) or any(
            not isinstance(reason, PolicyComplianceReasonCode)
            for reason in self.reason_codes
        ):
            raise GuidelinePolicyContractError(
                "policy_receipt_projection_reason_codes_invalid"
            )
        object.__setattr__(
            self,
            "reason_codes",
            tuple(sorted(set(self.reason_codes), key=lambda item: item.value)),
        )
        if self.projection is PolicyProjection.SUMMARY:
            if any(
                getattr(self, field_name) is not None
                for field_name in (
                    "subject_content_digest",
                    "input_digest",
                    "policy_set_digest",
                    "binding_head_digest",
                    "catalog_version",
                    "ruleset_version",
                    "adopted_revisions",
                )
            ):
                raise GuidelinePolicyContractError("policy_receipt_summary_not_slim")
        elif any(
            getattr(self, field_name) is None
            for field_name in (
                "subject_content_digest",
                "input_digest",
                "policy_set_digest",
                "binding_head_digest",
                "catalog_version",
                "ruleset_version",
                "adopted_revisions",
            )
        ):
            raise GuidelinePolicyContractError("policy_receipt_detail_incomplete")

    @property
    def id(self) -> str:
        return self.receipt_id


def project_policy_compliance_receipt(
    receipt: PolicyComplianceReceipt,
    *,
    projection: PolicyProjection,
    current: PolicyComplianceCurrentSnapshot | None,
) -> PolicyComplianceReceiptListItem:
    if not isinstance(projection, PolicyProjection):
        raise GuidelinePolicyContractError("policy_receipt_projection_invalid")
    assessment = assess_policy_receipt_currentness(receipt, current)
    findings = receipt.findings
    detail = projection is PolicyProjection.DETAIL
    return PolicyComplianceReceiptListItem(
        projection=projection,
        receipt_id=receipt.receipt_id,
        subject=receipt.subject,
        outcome=receipt.outcome,
        state=receipt.state,
        currentness=assessment.currentness,
        currentness_reasons=assessment.reasons,
        evaluator_version=receipt.evaluator_version,
        evaluated_by=receipt.evaluated_by,
        evaluated_at=receipt.evaluated_at,
        finding_count=len(findings),
        rule_count=receipt.rule_count,
        failed_rule_count=receipt.failed_rule_count,
        error_rule_count=receipt.error_rule_count,
        blocking_finding_count=sum(1 for finding in findings if finding.blocking),
        waived_finding_count=sum(
            1 for finding in findings if finding.waiver_id is not None
        ),
        reason_codes=receipt.reason_codes,
        subject_content_digest=(receipt.subject_content_digest if detail else None),
        input_digest=receipt.input_digest if detail else None,
        policy_set_digest=receipt.policy_set_digest if detail else None,
        binding_head_digest=receipt.binding_head_digest if detail else None,
        catalog_version=receipt.catalog_version if detail else None,
        ruleset_version=receipt.ruleset_version if detail else None,
        adopted_revisions=receipt.adopted_revisions if detail else None,
    )


@dataclass(frozen=True, slots=True)
class PolicyComplianceFindingListItem:
    """Projection-safe finding row ordered by severity/rule/id."""

    projection: PolicyProjection
    finding_id: str
    receipt_id: str
    subject: PolicySubjectRef
    guideline_id: str
    revision_id: str
    rule_id: str
    outcome: PolicyEvaluationOutcome
    enforcement: GuidelineEnforcement
    severity_rank: int
    blocking: bool
    created_at: datetime
    message: str | None = None
    evidence_refs: tuple[str, ...] | None = None
    waiver_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.projection, PolicyProjection):
            raise GuidelinePolicyContractError("policy_finding_projection_invalid")
        if not isinstance(self.subject, PolicySubjectRef):
            raise GuidelinePolicyContractError(
                "policy_finding_projection_subject_invalid"
            )
        for field_name in (
            "finding_id",
            "receipt_id",
            "guideline_id",
            "revision_id",
            "rule_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"policy_finding_projection_{field_name}_required",
                ),
            )
        if (
            not isinstance(self.severity_rank, int)
            or isinstance(self.severity_rank, bool)
            or self.severity_rank < 0
        ):
            raise GuidelinePolicyContractError(
                "policy_finding_projection_severity_invalid"
            )
        if not isinstance(self.blocking, bool):
            raise GuidelinePolicyContractError(
                "policy_finding_projection_blocking_invalid"
            )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "policy_finding_projection_created_at_invalid",
            ),
        )
        if self.projection is PolicyProjection.SUMMARY:
            if (
                self.message is not None
                or self.evidence_refs is not None
                or self.waiver_id is not None
            ):
                raise GuidelinePolicyContractError("policy_finding_summary_not_slim")
        elif self.message is None or self.evidence_refs is None:
            raise GuidelinePolicyContractError("policy_finding_detail_incomplete")

    @property
    def id(self) -> str:
        return self.finding_id


def project_policy_compliance_finding(
    finding: PolicyComplianceFinding,
    *,
    projection: PolicyProjection,
) -> PolicyComplianceFindingListItem:
    if not isinstance(finding, PolicyComplianceFinding):
        raise GuidelinePolicyContractError("policy_finding_invalid")
    if not isinstance(projection, PolicyProjection):
        raise GuidelinePolicyContractError("policy_finding_projection_invalid")
    detail = projection is PolicyProjection.DETAIL
    return PolicyComplianceFindingListItem(
        projection=projection,
        finding_id=finding.finding_id,
        receipt_id=finding.receipt_id,
        subject=finding.subject,
        guideline_id=finding.guideline_id,
        revision_id=finding.revision_id,
        rule_id=finding.rule_id,
        outcome=finding.outcome,
        enforcement=finding.enforcement,
        severity_rank=policy_finding_severity_rank(finding),
        blocking=finding.blocking,
        created_at=finding.created_at,
        message=finding.message if detail else None,
        evidence_refs=finding.evidence_refs if detail else None,
        waiver_id=finding.waiver_id if detail else None,
    )


@dataclass(frozen=True, slots=True)
class PolicyWaiverListItem:
    """Projection-safe waiver head; summary excludes reasons and evidence."""

    projection: PolicyProjection
    waiver_id: str
    board_id: str
    finding_id: str
    receipt_id: str
    guideline_id: str
    revision_id: str
    rule_id: str
    subject: PolicySubjectRef
    status: PolicyWaiverStatus
    source_current: bool
    effective: bool
    requested_by: str
    requested_at: datetime
    expires_at: datetime
    waiver_revision: int
    last_event_at: datetime
    expire_reason_code: PolicyWaiverExpireReasonCode | None
    justification: str | None = None
    evidence_refs: tuple[str, ...] | None = None
    reviewed_by: str | None = None
    reviewed_at: datetime | None = None
    review_reason: str | None = None
    revoked_by: str | None = None
    revoked_at: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.projection, PolicyProjection):
            raise GuidelinePolicyContractError("policy_waiver_projection_invalid")
        if not isinstance(self.subject, PolicySubjectRef):
            raise GuidelinePolicyContractError(
                "policy_waiver_projection_subject_invalid"
            )
        if not isinstance(self.status, PolicyWaiverStatus):
            raise GuidelinePolicyContractError(
                "policy_waiver_projection_status_invalid"
            )
        if self.expire_reason_code is not None and not isinstance(
            self.expire_reason_code,
            PolicyWaiverExpireReasonCode,
        ):
            raise GuidelinePolicyContractError(
                "policy_waiver_projection_expire_reason_code_invalid"
            )
        if (self.status is PolicyWaiverStatus.EXPIRED) != (
            self.expire_reason_code is not None
        ):
            raise GuidelinePolicyContractError(
                "policy_waiver_projection_expire_reason_code_mismatch"
            )
        for field_name in (
            "waiver_id",
            "board_id",
            "finding_id",
            "receipt_id",
            "guideline_id",
            "revision_id",
            "rule_id",
            "requested_by",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"policy_waiver_projection_{field_name}_required",
                ),
            )
        if self.board_id != self.subject.board_id:
            raise GuidelinePolicyContractError(
                "policy_waiver_projection_board_mismatch"
            )
        for field_name in ("source_current", "effective"):
            if not isinstance(getattr(self, field_name), bool):
                raise GuidelinePolicyContractError(
                    f"policy_waiver_projection_{field_name}_invalid"
                )
        if self.effective and (
            not self.source_current or self.status is not PolicyWaiverStatus.APPROVED
        ):
            raise GuidelinePolicyContractError(
                "policy_waiver_projection_effective_invalid"
            )
        for field_name in (
            "requested_at",
            "expires_at",
            "last_event_at",
            "reviewed_at",
            "revoked_at",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    _aware_utc(
                        value,
                        f"policy_waiver_projection_{field_name}_invalid",
                    ),
                )
        if (
            not isinstance(self.waiver_revision, int)
            or isinstance(self.waiver_revision, bool)
            or self.waiver_revision < 1
        ):
            raise GuidelinePolicyContractError(
                "policy_waiver_projection_revision_invalid"
            )
        detail_fields = (
            "justification",
            "evidence_refs",
        )
        if self.projection is PolicyProjection.SUMMARY:
            if any(
                getattr(self, field_name) is not None for field_name in detail_fields
            ):
                raise GuidelinePolicyContractError("policy_waiver_summary_not_slim")
            if any(
                value is not None
                for value in (
                    self.reviewed_by,
                    self.reviewed_at,
                    self.review_reason,
                    self.revoked_by,
                    self.revoked_at,
                )
            ):
                raise GuidelinePolicyContractError("policy_waiver_summary_not_slim")
        elif any(getattr(self, field_name) is None for field_name in detail_fields):
            raise GuidelinePolicyContractError("policy_waiver_detail_incomplete")

    @property
    def id(self) -> str:
        return self.waiver_id

    @property
    def created_at(self) -> datetime:
        """Canonical list-ordering alias for the request timestamp."""

        return self.requested_at


def project_policy_waiver(
    waiver: PolicyWaiver,
    *,
    projection: PolicyProjection,
    source_current: bool,
    evaluated_at: datetime,
) -> PolicyWaiverListItem:
    if not isinstance(waiver, PolicyWaiver):
        raise GuidelinePolicyContractError("policy_waiver_invalid")
    if not isinstance(projection, PolicyProjection):
        raise GuidelinePolicyContractError("policy_waiver_projection_invalid")
    if not isinstance(source_current, bool):
        raise GuidelinePolicyContractError(
            "policy_waiver_projection_source_current_invalid"
        )
    now = _aware_utc(
        evaluated_at,
        "policy_waiver_projection_evaluated_at_invalid",
    )
    detail = projection is PolicyProjection.DETAIL
    return PolicyWaiverListItem(
        projection=projection,
        waiver_id=waiver.waiver_id,
        board_id=waiver.board_id,
        finding_id=waiver.finding_id,
        receipt_id=waiver.receipt_id,
        guideline_id=waiver.guideline_id,
        revision_id=waiver.revision_id,
        rule_id=waiver.rule_id,
        subject=waiver.subject,
        status=waiver.status,
        source_current=source_current,
        effective=source_current and waiver.is_effective_at(now),
        requested_by=waiver.requested_by,
        requested_at=waiver.requested_at,
        expires_at=waiver.expires_at,
        waiver_revision=waiver.waiver_revision,
        last_event_at=waiver.last_event_at,
        expire_reason_code=waiver.expire_reason_code,
        justification=waiver.justification if detail else None,
        evidence_refs=waiver.evidence_refs if detail else None,
        reviewed_by=waiver.reviewed_by if detail else None,
        reviewed_at=waiver.reviewed_at if detail else None,
        review_reason=waiver.review_reason if detail else None,
        revoked_by=waiver.revoked_by if detail else None,
        revoked_at=waiver.revoked_at if detail else None,
    )


POLICY_RECEIPT_ORDERING: tuple[str, str] = (
    "evaluated_at DESC",
    "receipt_id DESC",
)
POLICY_FINDING_ORDERING: tuple[str, str, str] = (
    "severity_rank DESC",
    "rule_id ASC",
    "finding_id ASC",
)
POLICY_WAIVER_ORDERING: tuple[str, str] = (
    "created_at DESC",
    "id DESC",
)
POLICY_IMPACT_ORDERING: tuple[str, str, str] = (
    "entity_type ASC",
    "entity_id ASC",
    "impact_item_id ASC",
)


@dataclass(frozen=True, slots=True)
class PolicyReceiptPageCursor:
    evaluated_at: datetime
    item_id: str
    filter_digest: str
    projection_digest: str
    schema_version: str = POLICY_KEYSET_CONTRACT_VERSION
    ordering: tuple[str, str] = POLICY_RECEIPT_ORDERING

    def __post_init__(self) -> None:
        if self.schema_version != POLICY_KEYSET_CONTRACT_VERSION:
            raise GuidelinePolicyContractError("policy_cursor_schema_version_invalid")
        if tuple(self.ordering) != POLICY_RECEIPT_ORDERING:
            raise GuidelinePolicyContractError("policy_receipt_cursor_ordering_invalid")
        object.__setattr__(
            self,
            "evaluated_at",
            _aware_utc(
                self.evaluated_at,
                "policy_receipt_cursor_time_invalid",
            ),
        )
        object.__setattr__(
            self,
            "item_id",
            _required_text(
                self.item_id,
                "policy_receipt_cursor_item_id_required",
            ),
        )
        for field_name in ("filter_digest", "projection_digest"):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"policy_receipt_cursor_{field_name}_invalid",
                ),
            )
        object.__setattr__(self, "ordering", POLICY_RECEIPT_ORDERING)


@dataclass(frozen=True, slots=True)
class PolicyFindingPageCursor:
    severity_rank: int
    rule_id: str
    item_id: str
    filter_digest: str
    projection_digest: str
    schema_version: str = POLICY_KEYSET_CONTRACT_VERSION
    ordering: tuple[str, str, str] = POLICY_FINDING_ORDERING

    def __post_init__(self) -> None:
        if self.schema_version != POLICY_KEYSET_CONTRACT_VERSION:
            raise GuidelinePolicyContractError("policy_cursor_schema_version_invalid")
        if tuple(self.ordering) != POLICY_FINDING_ORDERING:
            raise GuidelinePolicyContractError("policy_finding_cursor_ordering_invalid")
        if (
            not isinstance(self.severity_rank, int)
            or isinstance(self.severity_rank, bool)
            or self.severity_rank < 0
        ):
            raise GuidelinePolicyContractError("policy_finding_cursor_severity_invalid")
        for field_name in ("rule_id", "item_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"policy_finding_cursor_{field_name}_required",
                ),
            )
        for field_name in ("filter_digest", "projection_digest"):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"policy_finding_cursor_{field_name}_invalid",
                ),
            )
        object.__setattr__(self, "ordering", POLICY_FINDING_ORDERING)


@dataclass(frozen=True, slots=True)
class PolicyWaiverPageCursor:
    created_at: datetime
    item_id: str
    filter_digest: str
    projection_digest: str
    schema_version: str = POLICY_KEYSET_CONTRACT_VERSION
    ordering: tuple[str, str] = POLICY_WAIVER_ORDERING

    def __post_init__(self) -> None:
        if self.schema_version != POLICY_KEYSET_CONTRACT_VERSION:
            raise GuidelinePolicyContractError("policy_cursor_schema_version_invalid")
        if tuple(self.ordering) != POLICY_WAIVER_ORDERING:
            raise GuidelinePolicyContractError("policy_waiver_cursor_ordering_invalid")
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "policy_waiver_cursor_time_invalid",
            ),
        )
        object.__setattr__(
            self,
            "item_id",
            _required_text(
                self.item_id,
                "policy_waiver_cursor_item_id_required",
            ),
        )
        for field_name in ("filter_digest", "projection_digest"):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"policy_waiver_cursor_{field_name}_invalid",
                ),
            )
        object.__setattr__(self, "ordering", POLICY_WAIVER_ORDERING)


@dataclass(frozen=True, slots=True)
class PolicyImpactPageCursor:
    entity_type: str
    entity_id: str
    item_id: str
    filter_digest: str
    projection_digest: str
    schema_version: str = POLICY_KEYSET_CONTRACT_VERSION
    ordering: tuple[str, str, str] = POLICY_IMPACT_ORDERING

    def __post_init__(self) -> None:
        if self.schema_version != POLICY_KEYSET_CONTRACT_VERSION:
            raise GuidelinePolicyContractError("policy_cursor_schema_version_invalid")
        if tuple(self.ordering) != POLICY_IMPACT_ORDERING:
            raise GuidelinePolicyContractError("policy_impact_cursor_ordering_invalid")
        for field_name in ("entity_type", "entity_id", "item_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"policy_impact_cursor_{field_name}_required",
                ),
            )
        for field_name in ("filter_digest", "projection_digest"):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"policy_impact_cursor_{field_name}_invalid",
                ),
            )
        object.__setattr__(self, "ordering", POLICY_IMPACT_ORDERING)


_PolicyPageItemT = TypeVar("_PolicyPageItemT")
_PolicyPageCursorT = TypeVar("_PolicyPageCursorT")


@dataclass(frozen=True, slots=True)
class PolicyKeysetPage(Generic[_PolicyPageItemT, _PolicyPageCursorT]):
    items: tuple[_PolicyPageItemT, ...]
    limit: int
    next_cursor: _PolicyPageCursorT | None
    has_more: bool

    ordering: ClassVar[tuple[str, ...]] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple | list):
            raise GuidelinePolicyContractError("policy_page_items_invalid")
        object.__setattr__(self, "items", tuple(self.items))
        object.__setattr__(self, "limit", _positive_limit(self.limit))
        if not isinstance(self.has_more, bool):
            raise GuidelinePolicyContractError("policy_page_has_more_invalid")
        if len(self.items) > self.limit:
            raise GuidelinePolicyContractError("policy_page_over_limit")
        if self.has_more != (self.next_cursor is not None):
            raise GuidelinePolicyContractError("policy_page_cursor_mismatch")


@dataclass(frozen=True, slots=True)
class PolicyComplianceReceiptPage(
    PolicyKeysetPage[
        PolicyComplianceReceiptListItem,
        PolicyReceiptPageCursor,
    ]
):
    ordering: ClassVar[tuple[str, ...]] = POLICY_RECEIPT_ORDERING

    def __post_init__(self) -> None:
        PolicyKeysetPage.__post_init__(self)
        if any(
            not isinstance(item, PolicyComplianceReceiptListItem) for item in self.items
        ):
            raise GuidelinePolicyContractError("policy_receipt_page_item_invalid")
        if self.next_cursor is not None and not isinstance(
            self.next_cursor,
            PolicyReceiptPageCursor,
        ):
            raise GuidelinePolicyContractError("policy_receipt_page_cursor_invalid")


@dataclass(frozen=True, slots=True)
class PolicyComplianceFindingPage(
    PolicyKeysetPage[
        PolicyComplianceFindingListItem,
        PolicyFindingPageCursor,
    ]
):
    ordering: ClassVar[tuple[str, ...]] = POLICY_FINDING_ORDERING

    def __post_init__(self) -> None:
        PolicyKeysetPage.__post_init__(self)
        if any(
            not isinstance(item, PolicyComplianceFindingListItem) for item in self.items
        ):
            raise GuidelinePolicyContractError("policy_finding_page_item_invalid")
        if self.next_cursor is not None and not isinstance(
            self.next_cursor,
            PolicyFindingPageCursor,
        ):
            raise GuidelinePolicyContractError("policy_finding_page_cursor_invalid")


@dataclass(frozen=True, slots=True)
class PolicyWaiverPage(
    PolicyKeysetPage[
        PolicyWaiverListItem,
        PolicyWaiverPageCursor,
    ]
):
    ordering: ClassVar[tuple[str, ...]] = POLICY_WAIVER_ORDERING

    def __post_init__(self) -> None:
        PolicyKeysetPage.__post_init__(self)
        if any(not isinstance(item, PolicyWaiverListItem) for item in self.items):
            raise GuidelinePolicyContractError("policy_waiver_page_item_invalid")
        if self.next_cursor is not None and not isinstance(
            self.next_cursor,
            PolicyWaiverPageCursor,
        ):
            raise GuidelinePolicyContractError("policy_waiver_page_cursor_invalid")


@dataclass(frozen=True, slots=True)
class GuidelineImpactItemPage(
    PolicyKeysetPage[
        GuidelineImpactItem,
        PolicyImpactPageCursor,
    ]
):
    ordering: ClassVar[tuple[str, ...]] = POLICY_IMPACT_ORDERING

    def __post_init__(self) -> None:
        PolicyKeysetPage.__post_init__(self)
        if any(not isinstance(item, GuidelineImpactItem) for item in self.items):
            raise GuidelinePolicyContractError("policy_impact_page_item_invalid")
        if self.next_cursor is not None and not isinstance(
            self.next_cursor,
            PolicyImpactPageCursor,
        ):
            raise GuidelinePolicyContractError("policy_impact_page_cursor_invalid")


class PolicyCursorCodec:
    """HMAC-authenticated opaque transport codec for policy-keyset/v1."""

    def __init__(self, signing_key: bytes) -> None:
        if not isinstance(signing_key, bytes) or len(signing_key) < 32:
            raise GuidelinePolicyContractError("policy_cursor_signing_key_invalid")
        self._signing_key = signing_key

    @staticmethod
    def _b64encode(value: bytes) -> str:
        return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")

    @staticmethod
    def _b64decode(value: str) -> bytes:
        if not isinstance(value, str) or not value:
            raise GuidelinePolicyContractError("invalid_cursor")
        padding = "=" * (-len(value) % 4)
        try:
            decoded = base64.b64decode(
                value + padding,
                altchars=b"-_",
                validate=True,
            )
        except Exception as exc:
            raise GuidelinePolicyContractError("invalid_cursor") from exc
        if PolicyCursorCodec._b64encode(decoded) != value:
            raise GuidelinePolicyContractError("invalid_cursor")
        return decoded

    def encode(
        self,
        cursor: (
            GuidelineRevisionPageCursor
            | PolicyReceiptPageCursor
            | PolicyFindingPageCursor
            | PolicyWaiverPageCursor
            | PolicyImpactPageCursor
            | SemanticAssessmentPageCursor
            | SemanticFindingPageCursor
            | SemanticWaiverPageCursor
            | SemanticSkipPageCursor
        ),
    ) -> str:
        if isinstance(cursor, GuidelineRevisionPageCursor):
            payload: dict[str, object] = {
                "schema_version": cursor.schema_version,
                "kind": "revision",
                "ordering": list(cursor.ordering),
                "revision_number": cursor.revision_number,
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        elif isinstance(cursor, PolicyReceiptPageCursor):
            payload: dict[str, object] = {
                "schema_version": cursor.schema_version,
                "kind": "receipt",
                "ordering": list(cursor.ordering),
                "evaluated_at": cursor.evaluated_at.isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        elif isinstance(cursor, PolicyFindingPageCursor):
            payload = {
                "schema_version": cursor.schema_version,
                "kind": "finding",
                "ordering": list(cursor.ordering),
                "severity_rank": cursor.severity_rank,
                "rule_id": cursor.rule_id,
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        elif isinstance(cursor, PolicyWaiverPageCursor):
            payload = {
                "schema_version": cursor.schema_version,
                "kind": "waiver",
                "ordering": list(cursor.ordering),
                "created_at": cursor.created_at.isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        elif isinstance(cursor, PolicyImpactPageCursor):
            payload = {
                "schema_version": cursor.schema_version,
                "kind": "impact",
                "ordering": list(cursor.ordering),
                "entity_type": cursor.entity_type,
                "entity_id": cursor.entity_id,
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        elif isinstance(cursor, SemanticAssessmentPageCursor):
            payload = {
                "schema_version": cursor.schema_version,
                "kind": "semantic_assessment",
                "ordering": list(cursor.ordering),
                "recorded_at": cursor.recorded_at.isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        elif isinstance(cursor, SemanticFindingPageCursor):
            payload = {
                "schema_version": cursor.schema_version,
                "kind": "semantic_finding",
                "ordering": list(cursor.ordering),
                "created_at": cursor.created_at.isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        elif isinstance(cursor, SemanticWaiverPageCursor):
            payload = {
                "schema_version": cursor.schema_version,
                "kind": "semantic_waiver",
                "ordering": list(cursor.ordering),
                "requested_at": cursor.requested_at.isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        elif isinstance(cursor, SemanticSkipPageCursor):
            payload = {
                "schema_version": cursor.schema_version,
                "kind": "semantic_skip",
                "ordering": list(cursor.ordering),
                "created_at": cursor.created_at.isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        else:
            raise GuidelinePolicyContractError("invalid_cursor")
        encoded = json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        signature = hmac.new(
            self._signing_key,
            encoded,
            hashlib.sha256,
        ).digest()
        return f"{self._b64encode(encoded)}.{self._b64encode(signature)}"

    def decode(
        self,
        token: str,
        *,
        expected_kind: str,
    ) -> (
        GuidelineRevisionPageCursor
        | PolicyReceiptPageCursor
        | PolicyFindingPageCursor
        | PolicyWaiverPageCursor
        | PolicyImpactPageCursor
        | SemanticAssessmentPageCursor
        | SemanticFindingPageCursor
        | SemanticWaiverPageCursor
        | SemanticSkipPageCursor
    ):
        try:
            if (
                not isinstance(token, str)
                or not token
                or len(token) > POLICY_CURSOR_TOKEN_MAX_LENGTH
                or token.count(".") != 1
            ):
                raise GuidelinePolicyContractError("invalid_cursor")
            if expected_kind not in {
                "revision",
                "receipt",
                "finding",
                "waiver",
                "impact",
                "semantic_assessment",
                "semantic_finding",
                "semantic_waiver",
                "semantic_skip",
            }:
                raise GuidelinePolicyContractError("invalid_cursor")
            encoded_part, signature_part = token.split(".", 1)
            encoded = self._b64decode(encoded_part)
            signature = self._b64decode(signature_part)
            expected_signature = hmac.new(
                self._signing_key,
                encoded,
                hashlib.sha256,
            ).digest()
            if not hmac.compare_digest(signature, expected_signature):
                raise GuidelinePolicyContractError("invalid_cursor")
            payload = json.loads(encoded.decode("utf-8"))
            canonical_encoded = json.dumps(
                payload,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            if not hmac.compare_digest(encoded, canonical_encoded):
                raise GuidelinePolicyContractError("invalid_cursor")
            semantic_kind = expected_kind.startswith("semantic_")
            expected_schema_version = (
                SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION
                if semantic_kind
                else POLICY_KEYSET_CONTRACT_VERSION
            )
            if (
                not isinstance(payload, dict)
                or payload.get("schema_version") != expected_schema_version
                or payload.get("kind") != expected_kind
            ):
                raise GuidelinePolicyContractError("invalid_cursor")
            if expected_kind == "revision":
                if set(payload) != {
                    "schema_version",
                    "kind",
                    "ordering",
                    "revision_number",
                    "item_id",
                    "filter_digest",
                    "projection_digest",
                }:
                    raise GuidelinePolicyContractError("invalid_cursor")
                return GuidelineRevisionPageCursor(
                    revision_number=payload["revision_number"],
                    item_id=payload["item_id"],
                    filter_digest=payload["filter_digest"],
                    projection_digest=payload["projection_digest"],
                    schema_version=payload["schema_version"],
                    ordering=tuple(payload["ordering"]),
                )
            if expected_kind == "receipt":
                if set(payload) != {
                    "schema_version",
                    "kind",
                    "ordering",
                    "evaluated_at",
                    "item_id",
                    "filter_digest",
                    "projection_digest",
                }:
                    raise GuidelinePolicyContractError("invalid_cursor")
                timestamp = str(payload["evaluated_at"])
                if timestamp.endswith("Z"):
                    timestamp = timestamp[:-1] + "+00:00"
                return PolicyReceiptPageCursor(
                    evaluated_at=datetime.fromisoformat(timestamp),
                    item_id=payload["item_id"],
                    filter_digest=payload["filter_digest"],
                    projection_digest=payload["projection_digest"],
                    schema_version=payload["schema_version"],
                    ordering=tuple(payload["ordering"]),
                )
            if expected_kind == "waiver":
                if set(payload) != {
                    "schema_version",
                    "kind",
                    "ordering",
                    "created_at",
                    "item_id",
                    "filter_digest",
                    "projection_digest",
                }:
                    raise GuidelinePolicyContractError("invalid_cursor")
                timestamp = str(payload["created_at"])
                if timestamp.endswith("Z"):
                    timestamp = timestamp[:-1] + "+00:00"
                return PolicyWaiverPageCursor(
                    created_at=datetime.fromisoformat(timestamp),
                    item_id=payload["item_id"],
                    filter_digest=payload["filter_digest"],
                    projection_digest=payload["projection_digest"],
                    schema_version=payload["schema_version"],
                    ordering=tuple(payload["ordering"]),
                )
            if expected_kind == "impact":
                if set(payload) != {
                    "schema_version",
                    "kind",
                    "ordering",
                    "entity_type",
                    "entity_id",
                    "item_id",
                    "filter_digest",
                    "projection_digest",
                }:
                    raise GuidelinePolicyContractError("invalid_cursor")
                return PolicyImpactPageCursor(
                    entity_type=payload["entity_type"],
                    entity_id=payload["entity_id"],
                    item_id=payload["item_id"],
                    filter_digest=payload["filter_digest"],
                    projection_digest=payload["projection_digest"],
                    schema_version=payload["schema_version"],
                    ordering=tuple(payload["ordering"]),
                )
            if expected_kind in {
                "semantic_assessment",
                "semantic_finding",
                "semantic_waiver",
                "semantic_skip",
            }:
                timestamp_field = {
                    "semantic_assessment": "recorded_at",
                    "semantic_finding": "created_at",
                    "semantic_waiver": "requested_at",
                    "semantic_skip": "created_at",
                }[expected_kind]
                if set(payload) != {
                    "schema_version",
                    "kind",
                    "ordering",
                    timestamp_field,
                    "item_id",
                    "filter_digest",
                    "projection_digest",
                }:
                    raise GuidelinePolicyContractError("invalid_cursor")
                timestamp = str(payload[timestamp_field])
                if timestamp.endswith("Z"):
                    timestamp = timestamp[:-1] + "+00:00"
                cursor_type = {
                    "semantic_assessment": SemanticAssessmentPageCursor,
                    "semantic_finding": SemanticFindingPageCursor,
                    "semantic_waiver": SemanticWaiverPageCursor,
                    "semantic_skip": SemanticSkipPageCursor,
                }[expected_kind]
                semantic_cursor = cursor_type(
                    at=datetime.fromisoformat(timestamp),
                    item_id=payload["item_id"],
                    filter_digest=payload["filter_digest"],
                    projection_digest=payload["projection_digest"],
                    schema_version=payload["schema_version"],
                    ordering=tuple(payload["ordering"]),
                )
                if (
                    self.encode(semantic_cursor).split(".", 1)[0]
                    != encoded_part
                ):
                    raise GuidelinePolicyContractError("invalid_cursor")
                return semantic_cursor
            if set(payload) != {
                "schema_version",
                "kind",
                "ordering",
                "severity_rank",
                "rule_id",
                "item_id",
                "filter_digest",
                "projection_digest",
            }:
                raise GuidelinePolicyContractError("invalid_cursor")
            return PolicyFindingPageCursor(
                severity_rank=payload["severity_rank"],
                rule_id=payload["rule_id"],
                item_id=payload["item_id"],
                filter_digest=payload["filter_digest"],
                projection_digest=payload["projection_digest"],
                schema_version=payload["schema_version"],
                ordering=tuple(payload["ordering"]),
            )
        except GuidelinePolicyContractError:
            raise
        except Exception as exc:
            raise GuidelinePolicyContractError("invalid_cursor") from exc


__all__ = [
    "POLICY_IMPACT_ORDERING",
    "POLICY_KEYSET_CONTRACT_VERSION",
    "POLICY_CURSOR_TOKEN_MAX_LENGTH",
    "POLICY_WAIVER_ORDERING",
    "PolicyCursorCodec",
    "GuidelineImpactItemPage",
    "GuidelineRevisionListItem",
    "GuidelineRevisionProjectionPage",
    "PolicyImpactPageCursor",
    "PolicyKeysetPage",
    "PolicyProjection",
    "PolicyWaiverListItem",
    "PolicyWaiverPage",
    "PolicyWaiverPageCursor",
    "SemanticAssessmentPageCursor",
    "SemanticFindingPageCursor",
    "SemanticSkipPageCursor",
    "SemanticWaiverPageCursor",
    "project_policy_waiver",
    "project_guideline_revision",
]
