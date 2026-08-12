"""Exact semantic guideline waivers and human-owned binding skips.

Waivers are scoped to one immutable failed metric finding.  Skips are scoped
to one exact subject×binding configuration and may only be created or revoked
by an authenticated human surface.  Both lifecycles are append-only and use
revision/predecessor fences so adapters can enforce CAS without duplicating
domain rules.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_BINDING_ID_MAX_LENGTH,
    GUIDELINE_ID_MAX_LENGTH,
    GUIDELINE_REVISION_ID_MAX_LENGTH,
    POLICY_ACTOR_ID_MAX_LENGTH,
    POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
    POLICY_METRIC_CODE_MAX_LENGTH,
    POLICY_METRIC_ID_MAX_LENGTH,
    POLICY_RECEIPT_ID_MAX_LENGTH,
    BoardGuidelineBinding,
    GuidelineRevision,
    PolicyCurrentness,
    PolicySubjectRef,
    PolicySubjectSnapshot,
    normalize_policy_bounded_text,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentContractError,
)
from okto_pulse.core.domain.guideline_semantic_currentness import (
    SemanticAssessmentCurrentnessReason,
)
from okto_pulse.core.domain.guideline_semantic_findings import (
    SemanticMetricFinding,
)
from okto_pulse.core.domain.quality_assessment import EvidenceRef
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256


SEMANTIC_METRIC_WAIVER_CONTRACT_VERSION = "semantic-metric-waiver/v1"
SEMANTIC_METRIC_WAIVER_EVENT_CONTRACT_VERSION = "semantic-metric-waiver-event/v1"
SEMANTIC_POLICY_SKIP_CONTRACT_VERSION = "semantic-policy-skip/v1"
SEMANTIC_POLICY_SKIP_EVENT_CONTRACT_VERSION = "semantic-policy-skip-event/v1"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _required_text(
    value: object,
    code: str,
    *,
    max_length: int = 4096,
) -> str:
    if not isinstance(value, str):
        raise SemanticAssessmentContractError(code)
    normalized = value.strip()
    if not normalized or len(normalized) > max_length:
        raise SemanticAssessmentContractError(code)
    return normalized


def _optional_text(
    value: object,
    code: str,
    *,
    max_length: int = 4096,
) -> str | None:
    if value is None:
        return None
    return _required_text(value, code, max_length=max_length)


def _sha256(value: object, code: str) -> str:
    if not isinstance(value, str):
        raise SemanticAssessmentContractError(code)
    normalized = value.strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise SemanticAssessmentContractError(code)
    return normalized


def _positive_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise SemanticAssessmentContractError(code)
    return value


def _aware_utc(
    value: object,
    code: str,
    *,
    optional: bool = False,
) -> datetime | None:
    if value is None and optional:
        return None
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise SemanticAssessmentContractError(code)
    return value.astimezone(timezone.utc)


def _evidence_refs(
    value: object,
    code: str,
) -> tuple[EvidenceRef, ...]:
    if (
        not isinstance(value, tuple | list)
        or not value
        or any(not isinstance(item, EvidenceRef) for item in value)
    ):
        raise SemanticAssessmentContractError(code)
    resolved = tuple(value)
    if len(set(resolved)) != len(resolved):
        raise SemanticAssessmentContractError(code)
    return tuple(
        sorted(
            resolved,
            key=lambda item: (
                item.source_type,
                item.source_id,
                item.source_version,
                item.content_hash,
            ),
        )
    )


def _evidence_payload(item: EvidenceRef) -> dict[str, object]:
    return {
        "source_type": item.source_type,
        "source_id": item.source_id,
        "source_version": item.source_version,
        "content_hash": item.content_hash,
    }


class SemanticExceptionActorKind(str, Enum):
    HUMAN = "human"
    AGENT = "agent"


class SemanticMetricWaiverStatus(str, Enum):
    REQUESTED = "requested"
    APPROVED = "approved"
    REJECTED = "rejected"
    REVOKED = "revoked"
    EXPIRED = "expired"


class SemanticMetricWaiverEventType(str, Enum):
    REQUEST = "request"
    APPROVE = "approve"
    REJECT = "reject"
    REVOKE = "revoke"
    EXPIRE = "expire"
    REVALIDATE = "revalidate"


class SemanticMetricWaiverExpireReason(str, Enum):
    SCHEDULED_EXPIRY = "scheduled_expiry"
    SUBJECT_SCOPE_CHANGED = "subject_scope_changed"
    GUIDELINE_REVISION_CHANGED = "guideline_revision_changed"
    BINDING_CONFIGURATION_CHANGED = "binding_configuration_changed"
    METRIC_RESULT_CHANGED = "metric_result_changed"


class SemanticMetricWaiverRevalidationStatus(str, Enum):
    """Closed transport result for one append-only revalidation decision."""

    APPROVED = "approved"
    EXPIRED = "expired"
    ANCHOR_STALE = "anchor_stale"
    REVOKED = "revoked"


class SemanticMetricWaiverRevalidationReason(str, Enum):
    """Stable top-level reason; detailed drift stays separately auditable."""

    CURRENT = "current"
    SCHEDULED_EXPIRY = "scheduled_expiry"
    ANCHOR_MISSING = "anchor_missing"
    SUBJECT_SCOPE_CHANGED = "subject_scope_changed"
    GUIDELINE_REVISION_CHANGED = "guideline_revision_changed"
    BINDING_CONFIGURATION_CHANGED = "binding_configuration_changed"
    METRIC_RESULT_CHANGED = "metric_result_changed"
    REVOKED = "revoked"


class SemanticPolicySkipStatus(str, Enum):
    ACTIVE = "active"
    REVOKED = "revoked"


class SemanticPolicySkipEventType(str, Enum):
    CREATE = "create"
    REVOKE = "revoke"


def _semantic_metric_waiver_event_request_digest(
    *,
    event_type: SemanticMetricWaiverEventType,
    waiver_id: str,
    waiver_revision: int,
    scope_digest: str,
    reason: str,
    evidence_refs: tuple[EvidenceRef, ...],
    actor_id: str,
    expires_at: datetime | None,
    expire_reason: SemanticMetricWaiverExpireReason | None,
    idempotency_key: str,
    evaluated_at: datetime | None = None,
    revalidation_status: (SemanticMetricWaiverRevalidationStatus | None) = None,
    revalidation_current: bool | None = None,
    revalidation_reason_code: (SemanticMetricWaiverRevalidationReason | None) = None,
    currentness_reasons: tuple[SemanticAssessmentCurrentnessReason, ...] = (),
    scheduled_expiry_observed: bool = False,
) -> str:
    if event_type is SemanticMetricWaiverEventType.REQUEST:
        payload: dict[str, object] = {
            "contract": SEMANTIC_METRIC_WAIVER_EVENT_CONTRACT_VERSION,
            "event_type": "request",
            "scope_digest": scope_digest,
            "justification": reason,
            "evidence_refs": [_evidence_payload(item) for item in evidence_refs],
            "requested_by": actor_id,
            "expires_at": (expires_at.isoformat() if expires_at is not None else None),
            "idempotency_key": idempotency_key,
        }
    elif event_type is SemanticMetricWaiverEventType.REVALIDATE:
        payload = {
            "contract": SEMANTIC_METRIC_WAIVER_EVENT_CONTRACT_VERSION,
            "event_type": event_type.value,
            "waiver_id": waiver_id,
            "expected_waiver_revision": waiver_revision - 1,
            "actor_id": actor_id,
            "reason": reason,
            "evidence_refs": [_evidence_payload(item) for item in evidence_refs],
            "expires_at": (expires_at.isoformat() if expires_at is not None else None),
            "expire_reason": (
                expire_reason.value if expire_reason is not None else None
            ),
            "evaluated_at": (
                evaluated_at.isoformat() if evaluated_at is not None else None
            ),
            "revalidation_status": (
                revalidation_status.value if revalidation_status is not None else None
            ),
            "revalidation_current": revalidation_current,
            "revalidation_reason_code": (
                revalidation_reason_code.value
                if revalidation_reason_code is not None
                else None
            ),
            "currentness_reasons": [item.value for item in currentness_reasons],
            "scheduled_expiry_observed": scheduled_expiry_observed,
            "idempotency_key": idempotency_key,
        }
    else:
        payload = {
            "contract": SEMANTIC_METRIC_WAIVER_EVENT_CONTRACT_VERSION,
            "event_type": event_type.value,
            "waiver_id": waiver_id,
            "expected_waiver_revision": waiver_revision - 1,
            "actor_id": actor_id,
            "reason": reason,
            "evidence_refs": [_evidence_payload(item) for item in evidence_refs],
            "expires_at": (expires_at.isoformat() if expires_at is not None else None),
            "expire_reason": (
                expire_reason.value if expire_reason is not None else None
            ),
            "idempotency_key": idempotency_key,
        }
    return canonical_sha256(payload)


def _semantic_policy_skip_event_request_digest(
    *,
    event_type: SemanticPolicySkipEventType,
    skip_id: str,
    skip_revision: int,
    scope_digest: str,
    reason: str,
    actor_id: str,
    actor_kind: SemanticExceptionActorKind,
    idempotency_key: str,
) -> str:
    if event_type is SemanticPolicySkipEventType.CREATE:
        payload: dict[str, object] = {
            "contract": SEMANTIC_POLICY_SKIP_EVENT_CONTRACT_VERSION,
            "event_type": "create",
            "scope_digest": scope_digest,
            "reason": reason,
            "actor_id": actor_id,
            "actor_kind": actor_kind.value,
            "idempotency_key": idempotency_key,
        }
    else:
        payload = {
            "contract": SEMANTIC_POLICY_SKIP_EVENT_CONTRACT_VERSION,
            "event_type": "revoke",
            "skip_id": skip_id,
            "expected_skip_revision": skip_revision - 1,
            "scope_digest": scope_digest,
            "actor_id": actor_id,
            "actor_kind": actor_kind.value,
            "reason": reason,
            "idempotency_key": idempotency_key,
        }
    return canonical_sha256(payload)


@dataclass(frozen=True, slots=True)
class SemanticMetricWaiverAnchor:
    """Every immutable fence required to identify one failed finding."""

    metric_result_id: str
    metric_result_digest: str
    finding_id: str
    finding_digest: str
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
    assessment_assessor_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError(
                "semantic_waiver_anchor_subject_invalid"
            )
        for field_name, max_length in (
            ("metric_result_id", POLICY_RECEIPT_ID_MAX_LENGTH),
            ("finding_id", POLICY_RECEIPT_ID_MAX_LENGTH),
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
                    code=f"semantic_waiver_anchor_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "assessment_assessor_id",
            normalize_policy_bounded_text(
                self.assessment_assessor_id,
                max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                code=("semantic_waiver_anchor_assessment_assessor_id_invalid"),
            ),
        )
        object.__setattr__(
            self,
            "binding_revision",
            _positive_int(
                self.binding_revision,
                "semantic_waiver_anchor_binding_revision_invalid",
            ),
        )
        for field_name in (
            "metric_result_digest",
            "finding_digest",
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
                    f"semantic_waiver_anchor_{field_name}_invalid",
                ),
            )

    @classmethod
    def from_finding(
        cls,
        finding: SemanticMetricFinding,
        *,
        assessment_assessor_id: str,
    ) -> SemanticMetricWaiverAnchor:
        if not isinstance(finding, SemanticMetricFinding):
            raise SemanticAssessmentContractError("semantic_waiver_finding_invalid")
        return cls(
            metric_result_id=finding.metric_result_id,
            metric_result_digest=finding.metric_result_digest,
            finding_id=finding.finding_id,
            finding_digest=finding.finding_digest,
            receipt_id=finding.receipt_id,
            receipt_digest=finding.receipt_digest,
            subject=finding.subject,
            subject_content_digest=finding.subject_content_digest,
            guideline_id=finding.guideline_id,
            guideline_revision_id=finding.guideline_revision_id,
            guideline_revision_digest=finding.guideline_revision_digest,
            binding_id=finding.binding_id,
            binding_revision=finding.binding_revision,
            binding_configuration_digest=(finding.binding_configuration_digest),
            metric_id=finding.metric_id,
            metric_code=finding.metric_code,
            assessment_assessor_id=assessment_assessor_id,
        )

    def matches_finding(self, finding: SemanticMetricFinding) -> bool:
        return isinstance(
            finding, SemanticMetricFinding
        ) and self == SemanticMetricWaiverAnchor.from_finding(
            finding,
            assessment_assessor_id=self.assessment_assessor_id,
        )


def semantic_metric_waiver_scope_digest(
    anchor: SemanticMetricWaiverAnchor,
) -> str:
    if not isinstance(anchor, SemanticMetricWaiverAnchor):
        raise SemanticAssessmentContractError("semantic_waiver_anchor_invalid")
    return canonical_sha256(
        {
            "contract": SEMANTIC_METRIC_WAIVER_CONTRACT_VERSION,
            "metric_result_id": anchor.metric_result_id,
            "metric_result_digest": anchor.metric_result_digest,
            "finding_id": anchor.finding_id,
            "finding_digest": anchor.finding_digest,
            "receipt_id": anchor.receipt_id,
            "receipt_digest": anchor.receipt_digest,
            "assessment_assessor_id": anchor.assessment_assessor_id,
            "subject": {
                "board_id": anchor.subject.board_id,
                "subject_type": anchor.subject.entity_type.value,
                "subject_id": anchor.subject.subject_id,
                "subject_version": anchor.subject.subject_version,
                **(
                    {"subject_edition": anchor.subject.subject_edition}
                    if anchor.subject.subject_edition is not None
                    else {}
                ),
                "content_digest": anchor.subject_content_digest,
            },
            "guideline": {
                "guideline_id": anchor.guideline_id,
                "revision_id": anchor.guideline_revision_id,
                "revision_digest": anchor.guideline_revision_digest,
            },
            "binding": {
                "binding_id": anchor.binding_id,
                "binding_revision": anchor.binding_revision,
                "configuration_digest": (anchor.binding_configuration_digest),
            },
            "metric_id": anchor.metric_id,
            "metric_code": anchor.metric_code,
        }
    )


def semantic_metric_waiver_revalidation_conflict(
    current: SemanticMetricWaiver,
    candidates: Iterable[SemanticMetricWaiver],
    *,
    occurred_at: datetime,
) -> str | None:
    """Return the competing live waiver that makes revalidation ambiguous.

    Persistence adapters call this after acquiring the board serialization
    lock. A requested or approved *other* waiver on the same exact immutable
    anchor owns that scope until its expiry, so an older expired waiver cannot
    be resurrected alongside it.
    """

    if not isinstance(current, SemanticMetricWaiver):
        raise SemanticAssessmentContractError("semantic_waiver_current_invalid")
    instant = _aware_utc(
        occurred_at,
        "semantic_waiver_event_occurred_at_invalid",
    )
    for candidate in candidates:
        if not isinstance(candidate, SemanticMetricWaiver):
            raise SemanticAssessmentContractError("semantic_waiver_competitor_invalid")
        if (
            candidate.waiver_id != current.waiver_id
            and candidate.scope_digest == current.scope_digest
            and candidate.anchor == current.anchor
            and candidate.status
            in {
                SemanticMetricWaiverStatus.REQUESTED,
                SemanticMetricWaiverStatus.APPROVED,
            }
            and (candidate.expires_at is None or candidate.expires_at > instant)
        ):
            return candidate.waiver_id
    return None


def _validate_semantic_waiver_revalidation_decision(
    *,
    status: SemanticMetricWaiverRevalidationStatus,
    current: bool,
    reason: SemanticMetricWaiverRevalidationReason,
    currentness_reasons: tuple[SemanticAssessmentCurrentnessReason, ...],
    scheduled_expiry_observed: bool,
) -> None:
    if current != (status is SemanticMetricWaiverRevalidationStatus.APPROVED):
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_current_invalid"
        )
    allowed_reasons = {
        SemanticMetricWaiverRevalidationStatus.APPROVED: {
            SemanticMetricWaiverRevalidationReason.CURRENT,
        },
        SemanticMetricWaiverRevalidationStatus.EXPIRED: {
            SemanticMetricWaiverRevalidationReason.SCHEDULED_EXPIRY,
            SemanticMetricWaiverRevalidationReason.SUBJECT_SCOPE_CHANGED,
            SemanticMetricWaiverRevalidationReason.GUIDELINE_REVISION_CHANGED,
            SemanticMetricWaiverRevalidationReason.BINDING_CONFIGURATION_CHANGED,
            SemanticMetricWaiverRevalidationReason.METRIC_RESULT_CHANGED,
        },
        SemanticMetricWaiverRevalidationStatus.ANCHOR_STALE: {
            SemanticMetricWaiverRevalidationReason.ANCHOR_MISSING,
            SemanticMetricWaiverRevalidationReason.SUBJECT_SCOPE_CHANGED,
            SemanticMetricWaiverRevalidationReason.GUIDELINE_REVISION_CHANGED,
            SemanticMetricWaiverRevalidationReason.BINDING_CONFIGURATION_CHANGED,
            SemanticMetricWaiverRevalidationReason.METRIC_RESULT_CHANGED,
        },
        SemanticMetricWaiverRevalidationStatus.REVOKED: {
            SemanticMetricWaiverRevalidationReason.REVOKED,
        },
    }
    if reason not in allowed_reasons[status]:
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_reason_invalid"
        )
    if (
        status
        in {
            SemanticMetricWaiverRevalidationStatus.APPROVED,
            SemanticMetricWaiverRevalidationStatus.EXPIRED,
        }
        and currentness_reasons
    ):
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_currentness_reasons_invalid"
        )
    if (
        status is SemanticMetricWaiverRevalidationStatus.APPROVED
        and scheduled_expiry_observed
    ):
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_expiry_observed_invalid"
        )


@dataclass(frozen=True, slots=True)
class SemanticMetricWaiver:
    """Materialized append-only waiver head."""

    waiver_id: str
    anchor: SemanticMetricWaiverAnchor
    scope_digest: str
    justification: str
    evidence_refs: tuple[EvidenceRef, ...]
    requested_by: str
    requested_at: datetime
    original_expires_at: datetime | None
    status: SemanticMetricWaiverStatus
    waiver_revision: int
    expires_at: datetime | None
    last_event_id: str
    last_event_type: SemanticMetricWaiverEventType
    last_event_at: datetime
    last_event_idempotency_key: str
    reviewed_by: str | None = None
    reviewed_at: datetime | None = None
    review_reason: str | None = None
    revoked_by: str | None = None
    revoked_at: datetime | None = None
    expire_reason: SemanticMetricWaiverExpireReason | None = None
    last_revalidation_status: SemanticMetricWaiverRevalidationStatus | None = None
    last_revalidation_current: bool | None = None
    last_revalidation_reason_code: SemanticMetricWaiverRevalidationReason | None = None
    last_revalidation_evaluated_at: datetime | None = None
    last_revalidation_currentness_reasons: tuple[
        SemanticAssessmentCurrentnessReason, ...
    ] = ()
    last_revalidation_scheduled_expiry_observed: bool = False
    head_digest: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.anchor, SemanticMetricWaiverAnchor):
            raise SemanticAssessmentContractError("semantic_waiver_anchor_invalid")
        object.__setattr__(
            self,
            "waiver_id",
            normalize_policy_bounded_text(
                self.waiver_id,
                max_length=POLICY_RECEIPT_ID_MAX_LENGTH,
                code="semantic_waiver_id_required",
            ),
        )
        expected_scope = semantic_metric_waiver_scope_digest(self.anchor)
        supplied_scope = _sha256(
            self.scope_digest,
            "semantic_waiver_scope_digest_invalid",
        )
        if supplied_scope != expected_scope:
            raise SemanticAssessmentContractError(
                "semantic_waiver_scope_digest_mismatch"
            )
        object.__setattr__(self, "scope_digest", supplied_scope)
        object.__setattr__(
            self,
            "justification",
            _required_text(
                self.justification,
                "semantic_waiver_justification_required",
            ),
        )
        object.__setattr__(
            self,
            "evidence_refs",
            _evidence_refs(
                self.evidence_refs,
                "semantic_waiver_evidence_required",
            ),
        )
        object.__setattr__(
            self,
            "requested_by",
            normalize_policy_bounded_text(
                self.requested_by,
                max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                code="semantic_waiver_requested_by_required",
            ),
        )
        object.__setattr__(
            self,
            "requested_at",
            _aware_utc(
                self.requested_at,
                "semantic_waiver_requested_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "original_expires_at",
            _aware_utc(
                self.original_expires_at,
                "semantic_waiver_original_expires_at_invalid",
                optional=True,
            ),
        )
        if not isinstance(self.status, SemanticMetricWaiverStatus):
            raise SemanticAssessmentContractError("semantic_waiver_status_invalid")
        object.__setattr__(
            self,
            "waiver_revision",
            _positive_int(
                self.waiver_revision,
                "semantic_waiver_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "expires_at",
            _aware_utc(
                self.expires_at,
                "semantic_waiver_expires_at_invalid",
                optional=True,
            ),
        )
        object.__setattr__(
            self,
            "last_event_id",
            normalize_policy_bounded_text(
                self.last_event_id,
                max_length=POLICY_RECEIPT_ID_MAX_LENGTH,
                code="semantic_waiver_last_event_id_required",
            ),
        )
        if not isinstance(
            self.last_event_type,
            SemanticMetricWaiverEventType,
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_last_event_type_invalid"
            )
        object.__setattr__(
            self,
            "last_event_at",
            _aware_utc(
                self.last_event_at,
                "semantic_waiver_last_event_at_invalid",
            ),
        )
        if (
            self.original_expires_at is not None
            and self.original_expires_at <= self.requested_at
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_original_expiry_not_future"
            )
        if self.expires_at is not None and self.expires_at <= self.requested_at:
            raise SemanticAssessmentContractError(
                "semantic_waiver_expiry_not_after_request"
            )
        if self.last_event_at < self.requested_at:
            raise SemanticAssessmentContractError(
                "semantic_waiver_last_event_time_regressed"
            )
        object.__setattr__(
            self,
            "last_event_idempotency_key",
            normalize_policy_bounded_text(
                self.last_event_idempotency_key,
                max_length=POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                code="semantic_waiver_last_event_idempotency_key_required",
            ),
        )
        object.__setattr__(
            self,
            "reviewed_by",
            (
                normalize_policy_bounded_text(
                    self.reviewed_by,
                    max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                    code="semantic_waiver_reviewed_by_invalid",
                )
                if self.reviewed_by is not None
                else None
            ),
        )
        object.__setattr__(
            self,
            "reviewed_at",
            _aware_utc(
                self.reviewed_at,
                "semantic_waiver_reviewed_at_invalid",
                optional=True,
            ),
        )
        object.__setattr__(
            self,
            "review_reason",
            _optional_text(
                self.review_reason,
                "semantic_waiver_review_reason_invalid",
            ),
        )
        object.__setattr__(
            self,
            "revoked_by",
            (
                normalize_policy_bounded_text(
                    self.revoked_by,
                    max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                    code="semantic_waiver_revoked_by_invalid",
                )
                if self.revoked_by is not None
                else None
            ),
        )
        object.__setattr__(
            self,
            "revoked_at",
            _aware_utc(
                self.revoked_at,
                "semantic_waiver_revoked_at_invalid",
                optional=True,
            ),
        )
        if self.expire_reason is not None and not isinstance(
            self.expire_reason,
            SemanticMetricWaiverExpireReason,
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_expire_reason_invalid"
            )
        self._validate_last_revalidation()

        reviewed = (
            self.reviewed_by is not None
            and self.reviewed_at is not None
            and self.review_reason is not None
        )
        if self.reviewed_at is not None and (
            self.reviewed_at < self.requested_at
            or self.reviewed_at > self.last_event_at
        ):
            raise SemanticAssessmentContractError("semantic_waiver_review_time_invalid")
        if self.revoked_at is not None and (
            self.revoked_at < self.requested_at or self.revoked_at > self.last_event_at
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_revocation_time_invalid"
            )
        if self.status is SemanticMetricWaiverStatus.REQUESTED:
            if reviewed or any(
                value is not None
                for value in (
                    self.revoked_by,
                    self.revoked_at,
                    self.expire_reason,
                )
            ):
                raise SemanticAssessmentContractError(
                    "semantic_waiver_requested_state_invalid"
                )
        else:
            if not reviewed or self.reviewed_by == self.requested_by:
                raise SemanticAssessmentContractError(
                    "semantic_waiver_independent_review_required"
                )
        if self.status is SemanticMetricWaiverStatus.REVOKED:
            if self.revoked_by is None or self.revoked_at is None:
                raise SemanticAssessmentContractError(
                    "semantic_waiver_revocation_required"
                )
        elif self.revoked_by is not None or self.revoked_at is not None:
            raise SemanticAssessmentContractError("semantic_waiver_revocation_invalid")
        if self.status is SemanticMetricWaiverStatus.EXPIRED:
            if self.expire_reason is None:
                raise SemanticAssessmentContractError(
                    "semantic_waiver_expire_reason_required"
                )
        elif self.expire_reason is not None:
            raise SemanticAssessmentContractError(
                "semantic_waiver_expire_reason_invalid"
            )
        expected_last_event_types = {
            SemanticMetricWaiverStatus.REQUESTED: {
                SemanticMetricWaiverEventType.REQUEST,
            },
            SemanticMetricWaiverStatus.APPROVED: {
                SemanticMetricWaiverEventType.APPROVE,
                SemanticMetricWaiverEventType.REVALIDATE,
            },
            SemanticMetricWaiverStatus.REJECTED: {
                SemanticMetricWaiverEventType.REJECT,
            },
            SemanticMetricWaiverStatus.REVOKED: {
                SemanticMetricWaiverEventType.REVOKE,
                SemanticMetricWaiverEventType.REVALIDATE,
            },
            SemanticMetricWaiverStatus.EXPIRED: {
                SemanticMetricWaiverEventType.EXPIRE,
                SemanticMetricWaiverEventType.REVALIDATE,
            },
        }
        if self.last_event_type not in expected_last_event_types[self.status]:
            raise SemanticAssessmentContractError(
                "semantic_waiver_last_event_state_mismatch"
            )
        if self.status is SemanticMetricWaiverStatus.REQUESTED and (
            self.waiver_revision != 1 or self.last_event_at != self.requested_at
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_requested_fence_invalid"
            )
        minimum_revisions = {
            SemanticMetricWaiverStatus.REQUESTED: 1,
            SemanticMetricWaiverStatus.APPROVED: 2,
            SemanticMetricWaiverStatus.REJECTED: 2,
            SemanticMetricWaiverStatus.REVOKED: 3,
            SemanticMetricWaiverStatus.EXPIRED: 3,
        }
        if self.waiver_revision < minimum_revisions[self.status] or (
            self.status is SemanticMetricWaiverStatus.REJECTED
            and self.waiver_revision != 2
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_state_revision_invalid"
            )
        if (
            self.last_event_type
            in {
                SemanticMetricWaiverEventType.APPROVE,
                SemanticMetricWaiverEventType.REJECT,
            }
            and self.reviewed_at != self.last_event_at
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_review_event_time_mismatch"
            )
        if (
            self.status is SemanticMetricWaiverStatus.APPROVED
            and self.expires_at is not None
            and self.expires_at <= self.last_event_at
            and not (
                self.last_event_type is SemanticMetricWaiverEventType.REVALIDATE
                and self.last_revalidation_status
                is SemanticMetricWaiverRevalidationStatus.ANCHOR_STALE
            )
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_approved_expiry_not_future"
            )
        expected_head = semantic_metric_waiver_head_digest(self)
        if self.head_digest is not None:
            supplied_head = _sha256(
                self.head_digest,
                "semantic_waiver_head_digest_invalid",
            )
            if supplied_head != expected_head:
                raise SemanticAssessmentContractError(
                    "semantic_waiver_head_digest_mismatch"
                )
        object.__setattr__(self, "head_digest", expected_head)

    def _validate_last_revalidation(self) -> None:
        status = self.last_revalidation_status
        current = self.last_revalidation_current
        reason = self.last_revalidation_reason_code
        evaluated_at = self.last_revalidation_evaluated_at
        reasons = self.last_revalidation_currentness_reasons
        expiry_observed = self.last_revalidation_scheduled_expiry_observed
        if not isinstance(reasons, tuple) or any(
            not isinstance(item, SemanticAssessmentCurrentnessReason)
            for item in reasons
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_revalidation_currentness_reasons_invalid"
            )
        if len(set(reasons)) != len(reasons):
            raise SemanticAssessmentContractError(
                "semantic_waiver_revalidation_currentness_reasons_duplicate"
            )
        canonical_reasons = tuple(
            item for item in SemanticAssessmentCurrentnessReason if item in reasons
        )
        if reasons != canonical_reasons:
            raise SemanticAssessmentContractError(
                "semantic_waiver_revalidation_currentness_reasons_order_invalid"
            )
        if not isinstance(expiry_observed, bool):
            raise SemanticAssessmentContractError(
                "semantic_waiver_revalidation_expiry_observed_invalid"
            )
        values_absent = (
            status is None
            and current is None
            and reason is None
            and evaluated_at is None
            and not reasons
            and not expiry_observed
        )
        if values_absent:
            return
        if (
            not isinstance(status, SemanticMetricWaiverRevalidationStatus)
            or not isinstance(current, bool)
            or not isinstance(
                reason,
                SemanticMetricWaiverRevalidationReason,
            )
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_revalidation_decision_invalid"
            )
        object.__setattr__(
            self,
            "last_revalidation_evaluated_at",
            _aware_utc(
                evaluated_at,
                "semantic_waiver_revalidation_evaluated_at_invalid",
            ),
        )
        _validate_semantic_waiver_revalidation_decision(
            status=status,
            current=current,
            reason=reason,
            currentness_reasons=reasons,
            scheduled_expiry_observed=expiry_observed,
        )

    def is_active_for(
        self,
        finding: SemanticMetricFinding,
        *,
        currentness: PolicyCurrentness,
        at: datetime,
    ) -> bool:
        at = _aware_utc(at, "semantic_waiver_check_at_invalid")
        return bool(
            self.status is SemanticMetricWaiverStatus.APPROVED
            and currentness is PolicyCurrentness.CURRENT
            and self.anchor.matches_finding(finding)
            and (self.expires_at is None or at < self.expires_at)
        )


def semantic_metric_waiver_head_digest(
    waiver: SemanticMetricWaiver,
) -> str:
    return canonical_sha256(
        {
            "contract": SEMANTIC_METRIC_WAIVER_CONTRACT_VERSION,
            "waiver_id": waiver.waiver_id,
            "scope_digest": waiver.scope_digest,
            "justification": waiver.justification,
            "evidence_refs": [_evidence_payload(item) for item in waiver.evidence_refs],
            "requested_by": waiver.requested_by,
            "requested_at": waiver.requested_at.isoformat(),
            "original_expires_at": (
                waiver.original_expires_at.isoformat()
                if waiver.original_expires_at is not None
                else None
            ),
            "status": waiver.status.value,
            "waiver_revision": waiver.waiver_revision,
            "expires_at": (
                waiver.expires_at.isoformat() if waiver.expires_at is not None else None
            ),
            "last_event_id": waiver.last_event_id,
            "last_event_type": waiver.last_event_type.value,
            "last_event_at": waiver.last_event_at.isoformat(),
            "last_event_idempotency_key": (waiver.last_event_idempotency_key),
            "reviewed_by": waiver.reviewed_by,
            "reviewed_at": (
                waiver.reviewed_at.isoformat()
                if waiver.reviewed_at is not None
                else None
            ),
            "review_reason": waiver.review_reason,
            "revoked_by": waiver.revoked_by,
            "revoked_at": (
                waiver.revoked_at.isoformat() if waiver.revoked_at is not None else None
            ),
            "expire_reason": (
                waiver.expire_reason.value if waiver.expire_reason is not None else None
            ),
            "last_revalidation": (
                {
                    "status": waiver.last_revalidation_status.value,
                    "current": waiver.last_revalidation_current,
                    "reason_code": (waiver.last_revalidation_reason_code.value),
                    "evaluated_at": (waiver.last_revalidation_evaluated_at.isoformat()),
                    "currentness_reasons": [
                        item.value
                        for item in (waiver.last_revalidation_currentness_reasons)
                    ],
                    "scheduled_expiry_observed": (
                        waiver.last_revalidation_scheduled_expiry_observed
                    ),
                }
                if waiver.last_revalidation_status is not None
                else None
            ),
        }
    )


@dataclass(frozen=True, slots=True)
class SemanticMetricWaiverEvent:
    event_id: str
    predecessor_event_id: str | None
    waiver_id: str
    waiver_revision: int
    event_type: SemanticMetricWaiverEventType
    from_status: SemanticMetricWaiverStatus | None
    to_status: SemanticMetricWaiverStatus
    actor_id: str
    occurred_at: datetime
    reason: str
    evidence_refs: tuple[EvidenceRef, ...]
    expires_at: datetime | None
    scope_digest: str
    waiver_digest: str
    idempotency_key: str
    request_digest: str
    expire_reason: SemanticMetricWaiverExpireReason | None = None
    evaluated_at: datetime | None = None
    revalidation_status: SemanticMetricWaiverRevalidationStatus | None = None
    revalidation_current: bool | None = None
    revalidation_reason_code: SemanticMetricWaiverRevalidationReason | None = None
    currentness_reasons: tuple[SemanticAssessmentCurrentnessReason, ...] = ()
    scheduled_expiry_observed: bool = False

    def __post_init__(self) -> None:
        for field_name in ("event_id", "waiver_id"):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=POLICY_RECEIPT_ID_MAX_LENGTH,
                    code=f"semantic_waiver_event_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "predecessor_event_id",
            (
                normalize_policy_bounded_text(
                    self.predecessor_event_id,
                    max_length=POLICY_RECEIPT_ID_MAX_LENGTH,
                    code="semantic_waiver_event_predecessor_invalid",
                )
                if self.predecessor_event_id is not None
                else None
            ),
        )
        object.__setattr__(
            self,
            "waiver_revision",
            _positive_int(
                self.waiver_revision,
                "semantic_waiver_event_revision_invalid",
            ),
        )
        if not isinstance(self.event_type, SemanticMetricWaiverEventType):
            raise SemanticAssessmentContractError("semantic_waiver_event_type_invalid")
        if self.from_status is not None and not isinstance(
            self.from_status,
            SemanticMetricWaiverStatus,
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_event_from_status_invalid"
            )
        if not isinstance(self.to_status, SemanticMetricWaiverStatus):
            raise SemanticAssessmentContractError(
                "semantic_waiver_event_to_status_invalid"
            )
        object.__setattr__(
            self,
            "actor_id",
            normalize_policy_bounded_text(
                self.actor_id,
                max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                code="semantic_waiver_event_actor_required",
            ),
        )
        object.__setattr__(
            self,
            "occurred_at",
            _aware_utc(
                self.occurred_at,
                "semantic_waiver_event_occurred_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "reason",
            _required_text(
                self.reason,
                "semantic_waiver_event_reason_required",
            ),
        )
        object.__setattr__(
            self,
            "evidence_refs",
            _evidence_refs(
                self.evidence_refs,
                "semantic_waiver_event_evidence_required",
            ),
        )
        object.__setattr__(
            self,
            "expires_at",
            _aware_utc(
                self.expires_at,
                "semantic_waiver_event_expires_at_invalid",
                optional=True,
            ),
        )
        for field_name in ("scope_digest", "waiver_digest", "request_digest"):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"semantic_waiver_event_{field_name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "idempotency_key",
            normalize_policy_bounded_text(
                self.idempotency_key,
                max_length=POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                code="semantic_waiver_event_idempotency_key_required",
            ),
        )
        if self.expire_reason is not None and not isinstance(
            self.expire_reason,
            SemanticMetricWaiverExpireReason,
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_event_expire_reason_invalid"
            )
        self._validate_revalidation()
        expected = {
            SemanticMetricWaiverEventType.REQUEST: (
                None,
                SemanticMetricWaiverStatus.REQUESTED,
            ),
            SemanticMetricWaiverEventType.APPROVE: (
                SemanticMetricWaiverStatus.REQUESTED,
                SemanticMetricWaiverStatus.APPROVED,
            ),
            SemanticMetricWaiverEventType.REJECT: (
                SemanticMetricWaiverStatus.REQUESTED,
                SemanticMetricWaiverStatus.REJECTED,
            ),
            SemanticMetricWaiverEventType.REVOKE: (
                SemanticMetricWaiverStatus.APPROVED,
                SemanticMetricWaiverStatus.REVOKED,
            ),
            SemanticMetricWaiverEventType.EXPIRE: (
                SemanticMetricWaiverStatus.APPROVED,
                SemanticMetricWaiverStatus.EXPIRED,
            ),
        }
        if self.event_type is SemanticMetricWaiverEventType.REVALIDATE:
            valid = (
                self.from_status
                in {
                    SemanticMetricWaiverStatus.APPROVED,
                    SemanticMetricWaiverStatus.EXPIRED,
                    SemanticMetricWaiverStatus.REVOKED,
                }
                and self.to_status
                in {
                    self.from_status,
                    SemanticMetricWaiverStatus.EXPIRED,
                }
                and not (
                    self.from_status is SemanticMetricWaiverStatus.EXPIRED
                    and self.to_status is not SemanticMetricWaiverStatus.EXPIRED
                )
                and not (
                    self.from_status is SemanticMetricWaiverStatus.REVOKED
                    and self.to_status is not SemanticMetricWaiverStatus.REVOKED
                )
            )
        else:
            valid = expected[self.event_type] == (
                self.from_status,
                self.to_status,
            )
        if not valid:
            raise SemanticAssessmentContractError(
                "semantic_waiver_event_transition_invalid"
            )
        if self.event_type is SemanticMetricWaiverEventType.REQUEST:
            if self.predecessor_event_id is not None or self.waiver_revision != 1:
                raise SemanticAssessmentContractError(
                    "semantic_waiver_event_initial_fence_invalid"
                )
        elif self.predecessor_event_id is None or self.waiver_revision < 2:
            raise SemanticAssessmentContractError(
                "semantic_waiver_event_predecessor_required"
            )
        expected_revision = {
            SemanticMetricWaiverEventType.REQUEST: 1,
            SemanticMetricWaiverEventType.APPROVE: 2,
            SemanticMetricWaiverEventType.REJECT: 2,
        }
        if (
            self.event_type in expected_revision
            and self.waiver_revision != expected_revision[self.event_type]
        ) or (
            self.event_type
            in {
                SemanticMetricWaiverEventType.REVOKE,
                SemanticMetricWaiverEventType.EXPIRE,
                SemanticMetricWaiverEventType.REVALIDATE,
            }
            and self.waiver_revision < 3
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_event_revision_transition_invalid"
            )
        expire_reason_expected = (
            self.event_type is SemanticMetricWaiverEventType.EXPIRE
            or (
                self.event_type is SemanticMetricWaiverEventType.REVALIDATE
                and self.to_status is SemanticMetricWaiverStatus.EXPIRED
            )
        )
        if expire_reason_expected != (self.expire_reason is not None):
            raise SemanticAssessmentContractError(
                "semantic_waiver_event_expire_reason_invalid"
            )
        if self.expire_reason is SemanticMetricWaiverExpireReason.SCHEDULED_EXPIRY and (
            self.expires_at is None
            or (
                (
                    self.evaluated_at
                    if self.event_type is SemanticMetricWaiverEventType.REVALIDATE
                    else self.occurred_at
                )
                < self.expires_at
            )
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_event_scheduled_expiry_invalid"
            )
        expected_request_digest = _semantic_metric_waiver_event_request_digest(
            event_type=self.event_type,
            waiver_id=self.waiver_id,
            waiver_revision=self.waiver_revision,
            scope_digest=self.scope_digest,
            reason=self.reason,
            evidence_refs=self.evidence_refs,
            actor_id=self.actor_id,
            expires_at=self.expires_at,
            expire_reason=self.expire_reason,
            idempotency_key=self.idempotency_key,
            evaluated_at=self.evaluated_at,
            revalidation_status=self.revalidation_status,
            revalidation_current=self.revalidation_current,
            revalidation_reason_code=self.revalidation_reason_code,
            currentness_reasons=self.currentness_reasons,
            scheduled_expiry_observed=(self.scheduled_expiry_observed),
        )
        if self.request_digest != expected_request_digest:
            raise SemanticAssessmentContractError(
                "semantic_waiver_event_request_digest_mismatch"
            )

    def _validate_revalidation(self) -> None:
        fields_absent = (
            self.evaluated_at is None
            and self.revalidation_status is None
            and self.revalidation_current is None
            and self.revalidation_reason_code is None
            and not self.currentness_reasons
            and not self.scheduled_expiry_observed
        )
        if self.event_type is not SemanticMetricWaiverEventType.REVALIDATE:
            if not fields_absent:
                raise SemanticAssessmentContractError(
                    "semantic_waiver_event_revalidation_fields_not_allowed"
                )
            return
        if (
            not isinstance(
                self.revalidation_status,
                SemanticMetricWaiverRevalidationStatus,
            )
            or not isinstance(self.revalidation_current, bool)
            or not isinstance(
                self.revalidation_reason_code,
                SemanticMetricWaiverRevalidationReason,
            )
            or not isinstance(self.currentness_reasons, tuple)
            or any(
                not isinstance(
                    reason,
                    SemanticAssessmentCurrentnessReason,
                )
                for reason in self.currentness_reasons
            )
            or not isinstance(self.scheduled_expiry_observed, bool)
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_event_revalidation_fields_invalid"
            )
        object.__setattr__(
            self,
            "evaluated_at",
            _aware_utc(
                self.evaluated_at,
                "semantic_waiver_revalidation_evaluated_at_invalid",
            ),
        )
        if len(set(self.currentness_reasons)) != len(self.currentness_reasons):
            raise SemanticAssessmentContractError(
                "semantic_waiver_revalidation_currentness_reasons_duplicate"
            )
        canonical_reasons = tuple(
            item
            for item in SemanticAssessmentCurrentnessReason
            if item in self.currentness_reasons
        )
        if self.currentness_reasons != canonical_reasons:
            raise SemanticAssessmentContractError(
                "semantic_waiver_revalidation_currentness_reasons_order_invalid"
            )
        _validate_semantic_waiver_revalidation_decision(
            status=self.revalidation_status,
            current=self.revalidation_current,
            reason=self.revalidation_reason_code,
            currentness_reasons=self.currentness_reasons,
            scheduled_expiry_observed=self.scheduled_expiry_observed,
        )


@dataclass(frozen=True, slots=True)
class SemanticMetricWaiverMutation:
    waiver: SemanticMetricWaiver
    event: SemanticMetricWaiverEvent

    def __post_init__(self) -> None:
        if not isinstance(self.waiver, SemanticMetricWaiver) or not isinstance(
            self.event,
            SemanticMetricWaiverEvent,
        ):
            raise SemanticAssessmentContractError("semantic_waiver_mutation_invalid")
        if (
            self.waiver.waiver_id != self.event.waiver_id
            or self.waiver.waiver_revision != self.event.waiver_revision
            or self.waiver.last_event_id != self.event.event_id
            or self.waiver.status is not self.event.to_status
            or self.waiver.scope_digest != self.event.scope_digest
            or self.waiver.head_digest != self.event.waiver_digest
            or self.waiver.last_event_idempotency_key != self.event.idempotency_key
        ):
            raise SemanticAssessmentContractError(
                "semantic_waiver_mutation_inconsistent"
            )
        if self.event.event_type is SemanticMetricWaiverEventType.REVALIDATE:
            if (
                self.waiver.last_revalidation_status
                is not self.event.revalidation_status
                or self.waiver.last_revalidation_current
                != self.event.revalidation_current
                or self.waiver.last_revalidation_reason_code
                is not self.event.revalidation_reason_code
                or self.waiver.last_revalidation_evaluated_at != self.event.evaluated_at
                or self.waiver.last_revalidation_currentness_reasons
                != self.event.currentness_reasons
                or (self.waiver.last_revalidation_scheduled_expiry_observed)
                != self.event.scheduled_expiry_observed
            ):
                raise SemanticAssessmentContractError(
                    "semantic_waiver_revalidation_mutation_inconsistent"
                )


def request_semantic_metric_waiver(
    *,
    waiver_id: str,
    event_id: str,
    anchor: SemanticMetricWaiverAnchor,
    justification: str,
    evidence_refs: tuple[EvidenceRef, ...] | list[EvidenceRef],
    requested_by: str,
    requested_at: datetime,
    expires_at: datetime | None,
    idempotency_key: str,
) -> SemanticMetricWaiverMutation:
    if not isinstance(anchor, SemanticMetricWaiverAnchor):
        raise SemanticAssessmentContractError("semantic_waiver_assessor_fence_required")
    idempotency_key = normalize_policy_bounded_text(
        idempotency_key,
        max_length=POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
        code="semantic_waiver_event_idempotency_key_required",
    )
    requested_at = _aware_utc(
        requested_at,
        "semantic_waiver_requested_at_invalid",
    )
    expires_at = _aware_utc(
        expires_at,
        "semantic_waiver_expires_at_invalid",
        optional=True,
    )
    if expires_at is not None and expires_at <= requested_at:
        raise SemanticAssessmentContractError("semantic_waiver_expiry_not_future")
    scope_digest = semantic_metric_waiver_scope_digest(anchor)
    evidence = _evidence_refs(
        evidence_refs,
        "semantic_waiver_evidence_required",
    )
    waiver = SemanticMetricWaiver(
        waiver_id=waiver_id,
        anchor=anchor,
        scope_digest=scope_digest,
        justification=justification,
        evidence_refs=evidence,
        requested_by=requested_by,
        requested_at=requested_at,
        original_expires_at=expires_at,
        status=SemanticMetricWaiverStatus.REQUESTED,
        waiver_revision=1,
        expires_at=expires_at,
        last_event_id=event_id,
        last_event_type=SemanticMetricWaiverEventType.REQUEST,
        last_event_at=requested_at,
        last_event_idempotency_key=idempotency_key,
    )
    request_digest = _semantic_metric_waiver_event_request_digest(
        event_type=SemanticMetricWaiverEventType.REQUEST,
        waiver_id=waiver.waiver_id,
        waiver_revision=1,
        scope_digest=scope_digest,
        reason=waiver.justification,
        evidence_refs=evidence,
        actor_id=waiver.requested_by,
        expires_at=expires_at,
        expire_reason=None,
        idempotency_key=idempotency_key,
    )
    event = SemanticMetricWaiverEvent(
        event_id=event_id,
        predecessor_event_id=None,
        waiver_id=waiver.waiver_id,
        waiver_revision=1,
        event_type=SemanticMetricWaiverEventType.REQUEST,
        from_status=None,
        to_status=SemanticMetricWaiverStatus.REQUESTED,
        actor_id=waiver.requested_by,
        occurred_at=requested_at,
        reason=waiver.justification,
        evidence_refs=evidence,
        expires_at=expires_at,
        scope_digest=scope_digest,
        waiver_digest=waiver.head_digest,
        idempotency_key=idempotency_key,
        request_digest=request_digest,
    )
    return SemanticMetricWaiverMutation(waiver=waiver, event=event)


def transition_semantic_metric_waiver(
    current: SemanticMetricWaiver,
    *,
    event_id: str,
    expected_waiver_revision: int,
    event_type: SemanticMetricWaiverEventType,
    actor_id: str,
    occurred_at: datetime,
    reason: str,
    evidence_refs: tuple[EvidenceRef, ...] | list[EvidenceRef],
    idempotency_key: str,
    expires_at: datetime | None = None,
    expire_reason: SemanticMetricWaiverExpireReason | None = None,
    current_finding: SemanticMetricFinding | None = None,
) -> SemanticMetricWaiverMutation:
    if not isinstance(current, SemanticMetricWaiver):
        raise SemanticAssessmentContractError("semantic_waiver_current_invalid")
    if not isinstance(event_type, SemanticMetricWaiverEventType):
        raise SemanticAssessmentContractError("semantic_waiver_event_type_invalid")
    if expire_reason is not None and not isinstance(
        expire_reason,
        SemanticMetricWaiverExpireReason,
    ):
        raise SemanticAssessmentContractError("semantic_waiver_expire_reason_invalid")
    if current_finding is not None:
        raise SemanticAssessmentContractError(
            "semantic_waiver_current_finding_not_allowed"
        )
    if expected_waiver_revision != current.waiver_revision:
        raise SemanticAssessmentContractError("semantic_waiver_revision_conflict")
    if event_type is SemanticMetricWaiverEventType.REQUEST:
        raise SemanticAssessmentContractError("semantic_waiver_transition_invalid")
    if event_type is SemanticMetricWaiverEventType.REVALIDATE:
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_contract_required"
        )
    occurred_at = _aware_utc(
        occurred_at,
        "semantic_waiver_event_occurred_at_invalid",
    )
    if occurred_at < current.last_event_at:
        raise SemanticAssessmentContractError("semantic_waiver_event_time_regressed")
    actor_id = normalize_policy_bounded_text(
        actor_id,
        max_length=POLICY_ACTOR_ID_MAX_LENGTH,
        code="semantic_waiver_event_actor_required",
    )
    reason = _required_text(reason, "semantic_waiver_event_reason_required")
    idempotency_key = normalize_policy_bounded_text(
        idempotency_key,
        max_length=POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
        code="semantic_waiver_event_idempotency_key_required",
    )
    evidence = _evidence_refs(
        evidence_refs,
        "semantic_waiver_event_evidence_required",
    )
    requested_expires_at = _aware_utc(
        expires_at,
        "semantic_waiver_event_expires_at_invalid",
        optional=True,
    )
    if (
        event_type
        not in {
            SemanticMetricWaiverEventType.APPROVE,
        }
        and requested_expires_at is not None
    ):
        raise SemanticAssessmentContractError(
            "semantic_waiver_event_expiry_not_allowed"
        )
    transitions = {
        SemanticMetricWaiverEventType.APPROVE: (
            {SemanticMetricWaiverStatus.REQUESTED},
            SemanticMetricWaiverStatus.APPROVED,
        ),
        SemanticMetricWaiverEventType.REJECT: (
            {SemanticMetricWaiverStatus.REQUESTED},
            SemanticMetricWaiverStatus.REJECTED,
        ),
        SemanticMetricWaiverEventType.REVOKE: (
            {SemanticMetricWaiverStatus.APPROVED},
            SemanticMetricWaiverStatus.REVOKED,
        ),
        SemanticMetricWaiverEventType.EXPIRE: (
            {SemanticMetricWaiverStatus.APPROVED},
            SemanticMetricWaiverStatus.EXPIRED,
        ),
    }
    allowed_from, target = transitions[event_type]
    if current.status not in allowed_from:
        raise SemanticAssessmentContractError("semantic_waiver_transition_invalid")
    if event_type in {
        SemanticMetricWaiverEventType.APPROVE,
        SemanticMetricWaiverEventType.REJECT,
    } and actor_id in {
        current.requested_by,
        current.anchor.assessment_assessor_id,
    }:
        raise SemanticAssessmentContractError(
            "semantic_waiver_independent_review_required"
        )
    if event_type is SemanticMetricWaiverEventType.EXPIRE:
        if expire_reason is None:
            raise SemanticAssessmentContractError(
                "semantic_waiver_expire_reason_required"
            )
    elif expire_reason is not None:
        raise SemanticAssessmentContractError("semantic_waiver_expire_reason_invalid")
    next_expires_at = current.expires_at
    if (
        event_type
        in {
            SemanticMetricWaiverEventType.APPROVE,
        }
        and requested_expires_at is not None
    ):
        next_expires_at = requested_expires_at
    if (
        target is SemanticMetricWaiverStatus.APPROVED
        and next_expires_at is not None
        and next_expires_at <= occurred_at
    ):
        raise SemanticAssessmentContractError("semantic_waiver_expiry_not_future")
    if (
        event_type is SemanticMetricWaiverEventType.EXPIRE
        and expire_reason is SemanticMetricWaiverExpireReason.SCHEDULED_EXPIRY
        and (current.expires_at is None or occurred_at < current.expires_at)
    ):
        raise SemanticAssessmentContractError(
            "semantic_waiver_scheduled_expiry_not_reached"
        )
    reviewed_by = current.reviewed_by
    reviewed_at = current.reviewed_at
    review_reason = current.review_reason
    if event_type in {
        SemanticMetricWaiverEventType.APPROVE,
        SemanticMetricWaiverEventType.REJECT,
    }:
        reviewed_by = actor_id
        reviewed_at = occurred_at
        review_reason = reason
    revoked_by = (
        actor_id if event_type is SemanticMetricWaiverEventType.REVOKE else None
    )
    revoked_at = (
        occurred_at if event_type is SemanticMetricWaiverEventType.REVOKE else None
    )
    next_revision = current.waiver_revision + 1
    waiver = SemanticMetricWaiver(
        waiver_id=current.waiver_id,
        anchor=current.anchor,
        scope_digest=current.scope_digest,
        justification=current.justification,
        evidence_refs=current.evidence_refs,
        requested_by=current.requested_by,
        requested_at=current.requested_at,
        original_expires_at=current.original_expires_at,
        status=target,
        waiver_revision=next_revision,
        expires_at=next_expires_at,
        last_event_id=event_id,
        last_event_type=event_type,
        last_event_at=occurred_at,
        last_event_idempotency_key=idempotency_key,
        reviewed_by=reviewed_by,
        reviewed_at=reviewed_at,
        review_reason=review_reason,
        revoked_by=revoked_by,
        revoked_at=revoked_at,
        expire_reason=expire_reason,
        last_revalidation_status=current.last_revalidation_status,
        last_revalidation_current=current.last_revalidation_current,
        last_revalidation_reason_code=(current.last_revalidation_reason_code),
        last_revalidation_evaluated_at=(current.last_revalidation_evaluated_at),
        last_revalidation_currentness_reasons=(
            current.last_revalidation_currentness_reasons
        ),
        last_revalidation_scheduled_expiry_observed=(
            current.last_revalidation_scheduled_expiry_observed
        ),
    )
    request_digest = _semantic_metric_waiver_event_request_digest(
        event_type=event_type,
        waiver_id=current.waiver_id,
        waiver_revision=next_revision,
        scope_digest=current.scope_digest,
        reason=reason,
        evidence_refs=evidence,
        actor_id=actor_id,
        expires_at=next_expires_at,
        expire_reason=expire_reason,
        idempotency_key=idempotency_key,
    )
    event = SemanticMetricWaiverEvent(
        event_id=event_id,
        predecessor_event_id=current.last_event_id,
        waiver_id=current.waiver_id,
        waiver_revision=next_revision,
        event_type=event_type,
        from_status=current.status,
        to_status=target,
        actor_id=actor_id,
        occurred_at=occurred_at,
        reason=reason,
        evidence_refs=evidence,
        expires_at=next_expires_at,
        scope_digest=current.scope_digest,
        waiver_digest=waiver.head_digest,
        idempotency_key=idempotency_key,
        request_digest=request_digest,
        expire_reason=expire_reason,
    )
    return SemanticMetricWaiverMutation(waiver=waiver, event=event)


def revalidate_semantic_metric_waiver(
    current: SemanticMetricWaiver,
    *,
    event_id: str,
    expected_waiver_revision: int,
    actor_id: str,
    occurred_at: datetime,
    evaluated_at: datetime,
    status: SemanticMetricWaiverRevalidationStatus,
    reason_code: SemanticMetricWaiverRevalidationReason,
    currentness_reasons: tuple[SemanticAssessmentCurrentnessReason, ...],
    scheduled_expiry_observed: bool,
    evidence_refs: tuple[EvidenceRef, ...] | list[EvidenceRef],
    idempotency_key: str,
) -> SemanticMetricWaiverMutation:
    """Append one exact currentness decision without rebinding its anchor."""

    if not isinstance(current, SemanticMetricWaiver):
        raise SemanticAssessmentContractError("semantic_waiver_current_invalid")
    if expected_waiver_revision != current.waiver_revision:
        raise SemanticAssessmentContractError("semantic_waiver_revision_conflict")
    if current.status not in {
        SemanticMetricWaiverStatus.APPROVED,
        SemanticMetricWaiverStatus.EXPIRED,
        SemanticMetricWaiverStatus.REVOKED,
    }:
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_state_invalid"
        )
    occurred_at = _aware_utc(
        occurred_at,
        "semantic_waiver_event_occurred_at_invalid",
    )
    evaluated_at = _aware_utc(
        evaluated_at,
        "semantic_waiver_revalidation_evaluated_at_invalid",
    )
    if occurred_at < current.last_event_at:
        raise SemanticAssessmentContractError("semantic_waiver_event_time_regressed")
    if evaluated_at < current.last_event_at:
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_time_regressed"
        )
    actor_id = normalize_policy_bounded_text(
        actor_id,
        max_length=POLICY_ACTOR_ID_MAX_LENGTH,
        code="semantic_waiver_event_actor_required",
    )
    if actor_id == current.requested_by:
        raise SemanticAssessmentContractError(
            "semantic_waiver_independent_review_required"
        )
    if actor_id == current.anchor.assessment_assessor_id:
        raise SemanticAssessmentContractError(
            "semantic_waiver_independent_review_required"
        )
    idempotency_key = normalize_policy_bounded_text(
        idempotency_key,
        max_length=POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
        code="semantic_waiver_event_idempotency_key_required",
    )
    evidence = _evidence_refs(
        evidence_refs,
        "semantic_waiver_event_evidence_required",
    )
    if not isinstance(status, SemanticMetricWaiverRevalidationStatus):
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_status_invalid"
        )
    if not isinstance(reason_code, SemanticMetricWaiverRevalidationReason):
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_reason_invalid"
        )
    if not isinstance(currentness_reasons, tuple):
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_currentness_reasons_invalid"
        )
    if not isinstance(scheduled_expiry_observed, bool):
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_expiry_observed_invalid"
        )
    expiry_observed = bool(
        current.expires_at is not None and current.expires_at <= evaluated_at
    )
    if scheduled_expiry_observed != expiry_observed:
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_expiry_observed_mismatch"
        )
    revalidation_current = status is SemanticMetricWaiverRevalidationStatus.APPROVED
    _validate_semantic_waiver_revalidation_decision(
        status=status,
        current=revalidation_current,
        reason=reason_code,
        currentness_reasons=currentness_reasons,
        scheduled_expiry_observed=scheduled_expiry_observed,
    )
    if current.status is SemanticMetricWaiverStatus.REVOKED:
        if status is not SemanticMetricWaiverRevalidationStatus.REVOKED:
            raise SemanticAssessmentContractError(
                "semantic_waiver_revalidation_precedence_invalid"
            )
    elif status is SemanticMetricWaiverRevalidationStatus.REVOKED:
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_precedence_invalid"
        )
    elif current.status is SemanticMetricWaiverStatus.EXPIRED and status not in {
        SemanticMetricWaiverRevalidationStatus.EXPIRED,
        SemanticMetricWaiverRevalidationStatus.ANCHOR_STALE,
    }:
        raise SemanticAssessmentContractError("semantic_waiver_reactivation_forbidden")
    elif current.status is SemanticMetricWaiverStatus.APPROVED and status not in {
        SemanticMetricWaiverRevalidationStatus.APPROVED,
        SemanticMetricWaiverRevalidationStatus.EXPIRED,
        SemanticMetricWaiverRevalidationStatus.ANCHOR_STALE,
    }:
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_precedence_invalid"
        )
    if (
        status is SemanticMetricWaiverRevalidationStatus.APPROVED and expiry_observed
    ) or (
        status is SemanticMetricWaiverRevalidationStatus.EXPIRED
        and not expiry_observed
        and current.status is not SemanticMetricWaiverStatus.EXPIRED
    ):
        raise SemanticAssessmentContractError(
            "semantic_waiver_revalidation_expiry_state_invalid"
        )

    target = current.status
    next_expire_reason = current.expire_reason
    if (
        current.status is SemanticMetricWaiverStatus.APPROVED
        and status is SemanticMetricWaiverRevalidationStatus.EXPIRED
    ):
        target = SemanticMetricWaiverStatus.EXPIRED
        next_expire_reason = SemanticMetricWaiverExpireReason.SCHEDULED_EXPIRY

    next_revision = current.waiver_revision + 1
    waiver = SemanticMetricWaiver(
        waiver_id=current.waiver_id,
        anchor=current.anchor,
        scope_digest=current.scope_digest,
        justification=current.justification,
        evidence_refs=current.evidence_refs,
        requested_by=current.requested_by,
        requested_at=current.requested_at,
        original_expires_at=current.original_expires_at,
        status=target,
        waiver_revision=next_revision,
        expires_at=current.expires_at,
        last_event_id=event_id,
        last_event_type=SemanticMetricWaiverEventType.REVALIDATE,
        last_event_at=occurred_at,
        last_event_idempotency_key=idempotency_key,
        reviewed_by=current.reviewed_by,
        reviewed_at=current.reviewed_at,
        review_reason=current.review_reason,
        revoked_by=current.revoked_by,
        revoked_at=current.revoked_at,
        expire_reason=next_expire_reason,
        last_revalidation_status=status,
        last_revalidation_current=revalidation_current,
        last_revalidation_reason_code=reason_code,
        last_revalidation_evaluated_at=evaluated_at,
        last_revalidation_currentness_reasons=currentness_reasons,
        last_revalidation_scheduled_expiry_observed=(scheduled_expiry_observed),
    )
    request_digest = _semantic_metric_waiver_event_request_digest(
        event_type=SemanticMetricWaiverEventType.REVALIDATE,
        waiver_id=current.waiver_id,
        waiver_revision=next_revision,
        scope_digest=current.scope_digest,
        reason=reason_code.value,
        evidence_refs=evidence,
        actor_id=actor_id,
        expires_at=current.expires_at,
        expire_reason=next_expire_reason,
        idempotency_key=idempotency_key,
        evaluated_at=evaluated_at,
        revalidation_status=status,
        revalidation_current=revalidation_current,
        revalidation_reason_code=reason_code,
        currentness_reasons=currentness_reasons,
        scheduled_expiry_observed=scheduled_expiry_observed,
    )
    event = SemanticMetricWaiverEvent(
        event_id=event_id,
        predecessor_event_id=current.last_event_id,
        waiver_id=current.waiver_id,
        waiver_revision=next_revision,
        event_type=SemanticMetricWaiverEventType.REVALIDATE,
        from_status=current.status,
        to_status=target,
        actor_id=actor_id,
        occurred_at=occurred_at,
        reason=reason_code.value,
        evidence_refs=evidence,
        expires_at=current.expires_at,
        scope_digest=current.scope_digest,
        waiver_digest=waiver.head_digest,
        idempotency_key=idempotency_key,
        request_digest=request_digest,
        expire_reason=next_expire_reason,
        evaluated_at=evaluated_at,
        revalidation_status=status,
        revalidation_current=revalidation_current,
        revalidation_reason_code=reason_code,
        currentness_reasons=currentness_reasons,
        scheduled_expiry_observed=scheduled_expiry_observed,
    )
    return SemanticMetricWaiverMutation(waiver=waiver, event=event)


@dataclass(frozen=True, slots=True)
class SemanticPolicySkipScope:
    subject: PolicySubjectRef
    subject_content_digest: str
    guideline_id: str
    guideline_revision_id: str
    guideline_revision_digest: str
    binding_id: str
    binding_revision: int
    binding_configuration_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError("semantic_skip_scope_subject_invalid")
        for field_name, max_length in (
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("guideline_revision_id", GUIDELINE_REVISION_ID_MAX_LENGTH),
            ("binding_id", GUIDELINE_BINDING_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=max_length,
                    code=f"semantic_skip_scope_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "binding_revision",
            _positive_int(
                self.binding_revision,
                "semantic_skip_scope_binding_revision_invalid",
            ),
        )
        for field_name in (
            "subject_content_digest",
            "guideline_revision_digest",
            "binding_configuration_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"semantic_skip_scope_{field_name}_invalid",
                ),
            )

    @classmethod
    def from_authority(
        cls,
        *,
        subject_snapshot: PolicySubjectSnapshot,
        binding: BoardGuidelineBinding,
        revision: GuidelineRevision,
    ) -> SemanticPolicySkipScope:
        if not isinstance(subject_snapshot, PolicySubjectSnapshot):
            raise SemanticAssessmentContractError(
                "semantic_skip_subject_snapshot_invalid"
            )
        if not isinstance(binding, BoardGuidelineBinding):
            raise SemanticAssessmentContractError("semantic_skip_binding_invalid")
        if not isinstance(revision, GuidelineRevision):
            raise SemanticAssessmentContractError("semantic_skip_revision_invalid")
        if (
            subject_snapshot.subject.board_id != binding.board_id
            or binding.guideline_id != revision.guideline_id
            or binding.revision_id != revision.revision_id
            or binding.revision_digest != revision.revision_digest
        ):
            raise SemanticAssessmentContractError(
                "semantic_skip_authority_scope_mismatch"
            )
        return cls(
            subject=subject_snapshot.subject,
            subject_content_digest=subject_snapshot.content_digest,
            guideline_id=revision.guideline_id,
            guideline_revision_id=revision.revision_id,
            guideline_revision_digest=revision.revision_digest,
            binding_id=binding.binding_id,
            binding_revision=binding.binding_revision,
            binding_configuration_digest=binding.configuration_digest,
        )

    def is_current_for(
        self,
        *,
        subject_snapshot: PolicySubjectSnapshot,
        binding: BoardGuidelineBinding,
        revision: GuidelineRevision,
    ) -> bool:
        try:
            current = SemanticPolicySkipScope.from_authority(
                subject_snapshot=subject_snapshot,
                binding=binding,
                revision=revision,
            )
        except SemanticAssessmentContractError:
            return False
        return self == current


def semantic_policy_skip_scope_digest(
    scope: SemanticPolicySkipScope,
) -> str:
    if not isinstance(scope, SemanticPolicySkipScope):
        raise SemanticAssessmentContractError("semantic_skip_scope_invalid")
    return canonical_sha256(
        {
            "contract": SEMANTIC_POLICY_SKIP_CONTRACT_VERSION,
            "subject": {
                "board_id": scope.subject.board_id,
                "subject_type": scope.subject.entity_type.value,
                "subject_id": scope.subject.subject_id,
                "subject_version": scope.subject.subject_version,
                **(
                    {"subject_edition": scope.subject.subject_edition}
                    if scope.subject.subject_edition is not None
                    else {}
                ),
                "content_digest": scope.subject_content_digest,
            },
            "guideline": {
                "guideline_id": scope.guideline_id,
                "revision_id": scope.guideline_revision_id,
                "revision_digest": scope.guideline_revision_digest,
            },
            "binding": {
                "binding_id": scope.binding_id,
                "binding_revision": scope.binding_revision,
                "configuration_digest": (scope.binding_configuration_digest),
            },
        }
    )


@dataclass(frozen=True, slots=True)
class SemanticPolicySkip:
    skip_id: str
    skip_revision: int
    scope: SemanticPolicySkipScope
    scope_digest: str
    status: SemanticPolicySkipStatus
    reason: str
    created_by: str
    created_at: datetime
    last_event_id: str
    last_event_type: SemanticPolicySkipEventType
    last_event_at: datetime
    idempotency_key: str
    request_digest: str
    revoked_by: str | None = None
    revoked_at: datetime | None = None
    revocation_reason: str | None = None
    skip_digest: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "skip_id",
            normalize_policy_bounded_text(
                self.skip_id,
                max_length=POLICY_RECEIPT_ID_MAX_LENGTH,
                code="semantic_skip_id_required",
            ),
        )
        object.__setattr__(
            self,
            "skip_revision",
            _positive_int(
                self.skip_revision,
                "semantic_skip_revision_invalid",
            ),
        )
        if not isinstance(self.scope, SemanticPolicySkipScope):
            raise SemanticAssessmentContractError("semantic_skip_scope_invalid")
        supplied_scope = _sha256(
            self.scope_digest,
            "semantic_skip_scope_digest_invalid",
        )
        if supplied_scope != semantic_policy_skip_scope_digest(self.scope):
            raise SemanticAssessmentContractError("semantic_skip_scope_digest_mismatch")
        object.__setattr__(self, "scope_digest", supplied_scope)
        if not isinstance(self.status, SemanticPolicySkipStatus):
            raise SemanticAssessmentContractError("semantic_skip_status_invalid")
        object.__setattr__(
            self,
            "reason",
            _required_text(self.reason, "semantic_skip_reason_required"),
        )
        object.__setattr__(
            self,
            "created_by",
            normalize_policy_bounded_text(
                self.created_by,
                max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                code="semantic_skip_created_by_required",
            ),
        )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "semantic_skip_created_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "last_event_id",
            normalize_policy_bounded_text(
                self.last_event_id,
                max_length=POLICY_RECEIPT_ID_MAX_LENGTH,
                code="semantic_skip_last_event_id_required",
            ),
        )
        if not isinstance(self.last_event_type, SemanticPolicySkipEventType):
            raise SemanticAssessmentContractError(
                "semantic_skip_last_event_type_invalid"
            )
        object.__setattr__(
            self,
            "last_event_at",
            _aware_utc(
                self.last_event_at,
                "semantic_skip_last_event_at_invalid",
            ),
        )
        if self.last_event_at < self.created_at:
            raise SemanticAssessmentContractError(
                "semantic_skip_last_event_time_regressed"
            )
        object.__setattr__(
            self,
            "idempotency_key",
            normalize_policy_bounded_text(
                self.idempotency_key,
                max_length=POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                code="semantic_skip_idempotency_key_required",
            ),
        )
        object.__setattr__(
            self,
            "request_digest",
            _sha256(
                self.request_digest,
                "semantic_skip_request_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "revoked_by",
            (
                normalize_policy_bounded_text(
                    self.revoked_by,
                    max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                    code="semantic_skip_revoked_by_invalid",
                )
                if self.revoked_by is not None
                else None
            ),
        )
        object.__setattr__(
            self,
            "revoked_at",
            _aware_utc(
                self.revoked_at,
                "semantic_skip_revoked_at_invalid",
                optional=True,
            ),
        )
        object.__setattr__(
            self,
            "revocation_reason",
            _optional_text(
                self.revocation_reason,
                "semantic_skip_revocation_reason_invalid",
            ),
        )
        if self.status is SemanticPolicySkipStatus.ACTIVE:
            if (
                self.last_event_type is not SemanticPolicySkipEventType.CREATE
                or self.skip_revision != 1
                or self.last_event_at != self.created_at
                or any(
                    value is not None
                    for value in (
                        self.revoked_by,
                        self.revoked_at,
                        self.revocation_reason,
                    )
                )
            ):
                raise SemanticAssessmentContractError(
                    "semantic_skip_active_state_invalid"
                )
        elif (
            self.last_event_type is not SemanticPolicySkipEventType.REVOKE
            or self.skip_revision != 2
            or self.revoked_by is None
            or self.revoked_at is None
            or self.revocation_reason is None
            or self.revoked_at != self.last_event_at
        ):
            raise SemanticAssessmentContractError("semantic_skip_revoked_state_invalid")
        request_actor_id = (
            self.created_by
            if self.status is SemanticPolicySkipStatus.ACTIVE
            else self.revoked_by
        )
        request_reason = (
            self.reason
            if self.status is SemanticPolicySkipStatus.ACTIVE
            else self.revocation_reason
        )
        assert request_actor_id is not None
        assert request_reason is not None
        expected_request_digest = _semantic_policy_skip_event_request_digest(
            event_type=self.last_event_type,
            skip_id=self.skip_id,
            skip_revision=self.skip_revision,
            scope_digest=self.scope_digest,
            reason=request_reason,
            actor_id=request_actor_id,
            actor_kind=SemanticExceptionActorKind.HUMAN,
            idempotency_key=self.idempotency_key,
        )
        if self.request_digest != expected_request_digest:
            raise SemanticAssessmentContractError(
                "semantic_skip_request_digest_mismatch"
            )
        expected_digest = semantic_policy_skip_digest(self)
        if self.skip_digest is not None:
            supplied_digest = _sha256(
                self.skip_digest,
                "semantic_skip_digest_invalid",
            )
            if supplied_digest != expected_digest:
                raise SemanticAssessmentContractError("semantic_skip_digest_mismatch")
        object.__setattr__(self, "skip_digest", expected_digest)

    def is_active_and_current_for(
        self,
        *,
        subject_snapshot: PolicySubjectSnapshot,
        binding: BoardGuidelineBinding,
        revision: GuidelineRevision,
    ) -> bool:
        return (
            self.status is SemanticPolicySkipStatus.ACTIVE
            and self.scope.is_current_for(
                subject_snapshot=subject_snapshot,
                binding=binding,
                revision=revision,
            )
        )


def semantic_policy_skip_digest(skip: SemanticPolicySkip) -> str:
    return canonical_sha256(
        {
            "contract": SEMANTIC_POLICY_SKIP_CONTRACT_VERSION,
            "skip_id": skip.skip_id,
            "skip_revision": skip.skip_revision,
            "scope_digest": skip.scope_digest,
            "status": skip.status.value,
            "reason": skip.reason,
            "created_by": skip.created_by,
            "created_at": skip.created_at.isoformat(),
            "last_event_id": skip.last_event_id,
            "last_event_type": skip.last_event_type.value,
            "last_event_at": skip.last_event_at.isoformat(),
            "idempotency_key": skip.idempotency_key,
            "request_digest": skip.request_digest,
            "revoked_by": skip.revoked_by,
            "revoked_at": (
                skip.revoked_at.isoformat() if skip.revoked_at is not None else None
            ),
            "revocation_reason": skip.revocation_reason,
        }
    )


@dataclass(frozen=True, slots=True)
class SemanticPolicySkipEvent:
    event_id: str
    predecessor_event_id: str | None
    skip_id: str
    skip_revision: int
    event_type: SemanticPolicySkipEventType
    from_status: SemanticPolicySkipStatus | None
    to_status: SemanticPolicySkipStatus
    actor_id: str
    actor_kind: SemanticExceptionActorKind
    occurred_at: datetime
    reason: str
    scope_digest: str
    skip_digest: str
    idempotency_key: str
    request_digest: str

    def __post_init__(self) -> None:
        if self.actor_kind is not SemanticExceptionActorKind.HUMAN:
            raise SemanticAssessmentContractError("semantic_skip_human_actor_required")
        for field_name in ("event_id", "skip_id"):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=POLICY_RECEIPT_ID_MAX_LENGTH,
                    code=f"semantic_skip_event_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "predecessor_event_id",
            (
                normalize_policy_bounded_text(
                    self.predecessor_event_id,
                    max_length=POLICY_RECEIPT_ID_MAX_LENGTH,
                    code="semantic_skip_event_predecessor_invalid",
                )
                if self.predecessor_event_id is not None
                else None
            ),
        )
        object.__setattr__(
            self,
            "skip_revision",
            _positive_int(
                self.skip_revision,
                "semantic_skip_event_revision_invalid",
            ),
        )
        if not isinstance(self.event_type, SemanticPolicySkipEventType):
            raise SemanticAssessmentContractError("semantic_skip_event_type_invalid")
        if self.from_status is not None and not isinstance(
            self.from_status,
            SemanticPolicySkipStatus,
        ):
            raise SemanticAssessmentContractError(
                "semantic_skip_event_from_status_invalid"
            )
        if not isinstance(self.to_status, SemanticPolicySkipStatus):
            raise SemanticAssessmentContractError(
                "semantic_skip_event_to_status_invalid"
            )
        object.__setattr__(
            self,
            "actor_id",
            normalize_policy_bounded_text(
                self.actor_id,
                max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                code="semantic_skip_event_actor_required",
            ),
        )
        object.__setattr__(
            self,
            "occurred_at",
            _aware_utc(
                self.occurred_at,
                "semantic_skip_event_occurred_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "reason",
            _required_text(
                self.reason,
                "semantic_skip_event_reason_required",
            ),
        )
        for field_name in ("scope_digest", "skip_digest", "request_digest"):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"semantic_skip_event_{field_name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "idempotency_key",
            normalize_policy_bounded_text(
                self.idempotency_key,
                max_length=POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                code="semantic_skip_event_idempotency_key_required",
            ),
        )
        expected = (
            (
                None,
                SemanticPolicySkipStatus.ACTIVE,
                None,
                1,
            )
            if self.event_type is SemanticPolicySkipEventType.CREATE
            else (
                SemanticPolicySkipStatus.ACTIVE,
                SemanticPolicySkipStatus.REVOKED,
                self.predecessor_event_id,
                self.skip_revision,
            )
        )
        actual = (
            self.from_status,
            self.to_status,
            self.predecessor_event_id,
            self.skip_revision,
        )
        if self.event_type is SemanticPolicySkipEventType.CREATE:
            if actual != expected:
                raise SemanticAssessmentContractError(
                    "semantic_skip_event_transition_invalid"
                )
        elif (
            self.from_status is not SemanticPolicySkipStatus.ACTIVE
            or self.to_status is not SemanticPolicySkipStatus.REVOKED
            or self.predecessor_event_id is None
            or self.skip_revision != 2
        ):
            raise SemanticAssessmentContractError(
                "semantic_skip_event_transition_invalid"
            )
        expected_request_digest = _semantic_policy_skip_event_request_digest(
            event_type=self.event_type,
            skip_id=self.skip_id,
            skip_revision=self.skip_revision,
            scope_digest=self.scope_digest,
            reason=self.reason,
            actor_id=self.actor_id,
            actor_kind=self.actor_kind,
            idempotency_key=self.idempotency_key,
        )
        if self.request_digest != expected_request_digest:
            raise SemanticAssessmentContractError(
                "semantic_skip_event_request_digest_mismatch"
            )


@dataclass(frozen=True, slots=True)
class SemanticPolicySkipMutation:
    skip: SemanticPolicySkip
    event: SemanticPolicySkipEvent

    def __post_init__(self) -> None:
        if not isinstance(self.skip, SemanticPolicySkip) or not isinstance(
            self.event,
            SemanticPolicySkipEvent,
        ):
            raise SemanticAssessmentContractError("semantic_skip_mutation_invalid")
        if (
            self.skip.skip_id != self.event.skip_id
            or self.skip.skip_revision != self.event.skip_revision
            or self.skip.last_event_id != self.event.event_id
            or self.skip.status is not self.event.to_status
            or self.skip.scope_digest != self.event.scope_digest
            or self.skip.skip_digest != self.event.skip_digest
            or self.skip.request_digest != self.event.request_digest
        ):
            raise SemanticAssessmentContractError("semantic_skip_mutation_inconsistent")


def create_semantic_policy_skip(
    *,
    skip_id: str,
    event_id: str,
    scope: SemanticPolicySkipScope,
    reason: str,
    actor_id: str,
    actor_kind: SemanticExceptionActorKind,
    occurred_at: datetime,
    idempotency_key: str,
) -> SemanticPolicySkipMutation:
    if actor_kind is not SemanticExceptionActorKind.HUMAN:
        raise SemanticAssessmentContractError("semantic_skip_human_actor_required")
    occurred_at = _aware_utc(
        occurred_at,
        "semantic_skip_event_occurred_at_invalid",
    )
    skip_id = normalize_policy_bounded_text(
        skip_id,
        max_length=POLICY_RECEIPT_ID_MAX_LENGTH,
        code="semantic_skip_id_required",
    )
    actor_id = normalize_policy_bounded_text(
        actor_id,
        max_length=POLICY_ACTOR_ID_MAX_LENGTH,
        code="semantic_skip_event_actor_required",
    )
    reason = _required_text(reason, "semantic_skip_reason_required")
    idempotency_key = normalize_policy_bounded_text(
        idempotency_key,
        max_length=POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
        code="semantic_skip_event_idempotency_key_required",
    )
    scope_digest = semantic_policy_skip_scope_digest(scope)
    request_digest = _semantic_policy_skip_event_request_digest(
        event_type=SemanticPolicySkipEventType.CREATE,
        skip_id=skip_id,
        skip_revision=1,
        scope_digest=scope_digest,
        reason=reason,
        actor_id=actor_id,
        actor_kind=actor_kind,
        idempotency_key=idempotency_key,
    )
    skip = SemanticPolicySkip(
        skip_id=skip_id,
        skip_revision=1,
        scope=scope,
        scope_digest=scope_digest,
        status=SemanticPolicySkipStatus.ACTIVE,
        reason=reason,
        created_by=actor_id,
        created_at=occurred_at,
        last_event_id=event_id,
        last_event_type=SemanticPolicySkipEventType.CREATE,
        last_event_at=occurred_at,
        idempotency_key=idempotency_key,
        request_digest=request_digest,
    )
    event = SemanticPolicySkipEvent(
        event_id=event_id,
        predecessor_event_id=None,
        skip_id=skip.skip_id,
        skip_revision=1,
        event_type=SemanticPolicySkipEventType.CREATE,
        from_status=None,
        to_status=SemanticPolicySkipStatus.ACTIVE,
        actor_id=actor_id,
        actor_kind=actor_kind,
        occurred_at=occurred_at,
        reason=skip.reason,
        scope_digest=scope_digest,
        skip_digest=skip.skip_digest,
        idempotency_key=skip.idempotency_key,
        request_digest=request_digest,
    )
    return SemanticPolicySkipMutation(skip=skip, event=event)


def revoke_semantic_policy_skip(
    current: SemanticPolicySkip,
    *,
    event_id: str,
    expected_skip_revision: int,
    actor_id: str,
    actor_kind: SemanticExceptionActorKind,
    occurred_at: datetime,
    reason: str,
    idempotency_key: str,
) -> SemanticPolicySkipMutation:
    if not isinstance(current, SemanticPolicySkip):
        raise SemanticAssessmentContractError("semantic_skip_current_invalid")
    if actor_kind is not SemanticExceptionActorKind.HUMAN:
        raise SemanticAssessmentContractError("semantic_skip_human_actor_required")
    if current.status is not SemanticPolicySkipStatus.ACTIVE:
        raise SemanticAssessmentContractError("semantic_skip_transition_invalid")
    if expected_skip_revision != current.skip_revision:
        raise SemanticAssessmentContractError("semantic_skip_revision_conflict")
    occurred_at = _aware_utc(
        occurred_at,
        "semantic_skip_event_occurred_at_invalid",
    )
    if occurred_at < current.last_event_at:
        raise SemanticAssessmentContractError("semantic_skip_event_time_regressed")
    actor_id = normalize_policy_bounded_text(
        actor_id,
        max_length=POLICY_ACTOR_ID_MAX_LENGTH,
        code="semantic_skip_event_actor_required",
    )
    reason = _required_text(reason, "semantic_skip_event_reason_required")
    idempotency_key = normalize_policy_bounded_text(
        idempotency_key,
        max_length=POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
        code="semantic_skip_event_idempotency_key_required",
    )
    next_revision = current.skip_revision + 1
    request_digest = _semantic_policy_skip_event_request_digest(
        event_type=SemanticPolicySkipEventType.REVOKE,
        skip_id=current.skip_id,
        skip_revision=next_revision,
        scope_digest=current.scope_digest,
        reason=reason,
        actor_id=actor_id,
        actor_kind=actor_kind,
        idempotency_key=idempotency_key,
    )
    skip = SemanticPolicySkip(
        skip_id=current.skip_id,
        skip_revision=next_revision,
        scope=current.scope,
        scope_digest=current.scope_digest,
        status=SemanticPolicySkipStatus.REVOKED,
        reason=current.reason,
        created_by=current.created_by,
        created_at=current.created_at,
        last_event_id=event_id,
        last_event_type=SemanticPolicySkipEventType.REVOKE,
        last_event_at=occurred_at,
        idempotency_key=idempotency_key,
        request_digest=request_digest,
        revoked_by=actor_id,
        revoked_at=occurred_at,
        revocation_reason=reason,
    )
    event = SemanticPolicySkipEvent(
        event_id=event_id,
        predecessor_event_id=current.last_event_id,
        skip_id=current.skip_id,
        skip_revision=next_revision,
        event_type=SemanticPolicySkipEventType.REVOKE,
        from_status=SemanticPolicySkipStatus.ACTIVE,
        to_status=SemanticPolicySkipStatus.REVOKED,
        actor_id=actor_id,
        actor_kind=actor_kind,
        occurred_at=occurred_at,
        reason=reason,
        scope_digest=current.scope_digest,
        skip_digest=skip.skip_digest,
        idempotency_key=idempotency_key,
        request_digest=request_digest,
    )
    return SemanticPolicySkipMutation(skip=skip, event=event)


__all__ = [
    "SEMANTIC_METRIC_WAIVER_CONTRACT_VERSION",
    "SEMANTIC_METRIC_WAIVER_EVENT_CONTRACT_VERSION",
    "SEMANTIC_POLICY_SKIP_CONTRACT_VERSION",
    "SEMANTIC_POLICY_SKIP_EVENT_CONTRACT_VERSION",
    "SemanticExceptionActorKind",
    "SemanticMetricWaiver",
    "SemanticMetricWaiverAnchor",
    "SemanticMetricWaiverEvent",
    "SemanticMetricWaiverEventType",
    "SemanticMetricWaiverExpireReason",
    "SemanticMetricWaiverMutation",
    "SemanticMetricWaiverRevalidationReason",
    "SemanticMetricWaiverRevalidationStatus",
    "SemanticMetricWaiverStatus",
    "SemanticPolicySkip",
    "SemanticPolicySkipEvent",
    "SemanticPolicySkipEventType",
    "SemanticPolicySkipMutation",
    "SemanticPolicySkipScope",
    "SemanticPolicySkipStatus",
    "create_semantic_policy_skip",
    "request_semantic_metric_waiver",
    "revalidate_semantic_metric_waiver",
    "revoke_semantic_policy_skip",
    "semantic_metric_waiver_head_digest",
    "semantic_metric_waiver_revalidation_conflict",
    "semantic_metric_waiver_scope_digest",
    "semantic_policy_skip_digest",
    "semantic_policy_skip_scope_digest",
    "transition_semantic_metric_waiver",
]
