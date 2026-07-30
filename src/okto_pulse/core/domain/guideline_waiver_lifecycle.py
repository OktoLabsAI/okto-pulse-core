"""Pure governed lifecycle for append-only policy waivers.

The caller supplies identities and time.  No operation reads a clock, creates
an identifier, mutates an existing event, or persists anything.  Persistence
adapters can therefore append the returned event and derived head atomically
under a compare-and-swap fence.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import datetime, timezone

from okto_pulse.core.domain.guideline_policy import (
    NON_WAIVABLE_POLICY_CLASSES,
    GuidelineRevision,
    GuidelinePolicyContractError,
    GuidelineRule,
    PolicyComplianceFinding,
    PolicyCurrentness,
    PolicyEvaluationOutcome,
    PolicyWaiver,
    PolicyWaiverEvent,
    PolicyWaiverEventType,
    PolicyWaiverExpireReasonCode,
    PolicyWaiverStatus,
)
from okto_pulse.core.domain.guideline_compliance import (
    PolicyCurrentnessAssessment,
    PolicyCurrentnessReason,
)
from okto_pulse.core.domain.guideline_lifecycle import (
    guideline_revision_content_digest_v1,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256


POLICY_WAIVER_SCOPE_CONTRACT_VERSION = "waiver-scope/v1"
POLICY_WAIVER_EVENT_CONTRACT_VERSION = "waiver-event/v1"
POLICY_WAIVER_EVENT_ORDERING: tuple[str, str] = (
    "waiver_revision ASC",
    "event_id ASC",
)


@dataclass(frozen=True, slots=True)
class PolicyWaiverSource:
    """Canonical historical source plus an explicit live-fence assessment."""

    finding: PolicyComplianceFinding
    revision: GuidelineRevision
    currentness: PolicyCurrentnessAssessment

    def __post_init__(self) -> None:
        if not isinstance(self.finding, PolicyComplianceFinding):
            raise GuidelinePolicyContractError("policy_waiver_source_finding_invalid")
        if not isinstance(self.revision, GuidelineRevision):
            raise GuidelinePolicyContractError("policy_waiver_source_revision_invalid")
        if not isinstance(self.currentness, PolicyCurrentnessAssessment):
            raise GuidelinePolicyContractError(
                "policy_waiver_source_currentness_invalid"
            )
        expected_digest = guideline_revision_content_digest_v1(
            title=self.revision.title,
            content=self.revision.content,
            rules=self.revision.rules,
            tags=self.revision.tags,
        )
        if self.revision.content_digest != expected_digest:
            raise GuidelinePolicyContractError(
                "policy_waiver_source_revision_digest_mismatch"
            )
        if (
            self.revision.guideline_id != self.finding.guideline_id
            or self.revision.revision_id != self.finding.revision_id
        ):
            raise GuidelinePolicyContractError("policy_waiver_source_revision_mismatch")
        rule = self.rule
        if rule is None or not rule.applies_to(self.finding.subject.entity_type):
            raise GuidelinePolicyContractError("policy_waiver_rule_scope_mismatch")

    @property
    def rule(self) -> GuidelineRule | None:
        return next(
            (
                candidate
                for candidate in self.revision.rules
                if candidate.rule_id == self.finding.rule_id
            ),
            None,
        )


@dataclass(frozen=True, slots=True)
class PolicyWaiverMutation:
    """Indivisible, self-validating head/event transition.

    Persistence accepts this bundle rather than independently supplied values,
    preventing a fabricated event from authorizing a different derived head.
    """

    waiver: PolicyWaiver
    event: PolicyWaiverEvent
    previous: PolicyWaiver | None = None
    source: PolicyWaiverSource | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.waiver, PolicyWaiver) or not isinstance(
            self.event,
            PolicyWaiverEvent,
        ):
            raise GuidelinePolicyContractError("policy_waiver_mutation_payload_invalid")
        waiver = self.waiver
        event = self.event
        scope_digest = policy_waiver_scope_digest_for_head(waiver)
        if (
            event.waiver_id != waiver.waiver_id
            or event.board_id != waiver.board_id
            or event.waiver_revision != waiver.waiver_revision
            or event.to_status is not waiver.status
            or event.event_id != waiver.last_event_id
            or event.event_type is not waiver.last_event_type
            or event.occurred_at != waiver.last_event_at
            or event.expires_at != waiver.expires_at
            or event.expire_reason_code is not waiver.expire_reason_code
            or event.scope_digest != scope_digest
        ):
            raise GuidelinePolicyContractError(
                "policy_waiver_mutation_event_head_mismatch"
            )

        previous = self.previous
        if event.event_type is PolicyWaiverEventType.REQUEST:
            if (
                previous is not None
                or self.source is None
                or waiver.waiver_revision != 1
                or event.actor_id != waiver.requested_by
                or event.occurred_at != waiver.requested_at
                or event.occurred_at < self.source.finding.created_at
                or event.reason != waiver.justification
                or event.evidence_refs != waiver.evidence_refs
            ):
                raise GuidelinePolicyContractError(
                    "policy_waiver_mutation_request_invalid"
                )
            _validate_source(source=self.source, expected=waiver)
            return
        if not isinstance(previous, PolicyWaiver):
            raise GuidelinePolicyContractError(
                "policy_waiver_mutation_previous_required"
            )
        immutable_fields = (
            "waiver_id",
            "board_id",
            "finding_id",
            "receipt_id",
            "guideline_id",
            "revision_id",
            "rule_id",
            "subject",
            "justification",
            "evidence_refs",
            "requested_by",
            "requested_at",
        )
        if (
            any(
                getattr(previous, field_name) != getattr(waiver, field_name)
                for field_name in immutable_fields
            )
            or event.from_status is not previous.status
            or waiver.waiver_revision != previous.waiver_revision + 1
            or event.event_id == previous.last_event_id
            or event.occurred_at < previous.last_event_at
        ):
            raise GuidelinePolicyContractError(
                "policy_waiver_mutation_predecessor_mismatch"
            )
        if event.event_type in {
            PolicyWaiverEventType.APPROVE,
            PolicyWaiverEventType.REVALIDATE,
        }:
            if self.source is None:
                raise GuidelinePolicyContractError(
                    "policy_waiver_mutation_source_required"
                )
            _validate_source(source=self.source, expected=previous)
        elif (
            event.event_type is PolicyWaiverEventType.EXPIRE
            and event.expire_reason_code
            is not PolicyWaiverExpireReasonCode.SCHEDULED_EXPIRY
        ):
            if self.source is None:
                raise GuidelinePolicyContractError(
                    "policy_waiver_mutation_invalidation_source_required"
                )
            _validate_source(
                source=self.source,
                expected=previous,
                require_current=False,
            )
            if (
                policy_waiver_expire_reason_for(self.source)
                is not event.expire_reason_code
            ):
                raise GuidelinePolicyContractError(
                    "policy_waiver_mutation_invalidation_reason_mismatch"
                )
        elif self.source is not None:
            raise GuidelinePolicyContractError(
                "policy_waiver_mutation_source_forbidden"
            )
        if event.event_type is PolicyWaiverEventType.REVALIDATE:
            if waiver.expires_at <= previous.expires_at:
                raise GuidelinePolicyContractError(
                    "policy_waiver_revalidation_must_extend_expiry"
                )
            if (
                previous.status is PolicyWaiverStatus.APPROVED
                and event.occurred_at >= previous.expires_at
            ):
                raise GuidelinePolicyContractError(
                    "policy_waiver_expiry_event_required"
                )
            if (
                previous.status is PolicyWaiverStatus.EXPIRED
                and previous.expire_reason_code
                is not PolicyWaiverExpireReasonCode.SCHEDULED_EXPIRY
            ):
                raise GuidelinePolicyContractError(
                    "policy_waiver_structural_invalidation_terminal"
                )
        elif waiver.expires_at != previous.expires_at:
            raise GuidelinePolicyContractError("policy_waiver_expiry_change_forbidden")
        if event.event_type in {
            PolicyWaiverEventType.APPROVE,
            PolicyWaiverEventType.REJECT,
            PolicyWaiverEventType.REVALIDATE,
        } and (
            waiver.reviewed_by != event.actor_id
            or waiver.reviewed_at != event.occurred_at
            or waiver.review_reason != event.reason
        ):
            raise GuidelinePolicyContractError("policy_waiver_mutation_review_mismatch")
        if event.event_type not in {
            PolicyWaiverEventType.APPROVE,
            PolicyWaiverEventType.REJECT,
            PolicyWaiverEventType.REVALIDATE,
        } and (
            waiver.reviewed_by != previous.reviewed_by
            or waiver.reviewed_at != previous.reviewed_at
            or waiver.review_reason != previous.review_reason
        ):
            raise GuidelinePolicyContractError("policy_waiver_mutation_review_changed")
        if event.event_type is PolicyWaiverEventType.REVOKE and (
            waiver.revoked_by != event.actor_id
            or waiver.revoked_at != event.occurred_at
        ):
            raise GuidelinePolicyContractError(
                "policy_waiver_mutation_revocation_mismatch"
            )
        if event.event_type is not PolicyWaiverEventType.REVOKE and (
            waiver.revoked_by != previous.revoked_by
            or waiver.revoked_at != previous.revoked_at
        ):
            raise GuidelinePolicyContractError(
                "policy_waiver_mutation_revocation_changed"
            )

    def __iter__(self) -> Iterator[PolicyWaiver | PolicyWaiverEvent]:
        """Preserve ergonomic ``head, event = mutation`` unpacking."""

        yield self.waiver
        yield self.event


def _aware_utc(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise GuidelinePolicyContractError(code)
    return value.astimezone(timezone.utc)


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GuidelinePolicyContractError(code)
    return value.strip()


def _timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _evidence_refs(value: object) -> tuple[str, ...]:
    if not isinstance(value, tuple | list):
        raise GuidelinePolicyContractError("policy_waiver_evidence_refs_invalid")
    normalized = tuple(
        _required_text(item, "policy_waiver_evidence_ref_invalid") for item in value
    )
    if not normalized:
        raise GuidelinePolicyContractError("policy_waiver_evidence_refs_required")
    if len(set(normalized)) != len(normalized):
        raise GuidelinePolicyContractError("policy_waiver_evidence_refs_duplicate")
    return normalized


def policy_waiver_scope_digest(
    *,
    board_id: str,
    finding_id: str,
    receipt_id: str,
    guideline_id: str,
    revision_id: str,
    rule_id: str,
    entity_type: str,
    subject_id: str,
    subject_version: int,
) -> str:
    """Digest every exact scope axis; drift always changes the digest."""

    return canonical_sha256(
        {
            "contract": POLICY_WAIVER_SCOPE_CONTRACT_VERSION,
            "board_id": board_id,
            "finding_id": finding_id,
            "receipt_id": receipt_id,
            "guideline_id": guideline_id,
            "revision_id": revision_id,
            "rule_id": rule_id,
            "entity_type": entity_type,
            "subject_id": subject_id,
            "subject_version": subject_version,
        }
    )


def policy_waiver_scope_digest_for(
    finding: PolicyComplianceFinding,
) -> str:
    if not isinstance(finding, PolicyComplianceFinding):
        raise GuidelinePolicyContractError("policy_waiver_source_finding_invalid")
    return policy_waiver_scope_digest(
        board_id=finding.subject.board_id,
        finding_id=finding.finding_id,
        receipt_id=finding.receipt_id,
        guideline_id=finding.guideline_id,
        revision_id=finding.revision_id,
        rule_id=finding.rule_id,
        entity_type=finding.subject.entity_type.value,
        subject_id=finding.subject.subject_id,
        subject_version=finding.subject.subject_version,
    )


def policy_waiver_scope_digest_for_head(waiver: PolicyWaiver) -> str:
    if not isinstance(waiver, PolicyWaiver):
        raise GuidelinePolicyContractError("policy_waiver_invalid")
    return policy_waiver_scope_digest(
        board_id=waiver.board_id,
        finding_id=waiver.finding_id,
        receipt_id=waiver.receipt_id,
        guideline_id=waiver.guideline_id,
        revision_id=waiver.revision_id,
        rule_id=waiver.rule_id,
        entity_type=waiver.subject.entity_type.value,
        subject_id=waiver.subject.subject_id,
        subject_version=waiver.subject.subject_version,
    )


def policy_waiver_head_digest(waiver: PolicyWaiver) -> str:
    """Canonical digest of a derived head for persistence integrity checks."""

    scope_digest = policy_waiver_scope_digest_for_head(waiver)
    return canonical_sha256(
        {
            "contract": POLICY_WAIVER_EVENT_CONTRACT_VERSION,
            "waiver_id": waiver.waiver_id,
            "waiver_revision": waiver.waiver_revision,
            "scope_digest": scope_digest,
            "status": waiver.status.value,
            "justification": waiver.justification,
            "evidence_refs": list(waiver.evidence_refs),
            "requested_by": waiver.requested_by,
            "requested_at": _timestamp(waiver.requested_at),
            "expires_at": _timestamp(waiver.expires_at),
            "last_event_id": waiver.last_event_id,
            "last_event_type": waiver.last_event_type.value,
            "last_event_at": _timestamp(waiver.last_event_at),
            "reviewed_by": waiver.reviewed_by,
            "reviewed_at": _timestamp(waiver.reviewed_at),
            "review_reason": waiver.review_reason,
            "revoked_by": waiver.revoked_by,
            "revoked_at": _timestamp(waiver.revoked_at),
            "expire_reason_code": (
                waiver.expire_reason_code.value
                if waiver.expire_reason_code is not None
                else None
            ),
        }
    )


def _validate_source(
    *,
    source: PolicyWaiverSource,
    expected: PolicyWaiver | None = None,
    require_current: bool = True,
) -> GuidelineRule:
    if not isinstance(source, PolicyWaiverSource):
        raise GuidelinePolicyContractError("policy_waiver_source_invalid")
    finding = source.finding
    rule = source.rule
    if rule is None:
        raise GuidelinePolicyContractError("policy_waiver_rule_scope_mismatch")
    if (
        require_current
        and source.currentness.currentness is not PolicyCurrentness.CURRENT
    ):
        raise GuidelinePolicyContractError("policy_waiver_source_not_current")
    if (
        finding.outcome is not PolicyEvaluationOutcome.FAIL
        or finding.waiver_id is not None
    ):
        raise GuidelinePolicyContractError("policy_waiver_requires_unwaived_failure")
    if rule.rule_id != finding.rule_id or not rule.applies_to(
        finding.subject.entity_type
    ):
        raise GuidelinePolicyContractError("policy_waiver_rule_scope_mismatch")
    if not rule.waivable or rule.policy_class in NON_WAIVABLE_POLICY_CLASSES:
        raise GuidelinePolicyContractError("policy_waiver_non_waivable")
    if expected is not None and (
        expected.board_id != finding.subject.board_id
        or expected.finding_id != finding.finding_id
        or expected.receipt_id != finding.receipt_id
        or expected.guideline_id != finding.guideline_id
        or expected.revision_id != finding.revision_id
        or expected.rule_id != finding.rule_id
        or expected.subject != finding.subject
        or policy_waiver_scope_digest_for_head(expected)
        != policy_waiver_scope_digest_for(finding)
    ):
        raise GuidelinePolicyContractError("policy_waiver_source_scope_mismatch")
    return rule


def policy_waiver_expire_reason_for(
    source: PolicyWaiverSource,
) -> PolicyWaiverExpireReasonCode:
    """Derive one stable structural invalidation code from stale fences."""

    _validate_source(source=source, require_current=False)
    if source.currentness.currentness is not PolicyCurrentness.STALE:
        raise GuidelinePolicyContractError(
            "policy_waiver_invalidation_requires_stale_source"
        )
    reasons = set(source.currentness.reasons)
    if PolicyCurrentnessReason.CURRENT_SNAPSHOT_MISSING in reasons:
        raise GuidelinePolicyContractError(
            "policy_waiver_invalidation_evidence_unavailable"
        )
    # Most structural cause wins, yielding the same answer for any input order.
    if reasons & {
        PolicyCurrentnessReason.CATALOG_VERSION_CHANGED,
        PolicyCurrentnessReason.RULESET_VERSION_CHANGED,
    }:
        return PolicyWaiverExpireReasonCode.GUIDELINE_RULE_CHANGED
    if reasons & {
        PolicyCurrentnessReason.POLICY_SET_CHANGED,
        PolicyCurrentnessReason.BINDING_HEAD_CHANGED,
    }:
        return PolicyWaiverExpireReasonCode.GUIDELINE_REVISION_CHANGED
    if reasons & {
        PolicyCurrentnessReason.SUBJECT_VERSION_CHANGED,
        PolicyCurrentnessReason.SUBJECT_CONTENT_CHANGED,
        PolicyCurrentnessReason.INPUT_DIGEST_CHANGED,
    }:
        return PolicyWaiverExpireReasonCode.SUBJECT_SCOPE_CHANGED
    raise GuidelinePolicyContractError("policy_waiver_invalidation_reason_unsupported")


def request_policy_waiver(
    *,
    event_id: str,
    waiver_id: str,
    source: PolicyWaiverSource,
    requester_id: str,
    reason: str,
    evidence_refs: tuple[str, ...],
    expires_at: datetime,
    occurred_at: datetime,
) -> PolicyWaiverMutation:
    """Create revision 1 and its sole immutable REQUEST event."""

    _validate_source(source=source)
    finding = source.finding
    now = _aware_utc(
        occurred_at,
        "policy_waiver_event_occurred_at_invalid",
    )
    if now < finding.created_at:
        raise GuidelinePolicyContractError("policy_waiver_request_before_finding")
    expiry = _aware_utc(
        expires_at,
        "policy_waiver_event_expires_at_invalid",
    )
    if expiry <= now:
        raise GuidelinePolicyContractError("policy_waiver_event_expiry_not_future")
    justification = _required_text(
        reason,
        "policy_waiver_justification_required",
    )
    evidence = _evidence_refs(evidence_refs)
    requester = _required_text(
        requester_id,
        "policy_waiver_requested_by_required",
    )
    scope_digest = policy_waiver_scope_digest_for(finding)
    event = PolicyWaiverEvent(
        event_id=event_id,
        waiver_id=waiver_id,
        board_id=finding.subject.board_id,
        waiver_revision=1,
        event_type=PolicyWaiverEventType.REQUEST,
        from_status=None,
        to_status=PolicyWaiverStatus.REQUESTED,
        actor_id=requester,
        occurred_at=now,
        reason=justification,
        evidence_refs=evidence,
        expires_at=expiry,
        scope_digest=scope_digest,
    )
    head = PolicyWaiver(
        waiver_id=waiver_id,
        board_id=finding.subject.board_id,
        finding_id=finding.finding_id,
        receipt_id=finding.receipt_id,
        guideline_id=finding.guideline_id,
        revision_id=finding.revision_id,
        rule_id=finding.rule_id,
        subject=finding.subject,
        status=PolicyWaiverStatus.REQUESTED,
        justification=justification,
        evidence_refs=evidence,
        requested_by=requester,
        requested_at=now,
        waiver_revision=1,
        expires_at=expiry,
        last_event_id=event.event_id,
        last_event_type=event.event_type,
        last_event_at=event.occurred_at,
    )
    return PolicyWaiverMutation(
        waiver=head,
        event=event,
        source=source,
    )


def transition_policy_waiver(
    *,
    waiver: PolicyWaiver,
    event_id: str,
    event_type: PolicyWaiverEventType,
    actor_id: str,
    reason: str,
    occurred_at: datetime,
    expected_waiver_revision: int,
    evidence_refs: tuple[str, ...] = (),
    new_expires_at: datetime | None = None,
    source: PolicyWaiverSource | None = None,
    expire_reason_code: PolicyWaiverExpireReasonCode | None = None,
) -> PolicyWaiverMutation:
    """Apply one legal transition and return the next head plus one event."""

    if not isinstance(waiver, PolicyWaiver):
        raise GuidelinePolicyContractError("policy_waiver_invalid")
    if not isinstance(event_type, PolicyWaiverEventType):
        raise GuidelinePolicyContractError("policy_waiver_event_type_invalid")
    if (
        not isinstance(expected_waiver_revision, int)
        or isinstance(expected_waiver_revision, bool)
        or expected_waiver_revision != waiver.waiver_revision
    ):
        raise GuidelinePolicyContractError("policy_waiver_revision_conflict")
    if event_type is PolicyWaiverEventType.REQUEST:
        raise GuidelinePolicyContractError("policy_waiver_event_transition_invalid")
    if (
        event_type is not PolicyWaiverEventType.EXPIRE
        and expire_reason_code is not None
    ):
        raise GuidelinePolicyContractError(
            "policy_waiver_event_expire_reason_code_forbidden"
        )
    if (
        event_type
        not in {
            PolicyWaiverEventType.APPROVE,
            PolicyWaiverEventType.REVALIDATE,
            PolicyWaiverEventType.EXPIRE,
        }
        and source is not None
    ):
        raise GuidelinePolicyContractError("policy_waiver_transition_source_forbidden")

    actor = _required_text(actor_id, "policy_waiver_event_actor_id_required")
    event_reason = _required_text(
        reason,
        "policy_waiver_event_reason_required",
    )
    now = _aware_utc(
        occurred_at,
        "policy_waiver_event_occurred_at_invalid",
    )
    if now < waiver.last_event_at:
        raise GuidelinePolicyContractError("policy_waiver_event_time_regression")

    privilege_granting = event_type in {
        PolicyWaiverEventType.APPROVE,
        PolicyWaiverEventType.REVALIDATE,
    }
    if privilege_granting:
        if source is None:
            raise GuidelinePolicyContractError("policy_waiver_current_source_required")
        _validate_source(source=source, expected=waiver)
        if actor == waiver.requested_by:
            raise GuidelinePolicyContractError(
                "policy_waiver_independent_reviewer_required"
            )
    evidence = _evidence_refs(evidence_refs)

    next_status: PolicyWaiverStatus
    expiry = waiver.expires_at
    reviewed_by = waiver.reviewed_by
    reviewed_at = waiver.reviewed_at
    review_reason = waiver.review_reason
    revoked_by = waiver.revoked_by
    revoked_at = waiver.revoked_at
    expiry_reason = waiver.expire_reason_code

    if event_type is PolicyWaiverEventType.APPROVE:
        if waiver.status is not PolicyWaiverStatus.REQUESTED:
            raise GuidelinePolicyContractError("policy_waiver_event_transition_invalid")
        if now >= waiver.expires_at:
            raise GuidelinePolicyContractError("policy_waiver_request_expired")
        if new_expires_at is not None:
            raise GuidelinePolicyContractError(
                "policy_waiver_approval_scope_change_forbidden"
            )
        next_status = PolicyWaiverStatus.APPROVED
        reviewed_by = actor
        reviewed_at = now
        review_reason = event_reason
    elif event_type is PolicyWaiverEventType.REJECT:
        if waiver.status is not PolicyWaiverStatus.REQUESTED:
            raise GuidelinePolicyContractError("policy_waiver_event_transition_invalid")
        if actor == waiver.requested_by:
            raise GuidelinePolicyContractError(
                "policy_waiver_independent_reviewer_required"
            )
        if new_expires_at is not None:
            raise GuidelinePolicyContractError("policy_waiver_expiry_change_forbidden")
        next_status = PolicyWaiverStatus.REJECTED
        reviewed_by = actor
        reviewed_at = now
        review_reason = event_reason
    elif event_type is PolicyWaiverEventType.REVOKE:
        if waiver.status is not PolicyWaiverStatus.APPROVED:
            raise GuidelinePolicyContractError("policy_waiver_event_transition_invalid")
        if new_expires_at is not None:
            raise GuidelinePolicyContractError("policy_waiver_expiry_change_forbidden")
        next_status = PolicyWaiverStatus.REVOKED
        revoked_by = actor
        revoked_at = now
    elif event_type is PolicyWaiverEventType.EXPIRE:
        if waiver.status is not PolicyWaiverStatus.APPROVED:
            raise GuidelinePolicyContractError("policy_waiver_event_transition_invalid")
        if new_expires_at is not None:
            raise GuidelinePolicyContractError("policy_waiver_expiry_change_forbidden")
        expiry_reason = (
            expire_reason_code or PolicyWaiverExpireReasonCode.SCHEDULED_EXPIRY
        )
        if not isinstance(expiry_reason, PolicyWaiverExpireReasonCode):
            raise GuidelinePolicyContractError(
                "policy_waiver_event_expire_reason_code_invalid"
            )
        if expiry_reason is PolicyWaiverExpireReasonCode.SCHEDULED_EXPIRY:
            if now < waiver.expires_at:
                raise GuidelinePolicyContractError(
                    "policy_waiver_event_transition_invalid"
                )
            if source is not None:
                raise GuidelinePolicyContractError(
                    "policy_waiver_scheduled_expiry_source_forbidden"
                )
        else:
            if source is None:
                raise GuidelinePolicyContractError(
                    "policy_waiver_invalidation_source_required"
                )
            _validate_source(
                source=source,
                expected=waiver,
                require_current=False,
            )
            if policy_waiver_expire_reason_for(source) is not expiry_reason:
                raise GuidelinePolicyContractError(
                    "policy_waiver_invalidation_reason_mismatch"
                )
        next_status = PolicyWaiverStatus.EXPIRED
    else:
        if waiver.status not in {
            PolicyWaiverStatus.APPROVED,
            PolicyWaiverStatus.EXPIRED,
        }:
            raise GuidelinePolicyContractError("policy_waiver_event_transition_invalid")
        if waiver.status is PolicyWaiverStatus.APPROVED and now >= waiver.expires_at:
            raise GuidelinePolicyContractError("policy_waiver_expiry_event_required")
        if (
            waiver.status is PolicyWaiverStatus.EXPIRED
            and waiver.expire_reason_code
            is not PolicyWaiverExpireReasonCode.SCHEDULED_EXPIRY
        ):
            raise GuidelinePolicyContractError(
                "policy_waiver_structural_invalidation_terminal"
            )
        if new_expires_at is None:
            raise GuidelinePolicyContractError(
                "policy_waiver_revalidation_expiry_required"
            )
        expiry = _aware_utc(
            new_expires_at,
            "policy_waiver_event_expires_at_invalid",
        )
        if expiry <= now or expiry <= waiver.expires_at:
            raise GuidelinePolicyContractError(
                "policy_waiver_revalidation_must_extend_expiry"
            )
        next_status = PolicyWaiverStatus.APPROVED
        reviewed_by = actor
        reviewed_at = now
        review_reason = event_reason
        revoked_by = None
        revoked_at = None
        expiry_reason = None

    scope_digest = policy_waiver_scope_digest_for_head(waiver)
    event = PolicyWaiverEvent(
        event_id=event_id,
        waiver_id=waiver.waiver_id,
        board_id=waiver.board_id,
        waiver_revision=waiver.waiver_revision + 1,
        event_type=event_type,
        from_status=waiver.status,
        to_status=next_status,
        actor_id=actor,
        occurred_at=now,
        reason=event_reason,
        evidence_refs=evidence,
        expires_at=expiry,
        scope_digest=scope_digest,
        expire_reason_code=expiry_reason,
    )
    head = replace(
        waiver,
        status=next_status,
        waiver_revision=event.waiver_revision,
        expires_at=expiry,
        last_event_id=event.event_id,
        last_event_type=event.event_type,
        last_event_at=event.occurred_at,
        reviewed_by=reviewed_by,
        reviewed_at=reviewed_at,
        review_reason=review_reason,
        revoked_by=revoked_by,
        revoked_at=revoked_at,
        expire_reason_code=expiry_reason,
    )
    return PolicyWaiverMutation(
        waiver=head,
        event=event,
        previous=waiver,
        source=source,
    )


__all__ = [
    "POLICY_WAIVER_EVENT_CONTRACT_VERSION",
    "POLICY_WAIVER_EVENT_ORDERING",
    "POLICY_WAIVER_SCOPE_CONTRACT_VERSION",
    "PolicyWaiverMutation",
    "PolicyWaiverSource",
    "policy_waiver_head_digest",
    "policy_waiver_expire_reason_for",
    "policy_waiver_scope_digest",
    "policy_waiver_scope_digest_for",
    "policy_waiver_scope_digest_for_head",
    "request_policy_waiver",
    "transition_policy_waiver",
]
