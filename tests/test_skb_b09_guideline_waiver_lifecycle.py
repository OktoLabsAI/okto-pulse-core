"""SK-B B09 acceptance tests for governed append-only policy waivers."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.domain.guideline_compliance import (
    POLICY_WAIVER_ORDERING,
    PolicyCursorCodec,
    PolicyCurrentnessAssessment,
    PolicyCurrentnessReason,
    PolicyProjection,
    PolicyWaiverPage,
    PolicyWaiverPageCursor,
    project_policy_waiver,
)
from okto_pulse.core.domain.guideline_policy import (
    GuidelineEnforcement,
    GuidelinePolicyContractError,
    GuidelinePredicate,
    GuidelineRevision,
    GuidelineRule,
    PolicyComplianceFinding,
    PolicyCurrentness,
    PolicyEntityType,
    PolicyEvaluationOutcome,
    PolicySubjectRef,
    PolicyWaiverEvent,
    PolicyWaiverEventType,
    PolicyWaiverExpireReasonCode,
    PolicyWaiverStatus,
)
from okto_pulse.core.domain.guideline_lifecycle import (
    guideline_revision_content_digest_v1,
)
from okto_pulse.core.domain.guideline_waiver_lifecycle import (
    POLICY_WAIVER_EVENT_CONTRACT_VERSION,
    POLICY_WAIVER_SCOPE_CONTRACT_VERSION,
    PolicyWaiverMutation,
    PolicyWaiverSource,
    policy_waiver_head_digest,
    policy_waiver_scope_digest_for,
    policy_waiver_scope_digest_for_head,
    request_policy_waiver,
    transition_policy_waiver,
)
from okto_pulse.core.ports.guideline_policy import (
    GuidelinePolicyInvalidCursor,
    PolicyWaiverListQuery,
)


NOW = datetime(2026, 7, 29, 12, tzinfo=timezone.utc)
EXPIRY = NOW + timedelta(days=7)


def _subject(**changes: object) -> PolicySubjectRef:
    values: dict[str, object] = {
        "board_id": "board-1",
        "entity_type": PolicyEntityType.SPEC,
        "subject_id": "spec-1",
        "subject_version": 3,
    }
    values.update(changes)
    return PolicySubjectRef(**values)


def _rule(*, waivable: bool = True, rule_id: str = "rule-1") -> GuidelineRule:
    return GuidelineRule(
        rule_id=rule_id,
        code="spec.traceability",
        title="Traceability",
        description="The spec must preserve traceability.",
        target_entity_types=(PolicyEntityType.SPEC,),
        predicates=(GuidelinePredicate("exists"),),
        enforcement=GuidelineEnforcement.BLOCKING,
        waivable=waivable,
    )


def _finding(**changes: object) -> PolicyComplianceFinding:
    values: dict[str, object] = {
        "finding_id": "finding-1",
        "receipt_id": "receipt-1",
        "subject": _subject(),
        "guideline_id": "guideline-1",
        "revision_id": "revision-7",
        "rule_id": "rule-1",
        "outcome": PolicyEvaluationOutcome.FAIL,
        "enforcement": GuidelineEnforcement.BLOCKING,
        "message": "Traceability is incomplete.",
        "created_at": NOW - timedelta(minutes=1),
        "evidence_refs": ("spec://spec-1",),
    }
    values.update(changes)
    return PolicyComplianceFinding(**values)


def _source(
    *,
    finding: PolicyComplianceFinding | None = None,
    rule: GuidelineRule | None = None,
    current: bool = True,
    reasons: tuple[PolicyCurrentnessReason, ...] = (),
) -> PolicyWaiverSource:
    actual_finding = finding or _finding()
    actual_rule = rule or _rule(rule_id=actual_finding.rule_id)
    title = "Waiver source guideline"
    content = "Canonical executable rules for the waiver source."
    revision = GuidelineRevision(
        revision_id=actual_finding.revision_id,
        guideline_id=actual_finding.guideline_id,
        revision_number=1,
        semantic_version="1.0.0",
        title=title,
        content=content,
        content_digest=guideline_revision_content_digest_v1(
            title=title,
            content=content,
            rules=(actual_rule,),
        ),
        rules=(actual_rule,),
        created_by="author-1",
        created_at=NOW - timedelta(days=1),
    )
    return PolicyWaiverSource(
        finding=actual_finding,
        revision=revision,
        currentness=PolicyCurrentnessAssessment(
            currentness=(
                PolicyCurrentness.CURRENT if current else PolicyCurrentness.STALE
            ),
            reasons=(
                ()
                if current
                else reasons or (PolicyCurrentnessReason.SUBJECT_CONTENT_CHANGED,)
            ),
        ),
    )


def _requested():
    mutation = request_policy_waiver(
        event_id="event-1",
        waiver_id="waiver-1",
        source=_source(),
        requester_id="requester-1",
        reason="Temporary migration exception.",
        evidence_refs=("ticket://waiver-1",),
        expires_at=EXPIRY,
        occurred_at=NOW,
    )
    assert isinstance(mutation, PolicyWaiverMutation)
    return mutation


def _approved():
    requested, _ = _requested()
    return transition_policy_waiver(
        waiver=requested,
        event_id="event-2",
        event_type=PolicyWaiverEventType.APPROVE,
        actor_id="reviewer-1",
        reason="Bounded, independently reviewed exception.",
        evidence_refs=("review://waiver-1",),
        occurred_at=NOW + timedelta(hours=1),
        expected_waiver_revision=1,
        source=_source(),
    )


def test_contract_versions_and_closed_event_surface_are_frozen() -> None:
    assert POLICY_WAIVER_EVENT_CONTRACT_VERSION == "waiver-event/v1"
    assert POLICY_WAIVER_SCOPE_CONTRACT_VERSION == "waiver-scope/v1"
    assert {item.value for item in PolicyWaiverEventType} == {
        "request",
        "approve",
        "reject",
        "revoke",
        "expire",
        "revalidate",
    }
    assert "revalidate" not in {item.value for item in PolicyWaiverStatus}


def test_request_and_approve_preserve_exact_scope_and_audit_events() -> None:
    requested, request_event = _requested()
    approved, approve_event = _approved()

    assert requested.status is PolicyWaiverStatus.REQUESTED
    assert request_event.from_status is None
    assert request_event.to_status is PolicyWaiverStatus.REQUESTED
    assert approved.status is PolicyWaiverStatus.APPROVED
    assert approved.waiver_revision == 2
    assert approved.reviewed_by == "reviewer-1"
    assert approve_event.from_status is PolicyWaiverStatus.REQUESTED
    assert approve_event.to_status is PolicyWaiverStatus.APPROVED
    assert request_event.scope_digest == approve_event.scope_digest
    assert request_event.scope_digest == policy_waiver_scope_digest_for_head(approved)
    assert policy_waiver_scope_digest_for(_finding()) == request_event.scope_digest
    assert len(policy_waiver_head_digest(approved)) == 64
    assert approved.is_effective_at(NOW + timedelta(hours=2))


def test_reject_and_revoke_are_terminal_and_each_transition_is_one_event() -> None:
    requested, _ = _requested()
    rejected, rejected_event = transition_policy_waiver(
        waiver=requested,
        event_id="event-reject",
        event_type=PolicyWaiverEventType.REJECT,
        actor_id="reviewer-1",
        reason="Risk is not bounded.",
        evidence_refs=("review://waiver-1/rejection",),
        occurred_at=NOW + timedelta(hours=1),
        expected_waiver_revision=1,
    )
    assert rejected.status is PolicyWaiverStatus.REJECTED
    assert rejected_event.waiver_revision == 2

    approved, _ = _approved()
    revoked, revoked_event = transition_policy_waiver(
        waiver=approved,
        event_id="event-revoke",
        event_type=PolicyWaiverEventType.REVOKE,
        actor_id="security-1",
        reason="The exception is no longer authorized.",
        evidence_refs=("incident://waiver-1/revocation",),
        occurred_at=NOW + timedelta(hours=2),
        expected_waiver_revision=2,
    )
    assert revoked.status is PolicyWaiverStatus.REVOKED
    assert revoked_event.waiver_revision == 3
    assert not revoked.is_effective_at(NOW + timedelta(hours=3))

    for terminal, revision in ((rejected, 2), (revoked, 3)):
        with pytest.raises(
            GuidelinePolicyContractError,
            match="policy_waiver_event_transition_invalid",
        ):
            transition_policy_waiver(
                waiver=terminal,
                event_id=f"event-after-{terminal.status.value}",
                event_type=PolicyWaiverEventType.REVOKE,
                actor_id="security-1",
                reason="Illegal terminal transition.",
                evidence_refs=("audit://illegal-terminal-transition",),
                occurred_at=NOW + timedelta(days=1),
                expected_waiver_revision=revision,
            )


def test_expiry_is_fail_closed_at_boundary_and_revalidation_is_auditable() -> None:
    approved, _ = _approved()
    assert not approved.is_effective_at(EXPIRY)
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_event_transition_invalid",
    ):
        transition_policy_waiver(
            waiver=approved,
            event_id="event-expire-too-early",
            event_type=PolicyWaiverEventType.EXPIRE,
            actor_id="system",
            reason="Premature sweep.",
            evidence_refs=("clock://waiver-1/expiry",),
            occurred_at=EXPIRY - timedelta(microseconds=1),
            expected_waiver_revision=2,
        )

    expired, expire_event = transition_policy_waiver(
        waiver=approved,
        event_id="event-expire",
        event_type=PolicyWaiverEventType.EXPIRE,
        actor_id="system",
        reason="Expiry boundary reached.",
        evidence_refs=("clock://waiver-1/expiry",),
        occurred_at=EXPIRY,
        expected_waiver_revision=2,
    )
    revalidated, revalidate_event = transition_policy_waiver(
        waiver=expired,
        event_id="event-revalidate",
        event_type=PolicyWaiverEventType.REVALIDATE,
        actor_id="reviewer-2",
        reason="Revalidated against the unchanged current scope.",
        evidence_refs=("review://waiver-1/revalidation",),
        new_expires_at=EXPIRY + timedelta(days=7),
        occurred_at=EXPIRY + timedelta(minutes=1),
        expected_waiver_revision=3,
        source=_source(),
    )

    assert expire_event.to_status is PolicyWaiverStatus.EXPIRED
    assert (
        expire_event.expire_reason_code is PolicyWaiverExpireReasonCode.SCHEDULED_EXPIRY
    )
    assert revalidate_event.from_status is PolicyWaiverStatus.EXPIRED
    assert revalidate_event.to_status is PolicyWaiverStatus.APPROVED
    assert revalidated.last_event_type is PolicyWaiverEventType.REVALIDATE
    assert revalidated.waiver_revision == 4
    assert revalidated.is_effective_at(EXPIRY + timedelta(hours=1))


def test_structural_drift_materializes_one_expire_without_seventh_event() -> None:
    approved, _ = _approved()
    stale_source = _source(
        current=False,
        reasons=(PolicyCurrentnessReason.SUBJECT_VERSION_CHANGED,),
    )

    invalidated, event = transition_policy_waiver(
        waiver=approved,
        event_id="event-invalidate",
        event_type=PolicyWaiverEventType.EXPIRE,
        actor_id="policy-system",
        reason="The exact subject scope changed.",
        evidence_refs=("fence://subject-version-changed",),
        occurred_at=NOW + timedelta(hours=2),
        expected_waiver_revision=2,
        source=stale_source,
        expire_reason_code=(PolicyWaiverExpireReasonCode.SUBJECT_SCOPE_CHANGED),
    )

    assert invalidated.status is PolicyWaiverStatus.EXPIRED
    assert invalidated.expires_at == EXPIRY
    assert event.expires_at == EXPIRY
    assert (
        event.expire_reason_code is PolicyWaiverExpireReasonCode.SUBJECT_SCOPE_CHANGED
    )
    summary = project_policy_waiver(
        invalidated,
        projection=PolicyProjection.SUMMARY,
        source_current=False,
        evaluated_at=NOW + timedelta(hours=2),
    )
    assert summary.expire_reason_code is event.expire_reason_code
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_structural_invalidation_terminal",
    ):
        transition_policy_waiver(
            waiver=invalidated,
            event_id="event-illegal-revalidate",
            event_type=PolicyWaiverEventType.REVALIDATE,
            actor_id="reviewer-2",
            reason="Structural invalidations require a new lineage.",
            evidence_refs=("review://new-lineage-required",),
            new_expires_at=EXPIRY + timedelta(days=7),
            occurred_at=NOW + timedelta(hours=3),
            expected_waiver_revision=3,
            source=_source(),
        )


def test_invalidation_requires_authoritative_drift_and_stable_reason() -> None:
    approved, _ = _approved()
    unavailable = _source(
        current=False,
        reasons=(PolicyCurrentnessReason.CURRENT_SNAPSHOT_MISSING,),
    )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_invalidation_evidence_unavailable",
    ):
        transition_policy_waiver(
            waiver=approved,
            event_id="event-unavailable",
            event_type=PolicyWaiverEventType.EXPIRE,
            actor_id="policy-system",
            reason="Transient resolver failure is not structural drift.",
            evidence_refs=("resolver://snapshot-missing",),
            occurred_at=NOW + timedelta(hours=2),
            expected_waiver_revision=2,
            source=unavailable,
            expire_reason_code=(PolicyWaiverExpireReasonCode.SUBJECT_SCOPE_CHANGED),
        )

    drifted = _source(
        current=False,
        reasons=(PolicyCurrentnessReason.BINDING_HEAD_CHANGED,),
    )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_invalidation_reason_mismatch",
    ):
        transition_policy_waiver(
            waiver=approved,
            event_id="event-wrong-reason",
            event_type=PolicyWaiverEventType.EXPIRE,
            actor_id="policy-system",
            reason="The supplied reason code does not match the proof.",
            evidence_refs=("fence://binding-head-changed",),
            occurred_at=NOW + timedelta(hours=2),
            expected_waiver_revision=2,
            source=drifted,
            expire_reason_code=(PolicyWaiverExpireReasonCode.GUIDELINE_RULE_CHANGED),
        )


def test_mutation_rejects_fabricated_reviewer_trail_and_expiry_delta() -> None:
    requested, _ = _requested()
    approved, approve_event = _approved()
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_mutation_review_mismatch",
    ):
        PolicyWaiverMutation(
            waiver=approved,
            event=replace(
                approve_event,
                actor_id=requested.requested_by,
            ),
            previous=requested,
            source=_source(),
        )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_expiry_change_forbidden",
    ):
        PolicyWaiverMutation(
            waiver=replace(
                approved,
                expires_at=EXPIRY + timedelta(days=365),
            ),
            event=replace(
                approve_event,
                expires_at=EXPIRY + timedelta(days=365),
            ),
            previous=requested,
            source=_source(),
        )


def test_expired_approved_head_requires_expire_before_revalidation() -> None:
    approved, _ = _approved()
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_expiry_event_required",
    ):
        transition_policy_waiver(
            waiver=approved,
            event_id="event-skip-expire",
            event_type=PolicyWaiverEventType.REVALIDATE,
            actor_id="reviewer-2",
            reason="Cannot skip the auditable expiry event.",
            evidence_refs=("review://skip-expire",),
            new_expires_at=EXPIRY + timedelta(days=7),
            occurred_at=EXPIRY + timedelta(minutes=1),
            expected_waiver_revision=2,
            source=_source(),
        )


@pytest.mark.parametrize(
    ("event_type", "status"),
    (
        (PolicyWaiverEventType.REVOKE, PolicyWaiverStatus.REQUESTED),
        (PolicyWaiverEventType.EXPIRE, PolicyWaiverStatus.REQUESTED),
        (PolicyWaiverEventType.REVALIDATE, PolicyWaiverStatus.REQUESTED),
    ),
)
def test_requested_rejects_illegal_privilege_transitions(
    event_type: PolicyWaiverEventType,
    status: PolicyWaiverStatus,
) -> None:
    requested, _ = _requested()
    assert requested.status is status
    with pytest.raises(GuidelinePolicyContractError):
        transition_policy_waiver(
            waiver=requested,
            event_id=f"event-{event_type.value}",
            event_type=event_type,
            actor_id="reviewer-1",
            reason="Illegal transition.",
            evidence_refs=("audit://illegal-transition",),
            occurred_at=NOW + timedelta(hours=1),
            expected_waiver_revision=1,
        )


def test_request_and_privilege_grants_refuse_non_waivable_or_stale_scope() -> None:
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_non_waivable",
    ):
        request_policy_waiver(
            event_id="event-protected",
            waiver_id="waiver-protected",
            source=_source(rule=_rule(waivable=False)),
            requester_id="requester-1",
            reason="Must be refused.",
            evidence_refs=("ticket://protected",),
            expires_at=EXPIRY,
            occurred_at=NOW,
        )

    requested, _ = _requested()
    drifted = replace(
        _finding(),
        subject=replace(_subject(), subject_version=4),
    )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_source_scope_mismatch",
    ):
        transition_policy_waiver(
            waiver=requested,
            event_id="event-drifted",
            event_type=PolicyWaiverEventType.APPROVE,
            actor_id="reviewer-1",
            reason="Cannot approve drifted scope.",
            evidence_refs=("review://drifted",),
            occurred_at=NOW + timedelta(hours=1),
            expected_waiver_revision=1,
            source=_source(finding=drifted),
        )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_source_not_current",
    ):
        transition_policy_waiver(
            waiver=requested,
            event_id="event-stale",
            event_type=PolicyWaiverEventType.APPROVE,
            actor_id="reviewer-1",
            reason="Cannot approve stale evidence.",
            evidence_refs=("review://stale",),
            occurred_at=NOW + timedelta(hours=1),
            expected_waiver_revision=1,
            source=_source(current=False),
        )


def test_every_source_scope_axis_is_bound_and_request_cannot_predate_finding() -> None:
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_request_before_finding",
    ):
        request_policy_waiver(
            event_id="event-before-finding",
            waiver_id="waiver-before-finding",
            source=_source(),
            requester_id="requester-1",
            reason="Chronologically impossible request.",
            evidence_refs=("ticket://before-finding",),
            expires_at=EXPIRY,
            occurred_at=NOW - timedelta(minutes=2),
        )

    requested, _ = _requested()
    variants = (
        replace(_finding(), finding_id="finding-other"),
        replace(_finding(), receipt_id="receipt-other"),
        replace(_finding(), guideline_id="guideline-other"),
        replace(_finding(), revision_id="revision-other"),
        replace(_finding(), rule_id="rule-other"),
        replace(
            _finding(),
            subject=replace(_subject(), board_id="board-other"),
        ),
        replace(
            _finding(),
            subject=replace(_subject(), subject_id="spec-other"),
        ),
        replace(
            _finding(),
            subject=replace(_subject(), subject_version=4),
        ),
        replace(
            _finding(),
            subject=replace(
                _subject(),
                entity_type=PolicyEntityType.REFINEMENT,
            ),
        ),
    )
    for index, drifted in enumerate(variants):
        with pytest.raises(GuidelinePolicyContractError):
            transition_policy_waiver(
                waiver=requested,
                event_id=f"event-axis-{index}",
                event_type=PolicyWaiverEventType.APPROVE,
                actor_id="reviewer-1",
                reason="Scope drift is forbidden.",
                evidence_refs=("review://scope-drift",),
                occurred_at=NOW + timedelta(hours=1),
                expected_waiver_revision=1,
                source=_source(finding=drifted),
            )


def test_independent_review_expiry_and_revision_are_mandatory() -> None:
    requested, _ = _requested()
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_independent_reviewer_required",
    ):
        transition_policy_waiver(
            waiver=requested,
            event_id="event-self-approve",
            event_type=PolicyWaiverEventType.APPROVE,
            actor_id=requested.requested_by,
            reason="Self approval is forbidden.",
            evidence_refs=("review://self",),
            occurred_at=NOW + timedelta(hours=1),
            expected_waiver_revision=1,
            source=_source(),
        )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_revision_conflict",
    ):
        transition_policy_waiver(
            waiver=requested,
            event_id="event-stale-cas",
            event_type=PolicyWaiverEventType.REJECT,
            actor_id="reviewer-1",
            reason="Stale expected revision.",
            evidence_refs=("review://stale-cas",),
            occurred_at=NOW + timedelta(hours=1),
            expected_waiver_revision=9,
        )


def test_projection_is_slim_by_default_and_never_misstates_effectiveness() -> None:
    approved, _ = _approved()
    summary = project_policy_waiver(
        approved,
        projection=PolicyProjection.SUMMARY,
        source_current=True,
        evaluated_at=NOW + timedelta(hours=2),
    )
    detail = project_policy_waiver(
        approved,
        projection=PolicyProjection.DETAIL,
        source_current=True,
        evaluated_at=NOW + timedelta(hours=2),
    )
    stale = project_policy_waiver(
        approved,
        projection=PolicyProjection.SUMMARY,
        source_current=False,
        evaluated_at=NOW + timedelta(hours=2),
    )

    assert summary.justification is None
    assert summary.evidence_refs is None
    assert summary.review_reason is None
    assert summary.effective is True
    assert detail.justification == approved.justification
    assert detail.evidence_refs == approved.evidence_refs
    assert stale.effective is False


def test_waiver_keyset_cursor_is_hmac_bound_to_filters_and_projection() -> None:
    initial = PolicyWaiverListQuery(
        board_id="board-1",
        evaluated_at=NOW,
        guideline_id="guideline-1",
        status=PolicyWaiverStatus.APPROVED,
        projection=PolicyProjection.SUMMARY,
        limit=50,
    )
    cursor = PolicyWaiverPageCursor(
        created_at=NOW,
        item_id="waiver-100",
        filter_digest=initial.filter_digest,
        projection_digest=initial.projection_digest,
    )
    codec = PolicyCursorCodec(b"s" * 32)
    token = codec.encode(cursor)
    decoded = codec.decode(token, expected_kind="waiver")

    assert decoded == cursor
    assert cursor.ordering == POLICY_WAIVER_ORDERING
    PolicyWaiverListQuery(
        board_id="board-1",
        evaluated_at=NOW,
        guideline_id="guideline-1",
        status=PolicyWaiverStatus.APPROVED,
        projection=PolicyProjection.SUMMARY,
        cursor=cursor,
    )
    with pytest.raises(
        GuidelinePolicyInvalidCursor,
        match="policy_waiver_cursor_context_mismatch",
    ):
        PolicyWaiverListQuery(
            board_id="board-1",
            evaluated_at=NOW,
            guideline_id="guideline-1",
            status=PolicyWaiverStatus.APPROVED,
            projection=PolicyProjection.DETAIL,
            cursor=cursor,
        )
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(f"{token[:-1]}A", expected_kind="waiver")

    page = PolicyWaiverPage(
        items=(),
        limit=50,
        next_cursor=cursor,
        has_more=True,
    )
    assert page.ordering == POLICY_WAIVER_ORDERING


def test_event_contract_rejects_early_expiry_and_bad_transition_pair() -> None:
    scope_digest = policy_waiver_scope_digest_for(_finding())
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_event_transition_invalid",
    ):
        PolicyWaiverEvent(
            event_id="event-invalid-pair",
            waiver_id="waiver-1",
            board_id="board-1",
            waiver_revision=2,
            event_type=PolicyWaiverEventType.APPROVE,
            from_status=PolicyWaiverStatus.APPROVED,
            to_status=PolicyWaiverStatus.APPROVED,
            actor_id="reviewer-1",
            occurred_at=NOW,
            reason="Invalid pair.",
            evidence_refs=("review://invalid",),
            expires_at=EXPIRY,
            scope_digest=scope_digest,
        )
