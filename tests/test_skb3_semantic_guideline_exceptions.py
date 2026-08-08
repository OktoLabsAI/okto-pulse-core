"""SK-B3 I2 tests for exact findings, waivers, and human policy skips."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.domain.guideline_policy import (
    BoardGuidelineBinding,
    GuidelineEnforcement,
    GuidelineMetric,
    GuidelineMetricDirection,
    GuidelinePolicyContractError,
    GuidelineRevision,
    PolicyCurrentness,
    PolicyEntityType,
    PolicySubjectRef,
    PolicySubjectSnapshot,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentAssessor,
    SemanticAssessmentContractError,
    SemanticGuidelineAssessmentContext,
    SemanticGuidelineAssessmentSubmission,
    SemanticMetricAssessment,
    record_semantic_guideline_assessment,
)
from okto_pulse.core.domain.guideline_semantic_exceptions import (
    SemanticExceptionActorKind,
    SemanticMetricWaiverAnchor,
    SemanticMetricWaiverEventType,
    SemanticMetricWaiverExpireReason,
    SemanticMetricWaiverMutation,
    SemanticMetricWaiverRevalidationReason,
    SemanticMetricWaiverRevalidationStatus,
    SemanticMetricWaiverStatus,
    SemanticPolicySkipEventType,
    SemanticPolicySkipScope,
    SemanticPolicySkipStatus,
    create_semantic_policy_skip,
    request_semantic_metric_waiver,
    revalidate_semantic_metric_waiver,
    revoke_semantic_policy_skip,
    semantic_metric_waiver_revalidation_conflict,
    transition_semantic_metric_waiver,
)
from okto_pulse.core.domain.guideline_semantic_currentness import (
    SemanticAssessmentCurrentnessReason,
)
from okto_pulse.core.domain.guideline_semantic_findings import (
    project_semantic_metric_findings,
)
from okto_pulse.core.domain.quality_assessment import (
    EvidenceRef,
    FindingAnchorType,
    UnboundFindingAnchor,
)


NOW = datetime(2026, 7, 30, 21, tzinfo=timezone.utc)
DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64
DIGEST_D = "d" * 64


def _metric(
    metric_id: str,
    code: str,
    *,
    threshold: int,
) -> GuidelineMetric:
    return GuidelineMetric(
        metric_id=metric_id,
        code=code,
        title=code,
        description=f"Assess {code}.",
        evaluation_rubric=f"Inspect evidence and score {code} from 0 to 100.",
        target_entity_types=(PolicyEntityType.SPEC,),
        direction=GuidelineMetricDirection.MINIMUM,
        default_threshold=threshold,
    )


def _context_and_receipt():
    failed_metric = _metric(
        "metric-segregation",
        "architecture.segregation",
        threshold=80,
    )
    passed_metric = _metric(
        "metric-boundaries",
        "architecture.boundaries",
        threshold=70,
    )
    revision = GuidelineRevision(
        revision_id="revision-1",
        guideline_id="guideline-1",
        revision_number=1,
        semantic_version="1.0.0",
        title="Hexagonal architecture",
        content="Keep domain rules independent from infrastructure.",
        metrics=(failed_metric, passed_metric),
        created_by="author-1",
        created_at=NOW,
    )
    binding = BoardGuidelineBinding(
        binding_id="binding-1",
        board_id="board-1",
        guideline_id=revision.guideline_id,
        revision_id=revision.revision_id,
        semantic_version=revision.semantic_version,
        revision_digest=revision.revision_digest,
        priority=0,
        binding_revision=2,
        adopted_by="owner-1",
        adopted_at=NOW,
        enforcement=GuidelineEnforcement.BLOCKING,
        minimum_confidence=80,
        metric_threshold_overrides={},
    )
    subject_snapshot = PolicySubjectSnapshot(
        subject=PolicySubjectRef(
            board_id="board-1",
            entity_type=PolicyEntityType.SPEC,
            subject_id="spec-1",
            subject_version=4,
        ),
        content_digest=DIGEST_A,
        last_semantic_editor_id="editor-1",
        captured_at=NOW,
    )
    context = SemanticGuidelineAssessmentContext(
        subject_snapshot=subject_snapshot,
        binding=binding,
        revision=revision,
        policy_set_digest=DIGEST_B,
        binding_head_digest=DIGEST_C,
    )
    evidence = EvidenceRef(
        source_type="spec",
        source_id="spec-1",
        source_version=4,
        content_hash=DIGEST_A,
    )
    submission = SemanticGuidelineAssessmentSubmission(
        subject=subject_snapshot.subject,
        binding_id=binding.binding_id,
        expected_binding_revision=binding.binding_revision,
        guideline_revision_id=revision.revision_id,
        idempotency_key="assessment:spec-1:v4",
        confidence=90,
        assessor=SemanticAssessmentAssessor(
            agent_id="reviewer-1",
            model_id="model-a",
        ),
        metric_results=tuple(
            SemanticMetricAssessment(
                metric_id=metric.metric_id,
                score=score,
                rationale=f"Evidence supports score {score} for {metric.code}.",
                evidence_refs=(evidence,),
                pinpoints=(
                    UnboundFindingAnchor(
                        anchor_type=FindingAnchorType.STRUCTURED_CHILD,
                        anchor_ref=f"technical_requirements.{metric.metric_id}",
                        excerpt_hash=DIGEST_D,
                    ),
                ),
            )
            for metric, score in (
                (failed_metric, 60),
                (passed_metric, 90),
            )
        ),
    )
    receipt = record_semantic_guideline_assessment(
        submission,
        context,
        receipt_id="receipt-1",
        recorded_at=NOW,
    ).receipt
    return context, receipt, evidence


def _finding():
    context, receipt, evidence = _context_and_receipt()
    findings = project_semantic_metric_findings(receipt)
    assert len(findings) == 1
    return context, receipt, findings[0], evidence


def _waiver_anchor(finding) -> SemanticMetricWaiverAnchor:
    return SemanticMetricWaiverAnchor.from_finding(
        finding,
        assessment_assessor_id="reviewer-1",
    )


def _request_waiver(*, expires_at: datetime | None = None):
    _, _, finding, evidence = _finding()
    mutation = request_semantic_metric_waiver(
        waiver_id="waiver-1",
        event_id="waiver-event-1",
        anchor=_waiver_anchor(finding),
        justification="A bounded migration requires a temporary exception.",
        evidence_refs=(evidence,),
        requested_by="requester-1",
        requested_at=NOW,
        expires_at=expires_at,
        idempotency_key="waiver:request:1",
    )
    return mutation, finding, evidence


def _approve_waiver(*, expires_at: datetime | None = None):
    requested, finding, evidence = _request_waiver(expires_at=expires_at)
    approved = transition_semantic_metric_waiver(
        requested.waiver,
        event_id="waiver-event-2",
        expected_waiver_revision=1,
        event_type=SemanticMetricWaiverEventType.APPROVE,
        actor_id="reviewer-2",
        occurred_at=NOW + timedelta(minutes=1),
        reason="The temporary exception is bounded and independently reviewed.",
        evidence_refs=(evidence,),
        idempotency_key="waiver:approve:1",
    )
    return requested, approved, finding, evidence


def _skip_scope() -> SemanticPolicySkipScope:
    _, _, finding, _ = _finding()
    return SemanticPolicySkipScope(
        subject=finding.subject,
        subject_content_digest=finding.subject_content_digest,
        guideline_id=finding.guideline_id,
        guideline_revision_id=finding.guideline_revision_id,
        guideline_revision_digest=finding.guideline_revision_digest,
        binding_id=finding.binding_id,
        binding_revision=finding.binding_revision,
        binding_configuration_digest=finding.binding_configuration_digest,
    )


def test_findings_project_exactly_one_failed_result_and_normalize_utc() -> None:
    _, receipt, finding, _ = _finding()

    assert finding.metric_result_id == receipt.metric_results[0].metric_result_id
    assert finding.receipt_digest == receipt.receipt_digest
    assert finding.rationale == receipt.metric_results[0].rationale
    assert finding.evidence_refs == receipt.metric_results[0].evidence_refs
    assert finding.pinpoints == receipt.metric_results[0].pinpoints
    assert finding.created_at.tzinfo is timezone.utc
    assert len(finding.finding_id) == 64
    assert len(finding.finding_digest) == 64
    assert project_semantic_metric_findings(receipt) == (finding,)
    with pytest.raises(FrozenInstanceError):
        finding.rationale = "mutated"  # type: ignore[misc]


def test_waiver_is_exact_to_result_and_finding_and_requires_independent_review() -> (
    None
):
    requested, approved, finding, evidence = _approve_waiver()

    assert requested.waiver.status is SemanticMetricWaiverStatus.REQUESTED
    assert approved.waiver.status is SemanticMetricWaiverStatus.APPROVED
    assert approved.waiver.waiver_revision == 2
    assert approved.event.predecessor_event_id == requested.event.event_id
    assert approved.waiver.is_active_for(
        finding,
        currentness=PolicyCurrentness.CURRENT,
        at=NOW + timedelta(minutes=2),
    )
    assert not approved.waiver.is_active_for(
        finding,
        currentness=PolicyCurrentness.STALE,
        at=NOW + timedelta(minutes=2),
    )
    changed_finding = replace(
        finding,
        finding_id="finding-other",
        finding_digest=None,
    )
    assert not approved.waiver.is_active_for(
        changed_finding,
        currentness=PolicyCurrentness.CURRENT,
        at=NOW + timedelta(minutes=2),
    )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="semantic_waiver_independent_review_required",
    ):
        transition_semantic_metric_waiver(
            requested.waiver,
            event_id="waiver-event-self",
            expected_waiver_revision=1,
            event_type=SemanticMetricWaiverEventType.APPROVE,
            actor_id="requester-1",
            occurred_at=NOW + timedelta(minutes=1),
            reason="Self approval is forbidden.",
            evidence_refs=(evidence,),
            idempotency_key="waiver:self-approve",
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_independent_review_required",
    ):
        transition_semantic_metric_waiver(
            requested.waiver,
            event_id="waiver-event-assessor-review",
            expected_waiver_revision=1,
            event_type=SemanticMetricWaiverEventType.REJECT,
            actor_id="reviewer-1",
            occurred_at=NOW + timedelta(minutes=1),
            reason="The assessment author cannot review its own waiver.",
            evidence_refs=(evidence,),
            idempotency_key="waiver:assessor-review",
        )


def test_waiver_anchor_requires_a_permanent_assessment_assessor_fence() -> None:
    _, _, finding, _ = _finding()

    with pytest.raises(TypeError):
        SemanticMetricWaiverAnchor.from_finding(finding)  # type: ignore[call-arg]
    with pytest.raises(
        GuidelinePolicyContractError,
        match="semantic_waiver_anchor_assessment_assessor_id_invalid",
    ):
        SemanticMetricWaiverAnchor.from_finding(
            finding,
            assessment_assessor_id=" ",
        )


def test_waiver_preserves_requested_expiry_and_expires_fail_closed() -> None:
    expires_at = NOW + timedelta(hours=1)
    _, approved, finding, evidence = _approve_waiver(
        expires_at=expires_at,
    )

    assert approved.waiver.expires_at == expires_at
    assert not approved.waiver.is_active_for(
        finding,
        currentness=PolicyCurrentness.CURRENT,
        at=expires_at,
    )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_scheduled_expiry_not_reached",
    ):
        transition_semantic_metric_waiver(
            approved.waiver,
            event_id="waiver-event-early-expiry",
            expected_waiver_revision=2,
            event_type=SemanticMetricWaiverEventType.EXPIRE,
            actor_id="system",
            occurred_at=NOW + timedelta(minutes=2),
            reason="The scheduled expiry has not arrived.",
            evidence_refs=(evidence,),
            idempotency_key="waiver:expire:early",
            expire_reason=(SemanticMetricWaiverExpireReason.SCHEDULED_EXPIRY),
        )
    expired = transition_semantic_metric_waiver(
        approved.waiver,
        event_id="waiver-event-3",
        expected_waiver_revision=2,
        event_type=SemanticMetricWaiverEventType.EXPIRE,
        actor_id="system",
        occurred_at=expires_at,
        reason="The approved validity window elapsed.",
        evidence_refs=(evidence,),
        idempotency_key="waiver:expire:1",
        expire_reason=SemanticMetricWaiverExpireReason.SCHEDULED_EXPIRY,
    )
    assert expired.waiver.status is SemanticMetricWaiverStatus.EXPIRED
    assert not expired.waiver.is_active_for(
        finding,
        currentness=PolicyCurrentness.CURRENT,
        at=expires_at,
    )


def test_revalidation_detects_live_competing_exact_scope() -> None:
    expires_at = NOW + timedelta(hours=1)
    _, approved, finding, evidence = _approve_waiver(
        expires_at=expires_at,
    )
    expired = transition_semantic_metric_waiver(
        approved.waiver,
        event_id="waiver-event-expired-for-conflict",
        expected_waiver_revision=2,
        event_type=SemanticMetricWaiverEventType.EXPIRE,
        actor_id="system",
        occurred_at=expires_at,
        reason="The first waiver reached its scheduled expiry.",
        evidence_refs=(evidence,),
        idempotency_key="waiver:expire:for-conflict",
        expire_reason=SemanticMetricWaiverExpireReason.SCHEDULED_EXPIRY,
    )
    competitor = request_semantic_metric_waiver(
        waiver_id="waiver-2",
        event_id="waiver-event-competitor",
        anchor=_waiver_anchor(finding),
        justification="A replacement request owns the exact immutable scope.",
        evidence_refs=(evidence,),
        requested_by="requester-2",
        requested_at=expires_at + timedelta(minutes=1),
        expires_at=None,
        idempotency_key="waiver:request:competitor",
    )
    revalidation_at = expires_at + timedelta(minutes=2)

    assert (
        semantic_metric_waiver_revalidation_conflict(
            expired.waiver,
            (competitor.waiver,),
            occurred_at=revalidation_at,
        )
        == competitor.waiver.waiver_id
    )
    assert (
        semantic_metric_waiver_revalidation_conflict(
            expired.waiver,
            (),
            occurred_at=revalidation_at,
        )
        is None
    )


def test_revalidation_appends_current_decision_without_rebinding_anchor() -> None:
    _, approved, _, evidence = _approve_waiver()
    evaluated_at = NOW + timedelta(minutes=2)

    mutation = revalidate_semantic_metric_waiver(
        approved.waiver,
        event_id="waiver-event-current-revalidation",
        expected_waiver_revision=2,
        actor_id="revalidator-3",
        occurred_at=evaluated_at,
        evaluated_at=evaluated_at,
        status=SemanticMetricWaiverRevalidationStatus.APPROVED,
        reason_code=SemanticMetricWaiverRevalidationReason.CURRENT,
        currentness_reasons=(),
        scheduled_expiry_observed=False,
        evidence_refs=(evidence,),
        idempotency_key="waiver:revalidate:current",
    )

    assert mutation.waiver.anchor == approved.waiver.anchor
    assert mutation.waiver.scope_digest == approved.waiver.scope_digest
    assert mutation.waiver.status is SemanticMetricWaiverStatus.APPROVED
    assert mutation.waiver.waiver_revision == 3
    assert (
        mutation.waiver.last_revalidation_status
        is SemanticMetricWaiverRevalidationStatus.APPROVED
    )
    assert mutation.waiver.last_revalidation_current is True
    assert (
        mutation.waiver.last_revalidation_reason_code
        is SemanticMetricWaiverRevalidationReason.CURRENT
    )
    assert mutation.event.predecessor_event_id == approved.event.event_id
    assert mutation.event.evaluated_at == evaluated_at
    assert mutation.event.waiver_digest == mutation.waiver.head_digest
    assert mutation.waiver.last_event_idempotency_key == "waiver:revalidate:current"


def test_anchor_stale_precedes_observed_expiry_without_reactivation() -> None:
    expires_at = NOW + timedelta(hours=1)
    _, approved, _, evidence = _approve_waiver(expires_at=expires_at)
    evaluated_at = expires_at + timedelta(minutes=1)
    reasons = (
        SemanticAssessmentCurrentnessReason.SUBJECT_VERSION_CHANGED,
        SemanticAssessmentCurrentnessReason.SUBJECT_CONTENT_CHANGED,
    )

    mutation = revalidate_semantic_metric_waiver(
        approved.waiver,
        event_id="waiver-event-stale-and-expired",
        expected_waiver_revision=2,
        actor_id="revalidator-3",
        occurred_at=evaluated_at,
        evaluated_at=evaluated_at,
        status=SemanticMetricWaiverRevalidationStatus.ANCHOR_STALE,
        reason_code=(SemanticMetricWaiverRevalidationReason.SUBJECT_SCOPE_CHANGED),
        currentness_reasons=reasons,
        scheduled_expiry_observed=True,
        evidence_refs=(evidence,),
        idempotency_key="waiver:revalidate:stale-expired",
    )

    assert mutation.waiver.status is SemanticMetricWaiverStatus.APPROVED
    assert (
        mutation.waiver.last_revalidation_status
        is SemanticMetricWaiverRevalidationStatus.ANCHOR_STALE
    )
    assert mutation.waiver.last_revalidation_current is False
    assert mutation.waiver.last_revalidation_currentness_reasons == reasons
    assert mutation.waiver.last_revalidation_scheduled_expiry_observed is True
    assert (
        mutation.event.revalidation_status
        is SemanticMetricWaiverRevalidationStatus.ANCHOR_STALE
    )
    assert mutation.event.scheduled_expiry_observed is True


def test_revalidation_never_reactivates_expired_or_revoked_waivers() -> None:
    _, approved, _, evidence = _approve_waiver()
    expired = transition_semantic_metric_waiver(
        approved.waiver,
        event_id="waiver-event-expire-before-revalidation",
        expected_waiver_revision=2,
        event_type=SemanticMetricWaiverEventType.EXPIRE,
        actor_id="system",
        occurred_at=NOW + timedelta(minutes=2),
        reason="The immutable subject scope changed.",
        evidence_refs=(evidence,),
        idempotency_key="waiver:expire:before-revalidation",
        expire_reason=SemanticMetricWaiverExpireReason.SUBJECT_SCOPE_CHANGED,
    )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_reactivation_forbidden",
    ):
        revalidate_semantic_metric_waiver(
            expired.waiver,
            event_id="waiver-event-expired-reactivation",
            expected_waiver_revision=3,
            actor_id="revalidator-3",
            occurred_at=NOW + timedelta(minutes=3),
            evaluated_at=NOW + timedelta(minutes=3),
            status=SemanticMetricWaiverRevalidationStatus.APPROVED,
            reason_code=SemanticMetricWaiverRevalidationReason.CURRENT,
            currentness_reasons=(),
            scheduled_expiry_observed=False,
            evidence_refs=(evidence,),
            idempotency_key="waiver:revalidate:expired-reactivation",
        )

    _, approved_without_expiry, _, evidence = _approve_waiver()
    revoked = transition_semantic_metric_waiver(
        approved_without_expiry.waiver,
        event_id="waiver-event-revoked-before-revalidation",
        expected_waiver_revision=2,
        event_type=SemanticMetricWaiverEventType.REVOKE,
        actor_id="reviewer-4",
        occurred_at=NOW + timedelta(minutes=2),
        reason="The exception is no longer authorized.",
        evidence_refs=(evidence,),
        idempotency_key="waiver:revoke:before-revalidation",
    )
    revalidated = revalidate_semantic_metric_waiver(
        revoked.waiver,
        event_id="waiver-event-revoked-revalidation",
        expected_waiver_revision=3,
        actor_id="revalidator-3",
        occurred_at=NOW + timedelta(minutes=3),
        evaluated_at=NOW + timedelta(minutes=3),
        status=SemanticMetricWaiverRevalidationStatus.REVOKED,
        reason_code=SemanticMetricWaiverRevalidationReason.REVOKED,
        currentness_reasons=(),
        scheduled_expiry_observed=False,
        evidence_refs=(evidence,),
        idempotency_key="waiver:revalidate:revoked",
    )
    assert revalidated.waiver.status is SemanticMetricWaiverStatus.REVOKED
    assert revalidated.event.from_status is SemanticMetricWaiverStatus.REVOKED
    assert revalidated.event.to_status is SemanticMetricWaiverStatus.REVOKED


def test_revalidation_rejects_fence_bypass_and_digest_tampering() -> None:
    _, approved, _, evidence = _approve_waiver()
    evaluated_at = NOW + timedelta(minutes=2)
    common = {
        "event_id": "waiver-event-adversarial",
        "expected_waiver_revision": 2,
        "occurred_at": evaluated_at,
        "evaluated_at": evaluated_at,
        "status": SemanticMetricWaiverRevalidationStatus.APPROVED,
        "reason_code": SemanticMetricWaiverRevalidationReason.CURRENT,
        "currentness_reasons": (),
        "scheduled_expiry_observed": False,
        "evidence_refs": (evidence,),
        "idempotency_key": "waiver:revalidate:adversarial",
    }

    for actor_id in (
        approved.waiver.requested_by,
        approved.waiver.anchor.assessment_assessor_id,
    ):
        with pytest.raises(
            SemanticAssessmentContractError,
            match="semantic_waiver_independent_review_required",
        ):
            revalidate_semantic_metric_waiver(
                approved.waiver,
                actor_id=actor_id,
                **common,
            )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_revision_conflict",
    ):
        revalidate_semantic_metric_waiver(
            approved.waiver,
            actor_id="revalidator-3",
            **{**common, "expected_waiver_revision": 1},
        )

    mutation = revalidate_semantic_metric_waiver(
        approved.waiver,
        actor_id="revalidator-3",
        **common,
    )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_head_digest_mismatch",
    ):
        replace(mutation.waiver, head_digest="0" * 64)
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_event_request_digest_mismatch",
    ):
        replace(mutation.event, request_digest="0" * 64)
    changed_head = replace(
        mutation.waiver,
        last_event_idempotency_key="waiver:revalidate:tampered",
        head_digest=None,
    )
    assert changed_head.head_digest != mutation.waiver.head_digest
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_mutation_inconsistent",
    ):
        SemanticMetricWaiverMutation(
            waiver=changed_head,
            event=mutation.event,
        )


def test_waiver_cas_and_event_specific_inputs_are_closed() -> None:
    requested, finding, evidence = _request_waiver()

    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_revision_conflict",
    ):
        transition_semantic_metric_waiver(
            requested.waiver,
            event_id="waiver-event-conflict",
            expected_waiver_revision=2,
            event_type=SemanticMetricWaiverEventType.REJECT,
            actor_id="reviewer-2",
            occurred_at=NOW + timedelta(minutes=1),
            reason="Conflict must fail before persistence.",
            evidence_refs=(evidence,),
            idempotency_key="waiver:reject:conflict",
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_event_expiry_not_allowed",
    ):
        transition_semantic_metric_waiver(
            requested.waiver,
            event_id="waiver-event-extra-expiry",
            expected_waiver_revision=1,
            event_type=SemanticMetricWaiverEventType.REJECT,
            actor_id="reviewer-2",
            occurred_at=NOW + timedelta(minutes=1),
            reason="Reject without an expiry mutation.",
            evidence_refs=(evidence,),
            idempotency_key="waiver:reject:extra-expiry",
            expires_at=NOW + timedelta(days=1),
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_event_type_invalid",
    ):
        transition_semantic_metric_waiver(
            requested.waiver,
            event_id="waiver-event-invalid-type",
            expected_waiver_revision=1,
            event_type="approve",  # type: ignore[arg-type]
            actor_id="reviewer-2",
            occurred_at=NOW + timedelta(minutes=1),
            reason="A string event type is outside the closed interface.",
            evidence_refs=(evidence,),
            idempotency_key="waiver:invalid-type",
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_expire_reason_invalid",
    ):
        transition_semantic_metric_waiver(
            requested.waiver,
            event_id="waiver-event-invalid-expire-reason",
            expected_waiver_revision=1,
            event_type=SemanticMetricWaiverEventType.APPROVE,
            actor_id="reviewer-2",
            occurred_at=NOW + timedelta(minutes=1),
            reason="An unknown expiry reason is outside the closed interface.",
            evidence_refs=(evidence,),
            idempotency_key="waiver:invalid-expire-reason",
            expire_reason="later",  # type: ignore[arg-type]
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_current_finding_not_allowed",
    ):
        transition_semantic_metric_waiver(
            requested.waiver,
            event_id="waiver-event-extra-finding",
            expected_waiver_revision=1,
            event_type=SemanticMetricWaiverEventType.APPROVE,
            actor_id="reviewer-2",
            occurred_at=NOW + timedelta(minutes=1),
            reason="Only revalidation accepts a current finding.",
            evidence_refs=(evidence,),
            idempotency_key="waiver:extra-finding",
            current_finding=finding,
        )


def test_idempotency_digests_exclude_server_generated_ids_and_timestamps() -> None:
    _, _, finding, evidence = _finding()
    anchor = _waiver_anchor(finding)
    first_request = request_semantic_metric_waiver(
        waiver_id="waiver-generated-1",
        event_id="waiver-event-generated-1",
        anchor=anchor,
        justification="A bounded migration requires a temporary exception.",
        evidence_refs=(evidence,),
        requested_by="requester-1",
        requested_at=NOW,
        expires_at=NOW + timedelta(days=2),
        idempotency_key="waiver:logical-request",
    )
    replayed_request = request_semantic_metric_waiver(
        waiver_id="waiver-generated-2",
        event_id="waiver-event-generated-2",
        anchor=anchor,
        justification="A bounded migration requires a temporary exception.",
        evidence_refs=(evidence,),
        requested_by="requester-1",
        requested_at=NOW + timedelta(minutes=5),
        expires_at=NOW + timedelta(days=2),
        idempotency_key="waiver:logical-request",
    )
    assert first_request.event.request_digest == replayed_request.event.request_digest

    first_approval = transition_semantic_metric_waiver(
        first_request.waiver,
        event_id="waiver-approval-generated-1",
        expected_waiver_revision=1,
        event_type=SemanticMetricWaiverEventType.APPROVE,
        actor_id="reviewer-2",
        occurred_at=NOW + timedelta(minutes=10),
        reason="The exception is independently reviewed.",
        evidence_refs=(evidence,),
        idempotency_key="waiver:logical-approval",
    )
    replayed_approval = transition_semantic_metric_waiver(
        first_request.waiver,
        event_id="waiver-approval-generated-2",
        expected_waiver_revision=1,
        event_type=SemanticMetricWaiverEventType.APPROVE,
        actor_id="reviewer-2",
        occurred_at=NOW + timedelta(minutes=15),
        reason="The exception is independently reviewed.",
        evidence_refs=(evidence,),
        idempotency_key="waiver:logical-approval",
    )
    assert first_approval.event.request_digest == replayed_approval.event.request_digest

    scope = _skip_scope()
    first_skip = create_semantic_policy_skip(
        skip_id="skip-generated-1",
        event_id="skip-event-generated-1",
        scope=scope,
        reason="A human explicitly accepted this exact binding risk.",
        actor_id="owner-1",
        actor_kind=SemanticExceptionActorKind.HUMAN,
        occurred_at=NOW,
        idempotency_key="skip:logical-create",
    )
    replayed_skip = create_semantic_policy_skip(
        skip_id="skip-generated-2",
        event_id="skip-event-generated-2",
        scope=scope,
        reason="A human explicitly accepted this exact binding risk.",
        actor_id="owner-1",
        actor_kind=SemanticExceptionActorKind.HUMAN,
        occurred_at=NOW + timedelta(minutes=5),
        idempotency_key="skip:logical-create",
    )
    assert first_skip.event.request_digest == replayed_skip.event.request_digest

    first_revoke = revoke_semantic_policy_skip(
        first_skip.skip,
        event_id="skip-revoke-generated-1",
        expected_skip_revision=1,
        actor_id="owner-2",
        actor_kind=SemanticExceptionActorKind.HUMAN,
        occurred_at=NOW + timedelta(minutes=10),
        reason="The explicit bypass is no longer required.",
        idempotency_key="skip:logical-revoke",
    )
    replayed_revoke = revoke_semantic_policy_skip(
        first_skip.skip,
        event_id="skip-revoke-generated-2",
        expected_skip_revision=1,
        actor_id="owner-2",
        actor_kind=SemanticExceptionActorKind.HUMAN,
        occurred_at=NOW + timedelta(minutes=15),
        reason="The explicit bypass is no longer required.",
        idempotency_key="skip:logical-revoke",
    )
    assert first_revoke.event.request_digest == replayed_revoke.event.request_digest


def test_waiver_materialized_head_rejects_impossible_temporal_states() -> None:
    requested, approved, _, _ = _approve_waiver(
        expires_at=NOW + timedelta(days=1),
    )

    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_original_expiry_not_future",
    ):
        replace(
            requested.waiver,
            original_expires_at=NOW,
            head_digest=None,
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_last_event_time_regressed",
    ):
        replace(
            requested.waiver,
            last_event_at=NOW - timedelta(seconds=1),
            head_digest=None,
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_last_event_state_mismatch",
    ):
        replace(
            requested.waiver,
            last_event_type=SemanticMetricWaiverEventType.APPROVE,
            head_digest=None,
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_approved_expiry_not_future",
    ):
        replace(
            approved.waiver,
            expires_at=approved.waiver.last_event_at,
            head_digest=None,
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_event_revision_transition_invalid",
    ):
        replace(
            approved.event,
            waiver_revision=3,
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_waiver_event_request_digest_mismatch",
    ):
        replace(
            requested.event,
            request_digest="f" * 64,
        )


def test_skip_is_human_only_exact_and_append_only_with_cas() -> None:
    scope = _skip_scope()

    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_skip_human_actor_required",
    ):
        create_semantic_policy_skip(
            skip_id="skip-agent",
            event_id="skip-event-agent",
            scope=scope,
            reason="Agents cannot bypass semantic policy.",
            actor_id="agent-1",
            actor_kind=SemanticExceptionActorKind.AGENT,
            occurred_at=NOW,
            idempotency_key="skip:agent",
        )
    created = create_semantic_policy_skip(
        skip_id=" skip-1 ",
        event_id="skip-event-1",
        scope=scope,
        reason=" A human explicitly accepted this exact binding risk. ",
        actor_id=" owner-1 ",
        actor_kind=SemanticExceptionActorKind.HUMAN,
        occurred_at=NOW,
        idempotency_key=" skip:create:1 ",
    )
    assert created.skip.status is SemanticPolicySkipStatus.ACTIVE
    assert created.skip.skip_id == "skip-1"
    assert created.skip.reason == "A human explicitly accepted this exact binding risk."
    assert created.skip.idempotency_key == "skip:create:1"
    assert created.event.event_type is SemanticPolicySkipEventType.CREATE

    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_skip_revision_conflict",
    ):
        revoke_semantic_policy_skip(
            created.skip,
            event_id="skip-event-conflict",
            expected_skip_revision=2,
            actor_id="owner-1",
            actor_kind=SemanticExceptionActorKind.HUMAN,
            occurred_at=NOW + timedelta(minutes=1),
            reason="CAS conflict.",
            idempotency_key="skip:revoke:conflict",
        )
    revoked = revoke_semantic_policy_skip(
        created.skip,
        event_id="skip-event-2",
        expected_skip_revision=1,
        actor_id="owner-2",
        actor_kind=SemanticExceptionActorKind.HUMAN,
        occurred_at=NOW + timedelta(minutes=1),
        reason="The explicit bypass is no longer required.",
        idempotency_key="skip:revoke:1",
    )
    assert revoked.skip.status is SemanticPolicySkipStatus.REVOKED
    assert revoked.skip.skip_revision == 2
    assert revoked.event.predecessor_event_id == created.event.event_id
    assert revoked.skip.scope_digest == created.skip.scope_digest

    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_skip_active_state_invalid",
    ):
        replace(
            created.skip,
            skip_revision=2,
            skip_digest=None,
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_skip_last_event_time_regressed",
    ):
        replace(
            created.skip,
            last_event_at=NOW - timedelta(seconds=1),
            skip_digest=None,
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_skip_revoked_state_invalid",
    ):
        replace(
            revoked.skip,
            skip_revision=3,
            skip_digest=None,
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_skip_event_transition_invalid",
    ):
        replace(
            revoked.event,
            skip_revision=3,
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_skip_request_digest_mismatch",
    ):
        replace(
            created.skip,
            request_digest="f" * 64,
            skip_digest=None,
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_skip_event_request_digest_mismatch",
    ):
        replace(
            created.event,
            request_digest="f" * 64,
        )
