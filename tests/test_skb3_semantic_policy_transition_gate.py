"""SK-B3 I2 tests for native gates over semantic assessment evidence."""

from __future__ import annotations

from dataclasses import replace
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
    SemanticAssessmentInadmissibilityCause,
    SemanticGuidelineAssessmentContext,
    SemanticGuidelineAssessmentSubmission,
    SemanticMetricAssessment,
    record_semantic_guideline_assessment,
)
from okto_pulse.core.domain.guideline_semantic_currentness import (
    SemanticAssessmentCurrentnessReason,
    semantic_assessment_current_snapshot_from_context,
)
from okto_pulse.core.domain.guideline_semantic_exceptions import (
    SemanticExceptionActorKind,
    SemanticMetricWaiverAnchor,
    SemanticMetricWaiverEventType,
    SemanticPolicySkipScope,
    create_semantic_policy_skip,
    request_semantic_metric_waiver,
    transition_semantic_metric_waiver,
)
from okto_pulse.core.domain.guideline_semantic_findings import (
    project_semantic_metric_findings,
)
from okto_pulse.core.domain.guideline_semantic_transition import (
    POLICY_TRANSITION_CONTRACT_VERSION,
    PolicyTransitionDiagnosticCode,
    PolicyTransitionReasonCode,
    PolicyTransitionRejected,
    PolicyTransitionSnapshot,
    SemanticBindingComplianceSnapshot,
    evaluate_policy_transition,
    raise_for_policy_transition,
    require_policy_transition_decision_match,
)
from okto_pulse.core.domain.quality_assessment import (
    EvidenceRef,
    FindingAnchorType,
    UnboundFindingAnchor,
)
from okto_pulse.core.domain.sdlc_registry import SDLC_REGISTRY


NOW = datetime(2026, 7, 30, 22, tzinfo=timezone.utc)
DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64
DIGEST_D = "d" * 64


def _assessment(
    *,
    score: int,
    enforcement: GuidelineEnforcement = GuidelineEnforcement.BLOCKING,
):
    metric = GuidelineMetric(
        metric_id="metric-segregation",
        code="architecture.segregation",
        title="Segregation",
        description="Measure domain and infrastructure segregation.",
        evaluation_rubric="Inspect dependency direction and score 0 to 100.",
        target_entity_types=(PolicyEntityType.SPEC,),
        direction=GuidelineMetricDirection.MINIMUM,
        default_threshold=80,
    )
    revision = GuidelineRevision(
        revision_id="revision-1",
        guideline_id="guideline-1",
        revision_number=1,
        semantic_version="1.0.0",
        title="Hexagonal architecture",
        content="Keep business rules independent from technical capabilities.",
        metrics=(metric,),
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
        enforcement=enforcement,
        minimum_confidence=80,
        metric_threshold_overrides={},
    )
    subject = PolicySubjectSnapshot(
        subject=PolicySubjectRef(
            board_id="board-1",
            entity_type=PolicyEntityType.SPEC,
            subject_id="spec-1",
            subject_version=7,
        ),
        content_digest=DIGEST_A,
        last_semantic_editor_id="editor-1",
        captured_at=NOW,
    )
    context = SemanticGuidelineAssessmentContext(
        subject_snapshot=subject,
        binding=binding,
        revision=revision,
        policy_set_digest=DIGEST_B,
        binding_head_digest=DIGEST_C,
    )
    evidence = EvidenceRef(
        source_type="spec",
        source_id="spec-1",
        source_version=7,
        content_hash=DIGEST_A,
    )
    receipt = record_semantic_guideline_assessment(
        SemanticGuidelineAssessmentSubmission(
            subject=subject.subject,
            binding_id=binding.binding_id,
            expected_binding_revision=binding.binding_revision,
            guideline_revision_id=revision.revision_id,
            idempotency_key=f"assessment:spec-1:{score}",
            confidence=95,
            assessor=SemanticAssessmentAssessor(
                agent_id="reviewer-1",
                model_id="model-a",
            ),
            metric_results=(
                SemanticMetricAssessment(
                    metric_id=metric.metric_id,
                    score=score,
                    rationale=f"Evidence supports a score of {score}.",
                    evidence_refs=(evidence,),
                    pinpoints=(
                        UnboundFindingAnchor(
                            anchor_type=(
                                FindingAnchorType.STRUCTURED_CHILD
                            ),
                            anchor_ref="technical_requirements.tr_hexagonal",
                            excerpt_hash=DIGEST_D,
                        ),
                    ),
                ),
            ),
        ),
        context,
        receipt_id=f"receipt-{score}",
        recorded_at=NOW,
    ).receipt
    return context, receipt, evidence


def _binding_snapshot(
    *,
    score: int = 90,
    enforcement: GuidelineEnforcement = GuidelineEnforcement.BLOCKING,
    include_receipt: bool = True,
    include_findings: bool = True,
    **overrides: object,
) -> SemanticBindingComplianceSnapshot:
    context, receipt, _ = _assessment(
        score=score,
        enforcement=enforcement,
    )
    values: dict[str, object] = {
        "binding_id": context.binding.binding_id,
        "guideline_id": context.revision.guideline_id,
        "enforcement": enforcement,
        "applicable_metric_count": 1,
        "current_snapshot": (
            semantic_assessment_current_snapshot_from_context(context)
        ),
        "receipt": receipt if include_receipt else None,
        "findings": (
            project_semantic_metric_findings(receipt)
            if include_receipt and include_findings
            else ()
        ),
    }
    values.update(overrides)
    return SemanticBindingComplianceSnapshot(**values)


def _transition_snapshot(
    bindings: tuple[SemanticBindingComplianceSnapshot, ...],
    **overrides: object,
) -> PolicyTransitionSnapshot:
    values: dict[str, object] = {
        "board_id": "board-1",
        "entity_type": PolicyEntityType.SPEC,
        "subject_id": "spec-1",
        "expected_from_status": "approved",
        "bindings": bindings,
        "evaluated_at": NOW + timedelta(minutes=5),
    }
    values.update(overrides)
    return PolicyTransitionSnapshot(**values)


def _approved_waiver(binding: SemanticBindingComplianceSnapshot):
    assert binding.findings
    context, receipt, evidence = _assessment(score=60)
    assert context.binding.binding_id == binding.binding_id
    requested = request_semantic_metric_waiver(
        waiver_id="waiver-1",
        event_id="waiver-event-1",
        anchor=SemanticMetricWaiverAnchor.from_finding(
            binding.findings[0],
            assessment_assessor_id=receipt.assessor.agent_id,
        ),
        justification="A bounded migration needs a temporary exception.",
        evidence_refs=(evidence,),
        requested_by="requester-1",
        requested_at=NOW,
        expires_at=NOW + timedelta(days=1),
        idempotency_key="waiver:request:1",
    )
    return transition_semantic_metric_waiver(
        requested.waiver,
        event_id="waiver-event-2",
        expected_waiver_revision=1,
        event_type=SemanticMetricWaiverEventType.APPROVE,
        actor_id="reviewer-2",
        occurred_at=NOW + timedelta(minutes=1),
        reason="The exception is independently reviewed and time bounded.",
        evidence_refs=(evidence,),
        idempotency_key="waiver:approve:1",
    ).waiver


def _human_skip(binding: SemanticBindingComplianceSnapshot):
    current = binding.current_snapshot
    scope = SemanticPolicySkipScope(
        subject=current.subject,
        subject_content_digest=current.subject_content_digest,
        guideline_id=current.guideline_id,
        guideline_revision_id=current.guideline_revision_id,
        guideline_revision_digest=current.guideline_revision_digest,
        binding_id=current.binding_id,
        binding_revision=current.binding_revision,
        binding_configuration_digest=current.binding_configuration_digest,
    )
    return create_semantic_policy_skip(
        skip_id="skip-1",
        event_id="skip-event-1",
        scope=scope,
        reason="A human accepted this exact subject and binding risk.",
        actor_id="owner-1",
        actor_kind=SemanticExceptionActorKind.HUMAN,
        occurred_at=NOW,
        idempotency_key="skip:create:1",
    ).skip


def test_registry_keeps_exactly_the_frozen_fourteen_native_gate_edges() -> None:
    edges = {
        (entity_type, from_status, edge.to_status)
        for entity_type, lifecycle in SDLC_REGISTRY.items()
        for from_status, contracts in lifecycle.transitions.items()
        for edge in contracts
        if edge.policy_compliance
    }

    assert len(edges) == 14
    assert edges == {
        ("ideation", "evaluating", "done"),
        ("refinement", "approved", "done"),
        ("spec", "approved", "validated"),
        ("card", "in_progress", "done"),
        ("card", "validation", "done"),
        ("sprint", "review", "closed"),
        ("test_scenario", "draft", "automated"),
        ("test_scenario", "draft", "passed"),
        ("test_scenario", "draft", "failed"),
        ("test_scenario", "ready", "automated"),
        ("test_scenario", "ready", "passed"),
        ("test_scenario", "ready", "failed"),
        ("test_scenario", "automated", "passed"),
        ("test_scenario", "failed", "passed"),
    }
    assert POLICY_TRANSITION_CONTRACT_VERSION == "semantic-policy-transition/v2"


def test_illegal_and_non_gate_edges_return_before_semantic_evidence() -> None:
    snapshot = _transition_snapshot(())

    illegal = evaluate_policy_transition(snapshot, "done")
    backwards = evaluate_policy_transition(snapshot, "review")

    assert not illegal.allowed
    assert illegal.reason_code is PolicyTransitionReasonCode.TRANSITION_NOT_ALLOWED
    assert backwards.allowed
    assert (
        backwards.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_NOT_REQUIRED
    )


def test_context_only_binding_is_nonblocking_and_not_applicable() -> None:
    binding = _binding_snapshot(
        include_receipt=False,
        applicable_metric_count=0,
        findings=(),
    )

    decision = evaluate_policy_transition(
        _transition_snapshot((binding,)),
        "validated",
    )

    assert decision.allowed
    assert (
        decision.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_NOT_APPLICABLE
    )
    assert decision.applicable_metric_count == 0
    assert decision.diagnostic_codes == ()


@pytest.mark.parametrize(
    ("binding", "reason", "diagnostic"),
    (
        (
            _binding_snapshot(
                include_receipt=False,
                findings=(),
            ),
            PolicyTransitionReasonCode.POLICY_COMPLIANCE_RECEIPT_MISSING,
            (
                PolicyTransitionDiagnosticCode
                .POLICY_COMPLIANCE_RECEIPT_MISSING
            ),
        ),
        (
            _binding_snapshot(
                assessment_available=False,
                assessment_error_code="assessment_service_unavailable",
                include_receipt=False,
                findings=(),
            ),
            PolicyTransitionReasonCode.POLICY_ASSESSMENT_UNAVAILABLE,
            (
                PolicyTransitionDiagnosticCode
                .POLICY_ASSESSMENT_UNAVAILABLE
            ),
        ),
        (
            _binding_snapshot(
                include_receipt=False,
                findings=(),
                inadmissibility_cause=(
                    SemanticAssessmentInadmissibilityCause
                    .CONFIDENCE_BELOW_MINIMUM
                ),
            ),
            PolicyTransitionReasonCode.POLICY_COMPLIANCE_BLOCKED,
            (
                PolicyTransitionDiagnosticCode
                .POLICY_ASSESSMENT_INADMISSIBLE
            ),
        ),
    ),
)
def test_blocking_missing_unavailable_and_inadmissible_are_distinct(
    binding: SemanticBindingComplianceSnapshot,
    reason: PolicyTransitionReasonCode,
    diagnostic: PolicyTransitionDiagnosticCode,
) -> None:
    decision = evaluate_policy_transition(
        _transition_snapshot((binding,)),
        "validated",
    )

    assert not decision.allowed
    assert decision.reason_code is reason
    assert diagnostic in decision.diagnostic_codes


def test_stale_blocking_receipt_is_fail_closed_with_specific_reasons() -> None:
    binding = _binding_snapshot()
    stale_current = replace(
        binding.current_snapshot,
        subject=replace(
            binding.current_snapshot.subject,
            subject_version=8,
        ),
    )
    stale = replace(binding, current_snapshot=stale_current)

    decision = evaluate_policy_transition(
        _transition_snapshot((stale,)),
        "validated",
    )

    assert not decision.allowed
    assert (
        decision.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_RECEIPT_STALE
    )
    assert decision.currentness_reasons


@pytest.mark.parametrize(
    ("receipt_enforcement", "current_enforcement", "expected_allowed"),
    (
        (
            GuidelineEnforcement.BLOCKING,
            GuidelineEnforcement.ADVISORY,
            False,
        ),
        (
            GuidelineEnforcement.ADVISORY,
            GuidelineEnforcement.BLOCKING,
            False,
        ),
    ),
)
def test_enforcement_change_is_stale_not_a_scope_error(
    receipt_enforcement: GuidelineEnforcement,
    current_enforcement: GuidelineEnforcement,
    expected_allowed: bool,
) -> None:
    binding = _binding_snapshot(enforcement=receipt_enforcement)
    changed_current = replace(
        binding.current_snapshot,
        binding_revision=binding.current_snapshot.binding_revision + 1,
        binding_configuration_digest="f" * 64,
        input_digest="e" * 64,
    )
    changed = replace(
        binding,
        enforcement=current_enforcement,
        current_snapshot=changed_current,
    )

    decision = evaluate_policy_transition(
        _transition_snapshot((changed,)),
        "validated",
    )

    assert decision.allowed is expected_allowed
    assert (
        decision.binding_decisions[0].currentness
        is PolicyCurrentness.STALE
    )
    assert (
        SemanticAssessmentCurrentnessReason
        .BINDING_CONFIGURATION_CHANGED
        in decision.currentness_reasons
    )
    assert (
        decision.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_RECEIPT_STALE
    )


def test_binding_snapshot_rejects_incomplete_findings_and_unavailable_evidence() -> None:
    with pytest.raises(
        GuidelinePolicyContractError,
        match="semantic_transition_findings_projection_mismatch",
    ):
        _binding_snapshot(
            score=60,
            include_findings=False,
        )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="semantic_transition_unavailable_evidence_invalid",
    ):
        _binding_snapshot(
            assessment_available=False,
            assessment_error_code="assessment_service_unavailable",
        )


def test_threshold_failure_blocks_but_exact_current_waiver_unblocks() -> None:
    failed = _binding_snapshot(score=60)
    blocked = evaluate_policy_transition(
        _transition_snapshot((failed,)),
        "validated",
    )
    waived = replace(failed, waivers=(_approved_waiver(failed),))
    ready = evaluate_policy_transition(
        _transition_snapshot((waived,)),
        "validated",
    )

    assert not blocked.allowed
    assert (
        blocked.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_BLOCKED
    )
    assert (
        PolicyTransitionDiagnosticCode.POLICY_METRIC_THRESHOLD_FAILED
        in blocked.diagnostic_codes
    )
    assert blocked.failed_metric_count == 1
    assert blocked.blocking_metric_count == 1
    assert ready.allowed
    assert (
        ready.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_READY_WITH_WAIVERS
    )
    assert ready.waived_metric_count == 1
    assert ready.blocking_metric_count == 0


def test_human_skip_unblocks_only_its_exact_current_binding() -> None:
    failed = _binding_snapshot(score=60)
    skipped = replace(failed, skip=_human_skip(failed))

    ready = evaluate_policy_transition(
        _transition_snapshot((skipped,)),
        "validated",
    )
    changed_binding = replace(
        skipped,
        current_snapshot=replace(
            skipped.current_snapshot,
            binding_revision=skipped.current_snapshot.binding_revision + 1,
        ),
    )
    blocked = evaluate_policy_transition(
        _transition_snapshot((changed_binding,)),
        "validated",
    )

    assert ready.allowed
    assert ready.skipped_binding_count == 1
    assert (
        ready.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_READY_WITH_WAIVERS
    )
    assert not blocked.allowed
    assert (
        blocked.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_RECEIPT_STALE
    )


def test_advisory_failed_score_never_blocks_but_missing_receipt_does() -> None:
    advisory_missing = _binding_snapshot(
        enforcement=GuidelineEnforcement.ADVISORY,
        include_receipt=False,
        findings=(),
    )
    advisory_failed = _binding_snapshot(
        score=60,
        enforcement=GuidelineEnforcement.ADVISORY,
    )

    missing_decision = evaluate_policy_transition(
        _transition_snapshot((advisory_missing,)),
        "validated",
    )
    failed_decision = evaluate_policy_transition(
        _transition_snapshot((advisory_failed,)),
        "validated",
    )

    # Advisory scores stay advisory: a recorded, current receipt with a
    # failed metric never blocks the transition.
    assert failed_decision.allowed
    assert (
        failed_decision.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_ADVISORY_ONLY
    )
    assert failed_decision.applicable_blocking_metric_count == 0

    # Evidence PRESENCE is mandatory regardless of enforcement: an applicable
    # advisory binding without a recorded assessment rejects the transition.
    assert not missing_decision.allowed
    assert (
        missing_decision.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_RECEIPT_MISSING
    )
    assert not missing_decision.binding_decisions[0].allowed


def test_multi_binding_decision_is_conjunctive_and_metric_named() -> None:
    blocking_pass = _binding_snapshot()
    advisory_missing = _binding_snapshot(
        enforcement=GuidelineEnforcement.ADVISORY,
        include_receipt=False,
        findings=(),
    )
    second_current = replace(
        advisory_missing.current_snapshot,
        binding_id="binding-2",
        guideline_id="guideline-2",
    )
    advisory_missing = replace(
        advisory_missing,
        binding_id="binding-2",
        guideline_id="guideline-2",
        current_snapshot=second_current,
    )

    decision = evaluate_policy_transition(
        _transition_snapshot((advisory_missing, blocking_pass)),
        "validated",
    )

    assert not decision.allowed
    assert (
        decision.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_RECEIPT_MISSING
    )
    assert decision.applicable_metric_count == 2
    assert decision.applicable_blocking_metric_count == 1
    assert tuple(
        item.binding_id for item in decision.binding_decisions
    ) == ("binding-1", "binding-2")
    assert not hasattr(decision, "__dict__")


def test_binding_and_aggregate_decisions_reject_forged_outcomes() -> None:
    blocked = evaluate_policy_transition(
        _transition_snapshot((_binding_snapshot(score=60),)),
        "validated",
    )

    with pytest.raises(
        GuidelinePolicyContractError,
        match="semantic_transition_current_receipt_decision_invalid",
    ):
        replace(
            blocked.binding_decisions[0],
            allowed=True,
        )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_transition_decision_aggregate_counts_mismatch",
    ):
        replace(
            blocked,
            skipped_binding_count=1,
        )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_transition_decision_outcome_mismatch",
    ):
        replace(
            blocked,
            allowed=True,
            reason_codes=(
                PolicyTransitionReasonCode.POLICY_COMPLIANCE_READY,
            ),
        )


def test_subject_required_and_preview_mutation_digest_match_are_fail_closed() -> None:
    missing_subject = _transition_snapshot(
        (),
        subject_available=False,
    )
    decision = evaluate_policy_transition(missing_subject, "validated")

    assert not decision.allowed
    assert (
        decision.reason_code
        is PolicyTransitionReasonCode.POLICY_SUBJECT_REQUIRED
    )
    with pytest.raises(PolicyTransitionRejected) as error:
        raise_for_policy_transition(decision)
    assert error.value.code == "policy_subject_required"

    passing = evaluate_policy_transition(
        _transition_snapshot((_binding_snapshot(),)),
        "validated",
    )
    require_policy_transition_decision_match(
        passing.decision_digest,
        passing,
    )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_transition_decision_changed",
    ):
        require_policy_transition_decision_match(DIGEST_A, passing)
