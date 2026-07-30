"""SK-B B10 canonical policy-compliance transition gate."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from okto_pulse.core.domain.guideline_compliance import (
    PolicyComplianceCurrentSnapshot,
    PolicyCurrentnessReason,
)
from okto_pulse.core.domain.guideline_policy import (
    AdoptedGuidelineRevisionRef,
    GuidelineEnforcement,
    GuidelinePolicyContractError,
    PolicyComplianceFinding,
    PolicyComplianceReasonCode,
    PolicyComplianceReceipt,
    PolicyComplianceRuleResult,
    PolicyComplianceState,
    PolicyCurrentness,
    PolicyEntityType,
    PolicyEvaluationOutcome,
    PolicySubjectRef,
)
from okto_pulse.core.domain.guideline_policy_transition import (
    PolicyTransitionReasonCode,
    PolicyTransitionRejected,
    PolicyTransitionSnapshot,
    evaluate_policy_transition,
    raise_for_policy_transition,
    require_policy_transition_decision_match,
)
from okto_pulse.core.domain.sdlc_registry import (
    SDLC_REGISTRY,
    is_transition_allowed,
    transition_contracts,
    transition_requires_policy_compliance,
)
from okto_pulse.core.services.test_scenario_lifecycle import (
    InvalidScenarioStatusTransitionError,
    is_test_scenario_status_transition_allowed,
    require_test_scenario_status_transition,
)
from okto_pulse.core.services.main import GuidelineService


NOW = datetime(2026, 7, 29, 18, tzinfo=timezone.utc)
FROZEN_POLICY_EDGES = frozenset(
    {
        ("ideation", "evaluating", "done"),
        ("refinement", "approved", "done"),
        ("spec", "approved", "validated"),
        ("sprint", "review", "closed"),
        ("card", "in_progress", "done"),
        ("card", "validation", "done"),
        ("test_scenario", "draft", "automated"),
        ("test_scenario", "draft", "passed"),
        ("test_scenario", "draft", "failed"),
        ("test_scenario", "ready", "automated"),
        ("test_scenario", "ready", "passed"),
        ("test_scenario", "ready", "failed"),
        ("test_scenario", "automated", "passed"),
        ("test_scenario", "failed", "passed"),
    }
)
TEST_SCENARIO_EDGES = {
    "draft": ("ready", "automated", "passed", "failed"),
    "ready": ("draft", "automated", "passed", "failed"),
    "automated": ("ready", "passed"),
    "failed": ("ready", "passed"),
    "passed": ("ready",),
}


def _subject() -> PolicySubjectRef:
    return PolicySubjectRef(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        subject_id="spec-1",
        subject_version=4,
    )


def _receipt(
    *results: PolicyComplianceRuleResult,
) -> PolicyComplianceReceipt:
    findings = tuple(
        PolicyComplianceFinding(
            finding_id=f"finding-{index}",
            receipt_id="receipt-1",
            subject=_subject(),
            guideline_id=result.guideline_id,
            revision_id=result.revision_id,
            rule_id=result.rule_id,
            outcome=result.outcome,
            enforcement=result.enforcement,
            message=f"{result.rule_id} did not pass",
            created_at=NOW,
            waiver_id=result.waiver_id,
        )
        for index, result in enumerate(results, start=1)
        if result.outcome
        in {
            PolicyEvaluationOutcome.FAIL,
            PolicyEvaluationOutcome.ERROR,
        }
    )
    blocking = any(result.blocking for result in results)
    blocking_errors = any(
        result.blocking and result.outcome is PolicyEvaluationOutcome.ERROR
        for result in results
    )
    errors = any(result.outcome is PolicyEvaluationOutcome.ERROR for result in results)
    failed = any(result.outcome is PolicyEvaluationOutcome.FAIL for result in results)
    waived = any(result.waiver_id is not None for result in results)
    advisory_errors = errors and not blocking_errors
    if not results:
        state = PolicyComplianceState.NOT_APPLICABLE
        outcome = PolicyEvaluationOutcome.NOT_APPLICABLE
    elif blocking:
        state = PolicyComplianceState.BLOCKED
        outcome = (
            PolicyEvaluationOutcome.ERROR
            if blocking_errors
            else PolicyEvaluationOutcome.FAIL
        )
    elif waived:
        state = PolicyComplianceState.READY_WITH_WAIVERS
        outcome = (
            PolicyEvaluationOutcome.ERROR
            if advisory_errors
            else PolicyEvaluationOutcome.FAIL
        )
    else:
        state = PolicyComplianceState.READY
        outcome = (
            PolicyEvaluationOutcome.ERROR
            if advisory_errors
            else (
                PolicyEvaluationOutcome.FAIL if failed else PolicyEvaluationOutcome.PASS
            )
        )
    reasons: list[PolicyComplianceReasonCode] = []
    if not results:
        reasons.append(PolicyComplianceReasonCode.NO_APPLICABLE_RULES)
    if blocking_errors:
        reasons.append(PolicyComplianceReasonCode.POLICY_EVALUATION_UNAVAILABLE)
    if advisory_errors:
        reasons.append(PolicyComplianceReasonCode.POLICY_EVALUATION_DEGRADED)
    return PolicyComplianceReceipt(
        receipt_id="receipt-1",
        subject=_subject(),
        subject_content_digest="a" * 64,
        input_digest="b" * 64,
        policy_set_digest="c" * 64,
        binding_head_digest="d" * 64,
        catalog_version="guideline-predicate-catalog/v1",
        ruleset_version="guideline-ruleset/v1",
        adopted_revisions=(
            AdoptedGuidelineRevisionRef(
                binding_id="binding-1",
                binding_revision=2,
                guideline_id="guideline-1",
                revision_id="revision-1",
                semantic_version="1.1.0",
                revision_digest="e" * 64,
            ),
        ),
        outcome=outcome,
        state=state,
        currentness=PolicyCurrentness.CURRENT,
        findings=findings,
        evaluator_version="policy-evaluator/v1",
        evaluated_by="agent-1",
        evaluated_at=NOW,
        rule_results=results,
        reason_codes=tuple(reasons),
    )


def _rule(
    *,
    outcome: PolicyEvaluationOutcome = PolicyEvaluationOutcome.PASS,
    enforcement: GuidelineEnforcement = GuidelineEnforcement.BLOCKING,
    waiver_id: str | None = None,
    rule_id: str = "rule-1",
) -> PolicyComplianceRuleResult:
    return PolicyComplianceRuleResult(
        guideline_id="guideline-1",
        revision_id="revision-1",
        rule_id=rule_id,
        outcome=outcome,
        enforcement=enforcement,
        waiver_id=waiver_id,
    )


def _current(
    receipt: PolicyComplianceReceipt,
) -> PolicyComplianceCurrentSnapshot:
    return PolicyComplianceCurrentSnapshot(
        subject=receipt.subject,
        subject_content_digest=receipt.subject_content_digest,
        input_digest=receipt.input_digest,
        policy_set_digest=receipt.policy_set_digest,
        binding_head_digest=receipt.binding_head_digest,
        catalog_version=receipt.catalog_version,
        ruleset_version=receipt.ruleset_version,
    )


def _snapshot(
    *,
    receipt: PolicyComplianceReceipt | None = None,
    current: PolicyComplianceCurrentSnapshot | None = None,
    applicable_rule_count: int = 1,
    applicable_blocking_rule_count: int = 1,
    available: bool = True,
    from_status: str = "approved",
) -> PolicyTransitionSnapshot:
    return PolicyTransitionSnapshot(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        subject_id="spec-1",
        expected_from_status=from_status,
        applicable_rule_count=applicable_rule_count,
        applicable_blocking_rule_count=applicable_blocking_rule_count,
        receipt=receipt,
        current_snapshot=current,
        evaluation_available=available,
        evaluation_error_code=None if available else "evaluator_timeout",
    )


def test_registry_marks_exactly_the_frozen_policy_edges() -> None:
    actual = {
        (entity_type, from_status, edge.to_status)
        for entity_type, lifecycle in SDLC_REGISTRY.items()
        for from_status, edges in lifecycle.transitions.items()
        for edge in edges
        if edge.policy_compliance
    }

    assert actual == FROZEN_POLICY_EDGES
    for entity_type, from_status, to_status in actual:
        assert transition_requires_policy_compliance(
            entity_type,
            from_status,
            to_status,
        )
        assert is_transition_allowed(entity_type, from_status, to_status)


@pytest.mark.parametrize(
    ("entity_type", "from_status", "to_status"),
    (
        ("ideation", "evaluating", "approved"),
        ("ideation", "evaluating", "cancelled"),
        ("ideation", "done", "draft"),
        ("refinement", "approved", "review"),
        ("refinement", "cancelled", "draft"),
        ("spec", "approved", "review"),
        ("spec", "approved", "draft"),
        ("spec", "validated", "approved"),
        ("spec", "done", "draft"),
        ("sprint", "review", "active"),
        ("sprint", "closed", "draft"),
        ("card", "validation", "in_progress"),
        ("card", "done", "in_progress"),
        ("card", "in_progress", "cancelled"),
        ("test_scenario", "automated", "ready"),
        ("test_scenario", "failed", "ready"),
        ("test_scenario", "passed", "ready"),
    ),
)
def test_recovery_regression_and_cancellation_edges_are_never_policy_gated(
    entity_type: str,
    from_status: str,
    to_status: str,
) -> None:
    assert is_transition_allowed(entity_type, from_status, to_status)
    assert not transition_requires_policy_compliance(
        entity_type,
        from_status,
        to_status,
    )


def test_test_scenario_registry_is_the_closed_normative_matrix() -> None:
    actual = {
        status: tuple(
            edge.to_status for edge in transition_contracts("test_scenario", status)
        )
        for status in TEST_SCENARIO_EDGES
    }

    assert actual == TEST_SCENARIO_EDGES
    gated_edges = tuple(
        edge
        for status in TEST_SCENARIO_EDGES
        for edge in transition_contracts("test_scenario", status)
        if edge.policy_compliance
    )
    assert gated_edges
    assert all(
        edge.gate == "test_scenario_progression"
        and edge.preconditions == ("authenticated_test_evidence",)
        and "evidence_required" in edge.reason_codes
        and "policy_compliance_receipt_stale" in edge.reason_codes
        for edge in gated_edges
    )
    assert is_test_scenario_status_transition_allowed("passed", "passed")
    assert not is_test_scenario_status_transition_allowed(
        "automated",
        "failed",
    )
    assert not is_test_scenario_status_transition_allowed("passed", "failed")
    assert not is_test_scenario_status_transition_allowed("automated", "draft")
    with pytest.raises(InvalidScenarioStatusTransitionError) as error:
        require_test_scenario_status_transition("passed", "failed")
    assert error.value.code == "status_transition_not_allowed"


@pytest.mark.parametrize("target", ("review", "draft", "cancelled"))
def test_non_frozen_edges_stay_free_when_evaluator_is_unavailable(
    target: str,
) -> None:
    snapshot = _snapshot(
        available=False,
        applicable_rule_count=1,
        applicable_blocking_rule_count=1,
    )

    decision = evaluate_policy_transition(snapshot, target)

    assert decision.allowed
    assert not decision.policy_compliance_required
    assert (
        decision.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_NOT_REQUIRED
    )


def test_missing_and_stale_receipts_block_only_applicable_blocking_policy() -> None:
    missing = evaluate_policy_transition(_snapshot(), "validated")
    passed = _receipt(_rule())
    stale_current = replace(_current(passed), policy_set_digest="f" * 64)
    stale = evaluate_policy_transition(
        _snapshot(receipt=passed, current=stale_current),
        "validated",
    )

    assert not missing.allowed
    assert (
        missing.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_RECEIPT_MISSING
    )
    assert not stale.allowed
    assert (
        stale.reason_code is PolicyTransitionReasonCode.POLICY_COMPLIANCE_RECEIPT_STALE
    )
    assert stale.currentness_reasons == (PolicyCurrentnessReason.POLICY_SET_CHANGED,)


def test_stale_historical_counts_may_exceed_live_policy_after_unlink() -> None:
    historical = _receipt(
        _rule(outcome=PolicyEvaluationOutcome.FAIL, rule_id="rule-1"),
        _rule(outcome=PolicyEvaluationOutcome.FAIL, rule_id="rule-2"),
    )
    live = replace(_current(historical), policy_set_digest="f" * 64)

    decision = evaluate_policy_transition(
        _snapshot(
            receipt=historical,
            current=live,
            applicable_rule_count=1,
            applicable_blocking_rule_count=1,
        ),
        "validated",
    )

    assert not decision.allowed
    assert decision.blocking_rule_count == 2
    assert (
        decision.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_RECEIPT_STALE
    )


@pytest.mark.parametrize(
    ("receipt", "reason"),
    (
        (
            _receipt(_rule()),
            PolicyTransitionReasonCode.POLICY_COMPLIANCE_READY,
        ),
        (
            _receipt(
                _rule(
                    outcome=PolicyEvaluationOutcome.FAIL,
                    enforcement=GuidelineEnforcement.ADVISORY,
                )
            ),
            PolicyTransitionReasonCode.POLICY_COMPLIANCE_ADVISORY_ONLY,
        ),
        (
            _receipt(
                _rule(
                    outcome=PolicyEvaluationOutcome.ERROR,
                    enforcement=GuidelineEnforcement.ADVISORY,
                )
            ),
            PolicyTransitionReasonCode.POLICY_COMPLIANCE_ADVISORY_ONLY,
        ),
        (
            _receipt(
                _rule(
                    outcome=PolicyEvaluationOutcome.FAIL,
                    enforcement=GuidelineEnforcement.BLOCKING,
                    waiver_id="waiver-1",
                )
            ),
            PolicyTransitionReasonCode.POLICY_COMPLIANCE_READY_WITH_WAIVERS,
        ),
        (
            _receipt(),
            PolicyTransitionReasonCode.POLICY_COMPLIANCE_NOT_APPLICABLE,
        ),
    ),
)
def test_current_pass_advisory_and_not_applicable_receipts_allow(
    receipt: PolicyComplianceReceipt,
    reason: PolicyTransitionReasonCode,
) -> None:
    blocking_rules = sum(
        1
        for result in receipt.rule_results
        if result.enforcement is GuidelineEnforcement.BLOCKING
    )
    decision = evaluate_policy_transition(
        _snapshot(
            receipt=receipt,
            current=_current(receipt),
            applicable_rule_count=receipt.rule_count,
            applicable_blocking_rule_count=blocking_rules,
        ),
        "validated",
    )

    assert decision.allowed
    assert decision.reason_code is reason


@pytest.mark.parametrize(
    ("available", "receipt", "reason"),
    (
        (
            False,
            None,
            PolicyTransitionReasonCode.POLICY_EVALUATION_UNAVAILABLE,
        ),
        (
            True,
            _receipt(
                _rule(
                    outcome=PolicyEvaluationOutcome.ERROR,
                    enforcement=GuidelineEnforcement.BLOCKING,
                )
            ),
            PolicyTransitionReasonCode.POLICY_EVALUATION_UNAVAILABLE,
        ),
        (
            True,
            _receipt(
                _rule(
                    outcome=PolicyEvaluationOutcome.FAIL,
                    enforcement=GuidelineEnforcement.BLOCKING,
                )
            ),
            PolicyTransitionReasonCode.POLICY_COMPLIANCE_BLOCKED,
        ),
    ),
)
def test_blocking_failure_and_unavailability_fail_closed(
    available: bool,
    receipt: PolicyComplianceReceipt | None,
    reason: PolicyTransitionReasonCode,
) -> None:
    decision = evaluate_policy_transition(
        _snapshot(
            receipt=receipt,
            current=_current(receipt) if receipt is not None else None,
            available=available,
        ),
        "validated",
    )

    assert not decision.allowed
    assert decision.reason_code is reason


@pytest.mark.parametrize(
    ("available", "receipt", "current"),
    (
        (True, None, None),
        (False, None, None),
        (
            True,
            _receipt(
                _rule(
                    outcome=PolicyEvaluationOutcome.FAIL,
                    enforcement=GuidelineEnforcement.ADVISORY,
                )
            ),
            None,
        ),
    ),
)
def test_advisory_only_missing_stale_or_outage_is_degraded_but_allows(
    available: bool,
    receipt: PolicyComplianceReceipt | None,
    current: PolicyComplianceCurrentSnapshot | None,
) -> None:
    decision = evaluate_policy_transition(
        _snapshot(
            receipt=receipt,
            current=current,
            applicable_rule_count=1,
            applicable_blocking_rule_count=0,
            available=available,
        ),
        "validated",
    )

    assert decision.allowed
    assert decision.reason_code is PolicyTransitionReasonCode.POLICY_EVALUATION_DEGRADED


def test_zero_applicable_rules_needs_no_receipt_and_allows() -> None:
    decision = evaluate_policy_transition(
        _snapshot(
            applicable_rule_count=0,
            applicable_blocking_rule_count=0,
        ),
        "validated",
    )

    assert decision.allowed
    assert (
        decision.reason_code
        is PolicyTransitionReasonCode.POLICY_COMPLIANCE_NOT_APPLICABLE
    )


def test_decision_and_fence_digests_are_deterministic_and_close_toctou() -> None:
    receipt = _receipt(_rule())
    first = evaluate_policy_transition(
        _snapshot(receipt=receipt, current=_current(receipt)),
        "validated",
    )
    replay = evaluate_policy_transition(
        _snapshot(receipt=receipt, current=_current(receipt)),
        "validated",
    )
    changed = evaluate_policy_transition(
        _snapshot(
            receipt=receipt,
            current=replace(_current(receipt), input_digest="f" * 64),
        ),
        "validated",
    )

    assert replay.fence_digest == first.fence_digest
    assert replay.decision_digest == first.decision_digest
    require_policy_transition_decision_match(first.decision_digest, replay)
    assert changed.fence_digest != first.fence_digest
    assert changed.decision_digest != first.decision_digest
    with pytest.raises(GuidelinePolicyContractError) as error:
        require_policy_transition_decision_match(
            first.decision_digest,
            changed,
        )
    assert error.value.code == "policy_transition_decision_changed"


def test_typed_rejection_exposes_reason_and_all_fences_without_mutating_history() -> (
    None
):
    receipt = _receipt(
        _rule(
            outcome=PolicyEvaluationOutcome.FAIL,
            enforcement=GuidelineEnforcement.BLOCKING,
        )
    )
    historical = receipt
    decision = evaluate_policy_transition(
        _snapshot(receipt=receipt, current=_current(receipt)),
        "validated",
    )

    with pytest.raises(PolicyTransitionRejected) as error:
        raise_for_policy_transition(decision)

    assert error.value.code == "policy_compliance_blocked"
    assert error.value.reason_codes == decision.reason_codes
    assert error.value.decision_digest == decision.decision_digest
    assert error.value.fence_digest == decision.fence_digest
    assert error.value.currentness is PolicyCurrentness.CURRENT
    assert receipt is historical
    assert receipt.currentness is PolicyCurrentness.CURRENT


def test_current_receipt_counts_cannot_disagree_with_live_applicability() -> None:
    receipt = _receipt(_rule())

    with pytest.raises(GuidelinePolicyContractError) as error:
        _snapshot(
            receipt=receipt,
            current=_current(receipt),
            applicable_rule_count=1,
            applicable_blocking_rule_count=0,
        )

    assert error.value.code == "policy_transition_current_receipt_rule_counts_mismatch"


async def test_guideline_service_recovery_edge_never_resolves_policy(
    monkeypatch,
) -> None:
    service = GuidelineService(object())

    def _unexpected_policy(_self):
        raise AssertionError("recovery edge must not resolve policy persistence")

    monkeypatch.setattr(GuidelineService, "_policy", _unexpected_policy)

    decision = await service.preview_policy_transition(
        board_id="board-1",
        entity_type="spec",
        subject_id="spec-1",
        from_status="validated",
        to_status="approved",
    )

    assert decision is None


async def test_guideline_service_preview_uses_transaction_snapshot_resolver(
    monkeypatch,
) -> None:
    calls: list[dict[str, object]] = []

    class _Resolver:
        async def resolve_transition_snapshot(self, **kwargs):
            calls.append(kwargs)
            return _snapshot(
                applicable_rule_count=0,
                applicable_blocking_rule_count=0,
            )

    service = GuidelineService(object())
    monkeypatch.setattr(GuidelineService, "_policy", lambda _self: _Resolver())

    decision = await service.preview_policy_transition(
        board_id="board-1",
        entity_type="spec",
        subject_id="spec-1",
        from_status="approved",
        to_status="validated",
    )

    assert decision is not None
    assert decision.allowed
    assert decision.reason_code is PolicyTransitionReasonCode.POLICY_COMPLIANCE_NOT_APPLICABLE
    assert calls == [
        {
            "board_id": "board-1",
            "entity_type": PolicyEntityType.SPEC,
            "subject_id": "spec-1",
            "expected_from_status": "approved",
        }
    ]
