"""Compatibility-path checks for the SK-B3 semantic native gate."""

from __future__ import annotations

from dataclasses import fields

import okto_pulse.core.domain.guideline_policy_transition as public_gate
import okto_pulse.core.domain.guideline_semantic_transition as semantic_gate
from okto_pulse.core.domain.guideline_policy_transition import (
    PolicyTransitionDecision,
    PolicyTransitionDiagnosticCode,
    PolicyTransitionReasonCode,
    PolicyTransitionSnapshot,
)


def test_historical_module_path_exports_the_active_semantic_contract() -> None:
    assert (
        public_gate.POLICY_TRANSITION_CONTRACT_VERSION
        == "semantic-policy-transition/v2"
    )
    assert public_gate.PolicyTransitionSnapshot is (
        semantic_gate.PolicyTransitionSnapshot
    )
    assert public_gate.PolicyTransitionDecision is (
        semantic_gate.PolicyTransitionDecision
    )
    assert public_gate.evaluate_policy_transition is (
        semantic_gate.evaluate_policy_transition
    )


def test_transition_contract_has_no_executable_rule_fields_or_states() -> None:
    snapshot_fields = {item.name for item in fields(PolicyTransitionSnapshot)}
    decision_fields = {item.name for item in fields(PolicyTransitionDecision)}
    public_names = set(public_gate.__all__)

    assert "bindings" in snapshot_fields
    assert "applicable_metric_count" in snapshot_fields
    assert "binding_decisions" in decision_fields
    assert "applicable_metric_count" in decision_fields
    assert "failed_metric_count" in decision_fields
    assert not {
        "applicable_rule_count",
        "applicable_blocking_rule_count",
        "blocking_rule_count",
        "waived_rule_count",
    } & (snapshot_fields | decision_fields)
    assert "PolicyEvaluationOutcome" not in public_names
    assert "PolicyComplianceReceipt" not in public_names
    for name in (
        "applicable_rule_count",
        "applicable_blocking_rule_count",
        "blocking_rule_count",
        "waived_rule_count",
    ):
        assert not hasattr(PolicyTransitionDecision, name)


def test_reason_and_diagnostic_vocabularies_are_closed_and_unambiguous() -> None:
    assert {item.value for item in PolicyTransitionReasonCode} == {
        "transition_not_allowed",
        "policy_compliance_not_required",
        "policy_subject_required",
        "policy_compliance_receipt_missing",
        "policy_compliance_receipt_stale",
        "policy_compliance_blocked",
        "policy_assessment_unavailable",
        "policy_compliance_ready",
        "policy_compliance_ready_with_waivers",
        "policy_compliance_not_applicable",
        "policy_compliance_advisory_only",
    }
    assert {item.value for item in PolicyTransitionDiagnosticCode} == {
        "policy_compliance_receipt_missing",
        "policy_compliance_receipt_stale",
        "policy_assessment_unavailable",
        "policy_assessment_inadmissible",
        "policy_metric_threshold_failed",
    }
    assert "policy_evaluation_unavailable" not in {
        item.value for item in PolicyTransitionReasonCode
    }
    assert "policy_evaluation_degraded" not in {
        item.value for item in PolicyTransitionReasonCode
    }
