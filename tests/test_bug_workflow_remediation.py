"""Canonical bug workflow remediation message coverage."""

from __future__ import annotations

from okto_pulse.core.models.db import Card, CardStatus, CardType, Spec
from okto_pulse.core.services.bug_regression_scenarios import (
    BugRegressionCoverageState,
    BugRegressionEligibilityReason,
    BugRegressionNextAction,
    BugRegressionScenarioEligibilityResult,
    BugRegressionScenarioEligibilityResolver,
    EligibleBugRegressionScenario,
)
from okto_pulse.core.services.bug_workflow_remediation import (
    BugWorkflowHotfixLaneStatus,
    BugWorkflowNextAction,
    BugWorkflowRemediationMessageBuilder,
    BugWorkflowRemediationPath,
    bug_workflow_remediation_safe_labels,
)


def _spec() -> Spec:
    return Spec(
        id="spec-1",
        board_id="board-1",
        title="Bug workflow spec",
        created_by="agent",
        test_scenarios=[
            {
                "id": "ts-origin",
                "title": "Origin regression",
                "status": "passed",
                "linked_task_ids": ["origin-1"],
            },
            {
                "id": "ts-unrelated",
                "title": "Unrelated scenario",
                "status": "passed",
            },
        ],
    )


def _card(
    card_id: str,
    *,
    card_type: CardType = CardType.NORMAL,
    test_scenario_ids: list[str] | None = None,
) -> Card:
    return Card(
        id=card_id,
        board_id="board-1",
        spec_id="spec-1",
        title=card_id,
        status=CardStatus.DONE,
        card_type=card_type,
        created_by="agent",
        test_scenario_ids=test_scenario_ids,
    )


def test_builder_formats_path_a_without_unlock_guidance():
    result = BugRegressionScenarioEligibilityResolver().resolve(
        bug_card=_card("bug-1", card_type=CardType.BUG),
        spec=_spec(),
        origin_task=_card("origin-1"),
        candidate_scenario_ids=["ts-origin"],
    )

    message = BugWorkflowRemediationMessageBuilder().build_from_eligibility(result)
    payload = message.to_dict()

    assert payload["remediation_path"] == BugWorkflowRemediationPath.PATH_A_REUSE_SCENARIO.value
    assert payload["next_action"] == BugWorkflowNextAction.CREATE_REGRESSION_TEST_CARD.value
    assert payload["semantic_gap_required"] is False
    assert payload["eligible_scenarios_count"] == 1
    assert payload["actions"][0]["primary"] is True
    assert "unlock" not in payload["detail"].lower()
    assert "validated spec" in payload["detail"].lower()


def test_builder_formats_path_b_for_unrelated_scenario():
    result = BugRegressionScenarioEligibilityResolver().resolve(
        bug_card=_card("bug-1", card_type=CardType.BUG),
        spec=_spec(),
        origin_task=_card("origin-1"),
        candidate_scenario_ids=["ts-unrelated"],
    )

    message = BugWorkflowRemediationMessageBuilder().build_from_eligibility(result)
    payload = message.to_dict()

    assert payload["reason_code"] == "unrelated_scenario"
    assert payload["remediation_path"] == BugWorkflowRemediationPath.PATH_B_SEMANTIC_GAP.value
    assert payload["next_action"] == BugWorkflowNextAction.ESCALATE_SEMANTIC_GAP.value
    assert payload["semantic_gap_required"] is True
    assert "unrelated same-spec scenario" in payload["detail"]


def test_builder_formats_path_b_coverage_pending_without_path_a_guidance():
    result = BugRegressionScenarioEligibilityResult(
        bug_id="bug-1",
        spec_id="spec-1",
        eligible_scenarios=(),
        rejected_scenarios=(),
        semantic_gap_required=False,
        spec_mutation_required=False,
        next_action=BugRegressionNextAction.CONFIRM_VALIDATOR_COVERAGE,
        coverage_state=BugRegressionCoverageState.COVERAGE_PENDING,
        coverage_pending_scenarios=("ts-foreign",),
        amendment_revision_id="amd-1",
        amendment_status="done",
        lineage_state="complete",
        safe_next_actions=(BugRegressionNextAction.CONFIRM_VALIDATOR_COVERAGE.value,),
    )

    payload = BugWorkflowRemediationMessageBuilder().build_from_eligibility(result).to_dict()

    assert payload["reason_code"] == "coverage_pending"
    assert payload["remediation_path"] == (
        BugWorkflowRemediationPath.PATH_B_AMENDMENT_LINEAGE.value
    )
    assert payload["next_action"] == BugWorkflowNextAction.CONFIRM_VALIDATOR_COVERAGE.value
    assert payload["actions"][0]["action_id"] == "confirm_validator_coverage"
    assert "okto_pulse_confirm_amendment_coverage" in payload["detail"]
    assert "Path A" not in payload["detail"]
    assert payload["facts"]["coverage_state"] == "coverage_pending"
    assert payload["facts"]["amendment_revision_id"] == "amd-1"


def test_builder_formats_path_b_ready_without_regression_card_action():
    result = BugRegressionScenarioEligibilityResult(
        bug_id="bug-1",
        spec_id="spec-1",
        eligible_scenarios=(
            EligibleBugRegressionScenario(
                scenario_id="ts-foreign",
                title="Foreign regression",
                reason=BugRegressionEligibilityReason.PATH_B_AMENDMENT_LINEAGE,
                source_task_id="origin-1",
            ),
        ),
        rejected_scenarios=(),
        semantic_gap_required=False,
        spec_mutation_required=False,
        next_action=BugRegressionNextAction.CREATE_REGRESSION_TEST_CARD,
        coverage_state=BugRegressionCoverageState.PATH_B_READY,
        eligible_regression_artifacts=("ts-foreign",),
        amendment_revision_id="amd-1",
        amendment_status="done",
        lineage_state="complete",
    )

    payload = BugWorkflowRemediationMessageBuilder().build_from_eligibility(result).to_dict()

    assert payload["reason_code"] == "path_b_amendment_lineage"
    assert payload["remediation_path"] == (
        BugWorkflowRemediationPath.PATH_B_AMENDMENT_LINEAGE.value
    )
    assert payload["next_action"] == BugWorkflowNextAction.NONE.value
    assert payload["actions"] == []
    assert "Path A" not in payload["message"]
    assert "Path A" not in payload["detail"]
    assert payload["facts"]["coverage_state"] == "path_b_ready"


def test_builder_formats_path_c_hotfix_lane_without_reopen_guidance():
    message = BugWorkflowRemediationMessageBuilder().build_from_sprint_lane_block(
        code="sprint_not_active",
        remediation="activate_hotfix_lane",
        message="Card's sprint is not active",
        facts={
            "card_id": "bug-1",
            "spec_id": "spec-1",
            "sprint_id": "sprint-1",
            "sprint_status": "draft",
            "lane_type": "hotfix",
            "next_action": "activate_hotfix_lane",
        },
    )
    payload = message.to_dict()

    assert payload["remediation_path"] == BugWorkflowRemediationPath.PATH_C_HOTFIX_LANE.value
    assert payload["next_action"] == BugWorkflowNextAction.ACTIVATE_HOTFIX_LANE.value
    assert payload["hotfix_lane_status"] == BugWorkflowHotfixLaneStatus.INACTIVE.value
    assert "reopen" not in payload["detail"].lower()
    assert payload["facts"]["lane_type"] == "hotfix"


def test_missing_test_task_points_to_path_a_then_path_b_if_needed():
    message = BugWorkflowRemediationMessageBuilder().build_missing_regression_test_task()
    payload = message.to_dict()

    assert payload["reason_code"] == "missing_regression_test_task"
    assert payload["next_action"] == BugWorkflowNextAction.CREATE_REGRESSION_TEST_CARD.value
    assert payload["semantic_gap_required"] is False
    assert "Path A" in payload["detail"]
    assert "Path B" in payload["detail"]


def test_safe_labels_exclude_payload_text():
    message = BugWorkflowRemediationMessageBuilder().build_semantic_gap(
        reason_code="scenario_not_found"
    )

    labels = bug_workflow_remediation_safe_labels(message, surface="mcp")

    assert labels == {
        "reason_code": "scenario_not_found",
        "remediation_path": "path_b_semantic_gap",
        "next_action": "escalate_semantic_gap",
        "hotfix_lane_status": "not_applicable",
        "surface": "mcp",
        "outcome": "blocked",
    }
    serialized = repr(labels).lower()
    assert "expected_behavior" not in serialized
    assert "observed_behavior" not in serialized
    assert "description" not in serialized
