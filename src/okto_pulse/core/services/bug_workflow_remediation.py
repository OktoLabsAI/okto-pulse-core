"""Canonical bug workflow remediation guidance.

The builder in this module is intentionally pure. It formats already-computed
bug regression eligibility and sprint-lane facts into one bounded contract for
MCP, REST, UI, documentation, audit, and metrics surfaces.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Sequence

from okto_pulse.core.services.bug_regression_scenarios import (
    BugRegressionNextAction,
    BugRegressionScenarioEligibilityResult,
)


class BugWorkflowRemediationPath(str, Enum):
    """Canonical operator path for a blocked bug workflow."""

    PATH_A_REUSE_SCENARIO = "path_a_reuse_existing_scenario"
    PATH_B_SEMANTIC_GAP = "path_b_semantic_gap"
    PATH_C_HOTFIX_LANE = "path_c_hotfix_lane"
    STANDARD_SPRINT = "standard_sprint"
    NONE = "none"


class BugWorkflowNextAction(str, Enum):
    """Bounded next action vocabulary shared by REST, MCP, UI and telemetry."""

    CREATE_REGRESSION_TEST_CARD = "create_regression_test_card"
    ESCALATE_SEMANTIC_GAP = "escalate_semantic_gap"
    ASSIGN_HOTFIX_LANE = "assign_hotfix_lane"
    ACTIVATE_HOTFIX_LANE = "activate_hotfix_lane"
    ASSIGN_SPRINT = "assign_sprint"
    ACTIVATE_SPRINT = "activate_sprint"
    NONE = "none"


class BugWorkflowHotfixLaneStatus(str, Enum):
    """Bounded hotfix lane status exposed to operators."""

    NOT_APPLICABLE = "not_applicable"
    MISSING = "missing"
    INACTIVE = "inactive"
    READY = "ready"


@dataclass(frozen=True)
class BugWorkflowRemediationAction:
    """Single UI/MCP action descriptor with no sensitive payload."""

    action_id: str
    label: str
    description: str
    primary: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "action_id": self.action_id,
            "label": self.label,
            "description": self.description,
            "primary": self.primary,
        }


@dataclass(frozen=True)
class BugWorkflowRemediationMessage:
    """Canonical, additive remediation payload for blocked bug workflows."""

    reason_code: str
    remediation_path: BugWorkflowRemediationPath
    next_action: BugWorkflowNextAction
    semantic_gap_required: bool
    eligible_scenarios_count: int
    hotfix_lane_status: BugWorkflowHotfixLaneStatus
    message: str
    detail: str
    actions: tuple[BugWorkflowRemediationAction, ...] = field(default_factory=tuple)
    facts: Mapping[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "reason_code": self.reason_code,
            "remediation_path": self.remediation_path.value,
            "next_action": self.next_action.value,
            "semantic_gap_required": self.semantic_gap_required,
            "eligible_scenarios_count": self.eligible_scenarios_count,
            "hotfix_lane_status": self.hotfix_lane_status.value,
            "message": self.message,
            "detail": self.detail,
            "actions": [action.to_dict() for action in self.actions],
            "facts": dict(self.facts),
        }

    def safe_labels(self, *, surface: str, outcome: str = "blocked") -> dict[str, str]:
        """Return the only labels allowed for metrics/audit correlation."""

        return {
            "reason_code": self.reason_code,
            "remediation_path": self.remediation_path.value,
            "next_action": self.next_action.value,
            "hotfix_lane_status": self.hotfix_lane_status.value,
            "surface": surface,
            "outcome": outcome,
        }


class BugWorkflowRemediationMessageBuilder:
    """Build deterministic remediation payloads from existing workflow facts."""

    def build_from_eligibility(
        self,
        result: BugRegressionScenarioEligibilityResult,
        *,
        reason_code: str | None = None,
    ) -> BugWorkflowRemediationMessage:
        if result.semantic_gap_required:
            return self._semantic_gap_message(
                reason_code=reason_code or self._primary_rejection_reason(result),
                eligible_count=len(result.eligible_scenarios),
            )

        return BugWorkflowRemediationMessage(
            reason_code=reason_code or self._primary_eligible_reason(result),
            remediation_path=BugWorkflowRemediationPath.PATH_A_REUSE_SCENARIO,
            next_action=BugWorkflowNextAction.CREATE_REGRESSION_TEST_CARD,
            semantic_gap_required=False,
            eligible_scenarios_count=len(result.eligible_scenarios),
            hotfix_lane_status=BugWorkflowHotfixLaneStatus.NOT_APPLICABLE,
            message=(
                "Create a fresh regression test card that references one of the "
                "eligible existing scenarios."
            ),
            detail=(
                "This is Path A: reuse an existing scenario linked to the bug "
                "origin task or explicit affected tasks. Leave validated spec "
                "content unchanged when adding regression evidence."
            ),
            actions=(
                BugWorkflowRemediationAction(
                    action_id=BugWorkflowNextAction.CREATE_REGRESSION_TEST_CARD.value,
                    label="Create regression test card",
                    description=(
                        "Create a new test card in the bug spec using an eligible "
                        "scenario id, then link that test card to the bug."
                    ),
                    primary=True,
                ),
            ),
            facts={
                "spec_id": result.spec_id,
                "bug_id": result.bug_id,
                "eligible_scenarios_count": len(result.eligible_scenarios),
                "rejected_scenarios_count": len(result.rejected_scenarios),
            },
        )

    def build_missing_regression_test_task(
        self,
        *,
        eligible_scenarios_count: int = 0,
        hotfix_lane_status: BugWorkflowHotfixLaneStatus = (
            BugWorkflowHotfixLaneStatus.NOT_APPLICABLE
        ),
    ) -> BugWorkflowRemediationMessage:
        return BugWorkflowRemediationMessage(
            reason_code="missing_regression_test_task",
            remediation_path=BugWorkflowRemediationPath.PATH_A_REUSE_SCENARIO,
            next_action=BugWorkflowNextAction.CREATE_REGRESSION_TEST_CARD,
            semantic_gap_required=False,
            eligible_scenarios_count=max(0, int(eligible_scenarios_count)),
            hotfix_lane_status=hotfix_lane_status,
            message=(
                "Bug card requires at least one linked regression test card "
                "before it can move to in_progress."
            ),
            detail=(
                "Use Path A first: create a fresh test card that references an "
                "eligible existing scenario from the bug spec, link it to this "
                "bug, then retry the move. If no eligible scenario exists, treat "
                "the bug as a semantic gap and use Path B."
            ),
            actions=(
                BugWorkflowRemediationAction(
                    action_id=BugWorkflowNextAction.CREATE_REGRESSION_TEST_CARD.value,
                    label="Create regression test card",
                    description=(
                        "Create a post-bug test card with card_type='test' and "
                        "test_scenario_ids set to an eligible existing scenario."
                    ),
                    primary=True,
                ),
            ),
        )

    def build_from_sprint_lane_block(
        self,
        *,
        code: str,
        remediation: str | None,
        facts: Mapping[str, Any] | None = None,
        message: str | None = None,
    ) -> BugWorkflowRemediationMessage:
        facts = dict(facts or {})
        next_action = self._next_action_from_string(
            remediation or str(facts.get("next_action") or "")
        )
        hotfix_lane_status = self._hotfix_lane_status(
            code=code,
            next_action=next_action,
            facts=facts,
        )
        remediation_path = (
            BugWorkflowRemediationPath.PATH_C_HOTFIX_LANE
            if next_action
            in {
                BugWorkflowNextAction.ASSIGN_HOTFIX_LANE,
                BugWorkflowNextAction.ACTIVATE_HOTFIX_LANE,
            }
            else BugWorkflowRemediationPath.STANDARD_SPRINT
        )
        primary_action = BugWorkflowRemediationAction(
            action_id=next_action.value,
            label=self._action_label(next_action),
            description=self._action_description(next_action),
            primary=True,
        )
        return BugWorkflowRemediationMessage(
            reason_code=code,
            remediation_path=remediation_path,
            next_action=next_action,
            semantic_gap_required=False,
            eligible_scenarios_count=int(facts.get("eligible_scenarios_count") or 0),
            hotfix_lane_status=hotfix_lane_status,
            message=message or "Bug card cannot advance until its sprint lane is executable.",
            detail=(
                "This is Path C for post-closure bugs: assign the bug and its "
                "regression test card to an active hotfix sprint lane. Do not "
                "change the original closed delivery sprint."
                if remediation_path == BugWorkflowRemediationPath.PATH_C_HOTFIX_LANE
                else "Assign the card to an active sprint lane before advancing."
            ),
            actions=(primary_action,),
            facts=self._bounded_facts(facts),
        )

    def build_semantic_gap(
        self,
        *,
        reason_code: str = "no_eligible_scenarios",
    ) -> BugWorkflowRemediationMessage:
        return self._semantic_gap_message(reason_code=reason_code, eligible_count=0)

    def _semantic_gap_message(
        self,
        *,
        reason_code: str,
        eligible_count: int,
    ) -> BugWorkflowRemediationMessage:
        return BugWorkflowRemediationMessage(
            reason_code=reason_code,
            remediation_path=BugWorkflowRemediationPath.PATH_B_SEMANTIC_GAP,
            next_action=BugWorkflowNextAction.ESCALATE_SEMANTIC_GAP,
            semantic_gap_required=True,
            eligible_scenarios_count=max(0, int(eligible_count)),
            hotfix_lane_status=BugWorkflowHotfixLaneStatus.NOT_APPLICABLE,
            message="No eligible existing regression scenario can satisfy this bug gate.",
            detail=(
                "This is Path B: create an amendment, refinement, spec revision, "
                "or hotfix spec for the missing canonical coverage. Do not attach "
                "an unrelated same-spec scenario as a shortcut."
            ),
            actions=(
                BugWorkflowRemediationAction(
                    action_id=BugWorkflowNextAction.ESCALATE_SEMANTIC_GAP.value,
                    label="Escalate semantic gap",
                    description=(
                        "Route the bug to an amendment/refinement/spec revision/"
                        "hotfix spec before adding new canonical scenarios."
                    ),
                    primary=True,
                ),
            ),
        )

    @staticmethod
    def _primary_rejection_reason(
        result: BugRegressionScenarioEligibilityResult,
    ) -> str:
        if result.rejected_scenarios:
            return result.rejected_scenarios[0].reason.value
        return "no_eligible_scenarios"

    @staticmethod
    def _primary_eligible_reason(
        result: BugRegressionScenarioEligibilityResult,
    ) -> str:
        if result.eligible_scenarios:
            return result.eligible_scenarios[0].reason.value
        return "eligible_scenario"

    @staticmethod
    def _next_action_from_string(value: str) -> BugWorkflowNextAction:
        try:
            return BugWorkflowNextAction(value)
        except ValueError:
            return BugWorkflowNextAction.NONE

    @staticmethod
    def _hotfix_lane_status(
        *,
        code: str,
        next_action: BugWorkflowNextAction,
        facts: Mapping[str, Any],
    ) -> BugWorkflowHotfixLaneStatus:
        if next_action == BugWorkflowNextAction.ASSIGN_HOTFIX_LANE:
            return BugWorkflowHotfixLaneStatus.MISSING
        if next_action == BugWorkflowNextAction.ACTIVATE_HOTFIX_LANE:
            return BugWorkflowHotfixLaneStatus.INACTIVE
        if str(facts.get("lane_type") or "") == "hotfix" and code not in {
            "sprint_required",
            "sprint_not_active",
            "sprint_not_found",
        }:
            return BugWorkflowHotfixLaneStatus.READY
        return BugWorkflowHotfixLaneStatus.NOT_APPLICABLE

    @staticmethod
    def _action_label(action: BugWorkflowNextAction) -> str:
        labels = {
            BugWorkflowNextAction.CREATE_REGRESSION_TEST_CARD: "Create regression test card",
            BugWorkflowNextAction.ESCALATE_SEMANTIC_GAP: "Escalate semantic gap",
            BugWorkflowNextAction.ASSIGN_HOTFIX_LANE: "Assign hotfix lane",
            BugWorkflowNextAction.ACTIVATE_HOTFIX_LANE: "Activate hotfix lane",
            BugWorkflowNextAction.ASSIGN_SPRINT: "Assign sprint",
            BugWorkflowNextAction.ACTIVATE_SPRINT: "Activate sprint",
            BugWorkflowNextAction.NONE: "Review workflow blocker",
        }
        return labels[action]

    @staticmethod
    def _action_description(action: BugWorkflowNextAction) -> str:
        descriptions = {
            BugWorkflowNextAction.CREATE_REGRESSION_TEST_CARD: (
                "Create and link a fresh regression test card."
            ),
            BugWorkflowNextAction.ESCALATE_SEMANTIC_GAP: (
                "Create the required amendment/refinement/spec revision/hotfix spec."
            ),
            BugWorkflowNextAction.ASSIGN_HOTFIX_LANE: (
                "Create or choose a hotfix sprint lane and assign the bug and test card."
            ),
            BugWorkflowNextAction.ACTIVATE_HOTFIX_LANE: (
                "Move the assigned hotfix sprint lane to active before retrying."
            ),
            BugWorkflowNextAction.ASSIGN_SPRINT: (
                "Assign the card to an active sprint before retrying."
            ),
            BugWorkflowNextAction.ACTIVATE_SPRINT: (
                "Activate the assigned sprint before retrying."
            ),
            BugWorkflowNextAction.NONE: "Review the returned facts and choose a valid path.",
        }
        return descriptions[action]

    @staticmethod
    def _bounded_facts(facts: Mapping[str, Any]) -> dict[str, object]:
        allowed_keys = {
            "card_id",
            "spec_id",
            "spec_status",
            "sprint_id",
            "sprint_status",
            "lane_type",
            "next_action",
            "eligible_scenarios_count",
        }
        bounded: dict[str, object] = {}
        for key in allowed_keys:
            if key in facts:
                value = facts[key]
                if value is None or isinstance(value, (str, int, float, bool)):
                    bounded[key] = value
        return bounded


def serialize_bug_workflow_remediation(
    message: BugWorkflowRemediationMessage | None,
) -> dict[str, object] | None:
    """Convenience serializer for API and MCP payloads."""

    return message.to_dict() if message else None


def bug_workflow_remediation_safe_labels(
    message: BugWorkflowRemediationMessage,
    *,
    surface: str,
    outcome: str = "blocked",
) -> dict[str, str]:
    return message.safe_labels(surface=surface, outcome=outcome)
