"""Canonical bug workflow remediation guidance.

The builder in this module is intentionally pure. It formats already-computed
bug regression eligibility and amendment lineage facts into one bounded contract for
MCP, REST, UI, documentation, audit, and metrics surfaces.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping

from okto_pulse.core.services.bug_regression_scenarios import (
    BugRegressionCoverageState,
    BugRegressionScenarioEligibilityResult,
)


class BugWorkflowRemediationPath(str, Enum):
    """Canonical operator path for a blocked bug workflow."""

    PATH_A_REUSE_SCENARIO = "path_a_reuse_existing_scenario"
    PATH_B_SEMANTIC_GAP = "path_b_semantic_gap"
    PATH_B_AMENDMENT_LINEAGE = "path_b_amendment_lineage"
    NONE = "none"


class BugWorkflowNextAction(str, Enum):
    """Bounded next action vocabulary shared by REST, MCP, UI and telemetry."""

    CREATE_REGRESSION_TEST_CARD = "create_regression_test_card"
    ESCALATE_SEMANTIC_GAP = "escalate_semantic_gap"
    CONFIRM_VALIDATOR_COVERAGE = "confirm_validator_coverage"
    NONE = "none"




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

        if result.coverage_state in {
            BugRegressionCoverageState.COVERAGE_PENDING,
            BugRegressionCoverageState.PATH_B_READY,
        }:
            return self._path_b_amendment_message(
                result,
                reason_code=reason_code or self._primary_eligible_reason(result),
            )

        return BugWorkflowRemediationMessage(
            reason_code=reason_code or self._primary_eligible_reason(result),
            remediation_path=BugWorkflowRemediationPath.PATH_A_REUSE_SCENARIO,
            next_action=BugWorkflowNextAction.CREATE_REGRESSION_TEST_CARD,
            semantic_gap_required=False,
            eligible_scenarios_count=len(result.eligible_scenarios),
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
    ) -> BugWorkflowRemediationMessage:
        eligible_count = max(0, int(eligible_scenarios_count))
        if eligible_count == 0:
            # A missing test task is only remediable through Path A when the
            # canonical lineage resolver found a scenario that can actually be
            # reused.  Routing an empty eligible set to "create test card"
            # creates an orphan artifact that the deep gate rejects on retry.
            return self._semantic_gap_message(
                reason_code="missing_regression_test_task",
                eligible_count=0,
            )

        return BugWorkflowRemediationMessage(
            reason_code="missing_regression_test_task",
            remediation_path=BugWorkflowRemediationPath.PATH_A_REUSE_SCENARIO,
            next_action=BugWorkflowNextAction.CREATE_REGRESSION_TEST_CARD,
            semantic_gap_required=False,
            eligible_scenarios_count=eligible_count,
            message=(
                "Bug card requires at least one linked regression test card "
                "before it can move to in_progress."
            ),
            detail=(
                "Use Path A: create a fresh test card that references an "
                "eligible existing scenario from the bug spec, link it to this "
                "bug, then retry the move."
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

    def _path_b_amendment_message(
        self,
        result: BugRegressionScenarioEligibilityResult,
        *,
        reason_code: str,
    ) -> BugWorkflowRemediationMessage:
        if result.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING:
            return BugWorkflowRemediationMessage(
                reason_code=BugRegressionCoverageState.COVERAGE_PENDING.value,
                remediation_path=BugWorkflowRemediationPath.PATH_B_AMENDMENT_LINEAGE,
                next_action=BugWorkflowNextAction.CONFIRM_VALIDATOR_COVERAGE,
                semantic_gap_required=False,
                eligible_scenarios_count=len(result.eligible_scenarios),
                message=(
                    "Path B amendment lineage is eligible, but validator coverage "
                    "has not been confirmed."
                ),
                detail=(
                    "Register re-executable evidence on the declared regression "
                    "scenario, then have the validator run "
                    "okto_pulse_confirm_amendment_coverage. Amendment promotion "
                    "alone never closes coverage."
                ),
                actions=(
                    BugWorkflowRemediationAction(
                        action_id=BugWorkflowNextAction.CONFIRM_VALIDATOR_COVERAGE.value,
                        label="Confirm validator coverage",
                        description=(
                            "Validate the declared regression artifact and persist "
                            "the bound amendment coverage attestation."
                        ),
                        primary=True,
                    ),
                ),
                facts=self._path_b_facts(result),
            )

        return BugWorkflowRemediationMessage(
            reason_code=reason_code,
            remediation_path=BugWorkflowRemediationPath.PATH_B_AMENDMENT_LINEAGE,
            next_action=BugWorkflowNextAction.NONE,
            semantic_gap_required=False,
            eligible_scenarios_count=len(result.eligible_scenarios),
            message="Path B amendment lineage has validator-confirmed coverage.",
            detail=(
                "The regression artifact is backed by a complete amendment lineage "
                "and the validator coverage attestation is present. Continue the "
                "bug workflow; no additional scenario-reuse action is required."
            ),
            actions=(),
            facts=self._path_b_facts(result),
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
    def _path_b_facts(
        result: BugRegressionScenarioEligibilityResult,
    ) -> dict[str, object]:
        facts: dict[str, object] = {
            "spec_id": result.spec_id,
            "bug_id": result.bug_id,
            "coverage_state": result.coverage_state.value,
            "coverage_pending_scenarios_count": len(result.coverage_pending_scenarios),
            "eligible_scenarios_count": len(result.eligible_scenarios),
            "rejected_scenarios_count": len(result.rejected_scenarios),
        }
        if result.amendment_revision_id:
            facts["amendment_revision_id"] = result.amendment_revision_id
        if result.amendment_status:
            facts["amendment_status"] = result.amendment_status
        if result.lineage_state:
            facts["lineage_state"] = result.lineage_state
        return facts



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
