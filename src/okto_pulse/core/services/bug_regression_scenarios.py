"""Bug regression scenario eligibility resolution.

This module is intentionally pure: it classifies already-loaded spec/card
objects and never mutates the canonical spec or writes audit records.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Mapping, Sequence

from okto_pulse.core.models.db import Card, Spec


class BugRegressionEligibilityReason(str, Enum):
    """Why a scenario is eligible for a bug regression test card."""

    ORIGIN_TASK_DIRECT = "origin_task_direct"
    ORIGIN_TASK_LINKED_SCENARIO = "origin_task_linked_scenario"
    AFFECTED_TASK_DIRECT = "affected_task_direct"
    AFFECTED_TASK_LINKED_SCENARIO = "affected_task_linked_scenario"


class BugRegressionRejectionReason(str, Enum):
    """Why a candidate scenario cannot satisfy the bug regression gate."""

    SCENARIO_NOT_FOUND = "scenario_not_found"
    CROSS_SPEC_SCENARIO = "cross_spec_scenario"
    DELETED_SCENARIO = "deleted_scenario"
    UNRELATED_SCENARIO = "unrelated_scenario"


class BugRegressionNextAction(str, Enum):
    """Deterministic next action for the caller."""

    CREATE_REGRESSION_TEST_CARD = "create_regression_test_card"
    ESCALATE_SEMANTIC_GAP = "escalate_semantic_gap"


class BugRegressionGateDecision(str, Enum):
    """Bug linked-test-task gate decision."""

    ALLOW = "allow"
    BLOCK_UNRELATED_SCENARIO = "block_unrelated_scenario"
    BLOCK_SEMANTIC_GAP = "block_semantic_gap"


@dataclass(frozen=True)
class EligibleBugRegressionScenario:
    """Eligible scenario candidate with lineage proof."""

    scenario_id: str
    title: str | None
    reason: BugRegressionEligibilityReason
    source_task_id: str


@dataclass(frozen=True)
class RejectedBugRegressionScenario:
    """Rejected scenario candidate with bounded reason vocabulary."""

    scenario_id: str
    reason: BugRegressionRejectionReason
    detail: str | None = None


@dataclass(frozen=True)
class BugRegressionScenarioEligibilityResult:
    """Complete resolver outcome."""

    bug_id: str
    spec_id: str
    eligible_scenarios: tuple[EligibleBugRegressionScenario, ...]
    rejected_scenarios: tuple[RejectedBugRegressionScenario, ...]
    semantic_gap_required: bool
    spec_mutation_required: bool
    next_action: BugRegressionNextAction


@dataclass(frozen=True)
class BugRegressionGateValidationResult:
    """Eligibility-aware result for the bug regression test gate."""

    allowed: bool
    decision: BugRegressionGateDecision
    eligibility: BugRegressionScenarioEligibilityResult


class BugRegressionScenarioEligibilityResolver:
    """Resolve reusable regression scenarios from explicit task lineage only."""

    def resolve(
        self,
        *,
        bug_card: Card,
        spec: Spec,
        origin_task: Card | None,
        affected_tasks: Sequence[Card] | None = None,
        candidate_scenario_ids: Sequence[str] | None = None,
        candidate_spec_ids_by_scenario_id: Mapping[str, str] | None = None,
    ) -> BugRegressionScenarioEligibilityResult:
        """Classify candidate scenarios without database access or side effects."""

        scenarios_by_id = {
            str(scenario.get("id")): scenario
            for scenario in (spec.test_scenarios or [])
            if isinstance(scenario, dict) and scenario.get("id") is not None
        }
        candidate_ids = self._ordered_unique(
            candidate_scenario_ids
            if candidate_scenario_ids is not None
            else self._lineage_candidate_ids(spec, origin_task, affected_tasks)
        )

        lineage_reasons = self._lineage_reasons(spec, origin_task, affected_tasks)
        eligible: list[EligibleBugRegressionScenario] = []
        rejected: list[RejectedBugRegressionScenario] = []

        for scenario_id in candidate_ids:
            scenario = scenarios_by_id.get(scenario_id)
            if scenario is None:
                mapped_spec_id = (candidate_spec_ids_by_scenario_id or {}).get(scenario_id)
                if mapped_spec_id and mapped_spec_id != spec.id:
                    rejected.append(
                        RejectedBugRegressionScenario(
                            scenario_id=scenario_id,
                            reason=BugRegressionRejectionReason.CROSS_SPEC_SCENARIO,
                            detail=mapped_spec_id,
                        )
                    )
                else:
                    rejected.append(
                        RejectedBugRegressionScenario(
                            scenario_id=scenario_id,
                            reason=BugRegressionRejectionReason.SCENARIO_NOT_FOUND,
                        )
                    )
                continue

            if str(scenario.get("status") or "").lower() == "deleted":
                rejected.append(
                    RejectedBugRegressionScenario(
                        scenario_id=scenario_id,
                        reason=BugRegressionRejectionReason.DELETED_SCENARIO,
                    )
                )
                continue

            reason_and_task = lineage_reasons.get(scenario_id)
            if reason_and_task:
                reason, task_id = reason_and_task
                eligible.append(
                    EligibleBugRegressionScenario(
                        scenario_id=scenario_id,
                        title=scenario.get("title"),
                        reason=reason,
                        source_task_id=task_id,
                    )
                )
                continue

            rejected.append(
                RejectedBugRegressionScenario(
                    scenario_id=scenario_id,
                    reason=BugRegressionRejectionReason.UNRELATED_SCENARIO,
                )
            )

        semantic_gap_required = not eligible
        return BugRegressionScenarioEligibilityResult(
            bug_id=bug_card.id,
            spec_id=spec.id,
            eligible_scenarios=tuple(eligible),
            rejected_scenarios=tuple(rejected),
            semantic_gap_required=semantic_gap_required,
            spec_mutation_required=semantic_gap_required,
            next_action=(
                BugRegressionNextAction.ESCALATE_SEMANTIC_GAP
                if semantic_gap_required
                else BugRegressionNextAction.CREATE_REGRESSION_TEST_CARD
            ),
        )

    @staticmethod
    def _ordered_unique(values: Sequence[str] | None) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for value in values or []:
            scenario_id = str(value)
            if scenario_id not in seen:
                seen.add(scenario_id)
                ordered.append(scenario_id)
        return ordered

    def _lineage_candidate_ids(
        self,
        spec: Spec,
        origin_task: Card | None,
        affected_tasks: Sequence[Card] | None,
    ) -> list[str]:
        candidates: list[str] = []
        for task in self._tasks_in_priority_order(origin_task, affected_tasks):
            candidates.extend(task.test_scenario_ids or [])
            candidates.extend(self._scenario_ids_linked_to_task(spec, task))
        return candidates

    def _lineage_reasons(
        self,
        spec: Spec,
        origin_task: Card | None,
        affected_tasks: Sequence[Card] | None,
    ) -> dict[str, tuple[BugRegressionEligibilityReason, str]]:
        reasons: dict[str, tuple[BugRegressionEligibilityReason, str]] = {}

        if origin_task:
            for scenario_id in origin_task.test_scenario_ids or []:
                reasons.setdefault(
                    str(scenario_id),
                    (
                        BugRegressionEligibilityReason.ORIGIN_TASK_DIRECT,
                        origin_task.id,
                    ),
                )

        for task in affected_tasks or []:
            for scenario_id in task.test_scenario_ids or []:
                reasons.setdefault(
                    str(scenario_id),
                    (
                        BugRegressionEligibilityReason.AFFECTED_TASK_DIRECT,
                        task.id,
                    ),
                )

        # linked_task_ids on the canonical scenario are secondary proof. Direct
        # card linkage wins when both are present.
        for task, reason in self._linked_task_reason_order(origin_task, affected_tasks):
            for scenario_id in self._scenario_ids_linked_to_task(spec, task):
                reasons.setdefault(str(scenario_id), (reason, task.id))

        return reasons

    @staticmethod
    def _tasks_in_priority_order(
        origin_task: Card | None,
        affected_tasks: Sequence[Card] | None,
    ) -> list[Card]:
        tasks: list[Card] = []
        if origin_task:
            tasks.append(origin_task)
        tasks.extend(affected_tasks or [])
        return tasks

    @staticmethod
    def _linked_task_reason_order(
        origin_task: Card | None,
        affected_tasks: Sequence[Card] | None,
    ) -> list[tuple[Card, BugRegressionEligibilityReason]]:
        tasks: list[tuple[Card, BugRegressionEligibilityReason]] = []
        if origin_task:
            tasks.append(
                (
                    origin_task,
                    BugRegressionEligibilityReason.ORIGIN_TASK_LINKED_SCENARIO,
                )
            )
        for task in affected_tasks or []:
            tasks.append(
                (
                    task,
                    BugRegressionEligibilityReason.AFFECTED_TASK_LINKED_SCENARIO,
                )
            )
        return tasks

    @staticmethod
    def _scenario_ids_linked_to_task(spec: Spec, task: Card) -> list[str]:
        scenario_ids: list[str] = []
        for scenario in spec.test_scenarios or []:
            if not isinstance(scenario, dict):
                continue
            if task.id in (scenario.get("linked_task_ids") or []):
                scenario_id = scenario.get("id")
                if scenario_id is not None:
                    scenario_ids.append(str(scenario_id))
        return scenario_ids


class BugRegressionGateValidator:
    """Validate linked bug test tasks against scenario lineage eligibility."""

    def __init__(
        self,
        resolver: BugRegressionScenarioEligibilityResolver | None = None,
    ) -> None:
        self._resolver = resolver or BugRegressionScenarioEligibilityResolver()

    def validate_linked_test_tasks(
        self,
        *,
        bug_card: Card,
        linked_test_tasks: Sequence[Card],
        spec: Spec,
        origin_task: Card | None,
        affected_tasks: Sequence[Card] | None = None,
        candidate_spec_ids_by_scenario_id: Mapping[str, str] | None = None,
    ) -> BugRegressionGateValidationResult:
        candidate_scenario_ids = self._ordered_unique(
            scenario_id
            for task in linked_test_tasks
            for scenario_id in (task.test_scenario_ids or [])
        )
        eligibility = self._resolver.resolve(
            bug_card=bug_card,
            spec=spec,
            origin_task=origin_task,
            affected_tasks=affected_tasks,
            candidate_scenario_ids=candidate_scenario_ids,
            candidate_spec_ids_by_scenario_id=candidate_spec_ids_by_scenario_id,
        )

        if eligibility.eligible_scenarios and not eligibility.rejected_scenarios:
            return BugRegressionGateValidationResult(
                allowed=True,
                decision=BugRegressionGateDecision.ALLOW,
                eligibility=eligibility,
            )

        return BugRegressionGateValidationResult(
            allowed=False,
            decision=(
                BugRegressionGateDecision.BLOCK_SEMANTIC_GAP
                if eligibility.semantic_gap_required
                else BugRegressionGateDecision.BLOCK_UNRELATED_SCENARIO
            ),
            eligibility=eligibility,
        )

    @staticmethod
    def _ordered_unique(values: Iterable[object]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for value in values or []:
            scenario_id = str(value)
            if scenario_id not in seen:
                seen.add(scenario_id)
                ordered.append(scenario_id)
        return ordered
