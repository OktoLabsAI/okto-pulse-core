"""Pure admission policy for card state transitions."""

from __future__ import annotations

from dataclasses import dataclass, field

from okto_pulse.core.domain.enums import CardStatus, CardType, SpecStatus, SprintStatus
from okto_pulse.core.domain.spec_dependency import (
    spec_dependency_blocked_guidance,
    spec_dependency_blocking_facts,
    transition_starts_card_execution,
)


CARD_STATUS_ORDER = {
    CardStatus.NOT_STARTED: 0,
    CardStatus.STARTED: 1,
    CardStatus.IN_PROGRESS: 2,
    CardStatus.VALIDATION: 2,
    CardStatus.REJECTED: 2,
    CardStatus.DONE: 3,
    CardStatus.ON_HOLD: 2,
    CardStatus.CANCELLED: 3,
}

SPEC_STATUS_ORDER = {
    SpecStatus.DRAFT: 0,
    SpecStatus.REVIEW: 1,
    SpecStatus.APPROVED: 2,
    SpecStatus.VALIDATED: 3,
    SpecStatus.IN_PROGRESS: 4,
    SpecStatus.DONE: 5,
    SpecStatus.CANCELLED: 5,
}

_SEVERITY_ORDER = {"minor": 1, "major": 2, "critical": 3}


@dataclass(frozen=True, slots=True)
class PendingScenario:
    scenario_id: str
    title: str
    status: str


@dataclass(frozen=True, slots=True)
class CardTransitionFacts:
    card_id: str
    old_status: CardStatus
    new_status: CardStatus
    card_type: CardType = CardType.NORMAL
    archived: bool = False
    spec_id: str | None = None
    spec_title: str | None = None
    spec_status: SpecStatus | None = None
    sprint_count: int = 0
    sprint_id: str | None = None
    sprint_exists: bool = True
    sprint_status: SprintStatus | None = None
    sprint_title: str | None = None
    sprint_is_hotfix: bool = False
    hotfix_count: int = 0
    validation_required: bool = False
    pending_scenarios: tuple[PendingScenario, ...] = ()
    require_test_task_for_bug: bool = True
    bug_test_gate_min_severity: str = "minor"
    severity: str = "minor"
    has_regression_test_evidence: bool = False
    spec_dependency_blockers: tuple[object, ...] = ()
    spec_dependency_blocking_count: int = 0
    spec_dependency_archived_blocking_count: int = 0
    spec_dependency_unfinished_blocking_count: int = 0
    spec_dependency_blockers_truncated: bool = False


@dataclass(frozen=True, slots=True)
class CardTransitionBlock:
    code: str
    detail: str
    remediation: str | None = None
    facts: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CardTransitionDecision:
    allowed: bool
    block: CardTransitionBlock | None = None


def _blocked(
    code: str,
    detail: str,
    *,
    remediation: str | None = None,
    facts: dict[str, object] | None = None,
) -> CardTransitionDecision:
    return CardTransitionDecision(
        False,
        CardTransitionBlock(code, detail, remediation, facts or {}),
    )


def archived_card_block(facts: CardTransitionFacts) -> CardTransitionBlock | None:
    if facts.archived:
        return _blocked(
            "card_archived",
            "This card is archived. Restore it first using restore_tree before making changes.",
            remediation="restore_tree",
        ).block
    return None


def spec_dependency_block(facts: CardTransitionFacts) -> CardTransitionBlock | None:
    """Block every exact Card execution-start/resume edge on prerequisites."""

    if not facts.spec_id or not transition_starts_card_execution(
        facts.old_status,
        facts.new_status,
    ):
        return None
    blockers = tuple(facts.spec_dependency_blockers)
    if facts.spec_dependency_blocking_count <= 0:
        return None
    detail, remediation = spec_dependency_blocked_guidance(
        archived_blocking_count=facts.spec_dependency_archived_blocking_count,
        unfinished_blocking_count=facts.spec_dependency_unfinished_blocking_count,
    )

    return _blocked(
        "spec_dependencies_incomplete",
        detail,
        remediation=remediation,
        facts=spec_dependency_blocking_facts(
            spec_id=facts.spec_id,
            blockers=blockers,
            blocking_count=facts.spec_dependency_blocking_count,
            archived_blocking_count=(
                facts.spec_dependency_archived_blocking_count
            ),
            unfinished_blocking_count=(
                facts.spec_dependency_unfinished_blocking_count
            ),
            blockers_truncated=facts.spec_dependency_blockers_truncated,
        ),
    ).block


def spec_maturity_block(facts: CardTransitionFacts) -> CardTransitionBlock | None:
    if facts.new_status is CardStatus.CANCELLED:
        return None
    old_level = CARD_STATUS_ORDER.get(facts.old_status, 0)
    new_level = CARD_STATUS_ORDER.get(facts.new_status, 0)
    moves_forward = new_level > old_level
    if not (moves_forward and facts.spec_id and facts.spec_status is not None):
        return None
    minimum = (
        SpecStatus.VALIDATED
        if facts.card_type is CardType.TEST
        else SpecStatus.IN_PROGRESS
    )
    if SPEC_STATUS_ORDER.get(facts.spec_status, 0) < SPEC_STATUS_ORDER[minimum]:
        return _blocked(
            "spec_status_too_early",
            f"Cannot move card forward: spec '{facts.spec_title or facts.spec_id}' "
            f"must be at least '{minimum.value}' (currently "
            f"'{facts.spec_status.value}'). Move the spec forward before "
            "starting work on its cards.",
            remediation="advance_spec",
        ).block
    return None


def sprint_assignment_block(facts: CardTransitionFacts) -> CardTransitionBlock | None:
    if facts.new_status is CardStatus.CANCELLED:
        return None
    old_level = CARD_STATUS_ORDER.get(facts.old_status, 0)
    new_level = CARD_STATUS_ORDER.get(facts.new_status, 0)
    moves_forward = new_level > old_level
    if not (moves_forward and facts.spec_id and facts.sprint_count > 0):
        return None

    post_closure_bug = facts.card_type is CardType.BUG and (
        facts.spec_status is SpecStatus.DONE or facts.hotfix_count > 0
    )
    assign_remediation = (
        "assign_hotfix_lane" if post_closure_bug else "assign_sprint"
    )
    if not facts.sprint_id:
        return _blocked(
            "sprint_required",
            "This spec uses sprints. Card must be assigned to a sprint before advancing. "
            "Use okto_pulse_update_card or assign_tasks_to_sprint to assign it.",
            remediation=assign_remediation,
            facts={"card_id": facts.card_id, "spec_id": facts.spec_id},
        ).block
    if not facts.sprint_exists:
        return _blocked(
            "sprint_not_found",
            "Card's assigned sprint no longer exists. Assign it to an active sprint before advancing.",
            remediation=assign_remediation,
            facts={"card_id": facts.card_id, "sprint_id": facts.sprint_id},
        ).block
    if facts.sprint_status is not SprintStatus.ACTIVE:
        remediation = (
            "activate_hotfix_lane"
            if facts.sprint_is_hotfix
            else ("assign_hotfix_lane" if post_closure_bug else "activate_sprint")
        )
        return _blocked(
            "sprint_not_active",
            f"Card's sprint '{facts.sprint_title or facts.sprint_id}' is not active "
            f"(status: '{facts.sprint_status.value if facts.sprint_status else 'unknown'}'). "
            "Only cards in active sprints can advance.",
            remediation=remediation,
            facts={"card_id": facts.card_id, "sprint_id": facts.sprint_id},
        ).block
    return None


def validation_gate_block(facts: CardTransitionFacts) -> CardTransitionBlock | None:
    if (
        facts.new_status is CardStatus.DONE
        and facts.old_status
        in (
            CardStatus.IN_PROGRESS,
            CardStatus.STARTED,
            CardStatus.NOT_STARTED,
            CardStatus.VALIDATION,
        )
        and facts.card_type is not CardType.TEST
        and facts.validation_required
    ):
        detail = (
            "Validation gate is active. A reviewer must submit a task validation "
            "before the card can move from 'validation' to 'done'. Use "
            "submit_task_validation; a successful review promotes the card to "
            "'done'."
            if facts.old_status is CardStatus.VALIDATION
            else (
                "Validation gate is active. Move card to 'validation' status first. "
                "A reviewer must submit a task validation before the card can move "
                "to 'done'. Use move_card(status='validation', conclusion=..., "
                "completeness=..., completeness_justification=..., drift=..., "
                "drift_justification=...) then submit_task_validation."
            )
        )
        return _blocked(
            "task_validation_required",
            detail,
            remediation="move_to_validation_then_submit_task_validation",
        ).block
    return None


def test_completion_block(facts: CardTransitionFacts) -> CardTransitionBlock | None:
    if facts.new_status is CardStatus.DONE and facts.pending_scenarios:
        return _blocked(
            "test_scenarios_pending",
            "Linked test scenarios must be passed, failed, or automated before the test card can finish.",
            remediation="update_test_scenario_status",
            facts={
                "pending_scenario_ids": [
                    scenario.scenario_id for scenario in facts.pending_scenarios
                ]
            },
        ).block
    return None


def bug_regression_evidence_block(
    facts: CardTransitionFacts,
) -> CardTransitionBlock | None:
    if bug_regression_gate_applies(facts) and not facts.has_regression_test_evidence:
        return _blocked(
            "missing_regression_test_task",
            "Bug card requires at least one new regression test task before execution.",
            remediation="create_regression_test_card",
            facts={"card_id": facts.card_id, "spec_id": facts.spec_id},
        ).block
    return None


def bug_regression_gate_applies(facts: CardTransitionFacts) -> bool:
    old_level = CARD_STATUS_ORDER.get(facts.old_status, 0)
    new_level = CARD_STATUS_ORDER.get(facts.new_status, 0)
    bug_gate_applies = (
        facts.card_type is CardType.BUG
        and facts.require_test_task_for_bug
        and _SEVERITY_ORDER.get(facts.severity, 1)
        >= _SEVERITY_ORDER.get(facts.bug_test_gate_min_severity, 1)
    )
    starts_execution = (
        facts.new_status is not CardStatus.CANCELLED
        and new_level >= CARD_STATUS_ORDER[CardStatus.IN_PROGRESS]
        and old_level < CARD_STATUS_ORDER[CardStatus.IN_PROGRESS]
    )
    return bug_gate_applies and starts_execution


def evaluate_card_transition(facts: CardTransitionFacts) -> CardTransitionDecision:
    for evaluator in (
        archived_card_block,
        spec_dependency_block,
        spec_maturity_block,
        sprint_assignment_block,
        validation_gate_block,
        test_completion_block,
        bug_regression_evidence_block,
    ):
        block = evaluator(facts)
        if block is not None:
            return CardTransitionDecision(False, block)

    return CardTransitionDecision(True)


__all__ = [
    "CARD_STATUS_ORDER",
    "SPEC_STATUS_ORDER",
    "CardTransitionBlock",
    "CardTransitionDecision",
    "CardTransitionFacts",
    "PendingScenario",
    "archived_card_block",
    "bug_regression_evidence_block",
    "bug_regression_gate_applies",
    "evaluate_card_transition",
    "spec_dependency_block",
    "spec_maturity_block",
    "sprint_assignment_block",
    "test_completion_block",
    "validation_gate_block",
]
