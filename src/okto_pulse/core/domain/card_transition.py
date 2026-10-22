"""Pure admission policy for card state transitions."""

from __future__ import annotations

from dataclasses import dataclass, field

from okto_pulse.core.domain.enums import CardStatus, CardType, SpecStatus
from okto_pulse.core.domain.spec_dependency import (
    spec_dependency_blocked_guidance,
    spec_dependency_blocking_facts,
    transition_rechecks_card_prerequisites,
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
    """Block qualification as well as execution entry on current prerequisites."""

    if not facts.spec_id or not transition_rechecks_card_prerequisites(
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


def completed_spec_execution_block(
    facts: CardTransitionFacts,
) -> CardTransitionBlock | None:
    """Normal work cannot start or resume in a delivered Spec.

    F3 authority decision (2026-09-20): post-delivery Bug/Test work retains its
    own gates. Ordering, collaboration and non-execution edges are unaffected;
    an authorized Spec revision is required before normal execution can resume.
    """
    if not transition_starts_card_execution(facts.old_status, facts.new_status):
        return None
    return completed_spec_normal_work_block(
        card_type=facts.card_type, spec_status=facts.spec_status,
        card_id=facts.card_id, spec_id=facts.spec_id,
    )


def completed_spec_normal_work_block(
    *, card_type: CardType | str, spec_status: SpecStatus | str | None,
    card_id: str | None, spec_id: str | None,
) -> CardTransitionBlock | None:
    """Shared F3 policy for normal content writes and execution admission."""
    if card_type != CardType.NORMAL or spec_status != SpecStatus.DONE:
        return None
    return _blocked(
        "normal_card_spec_done",
        "Normal tasks cannot be created, edited, started or reopened while their Spec is Done. "
        "Use the authorized Spec revision workflow before resuming normal work.",
        remediation="revise_spec_before_normal_work",
        facts={"card_id": card_id, "spec_id": spec_id},
    ).block


def spec_maturity_block(facts: CardTransitionFacts) -> CardTransitionBlock | None:
    if not (facts.spec_id and facts.spec_status is not None
            and transition_rechecks_card_prerequisites(facts.old_status, facts.new_status)):
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
    bug_gate_applies = (
        facts.card_type is CardType.BUG
        and facts.require_test_task_for_bug
        and _SEVERITY_ORDER.get(facts.severity, 1)
        >= _SEVERITY_ORDER.get(facts.bug_test_gate_min_severity, 1)
    )
    # A legacy Started Bug is migrated to In Progress without granting proof
    # that its old entry gate passed. Recheck regression evidence on every later
    # execution advance; pause, cancellation and same-column reorder stay usable.
    advances_execution = (
        facts.new_status in {
            CardStatus.IN_PROGRESS, CardStatus.VALIDATION, CardStatus.DONE,
        }
        and facts.new_status is not facts.old_status
    )
    return bug_gate_applies and advances_execution


def evaluate_card_transition(facts: CardTransitionFacts) -> CardTransitionDecision:
    for evaluator in (
        archived_card_block,
        completed_spec_execution_block,
        spec_dependency_block,
        spec_maturity_block,
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
    "completed_spec_execution_block",
    "completed_spec_normal_work_block",
    "evaluate_card_transition",
    "spec_dependency_block",
    "spec_maturity_block",
    "test_completion_block",
    "validation_gate_block",
]
