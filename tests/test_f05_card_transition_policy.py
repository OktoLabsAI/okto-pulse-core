from __future__ import annotations

import pytest

from okto_pulse.core.domain.card_transition import (
    CardTransitionFacts,
    PendingScenario,
    evaluate_card_transition,
)
from okto_pulse.core.domain.enums import CardStatus, CardType, SpecStatus, SprintStatus


def _facts(**overrides) -> CardTransitionFacts:  # noqa: ANN003
    values = {
        "card_id": "card-1",
        "old_status": CardStatus.NOT_STARTED,
        "new_status": CardStatus.IN_PROGRESS,
        "spec_id": "spec-1",
        "spec_title": "Spec",
        "spec_status": SpecStatus.IN_PROGRESS,
    }
    values.update(overrides)
    return CardTransitionFacts(**values)


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"archived": True}, "card_archived"),
        ({"spec_status": SpecStatus.VALIDATED}, "spec_status_too_early"),
        ({"sprint_count": 1, "sprint_id": None}, "sprint_required"),
        (
            {"sprint_count": 1, "sprint_id": "s", "sprint_exists": False},
            "sprint_not_found",
        ),
        (
            {
                "sprint_count": 1,
                "sprint_id": "s",
                "sprint_status": SprintStatus.DRAFT,
            },
            "sprint_not_active",
        ),
        (
            {
                "old_status": CardStatus.IN_PROGRESS,
                "new_status": CardStatus.DONE,
                "validation_required": True,
            },
            "task_validation_required",
        ),
        (
            {
                "card_type": CardType.TEST,
                "old_status": CardStatus.IN_PROGRESS,
                "new_status": CardStatus.DONE,
                "pending_scenarios": (PendingScenario("ts-1", "TS", "draft"),),
            },
            "test_scenarios_pending",
        ),
        (
            {"card_type": CardType.BUG, "has_regression_test_evidence": False},
            "missing_regression_test_task",
        ),
    ],
)
def test_f05_card_transition_blocks_are_storage_neutral(
    overrides: dict, code: str
) -> None:
    decision = evaluate_card_transition(_facts(**overrides))
    assert decision.allowed is False
    assert decision.block is not None and decision.block.code == code


def test_f05_test_card_can_start_on_validated_spec() -> None:
    decision = evaluate_card_transition(
        _facts(card_type=CardType.TEST, spec_status=SpecStatus.VALIDATED)
    )
    assert decision.allowed is True


def test_f05_bug_gate_respects_board_severity_threshold() -> None:
    decision = evaluate_card_transition(
        _facts(
            card_type=CardType.BUG,
            severity="minor",
            bug_test_gate_min_severity="major",
        )
    )
    assert decision.allowed is True
