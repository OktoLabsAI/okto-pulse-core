from __future__ import annotations

import pytest

from okto_pulse.core.domain.card_transition import (
    CardTransitionFacts,
    PendingScenario,
    evaluate_card_transition,
)
from okto_pulse.core.domain.enums import CardStatus, CardType, SpecStatus


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


@pytest.mark.parametrize("old,target", [
    (CardStatus.NOT_STARTED, CardStatus.STARTED),
    (CardStatus.STARTED, CardStatus.IN_PROGRESS),
    (CardStatus.ON_HOLD, CardStatus.STARTED),
    (CardStatus.ON_HOLD, CardStatus.IN_PROGRESS),
    (CardStatus.REJECTED, CardStatus.IN_PROGRESS),
    (CardStatus.DONE, CardStatus.IN_PROGRESS),
])
@pytest.mark.parametrize("card_type", list(CardType))
def test_done_spec_normal_execution_gate_preserves_bug_and_test_controls(old, target, card_type):
    decision = evaluate_card_transition(_facts(
        old_status=old, new_status=target, card_type=card_type,
        spec_status=SpecStatus.DONE, has_regression_test_evidence=True,
    ))
    assert decision.allowed is (card_type != CardType.NORMAL)
    if card_type == CardType.NORMAL:
        assert decision.block.code == "normal_card_spec_done"


@pytest.mark.parametrize("old,target", [
    (CardStatus.DONE, CardStatus.DONE),
    (CardStatus.IN_PROGRESS, CardStatus.ON_HOLD),
    (CardStatus.NOT_STARTED, CardStatus.CANCELLED),
])
def test_done_spec_gate_does_not_block_ordering_pause_or_cancellation(old, target):
    assert evaluate_card_transition(_facts(
        old_status=old, new_status=target, spec_status=SpecStatus.DONE,
    )).allowed


def test_f05_bug_gate_respects_board_severity_threshold() -> None:
    decision = evaluate_card_transition(
        _facts(
            card_type=CardType.BUG,
            severity="minor",
            bug_test_gate_min_severity="major",
        )
    )
    assert decision.allowed is True


def test_f05_cancellation_does_not_start_execution() -> None:
    decision = evaluate_card_transition(
        _facts(
            new_status=CardStatus.CANCELLED,
            spec_status=SpecStatus.APPROVED,
            card_type=CardType.BUG,
            has_regression_test_evidence=False,
        )
    )
    assert decision.allowed is True


def test_normal_execution_needs_no_sprint_facts():
    assert evaluate_card_transition(_facts()).allowed
    assert not any("sprint" in name or "hotfix" in name for name in CardTransitionFacts.__dataclass_fields__)
