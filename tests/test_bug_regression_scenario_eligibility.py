"""Tests for bug regression scenario eligibility resolution."""

from __future__ import annotations

from copy import deepcopy

from sqlalchemy_test_models import Card, CardStatus, CardType, Spec
from okto_pulse.core.services.bug_regression_scenarios import (
    BugRegressionEligibilityReason,
    BugRegressionNextAction,
    BugRegressionRejectionReason,
    BugRegressionScenarioEligibilityResolver,
)


def _spec() -> Spec:
    return Spec(
        id="spec-1",
        board_id="board-1",
        title="Locked spec",
        created_by="agent",
        test_scenarios=[
            {
                "id": "ts-origin-direct",
                "title": "Origin direct regression",
                "status": "passed",
            },
            {
                "id": "ts-origin-linked",
                "title": "Origin linked regression",
                "status": "passed",
                "linked_task_ids": ["origin-1"],
            },
            {
                "id": "ts-affected-direct",
                "title": "Affected direct regression",
                "status": "passed",
            },
            {
                "id": "ts-affected-linked",
                "title": "Affected linked regression",
                "status": "passed",
                "linked_task_ids": ["affected-1"],
            },
            {
                "id": "ts-title-similar",
                "title": "Regression for the same looking title",
                "status": "passed",
            },
            {
                "id": "ts-deleted",
                "title": "Deleted regression",
                "status": "deleted",
                "linked_task_ids": ["origin-1"],
            },
        ],
    )


def _card(
    card_id: str,
    *,
    spec_id: str = "spec-1",
    card_type: CardType = CardType.NORMAL,
    test_scenario_ids: list[str] | None = None,
) -> Card:
    return Card(
        id=card_id,
        board_id="board-1",
        spec_id=spec_id,
        title=card_id,
        status=CardStatus.DONE,
        card_type=card_type,
        created_by="agent",
        test_scenario_ids=test_scenario_ids,
    )


def test_resolves_origin_and_affected_lineage_without_candidates():
    spec = _spec()
    origin = _card("origin-1", test_scenario_ids=["ts-origin-direct"])
    affected = _card("affected-1", test_scenario_ids=["ts-affected-direct"])
    bug = _card("bug-1", card_type=CardType.BUG)

    result = BugRegressionScenarioEligibilityResolver().resolve(
        bug_card=bug,
        spec=spec,
        origin_task=origin,
        affected_tasks=[affected],
    )

    assert result.semantic_gap_required is False
    assert result.spec_mutation_required is False
    assert result.next_action == BugRegressionNextAction.CREATE_REGRESSION_TEST_CARD
    assert [(item.scenario_id, item.reason, item.source_task_id) for item in result.eligible_scenarios] == [
        (
            "ts-origin-direct",
            BugRegressionEligibilityReason.ORIGIN_TASK_DIRECT,
            "origin-1",
        ),
        (
            "ts-origin-linked",
            BugRegressionEligibilityReason.ORIGIN_TASK_LINKED_SCENARIO,
            "origin-1",
        ),
        (
            "ts-affected-direct",
            BugRegressionEligibilityReason.AFFECTED_TASK_DIRECT,
            "affected-1",
        ),
        (
            "ts-affected-linked",
            BugRegressionEligibilityReason.AFFECTED_TASK_LINKED_SCENARIO,
            "affected-1",
        ),
    ]


def test_classifies_mixed_candidates_with_bounded_reasons():
    spec = _spec()
    origin = _card("origin-1", test_scenario_ids=["ts-origin-direct"])
    bug = _card("bug-1", card_type=CardType.BUG)

    result = BugRegressionScenarioEligibilityResolver().resolve(
        bug_card=bug,
        spec=spec,
        origin_task=origin,
        candidate_scenario_ids=[
            "ts-origin-linked",
            "ts-title-similar",
            "ts-missing",
            "ts-foreign",
            "ts-deleted",
        ],
        candidate_spec_ids_by_scenario_id={"ts-foreign": "other-spec"},
    )

    assert [item.scenario_id for item in result.eligible_scenarios] == ["ts-origin-linked"]
    assert result.eligible_scenarios[0].reason == (
        BugRegressionEligibilityReason.ORIGIN_TASK_LINKED_SCENARIO
    )
    assert [(item.scenario_id, item.reason) for item in result.rejected_scenarios] == [
        ("ts-title-similar", BugRegressionRejectionReason.UNRELATED_SCENARIO),
        ("ts-missing", BugRegressionRejectionReason.SCENARIO_NOT_FOUND),
        ("ts-foreign", BugRegressionRejectionReason.CROSS_SPEC_SCENARIO),
        ("ts-deleted", BugRegressionRejectionReason.DELETED_SCENARIO),
    ]


def test_unrelated_same_spec_candidate_requires_semantic_gap_not_title_matching():
    spec = _spec()
    origin = _card("origin-1", test_scenario_ids=[])
    bug = _card("bug-1", card_type=CardType.BUG)

    result = BugRegressionScenarioEligibilityResolver().resolve(
        bug_card=bug,
        spec=spec,
        origin_task=origin,
        candidate_scenario_ids=["ts-title-similar"],
    )

    assert result.eligible_scenarios == ()
    assert result.rejected_scenarios[0].reason == (
        BugRegressionRejectionReason.UNRELATED_SCENARIO
    )
    assert result.semantic_gap_required is True
    assert result.spec_mutation_required is True
    assert result.next_action == BugRegressionNextAction.ESCALATE_SEMANTIC_GAP


def test_resolver_does_not_mutate_canonical_spec_or_cards():
    spec = _spec()
    origin = _card("origin-1", test_scenario_ids=["ts-origin-direct"])
    bug = _card("bug-1", card_type=CardType.BUG)
    before_scenarios = deepcopy(spec.test_scenarios)
    before_origin_scenarios = list(origin.test_scenario_ids or [])

    BugRegressionScenarioEligibilityResolver().resolve(
        bug_card=bug,
        spec=spec,
        origin_task=origin,
        candidate_scenario_ids=["ts-origin-direct", "ts-title-similar"],
    )

    assert spec.test_scenarios == before_scenarios
    assert origin.test_scenario_ids == before_origin_scenarios
