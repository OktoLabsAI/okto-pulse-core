"""AC-VER-07/08/09/10 and ADV-03/14: planning is complete before presentation."""

from copy import deepcopy

import pytest

from okto_pulse.core.domain.verification_plan import resolve_verification_plan
from okto_pulse.core.application.use_cases.requirement_verification import (
    GetRequirementVerificationCommand,
    project_requirement_verification,
)
from test_requirement_verification import population, resolve, inherited


def scenario(**changes):
    return {
        "id": "ts",
        "scenario_type": "manual",
        "status": "ready",
        "given": "Five failed attempts",
        "when": "Access requested",
        "then": "Access blocked",
        "verification_method": "automated_test",
        "linked_criteria": ["ac-login", "ac-lock"],
        **changes,
    }


def card(**changes):
    return {
        "id": "test-card",
        "board_id": "board",
        "spec_id": "spec",
        "card_type": "test",
        "status": "not_started",
        "archived": False,
        "test_scenario_ids": ["ts"],
        **changes,
    }


def plan(data=None, **changes):
    data = population() if data is None else data
    return resolve_verification_plan(
        qualification=resolve(data),
        board_id="board",
        spec_id="spec",
        **{
            "scenarios": [scenario()],
            "cards": [card()],
            "criteria": data["acceptance_criteria"],
            "admitted_methods": frozenset({"automated_test"}),
            **changes,
        },
    )


def test_complete_plan_needs_no_execution_and_preserves_canonical_inputs():
    data = population()
    before = deepcopy(data)
    result = plan(data)
    assert result["method_plan_complete"] and result["verification_work_complete"]
    assert not any(
        result[key]
        for key in (
            "execution_evaluated",
            "delivery_evaluated",
            "semantic_review_evaluated",
            "rollout_evaluated",
        )
    )
    assert data == before
    assert {p["criterion_id"] for p in result["requirements"][0]["criteria_paths"]} == {
        "ac-login",
        "ac-lock",
    }


@pytest.mark.parametrize(
    "method",
    [
        None,
        "inspection",
        "static_analysis",
        "demonstration",
        "invalid",
        {"trusted": True},
    ],
)
def test_unsupported_or_absent_method_cannot_hide_behind_supported_scenario(method):
    result = plan(
        scenarios=[scenario(), scenario(id="second", verification_method=method)]
    )
    assert (
        not result["method_plan_complete"] and not result["verification_work_complete"]
    )


@pytest.mark.parametrize("capability", [None, frozenset(), frozenset({"inspection"})])
def test_unknown_or_missing_concrete_capability_never_grants_readiness(capability):
    assert not plan(admitted_methods=capability)["method_plan_complete"]


@pytest.mark.parametrize(
    "change",
    [
        {"status": "cancelled"},
        {"archived": True},
        {"card_type": "task"},
        {"test_scenario_ids": []},
        {"spec_id": "other"},
        {"board_id": "other"},
    ],
)
def test_only_live_same_scope_test_card_can_own_verification(change):
    assert not plan(cards=[card(**change)])["verification_work_complete"]


def test_removing_last_card_loses_work_readiness_without_losing_method():
    result = plan(cards=[])
    assert result["method_plan_complete"] and not result["verification_work_complete"]
    assert result["requirements"][0]["criteria_paths"][0]["scenario_plans"][0][
        "work_blockers"
    ] == ["verification_test_card_required"]


@pytest.mark.parametrize(
    "changes",
    [
        {"given": ""},
        {"when": " "},
        {"then": None},
        {"scenario_type": "technical"},
        {"status": []},
        {"linked_criteria": ["0"]},
    ],
)
def test_no_condition_type_state_or_identity_fallback(changes):
    assert not plan(scenarios=[scenario(**changes)])["method_plan_complete"]


def test_selected_inheritance_reuses_work_without_importing_unselected_criteria():
    data = population()
    data["business_rules"] = [
        {
            "id": "br",
            "rule": "Block after five attempts",
            "verification": inherited(data["functional_requirements"][0]),
        }
    ]
    result = plan(data, scenarios=[scenario(linked_criteria=["ac-lock"])])
    by_id = {row["requirement_id"]: row for row in result["requirements"]}
    assert by_id["br"]["verification_work_complete"]
    assert not by_id["fr-auth"]["verification_work_complete"]
    assert not result["verification_work_complete"]
    first = project_requirement_verification(
        result, GetRequirementVerificationCommand("board", "spec", limit=1)
    )
    assert first["items"][0]["requirement_id"] == "br"
    assert not first["verification_work_complete"]  # Pending lives beyond this page.


@pytest.mark.parametrize("field", ["scenarios", "cards", "criteria"])
def test_absent_oversized_or_duplicate_population_is_not_known_zero(field):
    for value in (None, [{}] * 5001):
        result = plan(**{field: value})
        assert not result["planning_population_complete"]
        assert not result["method_plan_complete"]
    if field == "scenarios":
        assert not plan(scenarios=[scenario(), scenario()])[
            "planning_population_complete"
        ]


def test_display_truncation_preserves_whole_population_verdict():
    scenarios = [scenario(id=f"ts-{i:02d}") for i in range(22)]
    cards = [card(test_scenario_ids=[s["id"] for s in scenarios])]
    scenarios[-1]["verification_method"] = "inspection"
    result = project_requirement_verification(
        plan(scenarios=scenarios, cards=cards),
        GetRequirementVerificationCommand("board", "spec"),
    )
    path = result["items"][0]["criteria_paths"][0]
    assert len(path["scenario_plans"]) == 20 and path["scenarios_truncated"]
    assert path["scenario_count"] == 22
    assert all(item["method_admitted"] for item in path["scenario_plans"])
    assert not result["method_plan_complete"]


@pytest.mark.parametrize("status", ["revoked", "superseded", "cancelled", "deprecated"])
def test_retired_criterion_history_does_not_add_work_to_active_plan(status):
    data = population()
    data["acceptance_criteria"].append(
        {"id": "old-ac", "text": "Old scope", "status": status}
    )
    before = deepcopy(data)
    result = plan(
        data,
        scenarios=[
            scenario(),
            scenario(
                id="old", linked_criteria=["old-ac"], verification_method="inspection"
            ),
        ],
    )
    assert result["method_plan_complete"] and result["verification_work_complete"]
    assert data == before
