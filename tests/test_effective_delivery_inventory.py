from copy import deepcopy
from types import SimpleNamespace
import pytest

from test_implementation_responsibility import (
    planned_population,
    split_population,
    card,
)
from test_requirement_verification import resolve
from okto_pulse.core.ports.delivery_inventory import default_delivery_inventory_policy


def inventory(data=None, cards=None):
    data = deepcopy(data if data is not None else planned_population())
    data.setdefault("api_contracts", [])
    data.setdefault("decisions", [])
    spec = SimpleNamespace(
        id="spec",
        board_id="board",
        title="Spec",
        description="Contract",
        context="Context",
        **data,
    )
    cards = cards if cards is not None else [card()]
    cards = [
        {"title": "Task", "description": "Implement", "details": "Details", **item}
        for item in cards
    ]
    return default_delivery_inventory_policy().effective_inventory(
        spec=spec, cards=cards, qualification=resolve(data)
    ), spec


def test_supplemental_obligations_do_not_disappear_from_qualified_plan():
    data = planned_population()
    data["api_contracts"] = [
        {"id": "api", "title": "Endpoint", "linked_task_ids": ["owner"]}
    ]
    data["decisions"] = [{"id": "decision", "title": "Decision"}]
    result, _ = inventory(data)
    assert result.population_complete and result.responsibilities.complete
    assert {row.binding.obligation_ref for row in result.rows} == {
        "fr:fr-auth",
        "ac:ac-login",
        "ac:ac-lock",
        "api:api",
        "decision:decision",
    }
    assert not result.complete
    assert any(
        row.binding.obligation_ref == "decision:decision"
        and "delivery_inventory_work_unassigned" in row.blockers
        for row in result.rows
    )
    assert {row.binding.obligation_ref for row in result.card_obligations("owner")} == {
        "fr:fr-auth",
        "api:api",
    }


def test_inherited_rule_shares_same_inventory_and_exact_card_allocation():
    result, _ = inventory(split_population(), [card("ui"), card("authorization")])
    rule = next(row for row in result.rows if row.binding.obligation_ref == "br:br")
    assert [fact.card_id for fact in rule.contributions] == ["authorization"]
    assert rule.contributions[0].origin == "inherited"
    assert "br:br" not in {
        row.binding.obligation_ref for row in result.card_obligations("ui")
    }


def test_supplemental_multi_card_link_does_not_invent_a_split():
    data = planned_population()
    data["api_contracts"] = [{"id": "api", "linked_task_ids": ["owner", "other"]}]
    result, _ = inventory(data, [card(), card("other")])
    row = next(row for row in result.rows if row.family == "api")
    assert (
        not row.contributions
        and "delivery_inventory_allocation_unresolved" in row.blockers
    )
    assert not any(row.binding.obligation_ref == "card:other" for row in result.rows)
    assert any(
        row.family == "api" and row.blockers for row in result.card_obligations("other")
    )


def test_fallback_definition_binds_normative_details_without_rewriting_legacy():
    before, spec = inventory(cards=[card(), card("loose", details="Before")])
    after, _ = inventory(cards=[card(), card("loose", details="After")])
    old = next(row for row in before.rows if row.binding.obligation_ref == "card:loose")
    new = next(row for row in after.rows if row.binding.obligation_ref == "card:loose")
    assert old.binding != new.binding
    policy = default_delivery_inventory_policy()
    legacy_before = policy.card_obligations(
        spec, SimpleNamespace(id="loose", title="Task", details="Before")
    )
    legacy_after = policy.card_obligations(
        spec, SimpleNamespace(id="loose", title="Task", details="After")
    )
    assert legacy_before == legacy_after


def test_unrelated_allocation_does_not_change_requirement_definition_or_other_scope():
    data = split_population()
    before, _ = inventory(data, [card("ui"), card("authorization")])
    data["functional_requirements"][0]["implementation_plan"]["contributions"][0][
        "summary"
    ] = "New UI scope"
    after, _ = inventory(data, [card("ui"), card("authorization")])
    a = next(row for row in before.rows if row.family == "fr")
    b = next(row for row in after.rows if row.family == "fr")
    assert a.binding == b.binding
    assert next(f for f in a.contributions if f.card_id == "authorization") == next(
        f for f in b.contributions if f.card_id == "authorization"
    )
    assert before.snapshot_sha256 != after.snapshot_sha256


def test_missing_supplemental_population_is_unknown_not_empty():
    data = planned_population()
    data["api_contracts"] = None
    result, _ = inventory(data)
    assert not result.population_complete and not result.complete
    assert "delivery_inventory_population_unavailable" in result.issues
    with pytest.raises(ValueError, match="population_unavailable"):
        result.card_obligations("owner")


def test_all_direct_allocations_allow_full_planning_without_delivery_credit():
    data = planned_population()
    for criterion in data["acceptance_criteria"]:
        criterion["linked_task_ids"] = ["owner"]
    result, _ = inventory(data)
    assert result.complete and len(result.rows) == 3
    assert len(result.snapshot_sha256) == 64


def test_large_or_ambiguous_population_never_has_a_complete_empty_result():
    data = planned_population()
    data["decisions"] = [{"id": str(i)} for i in range(5001)]
    result, _ = inventory(data)
    assert not result.population_complete and not result.complete
    data["decisions"] = [{"id": "duplicate"}, {"id": "duplicate"}]
    result, _ = inventory(data)
    assert (
        not result.population_complete
        and "delivery_inventory_population_invalid" in result.issues
    )
