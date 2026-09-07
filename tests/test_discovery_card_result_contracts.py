"""Discovery cards report actual coverage and bounded, spec-local history."""

from itertools import permutations
from types import SimpleNamespace

import pytest

from okto_pulse.core.services import discovery_executor as executor


def selected_fr():
    return executor.ValidatedSpecChildSelector(
        param_name="fr_id", spec_id="s1", spec_title="Spec one",
        child_type="functional_requirement", child_id="fr1", child_index=0,
        child_ref="spec:s1:functional_requirement:fr1",
        item={"id": "fr1", "text": "FR1 selected", "linked_task_ids": ["direct"]},
    )


def install_reader(monkeypatch, specs, cards=()):
    class Reader:
        async def list_specs(self, _db, *, board_id):
            assert board_id == "board"
            return specs

        async def list_board_cards(self, _db, *, board_id):
            assert board_id == "board"
            return cards

    monkeypatch.setattr(executor, "get_discovery_execution_read_port", lambda: Reader())


@pytest.mark.asyncio
async def test_fr_coverage_includes_explicit_rules_scenarios_and_effective_cards(monkeypatch):
    selected = selected_fr()
    spec = SimpleNamespace(
        id="s1", title="Spec one",
        test_scenarios=[
            {"id": "scenario", "title": "Explicit scenario",
             "linked_criteria": [selected.child_ref], "linked_task_ids": ["shared", "cancelled"]},
            {"id": "ac_only", "title": "AC zero is not FR zero", "linked_criteria": [0, "0"]},
            {"id": "fr10", "title": "Wrong FR", "linked_criteria": ["FR10 - other"]},
            "legacy malformed entry",
        ],
        business_rules=[
            {"id": "rule", "text": "Rule for selected FR", "linked_requirements": [0],
             "linked_task_ids": ["shared", "rule-only", "archived", "other-board"]},
            {"id": "wrong", "text": "Wrong rule", "linked_requirements": [1]},
            {"id": "revoked", "text": "Revoked rule", "linked_requirements": [0], "status": "revoked"},
        ],
    )
    other_spec = SimpleNamespace(
        id="s2", title="Other spec", test_scenarios=[],
        business_rules=[{"id": "rule-other", "linked_requirements": [0]}],
    )
    cards = [SimpleNamespace(id=key, title=key, board_id="board", status="done", archived=False)
             for key in ("direct", "shared", "rule-only", "unrelated")]
    cards.extend([
        SimpleNamespace(id="cancelled", title="Cancelled", board_id="board", status="cancelled", archived=False),
        SimpleNamespace(id="archived", title="Archived", board_id="board", status="done", archived=True),
        SimpleNamespace(id="other-board", title="Other", board_id="other", status="done", archived=False),
    ])
    install_reader(monkeypatch, [spec, other_spec], cards)

    result = await executor._exec_test_scenarios(
        None, "board", SimpleNamespace(name="coverage_for_fr"), {"fr_id": selected.child_ref},
        selector_values={"fr_id": selected},
    )

    assert {(r["id"], r["type"]) for r in result["rows"]} == {
        ("scenario", "TestScenario"), ("spec:s1:business_rule:rule", "BusinessRule"),
        ("direct", "Card"), ("shared", "Card"), ("rule-only", "Card"),
    }
    shared = next(r for r in result["rows"] if r["id"] == "shared")
    assert shared["meta"]["coverage_via"] == [
        "spec:s1:business_rule:rule", "spec:s1:test_scenario:scenario",
    ]
    assert shared["meta"]["entity_type"] == "card"
    assert all(r["meta"]["selected_child_ref"] == selected.child_ref for r in result["rows"])


@pytest.mark.asyncio
async def test_scenarios_without_tasks_remains_scenario_only(monkeypatch):
    spec = SimpleNamespace(id="s1", title="Spec", test_scenarios=[
        {"id": "empty", "title": "Empty", "linked_task_ids": []},
        {"id": "linked", "title": "Linked", "linked_task_ids": ["card"]},
    ])
    install_reader(monkeypatch, [spec])
    result = await executor._exec_test_scenarios(
        None, "board", SimpleNamespace(name="scenarios_without_tasks"), {},
    )
    assert [r["id"] for r in result["rows"]] == ["empty"]


@pytest.mark.asyncio
async def test_fr_named_identity_in_linked_criteria_does_not_match_ac_position(monkeypatch):
    from dataclasses import replace

    selected = replace(selected_fr(), child_id="fr_x", child_ref="spec:s1:functional_requirement:fr_x", item={})
    install_reader(monkeypatch, [SimpleNamespace(
        id="s1", title="Spec", business_rules=[], test_scenarios=[
            {"id": "explicit", "linked_criteria": ["fr_x"]},
            {"id": "ac_position", "linked_criteria": [0, "0"]},
            {"id": "other_fr", "linked_criteria": ["fr_other"]},
        ],
    )])
    result = await executor._exec_test_scenarios(
        None, "board", SimpleNamespace(name="coverage_for_fr"), {"fr_id": selected.child_ref},
        selector_values={"fr_id": selected},
    )
    assert [row["id"] for row in result["rows"]] == ["explicit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("partial", [False, True])
async def test_natural_query_preserves_warning_and_any_partial_rows(monkeypatch, partial):
    from okto_pulse.core.kg import tier_power

    nodes = [{"node_id": "n1", "title": "Partial hit", "node_type": "Decision", "similarity": 0.5}] if partial else []
    monkeypatch.setattr(tier_power, "execute_natural_query", lambda *args, **kwargs: {
        "nodes": nodes, "warning": "embedding_unavailable",
    })
    result = await executor._exec_query_natural("board", {"query": "topic"})
    assert result["warning"] == "embedding_unavailable"
    assert [row["id"] for row in result["rows"]] == (["n1"] if partial else [])


@pytest.mark.asyncio
@pytest.mark.parametrize("order", list(permutations(("old", "middle", "new"))))
async def test_supersedence_returns_complete_chain_without_suffix_duplicates(monkeypatch, order):
    decisions = {
        "old": {"id": "old", "title": "Old"},
        "middle": {"id": "middle", "title": "Middle", "supersedes_decision_id": "old"},
        "new": {"id": "new", "title": "New", "supersedes_decision_id": "middle"},
    }
    install_reader(monkeypatch, [SimpleNamespace(id="s1", title="One", decisions=[decisions[k] for k in order])])
    result = await executor._exec_supersedence_chains(None, "board")
    assert result["total"] == 1
    assert [r["id"] for r in result["rows"][0]["meta"]["chain"]] == ["new", "middle", "old"]
    assert "warning" not in result


@pytest.mark.asyncio
async def test_supersedence_keeps_identical_ids_in_their_own_spec(monkeypatch):
    install_reader(monkeypatch, [
        SimpleNamespace(id=spec_id, title=spec_id, decisions=[
            {"id": "old", "title": f"Old {spec_id}"},
            {"id": "new", "title": f"New {spec_id}", "supersedes_decision_id": "old"},
        ]) for spec_id in ("s1", "s2")
    ])
    result = await executor._exec_supersedence_chains(None, "board")
    assert result["total"] == 2
    for row in result["rows"]:
        assert {item["spec_id"] for item in row["meta"]["chain"]} == {row["meta"]["entity_id"]}


@pytest.mark.asyncio
async def test_supersedence_bounds_cycles_and_reports_missing_local_targets(monkeypatch):
    install_reader(monkeypatch, [
        SimpleNamespace(id="s1", title="One", decisions=[
            {"id": "a", "supersedes_decision_id": "b"},
            {"id": "b", "supersedes_decision_id": "a"},
            {"id": "dangling", "supersedes_decision_id": "other"},
        ]),
        SimpleNamespace(id="s2", title="Two", decisions=[{"id": "other"}]),
    ])
    result = await executor._exec_supersedence_chains(None, "board")
    assert result["chain_diagnostics"] == {"cycles": 1, "missing_targets": 1}
    assert result["warning"]
    assert sorted(row["meta"]["length"] for row in result["rows"]) == [1, 2]
    assert all(item["spec_id"] == "s1" for row in result["rows"] for item in row["meta"]["chain"])


@pytest.mark.asyncio
async def test_contradiction_with_nullable_confidence_is_still_reported(monkeypatch):
    from okto_pulse.core.kg import kg_service

    monkeypatch.setattr(kg_service, "get_kg_service", lambda: SimpleNamespace(
        find_contradictions=lambda *args, **kwargs: [
            {"id_a": "a", "title_a": "A", "id_b": "b", "title_b": "B", "confidence": None},
        ],
    ))
    result = await executor._exec_contradictions("board")
    assert result["total"] == 1
    assert result["rows"][0]["summary"] == "confidence n/a"
    assert result["rows"][0]["meta"]["confidence"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("archived", [False, True])
async def test_uncovered_requirements_treats_archived_card_links_as_inactive(monkeypatch, archived):
    spec = SimpleNamespace(
        id="s1", title="Spec", status="review", functional_requirements=[],
        acceptance_criteria=[], test_scenarios=[], api_contracts=[],
        integration_requirements=[], observability_requirements=[],
        technical_requirements=[{"id": "tr", "text": "TR", "linked_task_ids": ["card"]}],
        business_rules=[{"id": "br", "text": "BR", "linked_task_ids": ["card"]}],
        decisions=[{"id": "decision", "title": "Decision", "linked_task_ids": ["card"]}],
    )

    class Reader:
        async def get_board_settings(self, _db, *, board_id):
            return {}

        async def list_specs(self, _db, *, board_id):
            return [spec]

        async def list_board_cards(self, _db, *, board_id, include_archived=False):
            assert include_archived is True
            return [SimpleNamespace(id="card", spec_id="s1", status="done", archived=archived)]

    monkeypatch.setattr(executor, "get_discovery_execution_read_port", lambda: Reader())
    result = await executor._exec_uncovered_requirements(None, "board")
    assert {row["type"] for row in result["rows"]} == (
        {"UncoveredTR", "UncoveredBR", "UncoveredDecision"} if archived else set()
    )
