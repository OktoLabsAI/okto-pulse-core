"""Regression tests for SQLAlchemy -> DeterministicWorker adapter fields."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.application.processors.consolidation import (
    _card_to_dict,
    _resolve_missing_link_candidates,
    _spec_to_dict,
)
from okto_pulse.core.application.processors.deterministic_kg import (
    EmittedNode,
    MissingLinkCandidate,
    WORKER_VERSION,
    WorkerResult,
)


class _FakeScalars:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeExecuteResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return _FakeScalars(self._rows)


class _FakeDb:
    def __init__(self, card_rows, spec_rows):
        self._results = [_FakeExecuteResult(card_rows), _FakeExecuteResult(spec_rows)]

    async def execute(self, _statement):
        return self._results.pop(0)


def test_spec_to_dict_serializes_formal_decisions():
    decisions = [
        {
            "id": "dec_adapter_one",
            "title": "Use deterministic import first",
            "rationale": "Structured source data must arrive before cognitive work.",
            "context": "KG ingestion",
            "alternatives_considered": ["Recover with cognitive closeout"],
            "linked_requirements": ["0"],
            "status": "active",
        }
    ]
    spec = SimpleNamespace(
        id="spec-adapter",
        board_id="board-adapter",
        title="Adapter Spec",
        description="desc",
        context="ctx",
        status="draft",
        functional_requirements=[],
        technical_requirements=[],
        acceptance_criteria=[],
        business_rules=[],
        test_scenarios=[],
        api_contracts=[],
        decisions=decisions,
        architecture_designs=[],
    )

    payload = _spec_to_dict(spec)

    assert payload["decisions"] == decisions


def test_card_to_dict_serializes_linked_test_task_ids():
    card = SimpleNamespace(
        id="bug-card-adapter",
        board_id="board-adapter",
        title="Bug card",
        description="desc",
        card_type="bug",
        status="not_started",
        spec_id="spec-adapter",
        sprint_id=None,
        origin_task_id="task-origin",
        linked_test_task_ids=["test-card-1"],
        priority=None,
        severity="major",
        architecture_designs=[],
    )

    payload = _card_to_dict(card)

    assert payload["origin_task_id"] == "task-origin"
    assert payload["linked_test_task_ids"] == ["test-card-1"]
    assert payload["severity"] == "major"


@pytest.mark.asyncio
async def test_resolve_missing_links_emits_bug_covered_by_edges():
    bug_cid = "card_bug12345_entity"
    result = WorkerResult(
        nodes=[
            EmittedNode(
                candidate_id=bug_cid,
                node_type="Bug",
                title="Bug",
                content="desc",
                source_artifact_ref="card:bug12345",
            )
        ],
        missing_link_candidates=[
            MissingLinkCandidate(
                edge_type="tests",
                from_candidate_id=bug_cid,
                from_candidate_title="Bug",
                reason="linked_test_task_requires_cross_artifact_resolution",
                suggested_candidates=["test_task:testcard123"],
                artifact_ref="card:bug12345",
            )
        ],
    )
    test_card = SimpleNamespace(
        id="testcard123",
        board_id="board-kg-test",
        title="Regression test",
        description="covers the bug",
        card_type="test",
        status="in_progress",
        spec_id="spec12345",
        test_scenario_ids=["ts-pass"],
    )
    spec = SimpleNamespace(
        id="spec12345",
        board_id="board-kg-test",
        title="Spec with regression scenario",
        description="Spec context",
        context="Context",
        status="in_progress",
        test_scenarios=[
            {
                "id": "ts-pass",
                "title": "Bug stays covered",
                "given": "a regression",
                "when": "the flow runs",
                "then": "coverage is linked",
            }
        ],
    )

    resolved = await _resolve_missing_link_candidates(
        _FakeDb([test_card], [spec]),
        "board-kg-test",
        result,
    )

    assert resolved.missing_link_candidates == []
    edge_targets = {
        (edge.edge_type, edge.to_candidate_id, edge.rule_id)
        for edge in resolved.edges
    }
    assert (
        "covered_by",
        "card_testcard_entity",
        f"covered_by/linked_test_task_id@{WORKER_VERSION}",
    ) in edge_targets
    assert (
        "covered_by",
        "spec_spec1234_ts_0",
        f"covered_by/linked_test_scenario@{WORKER_VERSION}",
    ) in edge_targets
    node_types = {node.candidate_id: node.node_type for node in resolved.nodes}
    assert node_types["card_testcard_entity"] == "Entity"
    assert node_types["spec_spec1234_ts_0"] == "TestScenario"
    assert node_types["spec_spec1234_entity"] == "Entity"
    assert any(
        edge.edge_type == "belongs_to"
        and edge.from_candidate_id == "card_testcard_entity"
        and edge.to_candidate_id == "board_board-kg_entity"
        for edge in resolved.edges
    )
    assert any(
        edge.edge_type == "belongs_to"
        and edge.from_candidate_id == "spec_spec1234_ts_0"
        and edge.to_candidate_id == "spec_spec1234_entity"
        and edge.rule_id == f"belongs_to/bug_linked_test_scenario@{WORKER_VERSION}"
        for edge in resolved.edges
    )


@pytest.mark.asyncio
async def test_resolve_missing_bug_links_uses_entity_projection_for_bug_targets():
    bug_cid = "card_bug12345_entity"
    result = WorkerResult(
        nodes=[
            EmittedNode(
                candidate_id=bug_cid,
                node_type="Bug",
                title="Bug",
                content="desc",
                source_artifact_ref="card:bug12345",
            )
        ],
        missing_link_candidates=[
            MissingLinkCandidate(
                edge_type="violates",
                from_candidate_id=bug_cid,
                from_candidate_title="Bug",
                reason="origin_task_requires_cross_artifact_resolution",
                suggested_candidates=["task:originbug999"],
                artifact_ref="card:bug12345",
            ),
            MissingLinkCandidate(
                edge_type="tests",
                from_candidate_id=bug_cid,
                from_candidate_title="Bug",
                reason="linked_test_task_requires_cross_artifact_resolution",
                suggested_candidates=["test_task:testbug999"],
                artifact_ref="card:bug12345",
            ),
        ],
    )
    origin_bug = SimpleNamespace(
        id="originbug999",
        board_id="board-kg-test",
        title="Origin bug",
        description="origin bug still projects as target Entity",
        card_type="bug",
        status="done",
        spec_id=None,
        observed_behavior="observed",
        expected_behavior="expected",
        steps_to_reproduce="steps",
        linked_test_task_ids=["testcard"],
        conclusions=["fixed"],
    )
    test_bug = SimpleNamespace(
        id="testbug999",
        board_id="board-kg-test",
        title="Test bug",
        description="bad historical data",
        card_type="bug",
        status="done",
        spec_id=None,
        observed_behavior="observed",
        expected_behavior="expected",
        steps_to_reproduce="steps",
        linked_test_task_ids=["testcard"],
        conclusions=["fixed"],
        test_scenario_ids=[],
    )

    resolved = await _resolve_missing_link_candidates(
        _FakeDb([origin_bug, test_bug], []),
        "board-kg-test",
        result,
    )

    assert resolved.missing_link_candidates == []
    node_types = {node.candidate_id: node.node_type for node in resolved.nodes}
    source_refs = {node.candidate_id: node.source_artifact_ref for node in resolved.nodes}
    assert node_types[bug_cid] == "Bug"
    assert node_types["card_originbu_target_entity"] == "Entity"
    assert node_types["card_testbug9_target_entity"] == "Entity"
    assert (
        source_refs["card_originbu_target_entity"]
        == "card_relationship_target:originbug999"
    )
    assert (
        source_refs["card_testbug9_target_entity"]
        == "card_relationship_target:testbug999"
    )
    assert (
        "originates_from",
        bug_cid,
        "card_originbu_target_entity",
    ) in {
        (edge.edge_type, edge.from_candidate_id, edge.to_candidate_id)
        for edge in resolved.edges
    }
    assert (
        "covered_by",
        bug_cid,
        "card_testbug9_target_entity",
    ) in {
        (edge.edge_type, edge.from_candidate_id, edge.to_candidate_id)
        for edge in resolved.edges
    }
