"""Regression tests for SQLAlchemy -> DeterministicWorker adapter fields."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.workers.consolidation import (
    _card_to_dict,
    _resolve_missing_link_candidates,
    _spec_to_dict,
)
from okto_pulse.core.kg.workers.deterministic_worker import (
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
        title="Adapter Spec",
        description="desc",
        context="ctx",
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
        title="Bug card",
        description="desc",
        card_type="bug",
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
        title="Regression test",
        description="covers the bug",
        card_type="test",
        spec_id="spec12345",
        test_scenario_ids=["ts-pass"],
    )
    spec = SimpleNamespace(
        id="spec12345",
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
