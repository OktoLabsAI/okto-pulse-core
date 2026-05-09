"""Regression tests for SQLAlchemy -> DeterministicWorker adapter fields."""

from __future__ import annotations

from types import SimpleNamespace

from okto_pulse.core.kg.workers.consolidation import _card_to_dict, _spec_to_dict


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
