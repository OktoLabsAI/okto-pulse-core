from copy import deepcopy

import pytest

from okto_pulse.core.domain.test_card_verification_context import (
    build_test_card_verification_context,
)
from okto_pulse.core.mcp.context_projection import project_task_context


def population():
    return dict(
        scenario_ids=["s"],
        scenarios=[
            {
                "id": "s",
                "verification_method": "inspection",
                "linked_criteria": ["ac"],
                "given": "Source",
                "when": "Review imports",
                "then": "Only ports",
            }
        ],
        criteria=[
            {
                "id": "ac",
                "text": "Domain does not import adapters",
                "title": "Boundary",
                "verification_profile": "technical",
                "requirement_links": [
                    {
                        "requirement_type": "technical_requirement",
                        "requirement_id": "tr",
                        "aspect": "Imports",
                    }
                ],
            }
        ],
        admitted_methods=frozenset({"inspection"}),
    )


def test_only_assigned_scenarios_and_exact_criteria_are_resolved_without_mutation():
    data = population()
    data["scenarios"].append({"id": "other", "linked_criteria": ["other-ac"]})
    data["criteria"].append({"id": "other-ac", "text": "Do not include"})
    before = deepcopy(data)
    result = build_test_card_verification_context(**data)
    assert result["source_resolution_complete"] and not result["delivery_evaluated"]
    assert len(result["items"]) == 1
    assert result["items"][0]["criteria"][0]["criterion_id"] == "ac"
    assert "Do not include" not in str(result)
    assert data == before


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "ambiguous",
        "revoked",
        "metadata",
        "method",
        "no_scenario",
        "no_links",
    ],
)
def test_gaps_are_explicit_not_execution_credit(change):
    data = population()
    if change == "missing":
        data["criteria"] = []
    if change == "ambiguous":
        data["criteria"] *= 2
    if change == "revoked":
        data["criteria"][0]["status"] = "revoked"
    if change == "metadata":
        data["criteria"][0]["verification_profile"] = "invalid"
    if change == "method":
        data["admitted_methods"] = None
    if change == "no_scenario":
        data["scenarios"] = []
    if change == "no_links":
        data["scenarios"][0]["linked_criteria"] = []
    result = build_test_card_verification_context(**data)
    assert not result["source_resolution_complete"] and result["issues"]
    assert not result["delivery_evaluated"]


def test_full_keeps_resolved_plan_and_summary_marks_truncation():
    data = population()
    data["criteria"][0]["text"] = "Long observable condition " * 10000
    context = build_test_card_verification_context(**data)
    source = {"card": {"id": "card"}, "test_verification_context": context}
    full = project_task_context(source, card_id="card", profile="full", context_scope="all")
    assert full["test_verification_context"] == context
    summary = project_task_context(source, card_id="card", profile="summary", context_scope="all")
    assert summary["projection"]["truncated"]
    assert not summary["test_verification_context"]["content_complete"]
    assert "profile=full" in summary["test_verification_context"]["next_read"]
    assert context["content_complete"]  # No mutation of the source read model.


def test_summary_preserves_false_and_empty_semantics():
    context = build_test_card_verification_context(**population())
    summary = project_task_context(
        {"test_verification_context": context}, card_id="card", profile="summary"
    )
    assert summary["test_verification_context"]["delivery_evaluated"] is False
    assert summary["test_verification_context"]["issues"] == []
