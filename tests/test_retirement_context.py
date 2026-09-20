from copy import deepcopy

import pytest

from okto_pulse.core.ports.retirement_context import inspect_retirement_context


def sprint(**extra):
    return dict(description=None, objective=None, expected_outcome=None, evaluations=None, **extra)


@pytest.mark.parametrize("status", ["draft", "active", "review", "closed", "cancelled"])
@pytest.mark.parametrize("stale", [False, True])
def test_evaluation_recommendation_and_lifecycle_cannot_dispose_of_substantive_context(status, stale):
    facts = sprint(status=status)
    facts["evaluations"] = [{"id": "e", "recommendation": "approve", "stale": stale,
        "overall_score": 100, "overall_justification": "Use the existing protocol"}]
    before = deepcopy(facts)
    concern, = inspect_retirement_context("sprint", facts)
    assert concern.path == ("evaluations", 0)
    assert concern.reason == "evaluation_requires_disposition"
    assert facts == before


@pytest.mark.parametrize("answer,selected", [(None, ["choice"]), ("", None), ("text", None)])
def test_qa_state_uses_answered_timestamp_not_answer_truthiness(answer, selected):
    facts = {"question": "How?", "answer": answer, "selected": selected, "answered_at": "2026-09-20"}
    assert inspect_retirement_context("qa", facts)[0].reason == "answered_question_requires_disposition"
    facts["answered_at"] = None
    assert inspect_retirement_context("qa", facts)[0].reason == "unanswered_question"


def test_each_text_field_has_a_distinct_origin_and_empty_text_is_not_transferred():
    facts = sprint()
    assert inspect_retirement_context("sprint", facts) == ()
    facts.update(description=" ", objective="Decision", expected_outcome="Constraint")
    assert [item.path for item in inspect_retirement_context("sprint", facts)] == [("objective",), ("expected_outcome",)]


@pytest.mark.parametrize("facts", [{"summary": "Decision", "changes": None}, {"summary": None, "changes": [{"decision": "Keep gate"}]}])
def test_history_content_is_neither_copied_nor_dismissed_as_administrative(facts):
    assert inspect_retirement_context("history", facts)[0].reason == "history_requires_disposition"
    assert inspect_retirement_context("history", {"summary": " ", "changes": []}) == ()


@pytest.mark.parametrize("kind,facts", [("other", {}), ("qa", {"question": "", "answered_at": None}),
    ("history", {"summary": None, "changes": "unknown"}),
    ("sprint", {"description": 1}),
    ("sprint", {"description": None, "objective": None, "expected_outcome": None, "evaluations": {}})])
def test_unknown_contracts_fail_closed(kind, facts):
    with pytest.raises(ValueError, match="retirement_context_"):
        inspect_retirement_context(kind, facts)
