"""C7 authoritative lightweight DTO for ``GET /boards/{id}/cards``."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from okto_pulse.core.models import CardPageItem


EXPECTED_FIELDS = {
    "id",
    "board_id",
    "spec_id",
    "sprint_id",
    "title",
    "description",
    "status",
    "priority",
    "card_type",
    "position",
    "assignee_id",
    "labels",
    "archived",
    "created_by",
    "due_date",
    "severity",
    "test_scenario_ids",
    "linked_test_task_ids",
    "validations_count",
    "validations_fail_count",
    "validations_has_pass",
    "first_pass_confidence",
    "first_pass_completeness",
    "first_pass_drift",
    "conclusions_count",
    "last_conclusion_completeness",
    "last_conclusion_drift",
    "created_at",
    "updated_at",
    "open_qa_count",
}

ITEM = {
    "id": "card-1",
    "board_id": "board-1",
    "spec_id": None,
    "sprint_id": None,
    "title": "Lightweight card",
    "description": None,
    "status": "in_progress",
    "priority": "high",
    "card_type": "bug",
    "position": 7,
    "assignee_id": None,
    "labels": None,
    "archived": False,
    "created_by": "agent-1",
    "due_date": None,
    "severity": "major",
    "test_scenario_ids": None,
    "linked_test_task_ids": None,
    "validations_count": 2,
    "validations_fail_count": 1,
    "validations_has_pass": True,
    "first_pass_confidence": 82,
    "first_pass_completeness": 90,
    "first_pass_drift": 5,
    "conclusions_count": 1,
    "last_conclusion_completeness": 100,
    "last_conclusion_drift": 0,
    "created_at": "2026-07-20T08:00:00Z",
    "updated_at": "2026-07-20T09:00:00Z",
    "open_qa_count": 3,
}


def test_card_page_item_has_exactly_the_authoritative_required_fields() -> None:
    schema = CardPageItem.model_json_schema()

    assert set(schema["properties"]) == EXPECTED_FIELDS
    assert set(schema["required"]) == EXPECTED_FIELDS
    assert set(CardPageItem.model_fields) == EXPECTED_FIELDS


def test_card_page_item_accepts_orm_nulls_and_reuses_domain_enums() -> None:
    item = CardPageItem.model_validate(ITEM)

    assert item.spec_id is None
    assert item.sprint_id is None
    assert item.labels is None
    assert item.status.value == "in_progress"
    assert item.priority.value == "high"
    assert item.card_type.value == "bug"
    assert item.severity is not None and item.severity.value == "major"


def test_card_page_item_requires_nullable_metrics_but_accepts_null_values() -> None:
    payload = dict(ITEM)
    for field in (
        "first_pass_confidence",
        "first_pass_completeness",
        "first_pass_drift",
        "last_conclusion_completeness",
        "last_conclusion_drift",
    ):
        payload[field] = None

    CardPageItem.model_validate(payload)
    payload.pop("first_pass_confidence")
    with pytest.raises(ValidationError):
        CardPageItem.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("validations_count", -1),
        ("validations_fail_count", -1),
        ("conclusions_count", -1),
        ("open_qa_count", -1),
        ("first_pass_confidence", 101),
        ("first_pass_completeness", -1),
        ("first_pass_drift", 101),
        ("last_conclusion_completeness", 101),
        ("last_conclusion_drift", -1),
    ],
)
def test_card_page_item_rejects_invalid_derived_metrics(
    field: str,
    value: int,
) -> None:
    with pytest.raises(ValidationError):
        CardPageItem.model_validate({**ITEM, field: value})
