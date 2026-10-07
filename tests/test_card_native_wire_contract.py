"""Native Card contracts expose no Sprint or migrated policy surface."""

import pytest
from pydantic import ValidationError

from okto_pulse.core.models.schemas import (
    CardCreate, CardUpdate, CardResponse, CardSummaryForSpec,
)


@pytest.mark.parametrize("model", [CardCreate, CardUpdate])
@pytest.mark.parametrize("value", [None, "", "legacy", 0, {}])
def test_legacy_input_fails_even_when_null_or_empty(model, value):
    with pytest.raises(ValidationError, match="card_sprint_link_retired"):
        model.model_validate({"title": "Task", "sprint_id": value})


@pytest.mark.parametrize("model", [CardCreate, CardUpdate, CardResponse, CardSummaryForSpec])
def test_wire_contract_no_longer_advertises_sprint(model):
    assert "sprint_id" not in model.model_json_schema()["properties"]


def card_payload():
    return dict(id="card", board_id="board", spec_id="spec", title="Task",
                description=None, details=None, status="not_started", subject_version=1,
                priority="none", position=0, assignee_id=None, created_by="actor",
                created_at="2026-09-21T00:00:00Z", updated_at="2026-09-21T00:00:00Z",
                due_date=None, labels=None)



@pytest.mark.parametrize("model", [CardCreate, CardUpdate])
@pytest.mark.parametrize("value", [None, {"overrides": {"min_confidence": 90}}])
def test_migrated_policy_is_not_an_input_contract(model, value):
    with pytest.raises(ValidationError, match="extra_forbidden"):
        model.model_validate({"title": "Task", "migrated_validation_policy": value})


@pytest.mark.parametrize("model", [CardResponse, CardSummaryForSpec])
def test_native_read_does_not_advertise_migrated_policy(model):
    assert "migrated_validation_policy" not in model.model_json_schema()["properties"]


def test_native_card_projection_excludes_retired_fields():
    result = CardResponse.model_validate(card_payload()).model_dump()
    assert "sprint_id" not in result
    assert "migrated_validation_policy" not in result
