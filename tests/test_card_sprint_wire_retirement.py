"""BASE T34: reject legacy writes and retain migration integrity before projection."""

from types import SimpleNamespace

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


def migrated_policy():
    return dict(contract_version="card-validation-compatibility/v1",
                board_id="board", card_id="card", source_spec_id="spec",
                source_sprint_id="historical", migration_id="migration",
                overrides={"min_confidence": 90})


@pytest.mark.parametrize("object_input", [False, True])
def test_read_preserves_opaque_policy_provenance_without_live_link(object_input):
    payload = dict(card_payload(), migrated_validation_policy=migrated_policy(), sprint_id=None)
    value = SimpleNamespace(**payload) if object_input else payload
    result = CardResponse.model_validate(value).model_dump()
    assert "sprint_id" not in result
    assert result["migrated_validation_policy"]["source_sprint_id"] == "historical"
    assert result["migrated_validation_policy"]["overrides"]["min_confidence"] == 90


@pytest.mark.parametrize("object_input", [False, True])
def test_corrupt_double_source_is_rejected_before_sprint_field_is_dropped(object_input):
    payload = dict(card_payload(), migrated_validation_policy=migrated_policy(), sprint_id="live")
    value = SimpleNamespace(**payload) if object_input else payload
    with pytest.raises(ValidationError, match="requires_detached_sprint"):
        CardResponse.model_validate(value)


def test_legacy_read_without_override_does_not_expose_operational_link():
    result = CardResponse.model_validate(dict(card_payload(), sprint_id="historical"))
    assert "sprint_id" not in result.model_dump()
