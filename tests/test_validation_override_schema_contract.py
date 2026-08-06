"""Public schema contract for Task Validation inheritance overrides."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.enums import SpecStatus, SprintStatus
from okto_pulse.core.models.schemas import (
    SpecResponse,
    SpecUpdate,
    SprintResponse,
    SprintUpdate,
)


OVERRIDES = {
    "require_task_validation": False,
    "validation_min_confidence": 91,
    "validation_min_completeness": 92,
    "validation_max_drift": 8,
}


@pytest.mark.parametrize("schema", (SpecUpdate, SprintUpdate))
def test_update_schemas_preserve_validation_overrides(schema: type) -> None:
    payload = schema.model_validate(OVERRIDES)

    assert payload.model_dump(exclude_unset=True) == OVERRIDES


@pytest.mark.parametrize("schema", (SpecUpdate, SprintUpdate))
def test_update_schemas_preserve_explicit_override_clears(schema: type) -> None:
    clear_payload = dict.fromkeys(OVERRIDES)
    payload = schema.model_validate(clear_payload)

    assert payload.model_dump(exclude_unset=True) == clear_payload


@pytest.mark.parametrize("schema", (SpecUpdate, SprintUpdate))
@pytest.mark.parametrize("value", (-1, 101))
def test_update_schemas_reject_out_of_range_validation_overrides(
    schema: type,
    value: int,
) -> None:
    with pytest.raises(ValidationError):
        schema.model_validate({"validation_min_confidence": value})


def test_spec_response_exposes_validation_overrides() -> None:
    now = datetime.now(UTC)
    response = SpecResponse.model_validate(
        {
            "id": "spec-1",
            "board_id": "board-1",
            "title": "Spec",
            "description": None,
            "context": None,
            "functional_requirements": [],
            "technical_requirements": [],
            "acceptance_criteria": [],
            "status": SpecStatus.DRAFT,
            "edition": 1,
            "version": 1,
            "assignee_id": None,
            "created_by": "agent-1",
            "created_at": now,
            "updated_at": now,
            "labels": None,
            **OVERRIDES,
        }
    )

    assert {key: getattr(response, key) for key in OVERRIDES} == OVERRIDES


def test_sprint_response_exposes_validation_overrides() -> None:
    now = datetime.now(UTC)
    response = SprintResponse.model_validate(
        {
            "id": "sprint-1",
            "spec_id": "spec-1",
            "board_id": "board-1",
            "title": "Sprint",
            "status": SprintStatus.DRAFT,
            "spec_version": 1,
            "version": 1,
            "created_by": "agent-1",
            "created_at": now,
            "updated_at": now,
            **OVERRIDES,
        }
    )

    assert {key: getattr(response, key) for key in OVERRIDES} == OVERRIDES
