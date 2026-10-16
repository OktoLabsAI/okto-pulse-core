"""Edition validity precedes filtering/pagination; incompatible history is not repaired."""
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

from spec_validation_fixtures import native_validation

import pytest

from okto_pulse.core.models.schemas import SpecValidationResponse
from okto_pulse.core.services.main import SpecService


@pytest.mark.asyncio
@pytest.mark.parametrize("edition", [None, 0, -1, True, "1"])
@pytest.mark.parametrize("view", ["all", "current", "previous"])
async def test_incompatible_history_is_refused_before_filtering(edition, view):
    rows = [{"id": "old", "edition": edition, "validation_edition": edition},
            native_validation("current", 2)]
    original = deepcopy(rows)
    spec = SimpleNamespace(validations=rows, edition=2, current_validation_id="current")
    service = SpecService(object())
    service.get_spec = AsyncMock(return_value=spec)
    with pytest.raises(ValueError, match="spec_validation_edition_required"):
        await service.list_spec_validations("spec", lifecycle_state=view, limit=1)
    assert spec.validations == original
    assert spec.current_validation_id == "current"


@pytest.mark.asyncio
async def test_native_previous_attempts_and_pagination_preserve_their_editions():
    rows = [native_validation("previous", 1),
            native_validation("superseded", 2),
            native_validation("current", 2)]
    service = SpecService(object())
    service.get_spec = AsyncMock(return_value=SimpleNamespace(
        validations=rows, edition=2, current_validation_id="current"))
    result = await service.list_spec_validations("spec", lifecycle_state="previous", limit=1)
    assert result["current_validation_id"] == "current"
    assert result["total"] == 2 and result["has_more"]
    assert result["validations"][0]["id"] == "superseded"
    assert result["validations"][0]["edition"] == 2
    page = await service.list_spec_validations("spec", lifecycle_state="previous", offset=1, limit=1)
    assert page["validations"][0]["edition"] == 1
    assert not page["has_more"]
    with pytest.raises(ValueError, match="lifecycle_state"):
        await service.list_spec_validations("spec", lifecycle_state="history_only")


@pytest.mark.parametrize("field", ["edition", "validation_edition"])
@pytest.mark.parametrize("value", [None, 0, -1, True, "1"])
def test_response_requires_native_integer_editions(field, value):
    with pytest.raises(ValueError):
        SpecValidationResponse.model_validate({**native_validation("v"), field: value})


def test_response_refuses_contradictory_editions():
    with pytest.raises(ValueError, match="spec_validation_edition_mismatch"):
        SpecValidationResponse.model_validate(native_validation("v", validation_edition=2))


@pytest.mark.parametrize("field", ["score", "summary", "completeness",
    "completeness_justification", "general_justification"])
def test_response_refuses_removed_fields_even_when_null(field):
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        SpecValidationResponse.model_validate({**native_validation(), field: None})


@pytest.mark.parametrize("field", ["confidence", "clarity", "assertiveness", "decidability",
    "ambiguity", "confidence_justification", "outcome", "threshold_violations", "receipt_id"])
def test_response_refuses_sparse_record_instead_of_inventing_values(field):
    record = native_validation()
    del record[field]
    with pytest.raises(ValueError, match="Field required"):
        SpecValidationResponse.model_validate(record)


def test_removed_board_threshold_is_not_silently_accepted():
    from okto_pulse.core.models.schemas import BoardSettings
    with pytest.raises(ValueError, match="min_spec_completeness_removed"):
        BoardSettings(min_spec_completeness=80)
    assert "min_spec_completeness" not in BoardSettings().model_dump()


def test_recorded_thresholds_cannot_contain_removed_policy_or_omit_current_policy():
    record = native_validation()
    record["resolved_thresholds"]["min_spec_completeness"] = 80
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        SpecValidationResponse.model_validate(record)
    record = native_validation()
    del record["resolved_thresholds"]["min_spec_decidability"]
    with pytest.raises(ValueError, match="Field required"):
        SpecValidationResponse.model_validate(record)


@pytest.mark.asyncio
async def test_old_metric_history_is_refused_without_rewriting_records():
    record = native_validation()
    record["score"] = 90
    original = deepcopy(record)
    service = SpecService(object())
    spec = SimpleNamespace(validations=[record], edition=1, current_validation_id=record["id"])
    service.get_spec = AsyncMock(return_value=spec)
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        await service.list_spec_validations("spec")
    assert spec.validations == [original]
