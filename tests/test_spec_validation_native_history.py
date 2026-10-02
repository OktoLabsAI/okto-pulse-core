"""Edition validity precedes filtering/pagination; incompatible history is not repaired."""
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.models.schemas import SpecValidationResponse
from okto_pulse.core.services.main import SpecService


@pytest.mark.asyncio
@pytest.mark.parametrize("edition", [None, 0, -1, True, "1"])
@pytest.mark.parametrize("view", ["all", "current", "previous"])
async def test_incompatible_history_is_refused_before_filtering(edition, view):
    rows = [{"id": "old", "edition": edition, "validation_edition": edition},
            {"id": "current", "edition": 2, "validation_edition": 2}]
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
    rows = [{"id": "previous", "edition": 1, "validation_edition": 1},
            {"id": "superseded", "edition": 2, "validation_edition": 2},
            {"id": "current", "edition": 2, "validation_edition": 2}]
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
        SpecValidationResponse.model_validate({"id": "v", "edition": 1,
            "validation_edition": 1, field: value})


def test_response_refuses_contradictory_editions():
    with pytest.raises(ValueError, match="spec_validation_edition_mismatch"):
        SpecValidationResponse.model_validate({"id": "v", "edition": 1, "validation_edition": 2})
