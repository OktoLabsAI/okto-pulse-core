"""C9 exact transport contracts for spec and ideation lookups."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from okto_pulse.core.models import LookupItem, LookupResponse


def test_lookup_schemas_publish_only_the_required_authoritative_fields() -> None:
    item_schema = LookupItem.model_json_schema()
    response_schema = LookupResponse.model_json_schema()

    assert set(item_schema["properties"]) == {"id", "title", "status"}
    assert set(item_schema["required"]) == {"id", "title", "status"}
    assert set(response_schema["properties"]) == {"items", "total", "offset", "limit"}
    assert set(response_schema["required"]) == {"items", "total", "offset", "limit"}
    assert response_schema["properties"]["total"]["minimum"] == 0
    assert response_schema["properties"]["offset"]["minimum"] == 0
    assert response_schema["properties"]["limit"]["minimum"] == 1
    assert response_schema["properties"]["limit"]["maximum"] == 50


def test_lookup_response_accepts_spec_and_ideation_status_wire_values() -> None:
    response = LookupResponse.model_validate(
        {
            "items": [
                {"id": "spec-1", "title": "Spec", "status": "in_progress"},
                {"id": "idea-1", "title": "Ideation", "status": "approved"},
            ],
            "total": 2,
            "offset": 0,
            "limit": 50,
        }
    )

    assert [item.status for item in response.items] == ["in_progress", "approved"]


@pytest.mark.parametrize(
    "payload",
    [
        {"items": [], "total": -1, "offset": 0, "limit": 1},
        {"items": [], "total": 0, "offset": -1, "limit": 1},
        {"items": [], "total": 0, "offset": 0, "limit": 0},
        {"items": [], "total": 0, "offset": 0, "limit": 51},
        {"items": [], "total": 0, "offset": 0},
        {
            "items": [{"id": "spec-1", "title": "Missing status"}],
            "total": 1,
            "offset": 0,
            "limit": 1,
        },
    ],
)
def test_lookup_response_rejects_missing_fields_and_invalid_bounds(payload: dict) -> None:
    with pytest.raises(ValidationError):
        LookupResponse.model_validate(payload)
