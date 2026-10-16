"""The architecture dry-run and typed writes share one boundaries contract."""

import json

import pytest
from pydantic import ValidationError

from okto_pulse.core.models.schemas import ArchitectureEntity
from okto_pulse.core.services.architecture import ArchitectureDesignRepository
from okto_pulse.core.application.processors.deterministic_kg import _architecture_entity_content


def entity(**fields):
    return {"id": "checkout", "name": "Checkout Service", "entity_type": "service", **fields}


@pytest.mark.parametrize("value", ["Backend", None, {}, 42, [None], [1], [True], [""], [" \t\n"], ["Valid", ""]])
def test_dry_run_and_typed_entity_reject_the_same_invalid_boundaries(value):
    item = entity(boundaries=value)
    with pytest.raises(ValidationError):
        ArchitectureEntity.model_validate(item)
    result = ArchitectureDesignRepository(None).critique_payload({
        "title": "Checkout runtime", "global_description": "Checkout owns order commands.",
        "entities": [item],
    })
    assert result["valid"] is False
    assert any("entities[0].boundaries" in issue for issue in result["issues"])


@pytest.mark.parametrize("value", [[], ["Backend"], ["Tenant data, isolated", "  Preserve text\nAnd lines  "]])
def test_lists_preserve_content_order_and_kg_projection(value):
    item = entity(boundaries=value)
    assert ArchitectureEntity.model_validate(item).boundaries == value
    result = ArchitectureDesignRepository(None).critique_payload({
        "title": "Checkout runtime", "global_description": "Checkout owns order commands.",
        "entities": [item],
    })
    assert result["valid"] is True, result
    if value:
        assert json.dumps(value, ensure_ascii=False) in _architecture_entity_content(item)


def test_omitted_boundaries_defaults_to_independent_empty_lists():
    first, second = ArchitectureEntity(**entity()), ArchitectureEntity(**entity())
    first.boundaries.append("Private runtime")
    assert second.model_dump()["boundaries"] == []
