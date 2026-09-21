"""F5: shared analytics preserve their metrics without live Sprint reads."""

import json
from pathlib import Path

import pytest

from analytics_retirement_cases import (
    CASES,
    FixedClock,
    PopulationReader,
    evaluate_case,
    surviving_fields,
)
from okto_pulse.core.ports.analytics_read import AnalyticsFact
from okto_pulse.core.services import analytics_service as service

BASELINE = json.loads(
    (
        Path(__file__).parent / "fixtures/analytics_sprint_retirement_baseline.json"
    ).read_text(encoding="utf-8")
)["cases"]


@pytest.fixture
def reader(monkeypatch):
    port = PopulationReader(forbid_retired=True)
    # Simulate the target read model: neither a Sprint source nor Card.sprint_id.
    del port.rows["sprint"]
    port.rows["card"] = [
        AnalyticsFact({k: v for k, v in row.values.items() if k != "sprint_id"})
        for row in port.rows["card"]
    ]
    monkeypatch.setattr(service, "get_analytics_read_port", lambda: port)
    monkeypatch.setattr(service, "datetime", FixedClock)
    return port


@pytest.mark.asyncio
@pytest.mark.parametrize("name,window", CASES)
async def test_surviving_metrics_match_installed_pre_retirement_baseline(
    reader, name, window
):
    result = await evaluate_case(service, name, window)
    assert result == surviving_fields(BASELINE[name + "-" + window])


@pytest.mark.asyncio
async def test_no_boards_does_not_fabricate_retired_zero_metrics(reader):
    result = await service.compute_overview(None, "nobody")
    assert result == surviving_fields(BASELINE["overview-no-access"])
    assert [query.entity for query in reader.queries] == ["board"]


@pytest.mark.asyncio
async def test_spec_detail_keeps_cards_and_obligations_without_retired_query(reader):
    result = await service._spec_detail(None, "board-a", "spec-a")
    assert result == surviving_fields(result)
    assert {card["id"] for card in result["cards"]} == {
        "card-normal",
        "card-test",
        "card-bug",
    }
    assert result["business_rules"] == [{"id": "br-1", "text": "Keep approval"}]
    assert result["coverage_summary"]["brs_total"] == 1


@pytest.mark.asyncio
async def test_card_detail_requires_no_sprint_field_and_preserves_validation_history(
    reader,
):
    result = await service._card_detail(None, "board-a", "card-normal")
    assert result == surviving_fields(result)
    assert result["spec_id"] == "spec-a"
    assert result["completeness"] == 88
    assert len(result["validations"]) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entity,id", [("spec", "spec-foreign"), ("card", "card-foreign")]
)
async def test_foreign_entity_detail_remains_inaccessible(reader, entity, id):
    assert await getattr(service, "_" + entity + "_detail")(None, "board-a", id) is None
