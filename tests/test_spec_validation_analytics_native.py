"""Spec analytics consumes the same native record as history, without score conversion."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from spec_validation_fixtures import native_validation
from okto_pulse.core.services import analytics_service as analytics


def test_native_history_aggregates_all_five_dimensions_and_multicount_rejections():
    failed = native_validation("failed", confidence=60, clarity=70, decidability=60,
        outcome="failed", recommendation="reject", threshold_violations=[
            "confidence 60 < min 70", "clarity 70 < min 80", "decidability 60 < min 80"])
    passed = native_validation("passed", 2)
    spec = SimpleNamespace(validations=[failed, passed])
    original = deepcopy(spec.validations)
    result = analytics.aggregate_spec_validation_gate([spec])
    assert result["avg_scores"] == dict(confidence=75, clarity=80,
        assertiveness=90, decidability=75, ambiguity=10)
    assert result["total_submitted"] == 2 and result["success_rate"] == 50
    assert result["rejection_reasons"] == dict(confidence_below=1, clarity_below=1,
        assertiveness_below=0, decidability_below=1, ambiguity_above=0, reject_recommendation=1)
    assert spec.validations == original


@pytest.mark.parametrize("mutation", ["old_field", "missing_score", "editionless", "not_a_list"])
def test_incompatible_analytics_history_is_refused_without_mutation(mutation):
    record = native_validation()
    if mutation == "old_field":
        record["completeness"] = 90
    elif mutation == "missing_score":
        del record["confidence"]
    elif mutation == "editionless":
        del record["edition"]
    spec = SimpleNamespace(validations=record if mutation == "not_a_list" else [record])
    original = deepcopy(spec.validations)
    with pytest.raises(ValueError):
        analytics.aggregate_spec_validation_gate([spec])
    assert spec.validations == original


@pytest.mark.asyncio
async def test_board_breakdown_reports_current_spec_metrics_without_task_aliases(monkeypatch):
    spec = SimpleNamespace(id="spec", title="Native", status="validated",
        edition=1, version=1, current_validation_id="latest",
        validations=[native_validation("latest")])
    async def rows(_db, kind, **kwargs):
        return [spec] if kind == "spec" else []
    monkeypatch.setattr(analytics, "_analytics_list", rows)
    result = await analytics.compute_validations(object(), "board")
    row = result["spec_validation_gate"]["per_spec"][0]
    assert row["last_confidence"] == 90 and row["last_clarity"] == 90
    assert row["last_decidability"] == 90
    assert "last_completeness" not in row
    assert "completeness" in result["task_validation_gate"]["avg_scores"]
    monkeypatch.setattr(analytics, "spec_coverage_summary", lambda *args, **kwargs: {
        "decisions_coverage_pct": 100, "decisions_uncovered_ids": []})
    details = await analytics.compute_spec_analytics(object(), "board", "spec")
    timeline = details["validation_timeline"][0]
    assert {key: timeline[key] for key in ("confidence", "clarity", "decidability",
        "assertiveness", "ambiguity")} == dict(confidence=90, clarity=90,
            decidability=90, assertiveness=90, ambiguity=10)
    assert "completeness" not in timeline
