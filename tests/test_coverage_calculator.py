"""Tests for the pure Analytics objective coverage calculator."""

from __future__ import annotations

import pytest

from okto_pulse.core.services.coverage_calculator import (
    CoverageDimensionInput,
    compute_objective_coverage,
    compute_spec_objective_coverage,
    spec_saturation_envelope_from_coverage,
)
from okto_pulse.core.services.analytics_service import spec_saturation_envelope


def test_objective_coverage_is_equal_weighted_across_active_dimensions():
    result = compute_objective_coverage(
        [
            CoverageDimensionInput("large", pct=50.0, total=100, covered=50),
            CoverageDimensionInput("small", pct=100.0, total=1, covered=1),
        ]
    )

    assert result.objective_pct == 75.0
    assert result.active_category_count == 2
    assert result.blocking == ("large",)


def test_skipped_dimension_is_excluded_and_not_treated_as_covered():
    result = compute_objective_coverage(
        [
            CoverageDimensionInput("irs", pct=0.0, total=4, skipped=True),
            CoverageDimensionInput("ors", pct=100.0, total=2),
        ]
    )

    assert result.objective_pct == 100.0
    assert result.active_category_count == 1
    assert result.skipped_category_keys == ("irs",)
    ir_dim = next(dim for dim in result.dimensions if dim.key == "irs")
    assert ir_dim.active is False
    assert ir_dim.skipped is True
    assert ir_dim.blocking is False
    assert "irs" not in result.blocking


def test_zero_total_dimensions_are_neutral():
    result = compute_objective_coverage(
        [
            CoverageDimensionInput("contracts", pct=0.0, total=0),
            CoverageDimensionInput("trs", pct=100.0, total=1),
        ]
    )

    assert result.objective_pct == 100.0
    assert result.active_category_count == 1
    assert result.blocking == ()


def test_empty_active_set_returns_full_objective_coverage():
    result = compute_objective_coverage([])

    assert result.objective_pct == 100.0
    assert result.active_category_count == 0
    assert result.blocking == ()


def test_spec_payload_adapter_preserves_ir_or_skip_semantics():
    result = compute_spec_objective_coverage(
        {
            "ac_coverage_pct": 50.0,
            "ac_total": 2,
            "fr_coverage_pct": 100.0,
            "fr_total": 1,
            "ir_task_linkage_pct": 0.0,
            "irs_total": 3,
            "skip_ir_coverage": True,
            "or_task_linkage_pct": 0.0,
            "ors_total": 0,
        }
    )

    assert result.objective_pct == 75.0
    assert result.active_category_count == 2
    assert result.skipped_category_keys == ("irs",)
    assert result.blocking == ("acceptance_criteria",)


def test_analytics_service_saturation_envelope_delegates_without_shape_change():
    coverage = {
        "ac_coverage_pct": 50.0,
        "ac_total": 2,
        "fr_coverage_pct": 100.0,
        "fr_total": 1,
        "scenario_task_linkage_pct": 100.0,
        "scenarios_total": 1,
        "br_task_linkage_pct": 100.0,
        "brs_total": 1,
        "contract_task_linkage_pct": 100.0,
        "contracts_total": 1,
        "tr_task_linkage_pct": 100.0,
        "trs_total": 1,
        "decisions_coverage_pct": 100.0,
        "decisions_total": 1,
        "ir_task_linkage_pct": 0.0,
        "irs_total": 1,
        "skip_ir_coverage": True,
        "or_task_linkage_pct": 0.0,
        "ors_total": 0,
    }

    expected = spec_saturation_envelope_from_coverage(coverage)
    assert spec_saturation_envelope(coverage) == expected
    assert set(expected) == {"pct", "blocking"}


@pytest.mark.parametrize(
    "dimension",
    [
        CoverageDimensionInput("negative_total", pct=0.0, total=-1),
        CoverageDimensionInput("negative_pct", pct=-0.1, total=1),
        CoverageDimensionInput("pct_overflow", pct=100.1, total=1),
        CoverageDimensionInput("covered_overflow", pct=50.0, total=1, covered=2),
    ],
)
def test_invalid_dimension_inputs_raise_value_error(dimension):
    with pytest.raises(ValueError):
        compute_objective_coverage([dimension])
