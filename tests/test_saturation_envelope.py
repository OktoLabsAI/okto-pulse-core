"""Ideação MCP-token-optimization Story 1 — saturation envelope tests.

Verifies that `spec_saturation_envelope` produces the {pct, blocking[]} shape
described in the ideation across the relevant coverage states (full, partial,
skip-flagged, empty).
"""

from __future__ import annotations

from okto_pulse.core.services.analytics_service import spec_saturation_envelope


def _coverage(**overrides) -> dict:
    """Baseline coverage dict with all dimensions at 100% and totals > 0."""
    base = {
        "ac_coverage_pct": 100.0, "ac_total": 4,
        "fr_coverage_pct": 100.0, "fr_total": 3,
        "scenario_task_linkage_pct": 100.0, "scenarios_total": 5,
        "br_task_linkage_pct": 100.0, "brs_total": 2,
        "contract_task_linkage_pct": 100.0, "contracts_total": 1,
        "tr_task_linkage_pct": 100.0, "trs_total": 2,
        "decisions_coverage_pct": 100.0, "decisions_total": 1,
        "ir_task_linkage_pct": 100.0, "irs_total": 1,
        "or_task_linkage_pct": 100.0, "ors_total": 1,
        "skip_test_coverage": False, "skip_rules_coverage": False,
        "skip_decisions_coverage": False, "skip_ir_coverage": False,
        "skip_or_coverage": False,
    }
    base.update(overrides)
    return base


def test_full_coverage_pct_100_blocking_empty():
    env = spec_saturation_envelope(_coverage())
    assert env == {"pct": 100.0, "blocking": []}


def test_partial_coverage_lowers_pct_and_lists_blocking():
    env = spec_saturation_envelope(
        _coverage(
            ac_coverage_pct=50.0,
            decisions_coverage_pct=0.0,
        )
    )
    assert env["pct"] < 100
    assert set(env["blocking"]) == {"acceptance_criteria", "decisions"}


def test_skip_flag_excludes_dimension_from_average():
    env = spec_saturation_envelope(
        _coverage(
            ac_coverage_pct=0.0,
            skip_test_coverage=True,
        )
    )
    assert env["pct"] == 100.0
    assert "acceptance_criteria" not in env["blocking"]
    assert "scenarios" not in env["blocking"]


def test_zero_totals_excluded_from_blocking():
    env = spec_saturation_envelope(
        _coverage(
            contracts_total=0,
            irs_total=0,
            ors_total=0,
        )
    )
    assert env == {"pct": 100.0, "blocking": []}


def test_blocking_order_is_canonical():
    env = spec_saturation_envelope(
        _coverage(
            or_task_linkage_pct=0.0,
            ac_coverage_pct=50.0,
            decisions_coverage_pct=80.0,
        )
    )
    assert env["blocking"].index("acceptance_criteria") < env["blocking"].index("decisions")
    assert env["blocking"].index("decisions") < env["blocking"].index("ors")


def test_empty_coverage_dict_returns_full_saturation():
    assert spec_saturation_envelope({}) == {"pct": 100.0, "blocking": []}


def test_pct_rounded_to_one_decimal():
    env = spec_saturation_envelope(
        _coverage(
            ac_coverage_pct=33.333,
            fr_coverage_pct=66.666,
        )
    )
    assert env["pct"] == round(env["pct"], 1)
    assert isinstance(env["pct"], float)
