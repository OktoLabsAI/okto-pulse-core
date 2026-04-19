"""Unit tests for services/analytics_service.py pure functions.

Ideação #9 — garante que o service layer é a fonte canônica
de agregação para REST + MCP. Funções puras testadas com
fixtures mínimas, sem HTTP round-trip.
"""

from __future__ import annotations

from okto_pulse.core.services.analytics_service import (
    decisions_stats,
    filter_decisions_by_status,
    resolve_linked_criteria_to_indices,
    resolve_linked_fr_indices,
    spec_coverage_summary,
)


class TestResolveLinkedCriteria:
    def test_mixed_shapes_dedup(self):
        ac_list = ["AC0", "AC1", "AC2", "AC3"]
        out = resolve_linked_criteria_to_indices([0, "1", "AC2"], ac_list)
        assert out == {0, 1, 2}

    def test_out_of_range_dropped(self):
        assert resolve_linked_criteria_to_indices([99, "100", -1], ["A", "B"]) == set()

    def test_empty_inputs(self):
        assert resolve_linked_criteria_to_indices(None, ["A"]) == set()
        assert resolve_linked_criteria_to_indices([0], []) == set()

    def test_bool_rejected(self):
        # bool é subclass de int mas não deve virar 0/1
        assert resolve_linked_criteria_to_indices([True, False], ["A", "B", "C"]) == set()

    def test_whitespace_stripped(self):
        assert resolve_linked_criteria_to_indices([" 2 ", "  "], ["A", "B", "C", "D"]) == {2}


class TestResolveLinkedFR:
    def test_int_indices(self):
        assert resolve_linked_fr_indices([0, 2], ["FR0", "FR1", "FR2"]) == {0, 2}

    def test_text_match(self):
        frs = ["Endpoint returns 200", "Helper normalizes input"]
        assert resolve_linked_fr_indices(["Helper normalizes"], frs) == {1}

    def test_out_of_range(self):
        assert resolve_linked_fr_indices([99], ["A"]) == set()


class TestDecisionsFilter:
    FIXTURE = [
        {"id": "d1", "status": "active"},
        {"id": "d2", "status": "superseded"},
        {"id": "d3", "status": "revoked"},
        {"id": "d4"},  # legacy
    ]

    def test_default_active_plus_legacy(self):
        out = filter_decisions_by_status(self.FIXTURE)
        assert [d["id"] for d in out] == ["d1", "d4"]

    def test_include_superseded(self):
        out = filter_decisions_by_status(self.FIXTURE, include_superseded=True)
        assert len(out) == 4

    def test_empty(self):
        assert filter_decisions_by_status(None) == []
        assert filter_decisions_by_status([]) == []

    def test_non_dict_dropped(self):
        out = filter_decisions_by_status(self.FIXTURE + ["bad", 42])
        assert all(isinstance(d, dict) for d in out)


class TestDecisionsStats:
    def test_full_breakdown(self):
        fixture = [
            {"status": "active"},
            {"status": "active"},
            {"status": "superseded"},
            {"status": "revoked"},
            {"status": "custom"},  # other
            {},  # legacy → active
        ]
        out = decisions_stats(fixture)
        assert out == {"total": 6, "active": 3, "superseded": 1, "revoked": 1, "other": 1}

    def test_empty(self):
        assert decisions_stats(None) == {"total": 0, "active": 0, "superseded": 0, "revoked": 0, "other": 0}


class TestSpecCoverageSummary:
    class _FakeSpec:
        def __init__(self, **kwargs):
            self.acceptance_criteria = kwargs.get("acs", [])
            self.functional_requirements = kwargs.get("frs", [])
            self.test_scenarios = kwargs.get("scenarios", [])
            self.business_rules = kwargs.get("rules", [])
            self.api_contracts = kwargs.get("contracts", [])
            self.technical_requirements = kwargs.get("trs", [])

    def test_empty_spec_pct_100(self):
        # Spec sem nada → percentuais 100 (convenção pytest baseline)
        out = spec_coverage_summary(self._FakeSpec())
        assert out["ac_total"] == 0
        assert out["ac_coverage_pct"] == 100
        assert out["fr_coverage_pct"] == 100

    def test_mixed_coverage(self):
        spec = self._FakeSpec(
            acs=["AC0", "AC1", "AC2"],
            scenarios=[{"linked_criteria": [0, "AC1"]}],
        )
        out = spec_coverage_summary(spec)
        assert out["ac_covered"] == 2
        assert out["ac_uncovered_indices"] == [2]

    def test_task_linkage(self):
        spec = self._FakeSpec(
            trs=[
                {"text": "TR0", "linked_task_ids": ["card-1"]},
                {"text": "TR1", "linked_task_ids": []},
            ],
        )
        out = spec_coverage_summary(spec)
        assert out["trs_total"] == 2
        assert out["trs_linked"] == 1
        assert out["tr_task_linkage_pct"] == 50.0
