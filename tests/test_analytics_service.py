"""Unit tests for services/analytics_service.py pure functions.

Ideação #9 — garante que o service layer é a fonte canônica
de agregação para REST + MCP. Funções puras testadas com
fixtures mínimas, sem HTTP round-trip.
"""

from __future__ import annotations

from okto_pulse.core.services.analytics_service import (
    decisions_stats,
    filter_decisions_by_status,
    render_decisions_markdown,
    resolve_linked_criteria_to_ids,
    resolve_linked_criteria_to_indices,
    resolve_linked_fr_indices,
    spec_coverage_summary,
)


class TestResolveLinkedCriteria:
    def test_exact_ids_dedup(self):
        ac_list = [{"id": f"ac_{i}", "text": f"AC{i}"} for i in range(4)]
        out = resolve_linked_criteria_to_indices(["ac_0", "ac_2", "ac_0"], ac_list)
        assert out == {0, 2}

    def test_out_of_range_dropped(self):
        assert resolve_linked_criteria_to_indices([99, "100", -1], ["A", "B"]) == set()

    def test_empty_inputs(self):
        assert resolve_linked_criteria_to_indices(None, ["A"]) == set()
        assert resolve_linked_criteria_to_indices([0], []) == set()

    def test_bool_rejected(self):
        # bool é subclass de int mas não deve virar 0/1
        assert resolve_linked_criteria_to_indices([True, False], ["A", "B", "C"]) == set()

    def test_identity_is_not_trimmed_or_converted(self):
        ac_list = [{"id": "ac_2", "text": "Criterion"}]
        assert resolve_linked_criteria_to_indices([" ac_2 ", "0", 0], ac_list) == set()


class TestResolveLinkedCriteriaToIds:
    """Write-path resolver: STRICT, fail-closed, exact-match, dedup-ordered.

    Spec aafcc73f / KB 26b0e005 — mirrors the read resolver but projects to
    canonical ac_ids and surfaces unresolved tokens instead of dropping them.
    """

    _ACS = [
        {"id": "ac_0001", "text": "User can log in with valid token"},
        {"id": "ac_0002", "text": "Session expires after timeout"},
        {"id": "ac_0003", "text": "Invalid token is rejected"},
    ]

    def test_index_is_unresolved(self):
        assert resolve_linked_criteria_to_ids(["0"], self._ACS) == ([], ["0"])

    def test_ac_id_resolves_to_itself(self):
        assert resolve_linked_criteria_to_ids(["ac_0002"], self._ACS) == (["ac_0002"], [])

    def test_exact_text_is_not_identity(self):
        assert resolve_linked_criteria_to_ids(
            ["Invalid token is rejected"], self._ACS
        ) == ([], ["Invalid token is rejected"])

    def test_prefix_is_unresolved_on_write(self):
        # Content prefixes are not identities on either path.
        resolved, unresolved = resolve_linked_criteria_to_ids(["User can log"], self._ACS)
        assert resolved == []
        assert unresolved == ["User can log"]

    def test_mixed_valid_invalid_is_fail_closed(self):
        resolved, unresolved = resolve_linked_criteria_to_ids(["ac_0001", "ghost"], self._ACS)
        assert resolved == ["ac_0001"]
        assert unresolved == ["ghost"]

    def test_out_of_range_index_unresolved(self):
        resolved, unresolved = resolve_linked_criteria_to_ids(["99"], self._ACS)
        assert resolved == []
        assert unresolved == ["99"]

    def test_dedup_preserves_order(self):
        # Repeated exact IDs collapse; first-seen order is retained.
        resolved, unresolved = resolve_linked_criteria_to_ids(
            ["ac_0002", "ac_0002", "ac_0001"], self._ACS
        )
        assert resolved == ["ac_0002", "ac_0001"]
        assert unresolved == []

    def test_bool_rejected(self):
        resolved, unresolved = resolve_linked_criteria_to_ids([True, False], self._ACS)
        assert resolved == []
        assert unresolved == ["True", "False"]

    def test_string_children_never_acquire_an_identity(self):
        legacy = ["AC0 legacy text", "AC1 legacy text"]
        assert resolve_linked_criteria_to_ids(["0"], legacy) == ([], ["0"])
        assert resolve_linked_criteria_to_ids(
            ["AC1 legacy text"], legacy
        ) == ([], ["AC1 legacy text"])

    def test_never_emits_dict(self):
        resolved, _ = resolve_linked_criteria_to_ids(["0", "ac_0002"], self._ACS)
        assert all(isinstance(x, str) for x in resolved)

    def test_empty_inputs(self):
        assert resolve_linked_criteria_to_ids(None, self._ACS) == ([], [])
        assert resolve_linked_criteria_to_ids([], self._ACS) == ([], [])

    def test_read_resolver_does_not_infer_identity_from_prefix(self):
        assert resolve_linked_criteria_to_indices(["User can log"], self._ACS) == set()

    def test_duplicate_child_identity_is_unresolved_without_mutation(self):
        children = [dict(self._ACS[0]), dict(self._ACS[0])]
        before = [dict(child) for child in children]
        assert resolve_linked_criteria_to_ids(["ac_0001"], children) == ([], ["ac_0001"])
        assert resolve_linked_criteria_to_indices(["ac_0001"], children) == set()
        assert children == before


class TestResolveLinkedFR:
    def test_exact_ids(self):
        frs = [{"id": f"fr_{i}", "text": f"FR{i}"} for i in range(3)]
        assert resolve_linked_fr_indices(["fr_0", "fr_2"], frs) == {0, 2}

    def test_text_and_indices_do_not_match(self):
        frs = [{"id": "fr_0", "text": "Endpoint returns 200"}]
        assert resolve_linked_fr_indices(["Endpoint returns", "Endpoint returns 200", 0, "0"], frs) == set()

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


class TestRenderDecisionsMarkdown:
    """Ideação #10 Fase 2 — markdown helper for agent consumption."""

    ACTIVE = {
        "id": "d1",
        "title": "Use Kùzu embedded over Neo4j",
        "status": "active",
        "rationale": "Embedded DB reduces operational complexity",
        "context": "Chosen during early KG design",
        "alternatives_considered": ["Neo4j", "PostgreSQL graph extensions"],
        "linked_requirements": [0, 2],
        "linked_task_ids": ["card-abc"],
    }
    SUPERSEDED = {
        "id": "d2",
        "title": "Cache layer deferido",
        "status": "superseded",
        "supersedes_decision_id": "d-earlier",
        "rationale": "Not needed yet",
    }

    def test_empty(self):
        assert render_decisions_markdown(None) == ""
        assert render_decisions_markdown([]) == ""

    def test_active_only_by_default(self):
        md = render_decisions_markdown([self.ACTIVE, self.SUPERSEDED])
        assert "Use Kùzu embedded over Neo4j" in md
        assert "Cache layer deferido" not in md
        assert "## Decisions" in md
        assert "(active)" in md

    def test_include_superseded(self):
        md = render_decisions_markdown([self.ACTIVE, self.SUPERSEDED], include_superseded=True)
        assert "Use Kùzu embedded over Neo4j" in md
        assert "Cache layer deferido" in md
        assert "(superseded)" in md
        assert "Supersedes" in md

    def test_missing_fields_omitted(self):
        minimal = {"id": "m1", "title": "Minimal", "status": "active"}
        md = render_decisions_markdown([minimal])
        assert "Minimal" in md
        # No bullets for unspecified fields
        assert "Rationale" not in md
        assert "Alternatives" not in md

    def test_non_dict_entries_dropped(self):
        md = render_decisions_markdown([self.ACTIVE, "bad", 42])
        assert "Use Kùzu embedded" in md
        assert "bad" not in md

    def test_all_superseded_with_flag_off_returns_empty(self):
        md = render_decisions_markdown([self.SUPERSEDED])
        assert md == ""


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
            acs=[{"id": f"ac_{i}", "text": f"AC{i}"} for i in range(3)],
            scenarios=[{"linked_criteria": ["ac_0", "ac_1"]}],
        )
        out = spec_coverage_summary(spec)
        assert out["ac_covered"] == 2
        assert out["ac_uncovered_indices"] == [2]

    def test_exact_id_links_count_for_ac_and_fr_coverage(self):
        spec = self._FakeSpec(
            acs=[{"id": f"ac_{i}", "text": f"AC{i}"} for i in range(2)],
            frs=[{"id": f"fr_{i}", "text": f"FR{i}"} for i in range(2)],
            scenarios=[{"linked_criteria": ["ac_0", "ac_1"]}],
            rules=[{"linked_requirements": ["fr_0", "fr_1"]}],
        )
        out = spec_coverage_summary(spec)
        assert out["ac_coverage_pct"] == 100
        assert out["ac_covered"] == 2
        assert out["fr_coverage_pct"] == 100
        assert out["fr_covered"] == 2
        assert out["fr_uncovered_indices"] == []

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

    def test_decisions_coverage_active_only(self):
        """Ideação #10 Fase 1 — decisions_planning_pct conta só active."""
        spec = self._FakeSpec()
        spec.id = 'spec'
        out = spec_coverage_summary(
            spec,
            decisions=[
                {"id": "d1", "status": "active", "linked_task_ids": [], 'verification': {'inspection': {
                    'condition': 'Observe scope', 'scope_refs': [{'kind': 'spec', 'id': 'spec'}]}}},
                {"id": "d2", "status": "active", "linked_task_ids": []},
                {"id": "d3", "status": "superseded"},  # não conta
                {"id": "d4"},  # legacy → active → sem linked
            ],
        )
        assert out["decisions_total"] == 3  # d1, d2, d4
        assert out["decisions_planned"] == 1  # only d1
        assert out["decisions_planning_pct"] == 33.3
        assert set(out["decisions_pending_ids"]) == {"d2", "d4"}

    def test_decisions_empty_spec_pct_100(self):
        spec = self._FakeSpec()
        out = spec_coverage_summary(spec)
        assert out["decisions_total"] == 0
        assert out["decisions_planning_pct"] == 100
        assert out["decisions_pending_ids"] == []
