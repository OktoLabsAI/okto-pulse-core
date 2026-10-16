"""Exact-ID coverage remains stable across reorder; retired converters stay absent."""

from __future__ import annotations

from pathlib import Path

from okto_pulse.core.services.analytics_service import spec_coverage_summary

CORE = Path(__file__).resolve().parent.parent / "src" / "okto_pulse" / "core"


class _CovSpec:
    """Minimal spec shape accepted by spec_coverage_summary (pure function)."""

    def __init__(self, frs, business_rules):
        self.functional_requirements = frs
        self.acceptance_criteria = []
        self.business_rules = business_rules
        self.test_scenarios = []
        self.api_contracts = []
        self.technical_requirements = []
        self.decisions = []
        self.observability_requirements = []
        self.integration_requirements = []


def _br(ref: str) -> list:
    return [{
        "id": "br_x", "title": "R", "rule": "R", "when": "W", "then": "T",
        "linked_requirements": [ref],
    }]


# ---------------------------------------------------------------------------
# AC7 (ts_e8290ee8) — parity: index (legacy) and fr_id (new) → SAME result
# ---------------------------------------------------------------------------


def test_current_id_coverage_survives_reorder_without_positional_identity():
    frs = [{"id": "fr_a", "text": "A"}, {"id": "fr_b", "text": "B"}]
    first = spec_coverage_summary(_CovSpec(frs, _br("fr_a")))
    reordered = spec_coverage_summary(_CovSpec(list(reversed(frs)), _br("fr_a")))
    assert first["fr_covered"] == reordered["fr_covered"] == 1
    assert first["fr_uncovered_indices"] == [1]
    assert reordered["fr_uncovered_indices"] == [0]
    invalid = spec_coverage_summary(_CovSpec(frs, _br("0")))
    assert invalid["fr_covered"] == 0


def test_no_requirement_migration_in_write_surfaces():
    """Clean-break supersedes the former lazy-conversion obligation."""
    for path in ("services/spec_structured_entities.py", "services/main.py", "mcp/server.py"):
        source = (CORE / path).read_text(encoding="utf-8")
        assert "migrate_legacy_fr_refs" not in source
        assert "migrate_legacy_ac_refs" not in source
        assert "materialized_items" not in source
