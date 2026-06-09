"""R3b — canonical-id parity (AC7) + scope guard (AC8). Spec c61569b2.

The other 6 scenarios point to per-IMPL behavioral tests — each genuinely
exercises its own ``then`` (verified at the per-IMPL gates):
- AC1 ts_bfcc4474 -> tests/test_add_business_rule_structured_fr.py::test_add_business_rule_index_structured_fr
  (write persists the canonical fr_id in the RAW spec.business_rules column, not the index)
- AC2 ts_0a41792b -> tests/test_add_business_rule_structured_fr.py::test_add_business_rule_index_out_of_range
  (an unresolvable FR ref returns an "Unresolved" error, fail-closed)
- AC3 ts_ff3a7b6c -> tests/test_payload_compaction.py::test_list_business_rules_does_not_duplicate_fr_text
  (list_business_rules projection emits the fr_id, not the re-normalized index)
- AC4 ts_64b520dd -> tests/test_kg_deterministic_worker.py::test_impl3_decision_derives_from_resolves_via_fr_id
  (the KG worker resolves a derives_from edge by fr_id at confidence 1.0, not the 0.6 co-occurrence fallback)
- AC5 ts_6356cf07 -> tests/test_impl4_fr4_fr5.py::test_ac5_index_ref_resolves_same_as_fr_id_in_coverage
  (a legacy index-ref spec resolves in coverage; the permanent read-resolver is intact)
- AC6 ts_f3b81fd7 -> tests/test_impl4_fr4_fr5.py::test_ac6_update_spec_with_frs_migrates_linked_requirements_to_fr_ids
  (update_spec on a legacy spec materializes the index ref to fr_id — lazy on-touch)

This file adds AC7 (parity) and AC8 (scope guard).
"""

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


def test_ac7_parity_index_vs_fr_id_coverage():
    """AC7 — the same FR link expressed as a positional INDEX (legacy) and as
    the canonical fr_id (new) produces IDENTICAL coverage, via the permanent
    tolerant read-resolver."""
    frs = [
        {"id": "fr_aabb1122", "text": "User can register", "status": "active"},
        {"id": "fr_ccdd3344", "text": "User can log in", "status": "active"},
    ]
    cov_index = spec_coverage_summary(_CovSpec(frs, _br("0")))
    cov_fr_id = spec_coverage_summary(_CovSpec(frs, _br("fr_aabb1122")))

    for key in ("fr_covered", "fr_coverage_pct", "fr_uncovered_indices"):
        assert cov_index[key] == cov_fr_id[key], (
            f"parity broken on {key}: index={cov_index[key]} fr_id={cov_fr_id[key]}"
        )
    # Non-vacuous: the link genuinely resolved (1 of 2 covered), not both-zero.
    assert cov_index["fr_covered"] == 1, cov_index
    assert cov_index["fr_uncovered_indices"] == [1], cov_index


# ---------------------------------------------------------------------------
# AC8 (ts_5fe7ff0e) — scope guard: read-resolver kept, no batch, refs migrated lazy
# ---------------------------------------------------------------------------


def test_ac8_scope_guard_resolver_kept_and_no_batch():
    """AC8 — read-resolver preserved (FR4 permanent), the index-normalizer was
    cleaned, the lazy ref-migration is wired into update_spec, and NO batch /
    one-shot bulk refs-migration tool was added (owner rejected)."""
    analytics = (CORE / "services" / "analytics_service.py").read_text(encoding="utf-8")
    server = (CORE / "mcp" / "server.py").read_text(encoding="utf-8")
    sse = (CORE / "services" / "spec_structured_entities.py").read_text(encoding="utf-8")
    main = (CORE / "services" / "main.py").read_text(encoding="utf-8")

    # FR4 — the tolerant read-resolver is PERMANENT (not removed).
    assert "def resolve_linked_fr_indices" in analytics

    # IMPL-2 cleanup — the dead index-normalizer is gone from the write surface.
    assert "_parse_linked_requirements" not in server

    # FR5 — the lazy ref-migration helper exists and is wired into update_spec...
    assert "def migrate_legacy_fr_refs" in sse
    assert "migrate_legacy_fr_refs" in main  # the lazy on-touch plug

    # ...and is NOT exposed as a bulk MCP migration tool (no batch / one-shot).
    assert "migrate_legacy_fr_refs" not in server
