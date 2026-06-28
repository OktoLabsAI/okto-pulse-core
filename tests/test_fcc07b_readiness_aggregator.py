"""FCC-07B-IMP1 — canonical readiness AGGREGATOR over AdapterEvidence.

These are the IMPLEMENTATION tests of the aggregator. The aggregator is a layer
OVER ``evaluate_adapter_readiness`` — it never re-implements the verdict — so the
tests prove (a) it preserves that semantics verbatim (FR2, anti parallel-DTO),
(b) AdapterEvidence is the only accepted evidence DTO (FR1), (c) the report is a
deterministic, machine-readable projection containing every FR5 field, and
(d) the four readiness ACs (AC1/AC2/AC3/AC6).

AC6 uses a SYNTHETIC AdapterEvidence with dependency_audit_passed=False to stand
in for a Community Onda-A dependency audit — the core never imports the Community
function; the evidence enters purely as external input.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
    REQUIRED_EVIDENCE,
    AdapterEvidence,
    build_adapter_inventory,
    evaluate_adapter_readiness,
)
from okto_pulse.core.application.boundary.readiness_aggregator import (
    FAILED_IMPACT,
    GATEWAY_IMPACT,
    MISSING_IMPACT,
    ReadinessAggregateReport,
    ReadinessAggregateRow,
    ReadinessAggregator,
    aggregate_adapter_readiness,
)

# Minimum machine-readable fields the FR5 report row must always expose.
FR5_REQUIRED_FIELDS = {
    "adapter_key",
    "status",
    "missing_evidence",
    "failed_evidence",
    "owning_fcc",
    "source_module",
    "source_test_or_oracle",
    "next_specs",
    "oracles_required",
    "evidence_field_impact",
    "remediation",
}

# A core-owned adapter and a Community-owned adapter present in the inventory.
CORE_KEY = "filesystem_storage_provider"
COMMUNITY_KEY = "kuzu_graph_store"


def _by_key(adapter_key: str):
    return next(e for e in build_adapter_inventory() if e.adapter_key == adapter_key)


def _full_evidence() -> AdapterEvidence:
    return AdapterEvidence(**{name: True for name in REQUIRED_EVIDENCE})


# ===========================================================================
# AC1 — six REQUIRED_EVIDENCE=True -> ready, no missing/failed.
# ===========================================================================
def test_ac1_all_evidence_true_is_ready_without_missing_or_failed():
    report = aggregate_adapter_readiness({CORE_KEY: _full_evidence()})
    row = report.row_for(CORE_KEY)
    assert row is not None
    assert row.status == "ready"
    assert row.missing_evidence == ()
    assert row.failed_evidence == ()
    assert row.evidence_field_impact == ()  # nothing unsatisfied
    # the verdict's next_specs (wave) flows through for a ready adapter.
    assert row.next_specs == (_by_key(CORE_KEY).wave,)


# ===========================================================================
# AC2 — port_closed None OR False -> deferred ("removal cannot be declared
# complete"); the predecessor specs must close the port first.
# ===========================================================================
@pytest.mark.parametrize("port_value", [None, False])
def test_ac2_port_not_true_is_deferred(port_value):
    # every OTHER field present so the ONLY gating reason is the open port.
    kw = {name: True for name in REQUIRED_EVIDENCE}
    kw["port_closed"] = port_value
    report = aggregate_adapter_readiness({CORE_KEY: AdapterEvidence(**kw)})
    row = report.row_for(CORE_KEY)
    assert row is not None
    assert row.status == "deferred", (
        "port_closed not True must defer: removal cannot be declared complete"
    )
    assert "port_not_closed" in row.reasons
    # the gateway field surfaces in evidence_field_impact deterministically.
    impact = dict(row.evidence_field_impact)
    assert impact.get("port_closed") == GATEWAY_IMPACT
    # next_specs lists EVERY gating predecessor (never collapsed).
    assert row.next_specs == _by_key(CORE_KEY).predecessor_refs


# ===========================================================================
# AC3 — any field != port_closed as None/False (with the port closed) -> blocked
# and the exact field lands in missing_evidence OR failed_evidence.
# ===========================================================================
@pytest.mark.parametrize(
    "field", [n for n in REQUIRED_EVIDENCE if n != "port_closed"]
)
def test_ac3_missing_non_port_field_blocks_and_lands_in_missing(field):
    kw = {name: True for name in REQUIRED_EVIDENCE}
    kw[field] = None  # absent
    report = aggregate_adapter_readiness({CORE_KEY: AdapterEvidence(**kw)})
    row = report.row_for(CORE_KEY)
    assert row is not None
    assert row.status == "blocked"
    assert field in row.missing_evidence
    assert field not in row.failed_evidence
    assert dict(row.evidence_field_impact)[field] == MISSING_IMPACT


@pytest.mark.parametrize(
    "field", [n for n in REQUIRED_EVIDENCE if n != "port_closed"]
)
def test_ac3_failed_non_port_field_blocks_and_lands_in_failed(field):
    kw = {name: True for name in REQUIRED_EVIDENCE}
    kw[field] = False  # failed
    report = aggregate_adapter_readiness({CORE_KEY: AdapterEvidence(**kw)})
    row = report.row_for(CORE_KEY)
    assert row is not None
    assert row.status == "blocked"
    assert field in row.failed_evidence
    assert field not in row.missing_evidence
    assert dict(row.evidence_field_impact)[field] == FAILED_IMPACT


# ===========================================================================
# AC6 — Community Onda-A dependency audit FAILED (synthetic AdapterEvidence,
# NOT the Community function) -> blocked + dependency_audit_passed in failed.
# ===========================================================================
def test_ac6_community_dependency_audit_failed_blocks_with_failed_field():
    # SYNTHETIC evidence standing in for a Community Onda-A audit result. The
    # port is closed and all else passes, so the ONLY defect is the failed audit.
    kw = {name: True for name in REQUIRED_EVIDENCE}
    kw["dependency_audit_passed"] = False
    synthetic = AdapterEvidence(**kw)

    report = aggregate_adapter_readiness({COMMUNITY_KEY: synthetic})
    row = report.row_for(COMMUNITY_KEY)
    assert row is not None
    assert row.status == "blocked"
    assert "dependency_audit_passed" in row.failed_evidence
    assert "dependency_audit_passed" not in row.missing_evidence
    assert dict(row.evidence_field_impact)["dependency_audit_passed"] == FAILED_IMPACT


# ===========================================================================
# FR2 / anti parallel-DTO — the aggregator DELEGATES to evaluate_adapter_readiness
# verbatim for EVERY adapter_key (status/missing/failed/next_specs all match).
# ===========================================================================
def test_delegates_to_evaluate_adapter_readiness_for_every_key():
    inventory = build_adapter_inventory()
    # deterministic, varied evidence per key: full / partial / failed / empty.
    evidence_by_key: dict[str, AdapterEvidence] = {}
    for idx, entry in enumerate(inventory):
        bucket = idx % 4
        if bucket == 0:
            evidence_by_key[entry.adapter_key] = _full_evidence()
        elif bucket == 1:
            evidence_by_key[entry.adapter_key] = AdapterEvidence(
                port_closed=True, community_registered=True
            )
        elif bucket == 2:
            # port closed, all present, but one oracle FAILED -> blocked path.
            kw = {n: True for n in REQUIRED_EVIDENCE}
            kw["oracle_passed"] = False
            evidence_by_key[entry.adapter_key] = AdapterEvidence(**kw)
        # bucket == 3 -> no evidence supplied (empty -> deferred)

    report = aggregate_adapter_readiness(evidence_by_key)
    assert len(report.rows) == len(inventory)
    for entry in inventory:
        ev = evidence_by_key.get(entry.adapter_key, AdapterEvidence())
        expected = evaluate_adapter_readiness(entry, ev)
        row = report.row_for(entry.adapter_key)
        assert row is not None
        assert row.status == expected.status
        assert row.missing_evidence == expected.missing_evidence
        assert row.failed_evidence == expected.failed_evidence
        assert row.next_specs == expected.next_specs
        assert row.reasons == expected.reasons
        # owning_fcc / oracles_required come from the inventory entry.
        assert row.owning_fcc == entry.wave
        assert row.oracles_required == entry.oracles_required


# ===========================================================================
# Determinism — same input -> identical, sorted-by-adapter_key report.
# ===========================================================================
def test_report_is_deterministic_and_sorted_by_adapter_key():
    inventory = build_adapter_inventory()
    evidence = {CORE_KEY: _full_evidence(), COMMUNITY_KEY: AdapterEvidence()}

    first = aggregate_adapter_readiness(evidence)
    second = aggregate_adapter_readiness(evidence)

    keys = [row.adapter_key for row in first.rows]
    assert keys == sorted(keys), "rows must be ordered by adapter_key"
    assert len(keys) == len(inventory)
    # full structural equality across runs (frozen dataclasses are comparable).
    assert first == second
    assert first.as_dict() == second.as_dict()


# ===========================================================================
# missing/failed/oracles/next_specs/impact are ALWAYS tuples, NEVER None.
# ===========================================================================
def test_array_fields_are_never_none():
    # empty evidence for everything -> exercises the deferred path for all rows.
    report = aggregate_adapter_readiness({})
    assert report.rows  # the inventory is non-empty
    for row in report.rows:
        for value in (
            row.reasons,
            row.missing_evidence,
            row.failed_evidence,
            row.oracles_required,
            row.next_specs,
            row.evidence_field_impact,
        ):
            assert isinstance(value, tuple)
        # machine-readable projection keeps them as (possibly empty) lists/maps.
        d = row.as_dict()
        assert isinstance(d["missing_evidence"], list)
        assert isinstance(d["failed_evidence"], list)
        assert isinstance(d["evidence_field_impact"], dict)


# ===========================================================================
# FR5 — every row exposes the machine-readable minimum field set.
# ===========================================================================
def test_fr5_report_row_exposes_required_fields():
    report = aggregate_adapter_readiness({CORE_KEY: _full_evidence()})
    assert report.rows
    for row in report.rows:
        d = row.as_dict()
        assert FR5_REQUIRED_FIELDS <= set(d), (
            f"missing FR5 fields: {FR5_REQUIRED_FIELDS - set(d)}"
        )
    # report-level projection is JSON-shaped and stable.
    top = report.as_dict()
    assert set(top) == {"status_counts", "rows"}
    assert sum(top["status_counts"].values()) == len(report.rows)


# ===========================================================================
# B-IMP1 scope — binding-only fields stay None (FCC-07B-IMP2 fills them).
# ===========================================================================
def test_binding_only_fields_are_none_pending_imp2():
    report = aggregate_adapter_readiness({CORE_KEY: _full_evidence()})
    for row in report.rows:
        assert row.source_module is None
        assert row.source_test_or_oracle is None
        assert row.declared_removal_ref is None


# ===========================================================================
# FR1 — AdapterEvidence is the ONLY accepted evidence DTO; a parallel/look-alike
# DTO (identical field names) is rejected at the boundary.
# ===========================================================================
def test_fr1_rejects_parallel_evidence_dto():
    @dataclass(frozen=True)
    class ParallelEvidence:  # look-alike: same field names, different type
        port_closed: bool | None = True
        community_registered: bool | None = True
        oracle_passed: bool | None = True
        import_audit_passed: bool | None = True
        dependency_audit_passed: bool | None = True
        register_before_remove_passed: bool | None = True

    with pytest.raises(TypeError) as exc:
        aggregate_adapter_readiness({CORE_KEY: ParallelEvidence()})  # type: ignore[dict-item]
    assert "AdapterEvidence" in str(exc.value)


def test_fr1_rejects_non_mapping_input():
    with pytest.raises(TypeError):
        aggregate_adapter_readiness([(CORE_KEY, _full_evidence())])  # type: ignore[arg-type]


# ===========================================================================
# Scope boundary — unknown evidence keys are TOLERATED here (the unknown/
# duplicate fail-closed contract is FCC-07B-IMP2, NOT this layer).
# ===========================================================================
def test_unknown_evidence_key_is_tolerated_not_failed():
    report = aggregate_adapter_readiness(
        {CORE_KEY: _full_evidence(), "not_a_real_adapter": AdapterEvidence()}
    )
    # the unknown key produces no row and raises nothing.
    assert report.row_for("not_a_real_adapter") is None
    assert report.row_for(CORE_KEY) is not None
    assert {row.adapter_key for row in report.rows} == {
        e.adapter_key for e in build_adapter_inventory()
    }


# ===========================================================================
# ReadinessAggregator thin handle delegates to the function.
# ===========================================================================
def test_aggregator_class_delegates_to_function():
    evidence = {CORE_KEY: _full_evidence()}
    via_class = ReadinessAggregator().aggregate(evidence)
    via_func = aggregate_adapter_readiness(evidence)
    assert isinstance(via_class, ReadinessAggregateReport)
    assert isinstance(via_class.rows[0], ReadinessAggregateRow)
    assert via_class == via_func
