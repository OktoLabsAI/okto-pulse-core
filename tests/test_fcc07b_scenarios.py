"""FCC-07B — scenario automation for the spec test_scenarios (ts_*).

One cohesive given/when/then test per scenario, traceable to its ts_id, driving
the SAME public surface the impl uses:

* ``ts_fd1258a2`` — complete evidence -> ready structured report (AC1, AC7).
* ``ts_2c5d9d85`` — missing/failed evidence stays blocked/deferred (AC2, AC3, AC6).
* ``ts_c08d7f33`` — unknown adapter + missing removal binding fail closed (AC4, AC5).

Entry points are deliberate: AC1/AC2/AC3/AC6 are the readiness AGGREGATOR
(``aggregate_adapter_readiness``); AC4/AC5 are the fail-closed binding GATE
(``bind_and_validate_removals``) — the raw aggregator tolerates an unknown
evidence key, the gate is what fails closed. The core never imports
``okto_pulse.community``; a synthetic ``AdapterEvidence`` stands in for the
Community Onda-A dependency audit (it enters purely as external input).
"""

from __future__ import annotations

import pytest

from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
    REQUIRED_EVIDENCE,
    AdapterEvidence,
    build_adapter_inventory,
)
from okto_pulse.core.application.boundary.readiness_aggregator import (
    aggregate_adapter_readiness,
)
from okto_pulse.core.application.boundary.removal_evidence_binding import (
    DIAG_REMOVAL_WITHOUT_EVIDENCE,
    DIAG_UNKNOWN_ADAPTER_KEY,
    RemovalEvidenceBinding,
    bind_and_validate_removals,
)

CORE_KEY = "filesystem_storage_provider"


def _by_key(adapter_key: str):
    return next(e for e in build_adapter_inventory() if e.adapter_key == adapter_key)


def _full_evidence() -> AdapterEvidence:
    return AdapterEvidence(**{name: True for name in REQUIRED_EVIDENCE})


def _evidence_with(**overrides) -> AdapterEvidence:
    kw = {name: True for name in REQUIRED_EVIDENCE}
    kw.update(overrides)
    return AdapterEvidence(**kw)


# ===========================================================================
# ts_fd1258a2 — complete AdapterEvidence produces a ready structured report.
# GIVEN an inventory adapter_key with all six REQUIRED_EVIDENCE True
# WHEN the FCC07B aggregator runs
# THEN the verdict is ready, missing/failed are empty, and the row exposes
#      adapter_key/status/owning_fcc/evidence fields for downstream gates.
# ===========================================================================
def test_ts_fd1258a2_complete_evidence_produces_ready_structured_report():
    report = aggregate_adapter_readiness({CORE_KEY: _full_evidence()})

    row = report.row_for(CORE_KEY)
    assert row is not None
    assert row.status == "ready"
    assert row.missing_evidence == ()
    assert row.failed_evidence == ()

    # AC7 — the row is machine-readable: downstream FCC07A/C/D/E filter by
    # adapter_key / owning_fcc / status / evidence_field_impact, no text parsing.
    projected = row.as_dict()
    for field in ("adapter_key", "status", "owning_fcc", "evidence_field_impact"):
        assert field in projected, field
    assert projected["adapter_key"] == CORE_KEY
    assert projected["status"] == "ready"
    # the whole report is a JSON-serialisable structured projection.
    assert CORE_KEY in {r["adapter_key"] for r in report.as_dict()["rows"]}


# ===========================================================================
# ts_2c5d9d85 — missing or failed evidence remains blocked/deferred.
# GIVEN port_closed=None, a non-port field False, and a synthetic Community
#       Onda-A evidence with dependency_audit_passed=False
# WHEN the aggregator evaluates them
# THEN the port case is deferred; the non-port case is blocked; Onda-A stays
#      blocked with dependency_audit_passed listed in failed_evidence.
# ===========================================================================
def test_ts_2c5d9d85_missing_or_failed_evidence_stays_blocked_or_deferred():
    # AC2 — an open port defers (removal cannot be declared complete).
    deferred = aggregate_adapter_readiness(
        {CORE_KEY: _evidence_with(port_closed=None)}
    ).row_for(CORE_KEY)
    assert deferred is not None and deferred.status == "deferred"

    # AC3 — a non-port field failing blocks, and the exact field is surfaced.
    blocked = aggregate_adapter_readiness(
        {CORE_KEY: _evidence_with(import_audit_passed=False)}
    ).row_for(CORE_KEY)
    assert blocked is not None and blocked.status == "blocked"
    assert "import_audit_passed" in (blocked.missing_evidence + blocked.failed_evidence)

    # AC6 — synthetic Community Onda-A evidence (dependency_audit_passed=False)
    # stays blocked with the field in failed_evidence (the audit FAILED, not absent).
    onda_a = _evidence_with(dependency_audit_passed=False)
    onda_row = aggregate_adapter_readiness({CORE_KEY: onda_a}).row_for(CORE_KEY)
    assert onda_row is not None and onda_row.status == "blocked"
    assert "dependency_audit_passed" in onda_row.failed_evidence


# ===========================================================================
# ts_c08d7f33 — unknown adapter and missing removal binding fail closed.
# GIVEN an evidence entry for an adapter_key absent from build_adapter_inventory()
#       and a declared FCC removal with no evidence binding
# WHEN the FCC07B binding gate runs
# THEN it fails without marking any unknown adapter ready, and the report lists
#      the unknown adapter_key plus the missing binding with owning/remediation.
# ===========================================================================
def test_ts_c08d7f33_unknown_adapter_and_missing_binding_fail_closed():
    # A declared removal with NO evidence (AC5) ...
    missing_binding = RemovalEvidenceBinding.for_adapter(
        _by_key(CORE_KEY),
        evidence=None,
        declared_removal_ref="FCC-STORAGE removed (done)",
        source_module="okto_pulse/core/infra/storage.py",
        source_test_or_oracle="test_storage_provider_removed",
    )
    # ... and an evidence binding for an adapter_key NOT in the inventory (AC4).
    unknown_binding = RemovalEvidenceBinding(
        adapter_key="not_a_real_adapter",
        evidence=_full_evidence(),
        declared_removal_ref="bogus removal",
        source_module="nowhere.py",
        source_test_or_oracle="test_nowhere",
    )

    report = bind_and_validate_removals([missing_binding, unknown_binding])

    assert report.ok is False
    assert report.status == "blocked"

    # AC4 — the unknown adapter_key is a structured fail-closed; no readiness row,
    # and nothing is marked ready.
    unknown_diag = next(
        d for d in report.diagnostics if d.code == DIAG_UNKNOWN_ADAPTER_KEY
    )
    assert unknown_diag.adapter_key == "not_a_real_adapter"
    assert unknown_diag.remediation
    assert report.row_for("not_a_real_adapter") is None
    assert all(r.status != "ready" for r in report.rows)

    # AC5 — the declared-removal-without-evidence is flagged as a missing binding
    # with owning_fcc + remediation, and its adapter is blocked.
    missing_diag = next(
        d for d in report.diagnostics if d.code == DIAG_REMOVAL_WITHOUT_EVIDENCE
    )
    assert missing_diag.adapter_key == CORE_KEY
    assert missing_diag.remediation
    core_row = report.row_for(CORE_KEY)
    assert core_row is not None and core_row.status == "blocked"


if __name__ == "__main__":  # pragma: no cover - convenience
    raise SystemExit(pytest.main([__file__, "-q"]))
