"""FCC-07B-IMP2 — RemovalEvidenceBinding contract + fail-closed binding gate.

These are the IMPLEMENTATION tests of the BINDING layer ABOVE the IMP1
aggregator. They prove (a) the TR6 nine-field contract, (b) the binding fills the
three provenance fields IMP1 leaves ``None`` (FR5), (c) ``dependency_audit_passed``
is an INPUT consumed from FCC-07C (never recomputed), (d) the four fail-closed
conditions (TR5): unknown adapter_key (AC4), declared-removal-without-evidence
(AC5), duplicate binding, and evidence-without-removal, and (e) determinism.

Every FCC-07C value enters as a synthetic ``{adapter_key: AdapterEvidence}``
mapping (the core never imports ``okto_pulse.community``); one test additionally
drives the REAL FCC-07C projection through the binder for genuine interop teeth.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

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
    DIAG_DUPLICATE_BINDING,
    DIAG_EVIDENCE_WITHOUT_REMOVAL,
    DIAG_REMOVAL_WITHOUT_EVIDENCE,
    DIAG_UNKNOWN_ADAPTER_KEY,
    BindingDiagnostic,
    RemovalBindingReport,
    RemovalEvidenceBinder,
    RemovalEvidenceBinding,
    bind_and_validate_removals,
)

CORE_KEY = "filesystem_storage_provider"
COMMUNITY_KEY = "kuzu_graph_store"

TR6_FIELDS = {
    "adapter_key",
    "evidence",
    "declared_removal_ref",
    "owning_fcc",
    "source_module",
    "source_test_or_oracle",
    "oracles_required",
    "evidence_field_impact",
    "remediation",
}


def _by_key(adapter_key: str):
    return next(e for e in build_adapter_inventory() if e.adapter_key == adapter_key)


def _full_evidence() -> AdapterEvidence:
    return AdapterEvidence(**{name: True for name in REQUIRED_EVIDENCE})


def _ready_binding(adapter_key: str, **overrides) -> RemovalEvidenceBinding:
    kw = dict(
        evidence=_full_evidence(),
        declared_removal_ref=f"{adapter_key}-removal done",
        source_module=f"okto_pulse/core/{adapter_key}.py",
        source_test_or_oracle=f"test_{adapter_key}_removed",
    )
    kw.update(overrides)
    return RemovalEvidenceBinding.for_adapter(_by_key(adapter_key), **kw)


# ===========================================================================
# TR6 — the binding contract has EXACTLY the nine reconciled fields.
# ===========================================================================
def test_tr6_binding_has_exactly_nine_contract_fields():
    names = {f.name for f in dataclasses.fields(RemovalEvidenceBinding)}
    assert names == TR6_FIELDS
    # evidence is the canonical AdapterEvidence DTO (or None for the missing case).
    binding = _ready_binding(CORE_KEY)
    assert isinstance(binding.evidence, AdapterEvidence)
    # inventory-derived fields are populated by the factory.
    entry = _by_key(CORE_KEY)
    assert binding.owning_fcc == entry.wave
    assert binding.oracles_required == entry.oracles_required
    assert binding.remediation == (entry.removal_criterion or None)
    # the static evidence_field_impact annotation covers every REQUIRED_EVIDENCE
    # field and flags dependency_audit_passed as FCC-07C sourced.
    impact = dict(binding.evidence_field_impact)
    assert set(impact) == set(REQUIRED_EVIDENCE)
    assert "fcc07c" in impact["dependency_audit_passed"]


# ===========================================================================
# FR5 — the binding fills the three provenance fields IMP1 leaves None.
# ===========================================================================
def test_binding_fills_imp1_none_provenance_fields():
    # IMP1 alone leaves the provenance fields None...
    imp1 = aggregate_adapter_readiness({CORE_KEY: _full_evidence()})
    imp1_row = imp1.row_for(CORE_KEY)
    assert imp1_row is not None
    assert imp1_row.source_module is None
    assert imp1_row.source_test_or_oracle is None
    assert imp1_row.declared_removal_ref is None

    binding = _ready_binding(
        CORE_KEY,
        declared_removal_ref="FCC-STORAGE removed",
        source_module="okto_pulse/core/infra/storage.py",
        source_test_or_oracle="test_storage_provider_removed",
    )
    report = bind_and_validate_removals([binding])
    row = report.row_for(CORE_KEY)
    assert row is not None
    assert row.status == "ready"  # full evidence + bound removal
    # ...the binding layer FILLS them.
    assert row.source_module == "okto_pulse/core/infra/storage.py"
    assert row.source_test_or_oracle == "test_storage_provider_removed"
    assert row.declared_removal_ref == "FCC-STORAGE removed"
    # the rich IMP1 fields are preserved verbatim (no re-derivation).
    entry = _by_key(CORE_KEY)
    assert row.owning_fcc == entry.wave
    assert row.oracles_required == entry.oracles_required
    assert row.missing_evidence == imp1_row.missing_evidence
    assert row.reasons == imp1_row.reasons
    assert report.ok is True
    assert report.status == "ready"


# ===========================================================================
# AC4 — evidence for an adapter_key NOT in build_adapter_inventory -> structured
# unknown error and NO adapter is marked ready (even a valid ready binding).
# ===========================================================================
def test_ac4_unknown_adapter_key_fails_closed_with_no_ready():
    good = _ready_binding(CORE_KEY)  # would be ready on its own
    unknown = RemovalEvidenceBinding(
        adapter_key="not_a_real_adapter",
        evidence=_full_evidence(),
        declared_removal_ref="bogus removal",
        source_module="nowhere.py",
        source_test_or_oracle="test_nowhere",
    )

    report = bind_and_validate_removals([good, unknown])

    assert report.ok is False
    diag = next(d for d in report.diagnostics if d.code == DIAG_UNKNOWN_ADAPTER_KEY)
    # structured diagnostic lists adapter_key + origin + corrective action (FR4).
    assert diag.adapter_key == "not_a_real_adapter"
    assert diag.origin == "declared_binding"
    assert diag.remediation
    assert "REQUIRED_ADAPTER_KEYS" in (diag.remediation or "")
    # NO adapter is marked ready — the otherwise-ready CORE binding is suppressed.
    assert all(r.status != "ready" for r in report.rows)
    assert report.ready_suppressed is True
    assert report.status == "blocked"
    core_row = report.row_for(CORE_KEY)
    assert core_row is not None and core_row.status == "blocked"
    # the unknown key produces no readiness row.
    assert report.row_for("not_a_real_adapter") is None


# ===========================================================================
# AC4 (FCC-07C source) — an unknown adapter_key in the dependency_audit_evidence
# projection (the Community/packaging evidence) is the SAME structural fail-closed
# as an unknown binding: no adapter is marked ready, diagnostic origin marks the
# FCC-07C source. (Closes the gap where only declared bindings were checked.)
# ===========================================================================
def test_ac4_unknown_adapter_key_from_fcc07c_evidence_fails_closed():
    good = _ready_binding(CORE_KEY)  # would be ready on its own
    # The FCC-07C projection supplies a value for a known adapter AND an unknown
    # one (an adapter_key absent from build_adapter_inventory()).
    c_evidence = {
        CORE_KEY: AdapterEvidence(dependency_audit_passed=True),
        "not_a_real_adapter": AdapterEvidence(dependency_audit_passed=True),
    }

    report = bind_and_validate_removals(
        [good], dependency_audit_evidence=c_evidence
    )

    assert report.ok is False
    diag = next(
        d
        for d in report.diagnostics
        if d.code == DIAG_UNKNOWN_ADAPTER_KEY
        and d.adapter_key == "not_a_real_adapter"
    )
    # FR4: the diagnostic distinguishes the FCC-07C source from a declared binding.
    assert diag.origin == "dependency_audit_evidence"
    assert "dependency_audit_evidence" in (diag.remediation or "")
    # NO adapter is marked ready — the otherwise-ready CORE binding is suppressed.
    assert all(r.status != "ready" for r in report.rows)
    assert report.ready_suppressed is True
    assert report.status == "blocked"
    # the unknown FCC-07C key produces no readiness row.
    assert report.row_for("not_a_real_adapter") is None


# ===========================================================================
# AC5 — a declared FCC removal (declared_removal_ref) with NO evidence -> the
# report flags a missing binding with owning_fcc/remediation, status blocked.
# ===========================================================================
def test_ac5_declared_removal_without_evidence_is_missing_binding_blocked():
    entry = _by_key(CORE_KEY)
    binding = RemovalEvidenceBinding.for_adapter(
        entry,
        evidence=None,  # removal declared "done" but no evidence supplied
        declared_removal_ref="FCC-STORAGE removed (done)",
        source_module="okto_pulse/core/infra/storage.py",
        source_test_or_oracle="test_storage_provider_removed",
    )

    report = bind_and_validate_removals([binding])

    assert report.ok is False
    diag = next(
        d for d in report.diagnostics if d.code == DIAG_REMOVAL_WITHOUT_EVIDENCE
    )
    assert diag.adapter_key == CORE_KEY
    assert diag.owning_fcc == entry.wave  # missing binding carries the owning FCC
    assert diag.remediation == (entry.removal_criterion or None)
    assert diag.declared_removal_ref == "FCC-STORAGE removed (done)"
    # a blocked row is emitted for the declared removal with the provenance filled.
    row = report.row_for(CORE_KEY)
    assert row is not None
    assert row.status == "blocked"
    assert "missing_binding_evidence" in row.reasons
    assert row.declared_removal_ref == "FCC-STORAGE removed (done)"
    assert row.missing_evidence == REQUIRED_EVIDENCE
    assert report.status == "blocked"


# ===========================================================================
# Fail-closed — duplicate evidence (two bindings for one adapter_key).
# ===========================================================================
def test_duplicate_binding_fails_closed_and_suppresses_ready():
    b1 = _ready_binding(CORE_KEY, declared_removal_ref="removal A")
    b2 = _ready_binding(CORE_KEY, declared_removal_ref="removal B")

    report = bind_and_validate_removals([b1, b2])

    assert report.ok is False
    diag = next(d for d in report.diagnostics if d.code == DIAG_DUPLICATE_BINDING)
    assert diag.adapter_key == CORE_KEY
    assert "2" in diag.message and diag.remediation
    # exactly one diagnostic for the duplicate key (reported once).
    assert sum(d.code == DIAG_DUPLICATE_BINDING for d in report.diagnostics) == 1
    # ambiguous adapter gets no row, and nothing is ready.
    assert report.row_for(CORE_KEY) is None
    assert all(r.status != "ready" for r in report.rows)
    assert report.status == "blocked"


# ===========================================================================
# Fail-closed — evidence WITHOUT a declared removal -> forced blocked (never
# ready from unbound evidence).
# ===========================================================================
def test_evidence_without_declared_removal_is_forced_blocked():
    binding = _ready_binding(CORE_KEY, declared_removal_ref=None)  # full evidence

    report = bind_and_validate_removals([binding])

    assert report.ok is False
    diag = next(
        d for d in report.diagnostics if d.code == DIAG_EVIDENCE_WITHOUT_REMOVAL
    )
    assert diag.adapter_key == CORE_KEY
    assert diag.remediation
    row = report.row_for(CORE_KEY)
    assert row is not None
    # full evidence would be ready in IMP1, but unbound evidence is forced blocked.
    assert row.status == "blocked"
    assert any("evidence_without_declared_removal" in r for r in row.reasons)
    assert report.status == "blocked"


# ===========================================================================
# dependency_audit_passed is an INPUT consumed from FCC-07C, never recomputed:
# the supplied value drives the IMP1 verdict; B does not re-audit packaging.
# ===========================================================================
def test_dependency_audit_passed_is_consumed_from_c_input():
    entry = _by_key(COMMUNITY_KEY)
    # every field True EXCEPT dependency_audit_passed, which is left to FCC-07C.
    ev = AdapterEvidence(
        port_closed=True,
        community_registered=True,
        oracle_passed=True,
        import_audit_passed=True,
        dependency_audit_passed=None,
        register_before_remove_passed=True,
    )
    binding = RemovalEvidenceBinding.for_adapter(
        entry,
        evidence=ev,
        declared_removal_ref="R-P2-05 done",
        source_module=entry.current_module,
        source_test_or_oracle="test_kuzu_graph_store_removed",
    )

    # FCC-07C says PASS -> the merged field flips the adapter to ready.
    c_pass = {COMMUNITY_KEY: AdapterEvidence(dependency_audit_passed=True)}
    rep_pass = bind_and_validate_removals(
        [binding], dependency_audit_evidence=c_pass
    )
    assert rep_pass.row_for(COMMUNITY_KEY).status == "ready"
    assert rep_pass.ok is True

    # FCC-07C says FAIL -> blocked with the field in failed_evidence (no recompute).
    c_fail = {COMMUNITY_KEY: AdapterEvidence(dependency_audit_passed=False)}
    rep_fail = bind_and_validate_removals(
        [binding], dependency_audit_evidence=c_fail
    )
    row_fail = rep_fail.row_for(COMMUNITY_KEY)
    assert row_fail.status == "blocked"
    assert "dependency_audit_passed" in row_fail.failed_evidence

    # No FCC-07C input at all -> the field stays None (missing) -> blocked/missing.
    rep_none = bind_and_validate_removals([binding])
    assert "dependency_audit_passed" in rep_none.row_for(COMMUNITY_KEY).missing_evidence


def test_c_value_overrides_binding_declared_dependency_audit():
    """FCC-07C is the SINGLE authorized source: its value overrides the binding."""
    entry = _by_key(COMMUNITY_KEY)
    # binding optimistically declares dependency_audit_passed=True...
    ev = AdapterEvidence(**{name: True for name in REQUIRED_EVIDENCE})
    binding = RemovalEvidenceBinding.for_adapter(
        entry,
        evidence=ev,
        declared_removal_ref="R-P2-05 done",
        source_module=entry.current_module,
        source_test_or_oracle="test_kuzu_graph_store_removed",
    )
    # ...but FCC-07C authoritatively says FALSE -> the merge overrides to blocked.
    c_fail = {COMMUNITY_KEY: AdapterEvidence(dependency_audit_passed=False)}
    report = bind_and_validate_removals([binding], dependency_audit_evidence=c_fail)
    row = report.row_for(COMMUNITY_KEY)
    assert row.status == "blocked"
    assert "dependency_audit_passed" in row.failed_evidence


# ===========================================================================
# Real FCC-07C interop — drive the ACTUAL community-audit projection (its
# as_adapter_evidence) through the binder (no okto_pulse.community import).
# ===========================================================================
def _synthetic_community_pyproject(base: Path, dependencies):
    path = base / "community_pyproject.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    deps = ", ".join(json.dumps(d) for d in dependencies)
    path.write_text(
        '[project]\nname = "synthetic-community"\nversion = "0"\n'
        f"dependencies = [{deps}]\n",
        encoding="utf-8",
    )
    return path


def _clean_core_ownership_report(base: Path):
    from okto_pulse.core.application.boundary.packaging_ownership_gate import (
        PackagingOwnershipGate,
        PackagingOwnershipGateInput,
    )

    pyproject = base / "pyproject.toml"
    pyproject.parent.mkdir(parents=True, exist_ok=True)
    pyproject.write_text(
        '[project]\nname = "synthetic-core"\nversion = "0"\ndependencies = []\n',
        encoding="utf-8",
    )
    core = base / "src" / "okto_pulse" / "core"
    core.mkdir(parents=True, exist_ok=True)
    (core / "__init__.py").write_text("", encoding="utf-8")
    return PackagingOwnershipGate().run(
        PackagingOwnershipGateInput(
            repo_root=base,
            pyproject_path=pyproject,
            lock_path=base / "missing.lock",
            source_root=base / "src",
            audit_wheel=False,
            include_import_boundary=False,
        )
    )


def test_real_fcc07c_projection_feeds_the_binder(tmp_path):
    from okto_pulse.core.application.boundary.community_packaging_audit import (
        run_community_packaging_audit,
    )

    entry = _by_key(COMMUNITY_KEY)
    base_evidence = dict(
        port_closed=True,
        community_registered=True,
        oracle_passed=True,
        import_audit_passed=True,
        dependency_audit_passed=None,  # supplied by FCC-07C below
        register_before_remove_passed=True,
    )
    binding = RemovalEvidenceBinding.for_adapter(
        entry,
        evidence=AdapterEvidence(**base_evidence),
        declared_removal_ref="R-P2-05 done",
        source_module=entry.current_module,
        source_test_or_oracle="test_kuzu_graph_store_removed",
    )
    clean_core = _clean_core_ownership_report(tmp_path / "core")

    # COMPLETE community manifest (declares ladybug) -> FCC-07C passes -> ready.
    good = _synthetic_community_pyproject(
        tmp_path / "good",
        ["okto-pulse-core>=0.3.0", "ladybug>=0.16", "sentence-transformers>=2.5"],
    )
    _, proj_good = run_community_packaging_audit(
        community_pyproject_path=good, ownership_report=clean_core
    )
    rep_good = bind_and_validate_removals(
        [binding], dependency_audit_evidence=proj_good.as_adapter_evidence()
    )
    assert rep_good.row_for(COMMUNITY_KEY).status == "ready"

    # MISSING ladybug -> FCC-07C blocks -> the binder's row is blocked on the field.
    bad = _synthetic_community_pyproject(
        tmp_path / "bad",
        ["okto-pulse-core>=0.3.0", "sentence-transformers>=2.5"],
    )
    _, proj_bad = run_community_packaging_audit(
        community_pyproject_path=bad, ownership_report=clean_core
    )
    rep_bad = bind_and_validate_removals(
        [binding], dependency_audit_evidence=proj_bad.as_adapter_evidence()
    )
    row_bad = rep_bad.row_for(COMMUNITY_KEY)
    assert row_bad.status == "blocked"
    assert "dependency_audit_passed" in row_bad.failed_evidence


# ===========================================================================
# A clean set of multiple valid bindings -> ok, every bound adapter ready.
# ===========================================================================
def test_multiple_valid_bindings_all_ready():
    bindings = [
        _ready_binding(CORE_KEY),
        _ready_binding("inmemory_cache_backend"),
        _ready_binding("singleton_scheduler_control"),
    ]
    report = bind_and_validate_removals(bindings)
    assert report.ok is True
    assert report.diagnostics == ()
    assert report.ready_suppressed is False
    assert report.status == "ready"
    assert {r.adapter_key for r in report.rows} == {
        CORE_KEY,
        "inmemory_cache_backend",
        "singleton_scheduler_control",
    }
    assert all(r.status == "ready" for r in report.rows)
    # rows cover ONLY the bound adapters (not the whole inventory).
    assert len(report.rows) == 3


# ===========================================================================
# Determinism + serialisability.
# ===========================================================================
def test_report_is_deterministic_and_serialisable():
    bindings = [
        _ready_binding("singleton_scheduler_control"),
        _ready_binding(CORE_KEY),
        RemovalEvidenceBinding.for_adapter(
            _by_key("inmemory_session_store"),
            evidence=None,
            declared_removal_ref="declared but unproven",
            source_module="x.py",
            source_test_or_oracle="t",
        ),
    ]
    first = bind_and_validate_removals(bindings)
    second = bind_and_validate_removals(bindings)

    assert first == second
    assert first.as_dict() == second.as_dict()
    json.dumps(first.as_dict(), sort_keys=True)  # JSON-serialisable
    # rows sorted by adapter_key; diagnostics sorted by (adapter_key, code).
    keys = [r.adapter_key for r in first.rows]
    assert keys == sorted(keys)
    diag_keys = [(d.adapter_key, d.code) for d in first.diagnostics]
    assert diag_keys == sorted(diag_keys)


# ===========================================================================
# Input validation — only RemovalEvidenceBinding / AdapterEvidence accepted.
# ===========================================================================
def test_rejects_parallel_binding_dto():
    @dataclasses.dataclass(frozen=True)
    class ParallelBinding:  # look-alike, not the canonical binding DTO
        adapter_key: str = CORE_KEY

    with pytest.raises(TypeError) as exc:
        bind_and_validate_removals([ParallelBinding()])  # type: ignore[list-item]
    assert "RemovalEvidenceBinding" in str(exc.value)


def test_rejects_non_adapter_evidence_in_c_map():
    binding = _ready_binding(CORE_KEY)
    with pytest.raises(TypeError):
        bind_and_validate_removals(
            [binding], dependency_audit_evidence={CORE_KEY: object()}  # type: ignore[dict-item]
        )


# ===========================================================================
# RemovalEvidenceBinder thin handle delegates to the function.
# ===========================================================================
def test_binder_handle_delegates_to_function():
    bindings = [_ready_binding(CORE_KEY)]
    via_handle = RemovalEvidenceBinder().bind(bindings)
    via_func = bind_and_validate_removals(bindings)
    assert isinstance(via_handle, RemovalBindingReport)
    assert via_handle == via_func


# ===========================================================================
# The core module must NEVER import okto_pulse.community.
# ===========================================================================
def test_core_module_does_not_import_community():
    module = Path(
        "src/okto_pulse/core/application/boundary/removal_evidence_binding.py"
    )
    source = module.read_text(encoding="utf-8")
    assert "import okto_pulse.community" not in source
    assert "from okto_pulse.community" not in source


def test_diagnostic_is_frozen_and_serialisable():
    diag = BindingDiagnostic(
        code=DIAG_UNKNOWN_ADAPTER_KEY,
        adapter_key="x",
        origin="declared_binding",
        message="m",
    )
    assert diag.blocking is True
    json.dumps(diag.as_dict(), sort_keys=True)
