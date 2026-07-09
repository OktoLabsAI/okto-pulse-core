"""FCC-07C-IMP2 community-manifest audit + dependency_audit_passed projection.

These are the IMPLEMENTATION tests of the COMMUNITY-AWARE half of the packaging
gate. Every fixture is a synthetic ``tmp_path`` manifest (the Community
``pyproject.toml`` enters ONLY as a parameterizable file read — the core never
imports ``okto_pulse.community``), mirroring the FCC-07A / FCC-07C-IMP1 harness
so the assertions have real teeth and stay deterministic.

The audit REUSES the FCC-07A/R05-E dependency ledger classification (a required
dependency family is one the ledger marks ``community_owned``) and CONSUMES the
IMP1 ownership verdict (it never re-audits the core classification).
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
    AdapterEvidence,
    build_adapter_inventory,
    evaluate_adapter_readiness,
)
from okto_pulse.core.application.boundary.community_packaging_audit import (
    DIAG_COMMUNITY_DEPENDENCY_NOT_DECLARED,
    DIAG_COMMUNITY_DEPENDENCY_OPTIONAL_ONLY,
    audit_community_manifest,
    project_dependency_audit_evidence,
    run_community_packaging_audit,
)
from okto_pulse.core.application.boundary.packaging_ownership_gate import (
    PackagingOwnershipGate,
    PackagingOwnershipGateInput,
)

# Community-owned adapters that need the `ladybug` family.
_LADYBUG_ADAPTERS = {
    "kuzu_graph_store",
    "kuzu_cypher_executor",
    "kuzu_graph_schema_manager",
    "kuzu_graph_lifecycle",
    "kuzu_graph_transaction",
}
# Community-owned adapters that need the `sentence_transformers` family.
_SENTENCE_ADAPTERS = {
    "sentence_transformer_embedding_provider",
    "cross_encoder_reranker",
}


# --------------------------------------------------------------------------- #
# synthetic-manifest helpers
# --------------------------------------------------------------------------- #
def _community_pyproject(
    base: Path,
    *,
    dependencies: list[str] | tuple[str, ...] = (),
    optional: dict[str, list[str]] | None = None,
    name: str = "community_pyproject.toml",
) -> Path:
    path = base / name
    path.parent.mkdir(parents=True, exist_ok=True)
    deps = ", ".join(json.dumps(dep) for dep in dependencies)
    lines = ["[project]", 'name = "synthetic-community"', 'version = "0"',
             f"dependencies = [{deps}]"]
    if optional:
        lines.append("[project.optional-dependencies]")
        for group, specs in optional.items():
            joined = ", ".join(json.dumps(spec) for spec in specs)
            lines.append(f"{group} = [{joined}]")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _core_ownership_report(base: Path, *, dependencies: list[str] | tuple[str, ...] = ()):
    """Run the IMP1 packaging-ownership gate over a synthetic CORE repo."""
    pyproject = base / "pyproject.toml"
    pyproject.parent.mkdir(parents=True, exist_ok=True)
    deps = ", ".join(json.dumps(dep) for dep in dependencies)
    pyproject.write_text(
        '[project]\nname = "synthetic-core"\nversion = "0"\n'
        f"dependencies = [{deps}]\n",
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


_FULL_COMMUNITY_DEPS = [
    "okto-pulse-core>=0.3.0",
    "apscheduler>=3.10",
    "ladybug>=0.16",
    "requests>=2.32",
    "chardet>=5.0",
    "sentence-transformers>=2.5",
]


# ===========================================================================
# AC3 — a Community-owned adapter's required dependency family MISSING from the
# Community manifest is a BLOCKING finding (family + adapter_key + remediation).
# ===========================================================================
def test_ac3_missing_ladybug_blocks_with_family_adapter_and_remediation(tmp_path):
    # Community manifest declares sentence-transformers but NOT ladybug.
    community = _community_pyproject(
        tmp_path,
        dependencies=[
            "okto-pulse-core>=0.3.0",
            "apscheduler>=3.10",
            "requests>=2.32",
            "chardet>=5.0",
            "sentence-transformers>=2.5",
        ],
    )

    report = audit_community_manifest(community)

    assert report.ok is False
    assert report.community_pyproject_exists is True
    blocking_by_key = {f.adapter_key: f for f in report.blocking}
    # every ladybug-backed Community adapter is flagged...
    assert _LADYBUG_ADAPTERS <= set(blocking_by_key)
    finding = blocking_by_key["kuzu_graph_store"]
    assert finding.dependency_family == "ladybug"
    assert finding.declared is False
    assert finding.scope == "absent"
    assert finding.diagnostic_code == DIAG_COMMUNITY_DEPENDENCY_NOT_DECLARED
    # remediation points at the Community pyproject path the auditor read.
    assert community.as_posix() in (finding.remediation or "")
    assert "project.dependencies" in (finding.remediation or "")
    # ...and the satisfied sentence-transformers adapters are NOT blocking.
    assert not (set(blocking_by_key) & _SENTENCE_ADAPTERS)


def test_ac3_missing_dep_fails_dependency_audit_passed_for_that_adapter(tmp_path):
    community = _community_pyproject(
        tmp_path,
        dependencies=[
            "okto-pulse-core>=0.3.0",
            "apscheduler>=3.10",
            "requests>=2.32",
            "chardet>=5.0",
            "sentence-transformers>=2.5",
        ],
    )
    clean_core = _core_ownership_report(tmp_path / "core", dependencies=[])

    audit, projection = run_community_packaging_audit(
        community_pyproject_path=community, ownership_report=clean_core
    )

    evidence = projection.evidence_map()
    # ladybug-backed adapters fail their dependency audit (missing declaration)...
    for key in _LADYBUG_ADAPTERS:
        assert evidence[key] is False
    # ...sentence-transformers adapters still pass (their family IS declared).
    for key in _SENTENCE_ADAPTERS:
        assert evidence[key] is True
    assert projection.ok is False
    # the row carries the actionable reason.
    row = next(r for r in projection.rows if r.adapter_key == "kuzu_graph_store")
    assert row.community_blocking_families == ("ladybug",)
    assert any("community_dependency_not_declared:ladybug" in r for r in row.reasons)


# ===========================================================================
# AC7 — a COMPLETE Community manifest + a CLEAN core ownership verdict (no
# blocking) yields dependency_audit_passed=True for the Community adapters.
# ===========================================================================
def test_ac7_complete_manifest_and_clean_core_passes(tmp_path):
    community = _community_pyproject(tmp_path, dependencies=_FULL_COMMUNITY_DEPS)
    clean_core = _core_ownership_report(tmp_path / "core", dependencies=[])

    audit, projection = run_community_packaging_audit(
        community_pyproject_path=community, ownership_report=clean_core
    )

    assert audit.ok is True
    assert audit.blocking == ()
    assert clean_core.blocking == ()  # IMP1 verdict really is clean
    assert projection.ok is True
    evidence = projection.evidence_map()
    for key in _LADYBUG_ADAPTERS | _SENTENCE_ADAPTERS:
        assert evidence[key] is True
    # required families were actually exercised (not a vacuous pass).
    assert set(audit.required_families) == {
        "apscheduler",
        "chardet",
        "ladybug",
        "requests",
        "sentence_transformers",
    }


def test_ac7_evidence_consumable_as_adapter_evidence_for_fcc07b(tmp_path):
    community = _community_pyproject(tmp_path, dependencies=_FULL_COMMUNITY_DEPS)
    clean_core = _core_ownership_report(tmp_path / "core", dependencies=[])
    _, projection = run_community_packaging_audit(
        community_pyproject_path=community, ownership_report=clean_core
    )

    evidence = projection.as_adapter_evidence()
    sample = evidence["kuzu_graph_store"]
    assert isinstance(sample, AdapterEvidence)
    # C is the ONLY authorized source of dependency_audit_passed: it sets that
    # field and leaves every other evidence field untouched (None).
    assert sample.dependency_audit_passed is True
    assert sample.port_closed is None
    assert sample.community_registered is None
    assert sample.as_map()["dependency_audit_passed"] is True


def test_dependency_audit_passed_drives_fcc07b_readiness(tmp_path):
    """The projected field actually flips FCC-07B's readiness verdict."""
    inventory = build_adapter_inventory()
    entry = next(e for e in inventory if e.adapter_key == "kuzu_graph_store")
    clean_core = _core_ownership_report(tmp_path / "core", dependencies=[])

    good = _community_pyproject(tmp_path / "good", dependencies=_FULL_COMMUNITY_DEPS)
    bad = _community_pyproject(
        tmp_path / "bad",
        dependencies=[
            "okto-pulse-core>=0.3.0",
            "apscheduler>=3.10",
            "requests>=2.32",
            "chardet>=5.0",
            "sentence-transformers>=2.5",
        ],
    )
    _, proj_good = run_community_packaging_audit(
        community_pyproject_path=good, ownership_report=clean_core
    )
    _, proj_bad = run_community_packaging_audit(
        community_pyproject_path=bad, ownership_report=clean_core
    )

    base = AdapterEvidence(
        port_closed=True,
        community_registered=True,
        oracle_passed=True,
        import_audit_passed=True,
        dependency_audit_passed=proj_good.passed("kuzu_graph_store"),
        register_before_remove_passed=True,
    )
    assert evaluate_adapter_readiness(entry, base).status == "ready"

    failing = replace(base, dependency_audit_passed=proj_bad.passed("kuzu_graph_store"))
    verdict = evaluate_adapter_readiness(entry, failing)
    assert verdict.status == "blocked"
    assert "dependency_audit_passed" in verdict.failed_evidence


# ===========================================================================
# Projection CONSUMES the IMP1 verdict: a core LEAK fails the dependency audit
# even when the Community manifest declares the family (no re-audit, real reuse).
# ===========================================================================
def test_core_leak_fails_audit_even_when_community_declares(tmp_path):
    community = _community_pyproject(tmp_path, dependencies=_FULL_COMMUNITY_DEPS)
    # core manifest LEAKS ladybug -> IMP1 blocks the ladybug-backed adapters.
    leaky_core = _core_ownership_report(tmp_path / "core", dependencies=["ladybug>=0.16"])
    assert any(r.symbol == "ladybug" for r in leaky_core.blocking)

    audit, projection = run_community_packaging_audit(
        community_pyproject_path=community, ownership_report=leaky_core
    )

    # community manifest is COMPLETE on its own...
    assert audit.ok is True
    # ...but the projection still fails the ladybug adapters because IMP1 flagged
    # the core leak (the field consumes the ownership verdict, never re-derives it).
    evidence = projection.evidence_map()
    for key in _LADYBUG_ADAPTERS:
        assert evidence[key] is False
    row = next(r for r in projection.rows if r.adapter_key == "kuzu_graph_store")
    assert "ladybug" in row.core_ownership_blocking_symbols
    assert any("core_ownership_blocking:ladybug" in r for r in row.reasons)
    # an adapter NOT backed by the leaked family is unaffected.
    assert evidence["kuzu_graph_path_resolver"] is True


# ===========================================================================
# FR7 — runtime/optional/dev scope differentiation: a runtime adapter's family
# declared ONLY as an optional/dev extra is surfaced (warning), NOT blocked.
# ===========================================================================
def test_fr7_optional_only_family_is_warning_not_blocking(tmp_path):
    community = _community_pyproject(
        tmp_path,
        dependencies=[
            "okto-pulse-core>=0.3.0",
            "apscheduler>=3.10",
            "requests>=2.32",
            "chardet>=5.0",
            "sentence-transformers>=2.5",
        ],
        optional={"dev": ["ladybug>=0.16"]},  # ladybug ONLY as a dev extra
    )

    report = audit_community_manifest(community)

    assert report.ok is True  # declared (mis-scoped) -> not blocking
    assert report.blocking == ()
    warn_by_key = {f.adapter_key: f for f in report.warnings}
    assert _LADYBUG_ADAPTERS <= set(warn_by_key)
    finding = warn_by_key["kuzu_graph_store"]
    assert finding.declared is True
    assert finding.scope == "optional"
    assert finding.diagnostic_code == DIAG_COMMUNITY_DEPENDENCY_OPTIONAL_ONLY

    clean_core = _core_ownership_report(tmp_path / "core", dependencies=[])
    projection = project_dependency_audit_evidence(
        ownership_report=clean_core, community_audit=report
    )
    # optional-only is surfaced but the dependency_audit still passes.
    assert projection.evidence_map()["kuzu_graph_store"] is True
    row = next(r for r in projection.rows if r.adapter_key == "kuzu_graph_store")
    assert row.community_optional_only_families == ("ladybug",)


# ===========================================================================
# Required families REUSE the ledger: transitive packages (torch / huggingface_hub
# behind sentence-transformers) and stdlib are NOT required to be declared.
# ===========================================================================
def test_required_families_exclude_transitive_and_stdlib(tmp_path):
    # only sentence-transformers declared; torch/huggingface_hub absent.
    community = _community_pyproject(
        tmp_path,
        dependencies=[
            "okto-pulse-core>=0.3.0",
            "apscheduler>=3.10",
            "sentence-transformers>=2.5",
            "ladybug>=0.16",
            "requests>=2.32",
            "chardet>=5.0",
        ],
    )

    report = audit_community_manifest(community)

    # torch / huggingface_hub are NOT required (transitive of sentence-transformers).
    flagged_families = {f.dependency_family for f in report.findings}
    assert "torch" not in flagged_families
    assert "huggingface_hub" not in flagged_families
    assert "stdlib" not in flagged_families
    # embedding adapter is satisfied purely by the declared sentence-transformers.
    assert report.ok is True
    embed = [f for f in report.findings if f.adapter_key == "sentence_transformer_embedding_provider"]
    assert {f.dependency_family for f in embed} == {"sentence_transformers"}
    # kuzu_graph_path_resolver is stdlib-only -> no required family, no finding.
    assert not any(f.adapter_key == "kuzu_graph_path_resolver" for f in report.findings)


# ===========================================================================
# Fail-closed: a MISSING Community manifest flags every required family.
# ===========================================================================
def test_missing_community_pyproject_is_fail_closed(tmp_path):
    missing = tmp_path / "does_not_exist.toml"

    report = audit_community_manifest(missing)

    assert report.community_pyproject_exists is False
    assert report.ok is False
    blocked_keys = {f.adapter_key for f in report.blocking}
    assert _LADYBUG_ADAPTERS <= blocked_keys
    assert _SENTENCE_ADAPTERS <= blocked_keys


# ===========================================================================
# Determinism + serialisability.
# ===========================================================================
def test_projection_is_deterministic_and_serialisable(tmp_path):
    community = _community_pyproject(tmp_path, dependencies=_FULL_COMMUNITY_DEPS)
    clean_core = _core_ownership_report(tmp_path / "core", dependencies=[])

    _, first = run_community_packaging_audit(
        community_pyproject_path=community, ownership_report=clean_core
    )
    _, second = run_community_packaging_audit(
        community_pyproject_path=community, ownership_report=clean_core
    )

    assert first.as_dict() == second.as_dict()
    assert json.dumps(first.as_dict(), sort_keys=True)
    # rows are sorted by adapter_key (deterministic ordering).
    keys = [r.adapter_key for r in first.rows]
    assert keys == sorted(keys)
    # projection covers the whole inventory (every adapter gets the evidence field).
    assert len(first.rows) == len(build_adapter_inventory())


# ===========================================================================
# The core must NEVER import okto_pulse.community — the manifest enters by path.
# ===========================================================================
def test_core_module_does_not_import_community():
    module = Path(
        "src/okto_pulse/core/application/boundary/community_packaging_audit.py"
    )
    source = module.read_text(encoding="utf-8")
    assert "import okto_pulse.community" not in source
    assert "from okto_pulse.community" not in source
