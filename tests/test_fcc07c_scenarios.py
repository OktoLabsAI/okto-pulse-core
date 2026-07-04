"""FCC-07C scenario tests (test_scenario cards — NOT the IMP1/IMP2 unit tests).

Each test below is a dedicated automation of ONE spec ``test_scenario`` and is
named + commented with its ``ts_id`` for traceability. They exercise the REAL
public API of the FCC-07C packaging ownership gate
(:func:`run_packaging_ownership_gate` /
:class:`PackagingOwnershipGate`) and its community-aware half
(:func:`run_community_packaging_audit` / :func:`audit_community_manifest`) over
synthetic, deterministic ``tmp_path`` fixtures.

The core NEVER imports ``okto_pulse.community`` — every Community manifest enters
ONLY as a parameterizable file read, exactly as the production module does.

Classification authority is the FCC-07A ``MatrixClassification`` enum
(``core_common`` / ``community_owned`` / ``future_adapter`` /
``temporary_exception`` / ``removed`` / ``unknown`` / ``test_only`` /
``violation``); these tests assert the EXACT enum values the gate delegates to,
never a re-derived spelling.
"""

from __future__ import annotations

import json
from pathlib import Path

from okto_pulse.core.application.boundary.community_packaging_audit import (
    DIAG_COMMUNITY_DEPENDENCY_NOT_DECLARED,
    audit_community_manifest,
    project_dependency_audit_evidence,
    run_community_packaging_audit,
)
from okto_pulse.core.application.boundary.dependency_conformance import (
    audit_dependency_conformance,
)
from okto_pulse.core.application.boundary.dependency_ledger import (
    build_dependency_ledger,
    normalize_token,
)
from okto_pulse.core.application.boundary.packaging_ownership_gate import (
    PackagingOwnershipGateInput,
    run_packaging_ownership_gate,
)

# Community-owned adapters whose required dependency family is `ladybug`.
_LADYBUG_ADAPTERS = {
    "kuzu_graph_store",
    "kuzu_cypher_executor",
    "kuzu_graph_schema_manager",
    "kuzu_graph_lifecycle",
    "kuzu_graph_transaction",
}
# Community-owned adapters whose required dependency family is `sentence_transformers`.
_SENTENCE_ADAPTERS = {
    "sentence_transformer_embedding_provider",
    "cross_encoder_reranker",
}


# --------------------------------------------------------------------------- #
# synthetic-fixture helpers (mirror tests/test_fcc07c_packaging_gate.py +
# tests/test_fcc07c_community_audit.py so the assertions keep real teeth).
# --------------------------------------------------------------------------- #
def _core_repo(
    base: Path,
    *,
    dependencies: list[str] | tuple[str, ...] = (),
    optional: dict[str, list[str]] | None = None,
    core_files: dict[str, str] | None = None,
) -> tuple[Path, Path]:
    pyproject = base / "pyproject.toml"
    pyproject.parent.mkdir(parents=True, exist_ok=True)
    deps = ", ".join(json.dumps(dep) for dep in dependencies)
    lines = [
        "[project]",
        'name = "synthetic-core"',
        'version = "0"',
        f"dependencies = [{deps}]",
    ]
    if optional:
        lines.append("[project.optional-dependencies]")
        for group, specs in optional.items():
            joined = ", ".join(json.dumps(spec) for spec in specs)
            lines.append(f"{group} = [{joined}]")
    pyproject.write_text("\n".join(lines) + "\n", encoding="utf-8")

    src = base / "src"
    core = src / "okto_pulse" / "core"
    core.mkdir(parents=True, exist_ok=True)
    (core / "__init__.py").write_text("", encoding="utf-8")
    for rel, content in (core_files or {}).items():
        target = core / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    return pyproject, src


def _wheel_metadata(base: Path, requires: list[str]) -> Path:
    meta = base / "okto_pulse_core-0.dist-info" / "METADATA"
    meta.parent.mkdir(parents=True, exist_ok=True)
    body = ["Metadata-Version: 2.1", "Name: okto-pulse-core", "Version: 0"]
    body += [f"Requires-Dist: {req}" for req in requires]
    meta.write_text("\n".join(body) + "\n", encoding="utf-8")
    return meta


def _run_gate(
    base: Path,
    pyproject: Path,
    src: Path,
    *,
    lock: Path | None = None,
    wheel_metadata: Path | None = None,
    audit_wheel: bool = False,
    dependency_report=None,
    include_import_boundary: bool = False,
):
    return run_packaging_ownership_gate(
        PackagingOwnershipGateInput(
            repo_root=base,
            pyproject_path=pyproject,
            lock_path=lock or (base / "missing.lock"),
            source_root=src,
            wheel_metadata_path=wheel_metadata,
            audit_wheel=audit_wheel,
            dependency_report=dependency_report,
            include_import_boundary=include_import_boundary,
        )
    )


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
    lines = [
        "[project]",
        'name = "synthetic-community"',
        'version = "0"',
        f"dependencies = [{deps}]",
    ]
    if optional:
        lines.append("[project.optional-dependencies]")
        for group, specs in optional.items():
            joined = ", ".join(json.dumps(spec) for spec in specs)
            lines.append(f"{group} = [{joined}]")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


_FULL_COMMUNITY_DEPS = [
    "okto-pulse-core>=0.3.0",
    "apscheduler>=3.10",
    "ladybug>=0.16",
    "sentence-transformers>=2.5",
]


# =========================================================================== #
# ts_10c9055e (negative, AC1 + AC2 + AC6)
# GIVEN the core manifest/source/wheel declare `ladybug`, `sentence-transformers`
#   and `asyncpg`.
# WHEN the FCC-07C packaging ownership gate runs with wheel audit enabled.
# THEN ladybug/sentence-transformers fail as `community_owned` with a
#   move-out-of-core remediation (AC1); asyncpg fails as `removed` on every
#   runtime surface (AC2); and a STALE wheel still shipping a removed dependency
#   despite a clean pyproject fails with a rebuild/reinstall remediation (AC6).
# =========================================================================== #
def test_ts_10c9055e_community_owned_and_removed_deps_fail(tmp_path):
    # --- AC1 + AC2: all three governed deps present in manifest+source+wheel ---
    base = tmp_path / "leaky"
    pyproject, src = _core_repo(
        base,
        dependencies=["ladybug>=0.16", "sentence-transformers>=2.0", "asyncpg>=0.29"],
        core_files={"db.py": "import asyncpg\n"},
    )
    wheel = _wheel_metadata(
        base, ["ladybug>=0.16", "sentence-transformers>=2.0", "asyncpg>=0.29"]
    )

    report = _run_gate(base, pyproject, src, wheel_metadata=wheel, audit_wheel=True)

    assert report.ok is False
    assert report.status == "blocking"

    # AC1 — ladybug + sentence-transformers classify as community_owned. The
    # MANIFEST occurrence carries the move-to-owning-edition remediation.
    ladybug_manifest = next(
        r
        for r in report.blocking
        if r.symbol == "ladybug" and r.surface == "manifest"
    )
    assert ladybug_manifest.classification == "community_owned"
    assert ladybug_manifest.scope == "runtime"
    assert "owning edition" in (ladybug_manifest.remediation or "").lower()
    sentence = next(
        r
        for r in report.blocking
        if normalize_token(r.symbol) == "sentence_transformers"
        and r.surface == "manifest"
    )
    assert sentence.classification == "community_owned"
    assert sentence.scope == "runtime"

    # AC2 — asyncpg is `removed` on EVERY runtime surface (manifest+source+wheel).
    asyncpg_rows = [r for r in report.blocking if r.symbol == "asyncpg"]
    assert {r.surface for r in asyncpg_rows} == {"manifest", "source", "wheel"}
    assert all(r.classification == "removed" for r in asyncpg_rows)
    assert all(r.scope == "runtime" for r in asyncpg_rows)

    # --- AC6: stale wheel ships a removed dep DESPITE a clean pyproject ---------
    stale_base = tmp_path / "stale"
    clean_pyproject, clean_src = _core_repo(stale_base, dependencies=[])  # clean manifest
    stale_wheel = _wheel_metadata(stale_base, ["asyncpg>=0.29"])

    stale_report = _run_gate(
        stale_base,
        clean_pyproject,
        clean_src,
        wheel_metadata=stale_wheel,
        audit_wheel=True,
    )

    assert stale_report.ok is False
    wheel_block = [
        r
        for r in stale_report.blocking
        if r.surface == "wheel" and r.symbol == "asyncpg"
    ]
    assert wheel_block
    assert wheel_block[0].classification == "removed"
    remediation = (wheel_block[0].remediation or "").lower()
    assert "rebuild" in remediation
    assert "reinstall" in remediation
    # the clean manifest leaked NOTHING — the only asyncpg blocker is the wheel.
    assert not any(
        r.surface == "manifest" and r.symbol == "asyncpg" for r in stale_report.rows
    )


# =========================================================================== #
# ts_abce12bb (negative, AC3)
# GIVEN the Community adapter inventory needs a runtime dependency family that is
#   ABSENT from the Community pyproject.toml.
# WHEN the FCC-07C Community manifest audit runs.
# THEN it fails with the dependency_family + adapter_key and a remediation that
#   points at the Community packaging ownership site (community pyproject
#   [project.dependencies]).
# =========================================================================== #
def test_ts_abce12bb_community_manifest_missing_adapter_dep_blocks(tmp_path):
    # Community manifest declares sentence-transformers but NOT the ladybug family
    # that its kuzu_* runtime adapters require.
    community = _community_pyproject(
        tmp_path,
        dependencies=["okto-pulse-core>=0.3.0", "sentence-transformers>=2.5"],
    )

    # Build a clean core ownership verdict to project the dependency audit against.
    core_pyproject, core_src = _core_repo(tmp_path / "core", dependencies=[])
    clean_core = _run_gate(tmp_path / "core", core_pyproject, core_src)

    audit, projection = run_community_packaging_audit(
        community_pyproject_path=community, ownership_report=clean_core
    )

    # AC3 — the missing family is a BLOCKING finding per affected adapter_key.
    assert audit.ok is False
    assert audit.community_pyproject_exists is True
    blocking_by_key = {f.adapter_key: f for f in audit.blocking}
    assert _LADYBUG_ADAPTERS <= set(blocking_by_key)

    finding = blocking_by_key["kuzu_graph_store"]
    assert finding.dependency_family == "ladybug"  # dependency family
    assert finding.adapter_key == "kuzu_graph_store"  # adapter_key
    assert finding.declared is False
    assert finding.scope == "absent"
    assert finding.diagnostic_code == DIAG_COMMUNITY_DEPENDENCY_NOT_DECLARED
    # remediation points at the Community packaging ownership site.
    remediation = finding.remediation or ""
    assert community.as_posix() in remediation
    assert "project.dependencies" in remediation

    # the satisfied sentence-transformers adapters are NOT blocking.
    assert not (set(blocking_by_key) & _SENTENCE_ADAPTERS)

    # the missing declaration also fails the projected dependency_audit_passed.
    evidence = projection.evidence_map()
    for key in _LADYBUG_ADAPTERS:
        assert evidence[key] is False
    for key in _SENTENCE_ADAPTERS:
        assert evidence[key] is True
    assert projection.ok is False


# =========================================================================== #
# ts_a77cf454 (integration, AC4 + AC5 + AC7)
# GIVEN core packaging declares `requests` and `chardet`, one a VALID ledgered
#   temporary exception (requests) and one WITHOUT an owner (chardet removed
#   from the ledger).
# WHEN the gate classifies them.
# THEN the valid exception stays VISIBLE as `temporary_exception` (never silently
#   passed) (AC4); the no-owner dependency BLOCKS as `unknown`/`violation` while
#   preserving the original dependency_conformance finding (AC5); and the
#   projected dependency_audit_passed becomes True for the affected adapter_key
#   ONLY after the blocking row is gone (AC7).
# =========================================================================== #
def test_ts_a77cf454_requests_chardet_require_visible_ownership(tmp_path):
    # --- Phase 1: chardet is unledgered ("no owner"), requests stays valid ---
    base = tmp_path / "mixed"
    pyproject, src = _core_repo(
        base, dependencies=["requests>=2.0", "chardet>=5.0"]
    )
    # Drop ONLY chardet from the ledger -> it loses its owner; requests keeps
    # its valid ledgered temporary exception.
    stripped_ledger = tuple(
        e for e in build_dependency_ledger() if e.token != "chardet"
    )
    blocking_report = audit_dependency_conformance(
        repo_root=base,
        pyproject_path=pyproject,
        lock_path=base / "missing.lock",
        source_root=src,
        audit_wheel=False,
        ledger=stripped_ledger,
    )
    report = _run_gate(base, pyproject, src, dependency_report=blocking_report)

    # AC4 — requests is a VISIBLE temporary_exception, never silently passed.
    te = {r.symbol: r for r in report.temporary_exceptions}
    assert "requests" in te
    assert te["requests"].classification == "temporary_exception"
    assert te["requests"].action == "temporary_exception"
    assert not te["requests"].blocking
    # surfaced in the verdict rows (not dropped).
    assert "requests" in {r.symbol for r in report.rows}
    assert "requests" not in {r.symbol for r in report.blocking}

    # AC5 — the no-owner chardet BLOCKS via the MatrixClassification while the
    # original dependency_conformance finding is preserved on the row.
    by_symbol = {r.symbol: r for r in report.blocking}
    assert "chardet" in by_symbol
    assert by_symbol["chardet"].classification in ("unknown", "violation")
    assert by_symbol["chardet"].diagnostic_code == "unledgered_dependency"
    # reuse (not re-derivation): still linked to the residual adapter.
    assert by_symbol["chardet"].adapter_key == "local_telemetry_store"
    assert report.ok is False

    # --- AC7: dependency_audit_passed flips True only after the blocking is gone -
    community = _community_pyproject(tmp_path, dependencies=_FULL_COMMUNITY_DEPS)
    community_audit = audit_community_manifest(community)

    # while chardet still BLOCKS, the affected adapter's projected
    # dependency_audit_passed is False (a blocking finding is present).
    blocked_projection = project_dependency_audit_evidence(
        ownership_report=report, community_audit=community_audit
    )
    assert blocked_projection.passed("local_telemetry_store") is False

    # restore the full ledger -> chardet is a valid temporary_exception, no
    # blocking remains, and the projection is now consumable by FCC-07B (True).
    clean_report = _run_gate(base, pyproject, src)
    assert clean_report.blocking == ()
    assert {"requests", "chardet"} <= {
        r.symbol for r in clean_report.temporary_exceptions
    }
    clean_projection = project_dependency_audit_evidence(
        ownership_report=clean_report, community_audit=community_audit
    )
    assert clean_projection.passed("local_telemetry_store") is True


# =========================================================================== #
# ts_69d2ad1b (integration, AC8)
# GIVEN the SAME governed dependency (`ladybug`) appears in an optional/dev-test
#   scope AND in a runtime manifest/wheel scope.
# WHEN the packaging ownership gate classifies each occurrence.
# THEN the optional/dev-test occurrence is reported with an explicit non-runtime
#   scope and does NOT block runtime conformance; the runtime manifest/wheel
#   occurrence is classified via the MatrixClassification and BLOCKS because
#   ownership requires it.
# =========================================================================== #
def test_ts_69d2ad1b_runtime_optional_devtest_scopes_separated(tmp_path):
    # --- optional/dev-test-only scope: surfaced, non-runtime, NON-blocking ------
    opt_base = tmp_path / "opt"
    opt_pyproject, opt_src = _core_repo(
        opt_base, dependencies=[], optional={"dev": ["ladybug>=0.16"]}
    )
    opt_report = _run_gate(opt_base, opt_pyproject, opt_src)

    assert opt_report.ok is True  # optional/dev extra does NOT block runtime conformance
    assert opt_report.status == "xfail_advisory"
    assert opt_report.blocking == ()
    scoped = {r.symbol: r for r in opt_report.scoped_out}
    assert "ladybug" in scoped
    assert scoped["ladybug"].scope == "optional"  # explicit non-runtime scope
    assert scoped["ladybug"].classification == "community_owned"
    assert scoped["ladybug"].action == "scoped_out"

    # --- runtime manifest + wheel scope: classified via Matrix, BLOCKS ----------
    rt_base = tmp_path / "rt"
    rt_pyproject, rt_src = _core_repo(rt_base, dependencies=["ladybug>=0.16"])
    rt_wheel = _wheel_metadata(rt_base, ["ladybug>=0.16"])
    rt_report = _run_gate(
        rt_base, rt_pyproject, rt_src, wheel_metadata=rt_wheel, audit_wheel=True
    )

    assert rt_report.ok is False
    rt_blocking = [r for r in rt_report.blocking if r.symbol == "ladybug"]
    # both the runtime manifest and the runtime wheel occurrence block.
    assert {r.surface for r in rt_blocking} == {"manifest", "wheel"}
    assert all(r.scope == "runtime" for r in rt_blocking)
    assert all(r.classification == "community_owned" for r in rt_blocking)

    # SAME token, SAME classification — only the SCOPE axis differs and only the
    # runtime scope blocks (AC8: the verdict is the scope, never the spelling).
    assert scoped["ladybug"].classification == rt_blocking[0].classification
    assert scoped["ladybug"].scope != rt_blocking[0].scope
