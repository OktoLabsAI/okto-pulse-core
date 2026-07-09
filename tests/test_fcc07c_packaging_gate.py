"""FCC-07C-IMP1 packaging ownership gate tests.

These are the IMPLEMENTATION tests of the gate (NOT the separate scenario-test
cards). Every fixture is a synthetic tmp_path repo, mirroring the FCC-07A matrix
test harness, so the assertions have real teeth and stay deterministic.

The gate REUSES ``build_conformance_matrix`` / ``MatrixClassification`` — it never
re-derives a classification. The tests prove that delegation AND the ownership
policy the gate adds on top (which classifications block, runtime vs optional
scope).
"""

from __future__ import annotations

import json
from pathlib import Path

from okto_pulse.core.application.boundary.cli import main as boundary_cli_main
from okto_pulse.core.application.boundary.conformance_matrix import (
    build_conformance_matrix,
)
from okto_pulse.core.application.boundary.dependency_conformance import (
    audit_dependency_conformance,
)
from okto_pulse.core.application.boundary.dependency_ledger import (
    build_dependency_ledger,
    normalize_token,
)
from okto_pulse.core.application.boundary.packaging_ownership_gate import (
    OWNERSHIP_SURFACES,
    PackagingOwnershipGate,
    PackagingOwnershipGateInput,
)


# --------------------------------------------------------------------------- #
# synthetic-repo helpers
# --------------------------------------------------------------------------- #
def _repo(
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


def _run(
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
    return PackagingOwnershipGate().run(
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


# ===========================================================================
# AC1 — community_owned core manifest dependency fails (classification + remediation).
# ===========================================================================
def test_ac1_community_owned_manifest_dependency_blocks(tmp_path):
    pyproject, src = _repo(
        tmp_path, dependencies=["ladybug>=0.16", "sentence-transformers>=2.0"]
    )

    report = _run(tmp_path, pyproject, src)

    assert report.ok is False
    assert report.status == "blocking"
    by_symbol = {r.symbol: r for r in report.blocking}
    assert "ladybug" in by_symbol
    assert by_symbol["ladybug"].classification == "community_owned"
    assert by_symbol["ladybug"].surface == "manifest"
    assert by_symbol["ladybug"].scope == "runtime"
    assert by_symbol["ladybug"].remediation
    sentence = next(
        r for r in report.blocking if normalize_token(r.symbol) == "sentence_transformers"
    )
    assert sentence.classification == "community_owned"
    assert sentence.scope == "runtime"


# ===========================================================================
# AC2 — removed asyncpg in manifest + source + wheel all fail as `removed`.
# ===========================================================================
def test_ac2_removed_asyncpg_blocks_on_manifest_source_and_wheel(tmp_path):
    pyproject, src = _repo(
        tmp_path,
        dependencies=["asyncpg>=0.29"],
        core_files={"db.py": "import asyncpg\n"},
    )
    wheel = _wheel_metadata(tmp_path, ["asyncpg>=0.29"])

    report = _run(tmp_path, pyproject, src, wheel_metadata=wheel, audit_wheel=True)

    assert report.ok is False
    asyncpg_rows = [r for r in report.blocking if r.symbol == "asyncpg"]
    assert {r.surface for r in asyncpg_rows} == {"manifest", "source", "wheel"}
    assert all(r.classification == "removed" for r in asyncpg_rows)
    assert all(r.scope == "runtime" for r in asyncpg_rows)


# ===========================================================================
# AC4 — requests/chardet are Community-owned after AF40: a core runtime
# manifest declaration is visible and blocking.
# ===========================================================================
def test_ac4_requests_chardet_community_owned_core_manifest_blocks(tmp_path):
    pyproject, src = _repo(tmp_path, dependencies=["requests>=2.0", "chardet>=5.0"])

    report = _run(tmp_path, pyproject, src)

    assert report.ok is False
    assert report.status == "blocking"
    by_symbol = {r.symbol: r for r in report.blocking}
    assert by_symbol["requests"].classification == "community_owned"
    assert by_symbol["requests"].action == "block"
    assert by_symbol["requests"].adapter_key == "local_telemetry_store"
    assert by_symbol["chardet"].classification == "community_owned"
    assert by_symbol["chardet"].action == "block"
    assert by_symbol["chardet"].adapter_key == "local_telemetry_store"
    assert report.temporary_exceptions == ()


# ===========================================================================
# AC5 — requests/chardet WITHOUT a ledger exception block via the matrix
# (`unknown`), keeping the original dependency_conformance finding as diagnostic.
# ===========================================================================
def test_ac5_unledgered_requests_scheduler_block_via_matrix(tmp_path):
    pyproject, src = _repo(tmp_path, dependencies=["requests>=2.0", "chardet>=5.0"])
    stripped_ledger = tuple(
        entry
        for entry in build_dependency_ledger()
        if entry.token not in {"requests", "chardet"}
    )
    dependency_report = audit_dependency_conformance(
        repo_root=tmp_path,
        pyproject_path=pyproject,
        lock_path=tmp_path / "missing.lock",
        source_root=src,
        audit_wheel=False,
        ledger=stripped_ledger,
    )

    report = _run(tmp_path, pyproject, src, dependency_report=dependency_report)

    assert report.ok is False
    assert report.status == "blocking"
    by_symbol = {r.symbol: r for r in report.blocking}
    assert by_symbol["requests"].classification == "unknown"
    assert by_symbol["chardet"].classification == "unknown"
    # the original dependency_conformance diagnostic is preserved on the row.
    assert by_symbol["requests"].diagnostic_code == "unledgered_dependency"
    assert by_symbol["chardet"].diagnostic_code == "unledgered_dependency"
    # reuse, not re-derivation: the matrix still links them to residual adapters.
    assert by_symbol["requests"].adapter_key == "local_telemetry_store"
    assert by_symbol["chardet"].adapter_key == "local_telemetry_store"


# ===========================================================================
# AC6 — stale wheel metadata declaring a removed dep despite a clean pyproject
# fails with a rebuild/reinstall remediation.
# ===========================================================================
def test_ac6_stale_wheel_metadata_removed_dep_blocks_with_rebuild_remediation(tmp_path):
    pyproject, src = _repo(tmp_path, dependencies=[])  # clean manifest
    wheel = _wheel_metadata(tmp_path, ["asyncpg>=0.29"])

    report = _run(tmp_path, pyproject, src, wheel_metadata=wheel, audit_wheel=True)

    assert report.ok is False
    wheel_block = [
        r for r in report.blocking if r.surface == "wheel" and r.symbol == "asyncpg"
    ]
    assert wheel_block
    assert wheel_block[0].classification == "removed"
    remediation = (wheel_block[0].remediation or "").lower()
    assert "rebuild" in remediation
    assert "reinstall" in remediation
    # the manifest itself is clean — no manifest asyncpg row leaked.
    assert not any(
        r.surface == "manifest" and r.symbol == "asyncpg" for r in report.rows
    )


# ===========================================================================
# AC8 — dev/test/optional extras that do not ship in the runtime/wheel are
# scoped out (non-blocking); the SAME dep declared at runtime blocks.
# ===========================================================================
def test_ac8_optional_extra_scoped_out_but_runtime_blocks(tmp_path):
    # ladybug ONLY as an optional/dev extra -> explicit optional scope, no block.
    base_opt = tmp_path / "opt"
    pyproject_opt, src_opt = _repo(
        base_opt, dependencies=[], optional={"dev": ["ladybug>=0.16"]}
    )
    report_opt = _run(base_opt, pyproject_opt, src_opt)

    assert report_opt.ok is True
    assert report_opt.status == "xfail_advisory"
    assert report_opt.blocking == ()
    scoped = {r.symbol: r for r in report_opt.scoped_out}
    assert "ladybug" in scoped
    assert scoped["ladybug"].scope == "optional"
    assert scoped["ladybug"].classification == "community_owned"
    assert scoped["ladybug"].action == "scoped_out"

    # the SAME dep in the runtime [project.dependencies] -> blocks (ownership).
    base_rt = tmp_path / "rt"
    pyproject_rt, src_rt = _repo(base_rt, dependencies=["ladybug>=0.16"])
    report_rt = _run(base_rt, pyproject_rt, src_rt)

    assert report_rt.ok is False
    blocking = {r.symbol: r for r in report_rt.blocking}
    assert "ladybug" in blocking
    assert blocking["ladybug"].scope == "runtime"
    assert blocking["ladybug"].classification == "community_owned"


# ===========================================================================
# TR5 — sqlalchemy / aiosqlite are core_common (NOT name-banned); the verdict
# comes from the MatrixClassification, never the token spelling.
# ===========================================================================
def test_tr5_sqlalchemy_aiosqlite_core_common_not_name_banned(tmp_path):
    pyproject, src = _repo(
        tmp_path, dependencies=["sqlalchemy[asyncio]>=2.0", "aiosqlite>=0.19"]
    )

    report = _run(tmp_path, pyproject, src)

    assert report.ok is True
    rows = {r.symbol: r for r in report.rows}
    assert rows["sqlalchemy"].classification == "core_common"
    assert rows["sqlalchemy"].action == "core_common"
    assert not rows["sqlalchemy"].blocking
    assert rows["aiosqlite"].classification == "core_common"
    assert not rows["aiosqlite"].blocking


# ===========================================================================
# Source ownership via the import-boundary projection (productive community import).
# ===========================================================================
def test_productive_community_import_blocks_via_import_boundary(tmp_path):
    pyproject, src = _repo(
        tmp_path,
        dependencies=[],
        core_files={"application/use_case.py": "import okto_pulse.community.adapters\n"},
    )

    report = _run(tmp_path, pyproject, src, include_import_boundary=True)

    assert report.ok is False
    row = next(
        r for r in report.blocking if r.symbol == "okto_pulse.community.adapters"
    )
    assert row.surface == "source"
    assert row.classification == "community_owned"
    assert row.scope == "runtime"


# ===========================================================================
# The gate DELEGATES the classification to the matrix (no parallel classifier).
# ===========================================================================
def test_gate_delegates_classification_to_matrix(tmp_path):
    pyproject, src = _repo(
        tmp_path,
        dependencies=["ladybug>=0.16", "requests>=2.0", "sqlalchemy>=2.0", "numpy>=1.26"],
    )
    dependency_report = audit_dependency_conformance(
        repo_root=tmp_path,
        pyproject_path=pyproject,
        lock_path=tmp_path / "missing.lock",
        source_root=src,
        audit_wheel=False,
    )
    matrix = build_conformance_matrix(
        repo_root=tmp_path,
        pyproject_path=pyproject,
        lock_path=tmp_path / "missing.lock",
        source_root=src,
        audit_wheel=False,
        include_import_boundary=False,
        dependency_report=dependency_report,
    )
    report = _run(tmp_path, pyproject, src, dependency_report=dependency_report)

    matrix_classification = {
        (row.surface, row.symbol_or_dependency): row.classification
        for row in matrix.rows
        if row.surface in OWNERSHIP_SURFACES
    }
    assert matrix_classification  # sanity
    for gate_row in report.rows:
        assert (
            matrix_classification[(gate_row.surface, gate_row.symbol)]
            == gate_row.classification
        )


# ===========================================================================
# Determinism + serialisability + GateReport bridge.
# ===========================================================================
def test_verdict_is_deterministic_and_serialisable(tmp_path):
    pyproject, src = _repo(
        tmp_path, dependencies=["ladybug>=0.16", "requests>=2.0", "numpy>=1.26"]
    )

    first = _run(tmp_path, pyproject, src).as_dict()
    second = _run(tmp_path, pyproject, src).as_dict()

    assert first == second
    assert json.dumps(first, sort_keys=True)  # JSON-serialisable, no dataclass leak


def test_to_gate_report_bridge(tmp_path):
    pyproject, src = _repo(tmp_path, dependencies=["ladybug>=0.16"])

    report = _run(tmp_path, pyproject, src)
    gate_report = report.to_gate_report()

    assert gate_report.gate_id == "packaging_ownership"
    assert gate_report.status == "blocking"
    assert gate_report.severity == "high"
    assert "ladybug" in gate_report.observed_value
    assert json.dumps(gate_report.as_dict(), sort_keys=True)


# ===========================================================================
# TR3 — new CLI subcommand works; the existing one keeps functioning.
# ===========================================================================
def test_cli_packaging_ownership_returns_nonzero_on_blocking(tmp_path, capsys):
    pyproject, src = _repo(tmp_path, dependencies=["ladybug>=0.16"])

    code = boundary_cli_main(
        [
            "packaging-ownership",
            "--format",
            "json",
            "--repo-root",
            str(tmp_path),
            "--pyproject",
            str(pyproject),
            "--lock",
            str(tmp_path / "missing.lock"),
            "--source-root",
            str(src),
            "--no-wheel",
            "--no-import-boundary",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert payload["ok"] is False
    assert any(
        row["symbol"] == "ladybug" and row["classification"] == "community_owned"
        for row in payload["blocking"]
    )


def test_cli_dependency_conformance_subcommand_still_works(capsys):
    code = boundary_cli_main(["dependency-conformance", "--format", "json", "--no-wheel"])

    payload = json.loads(capsys.readouterr().out)
    assert code in (0, 1)
    assert "ledger_version" in payload
