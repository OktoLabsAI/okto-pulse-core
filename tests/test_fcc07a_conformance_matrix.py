"""FCC-07A conformance matrix tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import get_args

from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
    REQUIRED_ADAPTER_KEYS,
)
from okto_pulse.core.application.boundary.cli import main as boundary_cli_main
from okto_pulse.core.application.boundary.conformance_matrix import (
    MatrixClassification,
    TESTING_PROVIDER_PREFIX,
    SourceImportReference,
    _TOKEN_ADAPTER_HINTS,
    build_conformance_matrix,
)
from okto_pulse.core.application.boundary.dependency_conformance import (
    audit_dependency_conformance,
)
from okto_pulse.core.application.boundary.dependency_ledger import build_dependency_ledger

FCC07A_RESIDUAL_KEYS = {
    "mcp_auth_context",
    "global_discovery_db",
    "singleton_scheduler_control",
    "local_telemetry_store",
    "asyncpg_postgres_driver",
    "board_source_store",
    "board_rebuild_ingestion_adapter",
}


def test_adapter_hint_targets_are_canonical_required_adapter_keys():
    hinted_adapter_keys = {
        adapter_key
        for adapter_keys in _TOKEN_ADAPTER_HINTS.values()
        for adapter_key in adapter_keys
    }

    assert hinted_adapter_keys <= REQUIRED_ADAPTER_KEYS


def _repo(tmp_path: Path, dependencies: list[str], core_files: dict[str, str] | None = None):
    pyproject = tmp_path / "pyproject.toml"
    deps = ", ".join(json.dumps(dep) for dep in dependencies)
    pyproject.write_text(
        "[project]\n"
        'name = "synthetic-core"\n'
        'version = "0"\n'
        f"dependencies = [{deps}]\n",
        encoding="utf-8",
    )
    src = tmp_path / "src"
    core = src / "okto_pulse" / "core"
    core.mkdir(parents=True)
    (core / "__init__.py").write_text("", encoding="utf-8")
    for rel, content in (core_files or {}).items():
        target = core / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    return pyproject, src


def test_matrix_classifies_unledgered_and_removed_manifest_dependencies(tmp_path: Path):
    pyproject, src = _repo(tmp_path, ["kuzu>=0.1", "asyncpg>=0.29"])

    report = build_conformance_matrix(
        repo_root=tmp_path,
        pyproject_path=pyproject,
        lock_path=tmp_path / "missing.lock",
        source_root=src,
        audit_wheel=False,
        include_import_boundary=False,
    )

    manifest_rows = {
        row.symbol_or_dependency: row
        for row in report.rows
        if row.surface == "manifest"
    }
    assert manifest_rows["kuzu"].classification == "unknown"
    assert manifest_rows["kuzu"].severity == "blocking"
    assert manifest_rows["asyncpg"].classification == "removed"
    assert manifest_rows["asyncpg"].adapter_key == "asyncpg_postgres_driver"
    assert manifest_rows["asyncpg"].severity == "blocking"


def test_matrix_reports_removed_dependency_in_explicit_wheel_metadata(tmp_path: Path):
    pyproject, src = _repo(tmp_path, [])
    metadata = tmp_path / "okto_pulse_core-0.dist-info" / "METADATA"
    metadata.parent.mkdir()
    metadata.write_text(
        "Metadata-Version: 2.1\n"
        "Name: okto-pulse-core\n"
        "Version: 0\n"
        "Requires-Dist: asyncpg>=0.29\n",
        encoding="utf-8",
    )

    report = build_conformance_matrix(
        repo_root=tmp_path,
        pyproject_path=pyproject,
        lock_path=tmp_path / "missing.lock",
        source_root=src,
        wheel_metadata_path=metadata,
        audit_wheel=True,
        include_import_boundary=False,
    )

    wheel_row = next(
        row
        for row in report.rows
        if row.surface == "wheel" and row.symbol_or_dependency == "asyncpg"
    )
    assert wheel_row.classification == "removed"
    assert wheel_row.adapter_key == "asyncpg_postgres_driver"
    assert wheel_row.severity == "blocking"
    assert "published artifact" in (wheel_row.remediation or "")


def test_matrix_surfaces_absent_removed_dependencies_as_accepted_rows(tmp_path: Path):
    pyproject, src = _repo(tmp_path, [])

    report = build_conformance_matrix(
        repo_root=tmp_path,
        pyproject_path=pyproject,
        lock_path=tmp_path / "missing.lock",
        source_root=src,
        audit_wheel=False,
        include_import_boundary=False,
    )

    removed = {
        row.symbol_or_dependency: row
        for row in report.rows
        if row.surface == "dependency_conformance"
        and row.diagnostic_code == "removed_dependency_absent"
    }
    assert removed["aiofiles"].classification == "removed"
    assert removed["aiofiles"].severity == "accepted"
    assert removed["aiofiles"].owning_fcc_or_wave == "AF-05"
    assert removed["asyncpg"].classification == "removed"


def test_matrix_blocks_community_owned_relational_dependencies_in_core(tmp_path: Path):
    pyproject, src = _repo(
        tmp_path,
        ["sqlalchemy[asyncio]>=2.0", "aiosqlite>=0.19", "numpy>=1.26"],
    )

    report = build_conformance_matrix(
        repo_root=tmp_path,
        pyproject_path=pyproject,
        lock_path=tmp_path / "missing.lock",
        source_root=src,
        audit_wheel=False,
        include_import_boundary=False,
    )

    manifest_rows = [
        row for row in report.rows if row.surface == "manifest"
    ]
    aiosqlite = next(row for row in manifest_rows if row.symbol_or_dependency == "aiosqlite")
    assert aiosqlite.classification == "community_owned"
    assert aiosqlite.severity == "blocking"
    numpy = next(row for row in manifest_rows if row.symbol_or_dependency == "numpy")
    assert numpy.classification == "community_owned"
    assert numpy.severity == "blocking"
    assert numpy.adapter_key is None
    assert numpy.owning_fcc_or_wave is None
    assert numpy.evidence_field_impact is None
    assert report.ok is False


def test_matrix_links_unowned_requests_and_scheduler_to_residual_adapters(
    tmp_path: Path,
):
    pyproject, src = _repo(
        tmp_path,
        ["requests>=2.0", "apscheduler>=3.0"],
    )
    stripped_ledger = tuple(
        entry
        for entry in build_dependency_ledger()
        if entry.token not in {"requests", "apscheduler"}
    )
    dependency_report = audit_dependency_conformance(
        repo_root=tmp_path,
        pyproject_path=pyproject,
        lock_path=tmp_path / "missing.lock",
        source_root=src,
        audit_wheel=False,
        ledger=stripped_ledger,
    )

    report = build_conformance_matrix(
        repo_root=tmp_path,
        pyproject_path=pyproject,
        lock_path=tmp_path / "missing.lock",
        source_root=src,
        audit_wheel=False,
        dependency_report=dependency_report,
        include_import_boundary=False,
    )

    rows = {
        row.symbol_or_dependency: row
        for row in report.rows
        if row.surface == "manifest"
    }
    assert rows["requests"].severity == "blocking"
    assert rows["requests"].adapter_key == "local_telemetry_store"
    assert rows["requests"].owning_fcc_or_wave
    assert rows["apscheduler"].severity == "blocking"
    assert rows["apscheduler"].adapter_key == "singleton_scheduler_control"
    assert rows["apscheduler"].owning_fcc_or_wave


def test_matrix_projects_productive_community_import_from_import_boundary(tmp_path: Path):
    pyproject, src = _repo(
        tmp_path,
        [],
        {"application/use_case.py": "import okto_pulse.community.adapters\n"},
    )

    report = build_conformance_matrix(
        repo_root=tmp_path,
        pyproject_path=pyproject,
        lock_path=tmp_path / "missing.lock",
        source_root=src,
        audit_wheel=False,
        include_import_boundary=True,
    )

    row = next(
        row
        for row in report.rows
        if row.symbol_or_dependency == "okto_pulse.community.adapters"
    )
    assert row.surface == "source"
    assert row.classification == "community_owned"
    assert row.severity == "blocking"
    assert "use_case.py" in row.module


def test_matrix_classifies_sanctioned_test_provider_without_destroying_policy(tmp_path: Path):
    pyproject, src = _repo(tmp_path, [])
    imported = f"{TESTING_PROVIDER_PREFIX}.memory_graph_store"

    report = build_conformance_matrix(
        repo_root=tmp_path,
        pyproject_path=pyproject,
        lock_path=tmp_path / "missing.lock",
        source_root=src,
        audit_wheel=False,
        include_import_boundary=False,
        source_import_references=(
            SourceImportReference(
                imported=imported,
                location="tests/test_fixture.py:1",
                test_context=True,
            ),
            SourceImportReference(
                imported=imported,
                location="src/okto_pulse/core/runtime.py:1",
                test_context=False,
            ),
        ),
    )

    rows = {row.module: row for row in report.rows if row.symbol_or_dependency == imported}
    assert rows["tests/test_fixture.py:1"].classification == "test_only"
    assert rows["tests/test_fixture.py:1"].severity == "info"
    assert rows["src/okto_pulse/core/runtime.py:1"].classification == "violation"
    assert rows["src/okto_pulse/core/runtime.py:1"].severity == "blocking"


def test_real_inventory_residual_adapter_keys_have_owner_and_remediation(tmp_path: Path):
    pyproject, src = _repo(tmp_path, [])

    report = build_conformance_matrix(
        repo_root=tmp_path,
        pyproject_path=pyproject,
        lock_path=tmp_path / "missing.lock",
        source_root=src,
        audit_wheel=False,
        include_import_boundary=False,
    )

    rows_by_key = {
        row.adapter_key: row
        for row in report.rows
        if row.surface == "adapter_inventory" and row.adapter_key in FCC07A_RESIDUAL_KEYS
    }
    assert set(rows_by_key) == FCC07A_RESIDUAL_KEYS
    for key, row in rows_by_key.items():
        assert row.owning_fcc_or_wave, key
        assert row.remediation, key
        assert row.evidence_field_impact, key
    assert FCC07A_RESIDUAL_KEYS <= REQUIRED_ADAPTER_KEYS
    assert report.adapter_keys_missing == ()


def test_real_repo_matrix_is_terminally_conformant():
    report = build_conformance_matrix(audit_wheel=False)

    assert report.rows
    assert report.adapter_keys_missing == ()
    assert {
        row.classification for row in report.rows
    } <= set(get_args(MatrixClassification))
    assert report.ok is True
    assert not any(row.severity == "blocking" for row in report.rows)
    assert not any(row.classification == "violation" for row in report.rows)
    assert any(
        row.adapter_key in FCC07A_RESIDUAL_KEYS
        and row.surface == "adapter_inventory"
        for row in report.rows
    )


def test_cli_emits_deterministic_json_matrix(tmp_path: Path, capsys):
    pyproject, src = _repo(tmp_path, ["aiosqlite>=0.19"])

    code = boundary_cli_main(
        [
            "conformance-matrix",
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
        ]
    )

    out = capsys.readouterr().out
    payload = json.loads(out)
    assert code == 1
    assert payload["adapter_keys_missing"] == []
    assert payload["rows"] == sorted(
        payload["rows"],
        key=lambda row: (
            row["surface"],
            row["module"],
            row["symbol_or_dependency"],
            row["classification"],
            row["adapter_key"] or "",
        ),
    )
    assert any(
        row["symbol_or_dependency"] == "aiosqlite"
        and row["classification"] == "community_owned"
        and row["severity"] == "blocking"
        for row in payload["rows"]
    )


def test_cli_returns_nonzero_when_matrix_has_blocking_findings(
    tmp_path: Path,
    capsys,
):
    pyproject, src = _repo(
        tmp_path,
        [],
        {"application/use_case.py": "import okto_pulse.community.adapters\n"},
    )

    code = boundary_cli_main(
        [
            "fcc07a",
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
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert payload["ok"] is False
    assert any(
        row["symbol_or_dependency"] == "okto_pulse.community.adapters"
        and row["classification"] == "community_owned"
        and row["severity"] == "blocking"
        for row in payload["rows"]
    )
