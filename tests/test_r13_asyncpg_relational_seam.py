"""R13 - asyncpg removal plus deferred relational seam governance."""

from __future__ import annotations

import json
from pathlib import Path

from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
    AdapterInventoryEntry,
)
from okto_pulse.core.application.boundary.conformance_matrix import (
    build_conformance_matrix,
)
from okto_pulse.core.application.boundary.dependency_conformance import (
    audit_dependency_conformance,
)


def _repo(
    tmp_path: Path,
    dependencies: list[str],
    core_files: dict[str, str] | None = None,
) -> tuple[Path, Path]:
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


def test_r13_asyncpg_reintroduced_in_package_or_runtime_fails_closed(
    tmp_path: Path,
) -> None:
    pyproject, src = _repo(
        tmp_path,
        ["asyncpg>=0.29"],
        {"infra/database.py": "import asyncpg\n"},
    )

    dep_report = audit_dependency_conformance(
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
        dependency_report=dep_report,
        include_import_boundary=False,
    )

    assert dep_report.ok is False
    blocked = [
        row
        for row in matrix.rows
        if row.symbol_or_dependency == "asyncpg"
        and row.surface in {"manifest", "source"}
    ]
    assert {row.surface for row in blocked} == {"manifest", "source"}
    assert all(row.classification == "removed" for row in blocked)
    assert all(row.adapter_key == "asyncpg_postgres_driver" for row in blocked)
    assert all(row.severity == "blocking" for row in blocked)


def test_r13_relational_adapter_row_is_deferred_not_removed() -> None:
    dep_report = audit_dependency_conformance(audit_wheel=False)
    matrix = build_conformance_matrix(
        audit_wheel=False,
        dependency_report=dep_report,
        include_import_boundary=False,
    )

    assert dep_report.ok is True
    assert "asyncpg" in dep_report.removed_dependencies

    seam = next(
        row
        for row in matrix.rows
        if row.surface == "adapter_inventory"
        and row.adapter_key == "asyncpg_postgres_driver"
    )
    assert seam.classification == "future_adapter"
    assert seam.severity == "warning"
    assert seam.diagnostic_code == "adapter_readiness:deferred"
    assert seam.owning_fcc_or_wave == "R05-E"
    assert seam.evidence_field_impact
    assert seam.remediation and "relational adapter boundary" in seam.remediation


def test_r13_relational_seam_without_deferred_contract_blocks_matrix(
    tmp_path: Path,
) -> None:
    pyproject, src = _repo(tmp_path, [])
    bad_seam = AdapterInventoryEntry(
        adapter_key="asyncpg_postgres_driver",
        owner="",
        current_module="okto_pulse/core/infra/database.py",
        port_ref="(relational driver dependency)",
        wave="R13",
        predecessor_refs=(),
        target_destination="",
        packages=("asyncpg",),
        oracles_required=(),
        removal_criterion="",
        status="blocked",
    )

    matrix = build_conformance_matrix(
        repo_root=tmp_path,
        pyproject_path=pyproject,
        lock_path=tmp_path / "missing.lock",
        source_root=src,
        audit_wheel=False,
        adapter_inventory=(bad_seam,),
        required_adapter_keys=frozenset({"asyncpg_postgres_driver"}),
        include_import_boundary=False,
    )

    seam = next(row for row in matrix.rows if row.adapter_key == "asyncpg_postgres_driver")
    assert matrix.ok is False
    assert seam.classification == "future_adapter"
    assert seam.severity == "blocking"
    assert seam.diagnostic_code == "adapter_readiness:relational_seam_contract_violation"
    assert seam.evidence_field_impact
    assert "status:deferred" in seam.evidence_field_impact
    assert "owner" in seam.evidence_field_impact
    assert "oracles_required" in seam.evidence_field_impact
