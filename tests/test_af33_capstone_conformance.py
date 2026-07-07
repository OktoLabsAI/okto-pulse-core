from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
    build_adapter_inventory,
)
from okto_pulse.core.application.boundary.capstone_conformance import (
    CAPSTONE_OWNERSHIP_MATRIX,
    REQUIRED_SWAP_TARGETS,
    audit_capstone_ledgers,
    audit_capstone_integration_gates,
    audit_core_capstone_imports,
    build_capstone_conformance_report,
    render_capstone_ownership_matrix,
    validate_capstone_readme_sync,
)
from okto_pulse.core.application.boundary.dependency_ledger import (
    build_dependency_ledger,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
CORE_SOURCE_ROOT = REPO_ROOT / "src" / "okto_pulse" / "core"
COMMUNITY_ROOT = REPO_ROOT.parent / "okto_labs_pulse_community"


def _community_readme() -> str:
    readme = COMMUNITY_ROOT / "README.md"
    if not readme.exists():
        pytest.skip("Community sibling repo not available for AF33 README sync")
    return readme.read_text(encoding="utf-8")


def test_af33_readme_matrices_are_byte_identical_and_generated() -> None:
    core_readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    community_readme = _community_readme()

    report = validate_capstone_readme_sync(
        core_readme=core_readme,
        community_readme=community_readme,
    )

    assert report.ok is True, report.violations
    assert report.expected_fragment == render_capstone_ownership_matrix()
    for target in REQUIRED_SWAP_TARGETS:
        assert target in report.expected_fragment


def test_af33_readme_matrix_mismatch_fails_closed() -> None:
    core_readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    community_readme = _community_readme().replace(
        "filesystem -> S3",
        "filesystem -> local-only",
    )

    report = validate_capstone_readme_sync(
        core_readme=core_readme,
        community_readme=community_readme,
    )

    assert report.ok is False
    assert {violation.code for violation in report.violations} >= {
        "readme_matrix_mismatch",
        "readme_cross_repo_mismatch",
    }


def test_af33_core_concrete_adapter_imports_fail_closed(tmp_path: Path) -> None:
    rogue = tmp_path / "src" / "okto_pulse" / "core" / "application" / "rogue.py"
    rogue.parent.mkdir(parents=True)
    rogue.write_text(
        "import importlib\n"
        "import okto_pulse.community.adapters\n"
        "from apscheduler.schedulers.asyncio import AsyncIOScheduler\n"
        "graph = importlib.import_module('ladybug')\n",
        encoding="utf-8",
    )

    report = audit_core_capstone_imports(tmp_path)

    assert report.ok is False
    assert [
        (violation.code, violation.detail)
        for violation in report.violations
    ] == [
        ("core_concrete_adapter_import", "okto_pulse.community.adapters"),
        ("core_concrete_adapter_import", "apscheduler.schedulers.asyncio"),
        ("core_concrete_adapter_import", "ladybug"),
    ]


def test_af33_ledgers_require_owner_source_coverage_and_removal() -> None:
    bad_dependency = replace(
        build_dependency_ledger()[0],
        removal_criterion="",
    )
    bad_adapter = replace(
        build_adapter_inventory()[0],
        oracles_required=(),
    )
    bad_reach_in = {
        "file_path": "src/okto_pulse/community/adapters/rogue.py",
        "scope": "<module>",
        "module": "okto_pulse.core.models.db",
        "symbols": ("Card",),
        "category": "private_core_import",
        "owner": "",
        "reason": "temporary compatibility",
        "target_public_surface": "public facade",
        "removal_path": "remove reach-in",
        "withdrawal_criterion": "no import remains",
    }

    report = audit_capstone_ledgers(
        dependency_ledger=(bad_dependency,),
        adapter_inventory=(bad_adapter,),
        community_reach_in_ledger=(bad_reach_in,),
    )

    assert report.ok is False
    assert {
        (violation.code, violation.location, violation.detail)
        for violation in report.violations
    } == {
        ("dependency_ledger_missing_field", "asyncpg", "removal_criterion"),
        (
            "adapter_inventory_missing_coverage",
            "filesystem_storage_provider",
            "predecessor_refs/oracles_required",
        ),
        (
            "community_reach_in_ledger_missing_field",
            "src/okto_pulse/community/adapters/rogue.py:<module>",
            "owner",
        ),
    }


def test_af33_real_tree_capstone_conformance_is_green() -> None:
    community_source_root = (
        COMMUNITY_ROOT / "src" / "okto_pulse" / "community"
        if COMMUNITY_ROOT.exists()
        else None
    )
    report = build_capstone_conformance_report(
        core_source_root=CORE_SOURCE_ROOT,
        core_readme=(REPO_ROOT / "README.md").read_text(encoding="utf-8"),
        community_readme=_community_readme(),
        community_source_root=community_source_root,
    )

    assert report.ok is True, report
    assert report.imports.violations == ()
    assert report.ledgers.violations == ()
    assert report.integrations.violations == ()
    assert {
        result.gate_id for result in report.integrations.results
    } >= {
        "run_rebuild_audit_storage_gate",
        "run_relational_residue_gate",
        "SchedulerControlSymbolGate",
        "run_telemetry_sender_ownership_gate",
    }


def _write(root: Path, rel: str, source: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def test_af33_integration_gate_mutations_fail_closed(tmp_path: Path) -> None:
    core = tmp_path / "src" / "okto_pulse" / "core"
    _write(
        core,
        "kg/rebuild_root.py",
        "from pathlib import Path\n"
        "import tempfile\n"
        "base = Path(tempfile.gettempdir()) / 'okto_pulse_kg_rebuild'\n",
    )
    _write(
        core,
        "application/boundary/relational_adapter_import_gate.py",
        "ALLOWLIST = ('infra/database.py',)\n",
    )
    _write(
        core,
        "runtime/scheduler.py",
        "class SingletonSchedulerControl:\n"
        "    pass\n",
    )

    report = audit_capstone_integration_gates(core_source_root=tmp_path)

    assert report.ok is False
    failed = {violation.location for violation in report.violations}
    assert {
        "run_rebuild_audit_storage_gate",
        "run_relational_residue_gate",
        "SchedulerControlSymbolGate",
    } <= failed


def test_af33_matrix_references_af29_af30_af31_integration_gates() -> None:
    gate_refs = {
        gate
        for row in CAPSTONE_OWNERSHIP_MATRIX
        for gate in row.gate_refs
    }

    assert {
        "run_rebuild_audit_storage_gate",
        "run_relational_residue_gate",
        "run_telemetry_sender_ownership_gate",
        "SchedulerControlSymbolGate",
        "run_public_config_stability_gate",
    } <= gate_refs
