from __future__ import annotations

from pathlib import Path

from okto_pulse.core.application.boundary.rebuild_audit_storage_gate import (
    run_rebuild_audit_storage_gate,
)


CORE_ROOT = Path("src/okto_pulse/core")

INVENTORY_FILES = (
    "mcp/server.py",
    "mcp/kg_tools.py",
    "api/kg_rebuild.py",
    "api/kg_cognitive_pending.py",
    "api/kg_cognitive_candidate_commands.py",
    "api/kg_cognitive_candidates.py",
    "api/kg_cognitive_badges.py",
    "api/cognitive_action_center.py",
    "api/bug_cognitive_closure.py",
    "services/skip_overrides.py",
    "services/main.py",
    "services/kg_health_service.py",
    "services/cognitive_effectiveness_service.py",
    "services/application_kg.py",
    "kg/workers/cognitive_closeout.py",
    "kg/cognitive_closeout_production.py",
    "kg/cognitive_closeout_gate.py",
    "kg/canonical_partition_integrity.py",
    "kg/rebuild_audit.py",
    "application/boundary/rebuild_audit_storage_gate.py",
)


def test_af29_s3r_core_rebuild_audit_storage_gate_has_no_residue():
    assert run_rebuild_audit_storage_gate(CORE_ROOT) == ()


def test_af29_s3r_inventory_has_no_core_rebuild_root_resolver():
    forbidden_literals = (
        "default_" + "rebuild_base_dir",
        "OKTO_PULSE_" + "REBUILD_BASE_DIR",
        "_REBUILD_" + "BASE_DIR",
        "_LEGACY_" + "REBUILD_BASE_DIR_SEAM",
    )

    for rel_path in INVENTORY_FILES:
        source = (CORE_ROOT / rel_path).read_text(encoding="utf-8")
        for literal in forbidden_literals:
            assert literal not in source, f"{literal} leaked in {rel_path}"


def test_af29_s3r_production_core_does_not_call_tempdir_for_rebuild_roots():
    for path in sorted(CORE_ROOT.rglob("*.py")):
        rel = path.relative_to(CORE_ROOT).as_posix()
        if rel.startswith("application/boundary/"):
            continue
        source = path.read_text(encoding="utf-8")
        assert "tempfile." + "gettempdir" not in source, rel
        assert "get" + "tempdir()" not in source, rel
