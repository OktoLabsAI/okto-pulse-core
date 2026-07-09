"""FCC-02A — rebuild ledger reconciliation and pure port contracts."""

from __future__ import annotations

import ast
import importlib
from pathlib import Path

from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
    REQUIRED_EVIDENCE,
    build_adapter_inventory,
)


CORE_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = CORE_ROOT / "src"
COMMUNITY_ROOT = CORE_ROOT.parents[0] / "okto_labs_pulse_community"
COMMUNITY_SRC_ROOT = COMMUNITY_ROOT / "src"
CONTRACT_MODULE = "okto_pulse.core.application.rebuild_ports"

_TARGET_KEYS = {
    "board_source_store": {
        "port_ref": f"{CONTRACT_MODULE}.BoardSourceReader",
        "module": "okto_pulse/community/adapters/board_source_reader.py",
        "required_oracles": {
            "board_source_fetch_parity",
            "source_materialization_unaffected",
        },
    },
    "board_rebuild_ingestion_adapter": {
        "port_ref": f"{CONTRACT_MODULE}.RebuildIngestionPort/StepAdapterFactory",
        "module": "okto_pulse/community/adapters/board_rebuild_ingestion.py",
        "required_oracles": {
            "rebuild_enqueue_receipt_parity",
            "rebuild_quarantine_unaffected",
            "rebuild_fail_closed_provider_missing",
            "rebuild_registry_wiring",
        },
    },
}


def test_fcc02a_rebuild_ledger_entries_are_unique_and_reconciled() -> None:
    entries = build_adapter_inventory()
    grouped = {
        key: [entry for entry in entries if entry.adapter_key == key]
        for key in _TARGET_KEYS
    }

    for key, matches in grouped.items():
        assert len(matches) == 1, f"{key} must have exactly one ledger entry"
        entry = matches[0]
        spec = _TARGET_KEYS[key]
        assert entry.port_ref == spec["port_ref"]
        assert "no port yet" not in entry.port_ref.lower()
        assert entry.owner == "okto-pulse-community/kg"
        assert entry.current_module == spec["module"]
        assert "community" in entry.target_destination.lower()
        assert entry.predecessor_refs
        assert set(entry.oracles_required) == spec["required_oracles"]
        assert entry.removal_criterion.strip()
        assert entry.evidence_fields == REQUIRED_EVIDENCE


def test_fcc02a_rebuild_contract_module_is_pure_and_importable() -> None:
    module_path = SRC_ROOT / "okto_pulse/core/application/rebuild_ports.py"
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported_names = _imported_names(tree)
    source = module_path.read_text(encoding="utf-8")

    forbidden_imports = {
        "sqlite3",
        "sqlalchemy",
        "pathlib",
        "okto_pulse.community",
    }
    for forbidden in forbidden_imports:
        assert all(forbidden not in name for name in imported_names)
    assert "get_db" not in source
    assert "get_db_for_mcp" not in source

    module = importlib.import_module(CONTRACT_MODULE)
    assert hasattr(module, "BoardSourceReader")
    assert hasattr(module, "RebuildIngestionPort")
    assert hasattr(module, "RebuildStepAdapterFactory")
    assert hasattr(module, "SourceReadError")


def test_fcc02a_preserves_current_raw_adapter_placement() -> None:
    source_path = (
        COMMUNITY_SRC_ROOT / "okto_pulse/community/adapters/board_source_reader.py"
    )
    ingestion_path = (
        COMMUNITY_SRC_ROOT
        / "okto_pulse/community/adapters/board_rebuild_ingestion.py"
    )
    assert source_path.exists()
    assert ingestion_path.exists()

    source = source_path.read_text(encoding="utf-8")
    ingestion = ingestion_path.read_text(encoding="utf-8")
    assert "class CommunityBoardSourceReader" in source
    assert "sqlite3.connect" in source
    assert "class CommunityBoardRebuildIngestionAdapter" in ingestion
    assert "sqlite3.connect" in ingestion

    core_rebuild_contract = (
        SRC_ROOT / "okto_pulse/core/kg/board_rebuild_adapter.py"
    ).read_text(encoding="utf-8")
    assert "sqlite3.connect" not in core_rebuild_contract
    assert "class BoardRebuildIngestionAdapter" not in core_rebuild_contract


def _imported_names(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            names.add(module)
            names.update(f"{module}.{alias.name}" for alias in node.names)
    return names
