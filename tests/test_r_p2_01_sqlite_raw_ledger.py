"""R-P2-01/R10B — raw SQLite rebuild ingestion moved behind Community port."""

from __future__ import annotations

from pathlib import Path

from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
    REQUIRED_ADAPTER_KEYS,
    build_adapter_inventory,
)
from okto_pulse.core.application.boundary.source_read_consumer_gate import (
    ALLOWED_INGESTION_SQLITE_SUFFIXES,
)

_ADAPTER_KEY = "board_rebuild_ingestion_adapter"
_CORE_MODULE = "okto_pulse/core/kg/board_rebuild_adapter.py"
_COMMUNITY_MODULE = "okto_pulse/community/adapters/board_rebuild_ingestion.py"
_CORE_SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
_COMMUNITY_SRC_ROOT = _CORE_SRC_ROOT.parents[1] / "okto_labs_pulse_community" / "src"


def _by_key(key: str):
    return next(e for e in build_adapter_inventory() if e.adapter_key == key)


def test_rebuild_ingestion_adapter_is_registered_as_community_owned() -> None:
    keys = {e.adapter_key for e in build_adapter_inventory()}
    assert _ADAPTER_KEY in REQUIRED_ADAPTER_KEYS
    assert _ADAPTER_KEY in keys

    entry = _by_key(_ADAPTER_KEY)
    assert entry.owner == "okto-pulse-community/kg"
    assert entry.current_module == _COMMUNITY_MODULE
    assert entry.port_ref == "RebuildIngestionPort/StepAdapterFactory"
    assert entry.status == "ready"
    assert ("moved_by", "R10B") in entry.metadata


def test_core_rebuild_contract_no_longer_uses_raw_sqlite() -> None:
    src = (_CORE_SRC_ROOT / _CORE_MODULE).read_text(encoding="utf-8")
    assert "sqlite3.connect" not in src
    assert "import sqlite3" not in src
    assert "BoardRebuildIngestionAdapter" not in src
    assert "_expected_layers_from_sources" in src
    assert _CORE_MODULE not in ALLOWED_INGESTION_SQLITE_SUFFIXES


def test_community_rebuild_ingestion_owns_raw_sqlite_adapter() -> None:
    src = (_COMMUNITY_SRC_ROOT / _COMMUNITY_MODULE).read_text(encoding="utf-8")
    assert "class CommunityBoardRebuildIngestionAdapter" in src
    assert "sqlite3.connect" in src
    assert "resolve_pulse_db_path" in src
    assert "build_step_adapter" in src
