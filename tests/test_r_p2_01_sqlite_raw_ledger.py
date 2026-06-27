"""R-P2-01 — the raw-SQLite residuals are GOVERNED before any removal.

``BoardSourceStore`` (KG source materialization) and
``BoardRebuildIngestionAdapter`` (rebuild ingestion) still use raw
``sqlite3.connect``. R-P2-01 is governance-only (register-before-remove): this
suite fails when either residual is NOT in the adapter-readiness ledger, or when
its ``removal_criterion`` / ``oracles_required`` are empty — and proves the
governance does NOT remove the raw sqlite3 usage (the residual must still exist,
or the ledger entry would be vacuous). No test here requires removing
``sqlite3.connect``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
    REQUIRED_ADAPTER_KEYS,
    build_adapter_inventory,
)

_SQLITE_RAW_RESIDUALS = ("board_source_store", "board_rebuild_ingestion_adapter")
_RESIDUAL_MODULES = {
    "board_source_store": "okto_pulse/core/kg/board_source_store.py",
    "board_rebuild_ingestion_adapter": "okto_pulse/core/kg/board_rebuild_adapter.py",
}
_SRC_ROOT = Path(__file__).resolve().parents[1] / "src"


def _by_key(key: str):
    return next(e for e in build_adapter_inventory() if e.adapter_key == key)


def test_sqlite_raw_residuals_are_registered_in_the_ledger() -> None:
    keys = {e.adapter_key for e in build_adapter_inventory()}
    for key in _SQLITE_RAW_RESIDUALS:
        assert key in REQUIRED_ADAPTER_KEYS, f"{key} missing from REQUIRED_ADAPTER_KEYS"
        assert key in keys, f"{key} not registered in build_adapter_inventory"


@pytest.mark.parametrize("key", _SQLITE_RAW_RESIDUALS)
def test_residual_has_non_empty_removal_criterion_and_oracles(key: str) -> None:
    e = _by_key(key)
    # The governance fields that gate a future removal must be filled.
    assert e.removal_criterion.strip(), f"{key} has empty removal_criterion"
    assert e.oracles_required, f"{key} has empty oracles_required"
    assert all(o.strip() for o in e.oracles_required), f"{key} has a blank oracle"
    assert e.current_module == _RESIDUAL_MODULES[key]
    assert e.owner.strip() and e.port_ref.strip() and e.target_destination.strip()
    assert e.predecessor_refs, f"{key} has no predecessor_refs"


@pytest.mark.parametrize("key", _SQLITE_RAW_RESIDUALS)
def test_governed_residual_still_uses_raw_sqlite(key: str) -> None:
    # R-P2-01 is governance-ONLY: the raw sqlite3.connect is NOT removed in this
    # spec. The ledgered residual must still be a real raw-SQLite consumer — else
    # the ledger entry would be vacuous (or someone removed it out of scope).
    module = _RESIDUAL_MODULES[key]
    src = (_SRC_ROOT / module).read_text(encoding="utf-8")
    assert "sqlite3.connect" in src, (
        f"{key} ({module}) no longer uses raw sqlite3.connect — the R-P2-01 ledger "
        f"entry is stale, or the residual was removed out of this spec's scope"
    )
