"""MKG-A C2 — generation column migration (spec MKG-A-S1 TR4, scenario S8).

Fresh bootstrap creates the column via DDL; a legacy-shaped table (created
WITHOUT generation) gains it via the idempotent ALTER-ADD ensure path; a
NULL generation (legacy row) reads as 0 through core ``_node_generation``.
"""

from __future__ import annotations

import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from kg_schema_testing import (
    NODE_TYPES,
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)


@pytest.fixture
def kg_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_genmig_"))
    monkeypatch.setenv("KG_BASE_DIR", str(base))
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    yield base
    try:
        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


def _columns(kconn, node_type: str) -> set[str]:
    res = kconn.execute(f"CALL TABLE_INFO('{node_type}') RETURN *")
    cols: set[str] = set()
    try:
        while res.has_next():
            row = res.get_next()
            for value in row:
                if isinstance(value, str):
                    cols.add(value)
    finally:
        try:
            res.close()
        except Exception:
            pass
    return cols


def test_fresh_bootstrap_has_generation_on_all_node_types(kg_tempdir):
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        for node_type in NODE_TYPES:
            assert "generation" in _columns(kconn, node_type), node_type


def test_legacy_table_gains_generation_via_ensure(kg_tempdir):
    from okto_pulse.community.adapters.kg_runtime import (
        _ensure_generation_columns,
    )

    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        # Simulate a legacy table shape: a brand-new table without the
        # generation column (the bootstrap DDL only creates the canonical
        # NODE_TYPES, so a handcrafted table stands in for a pre-0.3.8 one).
        kconn.execute(
            "CREATE NODE TABLE IF NOT EXISTS LegacyShim ("
            "id STRING PRIMARY KEY, title STRING)"
        )
        assert "generation" not in _columns(kconn, "LegacyShim")
        added = _ensure_generation_columns(kconn, "LegacyShim")
        assert added == ["generation"]
        assert "generation" in _columns(kconn, "LegacyShim")
        # Idempotent second run adds nothing.
        assert _ensure_generation_columns(kconn, "LegacyShim") == []


def test_null_generation_reads_as_zero(kg_tempdir):
    from okto_pulse.core.kg.primitives import _node_generation

    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        # Legacy row: generation column exists (fresh DDL) but value is NULL
        # because the INSERT never set it — exactly the post-upgrade shape.
        kconn.execute(
            "CREATE (n:Entity {id: 'entity_legacy001', title: 'legacy'})"
        )
        assert _node_generation(kconn, "Entity", "entity_legacy001") == 0
        # Missing node and read errors also default to 0 (never fail commit).
        assert _node_generation(kconn, "Entity", "entity_missing") == 0
        assert _node_generation(kconn, "Entity", "") == 0
