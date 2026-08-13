"""MKG-E C1 — kind_of column migration (S1) + physical-enforcement
non-regression assertions (S8, static part)."""

from __future__ import annotations

import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.kg.schema_contract import (
    LEGACY_NODE_COLUMNS,
    NODE_TYPES,
    SCHEMA_VERSION,
    STABLE_NODE_PROPERTIES,
    SUBTYPE_COLUMNS,
    VECTOR_INDEX_TYPES,
)

from kg_schema_testing import (
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)


@pytest.fixture
def kg_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_subtypemig_"))
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
            for value in res.get_next():
                if isinstance(value, str):
                    cols.add(value)
    finally:
        try:
            res.close()
        except Exception:
            pass
    return cols


def test_s1_fresh_bootstrap_has_kind_of_and_version(kg_tempdir):
    assert SCHEMA_VERSION == "0.5.0"
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    conn_ctx = open_board_connection(board_id)
    with conn_ctx as (_kdb, kconn):
        for node_type in NODE_TYPES:
            assert "kind_of" in _columns(kconn, node_type), node_type


def test_s1_legacy_shaped_table_gains_kind_of_idempotently(kg_tempdir):
    from okto_pulse.community.adapters.kg_runtime import _ensure_subtype_columns

    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    conn_ctx = open_board_connection(board_id)
    with conn_ctx as (_kdb, kconn):
        kconn.execute(
            "CREATE NODE TABLE IF NOT EXISTS LegacyShimE ("
            "id STRING PRIMARY KEY, title STRING)"
        )
        added = _ensure_subtype_columns(kconn, "LegacyShimE")
        assert added == ["kind_of"]
        assert _ensure_subtype_columns(kconn, "LegacyShimE") == []


def test_s8_physical_enforcement_untouched(kg_tempdir):
    """Static half of S8: the closed physical taxonomy is unchanged and
    kind_of never enters the vector/digest surface."""
    from okto_pulse.core.application.processors.global_outbox import (
        DIGESTED_NODE_TYPES,
    )
    from okto_pulse.community.adapters.kg_runtime import (
        _node_has_legacy_columns,
    )

    assert len(NODE_TYPES) == 11
    assert tuple(DIGESTED_NODE_TYPES) == tuple(VECTOR_INDEX_TYPES)
    assert "kind_of" not in VECTOR_INDEX_TYPES
    assert "kind_of" in STABLE_NODE_PROPERTIES
    assert LEGACY_NODE_COLUMNS == ("validation_status", "corroboration_count")
    for name, _ in SUBTYPE_COLUMNS:
        assert name not in LEGACY_NODE_COLUMNS

    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    conn_ctx = open_board_connection(board_id)
    with conn_ctx as (_kdb, kconn):
        for node_type in NODE_TYPES:
            assert _node_has_legacy_columns(kconn, node_type) is False
