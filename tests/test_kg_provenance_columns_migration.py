"""MKG-B C1 — provenance + attestation columns migration (S1) and the
anti-legacy regression guard (S8)."""

from __future__ import annotations

import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.kg.schema_contract import (
    ATTESTATION_COLUMNS,
    LEGACY_NODE_COLUMNS,
    PROVENANCE_COLUMNS,
    SCHEMA_VERSION,
    STABLE_NODE_PROPERTIES,
)

from kg_schema_testing import (
    NODE_TYPES,
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)

NEW_COLUMNS = [name for name, _ in PROVENANCE_COLUMNS + ATTESTATION_COLUMNS]


@pytest.fixture
def kg_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_provmig_"))
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


def test_s1_fresh_bootstrap_has_all_new_columns_and_version(kg_tempdir):
    assert SCHEMA_VERSION == "0.4.0"
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    conn_ctx = open_board_connection(board_id)
    with conn_ctx as (_kdb, kconn):
        for node_type in NODE_TYPES:
            cols = _columns(kconn, node_type)
            for col in NEW_COLUMNS:
                assert col in cols, f"{node_type} missing {col}"


def test_s1_legacy_shaped_table_gains_columns_idempotently(kg_tempdir):
    from okto_pulse.community.adapters.kg_runtime import (
        _ensure_attestation_columns,
        _ensure_provenance_columns,
    )

    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    conn_ctx = open_board_connection(board_id)
    with conn_ctx as (_kdb, kconn):
        kconn.execute(
            "CREATE NODE TABLE IF NOT EXISTS LegacyShimB ("
            "id STRING PRIMARY KEY, title STRING)"
        )
        added = _ensure_provenance_columns(kconn, "LegacyShimB")
        added += _ensure_attestation_columns(kconn, "LegacyShimB")
        assert sorted(added) == sorted(NEW_COLUMNS)
        # Idempotent second run adds nothing.
        assert _ensure_provenance_columns(kconn, "LegacyShimB") == []
        assert _ensure_attestation_columns(kconn, "LegacyShimB") == []


def test_s8_legacy_names_never_reactivated(kg_tempdir):
    from okto_pulse.community.adapters.graph_ddl import COMMON_NODE_ATTRIBUTES
    from okto_pulse.community.adapters.kg_runtime import _node_has_legacy_columns

    # The retired names never appear in the new tuples, stable props or DDL.
    for legacy in LEGACY_NODE_COLUMNS:
        assert legacy not in NEW_COLUMNS
        assert legacy not in STABLE_NODE_PROPERTIES
        assert legacy not in COMMON_NODE_ATTRIBUTES
    # LEGACY_NODE_COLUMNS itself is byte-identical to the audited value.
    assert LEGACY_NODE_COLUMNS == ("validation_status", "corroboration_count")

    # A freshly bootstrapped board is NEVER classified as v0.2.0-legacy.
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    conn_ctx = open_board_connection(board_id)
    with conn_ctx as (_kdb, kconn):
        for node_type in NODE_TYPES:
            assert _node_has_legacy_columns(kconn, node_type) is False
