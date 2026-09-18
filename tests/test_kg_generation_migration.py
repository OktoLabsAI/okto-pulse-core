"""MKG-A C2 — the generation column (spec MKG-A-S1 TR4, scenario S8).

A fresh bootstrap creates the column on every node type, and a NULL generation
(a row written without it) reads as 0 through core ``_node_generation``.
"""

from __future__ import annotations

import gc
import uuid

import pytest

from kg_schema_testing import (
    NODE_TYPES,
    board_graph_columns,
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)


@pytest.fixture
def kg_tempdir(monkeypatch):
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    yield
    try:
        close_all_connections()
    except Exception:
        pass
    gc.collect()


def test_fresh_bootstrap_has_generation_on_all_node_types(kg_tempdir):
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    for node_type in NODE_TYPES:
        assert "generation" in board_graph_columns(board_id, node_type), node_type


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
