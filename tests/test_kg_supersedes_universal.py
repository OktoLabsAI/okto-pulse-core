"""MKG-D C2 — universal :supersedes endpoint pairs (S4 partial).

The schema contract declares a ``supersedes`` pair for every node type, and a
fresh Grafx bootstrap must make each of them a walkable edge.
"""

from __future__ import annotations

import gc
import uuid

import pytest

from okto_pulse.core.kg.schema_contract import MULTI_REL_TYPES, NODE_TYPES
from okto_pulse.core.kg.transaction import TransactionOrchestrator

from kg_schema_testing import (
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


def test_contract_declares_all_pairs():
    pairs = dict(MULTI_REL_TYPES)["supersedes"]
    assert set(pairs) == {(t, t) for t in NODE_TYPES}


@pytest.mark.parametrize("node_type", ["Learning", "Entity", "Assumption"])
def test_supersede_creates_walkable_edge_for_non_decision_types(
    kg_tempdir, node_type
):
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    conn_ctx = open_board_connection(board_id)
    with conn_ctx as (_kdb, kconn):
        old_id = f"{node_type.lower()}_old001"
        new_id = f"{node_type.lower()}_new001"
        kconn.execute(
            f"CREATE (n:{node_type} {{id: '{old_id}', title: 'antigo'}})"
        )
        orch = TransactionOrchestrator(
            kconn,

            session_id="sess-supuniv",
            board_id=board_id,
        )
        orch.supersede_node(
            node_type,
            new_id,
            old_id,
            {"title": "novo", "content": "c", "created_at": "2026-07-12T00:00:00"},
            revocation_reason="test",
        )
        res = kconn.execute(
            f"MATCH (a:{node_type})-[r:supersedes]->(b:{node_type}) "
            "WHERE a.id = $new AND b.id = $old RETURN count(r)",
            {"new": new_id, "old": old_id},
        )
        assert int(res.get_next()[0]) == 1
        res.close()
        res2 = kconn.execute(
            f"MATCH (b:{node_type}) WHERE b.id = $old RETURN b.superseded_by",
            {"old": old_id},
        )
        assert res2.get_next()[0] == new_id
        res2.close()
