"""MKG-D C3 — generic supersedence chain per node type (S4 partial).

Real graph: a 2-hop Learning chain is walkable via
get_supersedence_chain(node_type='Learning'); the Decision default keeps
the legacy behaviour; an unknown type fails closed.
"""

from __future__ import annotations

import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest
from kg_registry_testing import configure_real_graph_test_kg_registry

from okto_pulse.core.kg.cypher_templates import (
    GET_SUPERSEDENCE_CHAIN,
    supersedence_chain_template,
)
from okto_pulse.core.kg.kg_service import KGToolError, get_kg_service
from okto_pulse.core.kg.transaction import TransactionOrchestrator

from kg_schema_testing import (
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)


@pytest.fixture
def kg_board(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_chain_"))
    monkeypatch.setenv("KG_BASE_DIR", str(base))
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    configure_real_graph_test_kg_registry()
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    yield board_id
    try:
        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


def test_template_decision_is_legacy_byte_identical():
    assert supersedence_chain_template("Decision") == GET_SUPERSEDENCE_CHAIN


def test_template_rejects_unknown_label():
    with pytest.raises(ValueError):
        supersedence_chain_template("DROP TABLE x")


def test_learning_chain_is_walkable_two_hops(kg_board):
    board_id = kg_board
    conn_ctx = open_board_connection(board_id)
    with conn_ctx as (_kdb, kconn):
        kconn.execute(
            "CREATE (n:Learning {id: 'learning_v1', title: 'v1'})"
        )
        orch = TransactionOrchestrator(
            kconn, session_id="s-chain", board_id=board_id
        )
        orch.supersede_node(
            "Learning",
            "learning_v2",
            "learning_v1",
            {"title": "v2", "content": "c", "created_at": "2026-07-12T00:00:00"},
        )
        orch.supersede_node(
            "Learning",
            "learning_v3",
            "learning_v2",
            {"title": "v3", "content": "c", "created_at": "2026-07-12T00:00:01"},
        )

    svc = get_kg_service()
    # Walk DOWN the chain from the newest node (edge points new -> old).
    result = svc.get_supersedence_chain(
        board_id, "learning_v3", node_type="Learning"
    )
    assert result["depth"] == 2
    assert [n["id"] for n in result["chain"]] == ["learning_v2", "learning_v1"]


def test_decision_default_unchanged(kg_board):
    svc = get_kg_service()
    result = svc.get_supersedence_chain(kg_board, "decision_none")
    assert result["depth"] == 0
    assert result["current_active"] == "decision_none"


def test_invalid_node_type_fails_closed(kg_board):
    svc = get_kg_service()
    with pytest.raises(KGToolError) as excinfo:
        svc.get_supersedence_chain(kg_board, "x", node_type="NotAType")
    assert excinfo.value.code == "invalid_node_type"
