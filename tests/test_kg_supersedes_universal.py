"""MKG-D C2 — universal :supersedes pairs + fail-closed probe (S4 partial, S8).

The fresh bootstrap itself exercises the conversion path: REL_TYPES creates
the single-pair table first, then the MULTI_REL_TYPES ensure ALTERs the
remaining (T, T) pairs in — exactly what a legacy board goes through.
"""

from __future__ import annotations

import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

import okto_pulse.community.adapters.kg_runtime as kg_runtime
from okto_pulse.core.kg.schema_contract import MULTI_REL_TYPES, NODE_TYPES
from okto_pulse.core.kg.transaction import TransactionOrchestrator

from kg_schema_testing import (
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)


@pytest.fixture
def kg_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_supuniv_"))
    monkeypatch.setenv("KG_BASE_DIR", str(base))
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    yield base
    try:
        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


def test_contract_declares_all_pairs():
    pairs = dict(MULTI_REL_TYPES)["supersedes"]
    assert set(pairs) == {(t, t) for t in NODE_TYPES}


def test_fresh_bootstrap_converts_and_probe_reports_complete(kg_tempdir):
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    conn_ctx = open_board_connection(board_id)
    with conn_ctx as (_kdb, kconn):
        assert kg_runtime._probe_supersedes_pairs(kconn) == []


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


def test_probe_failure_raises_structured_error(kg_tempdir, monkeypatch):
    """S8 (negative): a swallowed ALTER failure surfaces via the probe as a
    structured error — never an invisible no-op."""
    board_id = str(uuid.uuid4())
    monkeypatch.setattr(
        kg_runtime,
        "_probe_supersedes_pairs",
        lambda conn: [("Learning", "Learning"), ("Entity", "Entity")],
    )
    with pytest.raises(kg_runtime.SupersedesPairsIncompleteError) as excinfo:
        bootstrap_board_graph(board_id)
    err = excinfo.value
    assert err.code == "kg_supersedes_pairs_incomplete"
    assert ("Learning", "Learning") in err.missing_pairs
    assert "rebuild" in err.remediation
