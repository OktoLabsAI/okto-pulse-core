"""Integration tests for priority_boost persistence in Kùzu (spec 0eb51d3e).

Covers TS8 / TS9 / TS10:

- TS8 / AC1+AC3: commit-persist + recompute preserves frozen boost
- TS9 / AC7: ALTER TABLE ADD migration is idempotent across re-bootstraps
- TS10 / AC10: _recompute_relevance_batch reads the persisted boost per node
"""

from __future__ import annotations

import os

import pytest

from okto_pulse.core.kg.connection_pool import reset_connection_pool_for_tests
from okto_pulse.core.kg.schema import (
    NODE_TYPES,
    PRIORITY_BOOST_COLUMNS,
    _ensure_priority_boost_columns,
    apply_schema_to_connection,
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)
from okto_pulse.core.kg.scoring import (
    _compute_relevance,
    _fetch_node_inputs,
    _recompute_relevance,
    _recompute_relevance_batch,
    reset_histogram,
)


@pytest.fixture
def fresh_board():
    bid = f"board-pb-{os.urandom(4).hex()}"
    bootstrap_board_graph(bid)
    reset_histogram()
    yield bid
    close_all_connections()
    reset_connection_pool_for_tests()


def _insert_entity(conn, node_id: str, *, source_conf: float, boost: float) -> None:
    """Insert a minimal Entity row directly via Cypher."""
    conn.execute(
        "CREATE (n:Entity {id: $id, title: $t, content: $c, "
        "context: '', justification: '', source_artifact_ref: '', "
        "source_session_id: 'sess-test', "
        "created_at: timestamp('2026-04-19T12:00:00'), "
        "created_by_agent: 'agent-test', "
        "source_confidence: $sc, relevance_score: 0.5, "
        "query_hits: 0, last_queried_at: NULL, "
        "priority_boost: $pb, "
        "embedding: $emb})",
        {
            "id": node_id, "t": "t", "c": "c",
            "sc": source_conf, "pb": boost,
            "emb": [0.1] * 384,
        },
    )


# ---------------------------------------------------------------------------
# TS9 (AC7): migration idempotence + fresh-board has the column
# ---------------------------------------------------------------------------


def test_priority_boost_columns_declared():
    names = [c[0] for c in PRIORITY_BOOST_COLUMNS]
    assert names == ["priority_boost"]


def test_ts9_fresh_board_has_priority_boost_column(fresh_board):
    """Fresh bootstrap includes priority_boost on every node type."""
    with open_board_connection(fresh_board) as (_db, conn):
        for ntype in NODE_TYPES:
            res = conn.execute(f"CALL TABLE_INFO('{ntype}') RETURN *")
            cols: set[str] = set()
            while res.has_next():
                for item in res.get_next():
                    if isinstance(item, str):
                        cols.add(item)
            assert "priority_boost" in cols, f"{ntype} missing priority_boost"


def test_ts9_ensure_priority_boost_columns_is_idempotent(fresh_board):
    """ALTER TABLE ADD priority_boost twice in a row is a safe no-op."""
    with open_board_connection(fresh_board) as (_db, conn):
        first = _ensure_priority_boost_columns(conn, "Entity")
        second = _ensure_priority_boost_columns(conn, "Entity")
    # Fresh board already has the column (DDL created it), so both passes
    # report zero additions — no exception bubbles up either way.
    assert first == []
    assert second == []


def test_ts9_apply_schema_to_connection_is_idempotent(fresh_board):
    """Running the full schema application twice doesn't error."""
    import gc

    bc = open_board_connection(fresh_board)
    try:
        apply_schema_to_connection(bc.conn)
        apply_schema_to_connection(bc.conn)
        # Verify column still present in the same session — avoids Kùzu
        # re-acquiring the file lock on Windows.
        res = bc.conn.execute("CALL TABLE_INFO('Entity') RETURN *")
        cols: set[str] = set()
        while res.has_next():
            for item in res.get_next():
                if isinstance(item, str):
                    cols.add(item)
        assert "priority_boost" in cols
    finally:
        bc.close()
        del bc
        gc.collect()


# ---------------------------------------------------------------------------
# TS8 (AC1 + AC3): commit persists priority_boost, recompute preserves it
# ---------------------------------------------------------------------------


def test_ts8_insert_persists_priority_boost(fresh_board):
    """An insert carrying priority_boost=0.10 reads back 0.10 in the column."""
    with open_board_connection(fresh_board) as (_db, conn):
        _insert_entity(conn, "e-ts8-insert", source_conf=0.5, boost=0.10)
        res = conn.execute(
            "MATCH (n:Entity {id: 'e-ts8-insert'}) RETURN n.priority_boost"
        )
        assert res.has_next()
        row = res.get_next()
        assert row[0] == pytest.approx(0.10)


def test_ts8_fetch_node_inputs_surfaces_priority_boost(fresh_board):
    """_fetch_node_inputs includes priority_boost in its returned dict."""
    with open_board_connection(fresh_board) as (_db, conn):
        _insert_entity(conn, "e-ts8-fetch", source_conf=0.5, boost=0.15)
        inputs = _fetch_node_inputs(conn, "Entity", "e-ts8-fetch")
        assert inputs is not None
        assert inputs["priority_boost"] == pytest.approx(0.15)


def test_ts8_recompute_preserves_frozen_boost_in_column(fresh_board):
    """AC3 (BR2 immutability): after recompute, the persisted
    priority_boost column is untouched.

    We check the column value, not the derived score — the score can move
    legitimately as degree/hits/penalty evolve; the boost must not.
    """
    with open_board_connection(fresh_board) as (_db, conn):
        _insert_entity(conn, "e-ts8-frozen", source_conf=0.5, boost=0.10)
        _recompute_relevance(conn, fresh_board, "Entity", "e-ts8-frozen",
                             trigger="test_frozen")
        res = conn.execute(
            "MATCH (n:Entity {id: 'e-ts8-frozen'}) "
            "RETURN n.priority_boost"
        )
        assert res.has_next()
        assert res.get_next()[0] == pytest.approx(0.10)


def test_ts8_recompute_twice_still_frozen(fresh_board):
    """Running recompute multiple times never touches priority_boost."""
    with open_board_connection(fresh_board) as (_db, conn):
        _insert_entity(conn, "e-ts8-twice", source_conf=0.8, boost=0.20)
        _recompute_relevance(conn, fresh_board, "Entity", "e-ts8-twice",
                             trigger="first")
        _recompute_relevance(conn, fresh_board, "Entity", "e-ts8-twice",
                             trigger="second")
        res = conn.execute(
            "MATCH (n:Entity {id: 'e-ts8-twice'}) RETURN n.priority_boost"
        )
        assert res.get_next()[0] == pytest.approx(0.20)


# ---------------------------------------------------------------------------
# TS10 (AC10): _recompute_relevance_batch applies persisted boost per node
# ---------------------------------------------------------------------------


def test_ts10_recompute_batch_surfaces_boost_to_fetch(fresh_board):
    """TS10 (AC10): _recompute_relevance_batch pulls the right boost per node.

    We verify via _fetch_node_inputs after the batch that each node's
    persisted boost matches what was inserted. This is the invariant the
    batch recompute relies on — if _fetch returns the correct boost, the
    inline _compute_relevance receives it and produces per-node scores.
    """
    with open_board_connection(fresh_board) as (_db, conn):
        _insert_entity(conn, "e-ts10-a", source_conf=0.5, boost=0.20)
        _insert_entity(conn, "e-ts10-b", source_conf=0.5, boost=0.0)
        _recompute_relevance_batch(
            conn,
            fresh_board,
            [("Entity", "e-ts10-a"), ("Entity", "e-ts10-b")],
            trigger="batch_test",
        )
        inputs_a = _fetch_node_inputs(conn, "Entity", "e-ts10-a")
        inputs_b = _fetch_node_inputs(conn, "Entity", "e-ts10-b")
        assert inputs_a is not None and inputs_b is not None
        # Boost columns are surfaced to the compute step per node...
        assert inputs_a["priority_boost"] == pytest.approx(0.20)
        assert inputs_b["priority_boost"] == pytest.approx(0.0)
        # ...and survive the batch recompute untouched (BR2 immutability).
        res = conn.execute(
            "MATCH (n:Entity) WHERE n.id IN ['e-ts10-a','e-ts10-b'] "
            "RETURN n.id, n.priority_boost"
        )
        persisted: dict[str, float] = {}
        while res.has_next():
            row = res.get_next()
            persisted[row[0]] = float(row[1])
    assert persisted["e-ts10-a"] == pytest.approx(0.20)
    assert persisted["e-ts10-b"] == pytest.approx(0.0)


def test_ts10_boost_produces_score_difference_pure():
    """AC10 pure-function proof: same signals + different boost → different
    score. Demonstrates the invariant the batch path is exercising.
    """
    score_boosted = _compute_relevance(0.5, 10, 0.5, 0.5, priority_boost=0.20)
    score_plain = _compute_relevance(0.5, 10, 0.5, 0.5, priority_boost=0.0)
    # The boost passes straight through the sum.
    assert score_boosted - score_plain == pytest.approx(0.20, abs=1e-9)
