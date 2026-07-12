"""MKG-D C4 — superseded filter across recall surfaces (S5) + HNSW over-fetch (S6).

S5 runs against a real graph (find_by_topic, related-context via
find_by_artifact/filtered). S6 exercises find_similar_nodes_by_type's
over-fetch + filter with a deterministic fake k-NN connection (the HNSW
index behaviour itself is unchanged; what MKG-D adds is the projection,
pool size and the active-only trim).
"""

from __future__ import annotations

import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.kg.cypher_templates import superseded_filter_clause
from okto_pulse.core.kg.interfaces.graph_store import QueryFilters
from okto_pulse.core.kg.scoring import DECAY_REORDER_POOL_MULTIPLIER
from okto_pulse.core.kg.search import find_similar_nodes_by_type

from kg_schema_testing import (
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)


@pytest.fixture
def kg_board(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_recallf_"))
    monkeypatch.setenv("KG_BASE_DIR", str(base))
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    yield board_id
    try:
        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


def _seed_decisions(board_id: str) -> None:
    conn_ctx = open_board_connection(board_id)
    with conn_ctx as (_kdb, kconn):
        kconn.execute(
            "CREATE (n:Decision {id: 'decision_active', title: 'Filtro ativo',"
            " content: 'c', source_confidence: 0.9, relevance_score: 0.9,"
            " graph_layer: 'canonical', source_artifact_ref: 'spec:recall'})"
        )
        kconn.execute(
            "CREATE (n:Decision {id: 'decision_old', title: 'Filtro antigo',"
            " content: 'c', source_confidence: 0.9, relevance_score: 0.9,"
            " graph_layer: 'canonical', source_artifact_ref: 'spec:recall',"
            " superseded_by: 'decision_active'})"
        )


def _store():
    from okto_pulse.community.adapters.kuzu_graph_store import (
        CommunityKuzuGraphStore,
    )

    return CommunityKuzuGraphStore()


def test_s5_find_by_topic_excludes_superseded_by_default(kg_board):
    _seed_decisions(kg_board)
    store = _store()
    rows = store.find_by_topic(kg_board, "Decision", "Filtro", QueryFilters())
    ids = {r[0] for r in rows}
    assert "decision_active" in ids
    assert "decision_old" not in ids


def test_s5_find_by_topic_opt_in_returns_superseded(kg_board):
    _seed_decisions(kg_board)
    store = _store()
    rows = store.find_by_topic(
        kg_board, "Decision", "Filtro", QueryFilters(include_superseded=True)
    )
    ids = {r[0] for r in rows}
    assert {"decision_active", "decision_old"} <= ids


def test_s5_related_context_excludes_superseded_hops(kg_board):
    conn_ctx = open_board_connection(kg_board)
    with conn_ctx as (_kdb, kconn):
        kconn.execute(
            "CREATE (n:Entity {id: 'entity_center', title: 'centro',"
            " source_confidence: 0.9, graph_layer: 'canonical',"
            " source_artifact_ref: 'spec:ctx'})"
        )
        kconn.execute(
            "CREATE (n:Entity {id: 'entity_hop_active', title: 'hop ativo',"
            " source_confidence: 0.9, graph_layer: 'canonical'})"
        )
        kconn.execute(
            "CREATE (n:Entity {id: 'entity_hop_old', title: 'hop antigo',"
            " source_confidence: 0.9, graph_layer: 'canonical',"
            " superseded_by: 'entity_hop_active'})"
        )
        for hop in ("entity_hop_active", "entity_hop_old"):
            kconn.execute(
                "MATCH (a:Entity {id: $hop}), (b:Entity {id: 'entity_center'}) "
                "CREATE (a)-[r:belongs_to {confidence: 1.0, layer: 'cognitive',"
                " created_by_session_id: 's', created_at: timestamp('2026-07-12T00:00:00'),"
                " rule_id: '', created_by: 's', fallback_reason: ''}]->(b)",
                {"hop": hop},
            )
    store = _store()
    rows = store.find_by_artifact(
        kg_board, "spec:ctx", QueryFilters(), graph_layer="all"
    )
    hop_ids = {r[2] for r in rows}
    assert "entity_hop_active" in hop_ids
    assert "entity_hop_old" not in hop_ids

    rows_opt_in = store.find_by_artifact(
        kg_board,
        "spec:ctx",
        QueryFilters(include_superseded=True),
        graph_layer="all",
    )
    hop_ids_opt_in = {r[2] for r in rows_opt_in}
    assert {"entity_hop_active", "entity_hop_old"} <= hop_ids_opt_in


class _FakeKnnResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def has_next(self):
        return bool(self._rows)

    def get_next(self):
        return self._rows.pop(0)


class _FakeKnnConn:
    """Deterministic stand-in for the k-NN call: half the index superseded."""

    def __init__(self, total: int):
        self.requested_k: int | None = None
        self._total = total

    def execute(self, cypher: str, params: dict):
        assert "QUERY_VECTOR_INDEX" in cypher
        assert "node.superseded_by" in cypher  # projection added by MKG-D
        self.requested_k = params["k"]
        rows = []
        for i in range(min(self._total, params["k"])):
            superseded = "newer" if i % 2 == 0 else None
            rows.append(
                [f"learning_{i:03d}", f"t{i}", None, 0.05 * i, superseded]
            )
        return _FakeKnnResult(rows)


def test_s6_hnsw_overfetch_returns_topk_active(kg_board):
    conn = _FakeKnnConn(total=64)
    results = find_similar_nodes_by_type(
        kg_board,
        "Learning",
        [0.0] * 384,
        top_k=5,
        min_similarity=0.0,
        conn=conn,
    )
    # Over-fetch pool requested (D6 — same multiplier as the decay reorder).
    assert conn.requested_k == 5 * DECAY_REORDER_POOL_MULTIPLIER
    # k ACTIVE results, none superseded, order preserved.
    assert len(results) == 5
    assert [r.kuzu_node_id for r in results] == [
        "learning_001",
        "learning_003",
        "learning_005",
        "learning_007",
        "learning_009",
    ]


def test_s6_opt_in_keeps_legacy_pool_and_returns_superseded(kg_board):
    conn = _FakeKnnConn(total=64)
    results = find_similar_nodes_by_type(
        kg_board,
        "Learning",
        [0.0] * 384,
        top_k=4,
        min_similarity=0.0,
        conn=conn,
        include_superseded=True,
    )
    assert conn.requested_k == 4
    assert [r.kuzu_node_id for r in results] == [
        "learning_000",
        "learning_001",
        "learning_002",
        "learning_003",
    ]


def test_filter_clause_shape():
    assert superseded_filter_clause("n") == (
        "($include_superseded = true OR n.superseded_by IS NULL)"
    )
