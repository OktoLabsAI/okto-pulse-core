"""MKG-D C4 — superseded filter across semantic recall surfaces.

S5 runs against a real graph (find_by_topic, related-context via
find_by_artifact/filtered). S6 proves that the Core propagates the active-only
decision through the semantic graph-store port. Native index over-fetch and
fallback are covered by the Community adapter tests.
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
from okto_pulse.core.kg.interfaces.registry import get_kg_registry
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


class _FakeSemanticVectorStore:
    def __init__(self) -> None:
        self.include_superseded: bool | None = None

    def vector_search(
        self,
        board_id,
        node_type,
        query_vec,
        top_k,
        min_similarity,
        *,
        include_superseded=False,
    ):
        self.include_superseded = include_superseded
        rows = [
            {
                "node_id": f"learning_{index:03d}",
                "node_type": node_type,
                "title": f"t{index}",
                "source_artifact_ref": None,
                "similarity": 1.0 - index * 0.05,
            }
            for index in range(10)
            if include_superseded or index % 2 == 1
        ]
        return rows[:top_k]


def test_s6_semantic_port_defaults_to_active_nodes(kg_board, monkeypatch):
    store = _FakeSemanticVectorStore()
    monkeypatch.setattr(get_kg_registry(), "graph_store", store)
    results = find_similar_nodes_by_type(
        kg_board,
        "Learning",
        [0.0] * 384,
        top_k=5,
        min_similarity=0.0,
    )
    assert store.include_superseded is False
    assert [r.graph_node_id for r in results] == [
        "learning_001",
        "learning_003",
        "learning_005",
        "learning_007",
        "learning_009",
    ]


def test_s6_semantic_port_propagates_superseded_opt_in(kg_board, monkeypatch):
    store = _FakeSemanticVectorStore()
    monkeypatch.setattr(get_kg_registry(), "graph_store", store)
    results = find_similar_nodes_by_type(
        kg_board,
        "Learning",
        [0.0] * 384,
        top_k=4,
        min_similarity=0.0,
        include_superseded=True,
    )
    assert store.include_superseded is True
    assert [r.graph_node_id for r in results] == [
        "learning_000",
        "learning_001",
        "learning_002",
        "learning_003",
    ]


def test_filter_clause_shape():
    assert superseded_filter_clause("n") == (
        "($include_superseded = true OR n.superseded_by IS NULL)"
    )
