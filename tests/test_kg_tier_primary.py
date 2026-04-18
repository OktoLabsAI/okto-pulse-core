"""Test suite for Tier Primario — 9 tools, cache, ACL, ranking.

Covers 4 test cards:
- 020c6300: Default filters + Cache + Invalidation
- 2a40aea3: ACL + Decision history chain
- 2667776d: MCP Registration + Ranking + Contradictions
- 928f6de0: Context Tools + Errors + Global Query
"""

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from okto_pulse.core.kg.cache import (
    cache_get,
    cache_put,
    cache_stats,
    clear_cache,
    invalidate_board,
)
from okto_pulse.core.kg.kg_service import (
    DefaultFilters,
    KGService,
    KGToolError,
    RankingWeights,
    get_kg_service,
    reset_kg_service_for_tests,
)
from okto_pulse.core.kg.schema import bootstrap_board_graph, open_board_connection
from okto_pulse.core.kg.tool_schemas import (
    AlternativesResponse,
    ConstraintExplanationResponse,
    ContradictionsResponse,
    DecisionHistoryResponse,
    GlobalQueryResponse,
    LearningsResponse,
    RelatedContextResponse,
    SimilarDecisionsResponse,
    SupersedenceChainResponse,
)

BOARD = "board-tier1-test"


@pytest.fixture(scope="module", autouse=True)
def _seed_data():
    """Seed a board with test data for the tier primario queries."""
    os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_t1_"))
    reset_kg_service_for_tests()
    clear_cache()

    handle = bootstrap_board_graph(BOARD)
    db, conn = open_board_connection(BOARD)
    emb_a = [0.1] * 384
    emb_b = [0.2] * 384
    emb_c = [0.3] * 384

    for nid, title, content, emb, vs in [
        ("dec-1", "Use Kuzu for KG", "Embedded graph DB", emb_a, "corroborated"),
        ("dec-2", "Use DuckDB for analytics", "Columnar DB", emb_b, "corroborated"),
        ("dec-3", "Deprecated SQLite KG", "Old approach", emb_c, "unvalidated"),
    ]:
        conn.execute(
            "CREATE (d:Decision {id: $id, title: $t, content: $c, "
            "source_artifact_ref: $ref, source_session_id: 's1', "
            "source_confidence: 0.9, validation_status: $vs, "
            "corroboration_count: 1, created_at: timestamp('2026-04-15T10:00:00'), "
            "created_by_agent: 'agent-1', embedding: $emb})",
            {"id": nid, "t": title, "c": content, "ref": "spec-1",
             "vs": vs, "emb": emb},
        )
    conn.execute(
        "CREATE (c:Constraint {id: 'cst-1', title: 'No Docker', "
        "content: 'Embedded only', source_artifact_ref: 'spec-1', "
        "source_session_id: 's1', source_confidence: 0.85, "
        "validation_status: 'corroborated', corroboration_count: 1, "
        "created_at: timestamp('2026-04-15T10:00:00'), "
        "created_by_agent: 'agent-1', embedding: $emb})",
        {"emb": emb_b},
    )
    conn.execute(
        "CREATE (a:Alternative {id: 'alt-1', title: 'Use Neo4j', "
        "content: 'Server-based', justification: 'Rejected: Docker', "
        "source_artifact_ref: 'spec-1', source_session_id: 's1', "
        "source_confidence: 0.7, validation_status: 'corroborated', "
        "corroboration_count: 0, created_at: timestamp('2026-04-15T10:00:00'), "
        "created_by_agent: 'agent-1', embedding: $emb})",
        {"emb": emb_c},
    )
    # Rels
    conn.execute(
        "MATCH (d:Decision {id: 'dec-1'}), (a:Alternative {id: 'alt-1'}) "
        "CREATE (d)-[:relates_to {confidence: 0.8, created_by_session_id: 's1', "
        "created_at: timestamp('2026-04-15T10:00:00')}]->(a)"
    )
    conn.execute(
        "MATCH (a:Decision {id: 'dec-1'}), (b:Decision {id: 'dec-2'}) "
        "CREATE (a)-[:contradicts {confidence: 0.6, created_by_session_id: 's1', "
        "created_at: timestamp('2026-04-15T10:00:00')}]->(b)"
    )
    del conn, db
    yield
    clear_cache()


@pytest.fixture(autouse=True)
def _clear_cache_per_test():
    clear_cache()
    yield


# ============================================================================
# Card 020c6300: Default filters + Cache + Invalidation
# ============================================================================


class TestDefaultFiltersCache:
    def test_default_filters_exclude_unvalidated(self):
        svc = get_kg_service()
        results = svc.get_decision_history(BOARD, "")
        titles = [r["title"] for r in results]
        assert "Deprecated SQLite KG" not in titles

    def test_default_min_confidence(self):
        svc = KGService(default_filters=DefaultFilters(min_confidence=0.95))
        results = svc.get_decision_history(BOARD, "")
        assert len(results) == 0

    def test_cache_hit_on_repeated_query(self):
        svc = get_kg_service()
        r1 = svc.get_decision_history(BOARD, "Kuzu")
        stats1 = cache_stats()
        r2 = svc.get_decision_history(BOARD, "Kuzu")
        assert r1 == r2
        assert stats1["size"] >= 1

    def test_invalidation_clears_board_cache(self):
        svc = get_kg_service()
        svc.get_decision_history(BOARD, "Kuzu")
        assert cache_stats()["size"] >= 1
        evicted = invalidate_board(BOARD)
        assert evicted >= 1
        assert cache_stats()["size"] == 0


# ============================================================================
# Card 2a40aea3: ACL + Decision history chain
# ============================================================================


class TestACLDecisionHistory:
    def test_acl_denies_unauthorized_board(self):
        svc = get_kg_service()
        with pytest.raises(KGToolError) as exc_info:
            svc.check_board_access(["other-board"], BOARD)
        assert exc_info.value.code == "permission_denied"

    def test_acl_allows_authorized_board(self):
        svc = get_kg_service()
        svc.check_board_access([BOARD], BOARD)

    def test_decision_history_returns_matching(self):
        svc = get_kg_service()
        results = svc.get_decision_history(BOARD, "Kuzu")
        assert len(results) >= 1
        assert results[0]["title"] == "Use Kuzu for KG"

    def test_decision_history_empty_topic(self):
        svc = get_kg_service()
        results = svc.get_decision_history(BOARD, "nonexistent_xyz_123")
        assert len(results) == 0

    def test_supersedence_chain_no_chain(self):
        svc = get_kg_service()
        result = svc.get_supersedence_chain(BOARD, "dec-1")
        assert result["depth"] == 0
        assert result["current_active"] == "dec-1"


# ============================================================================
# Card 2667776d: MCP Registration + Ranking + Contradictions
# ============================================================================


class TestRankingContradictions:
    def test_9_response_models_importable(self):
        models = [
            DecisionHistoryResponse, RelatedContextResponse,
            SupersedenceChainResponse, ContradictionsResponse,
            SimilarDecisionsResponse, ConstraintExplanationResponse,
            AlternativesResponse, LearningsResponse, GlobalQueryResponse,
        ]
        assert len(models) == 9

    def test_find_contradictions_all(self):
        svc = get_kg_service()
        pairs = svc.find_contradictions(BOARD)
        assert len(pairs) >= 1
        pair = pairs[0]
        assert "id_a" in pair and "id_b" in pair

    def test_find_contradictions_by_node(self):
        svc = get_kg_service()
        pairs = svc.find_contradictions(BOARD, node_id="dec-1")
        assert len(pairs) >= 1

    def test_ranking_weights_customizable(self):
        w1 = RankingWeights(semantic=1.0, graph_centrality=0, recency_decay=0, confidence=0)
        svc = KGService(ranking_weights=w1)
        assert svc.weights.semantic == 1.0

    def test_find_similar_decisions_no_crash(self):
        svc = get_kg_service()
        results = svc.find_similar_decisions(BOARD, "Use Kuzu for graph storage")
        assert isinstance(results, list)


# ============================================================================
# Card 928f6de0: Context Tools + Errors + Global Query
# ============================================================================


class TestContextToolsErrors:
    def test_explain_constraint(self):
        svc = get_kg_service()
        result = svc.explain_constraint(BOARD, "cst-1")
        assert result["title"] == "No Docker"
        assert "origins" in result
        assert "violations" in result

    def test_explain_constraint_not_found(self):
        svc = get_kg_service()
        with pytest.raises(KGToolError) as exc_info:
            svc.explain_constraint(BOARD, "nonexistent")
        assert exc_info.value.code == "not_found"

    def test_list_alternatives(self):
        svc = get_kg_service()
        alts = svc.list_alternatives(BOARD, "dec-1")
        assert len(alts) == 1
        assert alts[0]["title"] == "Use Neo4j"

    def test_get_related_context(self):
        svc = get_kg_service()
        results = svc.get_related_context(BOARD, "spec-1")
        assert isinstance(results, list)

    def test_query_global_empty_boards(self):
        svc = get_kg_service()
        results = svc.query_global("test", user_boards=[])
        assert results == []

    def test_query_global_with_board(self):
        svc = get_kg_service()
        results = svc.query_global("Kuzu", user_boards=[BOARD])
        assert isinstance(results, list)

    def test_schema_drift_detection(self):
        svc = get_kg_service()
        ver = svc.get_schema_version(BOARD)
        assert ver == "0.2.0"
