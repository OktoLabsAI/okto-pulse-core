"""Unit tests for the hybrid search catalog + classifier + pipeline
(cards fa2889ec + 09a87c07)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable

import pytest

from okto_pulse.core.kg.hybrid_search import (
    INTENT_ALTERNATIVES_LOOKUP,
    INTENT_CATALOG,
    INTENT_CONTRADICTION_CHECK,
    INTENT_DEPENDENCY_TRACE,
    INTENT_IMPACT_ANALYSIS,
    INTENT_LEARNINGS_FOR_BUG,
    HybridSearchError,
    IntentNotFoundError,
    classify_intent,
    kg_search_hybrid,
    resolve_intent,
)
from okto_pulse.core.kg.hybrid_search.hybrid import (
    GraphNeighbor,
    VectorSeed,
)
from okto_pulse.core.kg.hybrid_search.intents import RankingWeights


# ===========================================================================
# Intent catalog
# ===========================================================================


def test_catalog_has_exactly_five_intents():
    assert set(INTENT_CATALOG.keys()) == {
        INTENT_CONTRADICTION_CHECK,
        INTENT_IMPACT_ANALYSIS,
        INTENT_ALTERNATIVES_LOOKUP,
        INTENT_LEARNINGS_FOR_BUG,
        INTENT_DEPENDENCY_TRACE,
    }


def test_all_intents_have_valid_ranking_weights():
    for intent in INTENT_CATALOG.values():
        total = (
            intent.weights.vector_sim
            + intent.weights.graph_proximity_inv
            + intent.weights.edge_confidence
            + intent.weights.recency_decay
        )
        assert abs(total - 1.0) < 0.001


def test_ranking_weights_catch_misconfiguration():
    with pytest.raises(ValueError, match="sum to 1.0"):
        RankingWeights(vector_sim=0.5, graph_proximity_inv=0.5,
                       edge_confidence=0.5, recency_decay=0.5)


def test_resolve_intent_happy_path():
    assert resolve_intent(INTENT_CONTRADICTION_CHECK).name == INTENT_CONTRADICTION_CHECK


def test_resolve_intent_rejects_unknown():
    with pytest.raises(IntentNotFoundError) as exc:
        resolve_intent("free_text_query")
    assert "free_text_query" in str(exc.value)
    assert INTENT_CONTRADICTION_CHECK in exc.value.supported


def test_resolve_intent_rejects_empty():
    with pytest.raises(IntentNotFoundError):
        resolve_intent("")


# ===========================================================================
# Classifier
# ===========================================================================


def test_classifier_picks_contradiction_on_keyword():
    intent = classify_intent("Does my spec contradict anything?")
    assert intent.name == INTENT_CONTRADICTION_CHECK


def test_classifier_picks_impact_analysis():
    intent = classify_intent("If I change spec X what breaks downstream?")
    assert intent.name == INTENT_IMPACT_ANALYSIS


def test_classifier_picks_alternatives_lookup():
    intent = classify_intent("Quais alternativas foram descartadas?")
    assert intent.name == INTENT_ALTERNATIVES_LOOKUP


def test_classifier_picks_learnings_for_bug():
    intent = classify_intent("Any similar bug we already solved?")
    assert intent.name == INTENT_LEARNINGS_FOR_BUG


def test_classifier_picks_dependency_trace():
    intent = classify_intent("What does this decision depend on?")
    assert intent.name == INTENT_DEPENDENCY_TRACE


def test_classifier_falls_back_to_llm_when_ambiguous():
    captured = {}

    def llm(query: str, supported: tuple[str, ...]) -> str:
        captured["query"] = query
        captured["supported"] = supported
        return INTENT_IMPACT_ANALYSIS

    intent = classify_intent("something irrelevant here", llm_fallback=llm)
    assert intent.name == INTENT_IMPACT_ANALYSIS
    assert INTENT_CONTRADICTION_CHECK in captured["supported"]


def test_classifier_rejects_llm_hallucinated_intent():
    def hallucinator(q, s):
        return "totally_made_up"
    with pytest.raises(IntentNotFoundError):
        classify_intent("garbage", llm_fallback=hallucinator)


def test_classifier_without_llm_raises_on_zero_matches():
    with pytest.raises(IntentNotFoundError):
        classify_intent("totally blank query")


# ===========================================================================
# Pipeline with in-memory stubs
# ===========================================================================


class _StubSeedProvider:
    def __init__(self, seeds):
        self.seeds = seeds
        self.calls = 0

    def seed(self, *, board_id, query, node_types, top_k):
        self.calls += 1
        return list(self.seeds)


class _StubExpander:
    def __init__(self, neighbors):
        self.neighbors = neighbors
        self.calls = 0

    def expand(self, *, board_id, seed_ids, edges, max_hops):
        self.calls += 1
        return list(self.neighbors)


def _seed(node_id, sim=0.9, ntype="Decision", title="t", created_at=None):
    return VectorSeed(node_id=node_id, node_type=ntype, title=title,
                      similarity=sim, created_at=created_at)


def _neighbor(node_id, hop=1, conf=0.8, ntype="Decision", title="n",
              edge="contradicts"):
    return GraphNeighbor(node_id=node_id, node_type=ntype, title=title,
                         edge_type=edge, edge_confidence=conf,
                         hop_distance=hop)


def test_pipeline_returns_ranked_nodes():
    seeds = [_seed("d1", sim=0.9), _seed("d2", sim=0.7)]
    neighbors = [_neighbor("d3", hop=1, conf=0.8),
                 _neighbor("d4", hop=2, conf=0.7)]
    result = kg_search_hybrid(
        board_id="b1", query="contradict", intent=INTENT_CONTRADICTION_CHECK,
        vector_provider=_StubSeedProvider(seeds),
        graph_expander=_StubExpander(neighbors),
    )
    assert result.intent == INTENT_CONTRADICTION_CHECK
    assert len(result.seeds) == 2
    assert len(result.neighbors) == 2
    assert len(result.ranked) >= 2
    # Seeds must be present in the ranked output.
    ranked_ids = {r.node_id for r in result.ranked}
    assert {"d1", "d2"}.issubset(ranked_ids)


def test_pipeline_ranks_by_score_descending():
    seeds = [_seed("d_high", sim=0.95), _seed("d_low", sim=0.3)]
    neighbors: list[GraphNeighbor] = []
    result = kg_search_hybrid(
        board_id="b1", query="x", intent=INTENT_CONTRADICTION_CHECK,
        vector_provider=_StubSeedProvider(seeds),
        graph_expander=_StubExpander(neighbors),
    )
    scores = [r.score for r in result.ranked]
    assert scores == sorted(scores, reverse=True)


def test_pipeline_explicit_intent_bypasses_classifier():
    seeds = [_seed("s1")]
    result = kg_search_hybrid(
        board_id="b1", query="garbage that wouldn't classify",
        intent=INTENT_IMPACT_ANALYSIS,
        vector_provider=_StubSeedProvider(seeds),
        graph_expander=_StubExpander([]),
    )
    assert result.intent == INTENT_IMPACT_ANALYSIS


def test_pipeline_rejects_unknown_intent():
    with pytest.raises(IntentNotFoundError):
        kg_search_hybrid(
            board_id="b1", query="x", intent="totally_unknown",
            vector_provider=_StubSeedProvider([]),
            graph_expander=_StubExpander([]),
        )


def test_pipeline_requires_board_id():
    with pytest.raises(HybridSearchError, match="board_id"):
        kg_search_hybrid(
            board_id="", query="x", intent=INTENT_CONTRADICTION_CHECK,
            vector_provider=_StubSeedProvider([]),
            graph_expander=_StubExpander([]),
        )


def test_pipeline_requires_query():
    with pytest.raises(HybridSearchError, match="query"):
        kg_search_hybrid(
            board_id="b", query="", intent=INTENT_CONTRADICTION_CHECK,
            vector_provider=_StubSeedProvider([]),
            graph_expander=_StubExpander([]),
        )


def test_pipeline_skips_graph_expand_when_no_seeds():
    expander = _StubExpander([])
    result = kg_search_hybrid(
        board_id="b", query="contradict", intent=INTENT_CONTRADICTION_CHECK,
        vector_provider=_StubSeedProvider([]),
        graph_expander=expander,
    )
    assert expander.calls == 0
    assert result.ranked == ()


def test_pipeline_recency_decay_favours_recent_nodes():
    now = datetime.now(timezone.utc)
    recent = _seed("recent", sim=0.8, created_at=now)
    stale = _seed("stale", sim=0.8, created_at=now - timedelta(days=180))
    result = kg_search_hybrid(
        board_id="b", query="x", intent=INTENT_ALTERNATIVES_LOOKUP,
        vector_provider=_StubSeedProvider([recent, stale]),
        graph_expander=_StubExpander([]),
    )
    ranked_ids = [r.node_id for r in result.ranked]
    assert ranked_ids.index("recent") < ranked_ids.index("stale")


def test_pipeline_merges_node_when_seed_and_neighbor():
    seeds = [_seed("dup", sim=0.9)]
    neighbors = [_neighbor("dup", hop=1, conf=0.99, edge="contradicts")]
    result = kg_search_hybrid(
        board_id="b", query="x", intent=INTENT_CONTRADICTION_CHECK,
        vector_provider=_StubSeedProvider(seeds),
        graph_expander=_StubExpander(neighbors),
    )
    ranked_ids = [r.node_id for r in result.ranked]
    assert ranked_ids.count("dup") == 1
    dup = next(r for r in result.ranked if r.node_id == "dup")
    # Merged node keeps max edge confidence.
    assert dup.edge_confidence == pytest.approx(0.99)


def test_pipeline_reports_timing_and_partial_flag():
    result = kg_search_hybrid(
        board_id="b", query="contradict", intent=INTENT_CONTRADICTION_CHECK,
        vector_provider=_StubSeedProvider([_seed("s1")]),
        graph_expander=_StubExpander([]),
        sla_budget_ms=100.0,
    )
    assert result.timing.total_ms >= 0
    assert result.partial is False
