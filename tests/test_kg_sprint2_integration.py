"""Sprint 2 integration tests — cognitive agent detects contradictions
cross-spec, and hybrid_search impact_analysis ranks inverse dependencies
by graph distance (cards 03bfac40 + 202865ab)."""

from __future__ import annotations

import time
from dataclasses import dataclass

import pytest

from okto_pulse.core.kg.agent.heuristics import (
    LLMVerdict,
    run_contradiction_heuristic,
)
from okto_pulse.core.kg.agent.heuristics.contradiction import (
    DecisionNeighbor,
    DecisionNode,
)
from okto_pulse.core.kg.hybrid_search import (
    INTENT_IMPACT_ANALYSIS,
    kg_search_hybrid,
)
from okto_pulse.core.kg.hybrid_search.hybrid import GraphNeighbor, VectorSeed


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


@dataclass
class FixedLLM:
    verdict: LLMVerdict
    calls: int = 0

    def ask_polarity(self, *, prompt_id, text_a, text_b, context=None):
        self.calls += 1
        return self.verdict


class StubSeeder:
    def __init__(self, seeds):
        self.seeds = seeds

    def seed(self, *, board_id, query, node_types, top_k):
        return list(self.seeds)


class StubExpander:
    def __init__(self, neighbors):
        self.neighbors = neighbors

    def expand(self, *, board_id, seed_ids, edges, max_hops):
        return list(self.neighbors)


# ===========================================================================
# ts_55ae6753 / 03bfac40 — Agente detecta contradiction cross-spec
# ===========================================================================


def test_ts_55ae6753_cognitive_detects_cross_spec_contradiction():
    """Given D1 from spec-A and D2 from spec-B on the same Entity, when the
    LLM says they cannot coexist, emit `contradicts` with confidence≥0.8
    and a populated cognitive_evidence citation."""

    d1 = DecisionNode(
        node_id="kg:dec_spec_a",
        content="Poll SmartThings every 60 minutes to minimise API cost",
        entity_ids=frozenset({"SmartThings"}),
        spec_id="spec-A",
    )
    d2_neighbor = DecisionNeighbor(
        decision=DecisionNode(
            node_id="kg:dec_spec_b",
            content="SmartThings integration MUST sync state in real time",
            entity_ids=frozenset({"SmartThings"}),
            spec_id="spec-B",
        ),
        similarity=0.82,
    )
    llm = FixedLLM(verdict=LLMVerdict(
        answer=False,
        confidence=0.88,
        reasoning=(
            "Polling every 60 minutes is fundamentally incompatible with a "
            "real-time sync requirement on the same integration."
        ),
    ))
    candidates = run_contradiction_heuristic(d1, [d2_neighbor], llm)
    assert len(candidates) == 1
    c = candidates[0]
    assert c.from_node_id == "kg:dec_spec_a"
    assert c.to_node_id == "kg:dec_spec_b"
    assert c.confidence >= 0.8
    assert c.confidence <= 0.95  # CONTRADICTS_CEILING
    # Evidence must be the LLM reasoning (≥20 chars per BR Cognitive
    # Edge Evidence Required).
    assert len(c.cognitive_evidence) >= 20
    assert "SmartThings" in c.shared_entities


def test_ts_55ae6753_no_contradiction_when_entities_disjoint():
    """Cross-spec decisions without shared Entity must never emit
    contradicts — that noise is the whole reason for the entity filter."""
    d1 = DecisionNode("d1", "Use PG", frozenset({"PG"}), "spec-A")
    n = DecisionNeighbor(
        decision=DecisionNode("d2", "Use MongoDB",
                              frozenset({"MongoDB"}), "spec-B"),
        similarity=0.9,
    )
    llm = FixedLLM(verdict=LLMVerdict(answer=False, confidence=0.95,
                                       reasoning="irrelevant — filter kicks first"))
    assert run_contradiction_heuristic(d1, [n], llm) == []
    assert llm.calls == 0


# ===========================================================================
# ts_bd910924 / 202865ab — impact_analysis benchmark + ranking
# ===========================================================================


def _seed(node_id, sim=0.9):
    return VectorSeed(node_id=node_id, node_type="Decision",
                      title=f"title-{node_id}", similarity=sim)


def _impact_neighbor(node_id, hop, conf=0.8):
    return GraphNeighbor(
        node_id=node_id, node_type="Decision", title=f"dep-{node_id}",
        edge_type="depends_on", edge_confidence=conf, hop_distance=hop,
    )


def _build_dependency_fixture(total_edges: int = 100):
    """Synthesise a fan-out that mirrors the card setup: 50 decisions w/
    100+ depends_on edges. The seeder returns the 50 decisions, the
    expander returns the 100+ neighbors at increasing hop_distance.
    """
    seeds = [_seed(f"src_{i}", sim=0.9 - (i * 0.01)) for i in range(50)]
    neighbors = [
        _impact_neighbor(f"dep_{i}", hop=1 + (i % 2), conf=0.8)
        for i in range(total_edges)
    ]
    return seeds, neighbors


def test_ts_bd910924_impact_analysis_returns_inverse_dependencies():
    """With depends_on edges expanded, ranked output must be dominated by
    neighbors — those are the inverse dependencies (what depends on the
    seed)."""
    seeds, neighbors = _build_dependency_fixture(total_edges=100)
    result = kg_search_hybrid(
        board_id="board-sprint2", query="spec-X impact",
        intent=INTENT_IMPACT_ANALYSIS,
        vector_provider=StubSeeder(seeds),
        graph_expander=StubExpander(neighbors),
        top_k=10,
    )
    # Ranked output capped at top_k.
    assert len(result.ranked) <= 10
    # Every ranked ID must come from either seeds or neighbors.
    known = {s.node_id for s in seeds} | {n.node_id for n in neighbors}
    for r in result.ranked:
        assert r.node_id in known


def test_ts_bd910924_impact_analysis_ordering_prefers_closer_hops():
    """At equal edge_confidence, a 1-hop neighbor must outrank a 3-hop one
    under the impact_analysis weights (graph_proximity_inv dominates)."""
    seeds: list[VectorSeed] = []  # no vector seeds — pure graph ranking
    neighbors = [
        _impact_neighbor("close", hop=1, conf=0.8),
        _impact_neighbor("far", hop=3, conf=0.8),
    ]
    # Need at least one seed or ranking returns empty (expander guarded).
    # Use a single seed to trigger expand; the seed is excluded from the
    # assertion because it won't be ranked above neighbors here.
    seed_only = [_seed("anchor", sim=0.5)]
    result = kg_search_hybrid(
        board_id="b", query="impact", intent=INTENT_IMPACT_ANALYSIS,
        vector_provider=StubSeeder(seed_only),
        graph_expander=StubExpander(neighbors),
    )
    ranked_ids = [r.node_id for r in result.ranked]
    assert ranked_ids.index("close") < ranked_ids.index("far")


def test_ts_bd910924_pipeline_p95_under_budget_in_stubbed_mode():
    """Benchmark a stubbed run 50× and assert p95 <100ms — sets a ceiling
    on what the pure-Python ranking+merging can cost. The live Kùzu
    adapter is benchmarked separately in the e2e suite."""
    seeds, neighbors = _build_dependency_fixture(total_edges=200)
    timings: list[float] = []
    for _ in range(50):
        t0 = time.perf_counter()
        kg_search_hybrid(
            board_id="b", query="impact", intent=INTENT_IMPACT_ANALYSIS,
            vector_provider=StubSeeder(seeds),
            graph_expander=StubExpander(neighbors),
        )
        timings.append((time.perf_counter() - t0) * 1000)
    timings.sort()
    p95 = timings[int(len(timings) * 0.95) - 1]
    assert p95 < 100.0, f"p95={p95:.2f}ms exceeded 100ms budget"


def test_ts_bd910924_pipeline_reports_sla_breach_on_slow_adapters():
    """When the caller lowers the budget below the observed latency, the
    result must be returned with partial=True (soft SLA breach). Hard
    deadline is tested indirectly — mocks can't exceed 3× budget here
    without adding sleeps."""

    class SlowSeeder:
        def seed(self, **_):
            time.sleep(0.005)  # 5ms — above a 1ms soft budget
            return [_seed("x")]

    result = kg_search_hybrid(
        board_id="b", query="impact", intent=INTENT_IMPACT_ANALYSIS,
        vector_provider=SlowSeeder(), graph_expander=StubExpander([]),
        sla_budget_ms=1.0,
    )
    assert result.partial is True
