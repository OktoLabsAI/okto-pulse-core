"""Unit tests for kg.adaptive_hops (ideação 1fb13b51).

Covers the 4 strategies, factory contract, clamp behaviour, LRU cache,
and integration with kg_search_hybrid via stub provider/expander that
capture the max_hops value passed to expand().
"""

from __future__ import annotations

import pytest

from okto_pulse.core.kg.adaptive_hops import (
    FixedHopPlanner,
    HopDecision,
    LLMHopPlanner,
    clamp_hops,
    get_hop_planner,
    reset_planner_cache,
)
from okto_pulse.core.kg.hybrid_search import (
    INTENT_CONTRADICTION_CHECK,
    kg_search_hybrid,
)
from okto_pulse.core.kg.hybrid_search.hybrid import (
    VectorSeed,
)


# ===========================================================================
# clamp_hops
# ===========================================================================


def test_clamp_hops_inside_range():
    assert clamp_hops(1) == 1
    assert clamp_hops(2) == 2
    assert clamp_hops(3) == 3


def test_clamp_hops_clamps_floor():
    assert clamp_hops(0) == 1
    assert clamp_hops(-5) == 1


def test_clamp_hops_clamps_ceiling():
    assert clamp_hops(5) == 3
    assert clamp_hops(100) == 3


def test_clamp_hops_invalid_values_default():
    assert clamp_hops(None) == 2
    assert clamp_hops("oi") == 2
    assert clamp_hops([1, 2]) == 2


# ===========================================================================
# FixedHopPlanner
# ===========================================================================


def test_fixed_returns_configured_hops():
    p = FixedHopPlanner(fixed_max_hops=2)
    d = p.plan(query="x", intent_name="contradiction", seed_titles=["a"])
    assert d == HopDecision(hops=2, reason="fixed")


def test_fixed_with_one_hop():
    p = FixedHopPlanner(fixed_max_hops=1)
    d = p.plan(query="x", intent_name="contradiction", seed_titles=[])
    assert d.hops == 1
    assert d.reason == "fixed"


def test_fixed_clamps_construction():
    p = FixedHopPlanner(fixed_max_hops=10)  # out of range
    d = p.plan(query="x", intent_name="contradiction", seed_titles=[])
    assert d.hops == 3  # clamped


# ===========================================================================
# LLMHopPlanner
# ===========================================================================


def test_llm_planner_returns_llm_decision():
    p = LLMHopPlanner(lambda q, i, s: 2)
    d = p.plan(query="x", intent_name="contradiction", seed_titles=[])
    assert d.hops == 2
    assert d.reason == "llm"


def test_llm_planner_clamps_ceiling():
    p = LLMHopPlanner(lambda q, i, s: 5)
    d = p.plan(query="x", intent_name="contradiction", seed_titles=[])
    assert d.hops == 3


def test_llm_planner_clamps_floor():
    p = LLMHopPlanner(lambda q, i, s: 0)
    d = p.plan(query="x", intent_name="contradiction", seed_titles=[])
    assert d.hops == 1


def test_llm_planner_exception_fallback():
    def boom(q, i, s):
        raise RuntimeError("LLM down")

    p = LLMHopPlanner(boom, fallback_hops=2)
    d = p.plan(query="x", intent_name="contradiction", seed_titles=[])
    assert d.hops == 2
    assert d.reason == "llm_error_fallback"


def test_llm_planner_lru_cache_hit():
    reset_planner_cache()
    counter = {"count": 0}

    def fn(q, i, s):
        counter["count"] += 1
        return 2

    p = LLMHopPlanner(fn)
    p.plan(query="same", intent_name="contradiction", seed_titles=[])
    p.plan(query="same", intent_name="contradiction", seed_titles=[])
    assert counter["count"] == 1  # cached


def test_llm_planner_lru_cache_keyed_by_query_and_intent():
    counter = {"count": 0}

    def fn(q, i, s):
        counter["count"] += 1
        return 2

    p = LLMHopPlanner(fn)
    p.plan(query="same", intent_name="contradiction", seed_titles=[])
    p.plan(query="same", intent_name="impact_analysis", seed_titles=[])
    p.plan(query="other", intent_name="contradiction", seed_titles=[])
    assert counter["count"] == 3  # all distinct cache keys


# ===========================================================================
# Factory
# ===========================================================================


def test_factory_fixed_default():
    reset_planner_cache()
    p = get_hop_planner("fixed")
    assert p.name == "fixed"


def test_factory_llm_without_fn_raises():
    reset_planner_cache()
    with pytest.raises(ValueError, match="llm_fn"):
        get_hop_planner("llm")


def test_factory_llm_with_fn():
    reset_planner_cache()
    p = get_hop_planner("llm", llm_fn=lambda q, i, s: 1)
    assert p.name == "llm"


def test_factory_unknown_falls_back_to_fixed():
    reset_planner_cache()
    p = get_hop_planner("wat_is_this")
    assert p.name == "fixed"


def test_factory_caches_fixed():
    reset_planner_cache()
    a = get_hop_planner("fixed", fixed_max_hops=2)
    b = get_hop_planner("fixed", fixed_max_hops=2)
    assert a is b


def test_factory_signal_returns_placeholder():
    reset_planner_cache()
    p = get_hop_planner("signal")
    d = p.plan(query="x", intent_name="contradiction", seed_titles=[])
    assert d.reason == "signal_placeholder"


def test_factory_iterative_returns_placeholder():
    reset_planner_cache()
    p = get_hop_planner("iterative")
    d = p.plan(query="x", intent_name="contradiction", seed_titles=[])
    assert d.reason == "iterative_not_yet_implemented"


# ===========================================================================
# Integration with kg_search_hybrid
# ===========================================================================


class _CapturingSeedProvider:
    def __init__(self, seeds):
        self._seeds = seeds

    def seed(self, *, board_id, query, node_types, top_k):
        return list(self._seeds)


class _CapturingExpander:
    """Captures the max_hops the hybrid pipeline passes in."""

    def __init__(self, neighbors=None):
        self._neighbors = neighbors or []
        self.max_hops_seen: list[int] = []
        self.calls = 0

    def expand(self, *, board_id, seed_ids, edges, max_hops):
        self.calls += 1
        self.max_hops_seen.append(max_hops)
        return list(self._neighbors)


def _seed(node_id="d1", sim=0.9, title="t"):
    return VectorSeed(node_id=node_id, node_type="Decision", title=title, similarity=sim)


def test_kg_search_default_uses_intent_max_hops():
    seeds = [_seed("d1")]
    expander = _CapturingExpander()
    result = kg_search_hybrid(
        board_id="b1",
        query="q",
        intent=INTENT_CONTRADICTION_CHECK,
        vector_provider=_CapturingSeedProvider(seeds),
        graph_expander=expander,
    )
    # Default (hop_strategy="fixed") preserves intent.max_hops.
    assert result.hops_stopped_reason == "fixed"
    assert expander.max_hops_seen[0] == result.hops_used
    # backward-compat: hops_used is set to intent.max_hops.
    from okto_pulse.core.kg.hybrid_search.intents import INTENT_CATALOG
    assert result.hops_used == INTENT_CATALOG[INTENT_CONTRADICTION_CHECK].max_hops


def test_kg_search_llm_overrides_intent():
    seeds = [_seed("d1")]
    expander = _CapturingExpander()
    result = kg_search_hybrid(
        board_id="b1",
        query="q",
        intent=INTENT_CONTRADICTION_CHECK,
        vector_provider=_CapturingSeedProvider(seeds),
        graph_expander=expander,
        hop_strategy="llm",
        hop_llm_fn=lambda q, i, s: 3,
    )
    assert result.hops_stopped_reason == "llm"
    assert result.hops_used == 3
    assert expander.max_hops_seen == [3]


def test_kg_search_llm_ceiling_enforced():
    seeds = [_seed("d1")]
    expander = _CapturingExpander()
    result = kg_search_hybrid(
        board_id="b1",
        query="q",
        intent=INTENT_CONTRADICTION_CHECK,
        vector_provider=_CapturingSeedProvider(seeds),
        graph_expander=expander,
        hop_strategy="llm",
        hop_llm_fn=lambda q, i, s: 10,  # absurd value
    )
    assert result.hops_used == 3  # clamped
    assert expander.max_hops_seen == [3]


def test_kg_search_llm_fallback_on_exception():
    seeds = [_seed("d1")]
    expander = _CapturingExpander()

    def exploding(q, i, s):
        raise RuntimeError("LLM down")

    result = kg_search_hybrid(
        board_id="b1",
        query="q",
        intent=INTENT_CONTRADICTION_CHECK,
        vector_provider=_CapturingSeedProvider(seeds),
        graph_expander=expander,
        hop_strategy="llm",
        hop_llm_fn=exploding,
    )
    # Exception is caught inside LLMHopPlanner → reason is
    # llm_error_fallback; hops_used falls back to intent.max_hops.
    assert "llm_error_fallback" in result.hops_stopped_reason
    from okto_pulse.core.kg.hybrid_search.intents import INTENT_CATALOG
    assert result.hops_used == INTENT_CATALOG[INTENT_CONTRADICTION_CHECK].max_hops


def test_kg_search_planner_invoked_once_across_variants():
    """Even though the spec references fusion/decompose, the current
    kg_search_hybrid signature doesn't do rewrite itself — that lives
    in tier_power.execute_natural_query. At this level we verify that
    for a single call, the planner is called exactly once. Multiple
    variants sharing the decision is the higher-level property."""
    seeds = [_seed("d1")]
    expander = _CapturingExpander()
    counter = {"count": 0}

    def fn(q, i, s):
        counter["count"] += 1
        return 2

    kg_search_hybrid(
        board_id="b1",
        query="q",
        intent=INTENT_CONTRADICTION_CHECK,
        vector_provider=_CapturingSeedProvider(seeds),
        graph_expander=expander,
        hop_strategy="llm",
        hop_llm_fn=fn,
    )
    assert counter["count"] == 1
    assert expander.calls == 1  # single expand call


def test_reset_planner_cache():
    reset_planner_cache()
    a = get_hop_planner("fixed", fixed_max_hops=2)
    reset_planner_cache()
    b = get_hop_planner("fixed", fixed_max_hops=2)
    assert a is not b
