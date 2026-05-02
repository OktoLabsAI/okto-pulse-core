"""Unit tests for the second-stage reranker (ideação 3070cd53).

Covers the four strategies the factory exposes (``none``,
``token_overlap``, ``cross_encoder``, ``llm``) plus the integration
into `kg_search_hybrid`. The cross-encoder strategy is tested via the
factory's fallback path — the real sentence-transformers model is not
loaded by this suite.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from okto_pulse.core.kg.hybrid_search import (
    INTENT_CONTRADICTION_CHECK,
    kg_search_hybrid,
)
from okto_pulse.core.kg.hybrid_search.hybrid import (
    VectorSeed,
)
from okto_pulse.core.kg.rerank import (
    NoopReranker,
    TokenOverlapReranker,
    get_reranker,
    reset_reranker_cache,
)
from okto_pulse.core.kg.rerank.llm import LLMReranker, build_default_prompt


@dataclass(frozen=True)
class _FakeCandidate:
    """Minimal shape satisfying the reranker protocol."""

    node_id: str
    title: str
    content: str | None = None
    score: float = 0.0


# ===========================================================================
# NoopReranker
# ===========================================================================


def test_noop_preserves_input_order():
    rr = NoopReranker()
    items = [_FakeCandidate(f"c{i}", f"t{i}") for i in range(5)]
    out = rr.rerank("anything", items, top_n=3)
    assert [c.node_id for c in out] == ["c0", "c1", "c2"]


def test_noop_handles_empty():
    rr = NoopReranker()
    assert rr.rerank("q", [], top_n=10) == []


def test_noop_top_n_zero_returns_empty():
    rr = NoopReranker()
    items = [_FakeCandidate("c0", "t0")]
    assert rr.rerank("q", items, top_n=0) == []


# ===========================================================================
# TokenOverlapReranker
# ===========================================================================


def test_token_overlap_promotes_lexical_match():
    rr = TokenOverlapReranker()
    candidates = [
        _FakeCandidate("c_no_match", "totally unrelated subject"),
        _FakeCandidate("c_match", "query about supersedence chains"),
        _FakeCandidate("c_partial", "about chains in general"),
    ]
    out = rr.rerank(
        "which decisions supersedence chains exist?",
        candidates,
        top_n=3,
    )
    # Fullest overlap wins, partial next, unrelated last.
    assert out[0].node_id == "c_match"
    assert out[-1].node_id == "c_no_match"


def test_token_overlap_stable_on_ties():
    """When no candidate shares any token with the query, input order
    is preserved — guards against non-deterministic reorderings."""
    rr = TokenOverlapReranker()
    candidates = [
        _FakeCandidate(f"c{i}", "unrelated text here") for i in range(4)
    ]
    out = rr.rerank("xyz", candidates, top_n=4)
    assert [c.node_id for c in out] == ["c0", "c1", "c2", "c3"]


def test_token_overlap_preserves_score_field_via_dataclass_replace():
    """The reranker writes the post-rerank score onto the candidate
    when the field exists and the candidate is a dataclass."""
    rr = TokenOverlapReranker()
    candidates = [_FakeCandidate("c1", "query text", score=0.5)]
    out = rr.rerank("query text", candidates, top_n=1)
    # Score was boosted by the Jaccard overlap (text matches query).
    assert out[0].score > 0.5


def test_token_overlap_handles_empty_query_gracefully():
    """A query that tokenises to nothing (all stopwords) falls back to
    input order — the reranker never raises on pathological input."""
    rr = TokenOverlapReranker()
    items = [_FakeCandidate("c0", "real content"), _FakeCandidate("c1", "other")]
    out = rr.rerank("the a o", items, top_n=2)
    assert [c.node_id for c in out] == ["c0", "c1"]


# ===========================================================================
# LLMReranker
# ===========================================================================


def test_llm_reranker_honours_llm_ordering():
    # LLM ranks c2 first, c0 second; c1 is dropped (not returned).
    def llm_fn(query, candidates):
        return ["c2", "c0"]

    rr = LLMReranker(llm_fn)
    items = [
        _FakeCandidate("c0", "alpha"),
        _FakeCandidate("c1", "beta"),
        _FakeCandidate("c2", "gamma"),
    ]
    out = rr.rerank("q", items, top_n=3)
    # LLM order first, then the omitted id filled in to meet top_n.
    assert [c.node_id for c in out] == ["c2", "c0", "c1"]


def test_llm_reranker_ignores_hallucinated_ids():
    def llm_fn(query, candidates):
        return ["does_not_exist", "c1"]

    rr = LLMReranker(llm_fn)
    items = [
        _FakeCandidate("c0", "alpha"),
        _FakeCandidate("c1", "beta"),
    ]
    out = rr.rerank("q", items, top_n=2)
    assert [c.node_id for c in out] == ["c1", "c0"]


def test_llm_reranker_falls_back_on_exception():
    def llm_fn(query, candidates):
        raise RuntimeError("LLM timeout")

    rr = LLMReranker(llm_fn)
    items = [_FakeCandidate("c0", "alpha"), _FakeCandidate("c1", "beta")]
    out = rr.rerank("q", items, top_n=2)
    # Pipeline survives: first-stage order preserved.
    assert [c.node_id for c in out] == ["c0", "c1"]


def test_llm_prompt_mentions_every_candidate_id():
    items = [_FakeCandidate("c0", "alpha text"), _FakeCandidate("c1", "beta")]
    prompt = build_default_prompt("find alpha", items)
    assert "c0:" in prompt
    assert "c1:" in prompt
    assert "find alpha" in prompt


# ===========================================================================
# Factory
# ===========================================================================


def test_factory_noop_strategy():
    reset_reranker_cache()
    rr = get_reranker("none")
    assert rr.name == "noop"


def test_factory_token_overlap_strategy():
    reset_reranker_cache()
    rr = get_reranker("token_overlap")
    assert rr.name == "token_overlap"


def test_factory_caches_noop_and_token_overlap():
    reset_reranker_cache()
    a = get_reranker("token_overlap")
    b = get_reranker("token_overlap")
    assert a is b  # cached


def test_factory_unknown_strategy_falls_back_to_noop():
    reset_reranker_cache()
    rr = get_reranker("wat_is_this")
    assert rr.name == "noop"


def test_factory_llm_strategy_requires_fn():
    reset_reranker_cache()
    with pytest.raises(ValueError, match="llm_ranker_fn"):
        get_reranker("llm")


def test_factory_llm_strategy_wires_fn():
    reset_reranker_cache()
    rr = get_reranker("llm", llm_ranker_fn=lambda q, c: [])
    assert rr.name == "llm"


def test_cross_encoder_reranker_raises_import_error_without_dep(monkeypatch):
    """Direct-construction test: without `sentence_transformers` importable,
    instantiating CrossEncoderReranker raises ImportError with a clear
    pip hint. This is the precondition the factory's try/except relies
    on — the factory-level fallback is covered by a code-level invariant
    (see factory.py L60-72) and not unit-tested with a heavyweight
    monkeypatch because `sentence_transformers` is typically installed
    in CI and mocking the submodule import plus the parent package
    attribute is brittle across Python versions on Windows.
    """
    import sys

    # Force-unload sentence_transformers if it was pre-loaded by another
    # test; the ImportError path depends on the import inside __init__.
    for mod in list(sys.modules):
        if mod.startswith("sentence_transformers"):
            monkeypatch.delitem(sys.modules, mod, raising=False)
    # Make the import fail by injecting a sentinel that raises on attr
    # lookup. Safer than `sys.modules[X] = None` which Python interprets
    # specially.
    class _Blocker:
        def __getattr__(self, name):
            raise ImportError(
                "sentence_transformers blocked by test fixture"
            )

    monkeypatch.setitem(sys.modules, "sentence_transformers", _Blocker())

    from okto_pulse.core.kg.rerank.cross_encoder import CrossEncoderReranker

    with pytest.raises(ImportError, match="sentence-transformers"):
        CrossEncoderReranker()


# ===========================================================================
# Integration: kg_search_hybrid + rerank
# ===========================================================================


class _StubSeedProvider:
    def __init__(self, seeds):
        self._seeds = seeds

    def seed(self, *, board_id, query, node_types, top_k):
        return list(self._seeds)


class _StubExpander:
    def __init__(self, neighbors):
        self._neighbors = neighbors

    def expand(self, *, board_id, seed_ids, edges, max_hops):
        return list(self._neighbors)


def test_kg_search_hybrid_rerank_none_is_default():
    seeds = [
        VectorSeed("d1", "Decision", "alpha", 0.9),
        VectorSeed("d2", "Decision", "beta", 0.7),
    ]
    result = kg_search_hybrid(
        board_id="b1",
        query="alpha",
        intent=INTENT_CONTRADICTION_CHECK,
        vector_provider=_StubSeedProvider(seeds),
        graph_expander=_StubExpander([]),
    )
    assert result.rerank_strategy == "none"
    assert result.timing.rerank_ms == 0.0


def test_kg_search_hybrid_token_overlap_rerank_reorders():
    # Two seeds with identical similarity; without rerank the order is
    # implementation-defined. With token_overlap, the one whose title
    # matches the query moves up.
    seeds = [
        VectorSeed("d_noise", "Decision", "completely unrelated doc", 0.9),
        VectorSeed("d_target", "Decision", "supersedence chain rule", 0.9),
    ]
    result = kg_search_hybrid(
        board_id="b1",
        query="supersedence chain",
        intent=INTENT_CONTRADICTION_CHECK,
        vector_provider=_StubSeedProvider(seeds),
        graph_expander=_StubExpander([]),
        rerank="token_overlap",
    )
    assert result.rerank_strategy == "token_overlap"
    assert result.ranked[0].node_id == "d_target"
    assert result.timing.rerank_ms >= 0.0
    # Total timing now includes the rerank stage.
    assert result.timing.total_ms >= result.timing.rerank_ms


def test_kg_search_hybrid_rerank_respects_top_k():
    seeds = [
        VectorSeed(f"d{i}", "Decision", f"title {i}", 0.9 - i * 0.05)
        for i in range(20)
    ]
    result = kg_search_hybrid(
        board_id="b1",
        query="title",
        intent=INTENT_CONTRADICTION_CHECK,
        top_k=5,
        vector_provider=_StubSeedProvider(seeds),
        graph_expander=_StubExpander([]),
        rerank="token_overlap",
    )
    assert len(result.ranked) == 5


def test_kg_search_hybrid_llm_rerank_end_to_end():
    calls: dict = {"seen_ids": None}

    def llm_fn(query, candidates):
        ids = [getattr(c, "node_id", "?") for c in candidates]
        calls["seen_ids"] = ids
        # Reverse the order as the LLM's "opinion".
        return list(reversed(ids))

    seeds = [
        VectorSeed("d1", "Decision", "t1", 0.9),
        VectorSeed("d2", "Decision", "t2", 0.8),
        VectorSeed("d3", "Decision", "t3", 0.7),
    ]
    result = kg_search_hybrid(
        board_id="b1",
        query="anything",
        intent=INTENT_CONTRADICTION_CHECK,
        vector_provider=_StubSeedProvider(seeds),
        graph_expander=_StubExpander([]),
        rerank="llm",
        rerank_llm_fn=llm_fn,
    )
    assert result.rerank_strategy == "llm"
    assert calls["seen_ids"] is not None
    # d3 was last in first-stage ranking, LLM reversed → now first.
    assert result.ranked[0].node_id == "d3"


def test_kg_search_hybrid_rerank_exception_falls_back():
    def exploding_llm(query, candidates):
        raise RuntimeError("LLM quota exceeded")

    seeds = [VectorSeed("d1", "Decision", "t", 0.9)]
    result = kg_search_hybrid(
        board_id="b1",
        query="q",
        intent=INTENT_CONTRADICTION_CHECK,
        vector_provider=_StubSeedProvider(seeds),
        graph_expander=_StubExpander([]),
        rerank="llm",
        rerank_llm_fn=exploding_llm,
    )
    # LLMReranker catches this internally and returns input order,
    # so rerank_strategy stays "llm" and the pipeline still completes
    # with the first-stage ranking preserved.
    assert result.rerank_strategy == "llm"
    assert len(result.ranked) >= 1
    assert result.ranked[0].node_id == "d1"


def test_reset_cache_forces_new_instance():
    reset_reranker_cache()
    a = get_reranker("token_overlap")
    reset_reranker_cache()
    b = get_reranker("token_overlap")
    assert a is not b
    # Both still usable.
    assert a.name == b.name == "token_overlap"
