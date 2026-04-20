"""Unit tests for the query rewrite module (ideação 2cf21a31).

Covers the four strategies (noop/hyde/decompose/fusion), the factory
contract (caching, ValueError on missing fn, graceful fallback on
unknown strategy), RRF merge semantics, and the LRU cache behaviour.
Integration with ``tier_power.execute_natural_query`` is exercised via
monkeypatching the embedder + store so the test doesn't need a live
Kùzu handle.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from okto_pulse.core.kg.query_rewrite import (
    DecomposeRewriter,
    FusionRewriter,
    HyDERewriter,
    NoopRewriter,
    RewriteResult,
    get_rewriter,
    merge_rrf,
    reset_rewriter_cache,
)


# ===========================================================================
# NoopRewriter
# ===========================================================================


def test_noop_rewriter_passthrough():
    rr = NoopRewriter()
    result = rr.rewrite("any question")
    assert isinstance(result, RewriteResult)
    assert result.strategy == "none"
    assert result.original_query == "any question"
    assert result.rewritten_queries == ("any question",)
    assert result.hyde_passage is None


# ===========================================================================
# HyDERewriter
# ===========================================================================


def test_hyde_populates_passage():
    calls: list[str] = []

    def llm_fn(q: str) -> str:
        calls.append(q)
        return "hypothetical passage text"

    rr = HyDERewriter(llm_fn)
    result = rr.rewrite("minha query")
    assert result.strategy == "hyde"
    assert result.hyde_passage == "hypothetical passage text"
    assert result.rewritten_queries == ("minha query",)
    assert calls == ["minha query"]


def test_hyde_degrades_on_empty_passage():
    rr = HyDERewriter(lambda q: "   ")
    result = rr.rewrite("q")
    # Empty passage is treated as "rewriter can't help" — passthrough.
    assert result.strategy == "none"
    assert result.hyde_passage is None


def test_hyde_degrades_on_exception():
    def exploding(q: str) -> str:
        raise RuntimeError("LLM timeout")

    rr = HyDERewriter(exploding)
    result = rr.rewrite("q")
    assert result.strategy == "none"
    assert result.hyde_passage is None


# ===========================================================================
# DecomposeRewriter
# ===========================================================================


def test_decompose_with_valid_sub_queries():
    rr = DecomposeRewriter(lambda q: ["sub1", "sub2", "sub3"])
    result = rr.rewrite("query composta")
    assert result.strategy == "decompose"
    assert result.rewritten_queries == ("sub1", "sub2", "sub3")


def test_decompose_empty_list_degrades_silently():
    rr = DecomposeRewriter(lambda q: [])
    result = rr.rewrite("query composta")
    assert result.strategy == "none"
    assert result.rewritten_queries == ("query composta",)


def test_decompose_single_item_degrades():
    # 1 sub-query is the same as not decomposing.
    rr = DecomposeRewriter(lambda q: ["única"])
    result = rr.rewrite("query composta")
    assert result.strategy == "none"


def test_decompose_filters_empty_strings():
    rr = DecomposeRewriter(lambda q: ["sub1", "", "   ", "sub2"])
    result = rr.rewrite("q")
    assert result.strategy == "decompose"
    assert result.rewritten_queries == ("sub1", "sub2")


def test_decompose_exception_degrades():
    rr = DecomposeRewriter(lambda q: (_ for _ in ()).throw(RuntimeError("x")))
    result = rr.rewrite("q")
    assert result.strategy == "none"


# ===========================================================================
# FusionRewriter
# ===========================================================================


def test_fusion_returns_k_paraphrases():
    rr = FusionRewriter(
        lambda q, k: ["para1", "para2", "para3"],
        fusion_paraphrases=3,
    )
    result = rr.rewrite("algum tópico")
    assert result.strategy == "fusion"
    assert len(result.rewritten_queries) == 3
    assert result.rewritten_queries == ("para1", "para2", "para3")


def test_fusion_truncates_to_k():
    rr = FusionRewriter(
        lambda q, k: ["p1", "p2", "p3", "p4", "p5"],
        fusion_paraphrases=2,
    )
    result = rr.rewrite("q")
    assert len(result.rewritten_queries) == 2


def test_fusion_empty_response_degrades():
    rr = FusionRewriter(lambda q, k: [])
    result = rr.rewrite("q")
    assert result.strategy == "none"


def test_fusion_exception_degrades():
    def exploding(q, k):
        raise RuntimeError("timeout")

    rr = FusionRewriter(exploding)
    result = rr.rewrite("q")
    assert result.strategy == "none"


# ===========================================================================
# Factory
# ===========================================================================


def test_factory_none_returns_noop():
    reset_rewriter_cache()
    rr = get_rewriter("none")
    assert rr.name == "none"


def test_factory_unknown_strategy_falls_back_to_noop():
    reset_rewriter_cache()
    rr = get_rewriter("wat_is_this")
    assert rr.name == "none"


def test_factory_hyde_without_fn_raises():
    reset_rewriter_cache()
    with pytest.raises(ValueError, match="llm_fn"):
        get_rewriter("hyde")


def test_factory_decompose_without_fn_raises():
    reset_rewriter_cache()
    with pytest.raises(ValueError, match="llm_fn"):
        get_rewriter("decompose")


def test_factory_fusion_without_fn_raises():
    reset_rewriter_cache()
    with pytest.raises(ValueError, match="llm_fn"):
        get_rewriter("fusion")


def test_factory_hyde_with_fn_ok():
    reset_rewriter_cache()
    rr = get_rewriter("hyde", llm_fn=lambda q: "passage")
    assert rr.name == "hyde"


def test_factory_caches_noop():
    reset_rewriter_cache()
    a = get_rewriter("none")
    b = get_rewriter("none")
    assert a is b


def test_factory_fusion_respects_paraphrases():
    reset_rewriter_cache()
    rr = get_rewriter(
        "fusion",
        llm_fn=lambda q, k: ["p" + str(i) for i in range(k)],
        fusion_paraphrases=5,
    )
    result = rr.rewrite("q")
    assert len(result.rewritten_queries) == 5


# ===========================================================================
# LRU cache
# ===========================================================================


def test_hyde_lru_cache_reuses_result():
    reset_rewriter_cache()
    counter = {"count": 0}

    def llm_fn(q: str) -> str:
        counter["count"] += 1
        return f"passage for {q}"

    rr = get_rewriter("hyde", llm_fn=llm_fn)
    r1 = rr.rewrite("mesma query")
    r2 = rr.rewrite("mesma query")
    assert counter["count"] == 1  # second call served from cache
    assert r1 is r2  # functools.lru_cache returns the same object


def test_hyde_lru_cache_keyed_by_query():
    reset_rewriter_cache()
    counter = {"count": 0}

    def llm_fn(q: str) -> str:
        counter["count"] += 1
        return "p"

    rr = get_rewriter("hyde", llm_fn=llm_fn)
    rr.rewrite("query A")
    rr.rewrite("query B")
    rr.rewrite("query A")  # cached
    assert counter["count"] == 2


# ===========================================================================
# RRF merge
# ===========================================================================


@dataclass(frozen=True)
class _Node:
    node_id: str


def test_merge_rrf_basic_ordering():
    ra = [_Node("nA"), _Node("nB"), _Node("nC")]
    rb = [_Node("nB"), _Node("nA"), _Node("nD")]
    merged = merge_rrf([ra, rb], k=60)
    ids = [n.node_id for n in merged]
    # nA and nB tied at top — input-order tiebreak keeps nA first.
    assert ids[:2] == ["nA", "nB"]
    # Exclusive singletons fall to the tail.
    assert set(ids[2:]) == {"nC", "nD"}


def test_merge_rrf_ignores_items_without_node_id():
    ra = [_Node("nA"), object()]
    merged = merge_rrf([ra], k=60)
    assert len(merged) == 1
    assert merged[0].node_id == "nA"


def test_merge_rrf_accepts_dicts():
    ra = [{"node_id": "nA", "title": "t1"}, {"node_id": "nB", "title": "t2"}]
    rb = [{"node_id": "nB", "title": "t2"}, {"node_id": "nA", "title": "t1"}]
    merged = merge_rrf([ra, rb], k=60)
    ids = [item["node_id"] for item in merged]
    assert set(ids) == {"nA", "nB"}


def test_merge_rrf_empty_rankings_returns_empty():
    assert merge_rrf([], k=60) == []
    assert merge_rrf([[]], k=60) == []


# ===========================================================================
# Integration with execute_natural_query
# ===========================================================================


class _StubEmbedder:
    dim = 8

    def encode(self, text: str):
        # Deterministic pseudo-embedding — irrelevant for the stubs
        # below, which route by query string.
        return [0.1] * self.dim

    def encode_batch(self, texts):
        return [self.encode(t) for t in texts]


class _StubStore:
    """Routes vector_search by the query vector — in these tests the
    stub is indirected through a per-query dict set on the instance."""

    def __init__(self, by_query: dict[str, list[dict]]):
        self._by_query = by_query
        self._last_query = None

    def set_next_query(self, q: str):
        self._last_query = q

    def vector_search(self, *, board_id, node_type, query_vec, top_k, min_similarity):
        hits = self._by_query.get(self._last_query, [])
        # Return only those tagged with this node_type.
        return [h for h in hits if h.get("node_type") == node_type]

    def find_by_topic(self, board_id, node_type, q, filters):
        return []


@pytest.fixture
def stub_registry(monkeypatch):
    """Patch the KG registry so execute_natural_query uses our stubs.

    The stub store dispatches vector_search by a _last_query attribute
    set from the monkeypatched embedder; the embedder records which
    query was embedded so the store can answer accordingly.
    """
    from okto_pulse.core.kg.interfaces import registry as registry_mod
    from okto_pulse.core.kg import tier_power

    by_query: dict[str, list[dict]] = {}
    store = _StubStore(by_query)

    class _TrackingEmbedder(_StubEmbedder):
        def encode(self, text: str):
            store.set_next_query(text)
            return super().encode(text)

    embedder = _TrackingEmbedder()

    class _Registry:
        embedding_provider = embedder
        graph_store = store

    reg = _Registry()
    monkeypatch.setattr(
        registry_mod, "get_kg_registry", lambda: reg, raising=True,
    )
    # Also patch the _batch_lookup_created_at to return empty (no
    # temporal filter in these tests).
    monkeypatch.setattr(
        tier_power, "_batch_lookup_created_at",
        lambda board_id, node_ids: {},
        raising=True,
    )
    return by_query


def test_execute_natural_query_default_backward_compat(stub_registry):
    from okto_pulse.core.kg.tier_power import execute_natural_query

    stub_registry["algum tópico"] = [
        {"node_id": "n1", "node_type": "Decision", "title": "t1", "similarity": 0.9},
        {"node_id": "n2", "node_type": "Decision", "title": "t2", "similarity": 0.7},
    ]
    resp = execute_natural_query(board_id="b1", nl_query="algum tópico")
    assert resp["rewrite_strategy"] == "none"
    assert resp["rewrite_variants_count"] == 1
    assert len(resp["nodes"]) == 2
    assert resp["nodes"][0]["node_id"] == "n1"


def test_execute_natural_query_decompose_dedup(stub_registry):
    from okto_pulse.core.kg.tier_power import execute_natural_query

    reset_rewriter_cache()
    stub_registry["sub_a"] = [
        {"node_id": "n1", "node_type": "Decision", "title": "n1", "similarity": 0.9},
        {"node_id": "n2", "node_type": "Decision", "title": "n2", "similarity": 0.8},
    ]
    stub_registry["sub_b"] = [
        {"node_id": "n2", "node_type": "Decision", "title": "n2", "similarity": 0.7},
        {"node_id": "n3", "node_type": "Decision", "title": "n3", "similarity": 0.6},
    ]
    stub_registry["sub_c"] = [
        {"node_id": "n1", "node_type": "Decision", "title": "n1", "similarity": 0.5},
        {"node_id": "n3", "node_type": "Decision", "title": "n3", "similarity": 0.4},
    ]

    resp = execute_natural_query(
        board_id="b1",
        nl_query="query composta",
        rewrite="decompose",
        rewrite_llm_fn=lambda q: ["sub_a", "sub_b", "sub_c"],
    )
    assert resp["rewrite_strategy"] == "decompose"
    assert resp["rewrite_variants_count"] == 3
    ids = {n["node_id"] for n in resp["nodes"]}
    assert ids == {"n1", "n2", "n3"}


def test_execute_natural_query_fusion_rrf(stub_registry):
    from okto_pulse.core.kg.tier_power import execute_natural_query

    reset_rewriter_cache()
    stub_registry["para1"] = [
        {"node_id": "n1", "node_type": "Decision", "title": "n1", "similarity": 0.9},
        {"node_id": "n2", "node_type": "Decision", "title": "n2", "similarity": 0.8},
        {"node_id": "n3", "node_type": "Decision", "title": "n3", "similarity": 0.7},
    ]
    stub_registry["para2"] = [
        {"node_id": "n3", "node_type": "Decision", "title": "n3", "similarity": 0.9},
        {"node_id": "n2", "node_type": "Decision", "title": "n2", "similarity": 0.8},
        {"node_id": "n1", "node_type": "Decision", "title": "n1", "similarity": 0.7},
    ]
    stub_registry["para3"] = [
        {"node_id": "n2", "node_type": "Decision", "title": "n2", "similarity": 0.9},
        {"node_id": "n1", "node_type": "Decision", "title": "n1", "similarity": 0.8},
        {"node_id": "n3", "node_type": "Decision", "title": "n3", "similarity": 0.7},
    ]

    resp = execute_natural_query(
        board_id="b1",
        nl_query="algo",
        rewrite="fusion",
        rewrite_llm_fn=lambda q, k: ["para1", "para2", "para3"],
        fusion_paraphrases=3,
    )
    assert resp["rewrite_strategy"] == "fusion"
    assert resp["rewrite_variants_count"] == 3
    # n2 appears at ranks 2,2,1 → best RRF score.
    assert resp["nodes"][0]["node_id"] == "n2"


def test_execute_natural_query_fallback_on_llm_exception(stub_registry):
    from okto_pulse.core.kg.tier_power import execute_natural_query

    reset_rewriter_cache()
    stub_registry["q"] = [
        {"node_id": "n1", "node_type": "Decision", "title": "n1", "similarity": 0.9},
    ]

    def exploding(query):
        raise RuntimeError("LLM timeout")

    resp = execute_natural_query(
        board_id="b1",
        nl_query="q",
        rewrite="decompose",
        rewrite_llm_fn=exploding,
    )
    # Rewriter degraded gracefully → strategy="none", retrieve proceeded.
    assert resp["rewrite_strategy"] == "none"
    assert resp["rewrite_variants_count"] == 1
    assert resp["nodes"][0]["node_id"] == "n1"
