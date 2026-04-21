"""`kg_search_hybrid` — vector seed + graph expand + ranked blend.

Orchestrates the three stages of the hybrid pipeline:

    1. **Vector seed** — HNSW k-NN over every `vector_seed_types` of the
       intent. Per-type results merged and ordered by similarity.
    2. **Graph expand** — from each seed, walk up to `max_hops` along the
       intent's `expand_edges`. The implementation is parametrised via an
       injected `GraphExpander` so tests don't need a live Kùzu handle.
    3. **Hybrid ranking** — linear blend of vector_sim + graph_proximity_inv
       + edge_confidence + recency_decay, weighted per-intent.

A soft p95 budget is enforced via `deadline_ms`. When the deadline is
exceeded mid-flight, the partial results are returned with `partial=True`
so the SSE layer can surface the SLA breach (BR `Hybrid Search p95
Budget`).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Protocol

from .intents import (
    INTENT_CATALOG,
    IntentNotFoundError,
    SearchIntent,
    resolve_intent,
)

logger = logging.getLogger("okto_pulse.kg.hybrid_search")


class HybridSearchError(Exception):
    """Base class for hybrid-search failures (not found / invalid / timeout)."""


# ---------------------------------------------------------------------------
# Result shapes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class VectorSeed:
    node_id: str
    node_type: str
    title: str
    similarity: float
    created_at: datetime | None = None


@dataclass(frozen=True)
class GraphNeighbor:
    """One node returned by the graph expand, tagged with the path length."""

    node_id: str
    node_type: str
    title: str
    edge_type: str
    edge_confidence: float
    hop_distance: int  # number of hops from the seed


@dataclass(frozen=True)
class HybridSearchTiming:
    vector_seed_ms: float
    graph_expand_ms: float
    ranking_ms: float
    # Ideação 3070cd53: optional second-stage rerank. 0.0 when
    # rerank="none" (default) — preserves backward-compat totals.
    rerank_ms: float = 0.0

    @property
    def total_ms(self) -> float:
        return (
            self.vector_seed_ms
            + self.graph_expand_ms
            + self.ranking_ms
            + self.rerank_ms
        )


@dataclass(frozen=True)
class RankedNode:
    node_id: str
    node_type: str
    title: str
    score: float
    vector_sim: float
    graph_proximity_inv: float
    edge_confidence: float
    recency_decay: float


@dataclass(frozen=True)
class HybridSearchResult:
    intent: str
    query: str
    board_id: str
    seeds: tuple[VectorSeed, ...]
    neighbors: tuple[GraphNeighbor, ...]
    ranked: tuple[RankedNode, ...]
    timing: HybridSearchTiming
    partial: bool = False
    sla_budget_ms: float = 100.0
    version: str = "v1"
    # Ideação 3070cd53: which rerank strategy was actually applied
    # ("none" when the stage was skipped). Surfaced so the UI / audit
    # can tell first-stage and second-stage results apart.
    rerank_strategy: str = "none"
    # Ideação 1fb13b51: adaptive hop count observability. ``hops_used``
    # is the depth effectively passed to graph_expander.expand;
    # ``hops_stopped_reason`` is the planner's reason string.
    hops_used: int = 0
    hops_stopped_reason: str = "fixed"
    emitted_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ---------------------------------------------------------------------------
# Protocols for the adapters (kept thin — tests plug in dummies)
# ---------------------------------------------------------------------------


class VectorSeedProvider(Protocol):
    """Returns the top-k VectorSeed entries for an intent/query combo."""

    def seed(
        self,
        *,
        board_id: str,
        query: str,
        node_types: tuple[str, ...],
        top_k: int,
    ) -> list[VectorSeed]:
        ...


class GraphExpander(Protocol):
    """Runs the per-intent Kùzu path query."""

    def expand(
        self,
        *,
        board_id: str,
        seed_ids: tuple[str, ...],
        edges: tuple[str, ...],
        max_hops: int,
    ) -> list[GraphNeighbor]:
        ...


# ---------------------------------------------------------------------------
# Ranking helpers
# ---------------------------------------------------------------------------


def _recency_decay(created_at: datetime | None, *, now: datetime | None = None) -> float:
    """30-day half-life. Returns 1.0 for fresh nodes, 0.0 for very old ones."""
    if created_at is None:
        return 0.5
    now = now or datetime.now(timezone.utc)
    age_days = max(0.0, (now - created_at).total_seconds() / 86400.0)
    return 0.5 ** (age_days / 30.0)


def _graph_proximity_inv(hop: int) -> float:
    """Closer neighbours rank higher. 1 / (hop + 1) gives 1.0 at hop=0
    and 0.25 at hop=3 — matches the spec description."""
    return 1.0 / (hop + 1)


def _rank(
    intent: SearchIntent,
    seeds: list[VectorSeed],
    neighbors: list[GraphNeighbor],
    *,
    now: datetime | None = None,
) -> list[RankedNode]:
    """Blend per-node scores using the intent's weights."""
    w = intent.weights
    seed_sim: dict[str, float] = {s.node_id: s.similarity for s in seeds}
    seed_recency: dict[str, float] = {
        s.node_id: _recency_decay(s.created_at, now=now) for s in seeds
    }

    # Collect neighbor entries — if a node is both seed AND neighbor we merge.
    records: dict[str, RankedNode] = {}
    for s in seeds:
        records[s.node_id] = RankedNode(
            node_id=s.node_id,
            node_type=s.node_type,
            title=s.title,
            score=0.0,  # placeholder, filled below
            vector_sim=s.similarity,
            graph_proximity_inv=1.0,  # seed is hop=0
            edge_confidence=0.0,
            recency_decay=seed_recency[s.node_id],
        )
    for n in neighbors:
        prev = records.get(n.node_id)
        if prev is None:
            records[n.node_id] = RankedNode(
                node_id=n.node_id,
                node_type=n.node_type,
                title=n.title,
                score=0.0,
                vector_sim=seed_sim.get(n.node_id, 0.0),
                graph_proximity_inv=_graph_proximity_inv(n.hop_distance),
                edge_confidence=n.edge_confidence,
                recency_decay=0.5,  # unknown recency → neutral
            )
        else:
            # If node is both seed and neighbor, keep the better of the two
            # graph proximities and max the edge confidences.
            merged = RankedNode(
                node_id=prev.node_id,
                node_type=prev.node_type,
                title=prev.title,
                score=0.0,
                vector_sim=max(prev.vector_sim, seed_sim.get(n.node_id, 0.0)),
                graph_proximity_inv=max(
                    prev.graph_proximity_inv, _graph_proximity_inv(n.hop_distance),
                ),
                edge_confidence=max(prev.edge_confidence, n.edge_confidence),
                recency_decay=prev.recency_decay,
            )
            records[n.node_id] = merged

    ranked: list[RankedNode] = []
    for r in records.values():
        score = (
            r.vector_sim * w.vector_sim
            + r.graph_proximity_inv * w.graph_proximity_inv
            + r.edge_confidence * w.edge_confidence
            + r.recency_decay * w.recency_decay
        )
        ranked.append(RankedNode(
            node_id=r.node_id,
            node_type=r.node_type,
            title=r.title,
            score=score,
            vector_sim=r.vector_sim,
            graph_proximity_inv=r.graph_proximity_inv,
            edge_confidence=r.edge_confidence,
            recency_decay=r.recency_decay,
        ))
    ranked.sort(key=lambda r: r.score, reverse=True)
    return ranked


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def kg_search_hybrid(
    *,
    board_id: str,
    query: str,
    intent: str | None = None,
    top_k: int = 10,
    sla_budget_ms: float = 100.0,
    vector_provider: VectorSeedProvider,
    graph_expander: GraphExpander,
    classifier: Callable[[str], SearchIntent] | None = None,
    now: datetime | None = None,
    rerank: str = "none",
    rerank_pool: int = 50,
    rerank_llm_fn=None,
    hop_strategy: str = "fixed",
    hop_llm_fn=None,
) -> HybridSearchResult:
    """Run the hybrid pipeline and return the ranked result set.

    The explicit `intent` parameter wins over classification when set —
    mirrors the MCP contract where callers may short-circuit the
    regex+LLM classifier with a known intent.

    Ideação 3070cd53 — second-stage rerank (optional):

    - ``rerank``: one of ``"none"`` (default, passthrough),
      ``"token_overlap"`` (zero-dep lexical baseline),
      ``"cross_encoder"`` (sentence-transformers MS MARCO),
      ``"llm"`` (RankGPT-style, requires ``rerank_llm_fn``).
    - ``rerank_pool``: how many top-K first-stage results to hand over
      to the reranker. Larger pool = better precision but proportional
      latency. Default 50 matches common IR practice (rerank top-50 →
      return top-10). The reranker still returns at most ``top_k``.
    - ``rerank_llm_fn``: only used when ``rerank="llm"``. Callable
      ``(query, candidates) -> ordered ids``. See
      ``kg.rerank.llm.LLMRankerFn`` for the contract.

    When rerank is off, the first-stage top-K is returned as before
    and ``rerank_strategy="none"`` is set on the result.
    """
    if not board_id:
        raise HybridSearchError("board_id required")
    if not query:
        raise HybridSearchError("query required")

    if intent:
        resolved = resolve_intent(intent)
    else:
        classify = classifier or (lambda q: _default_classifier(q))
        resolved = classify(query)
        if resolved.name not in INTENT_CATALOG:
            raise IntentNotFoundError(resolved.name)

    deadline_budget = sla_budget_ms * 3.0  # 3× budget = hard SLA (BR)
    start = time.perf_counter()
    partial = False

    # 1. Vector seed
    t0 = time.perf_counter()
    seeds = vector_provider.seed(
        board_id=board_id,
        query=query,
        node_types=resolved.vector_seed_types,
        top_k=top_k,
    )
    vector_ms = (time.perf_counter() - t0) * 1000

    if (time.perf_counter() - start) * 1000 > deadline_budget:
        return _partial(resolved, query, board_id, seeds, [], vector_ms, 0.0, 0.0, sla_budget_ms)

    # 2.a Adaptive hop planning (ideação 1fb13b51). Runs ONCE per call;
    # failure always falls back to the intent's own max_hops so the
    # pipeline never aborts on planner failure.
    hops_used = resolved.max_hops
    hops_reason = "fixed"
    if hop_strategy and hop_strategy != "fixed":
        try:
            from okto_pulse.core.kg.adaptive_hops import (
                clamp_hops,
                get_hop_planner,
            )

            planner = get_hop_planner(
                hop_strategy,
                llm_fn=hop_llm_fn,
                fixed_max_hops=resolved.max_hops,
                fallback_hops=resolved.max_hops,
            )
            decision = planner.plan(
                query=query,
                intent_name=resolved.name,
                seed_titles=[s.title for s in seeds],
            )
            hops_used = clamp_hops(decision.hops)
            hops_reason = decision.reason
        except Exception as e:  # noqa: BLE001 — planner failure degrades
            logger.warning(
                "kg_search_hybrid.hop_planner_failed strategy=%s error=%s",
                hop_strategy, type(e).__name__,
            )
            hops_used = resolved.max_hops
            hops_reason = "planner_error_fallback"

    # 2.b Graph expand
    t1 = time.perf_counter()
    seed_ids = tuple(s.node_id for s in seeds)
    neighbors = graph_expander.expand(
        board_id=board_id,
        seed_ids=seed_ids,
        edges=resolved.expand_edges,
        max_hops=hops_used,
    ) if seed_ids else []
    expand_ms = (time.perf_counter() - t1) * 1000

    if (time.perf_counter() - start) * 1000 > deadline_budget:
        return _partial(
            resolved, query, board_id, seeds, neighbors,
            vector_ms, expand_ms, 0.0, sla_budget_ms,
        )

    # 3. Rank
    t2 = time.perf_counter()
    ranked = _rank(resolved, seeds, neighbors, now=now)
    ranking_ms = (time.perf_counter() - t2) * 1000

    # 4. Optional second-stage rerank (ideação 3070cd53).
    rerank_ms = 0.0
    rerank_applied = "none"
    rerank_requested = (rerank or "none").strip().lower()
    if rerank_requested != "none" and ranked:
        from okto_pulse.core.kg.rerank import get_reranker

        t3 = time.perf_counter()
        reranker = get_reranker(
            rerank_requested, llm_ranker_fn=rerank_llm_fn
        )
        rerank_applied = getattr(reranker, "name", rerank_requested)
        pool = list(ranked[: max(top_k, rerank_pool)])
        try:
            ranked = reranker.rerank(query, pool, top_n=top_k)
        except Exception:  # noqa: BLE001
            # Rerank is a quality booster, not a correctness gate.
            # Any failure (model miss, LLM timeout) falls back to the
            # first-stage ranking so the pipeline never errors on it.
            logger.warning(
                "kg_search_hybrid.rerank_failed strategy=%s intent=%s "
                "— returning first-stage results",
                rerank_applied, resolved.name,
                exc_info=True,
            )
            ranked = list(pool)
            rerank_applied = "none"
        rerank_ms = (time.perf_counter() - t3) * 1000

    elapsed = (time.perf_counter() - start) * 1000
    partial = elapsed > sla_budget_ms  # soft breach — surfaced but no abort
    if elapsed > deadline_budget:
        partial = True
        logger.warning(
            "kg_search_hybrid.sla_breach intent=%s elapsed_ms=%.1f budget_ms=%.1f",
            resolved.name, elapsed, deadline_budget,
        )

    # v0.3.0 R3: fire-and-forget hit hook for every node in the final
    # top-K. The counter update happens in the background; the response
    # never waits for the flush. Failures are swallowed by the task and
    # logged by kg_service._flush_hits.
    top_k_results = tuple(ranked[:top_k])
    try:
        import asyncio as _asyncio
        from okto_pulse.core.kg.kg_service import KGService as _KGService
        loop = _asyncio.get_running_loop()
        svc = _KGService()
        for result in top_k_results:
            node_id = getattr(result, "node_id", None)
            node_type = getattr(result, "node_type", None)
            if node_id and node_type:
                loop.create_task(svc.increment_hit(board_id, node_type, node_id))
    except Exception:
        # No running loop (sync caller) or module import error — skip hook.
        pass

    return HybridSearchResult(
        intent=resolved.name,
        query=query,
        board_id=board_id,
        seeds=tuple(seeds),
        neighbors=tuple(neighbors),
        ranked=top_k_results,
        timing=HybridSearchTiming(
            vector_seed_ms=vector_ms,
            graph_expand_ms=expand_ms,
            ranking_ms=ranking_ms,
            rerank_ms=rerank_ms,
        ),
        partial=partial,
        sla_budget_ms=sla_budget_ms,
        rerank_strategy=rerank_applied,
        hops_used=hops_used,
        hops_stopped_reason=hops_reason,
    )


def _partial(
    resolved: SearchIntent,
    query: str,
    board_id: str,
    seeds: list[VectorSeed],
    neighbors: list[GraphNeighbor],
    vector_ms: float,
    expand_ms: float,
    ranking_ms: float,
    sla_budget_ms: float,
) -> HybridSearchResult:
    return HybridSearchResult(
        intent=resolved.name,
        query=query,
        board_id=board_id,
        seeds=tuple(seeds),
        neighbors=tuple(neighbors),
        ranked=(),
        timing=HybridSearchTiming(
            vector_seed_ms=vector_ms,
            graph_expand_ms=expand_ms,
            ranking_ms=ranking_ms,
        ),
        partial=True,
        sla_budget_ms=sla_budget_ms,
    )


def _default_classifier(query: str) -> SearchIntent:
    """Delegate to the package classifier when no caller-supplied one is in
    play. Avoids a circular import at module import time."""
    from .classifier import classify_intent
    return classify_intent(query)
