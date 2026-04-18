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

    @property
    def total_ms(self) -> float:
        return self.vector_seed_ms + self.graph_expand_ms + self.ranking_ms


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
) -> HybridSearchResult:
    """Run the hybrid pipeline and return the ranked result set.

    The explicit `intent` parameter wins over classification when set —
    mirrors the MCP contract where callers may short-circuit the
    regex+LLM classifier with a known intent.
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

    # 2. Graph expand
    t1 = time.perf_counter()
    seed_ids = tuple(s.node_id for s in seeds)
    neighbors = graph_expander.expand(
        board_id=board_id,
        seed_ids=seed_ids,
        edges=resolved.expand_edges,
        max_hops=resolved.max_hops,
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
        ),
        partial=partial,
        sla_budget_ms=sla_budget_ms,
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
