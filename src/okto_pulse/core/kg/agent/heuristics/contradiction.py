"""Heuristic: Decision ↔ Decision `contradicts` (card b0120a89, spec f565115d).

Pipeline:
    1. Vector seed — top-N cross-spec Decisions with similarity > 0.6.
    2. Entity filter — keep only candidates that share ≥1 Entity with the
       source Decision (reduces LLM bill and false positives).
    3. Polarity LLM — ask "can these two coexist?". Answer NO ⇒ contradicts.
    4. Emit edge with confidence = llm.confidence * 0.95.

The heuristic is a pure function: it does NOT touch Kùzu directly. Callers
run the queries, assemble a `DecisionNeighbor` list, and pass them in. This
keeps the heuristic testable with fixtures instead of a live graph.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .llm_protocol import HeuristicLLM

# Default ceilings from the spec context. Heuristics are free to be less
# confident than their ceiling; they are never allowed to exceed it.
CONTRADICTS_CEILING = 0.95
CONTRADICTS_VECTOR_THRESHOLD = 0.6
CONTRADICTS_LLM_THRESHOLD = 0.6  # LLM confidence floor for emitting


@dataclass(frozen=True)
class DecisionNode:
    node_id: str
    content: str
    entity_ids: frozenset[str] = frozenset()
    spec_id: str | None = None


@dataclass(frozen=True)
class DecisionNeighbor:
    """One candidate neighbor returned by the vector-seed query."""

    decision: DecisionNode
    similarity: float


@dataclass(frozen=True)
class ContradictionCandidate:
    """Heuristic output — ready to hand to `add_edge_candidate`."""

    from_node_id: str  # source Decision
    to_node_id: str    # contradicting Decision
    confidence: float
    cognitive_evidence: str
    reasoning: str
    similarity: float
    shared_entities: tuple[str, ...]


def _filter_by_shared_entity(
    source: DecisionNode,
    neighbors: Iterable[DecisionNeighbor],
) -> list[DecisionNeighbor]:
    """Keep neighbors whose Entity set intersects the source's.

    This is the primary noise filter — two decisions talking about unrelated
    techs are exceedingly unlikely to contradict each other. An empty source
    Entity set disables the filter (cold-start case) so the LLM still gets
    a shot.
    """
    if not source.entity_ids:
        return list(neighbors)
    filtered: list[DecisionNeighbor] = []
    for n in neighbors:
        if source.entity_ids.intersection(n.decision.entity_ids):
            filtered.append(n)
    return filtered


def run_contradiction_heuristic(
    source: DecisionNode,
    neighbors: Iterable[DecisionNeighbor],
    llm: HeuristicLLM,
    *,
    vector_threshold: float = CONTRADICTS_VECTOR_THRESHOLD,
    llm_threshold: float = CONTRADICTS_LLM_THRESHOLD,
    ceiling: float = CONTRADICTS_CEILING,
) -> list[ContradictionCandidate]:
    """Produce `contradicts` edge proposals for a single source Decision.

    `neighbors` must already be cross-spec (exclude same-spec decisions —
    those are covered by other rules) and ranked by vector similarity. The
    heuristic filters by similarity ≥ `vector_threshold` then by shared
    Entity, then calls the LLM for the polarity check.

    Returns edge candidates sorted by confidence DESC so the caller can
    cap by top-k without re-sorting.
    """
    # Vector filter.
    seeded = [n for n in neighbors if n.similarity >= vector_threshold
              and n.decision.node_id != source.node_id]
    if not seeded:
        return []

    # Entity filter.
    filtered = _filter_by_shared_entity(source, seeded)
    if not filtered:
        return []

    out: list[ContradictionCandidate] = []
    for neighbor in filtered:
        verdict = llm.ask_polarity(
            prompt_id="contradiction_v1",
            text_a=source.content,
            text_b=neighbor.decision.content,
            context={
                "source_spec_id": source.spec_id or "",
                "neighbor_spec_id": neighbor.decision.spec_id or "",
                "similarity": f"{neighbor.similarity:.3f}",
            },
        )
        # Polarity: LLM answer True means "they CAN coexist" → no contradiction.
        # We emit only when the LLM says NO with sufficient confidence.
        if verdict.answer:
            continue
        if verdict.confidence < llm_threshold:
            continue
        confidence = min(verdict.confidence * ceiling, ceiling)
        shared = tuple(sorted(source.entity_ids & neighbor.decision.entity_ids))
        out.append(ContradictionCandidate(
            from_node_id=source.node_id,
            to_node_id=neighbor.decision.node_id,
            confidence=confidence,
            cognitive_evidence=verdict.reasoning,
            reasoning=verdict.reasoning,
            similarity=neighbor.similarity,
            shared_entities=shared,
        ))

    out.sort(key=lambda c: c.confidence, reverse=True)
    return out
