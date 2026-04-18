"""Heuristic: Decision → Decision `depends_on` (card f700479d, spec f565115d).

Pipeline:
    1. Cross-spec pair seed: pairs sharing ≥2 Entities (no same-spec pairs).
    2. No FK filter: only keep pairs that do NOT have a hierarchy/derives_from
       edge linking them already — that's Layer 1 territory.
    3. Prerequisite LLM: "Does A require B decided first?".
    4. Confidence ∈ [0.7, 0.85] (implicit edges are inherently softer).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .contradiction import DecisionNode
from .llm_protocol import HeuristicLLM

DEPENDS_ON_CEILING = 0.85
DEPENDS_ON_FLOOR = 0.7  # minimum accepted confidence; below gets dropped
DEPENDS_ON_LLM_THRESHOLD = 0.7
DEPENDS_ON_MIN_SHARED_ENTITIES = 2  # TR: >1


@dataclass(frozen=True)
class CandidatePair:
    """Two decisions a caller has identified as potentially linked."""

    source: DecisionNode
    target: DecisionNode


@dataclass(frozen=True)
class DependsOnCandidate:
    from_node_id: str  # depends on → target
    to_node_id: str    # prerequisite
    confidence: float
    cognitive_evidence: str
    reasoning: str
    shared_entities: tuple[str, ...]


def _qualifies(pair: CandidatePair) -> tuple[bool, frozenset[str]]:
    """Pair passes the Entity-overlap gate when they share ≥2 Entities and
    live in different specs. Returns (ok, shared_entities)."""
    if pair.source.node_id == pair.target.node_id:
        return False, frozenset()
    if pair.source.spec_id and pair.target.spec_id:
        if pair.source.spec_id == pair.target.spec_id:
            return False, frozenset()
    shared = pair.source.entity_ids & pair.target.entity_ids
    if len(shared) < DEPENDS_ON_MIN_SHARED_ENTITIES:
        return False, frozenset()
    return True, shared


def run_depends_on_heuristic(
    pairs: Iterable[CandidatePair],
    llm: HeuristicLLM,
    *,
    floor: float = DEPENDS_ON_FLOOR,
    ceiling: float = DEPENDS_ON_CEILING,
    llm_threshold: float = DEPENDS_ON_LLM_THRESHOLD,
) -> list[DependsOnCandidate]:
    """Propose `depends_on` edges for cross-spec pairs with shared Entities.

    `pairs` must be already filtered to exclude decisions that already have
    a direct FK/hierarchy edge — the heuristic is for IMPLICIT dependencies
    only. Confidence is linear in the LLM confidence, clamped to
    [floor, ceiling].
    """
    out: list[DependsOnCandidate] = []
    for pair in pairs:
        ok, shared = _qualifies(pair)
        if not ok:
            continue
        verdict = llm.ask_polarity(
            prompt_id="depends_on_v1",
            text_a=pair.source.content,
            text_b=pair.target.content,
            context={
                "question": "Does A require B to be implemented/decided first?",
                "shared_entities": ",".join(sorted(shared)),
            },
        )
        if not verdict.answer:
            continue
        if verdict.confidence < llm_threshold:
            continue
        confidence = max(floor, min(verdict.confidence, ceiling))
        out.append(DependsOnCandidate(
            from_node_id=pair.source.node_id,
            to_node_id=pair.target.node_id,
            confidence=confidence,
            cognitive_evidence=verdict.reasoning,
            reasoning=verdict.reasoning,
            shared_entities=tuple(sorted(shared)),
        ))

    out.sort(key=lambda c: c.confidence, reverse=True)
    return out
