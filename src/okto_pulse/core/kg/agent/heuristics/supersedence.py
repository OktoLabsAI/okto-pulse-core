"""Heuristic: Decision → Decision `supersedes` (card b0120a89, spec f565115d).

Pipeline:
    1. Seed from audit trail: the new Decision's closest prior-version peers
       (by vector similarity, same refinement chain preferred).
    2. Scope filter: same Entity set **and** same refinement_id OR same
       spec_id (mirrors the "same scope" clause from the spec).
    3. Polarity LLM: "Does the new decision FULLY invalidate the old one?".
       Answer YES ⇒ supersedes. The heuristic also flips `retired=true` on
       the superseded Decision via a metadata hint (caller persists it).
    4. Confidence = llm.confidence * 0.9 (BR ceiling 0.95, but supersedes
       is inherently more fragile than contradicts so we use 0.9).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .contradiction import DecisionNode, DecisionNeighbor
from .llm_protocol import HeuristicLLM

SUPERSEDES_CEILING = 0.9
SUPERSEDES_VECTOR_THRESHOLD = 0.65
SUPERSEDES_LLM_THRESHOLD = 0.7


@dataclass(frozen=True)
class SupersedenceCandidate:
    from_node_id: str  # NEW decision
    to_node_id: str    # OLD (superseded) decision
    confidence: float
    cognitive_evidence: str
    reasoning: str
    similarity: float
    mark_retired: bool = True  # caller must set retired=True on the target


def _scope_matches(a: DecisionNode, b: DecisionNode) -> bool:
    """Supersedence only applies within one scope; the heuristic defines
    scope as same `spec_id` OR overlap on Entity set. A cross-spec pair is
    better handled by `contradicts`.
    """
    if a.spec_id and b.spec_id and a.spec_id == b.spec_id:
        return True
    if a.entity_ids and b.entity_ids:
        return bool(a.entity_ids & b.entity_ids)
    return False


def run_supersedence_heuristic(
    new_decision: DecisionNode,
    prior_neighbors: Iterable[DecisionNeighbor],
    llm: HeuristicLLM,
    *,
    vector_threshold: float = SUPERSEDES_VECTOR_THRESHOLD,
    llm_threshold: float = SUPERSEDES_LLM_THRESHOLD,
    ceiling: float = SUPERSEDES_CEILING,
) -> list[SupersedenceCandidate]:
    """Return a list of `supersedes` edge proposals.

    `prior_neighbors` MUST only contain decisions from prior versions of the
    same artifact (or earlier audit-trail entries). The caller is responsible
    for that scoping — the heuristic just applies the scope filter + LLM
    verification on top.
    """
    out: list[SupersedenceCandidate] = []
    for neighbor in prior_neighbors:
        if neighbor.decision.node_id == new_decision.node_id:
            continue
        if neighbor.similarity < vector_threshold:
            continue
        if not _scope_matches(new_decision, neighbor.decision):
            continue
        verdict = llm.ask_polarity(
            prompt_id="supersedence_v1",
            text_a=new_decision.content,
            text_b=neighbor.decision.content,
            context={
                "question": "Does the new decision fully invalidate the old in the same scope?",
                "similarity": f"{neighbor.similarity:.3f}",
            },
        )
        if not verdict.answer:
            continue
        if verdict.confidence < llm_threshold:
            continue
        confidence = min(verdict.confidence * ceiling, ceiling)
        out.append(SupersedenceCandidate(
            from_node_id=new_decision.node_id,
            to_node_id=neighbor.decision.node_id,
            confidence=confidence,
            cognitive_evidence=verdict.reasoning,
            reasoning=verdict.reasoning,
            similarity=neighbor.similarity,
        ))

    out.sort(key=lambda c: c.confidence, reverse=True)
    return out
