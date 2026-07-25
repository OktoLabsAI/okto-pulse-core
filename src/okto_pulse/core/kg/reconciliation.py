"""Deterministic reconciliation rule engine.

Decides ADD / UPDATE / SUPERSEDE / NOOP for each node candidate based on:

1. Content hash of the full artifact → NOOP short-circuit for the whole session
2. exact source ref → same lineage; changed Decisions → SUPERSEDE
3. semantic similarity threshold → SUPERSEDE hint (agent confirms via override)
4. otherwise → ADD

This is the server-side "free" baseline the agent receives from
`propose_reconciliation`. The agent can override any hint in `commit_overrides`
when its semantic reading disagrees (e.g. promoting an UPDATE to a SUPERSEDE
because the justification narrative makes clear a decision was reversed).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from okto_pulse.core.kg.node_identity import normalize_text
from okto_pulse.core.kg.schemas import (
    NodeCandidate,
    ReconciliationHint,
    ReconciliationOperation,
)


SIMILARITY_SUPERSEDE_THRESHOLD = 0.85
SIMILARITY_UPDATE_THRESHOLD = 0.95


@dataclass
class ExistingNodeSummary:
    """Minimal info the engine needs about an existing graph node."""

    graph_node_id: str
    node_type: str
    stable_id: str | None  # ORN or external id if present
    title: str
    content: str | None = None
    context: str | None = None
    justification: str | None = None
    similarity: float = 0.0  # 0.0–1.0 against the candidate


def decision_semantics_differ(candidate: Any, existing: Any) -> bool:
    """Whether a Decision candidate changes assertion-bearing fields."""

    def _value(source: Any, field: str) -> Any:
        if isinstance(source, dict):
            return source.get(field)
        return getattr(source, field, None)

    return any(
        normalize_text(_value(candidate, field))
        != normalize_text(_value(existing, field))
        for field in ("title", "content", "context", "justification")
    )


def _is_decision(node_type: Any) -> bool:
    return _same_node_type(node_type, "Decision")


def _evidence_confidence(
    candidate: NodeCandidate,
    match: ExistingNodeSummary,
) -> float:
    """Use observed similarity when available, otherwise extraction confidence."""

    return (
        match.similarity
        if match.similarity > 0.0
        else candidate.source_confidence
    )


def reconcile_candidate(
    candidate: NodeCandidate,
    *,
    nothing_changed: bool,
    existing_matches: list[ExistingNodeSummary],
) -> ReconciliationHint:
    """Apply the deterministic rules to produce one hint for one candidate.

    Args:
        candidate: The agent-supplied node candidate.
        nothing_changed: True if the session's content_hash matches the last
            committed session — forces NOOP regardless of other signals.
        existing_matches: Nodes the server already found via similarity search,
            pre-sorted by similarity DESC.
    """
    if nothing_changed:
        return ReconciliationHint(
            candidate_id=candidate.candidate_id,
            operation=ReconciliationOperation.NOOP,
            target_node_id=None,
            confidence=1.0,
            reason="content_hash matches last committed session",
        )

    # An exact source ref proves lineage, not semantic identity. Decisions are
    # immutable assertions: any semantic delta must create a new generation.
    if candidate.source_artifact_ref:
        for match in existing_matches:
            if (
                match.stable_id == candidate.source_artifact_ref
                and _same_node_type(candidate.node_type, match.node_type)
            ):
                semantic_change = (
                    _is_decision(candidate.node_type)
                    and decision_semantics_differ(candidate, match)
                )
                return ReconciliationHint(
                    candidate_id=candidate.candidate_id,
                    operation=(
                        ReconciliationOperation.SUPERSEDE
                        if semantic_change
                        else ReconciliationOperation.UPDATE
                    ),
                    target_node_id=match.graph_node_id,
                    confidence=_evidence_confidence(candidate, match),
                    reason=(
                        f"exact source ref {candidate.source_artifact_ref!r} "
                        f"selects lineage {match.graph_node_id}; "
                        + (
                            "Decision semantics changed, so preserve the prior "
                            "assertion as a superseded generation"
                            if semantic_change
                            else "assertion semantics are unchanged"
                        )
                    ),
                )

    # Similarity-driven hints: highest-ranked match decides.
    if existing_matches:
        top = existing_matches[0]
        if top.similarity >= SIMILARITY_UPDATE_THRESHOLD:
            semantic_change = (
                _is_decision(candidate.node_type)
                and decision_semantics_differ(candidate, top)
            )
            return ReconciliationHint(
                candidate_id=candidate.candidate_id,
                operation=(
                    ReconciliationOperation.SUPERSEDE
                    if semantic_change
                    else ReconciliationOperation.UPDATE
                ),
                target_node_id=top.graph_node_id,
                confidence=top.similarity,
                reason=(
                    f"semantic match {top.similarity:.2f} ≥ "
                    f"{SIMILARITY_UPDATE_THRESHOLD:.2f} — "
                    + (
                        "Decision assertion differs; preserve history"
                        if semantic_change
                        else "same logical assertion"
                    )
                ),
            )
        if top.similarity >= SIMILARITY_SUPERSEDE_THRESHOLD:
            return ReconciliationHint(
                candidate_id=candidate.candidate_id,
                operation=ReconciliationOperation.SUPERSEDE,
                target_node_id=top.graph_node_id,
                confidence=top.similarity,
                reason=(
                    f"semantic match {top.similarity:.2f} in "
                    f"[{SIMILARITY_SUPERSEDE_THRESHOLD:.2f}, "
                    f"{SIMILARITY_UPDATE_THRESHOLD:.2f}) — likely supersedes; "
                    f"agent may override to UPDATE"
                ),
            )

    # Default: new candidate with no useful match.
    return ReconciliationHint(
        candidate_id=candidate.candidate_id,
        operation=ReconciliationOperation.ADD,
        target_node_id=None,
        confidence=candidate.source_confidence,
        reason="no semantic match in existing graph",
    )


def reconcile_session(
    candidates: dict[str, NodeCandidate],
    *,
    nothing_changed: bool,
    existing_matches_by_candidate: dict[str, list[ExistingNodeSummary]],
) -> dict[str, ReconciliationHint]:
    """Reconcile every candidate in a session. Returns candidate_id → hint."""
    hints: dict[str, ReconciliationHint] = {}
    for cid, cand in candidates.items():
        matches = existing_matches_by_candidate.get(cid, [])
        hints[cid] = reconcile_candidate(
            cand, nothing_changed=nothing_changed, existing_matches=matches,
        )
    return hints


def _same_node_type(candidate_type: Any, existing_type: str) -> bool:
    candidate_type_str = (
        candidate_type.value
        if hasattr(candidate_type, "value")
        else str(candidate_type)
    )
    return candidate_type_str == existing_type
