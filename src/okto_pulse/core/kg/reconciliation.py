"""Deterministic reconciliation rule engine.

Decides ADD / UPDATE / SUPERSEDE / NOOP for each node candidate based on:

1. Content hash of the full artifact → NOOP short-circuit for the whole session
2. stable id match (ORN, kuzu_node_id prefix kg:, etc.) → UPDATE
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

from okto_pulse.core.kg.schemas import (
    NodeCandidate,
    ReconciliationHint,
    ReconciliationOperation,
)


SIMILARITY_SUPERSEDE_THRESHOLD = 0.85
SIMILARITY_UPDATE_THRESHOLD = 0.95


@dataclass
class ExistingNodeSummary:
    """Minimal info the engine needs about an existing kuzu node."""

    kuzu_node_id: str
    node_type: str
    stable_id: str | None  # ORN or external id if present
    title: str
    similarity: float = 0.0  # 0.0–1.0 against the candidate


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

    # Stable-id match: candidate carries source_artifact_ref that already
    # exists on an existing node → UPDATE (same logical entity).
    if candidate.source_artifact_ref:
        for match in existing_matches:
            if (
                match.stable_id == candidate.source_artifact_ref
                and _same_node_type(candidate.node_type, match.node_type)
            ):
                return ReconciliationHint(
                    candidate_id=candidate.candidate_id,
                    operation=ReconciliationOperation.UPDATE,
                    target_node_id=match.kuzu_node_id,
                    confidence=0.95,
                    reason=(
                        f"stable id {candidate.source_artifact_ref!r} already "
                        f"exists as {match.kuzu_node_id}"
                    ),
                )

    # Similarity-driven hints: highest-ranked match decides.
    if existing_matches:
        top = existing_matches[0]
        if top.similarity >= SIMILARITY_UPDATE_THRESHOLD:
            return ReconciliationHint(
                candidate_id=candidate.candidate_id,
                operation=ReconciliationOperation.UPDATE,
                target_node_id=top.kuzu_node_id,
                confidence=top.similarity,
                reason=(
                    f"semantic match {top.similarity:.2f} ≥ "
                    f"{SIMILARITY_UPDATE_THRESHOLD:.2f} — same logical entity"
                ),
            )
        if top.similarity >= SIMILARITY_SUPERSEDE_THRESHOLD:
            return ReconciliationHint(
                candidate_id=candidate.candidate_id,
                operation=ReconciliationOperation.SUPERSEDE,
                target_node_id=top.kuzu_node_id,
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
