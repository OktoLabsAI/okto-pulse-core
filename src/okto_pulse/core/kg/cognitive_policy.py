"""Policy boundary between the Layer 1 deterministic worker and the cognitive
agent (spec f565115d + BR `Layer Ownership Isolation` in spec c48a5c33).

The cognitive agent, when acting via MCP primitives, may ONLY create edges
whose semantic requires judgement. Everything extractable from structured
pulse.db fields is Layer 1's exclusive responsibility. This file is the
authoritative catalog — referenced by:

- `primitives.add_edge_candidate` which enforces it server-side.
- The agent prompt (see `prompts/cognitive_agent_v1.md`) which documents
  the contract for the LLM so it doesn't waste turns on refused edges.

Outcome when the agent violates: 403 `layer_violation` with a structured
error payload listing the allowed edges so the next turn can recover.
"""

from __future__ import annotations

from typing import Final


# Edges that only Layer 1 may create. Kept in sync with the rule_id emitters
# in `workers/deterministic_worker.py`.
DETERMINISTIC_EDGE_TYPES: Final[frozenset[str]] = frozenset({
    "tests",
    "implements",
    "violates",
    "derives_from",
    "mentions",
    # Hierarchy backbone — Spec ⇄ Sprint ⇄ Card and child artifacts (Req,
    # Constraint, Criterion, TestScenario, APIContract, Decision, Bug) →
    # parent Spec entity. Layer 1 owns this; cognitive agent must not emit.
    "belongs_to",
})

# Edges that only the cognitive agent may create (non-trivial semantic).
COGNITIVE_EDGE_TYPES: Final[frozenset[str]] = frozenset({
    "contradicts",
    "supersedes",
    "depends_on",
    "relates_to",       # Decision→Alternative, agent owns it
    "validates",        # Learning→Bug, agent owns it
})


# Valid values for the layer metadata on an edge.
VALID_EDGE_LAYERS: Final[frozenset[str]] = frozenset({
    "deterministic", "cognitive", "fallback", "legacy",
})


# Fallback confidence cap — BR `Cognitive Fallback Confidence Cap`. Edges
# the agent proposes to resolve a missing_link_candidate can never carry
# confidence above this value, so callers can distinguish them from native
# Layer 1 emissions without joining audit tables.
FALLBACK_CONFIDENCE_CAP: Final[float] = 0.85


class LayerViolationError(ValueError):
    """Raised when an agent attempts to create a Layer 1-exclusive edge.

    Carries `allowed_layers` and `allowed_edges` so the server can return a
    helpful RFC7807 Problem Details body without each caller duplicating
    the logic.
    """

    def __init__(self, edge_type: str):
        self.edge_type = edge_type
        self.allowed_edges = sorted(COGNITIVE_EDGE_TYPES)
        super().__init__(
            f"edge_type '{edge_type}' is reserved for the Layer 1 deterministic "
            f"worker; cognitive agent may only propose {self.allowed_edges}"
        )


def check_cognitive_edge_allowed(edge_type: str) -> None:
    """Raise LayerViolationError if the agent tries a deterministic edge.

    Called from `add_edge_candidate` immediately after the candidate is
    accepted syntactically. Cheap (frozenset membership) so it's safe in
    the hot path.
    """
    if edge_type in DETERMINISTIC_EDGE_TYPES:
        raise LayerViolationError(edge_type)


def clamp_fallback_confidence(
    confidence: float, *, layer: str,
) -> tuple[float, bool]:
    """Return (clamped_confidence, was_clamped) per BR Cognitive Fallback Cap.

    For layer='fallback', confidence is clamped to FALLBACK_CONFIDENCE_CAP.
    For any other layer the input is returned unchanged. Callers log the
    clamp event (warn level) so agents operators see when their proposals
    are being capped, which often indicates unnecessarily high self-rated
    confidence in the LLM response.
    """
    if layer != "fallback":
        return confidence, False
    if confidence > FALLBACK_CONFIDENCE_CAP:
        return FALLBACK_CONFIDENCE_CAP, True
    return confidence, False
