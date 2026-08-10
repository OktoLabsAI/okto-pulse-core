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

from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
)


# Edges that only Layer 1 may create. Kept in sync with the rule_id emitters
# in `workers/deterministic_worker.py`.
DETERMINISTIC_EDGE_TYPES: Final[frozenset[str]] = frozenset({
    "tests",
    "implements",
    "violates",
    "derives_from",
    "mentions",
    "originates_from",
    "covered_by",
    # Code Traceability relationships are derived exclusively from persisted
    # Evidence/Target link and resolution records.  The cognitive agent never
    # infers them and Pulse never inspects an external repository for them.
    "supports",
    "overlaps",
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
    "relates_to",       # Decision→Alternative + cognitive artifact→taxonomy
    "validates",        # Learning→Bug, agent owns it
})


# S-KG-01 / BR-KG-02 — canonical cognitive taxonomy. A cognitive artifact proves
# provenance through EXISTING edge names only (no new edge types): ``validates``
# to a canonical Bug when a Learning is bug-derived, otherwise ``relates_to`` to
# one of these seven canonical operational/decision endpoints. This is the closed
# set the connectivity guard's ``cognitive_provenance`` policy and the schema's
# additive ``relates_to`` endpoint pairs are both derived from — so neither side
# can drift into inventing a new edge name (learned_from/informs/constrains/…).
LEARNING_RELATES_TO_TARGETS: Final[tuple[str, ...]] = (
    "Entity",
    "Decision",
    "Requirement",
    "Constraint",
    "TestScenario",
    "APIContract",
    "Criterion",
)

# Cognitive artifacts that use the non-bug cognitive_provenance rule. Learning is
# the original S-KG-01 source type; Alternative/Assumption use the same taxonomy
# so final-report closeouts can stay connected without cognitive writers emitting
# deterministic ``belongs_to`` edges.
COGNITIVE_PROVENANCE_NODE_TYPES: Final[tuple[str, ...]] = (
    "Learning",
    "Alternative",
    "Assumption",
)

# Reason code surfaced when a cognitive writer attempts a deterministic edge
# (e.g. ``belongs_to``) it does not own. Closed-vocabulary so MCP/REST error
# envelopes and the connectivity guard share one string (BR-KG-02 fail-closed
# reason codes).
FORBIDDEN_DETERMINISTIC_EDGE_REASON: Final[str] = "forbidden_deterministic_edge"


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
        # Closed-vocabulary reason so callers (MCP/REST envelopes, the
        # connectivity guard) can branch on it without parsing the message.
        self.reason = FORBIDDEN_DETERMINISTIC_EDGE_REASON
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


# Error code surfaced by the MCP/REST wrappers when the cognitive canonical
# invariant is violated (spec 007d1308 — FR2/AC4). Distinct from a generic
# internal failure so agents can branch on it and self-correct the request.
COGNITIVE_NODE_CANONICAL_CODE: Final[str] = "cognitive_node_candidates_must_be_canonical"


class CognitiveNodeLayerError(ValueError):
    """Raised when a cognitive node candidate carries ``graph_layer=working``.

    The cognitive agent's consolidation output is canonical knowledge by
    construction (dec_0b3368fe / dec_26c5cc2d, spec 007d1308); working-layer
    nodes are produced exclusively by the Layer 1 deterministic worker, which
    materializes immature SDLC sources per ``source_maturity``. Carries the
    received and required layers so wrappers can build an actionable error
    without duplicating the message.
    """

    def __init__(self, graph_layer: str | None):
        self.graph_layer = graph_layer
        self.required_graph_layer = GRAPH_LAYER_CANONICAL
        super().__init__(
            f"cognitive node candidate has graph_layer={graph_layer!r}; the "
            f"cognitive path only accepts {GRAPH_LAYER_CANONICAL!r} nodes. "
            f"Resubmit with graph_layer='canonical' (omitting it also defaults "
            f"to canonical). Working-layer nodes are the deterministic worker's "
            f"responsibility, not the cognitive agent's."
        )


def check_cognitive_node_canonical(
    graph_layer: str | None, *, is_system_worker: bool,
) -> None:
    """Enforce the cognitive canonical invariant on a node candidate.

    Raise :class:`CognitiveNodeLayerError` when a cognitive (non-system)
    caller proposes a working-layer node. The Layer 1 deterministic worker
    (``agent_id`` prefixed ``system:``) is exempt: it legitimately stages
    working nodes for immature sources. Cheap string compare — safe in the
    ``add_node_candidate`` hot path.
    """
    if is_system_worker:
        return
    if graph_layer == GRAPH_LAYER_WORKING:
        raise CognitiveNodeLayerError(graph_layer)


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
