"""Pydantic v2 request/response models for the 7 consolidation primitives.

These models form the wire contract between the code agent (MCP client) and
the server. Every primitive takes a typed request and returns a typed response
so TypeScript codegen (openapi-typescript) can produce strict types for the
dashboard.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class KGNodeType(str, Enum):
    DECISION = "Decision"
    CRITERION = "Criterion"
    CONSTRAINT = "Constraint"
    ASSUMPTION = "Assumption"
    REQUIREMENT = "Requirement"
    ENTITY = "Entity"
    API_CONTRACT = "APIContract"
    TEST_SCENARIO = "TestScenario"
    BUG = "Bug"
    LEARNING = "Learning"
    ALTERNATIVE = "Alternative"


class KGEdgeType(str, Enum):
    SUPERSEDES = "supersedes"
    CONTRADICTS = "contradicts"
    DERIVES_FROM = "derives_from"
    RELATES_TO = "relates_to"
    MENTIONS = "mentions"
    DEPENDS_ON = "depends_on"
    VIOLATES = "violates"
    IMPLEMENTS = "implements"
    TESTS = "tests"
    VALIDATES = "validates"
    BELONGS_TO = "belongs_to"


class ReconciliationOperation(str, Enum):
    ADD = "ADD"
    UPDATE = "UPDATE"
    SUPERSEDE = "SUPERSEDE"
    NOOP = "NOOP"


class SessionStatus(str, Enum):
    OPEN = "open"
    COMMITTED = "committed"
    ABORTED = "aborted"
    EXPIRED = "expired"


# ---------------------------------------------------------------------------
# Shared DTOs
# ---------------------------------------------------------------------------


class NodeCandidate(BaseModel):
    """A node proposed by the agent during a session (not yet committed)."""

    model_config = ConfigDict(use_enum_values=True)

    candidate_id: str = Field(..., description="Agent-supplied id, unique within session")
    node_type: KGNodeType
    title: str
    content: str | None = None
    context: str | None = None
    justification: str | None = None
    source_artifact_ref: str | None = None
    source_confidence: float = Field(0.7, ge=0.0, le=1.0)
    # v0.3.0: continuous score replacing validation_status. Starts neutral
    # (0.5) for fresh candidates; R2's scoring pipeline recomputes on commit
    # based on source_confidence, edge degree, query hits, and contradictions.
    # Range [0.0, 1.5] — the upper bound >1.0 lets frequently-hit nodes rise
    # above their source_confidence cap.
    relevance_score: float = Field(0.5, ge=0.0, le=1.5)
    # v0.3.1 (spec 0eb51d3e): additive boost derived from the source card's
    # priority at extraction time. Only the Layer 1 deterministic worker
    # sets this non-zero (process_card); cognitive candidates default to 0.0.
    # Frozen after insert — recompute paths read but never overwrite the
    # persisted column. Cap at +0.2 (CRITICAL) is enforced at the boundary
    # so no caller can slip a larger boost past the clamp at 1.5.
    priority_boost: float = Field(
        0.0,
        ge=0.0,
        le=0.2,
        description="Additive boost derived from source card priority, frozen at insert time",
    )


class EdgeCandidate(BaseModel):
    """An edge proposed by the agent during a session."""

    model_config = ConfigDict(use_enum_values=True)

    candidate_id: str
    edge_type: KGEdgeType
    from_candidate_id: str = Field(
        ...,
        description="candidate_id of source node (either this session OR existing kuzu_node_id prefix 'kg:')",
    )
    to_candidate_id: str
    confidence: float = Field(0.7, ge=0.0, le=1.0)
    # v0.2.0 provenance metadata (spec c48a5c33). Optional so legacy callers
    # keep working — TransactionOrchestrator fills sensible defaults. When the
    # Layer 1 deterministic worker feeds candidates in, these fields carry the
    # rule_id/layer up to Kùzu so /metrics can segment correctly.
    layer: str | None = None
    rule_id: str | None = None
    created_by: str | None = None
    fallback_reason: str | None = None


class ReconciliationHint(BaseModel):
    """Deterministic diff hint produced by the server for a node candidate."""

    model_config = ConfigDict(use_enum_values=True)

    candidate_id: str
    operation: ReconciliationOperation
    target_node_id: str | None = Field(
        None, description="Existing kuzu node id when operation=UPDATE/SUPERSEDE"
    )
    confidence: float = Field(..., ge=0.0, le=1.0)
    reason: str


class SimilarNode(BaseModel):
    """A node found via similarity search."""

    kuzu_node_id: str
    node_type: str
    title: str
    source_artifact_ref: str | None = None
    similarity: float = Field(..., ge=0.0, le=1.0)


# ---------------------------------------------------------------------------
# begin_consolidation
# ---------------------------------------------------------------------------


class BeginConsolidationRequest(BaseModel):
    board_id: str
    artifact_type: str
    artifact_id: str
    raw_content: str = Field(..., description="Full artifact content used for SHA256 dedup")
    deterministic_candidates: list[NodeCandidate] = Field(
        default_factory=list,
        description="Server-side pre-extracted candidates (ORNs, refs, structured fields)",
    )


class BeginConsolidationResponse(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    session_id: str
    board_id: str
    artifact_id: str
    artifact_type: str
    status: SessionStatus
    content_hash: str
    nothing_changed: bool = Field(
        False, description="True if SHA256 matches the last committed session — agent can skip"
    )
    previous_session_id: str | None = None
    expires_at: datetime
    deterministic_candidates_count: int = 0


# ---------------------------------------------------------------------------
# add_node_candidate
# ---------------------------------------------------------------------------


class AddNodeCandidateRequest(BaseModel):
    session_id: str
    candidate: NodeCandidate


class AddNodeCandidateResponse(BaseModel):
    session_id: str
    candidate_id: str
    accepted: bool
    node_count_in_session: int


# ---------------------------------------------------------------------------
# add_edge_candidate
# ---------------------------------------------------------------------------


class AddEdgeCandidateRequest(BaseModel):
    session_id: str
    candidate: EdgeCandidate


class AddEdgeCandidateResponse(BaseModel):
    session_id: str
    candidate_id: str
    accepted: bool
    edge_count_in_session: int


# ---------------------------------------------------------------------------
# get_similar_nodes
# ---------------------------------------------------------------------------


class GetSimilarNodesRequest(BaseModel):
    session_id: str
    candidate_id: str
    top_k: int = Field(5, ge=1, le=50)
    min_similarity: float = Field(0.3, ge=0.0, le=1.0)


class GetSimilarNodesResponse(BaseModel):
    session_id: str
    candidate_id: str
    similar: list[SimilarNode]


# ---------------------------------------------------------------------------
# propose_reconciliation
# ---------------------------------------------------------------------------


class ProposeReconciliationRequest(BaseModel):
    session_id: str


class ProposeReconciliationResponse(BaseModel):
    session_id: str
    hints: list[ReconciliationHint]


# ---------------------------------------------------------------------------
# commit_consolidation
# ---------------------------------------------------------------------------


class CommitConsolidationRequest(BaseModel):
    session_id: str
    summary_text: str | None = None
    agent_overrides: dict[str, ReconciliationHint] = Field(
        default_factory=dict,
        description="Agent-supplied hint overrides keyed by candidate_id",
    )


class CommitConsolidationResponse(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    session_id: str
    status: SessionStatus
    nodes_added: int
    nodes_updated: int
    nodes_superseded: int
    edges_added: int
    committed_at: datetime


# ---------------------------------------------------------------------------
# abort_consolidation
# ---------------------------------------------------------------------------


class AbortConsolidationRequest(BaseModel):
    session_id: str
    reason: str | None = None


class AbortConsolidationResponse(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    session_id: str
    status: SessionStatus
    compensating_delete_applied: bool = False


# ---------------------------------------------------------------------------
# Error envelope
# ---------------------------------------------------------------------------


class KGPrimitiveError(BaseModel):
    """Uniform error envelope for primitives — maps to MCP tool errors."""

    code: Literal[
        "session_not_found",
        "session_expired",
        "session_ownership_mismatch",
        "invalid_candidate",
        "candidate_not_found",
        "duplicate_candidate_id",
        "backend_error",
        "commit_failed",
        "session_already_committed",
    ]
    message: str
    session_id: str | None = None
    details: dict | None = None
