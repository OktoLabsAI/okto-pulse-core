"""Pydantic v2 Request/Response models for the 9 tier primario query tools.

Each tool has a typed response model so openapi-typescript can generate strict
TypeScript types for the dashboard and MCP clients get validated JSON.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Shared result DTOs
# ---------------------------------------------------------------------------


class KGNodeResult(BaseModel):
    id: str
    title: str
    content: str | None = None
    justification: str | None = None
    source_artifact_ref: str | None = None
    source_confidence: float = 0.0
    # v0.3.0: validation_status retired in favour of relevance_score.
    # Optional fields so older response bodies that omit them still
    # validate while consumers (frontend types, MCP) catch up.
    relevance_score: float | None = None
    query_hits: int | None = None
    last_queried_at: str | None = None
    created_at: str | None = None
    superseded_by: str | None = None


class ContradictionPair(BaseModel):
    id_a: str
    title_a: str
    id_b: str
    title_b: str
    confidence: float = 0.0


class ContextHop(BaseModel):
    center_id: str
    center_title: str
    hop1_id: str | None = None
    hop1_title: str | None = None
    hop2_id: str | None = None
    hop2_title: str | None = None
    rel1_type: str | None = None
    rel2_type: str | None = None


class SupersedenceEntry(BaseModel):
    id: str
    title: str
    created_at: str | None = None
    superseded_by: str | None = None
    superseded_at: str | None = None


class SimilarDecisionResult(BaseModel):
    id: str
    title: str
    source_artifact_ref: str | None = None
    similarity: float = 0.0
    combined_score: float = 0.0


class ConstraintExplanation(BaseModel):
    id: str
    title: str
    content: str | None = None
    justification: str | None = None
    source_artifact_ref: str | None = None
    source_confidence: float = 0.0
    origins: list[dict] = Field(default_factory=list)
    violations: list[dict] = Field(default_factory=list)


class AlternativeResult(BaseModel):
    id: str
    title: str
    content: str | None = None
    justification: str | None = None
    source_confidence: float = 0.0
    source_artifact_ref: str | None = None


class LearningResult(BaseModel):
    learning_id: str
    learning_title: str
    learning_content: str | None = None
    justification: str | None = None
    source_confidence: float = 0.0
    bug_id: str
    bug_title: str


class GlobalResult(BaseModel):
    board_id: str
    id: str
    title: str
    similarity: float = 0.0


# ---------------------------------------------------------------------------
# Tool responses
# ---------------------------------------------------------------------------


class DecisionHistoryResponse(BaseModel):
    decisions: list[KGNodeResult]
    count: int = 0


class RelatedContextResponse(BaseModel):
    context: list[ContextHop]
    count: int = 0


class SupersedenceChainResponse(BaseModel):
    chain: list[SupersedenceEntry]
    depth: int = 0
    current_active: str


class ContradictionsResponse(BaseModel):
    pairs: list[ContradictionPair]
    count: int = 0


class SimilarDecisionsResponse(BaseModel):
    decisions: list[SimilarDecisionResult]
    count: int = 0


class ConstraintExplanationResponse(BaseModel):
    constraint: ConstraintExplanation


class AlternativesResponse(BaseModel):
    alternatives: list[AlternativeResult]
    count: int = 0


class LearningsResponse(BaseModel):
    learnings: list[LearningResult]
    count: int = 0


class GlobalQueryResponse(BaseModel):
    results: list[GlobalResult]
    count: int = 0


# ---------------------------------------------------------------------------
# Error response
# ---------------------------------------------------------------------------


class KGToolErrorResponse(BaseModel):
    error: dict = Field(
        ...,
        description="Error envelope with code, message, details",
    )
