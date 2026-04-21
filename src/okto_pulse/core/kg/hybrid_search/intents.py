"""Closed catalog of hybrid search intents (BR `Intent Catalog Closure`).

Free-text intents are rejected at the API boundary. Every supported intent
carries its own vector seed shape, Kùzu expand template, and ranking
weights. Bumping SCHEMA_VERSION invalidates existing classifier
heuristics — downstream tools must version-pin.
"""

from __future__ import annotations

from dataclasses import dataclass, field

SCHEMA_VERSION = "v1"

INTENT_CONTRADICTION_CHECK = "contradiction_check"
INTENT_IMPACT_ANALYSIS = "impact_analysis"
INTENT_ALTERNATIVES_LOOKUP = "alternatives_lookup"
INTENT_LEARNINGS_FOR_BUG = "learnings_for_bug"
INTENT_DEPENDENCY_TRACE = "dependency_trace"


@dataclass(frozen=True)
class RankingWeights:
    """Linear blend applied by `hybrid._rank` over the per-result score
    components. Weights sum to 1.0 by convention — enforced in __post_init__
    so typos in the catalog are caught at import time."""

    vector_sim: float
    graph_proximity_inv: float
    edge_confidence: float
    recency_decay: float

    def __post_init__(self) -> None:
        total = self.vector_sim + self.graph_proximity_inv + self.edge_confidence + self.recency_decay
        if abs(total - 1.0) > 0.001:
            raise ValueError(
                f"RankingWeights must sum to 1.0, got {total:.3f}"
            )


@dataclass(frozen=True)
class SearchIntent:
    """One entry in the intent catalog."""

    name: str
    vector_seed_types: tuple[str, ...]
    expand_edges: tuple[str, ...]
    max_hops: int
    weights: RankingWeights
    description: str
    keywords: tuple[str, ...] = field(default_factory=tuple)


# Catalog. Keep in alphabetical order for diff hygiene.
INTENT_CATALOG: dict[str, SearchIntent] = {
    INTENT_ALTERNATIVES_LOOKUP: SearchIntent(
        name=INTENT_ALTERNATIVES_LOOKUP,
        vector_seed_types=("Alternative",),
        expand_edges=("relates_to",),
        max_hops=1,
        weights=RankingWeights(
            vector_sim=0.6, graph_proximity_inv=0.1,
            edge_confidence=0.1, recency_decay=0.2,
        ),
        description="What alternatives have been rejected?",
        keywords=(
            "alternative", "alternativa", "rejected", "discarded",
            "descartada", "considerado",
        ),
    ),
    INTENT_CONTRADICTION_CHECK: SearchIntent(
        name=INTENT_CONTRADICTION_CHECK,
        vector_seed_types=("Decision",),
        expand_edges=("contradicts", "supersedes"),
        max_hops=1,
        weights=RankingWeights(
            vector_sim=0.4, graph_proximity_inv=0.1,
            edge_confidence=0.4, recency_decay=0.1,
        ),
        description="Does this decision contradict any existing one?",
        keywords=(
            "contradict", "contradiz", "conflict", "conflita", "confronta",
            "inconsistent", "incompatível",
        ),
    ),
    INTENT_DEPENDENCY_TRACE: SearchIntent(
        name=INTENT_DEPENDENCY_TRACE,
        vector_seed_types=("Decision",),
        expand_edges=("depends_on", "mentions"),
        max_hops=3,
        weights=RankingWeights(
            vector_sim=0.2, graph_proximity_inv=0.6,
            edge_confidence=0.1, recency_decay=0.1,
        ),
        description="What does this decision require?",
        keywords=(
            "depend", "require", "prerequisite", "requisito",
            "pré-requisito", "dependência",
        ),
    ),
    INTENT_IMPACT_ANALYSIS: SearchIntent(
        name=INTENT_IMPACT_ANALYSIS,
        vector_seed_types=("Decision", "Requirement"),
        expand_edges=("depends_on", "contradicts"),
        max_hops=2,
        weights=RankingWeights(
            vector_sim=0.3, graph_proximity_inv=0.4,
            edge_confidence=0.2, recency_decay=0.1,
        ),
        description="If I change this, what breaks?",
        keywords=(
            "impact", "impacto", "break", "quebra", "afetará",
            "affect", "downstream", "depende-de",
        ),
    ),
    INTENT_LEARNINGS_FOR_BUG: SearchIntent(
        name=INTENT_LEARNINGS_FOR_BUG,
        vector_seed_types=("Bug", "Learning"),
        expand_edges=("validates", "violates"),
        max_hops=1,
        weights=RankingWeights(
            vector_sim=0.6, graph_proximity_inv=0.0,
            edge_confidence=0.4, recency_decay=0.0,
        ),
        description="Have we seen a similar bug before?",
        keywords=(
            "bug", "error", "failure", "falha", "similar-bug",
            "lesson", "lição", "aprendizado", "learning",
        ),
    ),
}


class IntentNotFoundError(ValueError):
    """Raised when the caller passes an intent outside the catalog.

    The HTTP/MCP layer translates this into 400 `unknown_intent` per BR
    `Intent Catalog Closure`. Carries `supported` so error messages can
    enumerate the allowed values.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.supported = tuple(sorted(INTENT_CATALOG.keys()))
        super().__init__(
            f"unknown intent '{name}' — supported: {self.supported}"
        )


def resolve_intent(name: str) -> SearchIntent:
    """Look up a `SearchIntent` or raise `IntentNotFoundError`."""
    if not name:
        raise IntentNotFoundError(name or "")
    intent = INTENT_CATALOG.get(name)
    if intent is None:
        raise IntentNotFoundError(name)
    return intent
