"""Hybrid search namespace (spec f565115d).

The existing `kg/search.py` module already owns vector-only similarity
queries, so we keep hybrid (vector + graph) search in its own package to
avoid a breaking refactor. This module exports the intent catalog, the
classifier, and the `kg_search_hybrid` entry point used by the MCP layer.
"""

from .intents import (
    INTENT_ALTERNATIVES_LOOKUP,
    INTENT_CATALOG,
    INTENT_CONTRADICTION_CHECK,
    INTENT_DEPENDENCY_TRACE,
    INTENT_IMPACT_ANALYSIS,
    INTENT_LEARNINGS_FOR_BUG,
    IntentNotFoundError,
    SearchIntent,
    resolve_intent,
)
from .classifier import classify_intent
from .hybrid import (
    HybridSearchResult,
    HybridSearchTiming,
    HybridSearchError,
    kg_search_hybrid,
)

__all__ = [
    "INTENT_ALTERNATIVES_LOOKUP",
    "INTENT_CATALOG",
    "INTENT_CONTRADICTION_CHECK",
    "INTENT_DEPENDENCY_TRACE",
    "INTENT_IMPACT_ANALYSIS",
    "INTENT_LEARNINGS_FOR_BUG",
    "IntentNotFoundError",
    "SearchIntent",
    "resolve_intent",
    "classify_intent",
    "HybridSearchResult",
    "HybridSearchTiming",
    "HybridSearchError",
    "kg_search_hybrid",
]
