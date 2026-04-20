"""QueryRewriter Protocol — pre-retrieve rewriting stage over natural queries.

Ideação 2cf21a31. The first-stage hybrid retrieval receives the user's
raw query and embeds it as a single vector seed. For abstract or
composite queries that seed is noisy: "which decisions contradict BR X
and are still active?" mixes two topics in one embedding, so the HNSW
k-NN returns a blurred set.

A rewriter pre-processes the query into a better retrieval input:
- HyDE generates a hypothetical passage and embeds THAT.
- Decompose splits the query into independent sub-queries.
- RAG-Fusion generates K paraphrases for score fusion.

Implementations satisfy this Protocol via duck typing (PEP 544).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from okto_pulse.core.kg.query_rewrite.interfaces import RewriteResult


@runtime_checkable
class QueryRewriter(Protocol):
    """Contract for a pre-retrieve query rewriter.

    Implementations MUST be deterministic for a given (strategy, query)
    tuple — the factory's LRU cache depends on this. Implementations
    SHOULD never raise for normal flow; pathological LLM responses are
    expected to degrade gracefully (e.g. decompose returning [] falls
    back to strategy='none').
    """

    name: str

    def rewrite(self, query: str) -> RewriteResult:
        """Rewrite a single query into a RewriteResult.

        Args:
            query: The user's raw natural-language query.

        Returns:
            A RewriteResult carrying the strategy that was actually
            applied (may be 'none' on graceful degradation), the
            original query, one or more rewritten queries, and an
            optional hypothetical passage for HyDE.
        """
        ...
