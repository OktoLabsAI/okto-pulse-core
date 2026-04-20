"""Second-stage reranker for hybrid search results (ideação 3070cd53).

Usage::

    from okto_pulse.core.kg.rerank import get_reranker

    rr = get_reranker("cross_encoder")       # or "llm", "token_overlap", "none"
    reordered = rr.rerank(query, candidates, top_n=10)

Strategies:

- ``none`` — NoopReranker, returns input unchanged (truncated to top_n).
  Default. Preserves existing behaviour when rerank is disabled.
- ``token_overlap`` — TokenOverlapReranker, zero-dep lexical Jaccard-style
  overlap between query tokens and candidate title+content. Deterministic,
  <1ms. Useful as a safety net and as the test default.
- ``cross_encoder`` — CrossEncoderReranker, sentence-transformers
  ms-marco-MiniLM cross-encoder. Best precision for short passages; needs
  the `[kg-embeddings]` extra installed.
- ``llm`` — LLMReranker, delegates to a configured LLM with a RankGPT-style
  prompt. Highest quality for abstract queries, highest latency.

`auto` is not exposed here; callers (e.g. `kg_search_hybrid`) decide the
strategy based on their own budget and fall back to `token_overlap` when
a heavier provider is unavailable.
"""

from .factory import get_reranker, reset_reranker_cache
from .interfaces import RerankCandidate
from .noop import NoopReranker
from .token_overlap import TokenOverlapReranker

__all__ = [
    "RerankCandidate",
    "NoopReranker",
    "TokenOverlapReranker",
    "get_reranker",
    "reset_reranker_cache",
]
