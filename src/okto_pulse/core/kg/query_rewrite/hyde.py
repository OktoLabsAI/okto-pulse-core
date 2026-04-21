"""HyDERewriter — Hypothetical Document Embeddings.

Gao et al. (2022): the user's query is often short and abstract, which
makes a direct embedding a poor retrieval seed. HyDE asks the LLM to
write a short hypothetical passage that would answer the query, then
embeds THAT passage for the vector seed. The retrieval still matches
against the original query for caller audit, but the HNSW k-NN runs
against the better seed.

The LLM contract is `Callable[[str], str]` — recibe a query crua,
retorna o passage. Provider injection — zero hard dep.
"""

from __future__ import annotations

import hashlib
import logging
from functools import lru_cache
from typing import Callable

from .interfaces import RewriteResult

logger = logging.getLogger("okto_pulse.kg.query_rewrite.hyde")

HyDELLMFn = Callable[[str], str]


class HyDERewriter:
    """Hypothetical passage rewriter."""

    name = "hyde"

    def __init__(self, llm_fn: HyDELLMFn) -> None:
        self._llm = llm_fn

    def rewrite(self, query: str) -> RewriteResult:
        # Delegate to cached helper keyed by (query). The outer
        # instance changes per factory call — safe because `_compute`
        # binds to the current instance's LLM via closure.
        return self._compute(query)

    @lru_cache(maxsize=256)
    def _compute(self, query: str) -> RewriteResult:
        try:
            passage = self._llm(query)
            if not isinstance(passage, str) or not passage.strip():
                # LLM returned empty or wrong type — degrade silently.
                logger.warning(
                    "hyde_rewriter.empty_passage query_hash=%s",
                    hashlib.sha256(query.encode("utf-8")).hexdigest()[:16],
                )
                return RewriteResult(
                    strategy="none",
                    original_query=query,
                    rewritten_queries=(query,),
                    hyde_passage=None,
                )
            return RewriteResult(
                strategy="hyde",
                original_query=query,
                rewritten_queries=(query,),
                hyde_passage=passage,
            )
        except Exception as e:  # noqa: BLE001 — any LLM failure degrades
            logger.warning(
                "hyde_rewriter.llm_error error=%s query_hash=%s",
                type(e).__name__,
                hashlib.sha256(query.encode("utf-8")).hexdigest()[:16],
            )
            return RewriteResult(
                strategy="none",
                original_query=query,
                rewritten_queries=(query,),
                hyde_passage=None,
            )
