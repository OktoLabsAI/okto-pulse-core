"""FusionRewriter — generates K paraphrases for RAG-Fusion.

The LLM produces K lexical/structural variants of the same question.
Each variant is retrieved independently and the rankings merged via
Reciprocal Rank Fusion (see rrf.merge_rrf). Useful when the query has
ambiguous word choice: different paraphrases surface different seeds
and RRF promotes nodes that appear well-ranked across all of them.

LLM contract: `Callable[[str, int], list[str]]` — recebe (query, K)
e retorna a lista de K paraphrases. Retornar menos que 1 item degrada
para 'none'.
"""

from __future__ import annotations

import hashlib
import logging
from functools import lru_cache
from typing import Callable

from .interfaces import RewriteResult

logger = logging.getLogger("okto_pulse.kg.query_rewrite.fusion")

FusionLLMFn = Callable[[str, int], list[str]]


class FusionRewriter:
    """K-paraphrases rewriter for RAG-Fusion."""

    name = "fusion"

    def __init__(self, llm_fn: FusionLLMFn, *, fusion_paraphrases: int = 3) -> None:
        self._llm = llm_fn
        self._k = max(1, fusion_paraphrases)

    def rewrite(self, query: str) -> RewriteResult:
        # The K is baked into the instance — cache is keyed by query
        # only, so two instances with different K don't share entries
        # because each instance has its own bound `_compute`.
        return self._compute(query)

    @lru_cache(maxsize=256)
    def _compute(self, query: str) -> RewriteResult:
        try:
            paraphrases = self._llm(query, self._k)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "fusion_rewriter.llm_error error=%s query_hash=%s",
                type(e).__name__,
                hashlib.sha256(query.encode("utf-8")).hexdigest()[:16],
            )
            return _passthrough(query)

        if not isinstance(paraphrases, (list, tuple)):
            return _passthrough(query)

        cleaned = tuple(
            p for p in paraphrases if isinstance(p, str) and p.strip()
        )
        if not cleaned:
            return _passthrough(query)

        return RewriteResult(
            strategy="fusion",
            original_query=query,
            rewritten_queries=cleaned[: self._k],
            hyde_passage=None,
        )


def _passthrough(query: str) -> RewriteResult:
    return RewriteResult(
        strategy="none",
        original_query=query,
        rewritten_queries=(query,),
        hyde_passage=None,
    )
