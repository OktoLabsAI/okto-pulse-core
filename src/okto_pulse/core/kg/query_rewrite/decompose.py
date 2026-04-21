"""DecomposeRewriter — splits a composite query into sub-queries.

Useful when the user asks "A and B" as a single question: the LLM
breaks it into independent queries each of which retrieves its own set,
and the outer pipeline merges via union with first-occurrence-wins
deduplication.

LLM contract: `Callable[[str], list[str]]` — recibe a query crua,
retorna a lista de sub-queries. Retornar [] ou um único item é um
sinal explícito de que a query não era composta — degrada para
strategy='none' silenciosamente.
"""

from __future__ import annotations

import hashlib
import logging
from functools import lru_cache
from typing import Callable

from .interfaces import RewriteResult

logger = logging.getLogger("okto_pulse.kg.query_rewrite.decompose")

DecomposeLLMFn = Callable[[str], list[str]]


class DecomposeRewriter:
    """Sub-query decomposer."""

    name = "decompose"

    def __init__(self, llm_fn: DecomposeLLMFn) -> None:
        self._llm = llm_fn

    def rewrite(self, query: str) -> RewriteResult:
        return self._compute(query)

    @lru_cache(maxsize=256)
    def _compute(self, query: str) -> RewriteResult:
        try:
            subs = self._llm(query)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "decompose_rewriter.llm_error error=%s query_hash=%s",
                type(e).__name__,
                hashlib.sha256(query.encode("utf-8")).hexdigest()[:16],
            )
            return _passthrough(query)

        # Non-list or empty → non-composite → passthrough.
        if not isinstance(subs, (list, tuple)) or len(subs) < 2:
            return _passthrough(query)

        cleaned = tuple(s for s in subs if isinstance(s, str) and s.strip())
        if len(cleaned) < 2:
            return _passthrough(query)

        return RewriteResult(
            strategy="decompose",
            original_query=query,
            rewritten_queries=cleaned,
            hyde_passage=None,
        )


def _passthrough(query: str) -> RewriteResult:
    return RewriteResult(
        strategy="none",
        original_query=query,
        rewritten_queries=(query,),
        hyde_passage=None,
    )
