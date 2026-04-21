"""NoopReranker — preserves input order, truncates to top_n.

Used when reranking is disabled (`rerank="none"`). Keeps the call site
uniform so `kg_search_hybrid` doesn't need a branch for the off state.
"""

from __future__ import annotations

from typing import Sequence


class NoopReranker:
    name = "noop"

    def rerank(
        self,
        query: str,  # noqa: ARG002 — part of protocol, unused here
        candidates: Sequence,
        *,
        top_n: int = 10,
    ) -> list:
        if top_n <= 0:
            return []
        return list(candidates[:top_n])
