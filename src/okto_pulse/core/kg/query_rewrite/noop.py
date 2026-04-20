"""NoopRewriter — identity passthrough.

Returns the query unchanged wrapped in a RewriteResult. Used as the
default strategy so callers that don't opt in keep the previous
behaviour (no LLM call, no latency).
"""

from __future__ import annotations

from .interfaces import RewriteResult


class NoopRewriter:
    name = "none"

    def rewrite(self, query: str) -> RewriteResult:
        return RewriteResult(
            strategy="none",
            original_query=query,
            rewritten_queries=(query,),
            hyde_passage=None,
        )
