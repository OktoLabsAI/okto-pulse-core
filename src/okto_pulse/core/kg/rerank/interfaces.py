"""Shared data contract for rerank candidates.

Ideação 3070cd53. The reranker is agnostic to the specific row type it
receives — hybrid_search feeds it `RankedNode`, but future callers may
pass plain dicts or a different ORM shape. `RerankCandidate` is the
minimal structural contract: an id + a displayable text block. The
concrete shape preserves its own fields; the reranker only reads the
two it cares about.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RerankCandidate:
    """Minimal row passed to a reranker. Implementations only touch
    ``node_id`` (stable key) and the text fields (``title`` + optional
    ``content``)."""

    node_id: str
    title: str
    content: str | None = None

    @property
    def text_for_rerank(self) -> str:
        """Concatenated text the reranker scores against. Title carries
        most of the lexical signal; content augments when available."""
        if self.content:
            return f"{self.title}\n{self.content}"
        return self.title
