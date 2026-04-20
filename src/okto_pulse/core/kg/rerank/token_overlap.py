"""TokenOverlapReranker — zero-dep Jaccard-style lexical reranker.

Ideação 3070cd53. Baseline reranker used when a heavier model is
unavailable and as the default in tests. Scores each candidate by the
Jaccard overlap between the query's token set and the candidate's
text token set, normalised by candidate length to avoid unfair bias
toward very long passages.

The score is *added* to the candidate's existing `score` field when
present (soft boost) so ties from the first-stage blend get broken by
lexical signal rather than ignored. Deterministic and trivially fast
(O(n·m) on short texts, typically <1 ms for top-50).

This is intentionally a weak model — useful as a sanity baseline, as a
regression guard (tests can snapshot its output) and as the fallback
when `cross_encoder` / `llm` rerankers are not configured.
"""

from __future__ import annotations

import re
from dataclasses import replace
from typing import Sequence

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = frozenset({
    # Small English stoplist — rerank is meant to surface content words.
    "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
    "of", "to", "in", "on", "at", "for", "by", "with", "and", "or",
    "but", "as", "from", "that", "this", "these", "those", "it",
    "what", "which", "who", "whom", "whose", "why", "how", "when",
    "where", "do", "does", "did", "have", "has", "had", "i", "me",
    "my", "we", "our", "you", "your", "he", "she", "they", "them",
    # Portuguese words commonly seen in the Okto Pulse SDLC corpus.
    "o", "a", "os", "as", "um", "uma", "de", "do", "da", "dos", "das",
    "em", "no", "na", "nos", "nas", "por", "para", "pelo", "pela",
    "com", "sem", "e", "ou", "mas", "que", "quem", "qual", "quais",
    "como", "quando", "onde", "porque", "por", "ser", "está", "são",
    "é", "foi", "foram", "tem", "têm", "ter", "tinha", "havia",
})


def _tokenise(text: str) -> frozenset[str]:
    """Lowercase, extract word tokens, drop stopwords and 1-char noise."""
    return frozenset(
        t for t in _TOKEN_RE.findall(text.lower())
        if t not in _STOPWORDS and len(t) > 1
    )


def _jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    return inter / len(a | b)


class TokenOverlapReranker:
    """Lexical-overlap reranker. See module docstring for semantics."""

    name = "token_overlap"

    def __init__(self, *, boost_weight: float = 1.0) -> None:
        # Weight the overlap score against the candidate's own score when
        # the candidate carries one. 1.0 = equal weight; >1.0 = lexical
        # dominates; <1.0 = lexical only breaks ties.
        self.boost_weight = boost_weight

    def rerank(
        self,
        query: str,
        candidates: Sequence,
        *,
        top_n: int = 10,
    ) -> list:
        if top_n <= 0 or not candidates:
            return []

        q_tokens = _tokenise(query)
        if not q_tokens:
            # Query became empty after tokenisation — nothing to rerank
            # against, preserve input order.
            return list(candidates[:top_n])

        scored: list[tuple[float, int, object]] = []
        for idx, cand in enumerate(candidates):
            text = self._text_of(cand)
            overlap = _jaccard(q_tokens, _tokenise(text))
            prev_score = float(getattr(cand, "score", 0.0) or 0.0)
            new_score = prev_score + self.boost_weight * overlap
            # idx as secondary key preserves input order on exact ties
            # (stability over implementation-defined ordering).
            scored.append((new_score, -idx, cand))

        scored.sort(key=lambda t: (t[0], t[1]), reverse=True)

        out: list = []
        for new_score, _neg_idx, cand in scored[:top_n]:
            # If the candidate is a dataclass with a `score` field,
            # produce a replaced copy so downstream consumers see the
            # post-rerank score. Otherwise return the candidate as-is.
            if hasattr(cand, "score") and hasattr(cand, "__dataclass_fields__"):
                try:
                    out.append(replace(cand, score=new_score))
                    continue
                except (TypeError, ValueError):
                    pass
            out.append(cand)
        return out

    @staticmethod
    def _text_of(candidate: object) -> str:
        """Extract a displayable text block from a candidate. Preferred
        sources: ``text_for_rerank`` property, else title+content, else
        just the title. Kept tolerant so callers can pass dataclasses,
        dicts or plain objects without a shared base."""
        if hasattr(candidate, "text_for_rerank"):
            return getattr(candidate, "text_for_rerank")
        title = getattr(candidate, "title", "") or ""
        content = getattr(candidate, "content", "") or ""
        if isinstance(candidate, dict):
            title = candidate.get("title", title)
            content = candidate.get("content", content)
        return f"{title}\n{content}" if content else title
