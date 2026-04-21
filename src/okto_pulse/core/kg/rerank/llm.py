"""LLMReranker — RankGPT-style passage reranking via an LLM.

Ideação 3070cd53. Places a fixed prompt in front of the LLM that asks
it to rank N candidates by relevance to the query and return an
ordered list of ids. Best for abstract or under-specified queries
where a cross-encoder's lexical signal is weaker than world knowledge.

The LLM client is injected as a callable (query, candidates) →
ordered list[str] of node_ids. This keeps this module independent of
the project's LLM provider (openai/anthropic/vertex — wiring lives
where it belongs, in the caller) and makes the class trivially
testable with a dummy callable.
"""

from __future__ import annotations

from typing import Callable, Sequence

from .token_overlap import TokenOverlapReranker

#: LLM callable contract. Receives the raw query and the list of
#: candidates, returns the *ordered* node_ids (most relevant first).
#: Implementations SHOULD cap output length at ``len(candidates)`` and
#: MUST return ids drawn from the input set (unknown ids are ignored
#: by the reranker; missing ids fall back to their input order).
LLMRankerFn = Callable[[str, Sequence[object]], list[str]]


class LLMReranker:
    """LLM-as-reranker over hybrid_search top-K."""

    name = "llm"

    def __init__(self, ranker_fn: LLMRankerFn) -> None:
        self._rank = ranker_fn

    def rerank(
        self,
        query: str,
        candidates: Sequence,
        *,
        top_n: int = 10,
    ) -> list:
        if top_n <= 0 or not candidates:
            return []

        try:
            ranked_ids = self._rank(query, candidates)
        except Exception:  # noqa: BLE001 — any failure falls back
            # Any LLM error (timeout, parse, quota) falls back to the
            # input order so the retrieval pipeline never fails because
            # of a rerank degradation.
            return list(candidates[:top_n])

        if not ranked_ids:
            return list(candidates[:top_n])

        by_id = {self._id_of(c): c for c in candidates}
        ordered: list = []
        seen: set[str] = set()
        for nid in ranked_ids:
            if nid in by_id and nid not in seen:
                ordered.append(by_id[nid])
                seen.add(nid)
            if len(ordered) >= top_n:
                break

        # Fill from the input order for any id the LLM omitted. Keeps
        # the output top_n even when the LLM returns a partial ranking.
        if len(ordered) < top_n:
            for c in candidates:
                cid = self._id_of(c)
                if cid in seen:
                    continue
                ordered.append(c)
                seen.add(cid)
                if len(ordered) >= top_n:
                    break

        return ordered[:top_n]

    @staticmethod
    def _id_of(candidate: object) -> str:
        return str(getattr(candidate, "node_id", None) or candidate)


def build_default_prompt(query: str, candidates: Sequence) -> str:
    """Compose the RankGPT-style prompt body. Exposed so project-level
    LLM providers can hand this to their preferred model and parse the
    reply via the LLMRankerFn contract.

    The prompt asks for a JSON array of ids, in order, best first. Kept
    minimal — instruction-tuned models follow this reliably."""
    lines = [
        f"Query: {query}",
        "",
        "Rank the following candidates by how directly they answer the query.",
        "Return ONLY a JSON array of node_id strings, best first, no prose.",
        "",
        "Candidates:",
    ]
    for c in candidates:
        nid = str(getattr(c, "node_id", None) or "")
        text = TokenOverlapReranker._text_of(c).strip().replace("\n", " ")
        if len(text) > 280:
            text = text[:277] + "..."
        lines.append(f"- {nid}: {text}")
    return "\n".join(lines)
