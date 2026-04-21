"""Reranker Protocol — second-stage ranking over the top-K hybrid search output.

Ideação 3070cd53. The hybrid_search linear blend (vector_sim +
graph_proximity + edge_confidence + recency_decay) is fast and well-suited
for the fast-retrieval first stage, but it does not model fine-grained
semantic relevance between the query and each candidate's text. A reranker
re-scores only the top-K (typically 20-50) using a heavier, more precise
model, then returns a reordered top-N (typically 10).

Implementations satisfy this Protocol via duck typing (PEP 544).
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable


@runtime_checkable
class RerankerCandidate(Protocol):
    """Duck-typed shape of a candidate passed to the reranker. Matches
    `hybrid_search.RankedNode` and any future row type that carries an
    identifier + a displayable text surface."""

    node_id: str
    title: str


@runtime_checkable
class Reranker(Protocol):
    """Given a query and a list of candidates, return them reordered by
    fine-grained relevance. Implementations MUST be deterministic for a
    given (query, candidates) tuple — tests rely on this for snapshots.

    The return list is a sorted *subset* of the input (top-N). The
    reranker never invents or drops candidates based on content; only
    explicit `top_n` truncation removes items.
    """

    name: str

    def rerank(
        self,
        query: str,
        candidates: Sequence[RerankerCandidate],
        *,
        top_n: int = 10,
    ) -> list[RerankerCandidate]:
        """Return the input candidates reordered by query relevance,
        truncated to ``top_n``. An empty input returns an empty list.
        """
        ...
