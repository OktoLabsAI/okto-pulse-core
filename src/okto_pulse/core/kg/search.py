"""Backend-neutral semantic similarity search for graph nodes."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from okto_pulse.core.kg.graph_availability import (
    graph_unavailable_error,
    is_graph_unavailable_error,
)
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.kg.reconciliation import ExistingNodeSummary
from okto_pulse.core.kg.schema_contract import VECTOR_INDEX_TYPES

logger = logging.getLogger("okto_pulse.kg.search")


@dataclass
class SimilarNodeRaw:
    """Raw k-NN result before conversion to reconciliation summary."""

    graph_node_id: str
    node_type: str
    title: str
    distance: float  # cosine distance, 0 = identical, 1 = orthogonal, 2 = opposite
    source_artifact_ref: str | None = None

    @property
    def similarity(self) -> float:
        """Convert cosine distance to a 0-1 similarity score (clamped)."""
        sim = 1.0 - self.distance
        return max(0.0, min(1.0, sim))


def find_similar_nodes_by_type(
    board_id: str,
    node_type: str,
    query_vector: list[float],
    *,
    top_k: int = 5,
    min_similarity: float = 0.0,
    include_superseded: bool = False,
) -> list[SimilarNodeRaw]:
    """Resolve semantic vector hits through the edition-owned graph adapter.

    The adapter owns indexed retrieval and its exhaustive fallback.
    """
    if node_type not in VECTOR_INDEX_TYPES:
        return []
    try:
        rows = get_kg_registry().graph_store.vector_search(
            board_id=board_id,
            node_type=node_type,
            query_vec=query_vector,
            top_k=top_k,
            min_similarity=min_similarity,
            include_superseded=include_superseded,
        )
        return [
            SimilarNodeRaw(
                graph_node_id=str(row["node_id"]),
                node_type=str(row.get("node_type") or node_type),
                title=str(row.get("title") or ""),
                source_artifact_ref=row.get("source_artifact_ref"),
                distance=1.0 - float(row["similarity"]),
            )
            for row in rows[:top_k]
        ]
    except Exception as exc:
        if is_graph_unavailable_error(exc):
            raise graph_unavailable_error(board_id) from exc
        logger.debug(
            "kg.search.semantic_query_failed board=%s type=%s err=%s",
            board_id, node_type, exc,
        )
        return []


def find_similar_for_candidate(
    board_id: str,
    node_type: str,
    query_vector: list[float],
    *,
    top_k: int = 5,
    min_similarity: float = 0.3,
) -> list[ExistingNodeSummary]:
    """Run vector search and return ExistingNodeSummary objects the
    reconciliation engine consumes directly.

    Connection reuse and driver details remain inside the graph adapter.
    """
    raw = find_similar_nodes_by_type(
        board_id=board_id,
        node_type=node_type,
        query_vector=query_vector,
        top_k=top_k,
        min_similarity=min_similarity,
    )
    return [
        ExistingNodeSummary(
            graph_node_id=r.graph_node_id,
            node_type=r.node_type,
            stable_id=r.source_artifact_ref or None,
            title=r.title,
            similarity=r.similarity,
        )
        for r in raw
    ]
