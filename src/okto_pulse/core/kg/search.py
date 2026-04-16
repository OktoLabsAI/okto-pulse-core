"""HNSW-backed similarity search over per-board Kùzu graphs.

Thin wrapper over `CALL QUERY_VECTOR_INDEX` that:
1. Loads the VECTOR extension lazily on each connection (no-op if loaded)
2. Runs k-NN against the configured HNSW index per node type
3. Converts cosine distance to similarity (1 - distance) and filters by
   a minimum threshold before returning

The query operates on a single node type at a time because Kùzu HNSW indexes
are per-table. Callers that need to search across multiple types should iterate
and merge the results — or call `find_similar_across_types` which does that.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from okto_pulse.core.kg.reconciliation import ExistingNodeSummary
from okto_pulse.core.kg.schema import (
    VECTOR_INDEX_TYPES,
    open_board_connection,
    vector_index_name,
)

logger = logging.getLogger("okto_pulse.kg.search")


@dataclass
class SimilarNodeRaw:
    """Raw k-NN result before conversion to reconciliation summary."""

    kuzu_node_id: str
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
) -> list[SimilarNodeRaw]:
    """Run k-NN against one node type's HNSW index.

    Returns at most `top_k` results, filtered by `min_similarity`. Empty list
    on any error (index missing, no rows, etc.) — callers should default to
    ADD when no matches come back.
    """
    if node_type not in VECTOR_INDEX_TYPES:
        return []

    results: list[SimilarNodeRaw] = []
    try:
        db, conn = open_board_connection(board_id)
        try:
            idx = vector_index_name(node_type)
            # Kùzu 0.11 positional signature: (table, idx, vec, k).
            # RETURN is via node.* projection.
            cypher = (
                f"CALL QUERY_VECTOR_INDEX("
                f"'{node_type}', '{idx}', $vec, $k) "
                f"RETURN node.id, node.title, node.source_artifact_ref, distance"
            )
            result = conn.execute(cypher, {"vec": query_vector, "k": top_k})
            while result.has_next():
                row = result.get_next()
                raw = SimilarNodeRaw(
                    kuzu_node_id=row[0],
                    node_type=node_type,
                    title=row[1],
                    source_artifact_ref=row[2] if len(row) > 2 else None,
                    distance=float(row[3] if len(row) > 3 else row[-1]),
                )
                if raw.similarity >= min_similarity:
                    results.append(raw)
        finally:
            del conn
            del db
    except Exception as exc:
        logger.debug(
            "kg.search.vector_query_failed board=%s type=%s err=%s",
            board_id, node_type, exc,
        )
        return []

    # Kùzu returns ordered by distance ascending; our SimilarNodeRaw exposes
    # similarity as 1-distance so we sort descending by similarity.
    results.sort(key=lambda r: r.similarity, reverse=True)
    return results[:top_k]


def find_similar_for_candidate(
    board_id: str,
    node_type: str,
    query_vector: list[float],
    *,
    top_k: int = 5,
    min_similarity: float = 0.3,
) -> list[ExistingNodeSummary]:
    """Run vector search and return ExistingNodeSummary objects the
    reconciliation engine consumes directly."""
    raw = find_similar_nodes_by_type(
        board_id=board_id,
        node_type=node_type,
        query_vector=query_vector,
        top_k=top_k,
        min_similarity=min_similarity,
    )
    return [
        ExistingNodeSummary(
            kuzu_node_id=r.kuzu_node_id,
            node_type=r.node_type,
            stable_id=r.source_artifact_ref or None,
            title=r.title,
            similarity=r.similarity,
        )
        for r in raw
    ]
