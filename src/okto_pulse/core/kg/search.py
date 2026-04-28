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


def _cosine_similarity(vec1: list[float], vec2: list[float]) -> float:
    """Calculate cosine similarity between two vectors.

    Returns 1.0 for identical vectors, 0.0 for orthogonal, -1.0 for opposite.
    Vectors must have the same dimension.
    """
    if len(vec1) != len(vec2):
        return 0.0

    # Calculate dot product
    dot_product = sum(a * b for a, b in zip(vec1, vec2))

    # Calculate magnitudes
    magnitude1 = sum(a * a for a in vec1) ** 0.5
    magnitude2 = sum(b * b for b in vec2) ** 0.5

    if magnitude1 == 0.0 or magnitude2 == 0.0:
        return 0.0

    # Cosine similarity
    return dot_product / (magnitude1 * magnitude2)


def _fallback_manual_similarity_search(
    board_id: str,
    node_type: str,
    query_vector: list[float],
    *,
    top_k: int = 5,
    min_similarity: float = 0.0,
    conn=None,
) -> list[SimilarNodeRaw]:
    """Fallback: manual cosine similarity calculation when QUERY_VECTOR_INDEX fails.

    Fetches all nodes with embeddings and calculates similarity manually.
    This is slower but works around Kùzu's vector search issues.
    """
    results: list[SimilarNodeRaw] = []
    own_conn = False
    try:
        if conn is None:
            conn = open_board_connection(board_id)
            own_conn = True
            with conn as (_db, conn):
                cypher = (
                    f"MATCH (n:{node_type}) "
                    f"WHERE n.embedding IS NOT NULL "
                    f"RETURN n.id, n.title, n.source_artifact_ref, n.embedding "
                    f"LIMIT 500"
                )
                result = conn.execute(cypher)
                while result.has_next():
                    row = result.get_next()
                    node_id = row[0]
                    title = row[1]
                    source_ref = row[2] if len(row) > 2 else None
                    embedding = row[3] if len(row) > 3 else None

                    if embedding and len(embedding) == len(query_vector):
                        similarity = _cosine_similarity(query_vector, embedding)
                        if similarity >= min_similarity:
                            results.append(SimilarNodeRaw(
                                kuzu_node_id=node_id,
                                node_type=node_type,
                                title=title,
                                source_artifact_ref=source_ref,
                                distance=1.0 - similarity,
                            ))
        else:
            cypher = (
                f"MATCH (n:{node_type}) "
                f"WHERE n.embedding IS NOT NULL "
                f"RETURN n.id, n.title, n.source_artifact_ref, n.embedding "
                f"LIMIT 500"
            )
            result = conn.execute(cypher)
            while result.has_next():
                row = result.get_next()
                node_id = row[0]
                title = row[1]
                source_ref = row[2] if len(row) > 2 else None
                embedding = row[3] if len(row) > 3 else None

                if embedding and len(embedding) == len(query_vector):
                    similarity = _cosine_similarity(query_vector, embedding)
                    if similarity >= min_similarity:
                        results.append(SimilarNodeRaw(
                            kuzu_node_id=node_id,
                            node_type=node_type,
                            title=title,
                            source_artifact_ref=source_ref,
                            distance=1.0 - similarity,
                        ))
    except Exception as exc:
        logger.warning(
            "kg.search.fallback_failed board=%s type=%s err=%s",
            board_id, node_type, exc,
        )
        return []
    finally:
        if own_conn and conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    # Sort by similarity descending
    results.sort(key=lambda r: r.similarity, reverse=True)
    return results[:top_k]


def find_similar_nodes_by_type(
    board_id: str,
    node_type: str,
    query_vector: list[float],
    *,
    top_k: int = 5,
    min_similarity: float = 0.0,
    conn=None,
) -> list[SimilarNodeRaw]:
    """Run k-NN against one node type's HNSW index with fallback to manual calculation.

    Returns at most `top_k` results, filtered by `min_similarity`. Empty list
    on any error (index missing, no rows, etc.) — callers should default to
    ADD when no matches come back.

    When ``conn`` is provided, it is reused instead of opening a new Kùzu
    connection (caller owns the connection lifecycle).  When ``conn`` is
    ``None`` (default), a fresh connection is opened and closed per call.
    """
    if node_type not in VECTOR_INDEX_TYPES:
        return []

    results: list[SimilarNodeRaw] = []
    own_conn = False
    try:
        if conn is None:
            conn = open_board_connection(board_id)
            own_conn = True
            with conn as (_db, conn):
                idx = vector_index_name(node_type)
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
        else:
            idx = vector_index_name(node_type)
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
    except Exception as exc:
        logger.debug(
            "kg.search.vector_query_failed board=%s type=%s err=%s",
            board_id, node_type, exc,
        )
        return []
    finally:
        if own_conn and conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    # If QUERY_VECTOR_INDEX returned no results, fall back to manual calculation
    if not results:
        logger.info(
            "kg.search.vector_index_empty board=%s type=%s using_fallback",
            board_id, node_type,
        )
        # Bug fix (kg.search.fallback_failed: Connection is closed): when
        # we own the connection it has already been released by the
        # ``with conn as (_db, conn):`` block above (BoardConnection.close
        # ran __exit__). Passing the dead handle to the fallback raises
        # "Connection is closed" the moment it tries .execute(). Force
        # the fallback to open its own fresh connection by passing None
        # whenever we owned it; only forward externally-managed conns.
        return _fallback_manual_similarity_search(
            board_id=board_id,
            node_type=node_type,
            query_vector=query_vector,
            top_k=top_k,
            min_similarity=min_similarity,
            conn=None if own_conn else conn,
        )

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
    conn=None,
) -> list[ExistingNodeSummary]:
    """Run vector search and return ExistingNodeSummary objects the
    reconciliation engine consumes directly.

    When ``conn`` is provided, it is passed through to the underlying
    similarity search so the caller can reuse a single Kùzu connection
    across multiple candidates.
    """
    raw = find_similar_nodes_by_type(
        board_id=board_id,
        node_type=node_type,
        query_vector=query_vector,
        top_k=top_k,
        min_similarity=min_similarity,
        conn=conn,
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
