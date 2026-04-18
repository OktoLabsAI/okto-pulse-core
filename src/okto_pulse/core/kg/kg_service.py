"""KG Service — shared logic layer consumed by MCP tools and REST endpoints.

Responsibilities:
- ACL enforcement before any query (FR-9): check_board_access(user, board_id)
- Default filters (FR-2): validation_status, min_confidence, max_rows
- Schema version check via BoardMeta node
- Delegates to graph_store (SemanticGraphStore) via the provider registry
- Returns typed dicts; callers (MCP/REST) wrap into Pydantic models

All public methods are sync because Kuzu's Python API is synchronous. The
MCP/REST adapters call them from async handlers via run_in_executor when
needed for high-concurrency workloads (MVP: direct call is fine since Kuzu
is embedded and single-writer).
"""

from __future__ import annotations

import logging
import time as _time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.kg import cypher_templates as tpl
from okto_pulse.core.kg.interfaces.graph_store import QueryFilters
from okto_pulse.core.kg.schema import SCHEMA_VERSION

logger = logging.getLogger("okto_pulse.kg.service")


@dataclass(frozen=True)
class DefaultFilters:
    min_confidence: float = 0.5
    max_rows: int = 100
    # v0.3.0: validation_status_exclude retained as a no-op for call-site
    # compatibility; R3 replaces it with `min_relevance_score: float = 0.3`
    # once the scoring pipeline lands. Keep the attribute so existing
    # signatures (DefaultFilters(validation_status_exclude=...)) don't break
    # during the R1→R3 window.
    validation_status_exclude: str = ""


@dataclass(frozen=True)
class KGToolError(Exception):
    """Typed error for tier primario tools (FR-8)."""

    code: str  # not_found, permission_denied, invalid_param, kuzu_error, timeout, schema_drift, empty_result
    message: str
    details: dict = field(default_factory=dict)

    def __str__(self):
        return f"KGToolError({self.code}): {self.message}"


# Ranking weights (FR-5): configurable, defaults sum to 1.0.
@dataclass
class RankingWeights:
    semantic: float = 0.5
    graph_centrality: float = 0.2
    recency_decay: float = 0.2
    confidence: float = 0.1


def _get_graph_store():
    """Return the graph_store from the registry."""
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    store = get_kg_registry().graph_store
    if store is None:
        raise KGToolError(
            code="kuzu_error",
            message="graph_store not configured in KG registry",
        )
    return store


def _filters(
    min_confidence: float | None = None,
    max_rows: int | None = None,
    defaults: DefaultFilters | None = None,
) -> QueryFilters:
    """Build QueryFilters from optional overrides + service defaults."""
    d = defaults or DefaultFilters()
    return QueryFilters(
        min_confidence=min_confidence or d.min_confidence,
        max_rows=max_rows or d.max_rows,
    )


class KGService:
    """Stateless service layer. Instantiate per-request or share across calls."""

    def __init__(
        self,
        *,
        default_filters: DefaultFilters | None = None,
        ranking_weights: RankingWeights | None = None,
    ):
        self.defaults = default_filters or DefaultFilters()
        self.weights = ranking_weights or RankingWeights()

    # ------------------------------------------------------------------
    # ACL (FR-9)
    # ------------------------------------------------------------------

    def check_board_access(self, user_boards: list[str], board_id: str) -> None:
        """Raise KGToolError(permission_denied) if user doesn't have access."""
        if board_id not in user_boards:
            raise KGToolError(
                code="permission_denied",
                message=f"No access to board {board_id}",
            )

    # ------------------------------------------------------------------
    # Schema version (FR-6)
    # ------------------------------------------------------------------

    def get_schema_version(self, board_id: str) -> str | None:
        store = _get_graph_store()
        return store.get_schema_version(board_id)

    def check_schema_version(self, board_id: str) -> None:
        ver = self.get_schema_version(board_id)
        if ver and ver != SCHEMA_VERSION:
            raise KGToolError(
                code="schema_drift",
                message=f"Board schema {ver} != expected {SCHEMA_VERSION}",
                details={"board_version": ver, "expected": SCHEMA_VERSION},
            )

    # ------------------------------------------------------------------
    # Cache-aware query helper
    # ------------------------------------------------------------------

    def _cached_call(
        self,
        tool_name: str,
        board_id: str,
        cache_params: dict[str, Any],
        fn,
        *,
        use_cache: bool = True,
    ):
        """Execute fn() with optional read-through cache and metrics."""
        from okto_pulse.core.kg.cache import emit_tool_metrics
        from okto_pulse.core.kg.interfaces.registry import get_kg_registry

        cache = get_kg_registry().cache_backend
        t0 = _time.monotonic()

        if use_cache and tool_name:
            hit, cached = cache.get(tool_name, board_id, cache_params)
            if hit:
                dur = (_time.monotonic() - t0) * 1000
                emit_tool_metrics(
                    tool_name=tool_name, board_id=board_id,
                    cache_hit=True, duration_ms=dur,
                    result_count=len(cached) if isinstance(cached, list) else 1,
                )
                return cached

        try:
            result = fn()
        except Exception as exc:
            dur = (_time.monotonic() - t0) * 1000
            if tool_name:
                emit_tool_metrics(
                    tool_name=tool_name, board_id=board_id,
                    cache_hit=False, duration_ms=dur,
                    result_count=0, error_code="kuzu_error",
                )
            raise KGToolError(
                code="kuzu_error",
                message=f"Query failed: {exc}",
            ) from exc

        if use_cache and tool_name:
            cache.put(tool_name, board_id, cache_params, result)

        dur = (_time.monotonic() - t0) * 1000
        if tool_name:
            emit_tool_metrics(
                tool_name=tool_name, board_id=board_id,
                cache_hit=False, duration_ms=dur,
                result_count=len(result) if isinstance(result, list) else 1,
            )
        return result

    # ------------------------------------------------------------------
    # 0a. get_node_detail (visualization — any node type)
    # ------------------------------------------------------------------

    def get_node_detail(self, board_id: str, node_id: str) -> dict | None:
        """Fetch one node by id across any node type in the per-board graph.

        Tries each NODE_TYPES table in turn (Kùzu has no polymorphic MATCH).
        Returns the first hit with the shape expected by the KGNode frontend
        type; `None` when the id isn't present in any table.
        """
        from okto_pulse.core.kg.schema import NODE_TYPES, open_board_connection

        with open_board_connection(board_id) as (_db, conn):
            for ntype in NODE_TYPES:
                cypher = (
                    f"MATCH (n:{ntype} {{id: $nid}}) "
                    f"RETURN n.id, n.title, n.content, n.justification, "
                    f"n.source_artifact_ref, n.source_confidence, "
                    f"n.relevance_score, n.query_hits, n.last_queried_at, "
                    f"n.created_at, n.superseded_by"
                )
                try:
                    res = conn.execute(cypher, {"nid": node_id})
                except Exception:
                    continue
                if res.has_next():
                    r = res.get_next()
                    return {
                        "id": r[0],
                        "title": r[1] or "",
                        "content": r[2] or "",
                        "justification": r[3] or "",
                        "source_artifact_ref": r[4],
                        "source_confidence": r[5] if r[5] is not None else 0.0,
                        "relevance_score": r[6] if r[6] is not None else 0.5,
                        "query_hits": r[7] if r[7] is not None else 0,
                        "last_queried_at": r[8],
                        "created_at": r[9].isoformat() if r[9] else None,
                        "superseded_by": r[10],
                        "node_type": ntype,
                    }
        return None

    # ------------------------------------------------------------------
    # 0. get_all_nodes (visualization — all types)
    # ------------------------------------------------------------------

    def get_all_nodes(
        self,
        board_id: str,
        *,
        min_confidence: float = 0.0,
        max_rows: int | None = None,
        cursor: str | None = None,
    ) -> list[dict]:
        """Return nodes ordered ``(created_at DESC, id DESC)`` — Spec 8 / S1.3.

        When ``cursor`` is provided it must be a string produced by
        :func:`okto_pulse.core.api.kg_routes.encode_cursor`; the query then
        returns rows strictly "after" that cursor in the stable order.
        """
        from okto_pulse.core.kg.schema import open_board_connection

        f = _filters(min_confidence, max_rows, self.defaults)
        params: dict = {
            "min_confidence": f.min_confidence,
            "max_rows": f.max_rows,
        }
        if cursor:
            from okto_pulse.core.api.kg_routes import decode_cursor
            cursor_ts, cursor_id = decode_cursor(cursor)
            params["cursor_ts"] = cursor_ts
            params["cursor_id"] = cursor_id
            template = tpl.GET_ALL_NODES_AFTER_CURSOR
        else:
            template = tpl.GET_ALL_NODES

        def _query():
            with open_board_connection(board_id) as (_db, conn):
                result = conn.execute(template, params)
                rows = []
                while result.has_next():
                    rows.append(result.get_next())
                return rows

        rows = self._cached_call("get_all_nodes", board_id, params, _query)
        return [
            {
                "id": r[0], "node_type": r[1], "title": r[2], "content": r[3],
                "created_at": r[4], "source_confidence": r[5],
                "relevance_score": r[6] if r[6] is not None else 0.5,
                "source_artifact_ref": r[7],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # 1. get_decision_history (FR-11)
    # ------------------------------------------------------------------

    def get_decision_history(
        self,
        board_id: str,
        topic: str,
        *,
        min_confidence: float | None = None,
        max_rows: int | None = None,
    ) -> list[dict]:
        store = _get_graph_store()
        f = _filters(min_confidence, max_rows, self.defaults)

        rows = self._cached_call(
            "get_decision_history", board_id, {"topic": topic},
            lambda: store.find_by_topic(board_id, "Decision", topic, f),
        )
        return [
            {
                "id": r[0], "title": r[1], "content": r[2],
                "created_at": r[3], "source_confidence": r[4],
                "relevance_score": r[5] if r[5] is not None else 0.5,
                "superseded_by": r[6],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # 2. get_related_context (FR-12)
    # ------------------------------------------------------------------

    def get_related_context(
        self,
        board_id: str,
        artifact_id: str,
        *,
        min_confidence: float | None = None,
        max_rows: int | None = None,
    ) -> list[dict]:
        store = _get_graph_store()
        f = _filters(min_confidence, max_rows, self.defaults)

        rows = self._cached_call(
            "get_related_context", board_id, {"artifact_id": artifact_id},
            lambda: store.find_by_artifact(board_id, artifact_id, f),
        )
        return [
            {
                "center_id": r[0], "center_title": r[1],
                "hop1_id": r[2], "hop1_title": r[3],
                "hop2_id": r[4], "hop2_title": r[5],
                "rel1_type": r[6], "rel2_type": r[7],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # 3. get_supersedence_chain (FR-15)
    # ------------------------------------------------------------------

    def get_supersedence_chain(
        self,
        board_id: str,
        decision_id: str,
    ) -> dict:
        store = _get_graph_store()
        chain: list[dict] = []
        current_id = decision_id
        visited: set[str] = set()

        def _str(ts):
            """Kùzu returns TIMESTAMP as datetime; SupersedenceEntry (pydantic)
            expects ISO strings. Normalise here rather than leaking the raw
            datetime through the API boundary."""
            if ts is None:
                return None
            if hasattr(ts, "isoformat"):
                return ts.isoformat()
            return str(ts)

        for _ in range(10):  # max depth safety
            rows = store.traverse_supersedence(board_id, current_id)
            if not rows:
                break
            next_node = {
                "id": rows[0][0], "title": rows[0][1],
                "created_at": _str(rows[0][2]),
                "superseded_by": rows[0][3],
                "superseded_at": _str(rows[0][4]),
            }
            if next_node["id"] in visited:
                break  # cycle guard
            visited.add(next_node["id"])
            chain.append(next_node)
            current_id = next_node["id"]
        return {
            "chain": chain,
            "depth": len(chain),
            "current_active": decision_id,
        }

    # ------------------------------------------------------------------
    # 4. find_contradictions (FR-14)
    # ------------------------------------------------------------------

    def find_contradictions(
        self,
        board_id: str,
        node_id: str | None = None,
        *,
        max_rows: int | None = None,
    ) -> list[dict]:
        store = _get_graph_store()
        limit = max_rows or min(50, self.defaults.max_rows)

        rows = self._cached_call(
            "find_contradictions", board_id, {"node_id": node_id},
            lambda: store.find_contradictions(board_id, node_id, limit),
        )
        return [
            {
                "id_a": r[0], "title_a": r[1],
                "id_b": r[2], "title_b": r[3],
                "confidence": r[4],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # 5. find_similar_decisions (FR-13) — HNSW + ranking
    # ------------------------------------------------------------------

    def find_similar_decisions(
        self,
        board_id: str,
        topic: str,
        *,
        top_k: int = 10,
        min_similarity: float = 0.3,
        weights: RankingWeights | None = None,
    ) -> list[dict]:
        from okto_pulse.core.kg.interfaces.registry import get_kg_registry

        w = weights or self.weights
        store = _get_graph_store()
        embedder = get_kg_registry().embedding_provider
        query_vec = embedder.encode(topic)

        raw = store.vector_search(
            board_id=board_id,
            node_type="Decision",
            query_vec=query_vec,
            top_k=top_k * 2,  # fetch extra for re-ranking
            min_similarity=min_similarity,
        )

        results = []
        for r in raw:
            semantic = r["similarity"]
            recency = 0.5  # default when we can't compute age
            confidence = 0.5  # placeholder until we fetch from node

            combined = (
                w.semantic * semantic
                + w.graph_centrality * 0.5  # in-degree placeholder
                + w.recency_decay * recency
                + w.confidence * confidence
            )
            results.append({
                "id": r["node_id"],
                "title": r["title"],
                "source_artifact_ref": r.get("source_artifact_ref"),
                "similarity": semantic,
                "combined_score": round(combined, 4),
            })

        results.sort(key=lambda x: x["combined_score"], reverse=True)
        return results[:top_k]

    # ------------------------------------------------------------------
    # 6. explain_constraint (FR-16)
    # ------------------------------------------------------------------

    def explain_constraint(
        self,
        board_id: str,
        constraint_id: str,
    ) -> dict:
        store = _get_graph_store()
        main, origin_rows, violation_rows = store.get_constraint_detail(
            board_id, constraint_id
        )
        if not main:
            raise KGToolError(
                code="not_found",
                message=f"Constraint not found: {constraint_id}",
            )
        r = main[0]
        return {
            "id": r[0], "title": r[1], "content": r[2],
            "justification": r[3], "source_artifact_ref": r[4],
            "source_confidence": r[5],
            "origins": [{"id": o[0], "title": o[1]} for o in origin_rows],
            "violations": [{"id": v[0], "title": v[1]} for v in violation_rows],
        }

    # ------------------------------------------------------------------
    # 7. list_alternatives (FR-17)
    # ------------------------------------------------------------------

    def list_alternatives(
        self,
        board_id: str,
        decision_id: str,
        *,
        max_rows: int | None = None,
    ) -> list[dict]:
        store = _get_graph_store()
        limit = max_rows or self.defaults.max_rows

        rows = self._cached_call(
            "list_alternatives", board_id, {"decision_id": decision_id},
            lambda: store.get_alternatives(board_id, decision_id, limit),
        )
        return [
            {
                "id": r[0], "title": r[1], "content": r[2],
                "justification": r[3], "source_confidence": r[4],
                "source_artifact_ref": r[5],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # 8. get_learning_from_bugs (FR-18)
    # ------------------------------------------------------------------

    def get_learning_from_bugs(
        self,
        board_id: str,
        area: str,
        *,
        min_confidence: float | None = None,
        max_rows: int | None = None,
    ) -> list[dict]:
        store = _get_graph_store()
        f = _filters(min_confidence, max_rows, self.defaults)

        rows = self._cached_call(
            "get_learning_from_bugs", board_id, {"area": area},
            lambda: store.get_learnings_for_area(board_id, area, f),
        )
        return [
            {
                "learning_id": r[0], "learning_title": r[1],
                "learning_content": r[2], "justification": r[3],
                "source_confidence": r[4],
                "bug_id": r[5], "bug_title": r[6],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # 9. query_global — delegates to global discovery layer
    # ------------------------------------------------------------------

    def query_global(
        self,
        nl_query: str,
        *,
        user_boards: list[str] | None = None,
        top_k: int = 10,
        min_similarity: float = 0.3,
    ) -> list[dict]:
        """Cross-board discovery via the global discovery meta-graph.

        Queries ~/.okto-pulse/global/discovery.kuzu directly — HNSW over
        DecisionDigest.embedding, scoped to the caller's boards via the
        CONTAINS_DECISION edge. Falls back to manual cosine when the HNSW
        index is empty (same failure mode as per-board search).
        """
        from okto_pulse.core.kg.interfaces.registry import get_kg_registry
        from okto_pulse.core.kg.global_discovery.schema import open_global_connection

        if not user_boards:
            return []

        embedder = get_kg_registry().embedding_provider
        query_vec = embedder.encode(nl_query)
        scope = list(user_boards)

        results: list[dict] = []
        try:
            _, conn = open_global_connection()
            try:
                # HNSW over DecisionDigest.embedding, joined to Board via
                # CONTAINS_DECISION so we can filter to the caller's scope.
                cypher = (
                    "CALL QUERY_VECTOR_INDEX("
                    "'DecisionDigest', 'digest_embedding_idx', $vec, $k) "
                    "WITH node, distance "
                    "MATCH (b:Board)-[:CONTAINS_DECISION]->(node) "
                    "WHERE b.board_id IN $boards "
                    "RETURN b.board_id, node.id, node.original_node_id, "
                    "node.title, node.one_line_summary, node.node_type, distance "
                    "ORDER BY distance ASC LIMIT $k"
                )
                res = conn.execute(
                    cypher,
                    {"vec": query_vec, "k": top_k, "boards": scope},
                )
                while res.has_next():
                    row = res.get_next()
                    dist = float(row[6])
                    sim = max(0.0, min(1.0, 1.0 - dist))
                    if sim < min_similarity:
                        continue
                    results.append({
                        "board_id": row[0],
                        "digest_id": row[1],
                        "id": row[2],
                        "title": row[3],
                        "summary": row[4],
                        "node_type": row[5],
                        "similarity": sim,
                    })
            finally:
                del conn
        except Exception as exc:
            logger.debug("kg.query_global.failed err=%s", exc)
            return []

        if results:
            return results[:top_k]

        # Fallback: linear scan over DecisionDigest if HNSW returned nothing
        # (index empty or not yet populated by the outbox worker). Mirrors
        # the per-board fallback in search.py so global stays usable while
        # the meta-graph is still warming up.
        try:
            _, conn = open_global_connection()
            try:
                cypher = (
                    "MATCH (b:Board)-[:CONTAINS_DECISION]->(d:DecisionDigest) "
                    "WHERE b.board_id IN $boards AND d.embedding IS NOT NULL "
                    "RETURN b.board_id, d.id, d.original_node_id, d.title, "
                    "d.one_line_summary, d.node_type, d.embedding LIMIT 500"
                )
                res = conn.execute(cypher, {"boards": scope})
                scored: list[dict] = []
                qv = query_vec
                qnorm = sum(x * x for x in qv) ** 0.5 or 1.0
                while res.has_next():
                    row = res.get_next()
                    emb = row[6]
                    if not emb or len(emb) != len(qv):
                        continue
                    dot = sum(a * b for a, b in zip(qv, emb))
                    enorm = sum(x * x for x in emb) ** 0.5 or 1.0
                    sim = max(0.0, min(1.0, dot / (qnorm * enorm)))
                    if sim < min_similarity:
                        continue
                    scored.append({
                        "board_id": row[0],
                        "digest_id": row[1],
                        "id": row[2],
                        "title": row[3],
                        "summary": row[4],
                        "node_type": row[5],
                        "similarity": sim,
                    })
                scored.sort(key=lambda r: r["similarity"], reverse=True)
                return scored[:top_k]
            finally:
                del conn
        except Exception as exc:
            logger.debug("kg.query_global.fallback_failed err=%s", exc)
            return []


# Module-level default instance.
_default_service: KGService | None = None


def get_kg_service() -> KGService:
    global _default_service
    if _default_service is None:
        _default_service = KGService()
    return _default_service


def reset_kg_service_for_tests() -> None:
    global _default_service
    _default_service = None
