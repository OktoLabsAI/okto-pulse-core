"""KG Service — shared logic layer consumed by MCP tools and REST endpoints.

Responsibilities:
- ACL enforcement before any query (FR-9): check_board_access(user, board_id)
- Default filters (FR-2): validation_status, min_confidence, max_rows
- Schema version check via BoardMeta node
- Executes parametrized Cypher templates (never string interpolation)
- Returns typed dicts; callers (MCP/REST) wrap into Pydantic models

All public methods are sync because Kuzu's Python API is synchronous. The
MCP/REST adapters call them from async handlers via run_in_executor when
needed for high-concurrency workloads (MVP: direct call is fine since Kuzu
is embedded and single-writer).
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.kg import cypher_templates as tpl
from okto_pulse.core.kg.schema import (
    SCHEMA_VERSION,
    open_board_connection,
    vector_index_name,
)

logger = logging.getLogger("okto_pulse.kg.service")


@dataclass(frozen=True)
class DefaultFilters:
    min_confidence: float = 0.5
    max_rows: int = 100
    validation_status_exclude: str = "unvalidated"


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
        db, conn = open_board_connection(board_id)
        try:
            r = conn.execute(
                "MATCH (m:BoardMeta {board_id: $b}) RETURN m.schema_version",
                {"b": board_id},
            )
            if r.has_next():
                return r.get_next()[0]
            return None
        finally:
            del conn, db

    def check_schema_version(self, board_id: str) -> None:
        ver = self.get_schema_version(board_id)
        if ver and ver != SCHEMA_VERSION:
            raise KGToolError(
                code="schema_drift",
                message=f"Board schema {ver} != expected {SCHEMA_VERSION}",
                details={"board_version": ver, "expected": SCHEMA_VERSION},
            )

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def _params(
        self,
        extra: dict[str, Any],
        *,
        min_confidence: float | None = None,
        max_rows: int | None = None,
    ) -> dict[str, Any]:
        """Merge default filters into a params dict."""
        params = dict(extra)
        params.setdefault("min_confidence", min_confidence or self.defaults.min_confidence)
        params.setdefault("max_rows", max_rows or self.defaults.max_rows)
        return params

    def _exec(
        self,
        board_id: str,
        cypher: str,
        params: dict[str, Any],
        *,
        tool_name: str = "",
        use_cache: bool = True,
    ) -> list[list]:
        """Execute a Cypher query with optional read-through cache."""
        import time as _time

        from okto_pulse.core.kg.cache import emit_tool_metrics
        from okto_pulse.core.kg.interfaces.registry import get_kg_registry

        cache = get_kg_registry().cache_backend
        cache_params = {k: v for k, v in params.items() if k != "max_rows"}
        t0 = _time.monotonic()

        if use_cache and tool_name:
            hit, cached = cache.get(tool_name, board_id, cache_params)
            if hit:
                dur = (_time.monotonic() - t0) * 1000
                emit_tool_metrics(
                    tool_name=tool_name, board_id=board_id,
                    cache_hit=True, duration_ms=dur,
                    result_count=len(cached),
                )
                return cached

        db, conn = open_board_connection(board_id)
        try:
            result = conn.execute(cypher, params)
            rows = []
            while result.has_next():
                rows.append(result.get_next())
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
                message=f"Cypher query failed: {exc}",
                details={"cypher": cypher[:200]},
            ) from exc
        finally:
            del conn, db

        if use_cache and tool_name:
            cache.put(tool_name, board_id, cache_params, rows)

        dur = (_time.monotonic() - t0) * 1000
        if tool_name:
            emit_tool_metrics(
                tool_name=tool_name, board_id=board_id,
                cache_hit=False, duration_ms=dur,
                result_count=len(rows),
            )
        return rows

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
        params = self._params(
            {"topic": topic},
            min_confidence=min_confidence,
            max_rows=max_rows,
        )
        rows = self._exec(board_id, tpl.GET_DECISION_HISTORY, params, tool_name="get_decision_history")
        return [
            {
                "id": r[0], "title": r[1], "content": r[2],
                "created_at": r[3], "source_confidence": r[4],
                "validation_status": r[5], "superseded_by": r[6],
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
        params = self._params(
            {"artifact_id": artifact_id},
            min_confidence=min_confidence,
            max_rows=max_rows,
        )
        rows = self._exec(board_id, tpl.GET_RELATED_CONTEXT, params, tool_name="get_related_context")
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
        chain: list[dict] = []
        current_id = decision_id
        visited: set[str] = set()
        for _ in range(10):  # max depth safety
            rows = self._exec(
                board_id, tpl.GET_SUPERSEDENCE_CHAIN,
                {"decision_id": current_id},
            )
            if not rows:
                break
            next_node = {
                "id": rows[0][0], "title": rows[0][1],
                "created_at": rows[0][2], "superseded_by": rows[0][3],
                "superseded_at": rows[0][4],
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
        limit = max_rows or min(50, self.defaults.max_rows)
        if node_id:
            rows = self._exec(
                board_id, tpl.FIND_CONTRADICTIONS_BY_NODE,
                {"node_id": node_id, "max_rows": limit},
            )
        else:
            rows = self._exec(
                board_id, tpl.FIND_CONTRADICTIONS_ALL,
                {"max_rows": limit},
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
        from okto_pulse.core.kg.search import find_similar_nodes_by_type

        w = weights or self.weights
        embedder = get_kg_registry().embedding_provider
        query_vec = embedder.encode(topic)

        raw = find_similar_nodes_by_type(
            board_id=board_id,
            node_type="Decision",
            query_vector=query_vec,
            top_k=top_k * 2,  # fetch extra for re-ranking
            min_similarity=min_similarity,
        )

        now = datetime.now(timezone.utc)
        results = []
        for r in raw:
            # Simplified recency_decay: exp(-age_days/30)
            recency = 0.5  # default when we can't compute age
            semantic = r.similarity
            confidence = 0.5  # placeholder until we fetch from node

            combined = (
                w.semantic * semantic
                + w.graph_centrality * 0.5  # in-degree placeholder
                + w.recency_decay * recency
                + w.confidence * confidence
            )
            results.append({
                "id": r.kuzu_node_id,
                "title": r.title,
                "source_artifact_ref": r.source_artifact_ref,
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
        params = {"constraint_id": constraint_id}
        rows = self._exec(board_id, tpl.EXPLAIN_CONSTRAINT, params, tool_name="explain_constraint", use_cache=False)
        if not rows:
            raise KGToolError(
                code="not_found",
                message=f"Constraint not found: {constraint_id}",
            )
        r = rows[0]
        # Fetch origins and violations via separate queries (Kuzu COLLECT
        # over OPTIONAL MATCH is fragile with map projections).
        origin_rows = self._exec(board_id, tpl.EXPLAIN_CONSTRAINT_ORIGINS, params, tool_name="explain_constraint", use_cache=False)
        violation_rows = self._exec(board_id, tpl.EXPLAIN_CONSTRAINT_VIOLATIONS, params, tool_name="explain_constraint", use_cache=False)
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
        params = {
            "decision_id": decision_id,
            "max_rows": max_rows or self.defaults.max_rows,
        }
        rows = self._exec(board_id, tpl.LIST_ALTERNATIVES, params, tool_name="list_alternatives")
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
        params = self._params(
            {"area": area},
            min_confidence=min_confidence,
            max_rows=max_rows,
        )
        rows = self._exec(board_id, tpl.GET_LEARNING_FROM_BUGS, params, tool_name="get_learning_from_bugs")
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
    ) -> list[dict]:
        """Cross-board discovery via the global Kuzu meta-graph.

        MVP: delegates to find_similar_decisions on each accessible board
        and merges results. Production: queries ~/.okto-pulse/global/discovery.kuzu
        directly (Global Discovery sprint).
        """
        from okto_pulse.core.kg.interfaces.registry import get_kg_registry

        if not user_boards:
            return []

        embedder = get_kg_registry().embedding_provider
        query_vec = embedder.encode(nl_query)
        all_results: list[dict] = []

        from okto_pulse.core.kg.search import find_similar_nodes_by_type
        from okto_pulse.core.kg.schema import board_kuzu_path

        for bid in user_boards:
            if not board_kuzu_path(bid).exists():
                continue
            raw = find_similar_nodes_by_type(
                board_id=bid,
                node_type="Decision",
                query_vector=query_vec,
                top_k=top_k,
                min_similarity=0.3,
            )
            for r in raw:
                all_results.append({
                    "board_id": bid,
                    "id": r.kuzu_node_id,
                    "title": r.title,
                    "similarity": r.similarity,
                })

        all_results.sort(key=lambda x: x["similarity"], reverse=True)
        return all_results[:top_k]


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
