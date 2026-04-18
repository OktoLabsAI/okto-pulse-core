"""KuzuGraphStore — satisfies SemanticGraphStore Protocol for embedded Kuzu.

Delegates all graph operations to per-board Kuzu databases via
`open_board_connection()` and parametrized Cypher templates. This is the
production default for single-node / community deployments.
"""

from __future__ import annotations

import logging
from typing import Any

from okto_pulse.core.kg.interfaces.graph_store import QueryFilters
from okto_pulse.core.kg.schema import (
    NODE_TYPES,
    REL_TYPES,
    SCHEMA_VERSION,
    VECTOR_INDEX_TYPES,
    bootstrap_board_graph,
    open_board_connection,
    vector_index_name,
)
from okto_pulse.core.kg import cypher_templates as tpl

logger = logging.getLogger("okto_pulse.kg.kuzu_graph_store")


class KuzuGraphStore:
    """Embedded Kuzu implementation of SemanticGraphStore."""

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def bootstrap(self, board_id: str) -> None:
        bootstrap_board_graph(board_id)

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def create_node(
        self, board_id: str, node_type: str, node_id: str, attrs: dict[str, Any]
    ) -> None:
        with open_board_connection(board_id) as (_db, conn):
            params = dict(attrs)
            params["id"] = node_id
            columns = ", ".join(f"{k}: ${k}" for k in params)
            conn.execute(f"CREATE (n:{node_type} {{{columns}}})", params)

    def create_edge(
        self, board_id: str, edge_type: str, from_id: str, to_id: str,
        attrs: dict[str, Any] | None = None,
    ) -> None:
        rel_row = next((r for r in REL_TYPES if r[0] == edge_type), None)
        if rel_row is None:
            raise ValueError(f"unknown edge_type: {edge_type}")
        _, from_type, to_type = rel_row

        edge_attrs = dict(attrs or {})
        edge_attrs.setdefault("confidence", 0.7)
        # v0.2.0 provenance defaults. The store can't tell cognitive vs
        # deterministic writes; callers MUST override `layer` explicitly for
        # Layer 1 emissions. If missing, we tag as "cognitive" so the
        # layer_isolation gate still lights up on orphan writes.
        edge_attrs.setdefault("layer", "cognitive")
        edge_attrs.setdefault("rule_id", "")
        edge_attrs.setdefault("created_by", edge_attrs.get("created_by_session_id", ""))
        edge_attrs.setdefault("fallback_reason", "")

        attr_cols = ", ".join(f"{k}: ${k}" for k in edge_attrs)
        stmt = (
            f"MATCH (a:{from_type} {{id: $from_id}}), "
            f"(b:{to_type} {{id: $to_id}}) "
            f"CREATE (a)-[r:{edge_type} {{{attr_cols}}}]->(b)"
        )
        params = dict(edge_attrs)
        params["from_id"] = from_id
        params["to_id"] = to_id

        with open_board_connection(board_id) as (_db, conn):
            conn.execute(stmt, params)

    def delete_nodes_by_session(self, board_id: str, session_id: str) -> int:
        count = 0
        with open_board_connection(board_id) as (_db, conn):
            for node_type in NODE_TYPES:
                try:
                    result = conn.execute(
                        f"MATCH (n:{node_type}) "
                        f"WHERE n.source_session_id = $sid "
                        f"RETURN count(n)",
                        {"sid": session_id},
                    )
                    if result.has_next():
                        count += result.get_next()[0]
                    conn.execute(
                        f"MATCH (n:{node_type}) "
                        f"WHERE n.source_session_id = $sid DETACH DELETE n",
                        {"sid": session_id},
                    )
                except Exception as exc:
                    logger.warning(
                        "delete_nodes_by_session failed type=%s err=%s",
                        node_type, exc,
                    )
        return count

    def delete_edges_by_session(self, board_id: str, session_id: str) -> int:
        count = 0
        with open_board_connection(board_id) as (_db, conn):
            for rel_name, from_type, to_type in REL_TYPES:
                try:
                    result = conn.execute(
                        f"MATCH (a:{from_type})-[r:{rel_name}]->(b:{to_type}) "
                        f"WHERE r.created_by_session_id = $sid "
                        f"RETURN count(r)",
                        {"sid": session_id},
                    )
                    if result.has_next():
                        count += result.get_next()[0]
                    conn.execute(
                        f"MATCH (a:{from_type})-[r:{rel_name}]->(b:{to_type}) "
                        f"WHERE r.created_by_session_id = $sid DELETE r",
                        {"sid": session_id},
                    )
                except Exception as exc:
                    logger.warning(
                        "delete_edges_by_session failed rel=%s err=%s",
                        rel_name, exc,
                    )
        return count

    # ------------------------------------------------------------------
    # Read operations (tier primario)
    # ------------------------------------------------------------------

    def _exec(self, board_id: str, cypher: str, params: dict[str, Any]) -> list[list]:
        with open_board_connection(board_id) as (_db, conn):
            result = conn.execute(cypher, params)
            rows = []
            while result.has_next():
                rows.append(result.get_next())
            return rows

    def find_by_topic(
        self, board_id: str, node_type: str, topic: str, filters: QueryFilters
    ) -> list[list]:
        cypher = (
            f"MATCH (n:{node_type}) "
            f"WHERE n.title CONTAINS $topic "
            f"AND n.validation_status <> 'unvalidated' "
            f"AND n.source_confidence >= $min_confidence "
            f"RETURN n.id, n.title, n.content, n.created_at, n.source_confidence, "
            f"n.validation_status, n.superseded_by "
            f"ORDER BY n.created_at DESC "
            f"LIMIT $max_rows"
        )
        return self._exec(board_id, cypher, {
            "topic": topic,
            "min_confidence": filters.min_confidence,
            "max_rows": filters.max_rows,
        })

    def find_by_artifact(
        self, board_id: str, artifact_id: str, filters: QueryFilters
    ) -> list[list]:
        return self._exec(board_id, tpl.GET_RELATED_CONTEXT, {
            "artifact_id": artifact_id,
            "min_confidence": filters.min_confidence,
            "max_rows": filters.max_rows,
        })

    def traverse_supersedence(
        self, board_id: str, decision_id: str, max_depth: int = 10
    ) -> list[list]:
        return self._exec(board_id, tpl.GET_SUPERSEDENCE_CHAIN, {
            "decision_id": decision_id,
        })

    def find_contradictions(
        self, board_id: str, node_id: str | None, limit: int
    ) -> list[list]:
        if node_id:
            return self._exec(board_id, tpl.FIND_CONTRADICTIONS_BY_NODE, {
                "node_id": node_id,
                "max_rows": limit,
            })
        return self._exec(board_id, tpl.FIND_CONTRADICTIONS_ALL, {
            "max_rows": limit,
        })

    def vector_search(
        self, board_id: str, node_type: str, query_vec: list[float],
        top_k: int, min_similarity: float,
    ) -> list[dict]:
        from okto_pulse.core.kg.search import find_similar_nodes_by_type

        raw = find_similar_nodes_by_type(
            board_id=board_id,
            node_type=node_type,
            query_vector=query_vec,
            top_k=top_k,
            min_similarity=min_similarity,
        )
        return [
            {
                "node_id": r.kuzu_node_id,
                "node_type": r.node_type,
                "title": r.title,
                "source_artifact_ref": r.source_artifact_ref,
                "similarity": r.similarity,
            }
            for r in raw
        ]

    def get_constraint_detail(
        self, board_id: str, constraint_id: str
    ) -> tuple[list[list], list[list], list[list]]:
        params = {"constraint_id": constraint_id}
        main = self._exec(board_id, tpl.EXPLAIN_CONSTRAINT, params)
        origins = self._exec(board_id, tpl.EXPLAIN_CONSTRAINT_ORIGINS, params)
        violations = self._exec(board_id, tpl.EXPLAIN_CONSTRAINT_VIOLATIONS, params)
        return main, origins, violations

    def get_alternatives(
        self, board_id: str, decision_id: str, limit: int
    ) -> list[list]:
        return self._exec(board_id, tpl.LIST_ALTERNATIVES, {
            "decision_id": decision_id,
            "max_rows": limit,
        })

    def get_learnings_for_area(
        self, board_id: str, area: str, filters: QueryFilters
    ) -> list[list]:
        return self._exec(board_id, tpl.GET_LEARNING_FROM_BUGS, {
            "area": area,
            "min_confidence": filters.min_confidence,
            "max_rows": filters.max_rows,
        })

    def get_schema_version(self, board_id: str) -> str | None:
        rows = self._exec(
            board_id,
            "MATCH (m:BoardMeta {board_id: $b}) RETURN m.schema_version",
            {"b": board_id},
        )
        if rows:
            return rows[0][0]
        return None

    def get_schema_info(self, board_id: str, *, include_internal: bool = False) -> dict:
        result: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "stable_node_types": [{"name": nt, "stable": True} for nt in NODE_TYPES],
            "stable_rel_types": [
                {"name": rt[0], "from": rt[1], "to": rt[2]} for rt in REL_TYPES
            ],
            "vector_indexes": [
                {
                    "node_type": nt,
                    "attribute": "embedding",
                    "dimension": 384,
                    "similarity_metric": "cosine",
                    "index_name": vector_index_name(nt),
                }
                for nt in VECTOR_INDEX_TYPES
            ],
        }
        if include_internal:
            result["internal_node_types"] = [{"name": "BoardMeta", "stable": False}]
            result["internal_rel_types"] = []
        return result
