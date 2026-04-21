"""InMemoryGraphStore — satisfies SemanticGraphStore Protocol for tests.

Dict-based storage with basic cosine similarity for vector_search.
No Kuzu dependency.
"""

from __future__ import annotations

import math
import uuid
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.kg.interfaces.graph_store import QueryFilters
from okto_pulse.core.kg.schema import (
    NODE_TYPES,
    REL_TYPES,
    SCHEMA_VERSION,
    VECTOR_INDEX_TYPES,
    vector_index_name,
)


class InMemoryGraphStore:
    def __init__(self):
        self._nodes: dict[str, dict[str, dict[str, Any]]] = {}
        self._edges: dict[str, list[dict[str, Any]]] = {}
        self._bootstrapped: set[str] = set()

    def _board_nodes(self, board_id: str) -> dict[str, dict[str, Any]]:
        return self._nodes.setdefault(board_id, {})

    def _board_edges(self, board_id: str) -> list[dict[str, Any]]:
        return self._edges.setdefault(board_id, [])

    def bootstrap(self, board_id: str) -> None:
        self._bootstrapped.add(board_id)
        self._nodes.setdefault(board_id, {})
        self._edges.setdefault(board_id, [])

    def create_node(
        self, board_id: str, node_type: str, node_id: str, attrs: dict[str, Any]
    ) -> None:
        nodes = self._board_nodes(board_id)
        node = dict(attrs)
        node["id"] = node_id
        node["_type"] = node_type
        nodes[node_id] = node

    def create_edge(
        self, board_id: str, edge_type: str, from_id: str, to_id: str,
        attrs: dict[str, Any] | None = None,
    ) -> None:
        edges = self._board_edges(board_id)
        edge = dict(attrs or {})
        edge["_type"] = edge_type
        edge["_from"] = from_id
        edge["_to"] = to_id
        edges.append(edge)

    def delete_nodes_by_session(self, board_id: str, session_id: str) -> int:
        nodes = self._board_nodes(board_id)
        to_delete = [
            nid for nid, n in nodes.items()
            if n.get("source_session_id") == session_id
        ]
        for nid in to_delete:
            del nodes[nid]
        return len(to_delete)

    def delete_edges_by_session(self, board_id: str, session_id: str) -> int:
        edges = self._board_edges(board_id)
        before = len(edges)
        self._edges[board_id] = [
            e for e in edges if e.get("created_by_session_id") != session_id
        ]
        return before - len(self._edges[board_id])

    def find_by_topic(
        self, board_id: str, node_type: str, topic: str, filters: QueryFilters
    ) -> list[list]:
        nodes = self._board_nodes(board_id)
        results = []
        topic_lower = topic.lower()
        for n in nodes.values():
            if n.get("_type") != node_type:
                continue
            title = (n.get("title") or "").lower()
            if topic_lower in title:
                conf = n.get("source_confidence", 0)
                if conf >= filters.min_confidence:
                    score = n.get("relevance_score", 0.5)
                    if score < filters.min_relevance:
                        continue
                    results.append([
                        n["id"], n.get("title"), n.get("content"),
                        n.get("created_at"), n.get("source_confidence"),
                        score, n.get("superseded_by"),
                    ])
        return results[:filters.max_rows]

    def find_by_artifact(
        self, board_id: str, artifact_id: str, filters: QueryFilters
    ) -> list[list]:
        nodes = self._board_nodes(board_id)
        results = []
        for n in nodes.values():
            if n.get("source_artifact_ref") == artifact_id:
                results.append([
                    n["id"], n.get("title"), None, None, None, None, None, None,
                ])
        return results[:filters.max_rows]

    def traverse_supersedence(
        self, board_id: str, decision_id: str, max_depth: int = 10
    ) -> list[list]:
        nodes = self._board_nodes(board_id)
        node = nodes.get(decision_id)
        if node is None:
            return []
        return [[
            node["id"], node.get("title"), node.get("created_at"),
            node.get("superseded_by"), None,
        ]]

    def find_contradictions(
        self, board_id: str, node_id: str | None, limit: int
    ) -> list[list]:
        edges = self._board_edges(board_id)
        nodes = self._board_nodes(board_id)
        results = []
        for e in edges:
            if e.get("_type") != "contradicts":
                continue
            if node_id and e["_from"] != node_id and e["_to"] != node_id:
                continue
            na = nodes.get(e["_from"], {})
            nb = nodes.get(e["_to"], {})
            results.append([
                e["_from"], na.get("title"),
                e["_to"], nb.get("title"),
                e.get("confidence", 0.5),
            ])
        return results[:limit]

    def vector_search(
        self, board_id: str, node_type: str, query_vec: list[float],
        top_k: int, min_similarity: float,
    ) -> list[dict]:
        nodes = self._board_nodes(board_id)
        results = []
        for n in nodes.values():
            if n.get("_type") != node_type:
                continue
            emb = n.get("embedding")
            if emb is None:
                continue
            sim = _cosine_similarity(query_vec, emb)
            if sim >= min_similarity:
                results.append({
                    "node_id": n["id"],
                    "node_type": node_type,
                    "title": n.get("title", ""),
                    "source_artifact_ref": n.get("source_artifact_ref"),
                    "similarity": sim,
                })
        results.sort(key=lambda x: x["similarity"], reverse=True)
        return results[:top_k]

    def get_constraint_detail(
        self, board_id: str, constraint_id: str
    ) -> tuple[list[list], list[list], list[list]]:
        nodes = self._board_nodes(board_id)
        node = nodes.get(constraint_id)
        if node is None:
            return [], [], []
        main = [[
            node["id"], node.get("title"), node.get("content"),
            node.get("justification"), node.get("source_artifact_ref"),
            node.get("source_confidence"),
        ]]
        return main, [], []

    def get_alternatives(
        self, board_id: str, decision_id: str, limit: int
    ) -> list[list]:
        edges = self._board_edges(board_id)
        nodes = self._board_nodes(board_id)
        results = []
        for e in edges:
            if e.get("_type") == "relates_to" and e["_from"] == decision_id:
                alt = nodes.get(e["_to"], {})
                results.append([
                    alt.get("id"), alt.get("title"), alt.get("content"),
                    alt.get("justification"), alt.get("source_confidence"),
                    alt.get("source_artifact_ref"),
                ])
        return results[:limit]

    def get_learnings_for_area(
        self, board_id: str, area: str, filters: QueryFilters
    ) -> list[list]:
        return self.find_by_topic(board_id, "Learning", area, filters)

    def get_schema_version(self, board_id: str) -> str | None:
        if board_id in self._bootstrapped:
            return SCHEMA_VERSION
        return None

    def get_schema_info(self, board_id: str, *, include_internal: bool = False) -> dict:
        result: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "stable_node_types": [{"name": nt, "stable": True} for nt in NODE_TYPES],
            "stable_rel_types": [
                {"name": rt[0], "from": rt[1], "to": rt[2]} for rt in REL_TYPES
            ],
            "vector_indexes": [
                {"node_type": nt, "attribute": "embedding",
                 "dimension": 384, "similarity_metric": "cosine",
                 "index_name": vector_index_name(nt)}
                for nt in VECTOR_INDEX_TYPES
            ],
        }
        if include_internal:
            result["internal_node_types"] = [{"name": "BoardMeta", "stable": False}]
            result["internal_rel_types"] = []
        return result

    def clear(self) -> None:
        self._nodes.clear()
        self._edges.clear()
        self._bootstrapped.clear()


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1.0
    nb = math.sqrt(sum(x * x for x in b)) or 1.0
    return max(0.0, min(1.0, dot / (na * nb)))
