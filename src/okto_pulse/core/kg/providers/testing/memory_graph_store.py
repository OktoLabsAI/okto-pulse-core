"""InMemoryGraphStore — satisfies SemanticGraphStore Protocol for tests.

Dict-based storage with basic cosine similarity for vector_search.
No Kuzu dependency.
"""

from __future__ import annotations

import math
from typing import Any

from okto_pulse.core.kg.interfaces.graph_lifecycle import (
    GraphHandle,
    GraphLifecycleStepResult,
    PurgeReport,
    RebuildReport,
)
from okto_pulse.core.kg.interfaces.graph_runtime_store import (
    GraphPurgeResult,
    GraphRuntimeState,
    GraphStorageFootprint,
)
from okto_pulse.core.kg.interfaces.graph_schema_manager import SchemaValidationResult
from okto_pulse.core.kg.interfaces.graph_store import GraphCapabilities, QueryFilters
from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult
from okto_pulse.core.kg.interfaces.storage_ref import StorageRef
from okto_pulse.core.kg.schema_contract import (
    NODE_TYPES,
    SCHEMA_VERSION,
    VECTOR_INDEX_TYPES,
    resolve_relationship_endpoint_pair,
    stable_rel_type_entries,
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
        *,
        from_type: str | None = None,
        to_type: str | None = None,
    ) -> None:
        nodes = self._board_nodes(board_id)
        from_type = from_type or nodes.get(from_id, {}).get("_type")
        to_type = to_type or nodes.get(to_id, {}).get("_type")
        from_type, to_type = resolve_relationship_endpoint_pair(
            edge_type,
            from_type=from_type,
            to_type=to_type,
        )
        edges = self._board_edges(board_id)
        edge = dict(attrs or {})
        edge["_type"] = edge_type
        edge["_from"] = from_id
        edge["_to"] = to_id
        edge["_from_type"] = from_type
        edge["_to_type"] = to_type
        edges.append(edge)

    def update_node(
        self,
        board_id: str,
        node_type: str,
        node_id: str,
        attrs: dict[str, Any],
    ) -> None:
        node = self._board_nodes(board_id).get(node_id)
        if node is not None and node.get("_type") == node_type:
            node.update(attrs)

    def mark_superseded(
        self,
        board_id: str,
        node_type: str,
        node_id: str,
        *,
        superseded_by: str,
        superseded_at: str,
        revocation_reason: str,
    ) -> None:
        self.update_node(
            board_id,
            node_type,
            node_id,
            {
                "superseded_by": superseded_by,
                "superseded_at": superseded_at,
                "revocation_reason": revocation_reason,
            },
        )

    def edge_exists(
        self,
        board_id: str,
        edge_type: str,
        from_type: str,
        to_type: str,
        from_id: str,
        to_id: str,
    ) -> bool:
        return any(
            edge.get("_type") == edge_type
            and edge.get("_from_type") == from_type
            and edge.get("_to_type") == to_type
            and edge.get("_from") == from_id
            and edge.get("_to") == to_id
            for edge in self._board_edges(board_id)
        )

    def find_node_types(self, board_id: str, node_id: str) -> tuple[str, ...]:
        node = self._board_nodes(board_id).get(node_id)
        return (str(node["_type"]),) if node and node.get("_type") else ()

    def increment_attestation(
        self,
        board_id: str,
        node_type: str,
        node_id: str,
        *,
        attested_at: str,
    ) -> None:
        node = self._board_nodes(board_id).get(node_id)
        if node is not None and node.get("_type") == node_type:
            node["attestation_count"] = int(node.get("attestation_count") or 1) + 1
            node["last_attested_at"] = attested_at

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

    def find_by_topic_semantic(
        self,
        board_id: str,
        node_type: str,
        query_vec: list[float],
        filters: QueryFilters,
        min_similarity: float = 0.3,
    ) -> list[list]:
        nodes = self._board_nodes(board_id)
        hits = self.vector_search(
            board_id,
            node_type,
            query_vec,
            top_k=filters.max_rows,
            min_similarity=min_similarity,
        )
        results = []
        for hit in hits:
            node = nodes.get(hit["node_id"], {})
            if float(node.get("source_confidence") or 0.0) < filters.min_confidence:
                continue
            if float(node.get("relevance_score") or 0.0) < filters.min_relevance:
                continue
            results.append([
                node.get("id"),
                node.get("title"),
                node.get("content"),
                node.get("created_at"),
                node.get("source_confidence"),
                node.get("relevance_score"),
                node.get("superseded_by"),
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

    def find_by_artifact_filtered(
        self,
        board_id: str,
        artifact_id: str,
        filters: QueryFilters,
        *,
        rel_types: list[str] | None = None,
        direction: str = "both",
        max_depth: int = 2,
        graph_layer: str = "all",
    ) -> list[list]:
        return self.find_by_artifact(board_id, artifact_id, filters)

    def traverse_supersedence(
        self, board_id: str, decision_id: str, max_depth: int = 10,
        node_type: str = "Decision",
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
        *,
        include_superseded: bool = False,
        graph_layer: str = "all",
    ) -> list[dict]:
        if graph_layer not in {"canonical", "working", "all"}:
            raise ValueError("invalid_graph_layer")
        nodes = self._board_nodes(board_id)
        results = []
        for n in nodes.values():
            if n.get("_type") != node_type:
                continue
            if n.get("superseded_by") and not include_superseded:
                continue
            if graph_layer != "all" and n.get("graph_layer") != graph_layer:
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
            "stable_rel_types": stable_rel_type_entries(),
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

    def list_schema_objects(self, board_id: str) -> tuple[str, ...]:
        return tuple(sorted(NODE_TYPES)) if board_id in self._bootstrapped else ()

    def list_node_properties(self, board_id: str, node_type: str) -> tuple[str, ...]:
        properties: set[str] = set()
        for node in self._board_nodes(board_id).values():
            if node.get("_type") == node_type:
                properties.update(key for key in node if not key.startswith("_"))
        return tuple(sorted(properties))

    def capabilities(self) -> GraphCapabilities:
        return GraphCapabilities(
            indexed_similarity=False,
            schema_introspection=True,
            mutable_indexed_attributes=True,
        )

    def clear(self) -> None:
        self._nodes.clear()
        self._edges.clear()
        self._bootstrapped.clear()


class InMemoryCypherExecutor:
    """CypherExecutor fake for tests that do not exercise a real graph backend."""

    def __init__(self) -> None:
        self.queries: list[tuple[str, str, dict[str, Any]]] = []

    def execute_read_only(
        self,
        board_id: str,
        cypher: str,
        params: dict[str, Any] | None = None,
        *,
        max_rows: int = 1000,
    ) -> dict:
        self.queries.append((board_id, cypher, dict(params or {})))
        return {
            "rows": [],
            "row_count": 0,
            "truncated": False,
            "execution_time_ms": 0.0,
            "max_rows": max_rows,
        }

    def is_supported(self) -> bool:
        return False


class _InMemoryGraphTransactionScope:
    def __init__(self, board_id: str, store: InMemoryGraphStore) -> None:
        self.board_id = board_id
        self.store = store
        self.statements: list[tuple[str, dict[str, Any] | None]] = []
        self.finished = False
        self.rolled_back = False

    def execute(
        self,
        cypher: str,
        params: dict[str, Any] | None = None,
    ) -> GraphStatementResult:
        self.statements.append((cypher, params))
        return GraphStatementResult()

    def create_node(
        self,
        node_type: str,
        node_id: str,
        attrs: dict[str, Any],
        *,
        source_session_id: str,
    ) -> None:
        self.store.create_node(
            self.board_id,
            node_type,
            node_id,
            {**attrs, "source_session_id": source_session_id},
        )

    def update_node(
        self,
        node_type: str,
        node_id: str,
        attrs: dict[str, Any],
    ) -> None:
        node = self.store._board_nodes(self.board_id).get(node_id)
        if node is not None and node.get("_type") == node_type:
            node.update(attrs)

    def mark_superseded(
        self,
        node_type: str,
        node_id: str,
        *,
        superseded_by: str,
        superseded_at: str,
        revocation_reason: str,
    ) -> None:
        self.update_node(
            node_type,
            node_id,
            {
                "superseded_by": superseded_by,
                "superseded_at": superseded_at,
                "revocation_reason": revocation_reason,
            },
        )

    def edge_exists(
        self,
        edge_type: str,
        from_type: str,
        to_type: str,
        from_id: str,
        to_id: str,
    ) -> bool:
        return any(
            edge.get("_type") == edge_type
            and edge.get("_from_type") == from_type
            and edge.get("_to_type") == to_type
            and edge.get("_from") == from_id
            and edge.get("_to") == to_id
            for edge in self.store._board_edges(self.board_id)
        )

    def create_edge(
        self,
        edge_type: str,
        from_type: str,
        to_type: str,
        from_id: str,
        to_id: str,
        attrs: dict[str, Any],
    ) -> bool:
        nodes = self.store._board_nodes(self.board_id)
        if from_id not in nodes or to_id not in nodes:
            return False
        self.store.create_edge(
            self.board_id,
            edge_type,
            from_id,
            to_id,
            attrs,
            from_type=from_type,
            to_type=to_type,
        )
        return True

    def find_node_types(self, node_id: str) -> tuple[str, ...]:
        node = self.store._board_nodes(self.board_id).get(node_id)
        return (str(node["_type"]),) if node and node.get("_type") else ()

    def delete_edges_by_session(self, session_id: str) -> None:
        self.store.delete_edges_by_session(self.board_id, session_id)

    def delete_nodes_by_session(
        self,
        session_id: str,
        node_types: tuple[str, ...],
    ) -> tuple[str, ...]:
        del node_types
        self.store.delete_nodes_by_session(self.board_id, session_id)
        return ()

    def increment_attestation(
        self,
        node_type: str,
        node_id: str,
        *,
        attested_at: str,
    ) -> None:
        node = self.store._board_nodes(self.board_id).get(node_id)
        if node is not None and node.get("_type") == node_type:
            node["attestation_count"] = int(node.get("attestation_count") or 1) + 1
            node["last_attested_at"] = attested_at

    async def commit(self) -> None:
        self.finished = True

    async def rollback(self) -> None:
        self.finished = True
        self.rolled_back = True

    async def __aenter__(self) -> "_InMemoryGraphTransactionScope":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()


class InMemoryGraphTransaction:
    def __init__(self, store: InMemoryGraphStore | None = None) -> None:
        self.store = store or InMemoryGraphStore()

    async def begin(self, board_id: str) -> _InMemoryGraphTransactionScope:
        return _InMemoryGraphTransactionScope(board_id, self.store)


class InMemoryGraphRuntimeStore:
    """Logical graph runtime fake for core tests."""

    def __init__(
        self,
        store: InMemoryGraphStore | None = None,
        schema_manager: "InMemoryGraphSchemaManager | None" = None,
    ) -> None:
        self.store = store or InMemoryGraphStore()
        self.schema_manager = schema_manager or InMemoryGraphSchemaManager(self.store)

    def graph_state(self, board_id: str) -> GraphRuntimeState:
        bootstrapped = board_id in self.store._bootstrapped
        return GraphRuntimeState(
            board_id=board_id,
            storage_ref=StorageRef(f"board:{board_id}", "memory_graph"),
            exists=bootstrapped,
            status="healthy" if bootstrapped else "absent",
            backend="logical_memory",
            schema_version=SCHEMA_VERSION if bootstrapped else None,
            locked=False,
            quarantined=False,
            unavailable_reason=None if bootstrapped else "graph_absent",
            details={"source": "in_memory_runtime"},
        )

    def exists(self, board_id: str) -> bool:
        return self.graph_state(board_id).exists

    def purge_board_graph(self, board_id: str, *, reason: str) -> GraphPurgeResult:
        existed = self.exists(board_id)
        self.store._nodes.pop(board_id, None)
        self.store._edges.pop(board_id, None)
        self.store._bootstrapped.discard(board_id)
        return GraphPurgeResult(
            board_id=board_id,
            removed=existed,
            not_found=not existed,
            status="purged" if existed else "not_found",
            reason=reason,
            backend="logical_memory",
            error_code=None,
        )

    def footprint(self, board_id: str) -> GraphStorageFootprint:
        exists = self.exists(board_id)
        return GraphStorageFootprint(
            board_id=board_id,
            storage_ref=StorageRef(f"board:{board_id}", "memory_graph"),
            status="unavailable",
            source="runtime_capability",
            total_bytes=None,
            primary_bytes=None,
            sidecar_bytes=None,
            configured_max_bytes=None,
            percentage=None,
            unavailable_reason=None if exists else "graph_absent",
        )


class InMemoryGraphSchemaManager:
    def __init__(self, store: InMemoryGraphStore | None = None) -> None:
        self.store = store or InMemoryGraphStore()

    def _active_store(self):
        try:
            from okto_pulse.core.kg.interfaces import get_kg_registry

            return get_kg_registry().graph_store or self.store
        except RuntimeError:
            return self.store

    async def ensure_bootstrapped(self, board_id: str) -> None:
        self.store.bootstrap(board_id)

    async def migrate(self, board_id: str) -> dict[str, Any]:
        self.store.bootstrap(board_id)
        return {
            "board_id": board_id,
            "migrated": True,
            "schema_version": SCHEMA_VERSION,
            "columns_added": {},
            "errors": [],
            "duration_ms": 0,
        }

    async def current_version(self, board_id: str) -> str:
        return self._active_store().get_schema_version(board_id) or SCHEMA_VERSION

    async def validate(self, board_id: str) -> SchemaValidationResult:
        try:
            current = self._active_store().get_schema_version(board_id)
        except Exception as exc:
            return SchemaValidationResult(
                board_id=board_id,
                valid=False,
                current_version=None,
                expected_version=SCHEMA_VERSION,
                issues=(f"schema version read failed: {exc}",),
            )
        return SchemaValidationResult(
            board_id=board_id,
            valid=current == SCHEMA_VERSION,
            current_version=current,
            expected_version=SCHEMA_VERSION,
            issues=() if current == SCHEMA_VERSION else ("no schema version recorded for board",),
        )


class InMemoryGraphLifecycle:
    def __init__(
        self,
        schema_manager: InMemoryGraphSchemaManager | None = None,
    ) -> None:
        self.schema_manager = schema_manager or InMemoryGraphSchemaManager()

    async def open(self, board_id: str) -> GraphHandle:
        await self.schema_manager.ensure_bootstrapped(board_id)
        return GraphHandle(
            board_id=board_id,
            storage_ref=StorageRef(f"board:{board_id}", "memory_graph"),
            opened=True,
            status="opened",
            locked=False,
            quarantined=False,
        )

    async def close(self, board_id: str | None = None) -> None:
        return None

    async def rebuild(self, board_id: str) -> RebuildReport:
        await self.schema_manager.ensure_bootstrapped(board_id)
        return RebuildReport(board_id=board_id, status="rebuilt", steps=("memory",))

    async def purge(self, board_id: str, *, reason: str) -> PurgeReport:
        return PurgeReport(
            board_id=board_id,
            status="noop",
            reason=reason,
            affected_storage_refs=(),
            quarantined=False,
        )

    def apply_step(
        self,
        board_id: str,
        graph_type: str,
        step: str,
    ) -> GraphLifecycleStepResult:
        del board_id, graph_type, step
        return GraphLifecycleStepResult(ok=True, detail="memory")


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1.0
    nb = math.sqrt(sum(x * x for x in b)) or 1.0
    return max(0.0, min(1.0, dot / (na * nb)))


def in_memory_safe_write_step_adapter(
    board_id: str,
    graph_type: str,
    step: str,
) -> GraphLifecycleStepResult:
    return GraphLifecycleStepResult(ok=True, detail="memory")
