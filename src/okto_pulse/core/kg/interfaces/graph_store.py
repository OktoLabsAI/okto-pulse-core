"""SemanticGraphStore Protocol — abstract graph operations for the KG layer.

The tier primario (9 query tools) consumes this interface. KuzuGraphStore
is the embedded default; future implementations include Neo4jGraphStore.
cypher_templates.py lives inside KuzuGraphStore as an implementation detail.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class QueryFilters:
    min_confidence: float = 0.5
    max_rows: int = 100
    validation_status_exclude: str = "unvalidated"


@runtime_checkable
class SemanticGraphStore(Protocol):
    # --- Read operations (tier primario) ---

    def find_by_topic(
        self, board_id: str, node_type: str, topic: str, filters: QueryFilters
    ) -> list[list]: ...

    def find_by_artifact(
        self, board_id: str, artifact_id: str, filters: QueryFilters
    ) -> list[list]: ...

    def traverse_supersedence(
        self, board_id: str, decision_id: str, max_depth: int = 10
    ) -> list[list]: ...

    def find_contradictions(
        self, board_id: str, node_id: str | None, limit: int
    ) -> list[list]: ...

    def vector_search(
        self, board_id: str, node_type: str, query_vec: list[float],
        top_k: int, min_similarity: float,
    ) -> list[dict]: ...

    def get_constraint_detail(
        self, board_id: str, constraint_id: str
    ) -> tuple[list[list], list[list], list[list]]: ...

    def get_alternatives(
        self, board_id: str, decision_id: str, limit: int
    ) -> list[list]: ...

    def get_learnings_for_area(
        self, board_id: str, area: str, filters: QueryFilters
    ) -> list[list]: ...

    def get_schema_version(self, board_id: str) -> str | None: ...

    def get_schema_info(self, board_id: str, *, include_internal: bool = False) -> dict: ...

    # --- Write operations (TransactionOrchestrator / bootstrap) ---

    def create_node(
        self, board_id: str, node_type: str, node_id: str, attrs: dict[str, Any]
    ) -> None: ...

    def create_edge(
        self, board_id: str, edge_type: str, from_id: str, to_id: str,
        attrs: dict[str, Any] | None = None,
    ) -> None: ...

    def delete_nodes_by_session(self, board_id: str, session_id: str) -> int: ...

    def delete_edges_by_session(self, board_id: str, session_id: str) -> int: ...

    def bootstrap(self, board_id: str) -> None: ...
