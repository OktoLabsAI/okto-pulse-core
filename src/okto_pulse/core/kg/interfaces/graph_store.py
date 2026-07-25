"""SemanticGraphStore Protocol — abstract graph operations for the KG layer.

The primary query tools consume this interface. Concrete graph stores and
query-template details live behind edition adapters.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class QueryFilters:
    min_confidence: float = 0.5
    max_rows: int = 100
    # v0.3.0 R3: continuous relevance threshold used by every read query.
    # Default 0.3 below the neutral 0.5 so newly-created nodes still
    # pass through the filter. Callers can pass 0.0 to disable.
    min_relevance: float = 0.3
    # Spec MKG-D-S1 (FR7): recall returns ACTIVE memory by default —
    # superseded nodes surface only via explicit opt-in.
    include_superseded: bool = False


@dataclass(frozen=True)
class GraphCapabilities:
    indexed_similarity: bool = False
    schema_introspection: bool = False
    mutable_indexed_attributes: bool = True


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
        self,
        board_id: str,
        decision_id: str,
        max_depth: int = 10,
        node_type: str = "Decision",
    ) -> list[list]: ...

    def find_contradictions(
        self, board_id: str, node_id: str | None, limit: int
    ) -> list[list]: ...

    def vector_search(
        self, board_id: str, node_type: str, query_vec: list[float],
        top_k: int, min_similarity: float,
        *,
        include_superseded: bool = False,
        graph_layer: str = "all",
    ) -> list[dict]: ...

    def find_active_by_source_ref(
        self,
        board_id: str,
        node_type: str,
        source_artifact_ref: str,
    ) -> dict[str, Any] | None:
        """Return the newest active assertion for an exact lineage ref."""
        ...

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

    def list_schema_objects(self, board_id: str) -> tuple[str, ...]: ...

    def list_node_properties(
        self,
        board_id: str,
        node_type: str,
    ) -> tuple[str, ...]: ...

    def capabilities(self) -> GraphCapabilities: ...

    # --- Write operations (TransactionOrchestrator / bootstrap) ---

    def create_node(
        self, board_id: str, node_type: str, node_id: str, attrs: dict[str, Any]
    ) -> None: ...

    def create_edge(
        self, board_id: str, edge_type: str, from_id: str, to_id: str,
        attrs: dict[str, Any] | None = None,
        *,
        from_type: str | None = None,
        to_type: str | None = None,
    ) -> None: ...

    def update_node(
        self,
        board_id: str,
        node_type: str,
        node_id: str,
        attrs: dict[str, Any],
    ) -> None: ...

    def mark_superseded(
        self,
        board_id: str,
        node_type: str,
        node_id: str,
        *,
        superseded_by: str,
        superseded_at: str,
        revocation_reason: str,
    ) -> None: ...

    def edge_exists(
        self,
        board_id: str,
        edge_type: str,
        from_type: str,
        to_type: str,
        from_id: str,
        to_id: str,
    ) -> bool: ...

    def find_node_types(self, board_id: str, node_id: str) -> tuple[str, ...]: ...

    def increment_attestation(
        self,
        board_id: str,
        node_type: str,
        node_id: str,
        *,
        attested_at: str,
    ) -> None: ...

    def delete_nodes_by_session(self, board_id: str, session_id: str) -> int: ...

    def delete_edges_by_session(self, board_id: str, session_id: str) -> int: ...

    def bootstrap(self, board_id: str) -> None: ...
