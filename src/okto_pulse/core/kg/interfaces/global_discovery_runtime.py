"""Semantic port for the edition-owned Global Discovery graph."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from okto_pulse.core.kg.interfaces.graph_lifecycle import GraphHandle
from okto_pulse.core.kg.interfaces.graph_runtime_store import (
    GraphPurgeResult,
    GraphRuntimeState,
)
from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult


@runtime_checkable
class GlobalDiscoveryRuntime(Protocol):
    """Edition-owned runtime for the cross-board discovery graph."""

    def state(self) -> GraphRuntimeState: ...

    def bootstrap(self) -> GraphHandle: ...

    def ensure_layer_schema(self) -> tuple[str, ...]: ...

    def execute(
        self,
        statement: str,
        params: dict[str, Any] | None = None,
    ) -> GraphStatementResult: ...

    def search_decision_digests(
        self,
        query_vector: list[float],
        *,
        board_ids: tuple[str, ...],
        graph_layer: str,
        top_k: int,
        min_similarity: float,
        exhaustive: bool = False,
    ) -> list[dict[str, Any]]:
        """Return materialized, board-scoped semantic digest hits.

        ``exhaustive`` requests the adapter's complete fallback path when an
        indexed page was underfilled after Core-level source validation.
        """
        ...

    def list_schema_objects(self) -> tuple[str, ...]: ...

    def upsert_board_summary(
        self,
        *,
        board_id: str,
        name: str,
        summary: str,
        summary_embedding: list[float],
        decision_delta: int,
        synced_at: str,
    ) -> None: ...

    def upsert_decision_digest(
        self,
        *,
        digest_id: str,
        board_id: str,
        original_node_id: str,
        title: str,
        summary: str,
        node_type: str,
        graph_layer: str,
        embedding: list[float],
        created_at: str,
    ) -> str: ...

    def link_board_digest(self, *, board_id: str, digest_id: str) -> None: ...

    def flush_after_write_batch(self) -> None: ...

    def close(self) -> None: ...

    def purge(self, *, reason: str = "manual") -> GraphPurgeResult: ...


__all__ = ["GlobalDiscoveryRuntime"]
