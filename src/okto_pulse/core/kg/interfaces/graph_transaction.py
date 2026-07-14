"""Graph transaction port with materialized, backend-neutral results."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Protocol, runtime_checkable


@dataclass
class GraphStatementResult:
    """Materialized rows returned by a semantic graph statement."""

    rows: tuple[tuple[Any, ...], ...] = ()
    columns: tuple[str, ...] = ()
    affected_count: int | None = None

    @classmethod
    def from_rows(
        cls,
        rows: Iterable[Iterable[Any]],
        *,
        columns: Iterable[str] = (),
        affected_count: int | None = None,
    ) -> "GraphStatementResult":
        return cls(
            rows=tuple(tuple(row) for row in rows),
            columns=tuple(str(column) for column in columns),
            affected_count=affected_count,
        )

    def __iter__(self):
        return (list(row) for row in self.rows)


@runtime_checkable
class GraphTransactionScope(Protocol):
    def execute(
        self,
        statement: str,
        params: dict[str, Any] | None = None,
    ) -> GraphStatementResult:
        """Run a statement and return materialized rows."""
        ...

    def create_node(
        self,
        node_type: str,
        node_id: str,
        attrs: dict[str, Any],
        *,
        source_session_id: str,
    ) -> None: ...

    def update_node(
        self,
        node_type: str,
        node_id: str,
        attrs: dict[str, Any],
    ) -> None: ...

    def mark_superseded(
        self,
        node_type: str,
        node_id: str,
        *,
        superseded_by: str,
        superseded_at: str,
        revocation_reason: str,
    ) -> None: ...

    def edge_exists(
        self,
        edge_type: str,
        from_type: str,
        to_type: str,
        from_id: str,
        to_id: str,
    ) -> bool: ...

    def create_edge(
        self,
        edge_type: str,
        from_type: str,
        to_type: str,
        from_id: str,
        to_id: str,
        attrs: dict[str, Any],
    ) -> bool: ...

    def find_node_types(self, node_id: str) -> tuple[str, ...]: ...

    def delete_edges_by_session(self, session_id: str) -> None: ...

    def delete_nodes_by_session(
        self,
        session_id: str,
        node_types: tuple[str, ...],
    ) -> tuple[str, ...]: ...

    def increment_attestation(
        self,
        node_type: str,
        node_id: str,
        *,
        attested_at: str,
    ) -> None: ...

    async def commit(self) -> None:
        """Finalize the scope."""
        ...

    async def rollback(self) -> None:
        """Abort the scope (best-effort on the embedded auto-commit adapter)."""
        ...

    async def __aenter__(self) -> "GraphTransactionScope": ...

    async def __aexit__(self, *exc: Any) -> None: ...


@runtime_checkable
class GraphTransaction(Protocol):
    async def begin(self, board_id: str) -> GraphTransactionScope:
        """Open a staged-write scope for ``board_id``."""
        ...


__all__ = ["GraphStatementResult", "GraphTransaction", "GraphTransactionScope"]
