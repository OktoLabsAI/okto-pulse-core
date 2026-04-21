"""CypherExecutor Protocol — backend-specific Cypher execution (tier power)."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class CypherExecutor(Protocol):
    def execute_read_only(
        self, board_id: str, cypher: str, params: dict[str, Any] | None = None,
        *, max_rows: int = 1000,
    ) -> dict:
        """Execute a validated read-only Cypher query. Returns dict with rows, row_count, etc."""
        ...

    def is_supported(self) -> bool:
        """Whether this backend supports Cypher queries."""
        ...
