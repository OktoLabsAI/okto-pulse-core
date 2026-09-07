"""CypherExecutor Protocol — backend-specific Cypher execution (tier power)."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable
from collections.abc import Sequence


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


@runtime_checkable
class ReadOnlyBatchCypherExecutor(Protocol):
    """Optional read capability; existing scalar executors need not implement it.

    Preserve board authorization, read-only validation and each statement's row
    limit. Results must align with all input statements, with no partial prefix
    on failure. An adapter can execute the batch in one read transaction without
    requiring Core to know its engine or transaction implementation.
    """

    def execute_read_only_batch(
        self, board_id: str,
        statements: Sequence[tuple[str, dict[str, Any] | None, int]],
    ) -> Sequence[dict[str, Any]]:
        ...
