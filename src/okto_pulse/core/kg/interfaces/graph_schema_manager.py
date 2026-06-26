"""GraphSchemaManager port (spec #06, tr_ec6a8c50).

Owns bootstrap / migration / version / validation of a board's graph schema
WITHOUT exposing ``kg.schema`` migration internals to consumers. Async: the
contract is the boundary; the embedded adapter runs the synchronous Kùzu calls.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class SchemaValidationResult:
    board_id: str
    valid: bool
    current_version: str | None
    expected_version: str
    issues: tuple[str, ...] = ()


@runtime_checkable
class GraphSchemaManager(Protocol):
    async def ensure_bootstrapped(self, board_id: str) -> None:
        """Idempotently guarantee the board's graph exists at the current schema."""
        ...

    async def migrate(self, board_id: str) -> dict[str, Any]:
        """Force-apply schema migrations (idempotent); returns the migration summary."""
        ...

    async def current_version(self, board_id: str) -> str:
        """The board's persisted schema version (or the code's expected version)."""
        ...

    async def validate(self, board_id: str) -> SchemaValidationResult:
        """Compare the board's schema version against the expected version."""
        ...
