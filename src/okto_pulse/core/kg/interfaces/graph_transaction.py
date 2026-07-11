"""Graph transaction port with materialized, backend-neutral results."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Protocol, runtime_checkable


@dataclass
class GraphStatementResult:
    """Materialized rows returned by a semantic graph statement."""

    rows: tuple[tuple[Any, ...], ...] = ()
    columns: tuple[str, ...] = ()
    affected_count: int | None = None
    _position: int = field(default=0, init=False, repr=False)

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

    def has_next(self) -> bool:
        return self._position < len(self.rows)

    def get_next(self) -> list[Any]:
        if not self.has_next():
            raise StopIteration
        row = list(self.rows[self._position])
        self._position += 1
        return row

    def get_column_names(self) -> list[str]:
        return list(self.columns)

    def close(self) -> None:
        self._position = len(self.rows)

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
