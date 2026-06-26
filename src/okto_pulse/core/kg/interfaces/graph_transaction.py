"""GraphTransaction port (spec #06, tr_73e982bb).

Abstracts staged graph writes for a board WITHOUT exposing
``open_board_connection`` / ``BoardConnection`` to consumers. ``begin(board_id)``
yields a :class:`GraphTransactionScope` — the staged-write context — with
``execute`` / ``commit`` / ``rollback``.

Embedded-adapter note: the live Kùzu/Ladybug write path auto-commits each
statement (there is no manual BEGIN/COMMIT anywhere in the codebase). So the
KuzuGraphTransaction adapter's ``commit()`` finalizes by closing the connection
and ``rollback()`` is best-effort — it closes the connection but cannot undo
statements already auto-committed. This is the documented embedded limitation
and is behavior-identical to the current direct ``open_board_connection`` usage;
a transactional backend can implement true rollback behind this same port.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class GraphTransactionScope(Protocol):
    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> Any:
        """Run a staged statement within the open scope."""
        ...

    async def commit(self) -> None:
        """Finalize the scope (close the connection)."""
        ...

    async def rollback(self) -> None:
        """Abort the scope (best-effort on the embedded auto-commit adapter)."""
        ...

    async def __aenter__(self) -> "GraphTransactionScope": ...

    async def __aexit__(self, *exc: Any) -> None: ...


@runtime_checkable
class GraphTransaction(Protocol):
    async def begin(self, board_id: str) -> GraphTransactionScope:
        """Open a staged-write scope for ``board_id`` (no open_board_connection leak)."""
        ...
