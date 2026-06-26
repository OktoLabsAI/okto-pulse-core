"""GraphLifecycle port (spec #06, tr_9ce8349f).

Owns open/close/rebuild/purge of a board's graph (including lock/quarantine
handling) WITHOUT exposing ``kg.schema.close_all_connections`` to consumers.
Async: the contract is the boundary; the embedded adapter runs the underlying
synchronous Kùzu/Ladybug calls. open/rebuild/purge return structured reports
(status / evidence / reason) so consumers never inspect raw paths or handles.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class GraphHandle:
    """Result of opening a board graph (expresses lock/quarantine state)."""

    board_id: str
    path: Path
    opened: bool
    backend: str
    locked: bool
    quarantined: bool


@dataclass(frozen=True)
class RebuildReport:
    """Structured outcome of a connection-level rebuild."""

    board_id: str
    status: str  # "rebuilt" | "skipped" | "failed"
    steps: tuple[str, ...] = ()
    reason: str | None = None


@dataclass(frozen=True)
class PurgeReport:
    """Structured outcome of a quarantine-then-clear purge."""

    board_id: str
    status: str  # "purged" | "noop"
    reason: str
    affected_paths: tuple[str, ...] = field(default_factory=tuple)
    quarantined: bool = False


@runtime_checkable
class GraphLifecycle(Protocol):
    async def open(self, board_id: str) -> GraphHandle:
        """Ensure the board's graph exists/openable; returns a GraphHandle."""
        ...

    async def close(self, board_id: str | None = None) -> None:
        """Release connections so the storage dir can be moved/removed.

        ``board_id=None`` closes the global singleton and every pooled
        per-board connection (replaces close_all_connections).
        """
        ...

    async def rebuild(self, board_id: str) -> RebuildReport:
        """Recycle the board's graph handle (release + re-ensure).

        Connection-level rebuild primitive; the full data-rebuild orchestration
        (kg rebuild) consumes this primitive rather than duplicating it.
        """
        ...

    async def purge(self, board_id: str, *, reason: str) -> PurgeReport:
        """Quarantine-then-clear the board's graph storage."""
        ...
