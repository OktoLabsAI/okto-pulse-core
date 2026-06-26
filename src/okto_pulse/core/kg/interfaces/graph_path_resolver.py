"""GraphPathResolver / StorageIntrospection port (spec #06, tr_12f3933b).

Resolves a board's graph storage location and state WITHOUT exposing
``kg.schema.board_kuzu_path`` to consumers. Synchronous — path/stat lookups are
local filesystem calls and the live surface is sync (no fictitious async).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class GraphStorageState:
    """Observed storage state for a board's graph file (introspection only).

    ``backend`` names the storage backend; ``locked`` is True when an active
    write-ahead-log holds the graph (an open-handle proxy); ``quarantined`` is a
    best-effort residue signal (the embedded quarantine service is write-only,
    so it is inferred from "board dir present but graph file cleared").
    """

    board_id: str
    path: Path
    exists: bool
    size_bytes: int
    backend: str
    locked: bool
    quarantined: bool
    sidecars: tuple[str, ...] = ()


@runtime_checkable
class GraphPathResolver(Protocol):
    def board_graph_path(self, board_id: str) -> Path:
        """Absolute path to the board's graph file (replaces board_kuzu_path)."""
        ...

    def exists(self, board_id: str) -> bool:
        """Whether the board's graph file exists on disk."""
        ...

    def storage_state(self, board_id: str) -> GraphStorageState:
        """Path + existence + size + sidecar files for the board's graph."""
        ...
