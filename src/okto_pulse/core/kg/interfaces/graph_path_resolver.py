"""GraphPathResolver / StorageIntrospection port (spec #06, tr_12f3933b).

Legacy storage-introspection compatibility port. New core logic should depend
on GraphRuntimeStore for graph presence, purge and footprint capabilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class GraphStorageState:
    """Observed storage state for a board's graph artifact (introspection only).

    ``backend`` names the storage backend; ``locked`` indicates an active
    runtime handle; ``quarantined`` is a best-effort adapter signal.
    """

    board_id: str
    path: Any
    exists: bool
    size_bytes: int
    backend: str
    locked: bool
    quarantined: bool
    sidecars: tuple[str, ...] = ()


@runtime_checkable
class GraphPathResolver(Protocol):
    def board_graph_path(self, board_id: str) -> Any:
        """Edition-specific graph locator for legacy compatibility."""
        ...

    def exists(self, board_id: str) -> bool:
        """Whether the board's graph artifact exists."""
        ...

    def storage_state(self, board_id: str) -> GraphStorageState:
        """Existence, size and adapter-specific storage state for the board graph."""
        ...
