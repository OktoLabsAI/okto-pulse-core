"""Logical graph runtime capability port.

The core asks this port about graph availability, purge and storage footprint
without learning how an edition stores the graph. Local file paths and backend
file names belong to adapters.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class GraphRuntimeState:
    board_id: str
    exists: bool
    status: str
    backend: str | None = None
    schema_version: str | None = None
    locked: bool = False
    quarantined: bool = False
    unavailable_reason: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GraphStorageFootprint:
    board_id: str
    status: str
    source: str
    total_bytes: int | None = None
    primary_bytes: int | None = None
    sidecar_bytes: int | None = None
    configured_max_bytes: int | None = None
    percentage: float | None = None
    unavailable_reason: str | None = None


@dataclass(frozen=True)
class GraphPurgeResult:
    board_id: str
    removed: bool
    not_found: bool
    status: str
    reason: str
    backend: str | None = None
    error_code: str | None = None


@runtime_checkable
class GraphRuntimeStore(Protocol):
    def graph_state(self, board_id: str) -> GraphRuntimeState:
        """Return the logical graph state for a board."""
        ...

    def exists(self, board_id: str) -> bool:
        """Return whether a board graph is present for runtime use."""
        ...

    def purge_board_graph(self, board_id: str, *, reason: str) -> GraphPurgeResult:
        """Purge board graph data through the edition-owned runtime."""
        ...

    def footprint(self, board_id: str) -> GraphStorageFootprint:
        """Return a logical storage footprint projection if the adapter has one."""
        ...


__all__ = [
    "GraphPurgeResult",
    "GraphRuntimeState",
    "GraphRuntimeStore",
    "GraphStorageFootprint",
]
