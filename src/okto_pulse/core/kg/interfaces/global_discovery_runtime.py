"""Port for the edition-owned Global Discovery runtime.

Core keeps the query semantics and schema definitions. The concrete LadybugDB
path, handle lifecycle and quarantine behavior are owned by the edition adapter.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class GlobalDiscoveryRuntime(Protocol):
    """Edition-owned runtime for the cross-board discovery graph."""

    def global_graph_path(self) -> Path: ...

    def bootstrap(self) -> Path: ...

    def ensure_layer_schema(self) -> list[str]: ...

    def open_connection(self) -> tuple[Any, Any]: ...

    def close(self) -> None: ...

    def purge(self, *, reason: str = "manual") -> list[str]: ...

    def require_write_token(self, *, operation: str = "") -> Any: ...

    def reset_for_tests(self) -> None: ...


__all__ = ["GlobalDiscoveryRuntime"]
