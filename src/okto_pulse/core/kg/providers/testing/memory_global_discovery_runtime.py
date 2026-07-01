"""Test-only fake for the GlobalDiscoveryRuntime port."""

from __future__ import annotations

from pathlib import Path
from typing import Any


class InMemoryGlobalDiscoveryRuntime:
    """Minimal fake for tests that do not exercise LadybugDB global discovery."""

    def __init__(self) -> None:
        self._path = Path("__inmemory_global_discovery__")
        self.closed = True
        self.purged_reasons: list[str] = []

    def global_graph_path(self) -> Path:
        return self._path

    def bootstrap(self) -> Path:
        self.closed = False
        return self._path

    def ensure_layer_schema(self) -> list[str]:
        return []

    def open_connection(self) -> tuple[Any, Any]:
        raise RuntimeError(
            "InMemoryGlobalDiscoveryRuntime does not expose a LadybugDB "
            "connection; configure the Community adapter for integration tests."
        )

    def close(self) -> None:
        self.closed = True

    def purge(self, *, reason: str = "manual") -> list[str]:
        self.purged_reasons.append(reason)
        self.closed = True
        return []

    def require_write_token(self, *, operation: str = "") -> Any:
        return None

    def reset_for_tests(self) -> None:
        self.close()


__all__ = ["InMemoryGlobalDiscoveryRuntime"]
