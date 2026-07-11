"""Semantic port for the edition-owned Global Discovery graph."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from okto_pulse.core.kg.interfaces.graph_lifecycle import GraphHandle
from okto_pulse.core.kg.interfaces.graph_runtime_store import (
    GraphPurgeResult,
    GraphRuntimeState,
)
from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult


@runtime_checkable
class GlobalDiscoveryRuntime(Protocol):
    """Edition-owned runtime for the cross-board discovery graph."""

    def state(self) -> GraphRuntimeState: ...

    def bootstrap(self) -> GraphHandle: ...

    def ensure_layer_schema(self) -> tuple[str, ...]: ...

    def execute(
        self,
        statement: str,
        params: dict[str, Any] | None = None,
    ) -> GraphStatementResult: ...

    def flush_after_write_batch(self) -> None: ...

    def close(self) -> None: ...

    def purge(self, *, reason: str = "manual") -> GraphPurgeResult: ...


__all__ = ["GlobalDiscoveryRuntime"]
