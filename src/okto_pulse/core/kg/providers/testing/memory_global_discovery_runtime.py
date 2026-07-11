"""Test-only fake for the GlobalDiscoveryRuntime port."""

from __future__ import annotations

from okto_pulse.core.kg.interfaces.graph_lifecycle import GraphHandle
from okto_pulse.core.kg.interfaces.graph_runtime_store import (
    GraphPurgeResult,
    GraphRuntimeState,
)
from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult
from okto_pulse.core.kg.interfaces.storage_ref import StorageRef


class InMemoryGlobalDiscoveryRuntime:
    """Minimal fake for tests that do not exercise LadybugDB global discovery."""

    def __init__(self) -> None:
        self._exists = False
        self.closed = True
        self.purged_reasons: list[str] = []

    @staticmethod
    def _storage_ref() -> StorageRef:
        return StorageRef("global-discovery", "memory_graph")

    def state(self) -> GraphRuntimeState:
        return GraphRuntimeState(
            board_id="_global",
            storage_ref=self._storage_ref(),
            exists=self._exists,
            status="healthy" if self._exists else "absent",
            backend="memory_graph",
            unavailable_reason=None if self._exists else "graph_absent",
        )

    def bootstrap(self) -> GraphHandle:
        self._exists = True
        self.closed = False
        return GraphHandle(
            board_id="_global",
            storage_ref=self._storage_ref(),
            opened=True,
            status="opened",
            locked=False,
            quarantined=False,
        )

    def ensure_layer_schema(self) -> tuple[str, ...]:
        return ()

    def execute(self, statement: str, params=None) -> GraphStatementResult:
        del statement, params
        if not self._exists:
            raise RuntimeError("global_graph_absent")
        return GraphStatementResult()

    def flush_after_write_batch(self) -> None:
        self.close()

    def close(self) -> None:
        self.closed = True

    def purge(self, *, reason: str = "manual") -> GraphPurgeResult:
        self.purged_reasons.append(reason)
        existed = self._exists
        self._exists = False
        self.closed = True
        return GraphPurgeResult(
            board_id="_global",
            removed=existed,
            not_found=not existed,
            status="purged" if existed else "not_found",
            reason=reason,
            backend="memory_graph",
        )

    def reset_for_tests(self) -> None:
        self.close()


__all__ = ["InMemoryGlobalDiscoveryRuntime"]
