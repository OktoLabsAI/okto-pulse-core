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
        self.boards: dict[str, dict] = {}
        self.digests: dict[str, dict] = {}
        self.links: set[tuple[str, str]] = set()

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

    def search_decision_digests(
        self,
        query_vector: list[float],
        *,
        board_ids: tuple[str, ...],
        graph_layer: str,
        top_k: int,
        min_similarity: float,
        exhaustive: bool = False,
    ) -> list[dict]:
        del query_vector, board_ids, graph_layer, top_k, min_similarity, exhaustive
        return []

    def list_schema_objects(self) -> tuple[str, ...]:
        return ("DecisionDigest",) if self._exists else ()

    def upsert_board_summary(self, **values) -> None:
        board_id = str(values["board_id"])
        current = self.boards.setdefault(board_id, {})
        current.update(values)
        current["decision_count"] = int(current.get("decision_count") or 0) + int(
            values["decision_delta"]
        )

    def upsert_decision_digest(self, **values) -> str:
        digest_id = str(values["digest_id"])
        outcome = "updated" if digest_id in self.digests else "created"
        self.digests[digest_id] = dict(values)
        return outcome

    def link_board_digest(self, *, board_id: str, digest_id: str) -> None:
        self.links.add((board_id, digest_id))

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
