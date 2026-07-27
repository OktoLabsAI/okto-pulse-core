"""Test-only fake for the GlobalDiscoveryRuntime port."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

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

    def state(self, *, generation: str | None = None) -> GraphRuntimeState:
        return GraphRuntimeState(
            board_id="_global",
            storage_ref=self._storage_ref(),
            exists=self._exists,
            status="healthy" if self._exists else "absent",
            backend="memory_graph",
            unavailable_reason=None if self._exists else "graph_absent",
            generation=generation,
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
        if not self._exists:
            raise RuntimeError("global_graph_absent")
        if "SET d.source_revoked = $revoked" in statement and params:
            matched = 0
            identities = {str(value) for value in params.get("ids", [])}
            for digest in self.digests.values():
                if (
                    str(digest.get("board_id")) == str(params.get("bid"))
                    and str(digest.get("original_node_id")) in identities
                ):
                    digest["source_revoked"] = bool(params.get("revoked"))
                    matched += 1
            return GraphStatementResult.from_rows([[matched]])
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
        current["decision_count"] = int(values["decision_count"])

    def upsert_decision_digest(self, **values) -> str:
        digest_id = str(values["digest_id"])
        outcome = "updated" if digest_id in self.digests else "created"
        self.digests[digest_id] = {**values, "source_revoked": False}
        return outcome

    def replace_decision_digest_identity(self, **values) -> int:
        board_id = str(values["board_id"])
        original_node_id = str(values["original_node_id"])
        removed = [
            digest_id
            for digest_id, digest in self.digests.items()
            if str(digest.get("board_id")) == board_id
            and str(digest.get("original_node_id")) == original_node_id
        ]
        for digest_id in removed:
            self.digests.pop(digest_id, None)
            self.links.discard((board_id, digest_id))
        digest_id = str(values["digest_id"])
        self.digests[digest_id] = {**values, "source_revoked": False}
        self.links.add((board_id, digest_id))
        return len(removed)

    def link_board_digest(self, *, board_id: str, digest_id: str) -> None:
        self.links.add((board_id, digest_id))

    def delete_decision_digests_guarded(
        self,
        *,
        board_id: str,
        original_node_ids: tuple[str, ...],
        include_malformed: bool = False,
    ) -> int:
        identities = set(original_node_ids)
        removed = [
            digest_id
            for digest_id, digest in self.digests.items()
            if str(digest.get("board_id")) == board_id
            and (
                str(digest.get("original_node_id") or "") in identities
                or (include_malformed and not str(digest.get("original_node_id") or ""))
            )
        ]
        for digest_id in removed:
            self.digests.pop(digest_id, None)
            self.links = {link for link in self.links if link[1] != digest_id}
        return len(removed)

    def delete_decision_digests_for_absent_sources(
        self,
        *,
        board_id: str,
        original_node_ids: tuple[str, ...],
        include_malformed: bool = False,
    ) -> int:
        return self.delete_decision_digests_guarded(
            board_id=board_id,
            original_node_ids=original_node_ids,
            include_malformed=include_malformed,
        )

    def normalize_board_digest_link(
        self,
        *,
        board_id: str,
        digest_id: str,
    ) -> int:
        removed = sum(1 for link in self.links if link[1] == digest_id)
        self.links = {link for link in self.links if link[1] != digest_id}
        self.links.add((board_id, digest_id))
        return removed

    def delete_invalid_board_digest_links(
        self,
        *,
        board_id: str,
        expected_digest_ids: tuple[str, ...],
    ) -> int:
        expected = set(expected_digest_ids)
        invalid = {
            link
            for link in self.links
            if link[0] == board_id
            and (
                link[1] not in expected
                or str(self.digests.get(link[1], {}).get("board_id")) != board_id
            )
        }
        self.links -= invalid
        return len(invalid)

    @contextmanager
    def post_write_verification_scope(self) -> Iterator[None]:
        yield

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

    def erase_storage_for_privacy(
        self,
        *,
        board_id: str,
        reason: str,
        survivor_board_ids: tuple[str, ...] | None = None,
    ) -> dict[str, object]:
        authoritative = (
            set(survivor_board_ids)
            if survivor_board_ids is not None
            else set(self.boards) - {board_id}
        )
        target_digest_ids = {
            digest_id
            for digest_id, digest in self.digests.items()
            if str(digest.get("board_id")) == board_id
        }
        existed = board_id in self.boards or bool(target_digest_ids)
        self.purged_reasons.append(reason)
        self.boards.pop(board_id, None)
        self.boards = {
            key: value for key, value in self.boards.items() if key in authoritative
        }
        for digest_id in target_digest_ids:
            self.digests.pop(digest_id, None)
        self.digests = {
            key: value
            for key, value in self.digests.items()
            if str(value.get("board_id")) in authoritative
        }
        self.links = {
            link
            for link in self.links
            if link[0] in authoritative and link[1] in self.digests
        }
        self._exists = True
        self.closed = True
        return {
            "board_id": board_id,
            "objects_removed": int(existed),
            "directories_removed": 0,
            "verified_absent": True,
            "survivors_restored": {
                "boards": len(self.boards),
                "digests": len(self.digests),
                "relationships": len(self.links),
            },
            "status": "purged" if existed else "not_found",
        }

    def reset_for_tests(self) -> None:
        self.close()


__all__ = ["InMemoryGlobalDiscoveryRuntime"]
