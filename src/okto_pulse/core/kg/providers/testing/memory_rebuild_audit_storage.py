"""In-memory rebuild/audit artifact store for core contract tests."""

from __future__ import annotations

import copy
import threading
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    RebuildAuditArtifactStore,
    RebuildAuditKey,
)


class InMemoryRebuildAuditArtifactStore(RebuildAuditArtifactStore):
    """Thread-safe fake implementing the rebuild audit storage port."""

    def __init__(self) -> None:
        self._records: dict[tuple[str, str, str | None, str | None], dict[str, Any]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _identity(key: RebuildAuditKey) -> tuple[str, str, str | None, str | None]:
        return (key.namespace, key.board_id, key.kg_generation_id, key.artifact_id)

    @staticmethod
    def _matches(prefix: RebuildAuditKey, key: RebuildAuditKey) -> bool:
        return (
            key.namespace == prefix.namespace
            and key.board_id == prefix.board_id
            and (
                prefix.kg_generation_id is None
                or key.kg_generation_id == prefix.kg_generation_id
            )
            and (
                prefix.artifact_id is None
                or key.artifact_id == prefix.artifact_id
            )
        )

    def write_json_atomic(
        self,
        key: RebuildAuditKey,
        payload: Mapping[str, Any],
    ) -> None:
        with self._lock:
            self._records[self._identity(key)] = copy.deepcopy(dict(payload))

    def read_json(self, key: RebuildAuditKey) -> dict[str, Any] | None:
        with self._lock:
            payload = self._records.get(self._identity(key))
            return copy.deepcopy(payload) if payload is not None else None

    def exists(self, key: RebuildAuditKey) -> bool:
        with self._lock:
            return self._identity(key) in self._records

    def list_json(self, prefix: RebuildAuditKey) -> Sequence[dict[str, Any]]:
        with self._lock:
            rows: list[dict[str, Any]] = []
            for raw_key, payload in sorted(self._records.items()):
                key = RebuildAuditKey(
                    namespace=raw_key[0],
                    board_id=raw_key[1],
                    kg_generation_id=raw_key[2],
                    artifact_id=raw_key[3],
                )
                if self._matches(prefix, key):
                    rows.append(copy.deepcopy(payload))
            return rows

    def replace_json(
        self,
        key: RebuildAuditKey,
        transform: Callable[[dict[str, Any] | None], dict[str, Any]],
    ) -> dict[str, Any]:
        with self._lock:
            identity = self._identity(key)
            current = self._records.get(identity)
            next_payload = transform(
                copy.deepcopy(current) if current is not None else None
            )
            self._records[identity] = copy.deepcopy(dict(next_payload))
            return copy.deepcopy(next_payload)


__all__ = ["InMemoryRebuildAuditArtifactStore"]
