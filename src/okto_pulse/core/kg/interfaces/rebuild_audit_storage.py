"""Storage port for rebuild/audit JSON artifacts.

The core KG rebuild flow owns the domain rules that decide when and what to
persist. The runtime edition owns where those JSON artifacts live.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol


RebuildAuditNamespace = Literal[
    "event_audit",
    "cognitive_pending",
    "confirmation_audit",
    "run_audit",
    "generation_current",
    "generation_history",
    "source_manifest",
    "confirmation_token",
    "rebuild_report",
    "candidate_decision",
    "rebaseline_audit",
]

REBUILD_AUDIT_GLOBAL_BOARD_ID = "_global"


@dataclass(frozen=True, slots=True)
class RebuildAuditKey:
    """Logical key for rebuild/audit JSON artifacts.

    ``artifact_id`` identifies event/confirmation/run artifacts.
    ``kg_generation_id`` identifies cognitive pending ledgers and
    generation-history entries.
    """

    namespace: RebuildAuditNamespace
    board_id: str
    kg_generation_id: str | None = None
    artifact_id: str | None = None

    def to_ref(self) -> str:
        parts = [
            "rebuild-audit:/",
            self.namespace,
            self.board_id,
        ]
        if self.kg_generation_id:
            parts.extend(["generation", self.kg_generation_id])
        if self.artifact_id:
            parts.extend(["artifact", self.artifact_id])
        return "/".join(parts)


class RebuildAuditArtifactStore(Protocol):
    """Edition-provided JSON artifact store for rebuild/audit state."""

    def write_json_atomic(
        self,
        key: RebuildAuditKey,
        payload: Mapping[str, Any],
    ) -> None:
        ...

    def read_json(self, key: RebuildAuditKey) -> dict[str, Any] | None:
        ...

    def exists(self, key: RebuildAuditKey) -> bool:
        ...

    def delete_json(self, key: RebuildAuditKey) -> bool:
        ...

    def list_json(self, prefix: RebuildAuditKey) -> Sequence[dict[str, Any]]:
        ...

    def replace_json(
        self,
        key: RebuildAuditKey,
        transform: Callable[[dict[str, Any] | None], dict[str, Any]],
    ) -> dict[str, Any]:
        ...

    def quarantine_paths(
        self,
        *,
        board_id: str,
        graph_type: str,
        affected_paths: Sequence[str],
        reason: str,
        reason_bucket: str,
        correlation_ids: Sequence[str],
        kg_generation_id: str | None,
        retention_days: int,
        scope_roots: Sequence[str],
        base_dir_hint: str | None = None,
    ) -> Mapping[str, Any]:
        ...

    def list_quarantine_manifests(
        self,
        *,
        active_after_iso: str | None = None,
        base_dir_hint: str | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        ...

    def read_quarantine_manifest(
        self,
        *,
        quarantine_id: str,
        base_dir_hint: str | None = None,
    ) -> Mapping[str, Any] | None:
        ...


__all__ = [
    "RebuildAuditArtifactStore",
    "REBUILD_AUDIT_GLOBAL_BOARD_ID",
    "RebuildAuditKey",
    "RebuildAuditNamespace",
]
