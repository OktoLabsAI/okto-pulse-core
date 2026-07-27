"""Storage port for rebuild/audit JSON artifacts.

The core KG rebuild flow owns the domain rules that decide when and what to
persist. The runtime edition owns where those JSON artifacts live.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from .storage_ref import StorageRef


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
    "global_discovery_reindex",
    "global_discovery_recovery",
    "contingency",
    "stress_evidence",
]

AtomicConsumeOutcome = Literal[
    "consumed",
    "receipt_exists",
    "source_missing",
    "source_mismatch",
    "receipt_conflict",
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

    def __post_init__(self) -> None:
        # These values are logical identifiers, never path fragments.  Keeping
        # that invariant at the edition boundary prevents every filesystem
        # implementation from having to rediscover traversal checks.
        for field_name in ("board_id", "kg_generation_id", "artifact_id"):
            raw = getattr(self, field_name)
            if raw is None:
                continue
            value = str(raw)
            if (
                not value
                or value in {".", ".."}
                or "/" in value
                or "\\" in value
                or "\x00" in value
            ):
                raise ValueError(f"{field_name} must be a safe logical identifier")
            object.__setattr__(self, field_name, value)

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

    def reference(self, key: RebuildAuditKey) -> str:
        """Return the edition's opaque external reference for ``key``."""
        ...

    def read_json_reference(self, reference: str) -> dict[str, Any] | None:
        """Read an opaque reference previously returned by ``reference``."""
        ...

    def write_json_atomic(
        self,
        key: RebuildAuditKey,
        payload: Mapping[str, Any],
    ) -> None: ...

    def read_json(self, key: RebuildAuditKey) -> dict[str, Any] | None: ...

    def exists(self, key: RebuildAuditKey) -> bool: ...

    def delete_json(self, key: RebuildAuditKey) -> bool: ...

    def purge_board_artifacts(self, board_id: str) -> Mapping[str, object]:
        """Physically erase every rebuild/audit/quarantine artifact for a board.

        Implementations must keep the operation within their configured storage
        boundary and fail closed when the board scope of an artifact cannot be
        established.
        """
        ...

    def list_json(self, prefix: RebuildAuditKey) -> Sequence[dict[str, Any]]: ...

    def list_json_bounded(
        self,
        prefix: RebuildAuditKey,
        *,
        max_results: int,
        max_document_bytes: int,
    ) -> Sequence[dict[str, Any]]:
        """List a strictly bounded artifact set.

        Implementations must stop enumeration once ``max_results + 1``
        matching documents are observed and fail instead of truncating.  Each
        document must be rejected before parsing when it exceeds
        ``max_document_bytes``.
        """
        ...

    def replace_json(
        self,
        key: RebuildAuditKey,
        transform: Callable[[dict[str, Any] | None], dict[str, Any]],
    ) -> dict[str, Any]:
        """Run one serialized interprocess read-modify-write transaction.

        The adapter must invoke ``transform`` while holding the same durable
        coordination lock used by all instances for this artifact domain.  If
        the callback raises, the previous value is left unchanged.
        """
        ...

    def replace_json_with_revision(
        self,
        *,
        key: RebuildAuditKey,
        transform: Callable[[dict[str, Any] | None], dict[str, Any]],
        revision_key: RebuildAuditKey,
        revision_transition: Callable[
            [dict[str, Any] | None],
            tuple[dict[str, Any], dict[str, Any]],
        ],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Commit one document behind a durable write-ahead revision fence.

        Under the same interprocess lock, implementations must compute both
        transforms first, persist the pending revision, persist the target,
        and finally persist the committed revision.  A crash may therefore
        leave a pending revision but can never expose an unfenced target
        mutation.  The returned pair is ``(target, committed_revision)``.
        """
        ...

    def consume_json_with_receipt(
        self,
        *,
        source_key: RebuildAuditKey,
        expected_source: Mapping[str, Any],
        receipt_key: RebuildAuditKey,
        receipt_payload: Mapping[str, Any],
    ) -> AtomicConsumeOutcome:
        """Consume ``source_key`` and durably create an authorization receipt.

        The edition adapter must serialize this operation across processes.  A
        matching existing receipt is idempotent; a different receipt at the
        same key is a fail-closed conflict.  The receipt is persisted before
        deleting the source so a crash can burn a token but cannot erase proof
        that the destructive operation was authorized.
        """
        ...

    def quarantine_storage(
        self,
        *,
        board_id: str,
        graph_type: str,
        affected_storage_refs: Sequence[StorageRef],
        reason: str,
        reason_bucket: str,
        correlation_ids: Sequence[str],
        kg_generation_id: str | None,
        retention_days: int,
        scope_storage_refs: Sequence[StorageRef],
        base_storage_ref_hint: StorageRef | None = None,
    ) -> Mapping[str, Any]: ...

    def list_quarantine_manifests(
        self,
        *,
        active_after_iso: str | None = None,
        base_storage_ref_hint: StorageRef | None = None,
    ) -> Sequence[Mapping[str, Any]]: ...

    def read_quarantine_manifest(
        self,
        *,
        quarantine_id: str,
        base_storage_ref_hint: StorageRef | None = None,
    ) -> Mapping[str, Any] | None: ...


class RebuildAuditArtifactStoreResolver(Protocol):
    """Edition adapter that resolves an opaque legacy storage scope.

    The compatibility token is intentionally typed as ``object``. Core callers
    may continue forwarding a historical ``base_dir`` argument, but only an
    edition adapter may interpret that token as a filesystem path, tenant key,
    bucket prefix, or any other concrete storage location.
    """

    def resolve(self, scope: object) -> RebuildAuditArtifactStore: ...


__all__ = [
    "RebuildAuditArtifactStore",
    "RebuildAuditArtifactStoreResolver",
    "REBUILD_AUDIT_GLOBAL_BOARD_ID",
    "RebuildAuditKey",
    "RebuildAuditNamespace",
    "AtomicConsumeOutcome",
]
