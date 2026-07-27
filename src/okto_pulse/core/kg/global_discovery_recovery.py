"""Governed recovery of the rebuildable Global Discovery cache.

This is deliberately separate from board rebuild.  A board rebuild replaces
one authoritative board graph; this flow replaces the derived, global cache
after an unreadable WAL/open failure.  The live global store is never opened
during preflight or candidate construction.
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import contextlib
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

from okto_pulse.core.kg.interfaces.global_discovery_recovery import (
    GlobalDiscoveryArtifactSnapshot,
    GlobalDiscoveryBoardSeed,
    GlobalDiscoveryDigestSeed,
    GlobalDiscoveryRecovery,
)
from okto_pulse.core.kg.global_discovery_recovery_control import (
    DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS,
    RECOVERY_PREPARED_TTL_SECONDS,
    RecoveryPreparationCommand,
    RecoveryPreparedResult,
    RecoveryProgressCounts,
    RecoveryRunBinding,
    RecoveryStartCommand,
    recovery_attempt_id,
)
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    RebuildAuditArtifactStore,
    RebuildAuditKey,
)
from okto_pulse.core.kg.global_discovery_writer import (
    DEFAULT_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS,
    GLOBAL_DISCOVERY_WRITER_SCOPE,
    GlobalDiscoveryWriterContention,
    GlobalDiscoveryWriterLease,
)
from okto_pulse.core.kg.rebuild_confirmation import RebuildConfirmationStore
from okto_pulse.core.kg.single_writer_lock import KGSingleWriterLock
from okto_pulse.core.runtime_context import runtime_lock, runtime_state


GLOBAL_RECOVERY_OPERATION = "reindex_discovery"
GLOBAL_RECOVERY_SCOPE = GLOBAL_DISCOVERY_WRITER_SCOPE
MANIFEST_VERSION = 2
CONFIRMATION_RECEIPT_VERSION = 1
WORKER_INPUT_VERSION = 1
PREPARED_INPUT_VERSION = 1
_MANIFEST_ATTEMPT_BINDING_VERSION = 1
_TERMINAL_STATES = frozenset({"completed", "rolled_back"})
_RESUMABLE_STATES = frozenset({"running", "failed"})
_MANIFEST_REF_RE = re.compile(r"^global_discovery_manifest_[A-Za-z0-9_-]{12,128}$")
_ACTIVE_RUN_ARTIFACT_ID = "active_run"
_ACTIVE_STATUS_BINDING_FIELDS = (
    "run_id",
    "state",
    "actor_id",
    "manifest_ref",
    "preflight_hash",
)
_run_lock = runtime_lock("kg.global_discovery_recovery.run")
_active_run = runtime_state("kg.global_discovery_recovery.active", dict)


class GlobalDiscoveryRecoveryError(RuntimeError):
    def __init__(self, code: str, reason: str) -> None:
        super().__init__(f"{code}: {reason}")
        self.code = code
        self.reason = reason


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def global_discovery_recovery_snapshot_fingerprint(
    *,
    relational_revision_fingerprint: str,
    cognitive_overlay_revision_fingerprint: str,
) -> str:
    """Bind every authoritative recovery input domain into one cheap fence."""

    relational = str(relational_revision_fingerprint).strip()
    cognitive = str(cognitive_overlay_revision_fingerprint).strip()
    if not relational or not cognitive:
        raise ValueError("both recovery revision fingerprints must be non-empty")
    return _canonical_sha256(
        {
            "relational_revision_fingerprint": relational,
            "cognitive_overlay_revision_fingerprint": cognitive,
        }
    )


def _run_artifact_token(run_id: str) -> str:
    """Return a fixed-width, path-safe token for a caller-controlled run id."""

    normalized = str(run_id).strip()
    if not normalized:
        raise ValueError("run_id must be non-empty")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _run_attempt_artifact_id(*, prefix: str, run_id: str, epoch: int) -> str:
    normalized_epoch = int(epoch)
    if normalized_epoch < 1:
        raise ValueError("epoch must be positive")
    return f"{prefix}_{_run_artifact_token(run_id)}__attempt-{normalized_epoch}"


def global_discovery_recovery_run_id(confirmation_id: str) -> str:
    normalized = str(confirmation_id).strip()
    if not normalized:
        raise ValueError("confirmation_id must be non-empty")
    return f"gdr_{hashlib.sha256(normalized.encode()).hexdigest()[:24]}"


def _active_status_binding(document: dict[str, Any]) -> dict[str, object]:
    return {key: document.get(key) for key in _ACTIVE_STATUS_BINDING_FIELDS}


def _active_pointer_document(status: dict[str, Any]) -> dict[str, object]:
    binding = {
        **_active_status_binding(status),
        # A same-run resume can cycle from running -> failed -> running with an
        # otherwise identical binding.  This nonce prevents that ABA from
        # satisfying a stale exact-pointer CAS.
        "active_transition_id": secrets.token_urlsafe(12),
    }
    return {**binding, "active_status_sha256": _canonical_sha256(binding)}


def _status_has_physical_evidence(status: dict[str, Any]) -> bool:
    return bool(
        status.get("candidate_sha256")
        or status.get("quarantine_ref")
        or status.get("recovery_journal_ref")
        or status.get("rollback_performed") is True
        or status.get("physical_cutover_outcome") in {"completed", "rolled_back"}
    )


def _status_holds_recovery_fence(status: dict[str, Any]) -> bool:
    """Fail closed unless durable truth proves that another run may proceed."""

    state = str(status.get("state") or "")
    if state in _TERMINAL_STATES | {"precondition_failed"}:
        return False
    if state in {"running", "delivery_pending"}:
        return True
    if state == "failed":
        # New statuses always carry an explicit monotonic phase marker.  An
        # absent marker is legacy/ambiguous and therefore remains fenced.
        return not (
            status.get("physical_cutover_started") is False
            and not _status_has_physical_evidence(status)
        )
    return True


def _requires_exact_physical_resume(status: dict[str, Any]) -> bool:
    """Allow only the same receipt-bound run to drain ambiguous physical work."""

    if str(status.get("state") or "") not in _RESUMABLE_STATES:
        return False
    if _status_has_physical_evidence(status):
        return True
    # ``None`` is a legacy status whose phase cannot be proven pre-cutover.
    return status.get("physical_cutover_started") is not False


@dataclass(frozen=True, slots=True)
class GlobalDiscoveryBoardInventory:
    board_id: str
    board_name: str
    source_count: int
    source_set_hash: str

    def to_dict(self) -> dict[str, object]:
        return {
            "board_id": self.board_id,
            "board_name": self.board_name,
            "source_count": self.source_count,
            "source_set_hash": self.source_set_hash,
        }


def inventory_from_source_set(
    *, board_id: str, board_name: str, source_set: object
) -> GlobalDiscoveryBoardInventory:
    """Project a board rebuild source set into stable, payload-free evidence."""

    rows = getattr(source_set, "materializable_sources", ())
    projected = []
    for row in rows:
        to_dict = getattr(row, "to_dict", None)
        raw = dict(to_dict()) if callable(to_dict) else dict(row)
        projected.append(
            {
                "artifact_type": str(raw.get("artifact_type", "")),
                "source_ref": str(raw.get("source_ref", "")),
                "source_version": str(raw.get("source_version", "")),
                "content_hash": str(raw.get("content_hash", "")),
                "id": str(raw.get("id", "")),
                "graph_layer": str(raw.get("graph_layer", "")),
            }
        )
    projected.sort(
        key=lambda row: (
            row["artifact_type"],
            row["source_ref"],
            row["source_version"],
            row["id"],
        )
    )
    return GlobalDiscoveryBoardInventory(
        board_id=board_id,
        board_name=board_name,
        source_count=len(projected),
        source_set_hash=_canonical_sha256(projected),
    )


def inventory_from_source_rows(
    *,
    board_id: str,
    board_name: str,
    source_rows: Iterable[dict[str, Any]],
    cognitive_durable_digest: dict[str, Any],
) -> GlobalDiscoveryBoardInventory:
    """Build recovery inventory from rows captured by one edition snapshot.

    Editions provide preloaded rows instead of a reader callback, so this
    projection cannot open a board or start another relational snapshot.  An
    explicitly supplied cognitive digest (including ``{}``) likewise prevents
    the enumerator from consulting the process-global cognitive store.
    """

    from okto_pulse.core.kg.rebuild_sources import RebuildSourceEnumerator

    rows = [dict(row) for row in source_rows]
    if not isinstance(cognitive_durable_digest, dict):
        raise TypeError("cognitive_durable_digest must be a dict")
    digest = dict(cognitive_durable_digest)
    source_set = RebuildSourceEnumerator(
        source_store=lambda requested_board_id: (
            list(rows) if requested_board_id == board_id else []
        ),
        cognitive_digest_provider=lambda requested_board_id: (
            dict(digest) if requested_board_id == board_id else {}
        ),
    ).enumerate(board_id=board_id)
    return inventory_from_source_set(
        board_id=board_id,
        board_name=board_name,
        source_set=source_set,
    )


def empty_global_discovery_recovery_board_seed(
    *,
    board_id: str,
    board_name: str,
    board_summary: str,
) -> GlobalDiscoveryBoardSeed:
    """Build the canonical recovery seed for an unmaterialized empty board.

    This is the zero-source form of ``GlobalOutboxProcessor``'s board seed.
    The graph-source inventory hash intentionally has a different domain from
    the relational recovery census hash; callers must not supply or conflate
    the two values.
    """

    from okto_pulse.core.kg.embedding import get_embedding_provider

    normalized_board_id = str(board_id).strip()
    if not normalized_board_id:
        raise ValueError("board_id must be non-empty")
    normalized_board_name = str(board_name)
    summary_embedding = tuple(
        float(value)
        for value in get_embedding_provider().encode(
            f"Board {normalized_board_name or normalized_board_id}"
        )
    )
    return GlobalDiscoveryBoardSeed(
        board_id=normalized_board_id,
        board_name=normalized_board_name,
        summary=str(board_summary),
        summary_embedding=summary_embedding,
        digests=(),
        source_inventory_hash=_canonical_sha256({"source_types": [], "digests": []}),
    )


def _validated_health_rows(
    *,
    boards: tuple[GlobalDiscoveryBoardInventory, ...],
    health_evidence: Iterable[dict[str, object]],
    require_recovery_signal: bool = True,
) -> tuple[dict[str, object], ...]:
    health_rows = tuple(
        sorted(
            (
                {
                    "board_id": str(row.get("board_id", "")),
                    "graph_state": str(row.get("graph_state", "")),
                    "discovery_state": str(row.get("discovery_state", "")),
                    "discovery_recovery_required": bool(
                        row.get("discovery_recovery_required", False)
                    ),
                    "primary_health_cause": str(row.get("primary_health_cause", "")),
                }
                for row in health_evidence
            ),
            key=lambda row: str(row["board_id"]),
        )
    )
    if tuple(row["board_id"] for row in health_rows) != tuple(
        row.board_id for row in boards
    ):
        raise GlobalDiscoveryRecoveryError(
            "health_inventory_mismatch",
            "health evidence must cover every manifest board exactly once",
        )
    if any(row["graph_state"] != "healthy" for row in health_rows):
        raise GlobalDiscoveryRecoveryError(
            "board_graph_not_healthy",
            "all authoritative board graphs must be healthy before global recovery",
        )
    if any(row["discovery_state"] == "quarantined" for row in health_rows):
        raise GlobalDiscoveryRecoveryError(
            "discovery_recovery_not_admitted",
            "quarantined discovery requires quarantine incident handling",
        )
    if require_recovery_signal and not any(
        row["discovery_state"] == "recovery_needed"
        and row["discovery_recovery_required"]
        for row in health_rows
    ):
        raise GlobalDiscoveryRecoveryError(
            "discovery_recovery_not_admitted",
            "no concrete discovery recovery_needed signal was observed",
        )
    return health_rows


@dataclass(frozen=True, slots=True)
class GlobalDiscoveryRecoveryManifest:
    manifest_ref: str
    run_id: str
    epoch: int
    attempt_id: str
    actor_id: str
    created_at: str
    prepared_at: datetime
    expires_at: datetime
    snapshot_fingerprint: str
    boards: tuple[GlobalDiscoveryBoardInventory, ...]
    board_inventory_hash: str
    live_artifact: GlobalDiscoveryArtifactSnapshot
    health_evidence: tuple[dict[str, object], ...]
    preflight_hash: str

    def binding_payload(self) -> dict[str, object]:
        return {
            "manifest_version": MANIFEST_VERSION,
            "scope": GLOBAL_RECOVERY_SCOPE,
            "manifest_ref": self.manifest_ref,
            "run_id": self.run_id,
            "epoch": self.epoch,
            "attempt_id": self.attempt_id,
            "actor_id": self.actor_id,
            "created_at": self.created_at,
            "prepared_at": self.prepared_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "snapshot_fingerprint": self.snapshot_fingerprint,
            "boards": [row.to_dict() for row in self.boards],
            "board_inventory_hash": self.board_inventory_hash,
            "live_artifact": self.live_artifact.to_dict(),
            "health_evidence": list(self.health_evidence),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.binding_payload(), "preflight_hash": self.preflight_hash}

    def assert_fresh(
        self,
        *,
        now: datetime,
        current_snapshot_fingerprint: str,
    ) -> None:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if (
            now >= self.expires_at
            or str(current_snapshot_fingerprint) != self.snapshot_fingerprint
        ):
            raise GlobalDiscoveryRecoveryError(
                "manifest_stale",
                "prepared manifest expired or its authoritative snapshot changed",
            )


@dataclass(frozen=True, slots=True)
class GlobalDiscoveryRecoveryPreparedRevocation:
    """Append-only evidence invalidating an unconfirmed prepared attempt."""

    run_id: str
    epoch: int
    attempt_id: str
    manifest_ref: str
    revoked_at: datetime
    requested_by_actor_id: str
    reason: str | None = None

    def __post_init__(self) -> None:
        run_id = str(self.run_id).strip()
        epoch = int(self.epoch)
        if not run_id or epoch < 1:
            raise ValueError("run_id must be non-empty and epoch must be positive")
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(self, "epoch", epoch)
        if self.attempt_id != recovery_attempt_id(run_id, epoch):
            raise ValueError("attempt_id must match run_id and epoch")
        manifest_ref = str(self.manifest_ref).strip()
        actor_id = str(self.requested_by_actor_id).strip()
        if not manifest_ref or not actor_id:
            raise ValueError("manifest_ref and requested_by_actor_id must be non-empty")
        object.__setattr__(self, "manifest_ref", manifest_ref)
        object.__setattr__(self, "requested_by_actor_id", actor_id)
        if self.revoked_at.tzinfo is None or self.revoked_at.utcoffset() is None:
            raise ValueError("revoked_at must be timezone-aware")
        if self.reason is not None:
            reason = str(self.reason).strip()
            if not reason or len(reason) > 512:
                raise ValueError("reason must contain 1..512 characters")
            object.__setattr__(self, "reason", reason)

    def binding_payload(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "epoch": self.epoch,
            "attempt_id": self.attempt_id,
            "manifest_ref": self.manifest_ref,
            "revoked_at": self.revoked_at.isoformat(),
            "requested_by_actor_id": self.requested_by_actor_id,
            "reason": self.reason,
        }

    def semantic_binding(self) -> dict[str, object]:
        payload = self.binding_payload()
        payload.pop("revoked_at")
        return payload

    def to_dict(self) -> dict[str, object]:
        payload = self.binding_payload()
        return {**payload, "evidence_sha256": _canonical_sha256(payload)}

    @classmethod
    def from_dict(
        cls, raw: dict[str, Any]
    ) -> GlobalDiscoveryRecoveryPreparedRevocation:
        payload = {key: value for key, value in raw.items() if key != "evidence_sha256"}
        if raw.get("evidence_sha256") != _canonical_sha256(payload):
            raise ValueError("revocation evidence hash mismatch")
        return cls(
            run_id=str(payload["run_id"]),
            epoch=int(payload["epoch"]),
            attempt_id=str(payload["attempt_id"]),
            manifest_ref=str(payload["manifest_ref"]),
            revoked_at=datetime.fromisoformat(str(payload["revoked_at"])),
            requested_by_actor_id=str(payload["requested_by_actor_id"]),
            reason=(None if payload.get("reason") is None else str(payload["reason"])),
        )


@dataclass(frozen=True, slots=True)
class GlobalDiscoveryRecoveryWorkerInputs:
    """Integrity-bound physical inputs persisted before worker dispatch."""

    command: RecoveryStartCommand
    expected_live_sha256: str
    boards: tuple[GlobalDiscoveryBoardSeed, ...]
    terminal_counts: RecoveryProgressCounts

    def __post_init__(self) -> None:
        if not isinstance(self.command, RecoveryStartCommand):
            raise TypeError("command must be RecoveryStartCommand")
        digest = str(self.expected_live_sha256).strip()
        if not digest:
            raise ValueError("expected_live_sha256 must be non-empty")
        object.__setattr__(self, "expected_live_sha256", digest)
        boards = tuple(sorted(self.boards, key=lambda row: row.board_id))
        if not boards or any(
            not isinstance(row, GlobalDiscoveryBoardSeed) for row in boards
        ):
            raise ValueError("boards must contain GlobalDiscoveryBoardSeed values")
        if len({row.board_id for row in boards}) != len(boards):
            raise ValueError("boards must have unique board ids")
        object.__setattr__(self, "boards", boards)
        if not isinstance(self.terminal_counts, RecoveryProgressCounts):
            raise TypeError("terminal_counts must be RecoveryProgressCounts")

    def _command_payload(self, *, include_started_at: bool) -> dict[str, object]:
        binding = self.command.binding
        payload: dict[str, object] = {
            "binding": {
                "run_id": binding.run_id,
                "actor_id": binding.actor_id,
                "confirmation_fingerprint": binding.confirmation_fingerprint,
                "manifest_ref": binding.manifest_ref,
                "preflight_hash": binding.preflight_hash,
                "reason": binding.reason,
            },
            "counts": self.command.counts.to_dict(),
            "attempt_budget_ms": self.command.attempt_budget_ms,
            "expected_epoch": self.command.expected_epoch,
            "confirmed_by_actor_id": self.command.confirmed_by_actor_id,
        }
        if include_started_at:
            payload["started_at"] = self.command.started_at.isoformat()
            payload["confirmation_consumed_at"] = (
                None
                if self.command.confirmation_consumed_at is None
                else self.command.confirmation_consumed_at.isoformat()
            )
        return payload

    def semantic_payload(self) -> dict[str, object]:
        """Stable collision identity; excludes only the first-writer timestamp."""

        return {
            "input_version": WORKER_INPUT_VERSION,
            "command": self._command_payload(include_started_at=False),
            "expected_live_sha256": self.expected_live_sha256,
            "boards": [row.to_dict() for row in self.boards],
            "terminal_counts": self.terminal_counts.to_dict(),
        }

    def binding_payload(self) -> dict[str, object]:
        return {
            **self.semantic_payload(),
            "command": self._command_payload(include_started_at=True),
            "semantic_sha256": _canonical_sha256(self.semantic_payload()),
        }

    def to_dict(self) -> dict[str, object]:
        payload = self.binding_payload()
        return {**payload, "payload_sha256": _canonical_sha256(payload)}

    @classmethod
    def from_dict(
        cls,
        raw: dict[str, Any],
    ) -> GlobalDiscoveryRecoveryWorkerInputs:
        payload = {key: value for key, value in raw.items() if key != "payload_sha256"}
        if raw.get("payload_sha256") != _canonical_sha256(payload):
            raise ValueError("payload hash mismatch")
        if int(payload["input_version"]) != WORKER_INPUT_VERSION:
            raise ValueError("unsupported worker input version")
        command_raw = dict(payload["command"])
        binding_raw = dict(command_raw["binding"])
        command = RecoveryStartCommand(
            binding=RecoveryRunBinding(
                run_id=str(binding_raw["run_id"]),
                actor_id=str(binding_raw["actor_id"]),
                confirmation_fingerprint=str(binding_raw["confirmation_fingerprint"]),
                manifest_ref=str(binding_raw["manifest_ref"]),
                preflight_hash=str(binding_raw["preflight_hash"]),
                reason=str(binding_raw["reason"]),
            ),
            started_at=datetime.fromisoformat(str(command_raw["started_at"])),
            counts=RecoveryProgressCounts(**dict(command_raw["counts"])),
            attempt_budget_ms=int(command_raw["attempt_budget_ms"]),
            expected_epoch=int(command_raw.get("expected_epoch", 1)),
            confirmed_by_actor_id=str(
                command_raw.get("confirmed_by_actor_id") or binding_raw["actor_id"]
            ),
            confirmation_consumed_at=datetime.fromisoformat(
                str(
                    command_raw.get("confirmation_consumed_at")
                    or command_raw["started_at"]
                )
            ),
        )
        boards = tuple(
            GlobalDiscoveryBoardSeed(
                board_id=str(row["board_id"]),
                board_name=str(row["board_name"]),
                summary=str(row["summary"]),
                summary_embedding=tuple(
                    float(value) for value in row["summary_embedding"]
                ),
                digests=tuple(
                    GlobalDiscoveryDigestSeed(
                        original_node_id=str(digest["original_node_id"]),
                        title=str(digest["title"]),
                        summary=str(digest["summary"]),
                        node_type=str(digest["node_type"]),
                        graph_layer=str(digest["graph_layer"]),
                        source_artifact_ref=str(digest["source_artifact_ref"]),
                        embedding=tuple(float(value) for value in digest["embedding"]),
                    )
                    for digest in row["digests"]
                ),
                source_inventory_hash=str(row["source_inventory_hash"]),
            )
            for row in payload["boards"]
        )
        result = cls(
            command=command,
            expected_live_sha256=str(payload["expected_live_sha256"]),
            boards=boards,
            terminal_counts=RecoveryProgressCounts(**dict(payload["terminal_counts"])),
        )
        if payload.get("semantic_sha256") != _canonical_sha256(
            result.semantic_payload()
        ):
            raise ValueError("semantic hash mismatch")
        return result


class GlobalDiscoveryRecoveryWorkerInputStore:
    """Create-only worker-input store over the edition artifact boundary."""

    def __init__(self, artifact_store: RebuildAuditArtifactStore) -> None:
        self._store = artifact_store

    @staticmethod
    def _key(run_id: str, epoch: int = 1) -> RebuildAuditKey:
        return RebuildAuditKey(
            namespace="global_discovery_recovery",
            board_id=GLOBAL_RECOVERY_SCOPE,
            artifact_id=_run_attempt_artifact_id(
                prefix="worker_inputs", run_id=run_id, epoch=epoch
            ),
        )

    @staticmethod
    def _decode(raw: dict[str, Any]) -> GlobalDiscoveryRecoveryWorkerInputs:
        try:
            return GlobalDiscoveryRecoveryWorkerInputs.from_dict(raw)
        except (KeyError, TypeError, ValueError) as exc:
            raise GlobalDiscoveryRecoveryError(
                "recovery_worker_inputs_invalid",
                "durable worker inputs are missing, corrupt or hash-invalid",
            ) from exc

    def load(
        self, run_id: str, *, epoch: int = 1
    ) -> GlobalDiscoveryRecoveryWorkerInputs | None:
        raw = self._store.read_json(self._key(run_id, epoch))
        return None if raw is None else self._decode(raw)

    def put(
        self,
        inputs: GlobalDiscoveryRecoveryWorkerInputs,
    ) -> GlobalDiscoveryRecoveryWorkerInputs:
        if not isinstance(inputs, GlobalDiscoveryRecoveryWorkerInputs):
            raise TypeError("inputs must be GlobalDiscoveryRecoveryWorkerInputs")
        desired = inputs.to_dict()

        def create_or_replay(current: dict[str, Any] | None) -> dict[str, Any]:
            if current is None:
                return desired
            existing = self._decode(current)
            if existing.semantic_payload() != inputs.semantic_payload():
                raise GlobalDiscoveryRecoveryError(
                    "recovery_worker_inputs_binding_conflict",
                    "run id is already bound to different worker inputs",
                )
            return dict(current)

        stored = self._store.replace_json(
            self._key(
                inputs.command.binding.run_id,
                inputs.command.expected_epoch,
            ),
            create_or_replay,
        )
        return self._decode(stored)


@dataclass(frozen=True, slots=True)
class GlobalDiscoveryRecoveryPreparedInputs:
    """Complete physical inputs staged before any confirmation is issued."""

    run_id: str
    epoch: int
    attempt_id: str
    manifest_ref: str
    preflight_hash: str
    snapshot_fingerprint: str
    expected_live_sha256: str
    boards: tuple[GlobalDiscoveryBoardSeed, ...]
    terminal_counts: RecoveryProgressCounts

    def __post_init__(self) -> None:
        run_id = str(self.run_id).strip()
        epoch = int(self.epoch)
        if not run_id or epoch < 1:
            raise ValueError("run_id must be non-empty and epoch must be positive")
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(self, "epoch", epoch)
        if self.attempt_id != recovery_attempt_id(run_id, epoch):
            raise ValueError("attempt_id must match run_id and epoch")
        for field in (
            "manifest_ref",
            "preflight_hash",
            "snapshot_fingerprint",
            "expected_live_sha256",
        ):
            value = str(getattr(self, field)).strip()
            if not value:
                raise ValueError(f"{field} must be non-empty")
            object.__setattr__(self, field, value)
        boards = tuple(sorted(self.boards, key=lambda row: row.board_id))
        if not boards or any(
            not isinstance(row, GlobalDiscoveryBoardSeed) for row in boards
        ):
            raise ValueError("boards must contain GlobalDiscoveryBoardSeed values")
        if len({row.board_id for row in boards}) != len(boards):
            raise ValueError("boards must have unique board ids")
        object.__setattr__(self, "boards", boards)
        if not isinstance(self.terminal_counts, RecoveryProgressCounts):
            raise TypeError("terminal_counts must be RecoveryProgressCounts")

    def binding_payload(self) -> dict[str, object]:
        return {
            "input_version": PREPARED_INPUT_VERSION,
            "run_id": self.run_id,
            "epoch": self.epoch,
            "attempt_id": self.attempt_id,
            "manifest_ref": self.manifest_ref,
            "preflight_hash": self.preflight_hash,
            "snapshot_fingerprint": self.snapshot_fingerprint,
            "expected_live_sha256": self.expected_live_sha256,
            "boards": [row.to_dict() for row in self.boards],
            "terminal_counts": self.terminal_counts.to_dict(),
        }

    def to_dict(self) -> dict[str, object]:
        payload = self.binding_payload()
        return {**payload, "payload_sha256": _canonical_sha256(payload)}

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> GlobalDiscoveryRecoveryPreparedInputs:
        payload = {key: value for key, value in raw.items() if key != "payload_sha256"}
        if raw.get("payload_sha256") != _canonical_sha256(payload):
            raise ValueError("payload hash mismatch")
        if int(payload["input_version"]) != PREPARED_INPUT_VERSION:
            raise ValueError("unsupported prepared input version")
        boards = tuple(
            GlobalDiscoveryBoardSeed(
                board_id=str(row["board_id"]),
                board_name=str(row["board_name"]),
                summary=str(row["summary"]),
                summary_embedding=tuple(
                    float(value) for value in row["summary_embedding"]
                ),
                digests=tuple(
                    GlobalDiscoveryDigestSeed(
                        original_node_id=str(digest["original_node_id"]),
                        title=str(digest["title"]),
                        summary=str(digest["summary"]),
                        node_type=str(digest["node_type"]),
                        graph_layer=str(digest["graph_layer"]),
                        source_artifact_ref=str(digest["source_artifact_ref"]),
                        embedding=tuple(float(value) for value in digest["embedding"]),
                    )
                    for digest in row["digests"]
                ),
                source_inventory_hash=str(row["source_inventory_hash"]),
            )
            for row in payload["boards"]
        )
        return cls(
            run_id=str(payload["run_id"]),
            epoch=int(payload["epoch"]),
            attempt_id=str(payload["attempt_id"]),
            manifest_ref=str(payload["manifest_ref"]),
            preflight_hash=str(payload["preflight_hash"]),
            snapshot_fingerprint=str(payload["snapshot_fingerprint"]),
            expected_live_sha256=str(payload["expected_live_sha256"]),
            boards=boards,
            terminal_counts=RecoveryProgressCounts(**dict(payload["terminal_counts"])),
        )


class GlobalDiscoveryRecoveryPreparedInputStore:
    """Create-only staged-input store keyed by physical run attempt."""

    def __init__(self, artifact_store: RebuildAuditArtifactStore) -> None:
        self._store = artifact_store

    @staticmethod
    def _key(run_id: str, epoch: int = 1) -> RebuildAuditKey:
        return RebuildAuditKey(
            namespace="global_discovery_recovery",
            board_id=GLOBAL_RECOVERY_SCOPE,
            artifact_id=_run_attempt_artifact_id(
                prefix="prepared_inputs", run_id=run_id, epoch=epoch
            ),
        )

    @staticmethod
    def _decode(raw: dict[str, Any]) -> GlobalDiscoveryRecoveryPreparedInputs:
        try:
            return GlobalDiscoveryRecoveryPreparedInputs.from_dict(raw)
        except (KeyError, TypeError, ValueError) as exc:
            raise GlobalDiscoveryRecoveryError(
                "recovery_prepared_inputs_invalid",
                "prepared inputs are missing, corrupt or hash-invalid",
            ) from exc

    def load(
        self, run_id: str, *, epoch: int = 1
    ) -> GlobalDiscoveryRecoveryPreparedInputs | None:
        raw = self._store.read_json(self._key(run_id, epoch))
        return None if raw is None else self._decode(raw)

    def put(
        self, inputs: GlobalDiscoveryRecoveryPreparedInputs
    ) -> GlobalDiscoveryRecoveryPreparedInputs:
        if not isinstance(inputs, GlobalDiscoveryRecoveryPreparedInputs):
            raise TypeError("inputs must be GlobalDiscoveryRecoveryPreparedInputs")
        desired = inputs.to_dict()

        def create_or_replay(current: dict[str, Any] | None) -> dict[str, Any]:
            if current is None:
                return desired
            existing = self._decode(current)
            if existing != inputs:
                raise GlobalDiscoveryRecoveryError(
                    "recovery_prepared_inputs_binding_conflict",
                    "run id is already bound to different prepared inputs",
                )
            return dict(current)

        stored = self._store.replace_json(
            self._key(inputs.run_id, inputs.epoch), create_or_replay
        )
        return self._decode(stored)


class GlobalDiscoveryRecoveryManifestStore:
    def __init__(self, artifact_store: RebuildAuditArtifactStore) -> None:
        self._store = artifact_store

    @staticmethod
    def _key(artifact_id: str) -> RebuildAuditKey:
        return RebuildAuditKey(
            namespace="global_discovery_recovery",
            board_id=GLOBAL_RECOVERY_SCOPE,
            artifact_id=artifact_id,
        )

    @classmethod
    def status_key(cls, run_id: str) -> RebuildAuditKey:
        return cls._key(f"status_{_run_artifact_token(run_id)}")

    @classmethod
    def _attempt_binding_key(cls, run_id: str, epoch: int) -> RebuildAuditKey:
        return cls._key(
            _run_attempt_artifact_id(
                prefix="manifest_binding",
                run_id=run_id,
                epoch=epoch,
            )
        )

    @staticmethod
    def _decode_attempt_binding(raw: dict[str, Any]) -> dict[str, object]:
        payload = {key: value for key, value in raw.items() if key != "payload_sha256"}
        if raw.get("payload_sha256") != _canonical_sha256(payload):
            raise GlobalDiscoveryRecoveryError(
                "manifest_attempt_binding_invalid",
                "prepared manifest attempt binding is hash-invalid",
            )
        try:
            if int(payload["binding_version"]) != _MANIFEST_ATTEMPT_BINDING_VERSION:
                raise ValueError("unsupported binding version")
            run_id = str(payload["run_id"]).strip()
            epoch = int(payload["epoch"])
            attempt_id = str(payload["attempt_id"])
            prepared_at = datetime.fromisoformat(str(payload["prepared_at"]))
            manifest_ref = str(payload["manifest_ref"])
            input_sha256 = str(payload["input_sha256"])
        except (KeyError, TypeError, ValueError) as exc:
            raise GlobalDiscoveryRecoveryError(
                "manifest_attempt_binding_invalid",
                "prepared manifest attempt binding is malformed",
            ) from exc
        if (
            not run_id
            or epoch < 1
            or attempt_id != recovery_attempt_id(run_id, epoch)
            or prepared_at.tzinfo is None
            or prepared_at.utcoffset() is None
            or _MANIFEST_REF_RE.fullmatch(manifest_ref) is None
            or re.fullmatch(r"[0-9a-f]{64}", input_sha256) is None
        ):
            raise GlobalDiscoveryRecoveryError(
                "manifest_attempt_binding_invalid",
                "prepared manifest attempt binding is incoherent",
            )
        return {
            "binding_version": _MANIFEST_ATTEMPT_BINDING_VERSION,
            "run_id": run_id,
            "epoch": epoch,
            "attempt_id": attempt_id,
            "prepared_at": prepared_at,
            "manifest_ref": manifest_ref,
            "input_sha256": input_sha256,
        }

    def load_attempt_manifest(
        self,
        run_id: str,
        *,
        epoch: int,
    ) -> GlobalDiscoveryRecoveryManifest | None:
        """Resolve an early publication binding after an abrupt process stop."""

        raw = self._store.read_json(self._attempt_binding_key(run_id, epoch))
        if raw is None:
            return None
        binding = self._decode_attempt_binding(raw)
        if binding["run_id"] != str(run_id) or binding["epoch"] != int(epoch):
            raise GlobalDiscoveryRecoveryError(
                "manifest_attempt_binding_invalid",
                "prepared manifest binding does not match its storage key",
            )
        manifest = self.load(str(binding["manifest_ref"]))
        if manifest is None:
            return None
        if manifest.run_id != str(run_id) or manifest.epoch != int(epoch):
            raise GlobalDiscoveryRecoveryError(
                "manifest_attempt_binding_invalid",
                "prepared manifest does not match its attempt binding",
            )
        return manifest

    def build(
        self,
        *,
        run_id: str | None = None,
        epoch: int = 1,
        actor_id: str,
        boards: Iterable[GlobalDiscoveryBoardInventory],
        live_artifact: GlobalDiscoveryArtifactSnapshot,
        health_evidence: Iterable[dict[str, object]],
        snapshot_fingerprint: str | None = None,
        prepared_at: datetime | None = None,
    ) -> GlobalDiscoveryRecoveryManifest:
        ordered = tuple(sorted(boards, key=lambda row: row.board_id))
        if len({row.board_id for row in ordered}) != len(ordered):
            raise GlobalDiscoveryRecoveryError(
                "duplicate_board_in_manifest", "board ids must be unique"
            )
        board_inventory_hash = _canonical_sha256([row.to_dict() for row in ordered])
        health_rows = _validated_health_rows(
            boards=ordered,
            health_evidence=health_evidence,
        )
        attempt_bound = run_id is not None
        normalized_run_id = str(run_id or f"gdr_{secrets.token_urlsafe(18)}").strip()
        normalized_epoch = int(epoch)
        if not normalized_run_id or normalized_epoch < 1:
            raise ValueError("run_id must be non-empty and epoch must be positive")
        prepared = prepared_at or datetime.now(timezone.utc)
        if prepared.tzinfo is None or prepared.utcoffset() is None:
            raise ValueError("prepared_at must be timezone-aware")
        fingerprint = str(snapshot_fingerprint or live_artifact.sha256).strip()
        if not fingerprint:
            raise ValueError("snapshot_fingerprint must be non-empty")
        non_temporal_payload = {
            "manifest_version": MANIFEST_VERSION,
            "scope": GLOBAL_RECOVERY_SCOPE,
            "run_id": normalized_run_id,
            "epoch": normalized_epoch,
            "attempt_id": recovery_attempt_id(normalized_run_id, normalized_epoch),
            "actor_id": str(actor_id),
            "snapshot_fingerprint": fingerprint,
            "boards": [row.to_dict() for row in ordered],
            "board_inventory_hash": board_inventory_hash,
            "live_artifact": live_artifact.to_dict(),
            "health_evidence": list(health_rows),
        }

        def temporal_payload(value: datetime) -> dict[str, object]:
            return {
                **non_temporal_payload,
                "created_at": value.isoformat(),
                "prepared_at": value.isoformat(),
                "expires_at": (
                    value + timedelta(seconds=RECOVERY_PREPARED_TTL_SECONDS)
                ).isoformat(),
            }

        if attempt_bound:
            proposed_ref = (
                "global_discovery_manifest_"
                f"{_canonical_sha256(temporal_payload(prepared))}"
            )
            input_sha256 = _canonical_sha256(non_temporal_payload)
            binding_payload: dict[str, object] = {
                "binding_version": _MANIFEST_ATTEMPT_BINDING_VERSION,
                "run_id": normalized_run_id,
                "epoch": normalized_epoch,
                "attempt_id": recovery_attempt_id(
                    normalized_run_id,
                    normalized_epoch,
                ),
                "prepared_at": prepared.isoformat(),
                "manifest_ref": proposed_ref,
                "input_sha256": input_sha256,
            }
            desired_binding = {
                **binding_payload,
                "payload_sha256": _canonical_sha256(binding_payload),
            }

            def bind_attempt(current: dict[str, Any] | None) -> dict[str, Any]:
                if current is None:
                    return desired_binding
                existing_binding = self._decode_attempt_binding(current)
                if (
                    existing_binding["run_id"] != normalized_run_id
                    or existing_binding["epoch"] != normalized_epoch
                    or existing_binding["attempt_id"]
                    != recovery_attempt_id(normalized_run_id, normalized_epoch)
                    or existing_binding["input_sha256"] != input_sha256
                ):
                    raise GlobalDiscoveryRecoveryError(
                        "manifest_attempt_binding_conflict",
                        "run attempt is already bound to different prepared inputs",
                    )
                return dict(current)

            stored_binding = self._decode_attempt_binding(
                self._store.replace_json(
                    self._attempt_binding_key(normalized_run_id, normalized_epoch),
                    bind_attempt,
                )
            )
            prepared = stored_binding["prepared_at"]  # type: ignore[assignment]

        semantic_payload = temporal_payload(prepared)
        manifest_ref = (
            f"global_discovery_manifest_{_canonical_sha256(semantic_payload)}"
        )
        if attempt_bound and stored_binding["manifest_ref"] != manifest_ref:
            raise GlobalDiscoveryRecoveryError(
                "manifest_attempt_binding_invalid",
                "prepared manifest reference differs from its attempt binding",
            )
        manifest = GlobalDiscoveryRecoveryManifest(
            manifest_ref=manifest_ref,
            run_id=normalized_run_id,
            epoch=normalized_epoch,
            attempt_id=recovery_attempt_id(normalized_run_id, normalized_epoch),
            actor_id=actor_id,
            created_at=prepared.isoformat(),
            prepared_at=prepared,
            expires_at=prepared + timedelta(seconds=RECOVERY_PREPARED_TTL_SECONDS),
            snapshot_fingerprint=fingerprint,
            boards=ordered,
            board_inventory_hash=board_inventory_hash,
            live_artifact=live_artifact,
            health_evidence=health_rows,
            preflight_hash="",
        )
        manifest = replace(
            manifest,
            preflight_hash=_canonical_sha256(manifest.binding_payload()),
        )
        collided = False

        def create_only(current: dict[str, Any] | None) -> dict[str, Any]:
            nonlocal collided
            if current is not None:
                if current == manifest.to_dict():
                    return dict(current)
                collided = True
                return dict(current)
            return manifest.to_dict()

        self._store.replace_json(self._key(manifest.manifest_ref), create_only)
        if collided:
            raise GlobalDiscoveryRecoveryError(
                "manifest_ref_collision",
                "generated manifest reference already exists",
            )
        return manifest

    def load(self, manifest_ref: str) -> GlobalDiscoveryRecoveryManifest | None:
        if _MANIFEST_REF_RE.fullmatch(manifest_ref) is None:
            return None
        raw = self._store.read_json(self._key(manifest_ref))
        if raw is None:
            return None
        try:
            if int(raw["manifest_version"]) != MANIFEST_VERSION:
                return None
            if raw["scope"] != GLOBAL_RECOVERY_SCOPE:
                return None
            boards = tuple(
                GlobalDiscoveryBoardInventory(
                    board_id=str(row["board_id"]),
                    board_name=str(row.get("board_name", "")),
                    source_count=int(row["source_count"]),
                    source_set_hash=str(row["source_set_hash"]),
                )
                for row in raw["boards"]
            )
            artifact = raw["live_artifact"]
            result = GlobalDiscoveryRecoveryManifest(
                manifest_ref=str(raw["manifest_ref"]),
                run_id=str(raw["run_id"]),
                epoch=int(raw["epoch"]),
                attempt_id=str(raw["attempt_id"]),
                actor_id=str(raw["actor_id"]),
                created_at=str(raw["created_at"]),
                prepared_at=datetime.fromisoformat(str(raw["prepared_at"])),
                expires_at=datetime.fromisoformat(str(raw["expires_at"])),
                snapshot_fingerprint=str(raw["snapshot_fingerprint"]),
                boards=boards,
                board_inventory_hash=str(raw["board_inventory_hash"]),
                live_artifact=GlobalDiscoveryArtifactSnapshot(
                    exists=bool(artifact["exists"]),
                    artifact_count=int(artifact["artifact_count"]),
                    total_bytes=int(artifact["total_bytes"]),
                    sha256=str(artifact["sha256"]),
                ),
                health_evidence=tuple(dict(row) for row in raw["health_evidence"]),
                preflight_hash=str(raw["preflight_hash"]),
            )
        except (KeyError, TypeError, ValueError):
            return None
        if result.manifest_ref != manifest_ref:
            return None
        content_payload = result.binding_payload()
        content_payload.pop("manifest_ref", None)
        if result.manifest_ref != (
            f"global_discovery_manifest_{_canonical_sha256(content_payload)}"
        ):
            return None
        if result.attempt_id != recovery_attempt_id(result.run_id, result.epoch):
            return None
        if result.expires_at != result.prepared_at + timedelta(
            seconds=RECOVERY_PREPARED_TTL_SECONDS
        ):
            return None
        if result.created_at != result.prepared_at.isoformat():
            return None
        if result.preflight_hash != _canonical_sha256(result.binding_payload()):
            return None
        if result.board_inventory_hash != _canonical_sha256(
            [row.to_dict() for row in result.boards]
        ):
            return None
        if tuple(sorted(row.board_id for row in result.boards)) != tuple(
            row.board_id for row in result.boards
        ):
            return None
        return result

    @classmethod
    def prepared_revocation_key(cls, *, run_id: str, epoch: int) -> RebuildAuditKey:
        return cls._key(
            _run_attempt_artifact_id(
                prefix="prepared_revocation", run_id=run_id, epoch=epoch
            )
        )

    def revoke_prepared(
        self,
        *,
        run_id: str,
        epoch: int,
        manifest_ref: str,
        revoked_at: datetime,
        requested_by_actor_id: str,
        reason: str | None,
    ) -> GlobalDiscoveryRecoveryPreparedRevocation:
        manifest = self.load(manifest_ref)
        if (
            manifest is None
            or manifest.run_id != str(run_id)
            or manifest.epoch != int(epoch)
        ):
            raise GlobalDiscoveryRecoveryError(
                "manifest_invalid", "prepared manifest binding is invalid"
            )
        evidence = GlobalDiscoveryRecoveryPreparedRevocation(
            run_id=manifest.run_id,
            epoch=manifest.epoch,
            attempt_id=manifest.attempt_id,
            manifest_ref=manifest.manifest_ref,
            revoked_at=revoked_at,
            requested_by_actor_id=requested_by_actor_id,
            reason=reason,
        )
        desired = evidence.to_dict()

        def create_or_replay(current: dict[str, Any] | None) -> dict[str, Any]:
            if current is None:
                return desired
            existing = GlobalDiscoveryRecoveryPreparedRevocation.from_dict(current)
            if existing.semantic_binding() != evidence.semantic_binding():
                raise GlobalDiscoveryRecoveryError(
                    "prepared_revocation_conflict",
                    "prepared attempt already has different revocation evidence",
                )
            return dict(current)

        stored = self._store.replace_json(
            self.prepared_revocation_key(run_id=run_id, epoch=epoch),
            create_or_replay,
        )
        return GlobalDiscoveryRecoveryPreparedRevocation.from_dict(stored)

    def read_prepared_revocation(
        self, *, run_id: str, epoch: int
    ) -> GlobalDiscoveryRecoveryPreparedRevocation | None:
        raw = self._store.read_json(
            self.prepared_revocation_key(run_id=run_id, epoch=epoch)
        )
        if raw is None:
            return None
        try:
            return GlobalDiscoveryRecoveryPreparedRevocation.from_dict(raw)
        except (KeyError, TypeError, ValueError) as exc:
            raise GlobalDiscoveryRecoveryError(
                "prepared_revocation_invalid",
                "prepared revocation evidence is corrupt or hash-invalid",
            ) from exc

    def write_status(self, run_id: str, payload: dict[str, object]) -> bool:
        document = {**payload, "run_id": run_id}
        self._store.write_json_atomic(
            self.status_key(run_id),
            document,
        )
        return self._update_active_pointer(run_id=run_id, document=document)

    def _update_active_pointer(
        self,
        *,
        run_id: str,
        document: dict[str, object],
    ) -> bool:
        desired_active = _active_pointer_document(document)
        active_updated = False

        def update_active(current: dict[str, Any] | None) -> dict[str, Any]:
            nonlocal active_updated
            if current is not None:
                self._validate_active_document(current)
            if (
                current is not None
                and current.get("run_id") != run_id
                and current.get("state") in {"running", "delivery_pending"}
            ):
                return dict(current)
            active_updated = True
            return dict(desired_active)

        active = self._store.replace_json(
            self._key(_ACTIVE_RUN_ARTIFACT_ID),
            update_active,
        )
        return active_updated or active == desired_active

    def compare_and_set_status(
        self,
        run_id: str,
        *,
        expected_states: frozenset[str] | None,
        payload: dict[str, object],
    ) -> tuple[dict[str, Any], bool]:
        """Create/transition a run status through the store's durable CAS."""

        class _StatusCASMiss(Exception):
            pass

        changed = False
        document = {**payload, "run_id": run_id}

        def transform(current: dict[str, Any] | None) -> dict[str, Any]:
            nonlocal changed
            admitted = (
                current is None
                if expected_states is None
                else current is not None
                and str(current.get("state")) in expected_states
            )
            if not admitted:
                if current is None:
                    raise _StatusCASMiss
                return dict(current)
            changed = True
            return dict(document)

        try:
            result = self._store.replace_json(
                self.status_key(run_id),
                transform,
            )
        except _StatusCASMiss:
            return {}, False
        if changed:
            self._update_active_pointer(run_id=run_id, document=document)
        return result, changed

    def read_status(self, run_id: str) -> dict[str, Any] | None:
        return self._store.read_json(self.status_key(run_id))

    @staticmethod
    def _validate_active_document(raw: dict[str, Any]) -> dict[str, object]:
        binding = _active_status_binding(raw)
        transition_id = raw.get("active_transition_id")
        if transition_id is not None:
            if not isinstance(transition_id, str) or not transition_id:
                raise GlobalDiscoveryRecoveryError(
                    "active_run_status_invalid",
                    "durable active-run pointer has an invalid transition id",
                )
            binding["active_transition_id"] = transition_id
        if raw.get("active_status_sha256") != _canonical_sha256(binding):
            raise GlobalDiscoveryRecoveryError(
                "active_run_status_invalid",
                "durable active-run pointer is corrupt or hash-invalid",
            )
        if not all(
            isinstance(binding.get(key), str) and bool(binding.get(key))
            for key in ("run_id", "state", "actor_id", "manifest_ref", "preflight_hash")
        ):
            raise GlobalDiscoveryRecoveryError(
                "active_run_status_invalid",
                "durable active-run pointer has an incomplete binding",
            )
        return binding

    def _read_active_document(self) -> dict[str, Any] | None:
        raw = self._store.read_json(self._key(_ACTIVE_RUN_ARTIFACT_ID))
        if raw is None:
            return None
        self._validate_active_document(raw)
        return raw

    def read_active_status(self) -> dict[str, Any] | None:
        raw = self._read_active_document()
        return dict(_active_status_binding(raw)) if raw is not None else None

    def read_reconciled_active_status(self) -> dict[str, Any] | None:
        """Return durable owner truth and CAS-repair only proven stale pointers."""

        observed = self._read_active_document()
        if observed is None:
            return None
        active = _active_status_binding(observed)
        run_id = str(active["run_id"])
        truth = self.read_status(run_id)
        if truth is None:
            raise GlobalDiscoveryRecoveryError(
                "active_run_status_invalid",
                "durable active-run pointer has no owner status",
            )
        if truth.get("run_id") != run_id or any(
            truth.get(key) != active.get(key)
            for key in ("actor_id", "manifest_ref", "preflight_hash")
        ):
            raise GlobalDiscoveryRecoveryError(
                "active_run_status_invalid",
                "durable active-run pointer and owner status bindings differ",
            )
        if _status_holds_recovery_fence(truth):
            return dict(truth)

        desired = _active_pointer_document(truth)
        reconciled = False

        class _ActiveCASMiss(Exception):
            pass

        def reconcile(current: dict[str, Any] | None) -> dict[str, Any]:
            nonlocal reconciled
            if current is None:
                raise _ActiveCASMiss
            self._validate_active_document(current)
            if current != observed:
                return dict(current)
            reconciled = True
            return dict(desired)

        try:
            current = self._store.replace_json(
                self._key(_ACTIVE_RUN_ARTIFACT_ID),
                reconcile,
            )
        except _ActiveCASMiss:
            # A concurrent deletion is not proof that the old owner released.
            return {**active, "state": "unknown"}
        if not reconciled:
            # Preserve a concurrent owner and force this caller to retry its
            # preflight instead of reasoning from a second non-atomic read.
            return {**_active_status_binding(current), "state": "unknown"}
        return dict(truth)

    @classmethod
    def confirmation_receipt_key(cls, run_id: str) -> RebuildAuditKey:
        return cls._key(f"confirmation_receipt_{run_id}")

    def has_confirmation_receipt(
        self,
        *,
        run_id: str,
        expected: dict[str, object],
    ) -> bool:
        raw = self._store.read_json(self.confirmation_receipt_key(run_id))
        if raw is None:
            return False
        if raw != expected:
            raise GlobalDiscoveryRecoveryError(
                "confirmation_receipt_invalid",
                "durable confirmation receipt is corrupt or binding-mismatched",
            )
        binding = {key: value for key, value in raw.items() if key != "receipt_sha256"}
        if raw.get("receipt_sha256") != _canonical_sha256(binding):
            raise GlobalDiscoveryRecoveryError(
                "confirmation_receipt_invalid",
                "durable confirmation receipt hash is invalid",
            )
        return True

    @classmethod
    def issued_confirmation_key(cls, confirmation_fingerprint: str) -> RebuildAuditKey:
        return cls._key(f"issued_confirmation_{confirmation_fingerprint}")

    def bind_issued_confirmation(
        self,
        *,
        confirmation_id: str,
        run_id: str,
        epoch: int,
        manifest_ref: str,
        preflight_hash: str,
        confirmed_by_actor_id: str,
        expires_at: str,
    ) -> dict[str, Any]:
        fingerprint = hashlib.sha256(confirmation_id.encode("utf-8")).hexdigest()
        binding: dict[str, object] = {
            "confirmation_fingerprint": fingerprint,
            "run_id": run_id,
            "epoch": int(epoch),
            "manifest_ref": manifest_ref,
            "preflight_hash": preflight_hash,
            "confirmed_by_actor_id": confirmed_by_actor_id,
            "expires_at": expires_at,
        }
        desired = {**binding, "binding_sha256": _canonical_sha256(binding)}

        def create_or_replay(current: dict[str, Any] | None) -> dict[str, Any]:
            if current is None:
                return desired
            if current != desired:
                raise GlobalDiscoveryRecoveryError(
                    "confirmation_binding_conflict",
                    "confirmation is already bound to another prepared run",
                )
            return dict(current)

        return self._store.replace_json(
            self.issued_confirmation_key(fingerprint), create_or_replay
        )

    def resolve_issued_confirmation(self, confirmation_id: str) -> dict[str, Any]:
        fingerprint = hashlib.sha256(str(confirmation_id).encode("utf-8")).hexdigest()
        raw = self._store.read_json(self.issued_confirmation_key(fingerprint))
        if raw is None:
            raise GlobalDiscoveryRecoveryError(
                "confirmation_refused", "issued confirmation binding is missing"
            )
        binding = {key: value for key, value in raw.items() if key != "binding_sha256"}
        if (
            raw.get("binding_sha256") != _canonical_sha256(binding)
            or binding.get("confirmation_fingerprint") != fingerprint
        ):
            raise GlobalDiscoveryRecoveryError(
                "confirmation_refused", "issued confirmation binding is corrupt"
            )
        return raw


class GlobalDiscoveryRecoveryService:
    def __init__(
        self,
        *,
        recovery: GlobalDiscoveryRecovery,
        artifact_store: RebuildAuditArtifactStore,
        confirmation_store: RebuildConfirmationStore | None = None,
        single_writer_lock: KGSingleWriterLock | None = None,
    ) -> None:
        self._recovery = recovery
        self._manifests = GlobalDiscoveryRecoveryManifestStore(artifact_store)
        self._prepared_inputs = GlobalDiscoveryRecoveryPreparedInputStore(
            artifact_store
        )
        self._worker_inputs = GlobalDiscoveryRecoveryWorkerInputStore(artifact_store)
        self._confirmation = confirmation_store or RebuildConfirmationStore(
            artifact_store=artifact_store
        )
        self._single_writer = single_writer_lock or KGSingleWriterLock()
        # Bind context-owned synchronization once.  The service is dispatched
        # to worker threads, which do not necessarily inherit ContextVars.
        self._run_lock = _run_lock.bind()
        self._active_run = _active_run.resolve()

    def current_snapshot_fingerprint(self) -> str:
        """Return the one cheap authoritative freshness fence allowed at start."""

        reader = getattr(self._recovery, "current_snapshot_fingerprint", None)
        if not callable(reader):
            raise GlobalDiscoveryRecoveryError(
                "snapshot_fingerprint_unavailable",
                "authoritative source snapshot fingerprint is unavailable",
            )
        value = reader()
        normalized = str(value).strip()
        if not normalized:
            raise GlobalDiscoveryRecoveryError(
                "snapshot_fingerprint_unavailable",
                "authoritative source snapshot fingerprint is unavailable",
            )
        return normalized

    def new_preparation_command(
        self,
        *,
        actor_id: str,
        admitted_at: datetime | None = None,
        attempt_budget_ms: int = DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS,
        run_id: str | None = None,
    ) -> RecoveryPreparationCommand:
        return RecoveryPreparationCommand(
            binding=RecoveryRunBinding(
                run_id=str(run_id or f"gdr_{secrets.token_urlsafe(18)}"),
                actor_id=actor_id,
            ),
            admitted_at=admitted_at or datetime.now(timezone.utc),
            counts=RecoveryProgressCounts(),
            attempt_budget_ms=attempt_budget_ms,
        )

    def revoke_prepared_run(
        self,
        *,
        run_id: str,
        epoch: int,
        manifest_ref: str,
        requested_by_actor_id: str,
        reason: str | None,
        revoked_at: datetime | None = None,
    ) -> GlobalDiscoveryRecoveryPreparedRevocation:
        """Append revocation evidence without deleting create-only artifacts."""

        return self._manifests.revoke_prepared(
            run_id=run_id,
            epoch=epoch,
            manifest_ref=manifest_ref,
            revoked_at=revoked_at or datetime.now(timezone.utc),
            requested_by_actor_id=requested_by_actor_id,
            reason=reason,
        )

    def stage_prepared_inputs(
        self,
        *,
        run_id: str,
        epoch: int,
        actor_id: str,
        boards: Iterable[GlobalDiscoveryBoardInventory],
        health_evidence: Iterable[dict[str, object]],
        candidate_boards: Iterable[GlobalDiscoveryBoardSeed],
        expected_snapshot_fingerprint: str,
        fence_check: Callable[[], None],
        prepared_at: datetime | None = None,
        terminal_counts: RecoveryProgressCounts | None = None,
    ) -> RecoveryPreparedResult:
        """Validate and persist complete worker inputs before exposing prepared."""

        expected = str(expected_snapshot_fingerprint).strip()
        if not expected:
            raise ValueError("expected_snapshot_fingerprint must be non-empty")
        if not callable(fence_check):
            raise TypeError("fence_check must be callable")
        if terminal_counts is not None and not isinstance(
            terminal_counts, RecoveryProgressCounts
        ):
            raise TypeError("terminal_counts must be RecoveryProgressCounts")
        existing = self._prepared_inputs.load(run_id, epoch=epoch)
        bound_manifest = self._manifests.load_attempt_manifest(
            run_id,
            epoch=epoch,
        )
        if (
            self._manifests.read_prepared_revocation(
                run_id=run_id,
                epoch=epoch,
            )
            is not None
        ):
            raise GlobalDiscoveryRecoveryError(
                "preparation_not_prepared",
                "prepared attempt has immutable revocation evidence",
            )
        observed = self.current_snapshot_fingerprint()
        if observed != expected:
            manifest_to_revoke = bound_manifest
            if existing is not None:
                manifest_to_revoke = self._manifests.load(existing.manifest_ref)
            if manifest_to_revoke is not None:
                with contextlib.suppress(Exception):
                    self._manifests.revoke_prepared(
                        run_id=manifest_to_revoke.run_id,
                        epoch=manifest_to_revoke.epoch,
                        manifest_ref=manifest_to_revoke.manifest_ref,
                        revoked_at=datetime.now(timezone.utc),
                        requested_by_actor_id=actor_id,
                        reason="preparation snapshot changed after artifact publication",
                    )
            raise GlobalDiscoveryRecoveryError(
                "snapshot_drift",
                "captured preparation snapshot changed before staging",
            )

        def assert_expected_snapshot(reason: str) -> None:
            if self.current_snapshot_fingerprint() != expected:
                raise GlobalDiscoveryRecoveryError("snapshot_drift", reason)

        if existing is not None:
            manifest = self._manifests.load(existing.manifest_ref)
            if (
                manifest is None
                or manifest.run_id != run_id
                or manifest.epoch != int(epoch)
                or manifest.preflight_hash != existing.preflight_hash
                or manifest.snapshot_fingerprint != expected
                or existing.snapshot_fingerprint != expected
            ):
                raise GlobalDiscoveryRecoveryError(
                    "recovery_prepared_inputs_invalid",
                    "staged inputs do not resolve to their bound manifest",
                )
            if (
                terminal_counts is not None
                and existing.terminal_counts != terminal_counts
            ):
                raise GlobalDiscoveryRecoveryError(
                    "recovery_prepared_inputs_binding_conflict",
                    "staged inputs are bound to different terminal counts",
                )
            if datetime.now(timezone.utc) >= manifest.expires_at:
                raise GlobalDiscoveryRecoveryError(
                    "manifest_stale", "prepared manifest has expired"
                )
            try:
                fence_check()
            except BaseException:
                with contextlib.suppress(Exception):
                    self._manifests.revoke_prepared(
                        run_id=manifest.run_id,
                        epoch=manifest.epoch,
                        manifest_ref=manifest.manifest_ref,
                        revoked_at=datetime.now(timezone.utc),
                        requested_by_actor_id=actor_id,
                        reason="preparation fence changed during artifact replay",
                    )
                raise
            return RecoveryPreparedResult(
                manifest_ref=manifest.manifest_ref,
                preflight_hash=manifest.preflight_hash,
                snapshot_fingerprint=manifest.snapshot_fingerprint,
                prepared_at=manifest.prepared_at,
                expires_at=manifest.expires_at,
                counts=existing.terminal_counts,
            )
        fence_check()
        ordered = tuple(sorted(boards, key=lambda row: row.board_id))
        seeds = tuple(sorted(candidate_boards, key=lambda row: row.board_id))
        if len({row.board_id for row in seeds}) != len(seeds) or tuple(
            row.board_id for row in seeds
        ) != tuple(row.board_id for row in ordered):
            raise GlobalDiscoveryRecoveryError(
                "candidate_inventory_mismatch",
                "candidate seeds must cover every manifest board exactly once",
            )
        if any(
            seed.board_name != inventory.board_name
            for seed, inventory in zip(seeds, ordered, strict=True)
        ):
            raise GlobalDiscoveryRecoveryError(
                "candidate_inventory_mismatch",
                "candidate board names must match the authoritative census",
            )
        health_rows = _validated_health_rows(
            boards=ordered,
            health_evidence=health_evidence,
            require_recovery_signal=True,
        )
        fence_check()
        assert_expected_snapshot(
            "captured preparation snapshot changed during validation"
        )
        live_artifact = self._recovery.inspect_live_artifact()
        fence_check()
        assert_expected_snapshot(
            "captured preparation snapshot changed while inspecting the live artifact"
        )
        fence_check()
        manifest = self._manifests.build(
            run_id=run_id,
            epoch=epoch,
            actor_id=actor_id,
            boards=ordered,
            live_artifact=live_artifact,
            health_evidence=health_rows,
            snapshot_fingerprint=expected,
            prepared_at=prepared_at,
        )
        try:
            fence_check()
            assert_expected_snapshot(
                "captured preparation snapshot changed before input staging"
            )
            sources_total = sum(row.source_count for row in ordered)
            digest_total = sum(len(row.digests) for row in seeds)
            physical_counts = RecoveryProgressCounts(
                boards_total=len(ordered),
                boards_scanned=len(ordered),
                sources_total=sources_total,
                sources_processed=sources_total,
                nodes_written=len(seeds) + digest_total,
                edges_written=digest_total,
            )
            counts = physical_counts
            if terminal_counts is not None:
                physical = physical_counts.to_dict()
                authoritative = terminal_counts.to_dict()
                mismatched = [
                    field
                    for field, value in physical.items()
                    if field != "errors" and authoritative[field] != value
                ]
                if mismatched or authoritative["errors"] < physical["errors"]:
                    raise ValueError(
                        "terminal_counts must match complete prepared work; "
                        "only the durable error high-water may increase"
                    )
                counts = terminal_counts
            self._prepared_inputs.put(
                GlobalDiscoveryRecoveryPreparedInputs(
                    run_id=manifest.run_id,
                    epoch=manifest.epoch,
                    attempt_id=manifest.attempt_id,
                    manifest_ref=manifest.manifest_ref,
                    preflight_hash=manifest.preflight_hash,
                    snapshot_fingerprint=manifest.snapshot_fingerprint,
                    expected_live_sha256=manifest.live_artifact.sha256,
                    boards=seeds,
                    terminal_counts=counts,
                )
            )
            fence_check()
            assert_expected_snapshot(
                "captured preparation snapshot changed while inputs were staged"
            )
        except BaseException:
            with contextlib.suppress(Exception):
                self._manifests.revoke_prepared(
                    run_id=manifest.run_id,
                    epoch=manifest.epoch,
                    manifest_ref=manifest.manifest_ref,
                    revoked_at=datetime.now(timezone.utc),
                    requested_by_actor_id=actor_id,
                    reason="preparation fence changed during artifact publication",
                )
            raise
        return RecoveryPreparedResult(
            manifest_ref=manifest.manifest_ref,
            preflight_hash=manifest.preflight_hash,
            snapshot_fingerprint=manifest.snapshot_fingerprint,
            prepared_at=manifest.prepared_at,
            expires_at=manifest.expires_at,
            counts=counts,
        )

    @staticmethod
    def _confirmation_receipt(
        *,
        run_id: str,
        confirmation_id: str,
        actor_id: str,
        manifest: GlobalDiscoveryRecoveryManifest,
    ) -> dict[str, object]:
        binding: dict[str, object] = {
            "receipt_version": CONFIRMATION_RECEIPT_VERSION,
            "run_id": run_id,
            "confirmation_fingerprint": hashlib.sha256(
                confirmation_id.encode("utf-8")
            ).hexdigest(),
            "scope": GLOBAL_RECOVERY_SCOPE,
            "operation": GLOBAL_RECOVERY_OPERATION,
            "actor_id": actor_id,
            "manifest_ref": manifest.manifest_ref,
            "preflight_hash": manifest.preflight_hash,
            "board_inventory_hash": manifest.board_inventory_hash,
        }
        return {**binding, "receipt_sha256": _canonical_sha256(binding)}

    def preflight(
        self,
        *,
        actor_id: str,
        boards: Iterable[GlobalDiscoveryBoardInventory],
        health_evidence: Iterable[dict[str, object]],
        run_id: str | None = None,
        epoch: int = 1,
    ) -> dict[str, object]:
        snapshot = self._recovery.inspect_live_artifact()
        manifest = self._manifests.build(
            run_id=run_id,
            epoch=epoch,
            actor_id=actor_id,
            boards=boards,
            live_artifact=snapshot,
            health_evidence=health_evidence,
            snapshot_fingerprint=self.current_snapshot_fingerprint(),
        )
        return {
            "outcome": "confirmation_required",
            "action_required": ("call_okto_pulse_kg_global_discovery_recovery_confirm"),
            "scope": GLOBAL_RECOVERY_SCOPE,
            "run_id": manifest.run_id,
            "attempt_id": manifest.attempt_id,
            "epoch": manifest.epoch,
            "board_count": len(manifest.boards),
            "source_count": sum(row.source_count for row in manifest.boards),
            "board_inventory_hash": manifest.board_inventory_hash,
            "live_artifact": snapshot.to_dict(),
            "manifest_ref": manifest.manifest_ref,
            "preflight_hash": manifest.preflight_hash,
            "snapshot_fingerprint": manifest.snapshot_fingerprint,
            "prepared_at": manifest.prepared_at.isoformat(),
            "expires_at": manifest.expires_at.isoformat(),
        }

    def confirm(
        self,
        *,
        actor_id: str,
        run_id: str | None = None,
        manifest_ref: str,
        preflight_hash: str,
        current_snapshot_fingerprint: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, object]:
        manifest = self._manifests.load(manifest_ref)
        if manifest is None:
            raise GlobalDiscoveryRecoveryError(
                "manifest_invalid", "manifest is missing, corrupt or hash-invalid"
            )
        if run_id is not None and manifest.run_id != str(run_id):
            raise GlobalDiscoveryRecoveryError(
                "manifest_invalid", "manifest is bound to another recovery run"
            )
        if manifest.preflight_hash != preflight_hash:
            raise GlobalDiscoveryRecoveryError(
                "preflight_hash_mismatch", "hash does not match manifest"
            )
        if (
            self._manifests.read_prepared_revocation(
                run_id=manifest.run_id,
                epoch=manifest.epoch,
            )
            is not None
        ):
            raise GlobalDiscoveryRecoveryError(
                "preparation_not_prepared",
                "prepared attempt has immutable revocation evidence",
            )
        if run_id is not None:
            prepared = self._prepared_inputs.load(manifest.run_id, epoch=manifest.epoch)
            if (
                prepared is None
                or prepared.manifest_ref != manifest.manifest_ref
                or prepared.preflight_hash != manifest.preflight_hash
            ):
                raise GlobalDiscoveryRecoveryError(
                    "preparation_not_prepared",
                    "run has no complete integrity-bound prepared inputs",
                )
        manifest.assert_fresh(
            now=now or datetime.now(timezone.utc),
            current_snapshot_fingerprint=(
                current_snapshot_fingerprint
                if current_snapshot_fingerprint is not None
                else self.current_snapshot_fingerprint()
            ),
        )
        token = self._confirmation.issue(
            board_id=GLOBAL_RECOVERY_SCOPE,
            actor_id=actor_id,
            operation=GLOBAL_RECOVERY_OPERATION,
            preflight_hash=preflight_hash,
            manifest_ref=manifest_ref,
        )
        self._manifests.bind_issued_confirmation(
            confirmation_id=token.confirmation_id,
            run_id=manifest.run_id,
            epoch=manifest.epoch,
            manifest_ref=manifest.manifest_ref,
            preflight_hash=manifest.preflight_hash,
            confirmed_by_actor_id=actor_id,
            expires_at=token.expires_at,
        )
        return {
            "outcome": "confirmation_issued",
            "action_required": "call_okto_pulse_kg_global_discovery_recovery_run",
            "confirmation_id": token.confirmation_id,
            "run_id": manifest.run_id,
            "attempt_id": manifest.attempt_id,
            "epoch": manifest.epoch,
            "manifest_ref": manifest_ref,
            "preflight_hash": preflight_hash,
            "expires_at": token.expires_at,
        }

    def prepare_durable_start(
        self,
        *,
        actor_id: str,
        confirmation_id: str,
        manifest_ref: str,
        preflight_hash: str,
        reason: str,
        current_snapshot_fingerprint: str | None = None,
        started_at: datetime | None = None,
        attempt_budget_ms: int = DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS,
    ) -> RecoveryStartCommand:
        """Consume one fresh confirmation using only already-prepared inputs."""

        if not reason.strip() or len(reason) > 512:
            raise GlobalDiscoveryRecoveryError(
                "invalid_reason", "reason must contain 1..512 characters"
            )
        issued = self._manifests.resolve_issued_confirmation(confirmation_id)
        run_id = str(issued["run_id"])
        if (
            issued.get("manifest_ref") != manifest_ref
            or issued.get("preflight_hash") != preflight_hash
        ):
            raise GlobalDiscoveryRecoveryError(
                "confirmation_refused",
                "confirmation does not bind the supplied manifest and hash",
            )
        manifest = self._manifests.load(manifest_ref)
        if manifest is None:
            raise GlobalDiscoveryRecoveryError(
                "manifest_invalid", "manifest is missing, corrupt or hash-invalid"
            )
        if manifest.run_id != run_id or manifest.epoch != int(issued["epoch"]):
            raise GlobalDiscoveryRecoveryError(
                "manifest_invalid",
                "manifest run/attempt binding does not match confirmation",
            )
        if manifest.preflight_hash != preflight_hash:
            raise GlobalDiscoveryRecoveryError(
                "preflight_hash_mismatch", "hash does not match manifest"
            )
        fingerprint = hashlib.sha256(confirmation_id.encode("utf-8")).hexdigest()
        binding = RecoveryRunBinding(
            run_id=run_id,
            actor_id=manifest.actor_id,
            confirmation_fingerprint=fingerprint,
            manifest_ref=manifest_ref,
            preflight_hash=preflight_hash,
            reason=reason,
        )
        receipt = self._confirmation_receipt(
            run_id=run_id,
            confirmation_id=confirmation_id,
            actor_id=str(issued["confirmed_by_actor_id"]),
            manifest=manifest,
        )
        receipt_valid = self._manifests.has_confirmation_receipt(
            run_id=run_id,
            expected=receipt,
        )
        if (
            self._manifests.read_prepared_revocation(
                run_id=manifest.run_id,
                epoch=manifest.epoch,
            )
            is not None
        ):
            raise GlobalDiscoveryRecoveryError(
                "preparation_not_prepared",
                "prepared attempt has immutable revocation evidence",
            )
        existing_inputs = self._worker_inputs.load(run_id, epoch=manifest.epoch)
        if existing_inputs is not None:
            if not receipt_valid:
                raise GlobalDiscoveryRecoveryError(
                    "confirmation_refused", "durable_receipt_missing"
                )
            if existing_inputs.command.binding != binding:
                raise GlobalDiscoveryRecoveryError(
                    "recovery_worker_inputs_binding_conflict",
                    "run id is already bound to another immutable request",
                )
            return existing_inputs.command

        manifest.assert_fresh(
            now=started_at or datetime.now(timezone.utc),
            current_snapshot_fingerprint=(
                current_snapshot_fingerprint
                if current_snapshot_fingerprint is not None
                else self.current_snapshot_fingerprint()
            ),
        )

        prepared = self._prepared_inputs.load(run_id, epoch=manifest.epoch)
        if prepared is None:
            raise GlobalDiscoveryRecoveryError(
                "preparation_not_prepared",
                "complete prepared inputs are required before start",
            )
        if (
            prepared.manifest_ref != manifest.manifest_ref
            or prepared.preflight_hash != manifest.preflight_hash
            or prepared.snapshot_fingerprint != manifest.snapshot_fingerprint
            or prepared.epoch != manifest.epoch
        ):
            raise GlobalDiscoveryRecoveryError(
                "recovery_prepared_inputs_invalid",
                "prepared input binding does not match the confirmed manifest",
            )

        if not receipt_valid:
            consumed = self._confirmation.consume(
                confirmation_id=confirmation_id,
                expected_board_id=GLOBAL_RECOVERY_SCOPE,
                expected_actor_id=str(issued["confirmed_by_actor_id"]),
                expected_operation=GLOBAL_RECOVERY_OPERATION,
                expected_preflight_hash=preflight_hash,
                expected_manifest_ref=manifest_ref,
                consumption_receipt_key=self._manifests.confirmation_receipt_key(
                    run_id
                ),
                consumption_receipt_payload=receipt,
            )
            receipt_valid = self._manifests.has_confirmation_receipt(
                run_id=run_id,
                expected=receipt,
            )
            if not receipt_valid:
                raise GlobalDiscoveryRecoveryError(
                    "confirmation_refused",
                    f"{consumed.outcome}:{consumed.reason or 'n/a'}",
                )

        admitted_start = started_at or datetime.now(timezone.utc)
        command = RecoveryStartCommand(
            binding=binding,
            started_at=admitted_start,
            counts=prepared.terminal_counts,
            attempt_budget_ms=attempt_budget_ms,
            expected_epoch=manifest.epoch,
            confirmed_by_actor_id=str(issued["confirmed_by_actor_id"]),
            confirmation_consumed_at=admitted_start,
        )
        stored = self._worker_inputs.put(
            GlobalDiscoveryRecoveryWorkerInputs(
                command=command,
                expected_live_sha256=prepared.expected_live_sha256,
                boards=prepared.boards,
                terminal_counts=prepared.terminal_counts,
            )
        )
        return stored.command

    def run(
        self,
        *,
        actor_id: str,
        confirmation_id: str,
        manifest_ref: str,
        preflight_hash: str,
        boards: Iterable[GlobalDiscoveryBoardInventory],
        health_evidence: Iterable[dict[str, object]],
        candidate_boards: Iterable[GlobalDiscoveryBoardSeed],
        reason: str,
    ) -> dict[str, object]:
        if not reason.strip() or len(reason) > 512:
            raise GlobalDiscoveryRecoveryError(
                "invalid_reason", "reason must contain 1..512 characters"
            )
        manifest = self._manifests.load(manifest_ref)
        if manifest is None:
            raise GlobalDiscoveryRecoveryError(
                "manifest_invalid", "manifest is missing, corrupt or hash-invalid"
            )
        if manifest.actor_id != actor_id:
            raise GlobalDiscoveryRecoveryError(
                "manifest_owner_mismatch", "manifest belongs to another actor"
            )
        if manifest.preflight_hash != preflight_hash:
            raise GlobalDiscoveryRecoveryError(
                "preflight_hash_mismatch", "hash does not match manifest"
            )
        run_id = f"gdr_{hashlib.sha256(confirmation_id.encode()).hexdigest()[:24]}"
        receipt = self._confirmation_receipt(
            run_id=run_id,
            confirmation_id=confirmation_id,
            actor_id=actor_id,
            manifest=manifest,
        )
        receipt_valid = self._manifests.has_confirmation_receipt(
            run_id=run_id,
            expected=receipt,
        )
        early_status = self._manifests.read_status(run_id)
        exact_physical_resume = False
        if early_status is not None:
            if (
                early_status.get("run_id") != run_id
                or early_status.get("actor_id") != actor_id
                or early_status.get("manifest_ref") != manifest_ref
                or early_status.get("preflight_hash") != preflight_hash
            ):
                raise GlobalDiscoveryRecoveryError(
                    "run_status_binding_mismatch",
                    "durable run status belongs to another recovery binding",
                )
            if not receipt_valid:
                raise GlobalDiscoveryRecoveryError(
                    "confirmation_refused", "durable_receipt_missing"
                )
            if early_status.get("state") == "precondition_failed":
                raise GlobalDiscoveryRecoveryError(
                    "confirmation_refused",
                    str(early_status.get("confirmation_outcome") or "precondition"),
                )
            if receipt_valid and early_status.get("state") in _TERMINAL_STATES | {
                "delivery_pending"
            }:
                return dict(early_status)
            exact_physical_resume = _requires_exact_physical_resume(early_status)
        current = tuple(sorted(boards, key=lambda row: row.board_id))
        current_hash = _canonical_sha256([row.to_dict() for row in current])
        if current_hash != manifest.board_inventory_hash:
            raise GlobalDiscoveryRecoveryError(
                "board_inventory_drift",
                "board/source inventory changed since preflight; run a new preflight",
            )
        # Admission is live policy, not historical evidence.  A confirmed
        # manifest cannot authorize a new destructive run after health has
        # changed.  The sole exception is an exact receipt-bound resume whose
        # durable phase says physical work may already have started.
        _validated_health_rows(
            boards=current,
            health_evidence=health_evidence,
            require_recovery_signal=not exact_physical_resume,
        )
        seeds = tuple(sorted(candidate_boards, key=lambda row: row.board_id))
        if len({row.board_id for row in seeds}) != len(seeds) or tuple(
            row.board_id for row in seeds
        ) != tuple(row.board_id for row in current):
            raise GlobalDiscoveryRecoveryError(
                "candidate_inventory_mismatch",
                "candidate seeds must cover every manifest board exactly once",
            )
        if not self._run_lock.acquire(blocking=False):
            raise GlobalDiscoveryRecoveryError(
                "recovery_in_progress", "another recovery owns the process lock"
            )
        # Deterministic opaque id lets a cancelled transport rediscover the
        # durable terminal status after the native operation has drained.
        lease_acquired = False
        authorization_proven = receipt_valid
        physical_cutover_started = False
        cutover = None
        try:
            existing_status = self._manifests.read_status(run_id)
            status_matches = bool(
                existing_status is not None
                and existing_status.get("run_id") == run_id
                and existing_status.get("actor_id") == actor_id
                and existing_status.get("manifest_ref") == manifest_ref
                and existing_status.get("preflight_hash") == preflight_hash
            )
            if existing_status is not None and not status_matches:
                raise GlobalDiscoveryRecoveryError(
                    "run_status_binding_mismatch",
                    "durable run status belongs to another recovery binding",
                )
            receipt_valid = self._manifests.has_confirmation_receipt(
                run_id=run_id,
                expected=receipt,
            )
            authorization_proven = receipt_valid
            if (
                existing_status is not None
                and existing_status.get("state") == "precondition_failed"
            ):
                raise GlobalDiscoveryRecoveryError(
                    "confirmation_refused",
                    str(existing_status.get("confirmation_outcome") or "precondition"),
                )
            if (
                receipt_valid
                and existing_status is not None
                and existing_status.get("state") in _TERMINAL_STATES
            ):
                # Transport retries after token consumption are idempotent;
                # they rediscover the durable result and never cut over twice.
                return dict(existing_status)
            if (
                receipt_valid
                and existing_status is not None
                and existing_status.get("state") == ("delivery_pending")
            ):
                # Cutover already committed.  The transport must retry only the
                # idempotent relational delivery and then call finalize_delivery.
                return dict(existing_status)
            # The artifact store is the cross-process authority.  A process
            # cache can outlive a re-composition to another tenant/root and
            # must never fence a durable store that has no active pointer.
            active = self._manifests.read_reconciled_active_status()
            if (
                active
                and _status_holds_recovery_fence(active)
                and active.get("run_id") != run_id
            ):
                raise GlobalDiscoveryRecoveryError(
                    "recovery_in_progress",
                    f"run={active.get('run_id', 'unknown')} owns recovery",
                )
            resuming = bool(
                existing_status is not None
                and existing_status.get("state") in _RESUMABLE_STATES
                and receipt_valid
            )
            if existing_status is not None and not receipt_valid:
                raise GlobalDiscoveryRecoveryError(
                    "confirmation_refused", "durable_receipt_missing"
                )
            if not resuming and existing_status is None and not receipt_valid:
                consumed = self._confirmation.consume(
                    confirmation_id=confirmation_id,
                    expected_board_id=GLOBAL_RECOVERY_SCOPE,
                    expected_actor_id=actor_id,
                    expected_operation=GLOBAL_RECOVERY_OPERATION,
                    expected_preflight_hash=preflight_hash,
                    expected_manifest_ref=manifest_ref,
                    consumption_receipt_key=(
                        self._manifests.confirmation_receipt_key(run_id)
                    ),
                    consumption_receipt_payload=receipt,
                )
                receipt_valid = self._manifests.has_confirmation_receipt(
                    run_id=run_id,
                    expected=receipt,
                )
                authorization_proven = receipt_valid
                if not receipt_valid:
                    raise GlobalDiscoveryRecoveryError(
                        "confirmation_refused",
                        f"{consumed.outcome}:{consumed.reason or 'n/a'}",
                    )
            self._active_run.clear()
            self._active_run.update(
                {"run_id": run_id, "actor_id": actor_id, "state": "running"}
            )
            running_payload = {
                "state": "running",
                "outcome": "running",
                "actor_id": actor_id,
                "manifest_ref": manifest_ref,
                "preflight_hash": preflight_hash,
                "confirmation_receipt_sha256": receipt["receipt_sha256"],
                "reason": reason,
            }
            if existing_status is None:
                running_payload["physical_cutover_started"] = False
            elif existing_status.get("physical_cutover_started") is True:
                running_payload["physical_cutover_started"] = True
            elif existing_status.get("physical_cutover_started") is False:
                running_payload["physical_cutover_started"] = False
            if existing_status is None:
                existing_status, claimed = self._manifests.compare_and_set_status(
                    run_id,
                    expected_states=None,
                    payload=running_payload,
                )
            else:
                existing_status, claimed = self._manifests.compare_and_set_status(
                    run_id,
                    expected_states=frozenset({"running", "failed"}),
                    payload=running_payload,
                )
            if not claimed:
                if existing_status.get("state") in _TERMINAL_STATES | {
                    "delivery_pending"
                }:
                    return dict(existing_status)
                raise GlobalDiscoveryRecoveryError(
                    "recovery_in_progress",
                    f"run={run_id} status transition is owned by another caller",
                )
            active_claim = self._manifests.read_active_status()
            if active_claim is None or active_claim.get("run_id") != run_id:
                raise GlobalDiscoveryRecoveryError(
                    "recovery_in_progress",
                    f"run={active_claim.get('run_id') if active_claim else 'unknown'} "
                    "owns recovery",
                )
            try:
                lease = GlobalDiscoveryWriterLease.acquire(
                    operation="global_discovery_recovery",
                    owner_id=f"{actor_id}:{run_id}",
                    ttl_seconds=DEFAULT_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS,
                    admin_lane=True,
                    lock=self._single_writer,
                )
                lease_acquired = True
            except GlobalDiscoveryWriterContention as exc:
                raise GlobalDiscoveryRecoveryError(
                    "recovery_lock_contention",
                    f"owned_by={exc.current_owner or 'unknown'}",
                ) from exc
            try:
                physical_payload = {
                    **running_payload,
                    "physical_cutover_started": True,
                }
                phase_status, phase_claimed = self._manifests.compare_and_set_status(
                    run_id,
                    expected_states=frozenset({"running"}),
                    payload=physical_payload,
                )
                if not phase_claimed:
                    if phase_status.get("state") in _TERMINAL_STATES | {
                        "delivery_pending"
                    }:
                        return dict(phase_status)
                    raise GlobalDiscoveryRecoveryError(
                        "recovery_in_progress",
                        f"run={run_id} physical phase is owned by another caller",
                    )
                physical_cutover_started = True
                active_phase = self._manifests.read_active_status()
                if active_phase is None or active_phase.get("run_id") != run_id:
                    raise GlobalDiscoveryRecoveryError(
                        "recovery_in_progress",
                        f"run={active_phase.get('run_id') if active_phase else 'unknown'} "
                        "owns recovery",
                    )
                with lease.renewing_guard():
                    cutover = self._recovery.rebuild_candidate_and_cutover(
                        run_id=run_id,
                        epoch=1,
                        attempt_id=recovery_attempt_id(run_id, 1),
                        expected_live_sha256=manifest.live_artifact.sha256,
                        boards=seeds,
                        fence_check=lease.assert_fenced,
                    )
                state = (
                    "delivery_pending"
                    if cutover.outcome == "completed"
                    else "rolled_back"
                )
                payload = {
                    "state": state,
                    "outcome": cutover.outcome,
                    "board_ids": [row.board_id for row in current],
                    "action_required": (
                        "resume_durable_relational_delivery"
                        if state == "delivery_pending"
                        else "inspect_recovery_status_and_retry_preflight"
                    ),
                    "actor_id": actor_id,
                    "manifest_ref": manifest_ref,
                    "preflight_hash": preflight_hash,
                    "confirmation_receipt_sha256": receipt["receipt_sha256"],
                    "reason": reason,
                    "physical_cutover_started": True,
                    **cutover.to_dict(),
                }
                # Persist the post-cutover truth before releasing the exact
                # cross-process fencing lease shared with normal global writers.
                self._manifests.write_status(run_id, payload)
            finally:
                lease.release()
            self._active_run.clear()
            self._active_run.update(
                {"run_id": run_id, "actor_id": actor_id, "state": state}
            )
            return {"run_id": run_id, **payload}
        except Exception as exc:
            current_status = self._manifests.read_status(run_id)
            if current_status is not None and current_status.get("state") in {
                "delivery_pending",
                "rolled_back",
                "completed",
                "precondition_failed",
            }:
                # Never overwrite a durable post-cutover truth because cleanup
                # (for example lease release) raised afterwards.
                if isinstance(exc, GlobalDiscoveryRecoveryError):
                    raise
                return dict(current_status)
            code = str(getattr(exc, "code", "recovery_failed"))
            if not authorization_proven:
                authorization_proven = self._manifests.has_confirmation_receipt(
                    run_id=run_id,
                    expected=receipt,
                )
            if not authorization_proven:
                # A guessed/stolen confirmation id must never establish the
                # canonical binding for its deterministic run id.
                if isinstance(exc, GlobalDiscoveryRecoveryError):
                    raise
                raise GlobalDiscoveryRecoveryError(
                    code,
                    "global recovery failed before authorization was proven",
                ) from exc
            if (
                not lease_acquired
                and current_status is not None
                and current_status.get("state") == "running"
                and code in {"recovery_lock_contention", "recovery_in_progress"}
            ):
                raise
            if current_status is not None and current_status.get("state") != "running":
                # A concurrent terminal/failed winner is immutable here.
                if isinstance(exc, GlobalDiscoveryRecoveryError):
                    raise
                raise GlobalDiscoveryRecoveryError(
                    code,
                    "global recovery failed; durable status was preserved",
                ) from exc
            physical_evidence: dict[str, object] = {}
            if cutover is not None:
                physical_evidence = {
                    **cutover.to_dict(),
                    "physical_cutover_outcome": cutover.outcome,
                }
            phase_marker: bool | None = None
            if physical_cutover_started or cutover is not None:
                phase_marker = True
            elif current_status is None:
                phase_marker = False
            elif current_status.get("physical_cutover_started") is True:
                phase_marker = True
            elif current_status.get("physical_cutover_started") is False:
                phase_marker = False
            payload = {
                **physical_evidence,
                "state": "failed",
                "outcome": "failed",
                "action_required": "inspect_recovery_status_and_retry_preflight",
                "actor_id": actor_id,
                "manifest_ref": manifest_ref,
                "preflight_hash": preflight_hash,
                "confirmation_receipt_sha256": receipt["receipt_sha256"],
                "failure_code": code,
                "reason": code,
            }
            if phase_marker is not None:
                payload["physical_cutover_started"] = phase_marker
            failed_status, failed_written = self._manifests.compare_and_set_status(
                run_id,
                expected_states=(
                    None if current_status is None else frozenset({"running"})
                ),
                payload=payload,
            )
            if not failed_written:
                if failed_status.get("state") in _TERMINAL_STATES | {
                    "delivery_pending"
                } and not isinstance(exc, GlobalDiscoveryRecoveryError):
                    return dict(failed_status)
                if isinstance(exc, GlobalDiscoveryRecoveryError):
                    raise
                raise GlobalDiscoveryRecoveryError(
                    code,
                    "global recovery failed; concurrent durable status was preserved",
                ) from exc
            self._active_run.clear()
            self._active_run.update(
                {"run_id": run_id, "actor_id": actor_id, "state": "failed"}
            )
            if isinstance(exc, GlobalDiscoveryRecoveryError):
                raise
            raise GlobalDiscoveryRecoveryError(
                code,
                "global recovery failed; inspect the durable run status",
            ) from exc
        finally:
            self._run_lock.release()

    def read_bound_status(
        self,
        *,
        actor_id: str,
        confirmation_id: str,
        manifest_ref: str,
        preflight_hash: str,
    ) -> dict[str, object] | None:
        """Return only a resumable post-cutover or terminal bound status."""

        manifest = self._manifests.load(manifest_ref)
        if manifest is None:
            raise GlobalDiscoveryRecoveryError(
                "manifest_invalid", "manifest is missing, corrupt or hash-invalid"
            )
        if manifest.actor_id != actor_id:
            raise GlobalDiscoveryRecoveryError(
                "manifest_owner_mismatch", "manifest belongs to another actor"
            )
        if manifest.preflight_hash != preflight_hash:
            raise GlobalDiscoveryRecoveryError(
                "preflight_hash_mismatch", "hash does not match manifest"
            )
        run_id = f"gdr_{hashlib.sha256(confirmation_id.encode()).hexdigest()[:24]}"
        receipt = self._confirmation_receipt(
            run_id=run_id,
            confirmation_id=confirmation_id,
            actor_id=actor_id,
            manifest=manifest,
        )
        if not self._manifests.has_confirmation_receipt(
            run_id=run_id,
            expected=receipt,
        ):
            return None
        status = self._manifests.read_status(run_id)
        if status is None:
            return None
        if (
            status.get("actor_id") != actor_id
            or status.get("manifest_ref") != manifest_ref
            or status.get("preflight_hash") != preflight_hash
        ):
            raise GlobalDiscoveryRecoveryError(
                "run_status_binding_mismatch",
                "durable run status belongs to another recovery binding",
            )
        if status.get("state") not in _TERMINAL_STATES | {"delivery_pending"}:
            return None
        result = dict(status)
        result.setdefault("run_id", run_id)
        result.setdefault("board_ids", [row.board_id for row in manifest.boards])
        return result

    def finalize_delivery(
        self,
        *,
        run_id: str,
        actor_id: str,
        manifest_ref: str,
        preflight_hash: str,
        delivery: dict[str, object],
    ) -> dict[str, object]:
        """Mark completion only after the caller confirms the UoW commit."""

        if not self._run_lock.acquire(blocking=False):
            raise GlobalDiscoveryRecoveryError(
                "recovery_in_progress", "another recovery owns the process lock"
            )
        try:
            status = self._manifests.read_status(run_id)
            if status is None:
                raise GlobalDiscoveryRecoveryError(
                    "run_status_missing", "durable recovery status is missing"
                )
            if (
                status.get("actor_id") != actor_id
                or status.get("manifest_ref") != manifest_ref
                or status.get("preflight_hash") != preflight_hash
            ):
                raise GlobalDiscoveryRecoveryError(
                    "run_status_binding_mismatch",
                    "durable run status belongs to another recovery binding",
                )
            if status.get("state") == "completed":
                return dict(status)
            if status.get("state") != "delivery_pending":
                raise GlobalDiscoveryRecoveryError(
                    "delivery_not_pending",
                    f"run state={status.get('state', 'unknown')} cannot be finalized",
                )
            payload = {
                **status,
                "state": "completed",
                "outcome": "completed",
                "action_required": "wait_for_outbox_then_call_okto_pulse_kg_health",
                "delivery": dict(delivery),
            }
            self._manifests.write_status(run_id, payload)
            self._active_run.clear()
            self._active_run.update(
                {"run_id": run_id, "actor_id": actor_id, "state": "completed"}
            )
            return {"run_id": run_id, **payload}
        finally:
            self._run_lock.release()


__all__ = [
    "GLOBAL_RECOVERY_OPERATION",
    "GLOBAL_RECOVERY_SCOPE",
    "GlobalDiscoveryBoardInventory",
    "GlobalDiscoveryRecoveryError",
    "GlobalDiscoveryRecoveryManifest",
    "GlobalDiscoveryRecoveryManifestStore",
    "GlobalDiscoveryRecoveryPreparedInputs",
    "GlobalDiscoveryRecoveryPreparedInputStore",
    "GlobalDiscoveryRecoveryPreparedRevocation",
    "GlobalDiscoveryRecoveryService",
    "GlobalDiscoveryRecoveryWorkerInputs",
    "GlobalDiscoveryRecoveryWorkerInputStore",
    "global_discovery_recovery_run_id",
    "inventory_from_source_rows",
    "inventory_from_source_set",
]
