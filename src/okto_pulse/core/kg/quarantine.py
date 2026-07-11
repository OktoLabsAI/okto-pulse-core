"""Quarantine-before-purge application policy.

Core validates graph type, retention and reason semantics. Edition adapters own
storage layout, scope resolution, compensation and durable manifest writes.
Only opaque ``StorageRef`` values cross this boundary.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from okto_pulse.core.kg.interfaces.storage_ref import StorageRef

logger = logging.getLogger("okto_pulse.kg.quarantine")


QUARANTINE_DIRNAME = "quarantine"
MANIFEST_FILENAME = "manifest.json"
DEFAULT_RETENTION_DAYS = 30  # TR9.
SOFTWARE_VERSION_FALLBACK = "unknown"

# Keep `json` available as a module attribute for artifact-store manifest
# writers and regression tests that monkeypatch the manifest write path.
JSON_MODULE = json


# Canonical graph_type values per contract api_ee77f56f request body.
CANONICAL_GRAPH_TYPES = frozenset({"board_graph", "global_discovery"})

# Reason enum — labels for the OR counter must be bounded to avoid the
# cardinality explosion that free-text reasons would cause. Operators
# pass detailed reason text in the manifest, but the counter sees only
# the bucket.
class QuarantineReason(str, Enum):
    CORRUPTION_DETECTED = "corruption_detected"
    WAL_TRUNCATION = "wal_truncation"
    ORPHANED_LOCK = "orphaned_lock"
    OPERATOR_MANUAL = "operator_manual"
    UNKNOWN = "unknown"


class QuarantineErrorCode(str, Enum):
    """Typed error codes per contract api_ee77f56f response_errors."""

    QUARANTINE_STORAGE_UNAVAILABLE = "quarantine_storage_unavailable"
    STORAGE_REF_OUT_OF_SCOPE = "storage_ref_out_of_scope"


class QuarantineError(Exception):
    """Raised when the quarantine service cannot complete the request.

    Always carries the error ``code`` (matching the contract) and a
    ``retryable`` flag so the caller can pick a sane fallback (retry
    after backoff vs surface to operator).
    """

    def __init__(
        self,
        code: QuarantineErrorCode,
        *,
        retryable: bool,
        reason: str,
    ) -> None:
        super().__init__(f"{code.value}: {reason}")
        self.code = code
        self.retryable = retryable
        self.reason = reason


@dataclass(frozen=True, slots=True)
class QuarantineConfig:
    """Tunables — defaults match TR9.

    `retention_days` is the safety floor between quarantine creation
    and operator-driven cleanup. Anything below 7 is rejected — the
    spec explicitly forbids losing post-mortem evidence before a
    typical incident response cycle.
    """

    retention_days: int = DEFAULT_RETENTION_DAYS

    def __post_init__(self) -> None:
        if self.retention_days < 7:
            raise ValueError(
                f"retention_days={self.retention_days} below safety floor of 7"
            )


@dataclass(frozen=True, slots=True)
class QuarantineResponse:
    """Frozen contract-shaped response per api_ee77f56f success body."""

    quarantine_id: str
    manifest_ref: str
    retention_until: str
    files_moved: int


@dataclass(frozen=True, slots=True)
class QuarantineManifest:
    """Semantic view of a quarantine manifest (TR8)."""

    quarantine_id: str
    board_id: str
    graph_type: str
    reason: str
    reason_bucket: str  # one of QuarantineReason
    correlation_ids: tuple[str, ...]
    affected_storage_refs: tuple[StorageRef, ...]
    kg_generation_id: str | None
    software_version: str
    quarantined_at: str
    retention_until: str
    files_moved: int

    def to_disk_dict(self) -> dict[str, Any]:
        return {
            "quarantine_id": self.quarantine_id,
            "board_id": self.board_id,
            "graph_type": self.graph_type,
            "reason": self.reason,
            "reason_bucket": self.reason_bucket,
            "correlation_ids": list(self.correlation_ids),
            "affected_storage_refs": [
                {"token": ref.token, "namespace": ref.namespace}
                for ref in self.affected_storage_refs
            ],
            "kg_generation_id": self.kg_generation_id,
            "software_version": self.software_version,
            "quarantined_at": self.quarantined_at,
            "retention_until": self.retention_until,
            "files_moved": self.files_moved,
        }


# --- Counter (OR or_05fd5cd3) -------------------------------------------------
#
# kg_quarantine_total canonical labels: board_id, graph_type, outcome, reason.
# outcome in {"created", "out_of_scope", "storage_unavailable", "manifest_failed"}
# reason in QuarantineReason enum values (bounded cardinality).

_QUARANTINE_COUNTER_LABELS = (
    "board_id",
    "graph_type",
    "outcome",
    "reason",
)

_QuarantineCounterKey = tuple[str, str, str, str]
_quarantine_counter: dict[_QuarantineCounterKey, int] = {}
_quarantine_counter_lock = threading.Lock()


def _bump_quarantine_counter(
    *, board_id: str, graph_type: str, outcome: str, reason: str
) -> None:
    key: _QuarantineCounterKey = (board_id, graph_type, outcome, reason)
    with _quarantine_counter_lock:
        _quarantine_counter[key] = _quarantine_counter.get(key, 0) + 1


def get_quarantine_counter(
    board_id: str,
    outcome: str,
    *,
    graph_type: str | None = None,
    reason: str | None = None,
) -> int:
    with _quarantine_counter_lock:
        total = 0
        for (b, gt, out, rsn), value in _quarantine_counter.items():
            if b != board_id or out != outcome:
                continue
            if graph_type is not None and gt != graph_type:
                continue
            if reason is not None and rsn != reason:
                continue
            total += value
        return total


def get_quarantine_counter_samples() -> list[dict[str, Any]]:
    with _quarantine_counter_lock:
        return [
            {
                "board_id": b,
                "graph_type": gt,
                "outcome": out,
                "reason": rsn,
                "count": value,
            }
            for (b, gt, out, rsn), value in _quarantine_counter.items()
        ]


def get_quarantine_counter_labels() -> tuple[str, ...]:
    return _QUARANTINE_COUNTER_LABELS


def reset_quarantine_counter() -> None:
    with _quarantine_counter_lock:
        _quarantine_counter.clear()


def _bucket_reason(reason: str) -> str:
    """Map an operator-supplied reason string to a bounded counter bucket.

    The full reason is preserved in the manifest; the bucket is for
    metric label cardinality only. Anything unknown falls into
    ``QuarantineReason.UNKNOWN``.
    """
    lowered = reason.lower()
    if "corrupt" in lowered:
        return QuarantineReason.CORRUPTION_DETECTED.value
    if "wal" in lowered and "trunc" in lowered:
        return QuarantineReason.WAL_TRUNCATION.value
    if "orphan" in lowered and "lock" in lowered:
        return QuarantineReason.ORPHANED_LOCK.value
    if "manual" in lowered or "operator" in lowered:
        return QuarantineReason.OPERATOR_MANUAL.value
    return QuarantineReason.UNKNOWN.value


# --- The service ---------------------------------------------------------------


class KGQuarantineService:
    """Core quarantine policy facade over an edition artifact store."""

    def __init__(
        self,
        *,
        scope_storage_refs: list[StorageRef],
        base_storage_ref_hint: StorageRef | None = None,
        config: QuarantineConfig | None = None,
        artifact_store: Any | None = None,
    ) -> None:
        if not scope_storage_refs:
            raise ValueError("scope_storage_refs must include at least one storage scope")
        self._base_storage_ref_hint = base_storage_ref_hint
        self._scope_storage_refs = tuple(scope_storage_refs)
        self._config = config or QuarantineConfig()
        self._artifact_store = artifact_store

    def create(
        self,
        *,
        board_id: str,
        graph_type: str,
        affected_storage_refs: list[StorageRef],
        reason: str,
        correlation_ids: list[str],
        kg_generation_id: str | None = None,
    ) -> QuarantineResponse:
        """Quarantine the supplied storage references and write the manifest.

        Returns the contract-shaped response on success; raises
        ``QuarantineError`` with the appropriate typed code on failure.
        Counter is bumped on every outcome (created OR failure mode).
        """
        if graph_type not in CANONICAL_GRAPH_TYPES:
            _bump_quarantine_counter(
                board_id=board_id,
                graph_type=graph_type,
                outcome="out_of_scope",
                reason=QuarantineReason.UNKNOWN.value,
            )
            raise QuarantineError(
                QuarantineErrorCode.STORAGE_REF_OUT_OF_SCOPE,
                retryable=False,
                reason=f"unknown graph_type: {graph_type}",
            )
        if not affected_storage_refs:
            _bump_quarantine_counter(
                board_id=board_id,
                graph_type=graph_type,
                outcome="out_of_scope",
                reason=QuarantineReason.UNKNOWN.value,
            )
            raise QuarantineError(
                QuarantineErrorCode.STORAGE_REF_OUT_OF_SCOPE,
                retryable=False,
                reason="affected_storage_refs must be non-empty",
            )

        store = self._store()
        reason_bucket = _bucket_reason(reason)
        try:
            payload = store.quarantine_storage(
                board_id=board_id,
                graph_type=graph_type,
                affected_storage_refs=tuple(affected_storage_refs),
                reason=reason,
                reason_bucket=reason_bucket,
                correlation_ids=tuple(correlation_ids),
                kg_generation_id=kg_generation_id,
                retention_days=self._config.retention_days,
                scope_storage_refs=self._scope_storage_refs,
                base_storage_ref_hint=self._base_storage_ref_hint,
            )
        except QuarantineError as exc:
            outcome = (
                "out_of_scope"
                if exc.code is QuarantineErrorCode.STORAGE_REF_OUT_OF_SCOPE
                else "storage_unavailable"
            )
            _bump_quarantine_counter(
                board_id=board_id,
                graph_type=graph_type,
                outcome=outcome,
                reason=reason_bucket,
            )
            raise
        except Exception as exc:
            _bump_quarantine_counter(
                board_id=board_id,
                graph_type=graph_type,
                outcome="storage_unavailable",
                reason=reason_bucket,
            )
            raise QuarantineError(
                QuarantineErrorCode.QUARANTINE_STORAGE_UNAVAILABLE,
                retryable=True,
                reason=f"artifact_store_quarantine_failed: {type(exc).__name__}",
            ) from exc

        manifest = self._manifest_from_payload(payload)
        logger.warning(
            "kg.quarantine.created quarantine_id=%s board=%s graph_type=%s "
            "files=%d reason=%s",
            manifest.quarantine_id, board_id, graph_type, manifest.files_moved, reason,
            extra={
                "event": "kg.quarantine.created",
                "quarantine_id": manifest.quarantine_id,
                "board_id": board_id,
                "graph_type": graph_type,
                "reason_bucket": manifest.reason_bucket,
                "correlation_ids": list(correlation_ids),
                "files_moved": manifest.files_moved,
            },
        )
        _bump_quarantine_counter(
            board_id=board_id,
            graph_type=graph_type,
            outcome="created",
            reason=manifest.reason_bucket,
        )
        return QuarantineResponse(
            quarantine_id=manifest.quarantine_id,
            manifest_ref=str(payload.get("manifest_ref") or manifest.quarantine_id),
            retention_until=manifest.retention_until,
            files_moved=manifest.files_moved,
        )

    def list_active(self) -> list[QuarantineManifest]:
        rows = self._store().list_quarantine_manifests(
            active_after_iso=datetime.now(timezone.utc).isoformat(),
            base_storage_ref_hint=self._base_storage_ref_hint,
        )
        return [self._manifest_from_payload(row) for row in rows]

    def inspect(self, quarantine_id: str) -> QuarantineManifest | None:
        payload = self._store().read_quarantine_manifest(
            quarantine_id=quarantine_id,
            base_storage_ref_hint=self._base_storage_ref_hint,
        )
        if payload is None:
            return None
        return self._manifest_from_payload(payload)

    def _store(self) -> Any:
        if self._artifact_store is not None:
            return self._artifact_store
        try:
            from okto_pulse.core.kg.interfaces import get_kg_registry

            return get_kg_registry().require_rebuild_audit_artifact_store()
        except Exception as exc:
            raise QuarantineError(
                QuarantineErrorCode.QUARANTINE_STORAGE_UNAVAILABLE,
                retryable=False,
                reason=f"quarantine artifact store unavailable: {exc}",
            ) from exc

    @staticmethod
    def _manifest_from_payload(payload: Mapping[str, Any]) -> QuarantineManifest:
        return QuarantineManifest(
            quarantine_id=str(payload["quarantine_id"]),
            board_id=str(payload["board_id"]),
            graph_type=str(payload["graph_type"]),
            reason=str(payload["reason"]),
            reason_bucket=str(payload["reason_bucket"]),
            correlation_ids=tuple(payload.get("correlation_ids") or ()),
            affected_storage_refs=tuple(
                StorageRef(
                    token=str(ref["token"]),
                    namespace=str(ref.get("namespace") or "graph"),
                )
                for ref in (payload.get("affected_storage_refs") or ())
            ),
            kg_generation_id=(
                str(payload["kg_generation_id"])
                if payload.get("kg_generation_id") is not None
                else None
            ),
            software_version=str(
                payload.get("software_version", SOFTWARE_VERSION_FALLBACK)
            ),
            quarantined_at=str(payload["quarantined_at"]),
            retention_until=str(payload["retention_until"]),
            files_moved=int(payload.get("files_moved", 0)),
        )


__all__ = [
    "CANONICAL_GRAPH_TYPES",
    "DEFAULT_RETENTION_DAYS",
    "KGQuarantineService",
    "MANIFEST_FILENAME",
    "QUARANTINE_DIRNAME",
    "QuarantineConfig",
    "QuarantineError",
    "QuarantineErrorCode",
    "QuarantineManifest",
    "QuarantineReason",
    "QuarantineResponse",
    "get_quarantine_counter",
    "get_quarantine_counter_labels",
    "get_quarantine_counter_samples",
    "reset_quarantine_counter",
]
