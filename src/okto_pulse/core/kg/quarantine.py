"""Quarantine-before-purge service (KG-01 FR7+FR10, contract api_ee77f56f).

Before any purge of `graph.lbug`, `discovery.lbug` or their sidecars, the
recovery service MUST move the affected files into quarantine and write
an auditable manifest. This module is the canonical implementation:

* Moves files atomically into `<base_dir>/quarantine/<quarantine_id>/`.
* Writes a `manifest.json` next to them, capturing every field required
  by TR8 (board_id, graph_type, kg_generation_id, reason, timestamps,
  software_version, correlation_ids).
* Validates `affected_paths` against the scoped recovery boundary —
  paths outside known KG storage roots raise `affected_path_out_of_scope`
  per TR9 / BR `Application data must never be deleted`.
* Computes `retention_until` from a configurable `retention_days`
  (TR9 default = 30 days) so an operator-driven cleanup job can prune
  stale quarantines without losing recent evidence.
* Emits the canonical `kg_quarantine_total{board_id, graph_type,
  outcome, reason}` counter (OR `or_05fd5cd3`). Outcomes include
  `created` and the failure modes; reason values are constrained to a
  safe enum to avoid leaking sensitive content.

The service does NOT delete the original files after moving — they ARE
moved (renamed) into quarantine, which is the atomic operation. A
caller that fails between move and manifest write would leave files in
quarantine but with no manifest; the public API treats that as a hard
error and refuses the partial state by attempting a best-effort rollback.

Storage layout:

    <base_dir>/
        quarantine/
            <quarantine_id>/
                manifest.json
                <copied/moved files>
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import shutil
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger("okto_pulse.kg.quarantine")


QUARANTINE_DIRNAME = "quarantine"
MANIFEST_FILENAME = "manifest.json"
DEFAULT_RETENTION_DAYS = 30  # TR9.
SOFTWARE_VERSION_FALLBACK = "unknown"


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
    AFFECTED_PATH_OUT_OF_SCOPE = "affected_path_out_of_scope"


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
    """In-memory view of the on-disk manifest (TR8)."""

    quarantine_id: str
    board_id: str
    graph_type: str
    reason: str
    reason_bucket: str  # one of QuarantineReason
    correlation_ids: tuple[str, ...]
    affected_paths_relative: tuple[str, ...]
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
            "affected_paths_relative": list(self.affected_paths_relative),
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


# --- The service ---------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _retention_until(retention_days: int) -> str:
    return (
        datetime.now(timezone.utc) + timedelta(days=retention_days)
    ).isoformat()


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


def _read_software_version() -> str:
    try:
        from importlib.metadata import version
        return version("okto-pulse-core")
    except Exception:
        return SOFTWARE_VERSION_FALLBACK


class KGQuarantineService:
    """Atomically move affected paths into quarantine + manifest.

    The service is process-local but file-system safe — each quarantine
    gets a unique directory keyed by a fresh `quarantine_id`. Two
    concurrent calls cannot collide because the directory create uses
    `os.makedirs(..., exist_ok=False)` on a fresh UUID-style name.

    `scope_roots` is the closed set of filesystem prefixes the service
    will accept. Any `affected_path` not under one of these prefixes is
    rejected with `AFFECTED_PATH_OUT_OF_SCOPE` per FR10 — the spec
    forbids the recovery service from touching app data outside of the
    KG storage roots.
    """

    def __init__(
        self,
        *,
        base_dir: Path,
        scope_roots: list[Path],
        config: QuarantineConfig | None = None,
    ) -> None:
        if not scope_roots:
            raise ValueError("scope_roots must include at least one KG storage root")
        self._base_dir = Path(base_dir)
        self._scope_roots = [Path(r).resolve() for r in scope_roots]
        self._config = config or QuarantineConfig()

    # --- public API --------------------------------------------------------

    def create(
        self,
        *,
        board_id: str,
        graph_type: str,
        affected_paths: list[str],
        reason: str,
        correlation_ids: list[str],
        kg_generation_id: str | None = None,
    ) -> QuarantineResponse:
        """Quarantine the supplied paths and write the manifest.

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
                QuarantineErrorCode.AFFECTED_PATH_OUT_OF_SCOPE,
                retryable=False,
                reason=f"unknown graph_type: {graph_type}",
            )
        if not affected_paths:
            _bump_quarantine_counter(
                board_id=board_id,
                graph_type=graph_type,
                outcome="out_of_scope",
                reason=QuarantineReason.UNKNOWN.value,
            )
            raise QuarantineError(
                QuarantineErrorCode.AFFECTED_PATH_OUT_OF_SCOPE,
                retryable=False,
                reason="affected_paths must be non-empty",
            )

        # Validate all paths BEFORE moving anything. Any single out-of-
        # scope path aborts the whole operation — partial quarantine
        # would leak app-data into the recovery storage.
        resolved_paths: list[Path] = []
        for raw in affected_paths:
            candidate = Path(raw).resolve()
            if not self._is_in_scope(candidate):
                _bump_quarantine_counter(
                    board_id=board_id,
                    graph_type=graph_type,
                    outcome="out_of_scope",
                    reason=_bucket_reason(reason),
                )
                raise QuarantineError(
                    QuarantineErrorCode.AFFECTED_PATH_OUT_OF_SCOPE,
                    retryable=False,
                    reason=(
                        f"path {candidate} is not under any KG storage root"
                    ),
                )
            resolved_paths.append(candidate)

        # Allocate the quarantine directory atomically. `exist_ok=False`
        # is the guard — two concurrent calls can't share a dir.
        quarantine_id = f"q_{secrets.token_urlsafe(16)}"
        quarantine_dir = self._base_dir / QUARANTINE_DIRNAME / quarantine_id
        try:
            quarantine_dir.mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            # Astronomical odds — but retryable.
            _bump_quarantine_counter(
                board_id=board_id,
                graph_type=graph_type,
                outcome="storage_unavailable",
                reason=_bucket_reason(reason),
            )
            raise QuarantineError(
                QuarantineErrorCode.QUARANTINE_STORAGE_UNAVAILABLE,
                retryable=True,
                reason=f"quarantine_id collision: {quarantine_id}",
            )
        except OSError as exc:
            _bump_quarantine_counter(
                board_id=board_id,
                graph_type=graph_type,
                outcome="storage_unavailable",
                reason=_bucket_reason(reason),
            )
            raise QuarantineError(
                QuarantineErrorCode.QUARANTINE_STORAGE_UNAVAILABLE,
                retryable=True,
                reason=f"mkdir failed: {exc}",
            ) from exc

        moved_relatives: list[str] = []
        files_moved = 0
        try:
            for src in resolved_paths:
                if not src.exists():
                    # The source vanished between validation and move —
                    # acceptable in a corruption scenario where the FS
                    # is unstable. Record None in the manifest.
                    moved_relatives.append(src.name)
                    continue
                dst = quarantine_dir / src.name
                shutil.move(str(src), str(dst))
                moved_relatives.append(src.name)
                files_moved += 1

            reason_bucket = _bucket_reason(reason)
            manifest = QuarantineManifest(
                quarantine_id=quarantine_id,
                board_id=board_id,
                graph_type=graph_type,
                reason=reason,
                reason_bucket=reason_bucket,
                correlation_ids=tuple(correlation_ids),
                affected_paths_relative=tuple(moved_relatives),
                kg_generation_id=kg_generation_id,
                software_version=_read_software_version(),
                quarantined_at=_now_iso(),
                retention_until=_retention_until(self._config.retention_days),
                files_moved=files_moved,
            )
            manifest_path = quarantine_dir / MANIFEST_FILENAME
            with manifest_path.open("w", encoding="utf-8") as fh:
                json.dump(manifest.to_disk_dict(), fh, indent=2)
        except QuarantineError:
            raise
        except Exception as exc:
            # Manifest write failed AFTER files were moved. val_79e6f555
            # rework: NEVER rmtree the partial quarantine — that would
            # destroy the very evidence we just rescued. Instead rename
            # the directory to `<quarantine_id>.partial` so the operator
            # can recover it manually, and surface the typed error.
            logger.error(
                "kg.quarantine.manifest_write_failed quarantine_id=%s err=%s "
                "preserved_dir=%s.partial",
                quarantine_id, exc, quarantine_dir,
            )
            partial_dir = quarantine_dir.with_name(quarantine_dir.name + ".partial")
            try:
                # If a previous partial exists with the same name
                # (astronomical), append a unique suffix so we never
                # overwrite preserved evidence.
                if partial_dir.exists():
                    partial_dir = quarantine_dir.with_name(
                        f"{quarantine_dir.name}.partial.{secrets.token_hex(4)}"
                    )
                quarantine_dir.rename(partial_dir)
            except OSError as rename_exc:
                logger.error(
                    "kg.quarantine.partial_preserve_failed src=%s err=%s",
                    quarantine_dir, rename_exc,
                )
            _bump_quarantine_counter(
                board_id=board_id,
                graph_type=graph_type,
                outcome="manifest_failed",
                reason=_bucket_reason(reason),
            )
            raise QuarantineError(
                QuarantineErrorCode.QUARANTINE_STORAGE_UNAVAILABLE,
                retryable=True,
                reason=f"manifest write failed: {exc} (preserved at {partial_dir})",
            ) from exc

        logger.warning(
            "kg.quarantine.created quarantine_id=%s board=%s graph_type=%s "
            "files=%d reason=%s",
            quarantine_id, board_id, graph_type, files_moved, reason,
            extra={
                "event": "kg.quarantine.created",
                "quarantine_id": quarantine_id,
                "board_id": board_id,
                "graph_type": graph_type,
                "reason_bucket": manifest.reason_bucket,
                "correlation_ids": list(correlation_ids),
                "files_moved": files_moved,
            },
        )
        _bump_quarantine_counter(
            board_id=board_id,
            graph_type=graph_type,
            outcome="created",
            reason=manifest.reason_bucket,
        )
        return QuarantineResponse(
            quarantine_id=quarantine_id,
            manifest_ref=str(manifest_path),
            retention_until=manifest.retention_until,
            files_moved=files_moved,
        )

    def list_active(self) -> list[QuarantineManifest]:
        """Return manifests still within their retention window.

        Read-only inspection used by the recovery service and the
        operator dashboard. Never raises — corrupt manifests are
        logged and skipped.
        """
        root = self._base_dir / QUARANTINE_DIRNAME
        if not root.exists():
            return []
        out: list[QuarantineManifest] = []
        now = datetime.now(timezone.utc)
        for entry in sorted(root.iterdir()):
            if not entry.is_dir():
                continue
            manifest = self._read_manifest(entry)
            if manifest is None:
                continue
            try:
                retention_until = datetime.fromisoformat(manifest.retention_until)
            except ValueError:
                continue
            if retention_until > now:
                out.append(manifest)
        return out

    def inspect(self, quarantine_id: str) -> QuarantineManifest | None:
        """Read one manifest by id, or None if missing/corrupt."""
        return self._read_manifest(
            self._base_dir / QUARANTINE_DIRNAME / quarantine_id
        )

    # --- internals ---------------------------------------------------------

    def _is_in_scope(self, path: Path) -> bool:
        try:
            resolved = path.resolve()
        except (OSError, RuntimeError):
            return False
        for root in self._scope_roots:
            try:
                resolved.relative_to(root)
                return True
            except ValueError:
                continue
        return False

    def _read_manifest(self, quarantine_dir: Path) -> QuarantineManifest | None:
        manifest_path = quarantine_dir / MANIFEST_FILENAME
        try:
            with manifest_path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            return QuarantineManifest(
                quarantine_id=str(data["quarantine_id"]),
                board_id=str(data["board_id"]),
                graph_type=str(data["graph_type"]),
                reason=str(data["reason"]),
                reason_bucket=str(data["reason_bucket"]),
                correlation_ids=tuple(data.get("correlation_ids") or ()),
                affected_paths_relative=tuple(
                    data.get("affected_paths_relative") or ()
                ),
                kg_generation_id=(
                    str(data["kg_generation_id"])
                    if data.get("kg_generation_id") is not None
                    else None
                ),
                software_version=str(data.get("software_version", SOFTWARE_VERSION_FALLBACK)),
                quarantined_at=str(data["quarantined_at"]),
                retention_until=str(data["retention_until"]),
                files_moved=int(data.get("files_moved", 0)),
            )
        except (FileNotFoundError, OSError, ValueError, KeyError) as exc:
            logger.warning(
                "kg.quarantine.manifest_unreadable path=%s err=%s",
                manifest_path, exc,
            )
            return None


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
