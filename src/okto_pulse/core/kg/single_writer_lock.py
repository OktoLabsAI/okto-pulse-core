"""Cross-process single-writer lock per board (KG-01, FR5, contract api_2f4b18e4).

Every write to a board's `graph.lbug` or `discovery.lbug` MUST acquire
this lock first. The lock is cross-process: distinct Python processes
(uvicorn worker, CLI, MCP server, cron tick) all coordinate through a
filesystem sentinel keyed by `board_id`. A successful acquire returns
an opaque `owner_token` that the caller MUST present to `release()` and
to the KG-01.3 safe write lifecycle — workers cannot forge tokens.

Stale recovery is built in. Each lock carries an `expires_at` (UTC ISO);
a holder that dies without releasing leaves a stale sentinel which the
next caller can reclaim if `expires_at < now`. Recovery is deterministic
and audited via a logged event — the spec forbids silent stale takeover.

`admin_lane` is a flag (TR7): the rebuild/reset path acquires the same
lock but under the admin lane, signalling to the gate (KG-01.2) that
normal writes should drain instead of trying to coexist. Admin lane
behaviour does NOT bypass the lock — there is exactly one writer at any
moment per board, period.

This module is process-local-safe AND cross-process-safe. The filesystem
primitive used (`O_CREAT | O_EXCL`) is atomic on POSIX and on Windows
NTFS for paths that don't pre-exist as directories, so we avoid the
need for `fcntl` (which is POSIX-only) or `msvcrt.locking` (which holds
the FD across processes). The trade-off: stale recovery requires a
deterministic `now` comparison, not OS-level lease semantics.

Storage layout:

    <board_dir>/.write.lock     JSON file with the lock manifest

The manifest is intentionally human-readable so operators can diagnose
a stuck lock with `cat .write.lock` instead of binary tooling.
"""

from __future__ import annotations

import errno
import json
import logging
import os
import secrets
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger("okto_pulse.kg.single_writer_lock")


# --- Observability counter (OR or_a6018284) -----------------------------------
#
# kg_single_writer_lock_total labels (canonical):
#   board_id, operation, outcome, stale_recovered
# outcome in {"acquired", "contention", "released", "release_token_mismatch",
#             "release_noop", "stale_recovery_failed"}
# stale_recovered in {"true", "false"} — "true" only when an acquisition
# took ownership after reclaiming an expired sentinel.

_LOCK_COUNTER_LABELS = (
    "board_id",
    "operation",
    "outcome",
    "stale_recovered",
)

_LockCounterKey = tuple[str, str, str, str]
_lock_counter: dict[_LockCounterKey, int] = {}
_lock_counter_lock = threading.Lock()


def _bump_lock_counter(
    *, board_id: str, operation: str, outcome: str, stale_recovered: bool
) -> None:
    key: _LockCounterKey = (
        board_id,
        operation,
        outcome,
        "true" if stale_recovered else "false",
    )
    with _lock_counter_lock:
        _lock_counter[key] = _lock_counter.get(key, 0) + 1


def get_lock_counter(
    board_id: str,
    outcome: str,
    *,
    operation: str | None = None,
    stale_recovered: bool | None = None,
) -> int:
    """Read kg_single_writer_lock_total slice. Filters are exact-match."""
    stale_label = (
        None if stale_recovered is None else ("true" if stale_recovered else "false")
    )
    with _lock_counter_lock:
        total = 0
        for (b, op, out, stale), value in _lock_counter.items():
            if b != board_id or out != outcome:
                continue
            if operation is not None and op != operation:
                continue
            if stale_label is not None and stale != stale_label:
                continue
            total += value
        return total


def get_lock_counter_samples() -> list[dict[str, Any]]:
    """Snapshot of every (label tuple, count). Shape matches the OR."""
    with _lock_counter_lock:
        return [
            {
                "board_id": b,
                "operation": op,
                "outcome": out,
                "stale_recovered": stale,
                "count": value,
            }
            for (b, op, out, stale), value in _lock_counter.items()
        ]


def get_lock_counter_labels() -> tuple[str, ...]:
    """Expose OR label tuple so emitters can validate shape."""
    return _LOCK_COUNTER_LABELS


def reset_lock_counter() -> None:
    """Test hook."""
    with _lock_counter_lock:
        _lock_counter.clear()


LOCK_FILENAME = ".write.lock"
RECOVERY_LOCK_FILENAME = ".write.lock.recovering"
DEFAULT_TTL_SECONDS = 300  # 5 minutes — generous default for bulk writes.
MAX_TTL_SECONDS = 3600     # 1 hour — defensive cap against runaway holders.
# Stale-recovery auxiliary lock TTL. Short — recovery is supposed to be
# a tiny critical section (re-read + unlink + create). Anything longer
# is a hung recoverer and gets reclaimed.
RECOVERY_LOCK_TTL_SECONDS = 30


class SingleWriterLockErrorCode(str, Enum):
    """Typed error codes per contract api_2f4b18e4 response_errors."""

    LOCK_CONTENTION = "lock_contention"
    STALE_LOCK_RECOVERY_FAILED = "stale_lock_recovery_failed"


class SingleWriterLockError(Exception):
    """Raised when acquire cannot return a decision at all.

    `acquire()` returning ``acquired=False`` is NOT an error — it is a
    legitimate decision the caller can retry on its own schedule. This
    exception is reserved for the unrecoverable case where the stale
    recovery path itself failed (e.g. file gone after we read it,
    permissions denied, disk full).
    """

    def __init__(
        self,
        code: SingleWriterLockErrorCode,
        *,
        retryable: bool,
        reason: str,
    ) -> None:
        super().__init__(f"{code.value}: {reason}")
        self.code = code
        self.retryable = retryable
        self.reason = reason


@dataclass(frozen=True, slots=True)
class LockAcquisition:
    """Frozen contract-shaped response per api_2f4b18e4 success body."""

    acquired: bool
    owner_token: str | None
    expires_at: str | None
    current_owner: str | None
    # Admin lane flag is exposed back to the caller so they (and tests)
    # can assert the lock was taken under the right lane. Not in the
    # contract; not breaking either.
    admin_lane: bool = False


@dataclass(frozen=True, slots=True)
class LockManifest:
    """In-process view of the on-disk sentinel."""

    owner_token: str
    owner_id: str
    operation: str
    acquired_at_epoch: float
    expires_at_epoch: float
    admin_lane: bool

    @staticmethod
    def from_disk_dict(data: dict[str, Any]) -> LockManifest:
        return LockManifest(
            owner_token=str(data["owner_token"]),
            owner_id=str(data["owner_id"]),
            operation=str(data["operation"]),
            acquired_at_epoch=float(data["acquired_at_epoch"]),
            expires_at_epoch=float(data["expires_at_epoch"]),
            admin_lane=bool(data.get("admin_lane", False)),
        )

    def to_disk_dict(self) -> dict[str, Any]:
        return {
            "owner_token": self.owner_token,
            "owner_id": self.owner_id,
            "operation": self.operation,
            "acquired_at_epoch": self.acquired_at_epoch,
            "expires_at_epoch": self.expires_at_epoch,
            "admin_lane": self.admin_lane,
            # ISO mirrors for human operators reading the file directly.
            "acquired_at": datetime.fromtimestamp(
                self.acquired_at_epoch, tz=timezone.utc
            ).isoformat(),
            "expires_at": datetime.fromtimestamp(
                self.expires_at_epoch, tz=timezone.utc
            ).isoformat(),
        }


def _lock_path(board_dir: Path) -> Path:
    return board_dir / LOCK_FILENAME


def _recovery_lock_path(board_dir: Path) -> Path:
    return board_dir / RECOVERY_LOCK_FILENAME


def _now_epoch() -> float:
    return time.time()


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def _read_manifest(path: Path) -> LockManifest | None:
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        return LockManifest.from_disk_dict(data)
    except FileNotFoundError:
        return None
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        logger.warning(
            "kg.single_writer_lock.manifest_corrupt path=%s err=%s",
            path, exc,
        )
        return None


class KGSingleWriterLock:
    """Per-board cross-process owner-token lock.

    Single instance can serve many boards — each board gets its own
    sentinel file under its storage directory. The class is thread-safe
    within one process via the OS-level atomic ``O_CREAT|O_EXCL`` create.

    Resolution strategy is supplied by the caller. By default the lock
    uses ``base_dir / board_id`` as the per-board directory, but the KG
    infrastructure may pass an alternate resolver (e.g. the LadybugDB
    board path) so the sentinel lives next to the graph file.
    """

    def __init__(
        self,
        base_dir: Path | None = None,
        *,
        board_dir_resolver: "callable[[str], Path] | None" = None,
    ) -> None:
        if board_dir_resolver is None and base_dir is None:
            raise ValueError(
                "KGSingleWriterLock requires either base_dir or board_dir_resolver"
            )
        self._base_dir = base_dir
        self._resolver = board_dir_resolver

    # --- public API --------------------------------------------------------

    def acquire(
        self,
        *,
        board_id: str,
        operation: str,
        owner_id: str,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        admin_lane: bool = False,
    ) -> LockAcquisition:
        """Try to acquire the lock for ``board_id``.

        Returns a frozen ``LockAcquisition``:

        * ``acquired=True`` — caller owns the lock; ``owner_token`` and
          ``expires_at`` are populated. Pass the token to ``release()``
          and to ``KGSafeWriteLifecycle.apply``.
        * ``acquired=False`` — someone else owns it; ``current_owner``
          identifies them. The caller decides whether to backoff and
          retry or surface the contention to the user.

        Raises ``SingleWriterLockError`` ONLY when the stale recovery
        path failed irrecoverably. Normal contention is NOT an error.
        """
        if ttl_seconds < 1:
            raise ValueError("ttl_seconds must be >= 1")
        if ttl_seconds > MAX_TTL_SECONDS:
            raise ValueError(
                f"ttl_seconds={ttl_seconds} exceeds MAX_TTL_SECONDS={MAX_TTL_SECONDS}"
            )

        board_dir = self._resolve_board_dir(board_id)
        board_dir.mkdir(parents=True, exist_ok=True)
        path = _lock_path(board_dir)

        # Fast path: no sentinel exists, try atomic create.
        acquisition = self._try_create(
            path=path,
            operation=operation,
            owner_id=owner_id,
            ttl_seconds=ttl_seconds,
            admin_lane=admin_lane,
        )
        if acquisition is not None:
            _bump_lock_counter(
                board_id=board_id,
                operation=operation,
                outcome="acquired",
                stale_recovered=False,
            )
            return acquisition

        # Sentinel exists. Inspect it.
        manifest = _read_manifest(path)
        if manifest is None:
            # File existed when we tried EXCL but vanished/corrupt now.
            # One more attempt before declaring contention.
            acquisition = self._try_create(
                path=path,
                operation=operation,
                owner_id=owner_id,
                ttl_seconds=ttl_seconds,
                admin_lane=admin_lane,
            )
            if acquisition is not None:
                _bump_lock_counter(
                    board_id=board_id,
                    operation=operation,
                    outcome="acquired",
                    stale_recovered=False,
                )
                return acquisition
            _bump_lock_counter(
                board_id=board_id,
                operation=operation,
                outcome="stale_recovery_failed",
                stale_recovered=False,
            )
            raise SingleWriterLockError(
                SingleWriterLockErrorCode.STALE_LOCK_RECOVERY_FAILED,
                retryable=True,
                reason="manifest_unreadable_and_recreate_failed",
            )

        # Live lock — report contention. NOT an exception per contract.
        if manifest.expires_at_epoch > _now_epoch():
            logger.info(
                "kg.single_writer_lock.contention board=%s current_owner=%s "
                "operation=%s admin_lane=%s expires_at=%s",
                board_id, manifest.owner_id, manifest.operation,
                manifest.admin_lane, _iso(manifest.expires_at_epoch),
            )
            _bump_lock_counter(
                board_id=board_id,
                operation=operation,
                outcome="contention",
                stale_recovered=False,
            )
            return LockAcquisition(
                acquired=False,
                owner_token=None,
                expires_at=_iso(manifest.expires_at_epoch),
                current_owner=manifest.owner_id,
                admin_lane=manifest.admin_lane,
            )

        # Stale lock — TTL expired. Recovery must be ATOMICALLY safe
        # against a concurrent recoverer. The first version (CAS via
        # re-read just before unlink) still left a window between the
        # CAS and the unlink syscall, reproduced by validator val_3f8803ab.
        #
        # The fix is a second sentinel: <board_dir>/.write.lock.recovering.
        # Only the process that wins the atomic O_CREAT|O_EXCL on that
        # auxiliary lock may perform the stale recovery sequence. Everyone
        # else sees the recovery-lock present and bails to contention —
        # they NEVER touch the primary .write.lock.
        #
        # Inside the recovery-lock critical section we still re-read the
        # primary manifest: if it changed (e.g. because the previous
        # recovery-lock holder finished a fresh acquire and released its
        # recovery-lock just before we entered ours), we treat that as
        # contention and don't touch the primary lock.
        recovery_path = _recovery_lock_path(board_dir)
        recovery_handle = self._try_create_recovery_lock(
            recovery_path, board_id, operation, owner_id
        )
        if recovery_handle is None:
            # val_189fe864: distinguish a LIVE recovery-lock (another
            # recoverer currently in critical section — retryable
            # contention) from a STALE recovery-lock (a crashed
            # recoverer left it behind — operator must clean up). We
            # never auto-reclaim, because that reintroduces the same
            # cross-process race we are trying to fix.
            recovery_manifest = self._read_recovery_manifest(recovery_path)
            expires = float(
                (recovery_manifest or {}).get("expires_at_epoch", 0)
            )
            if recovery_manifest is not None and expires <= _now_epoch():
                logger.error(
                    "kg.single_writer_lock.recovery_lock_stale board=%s "
                    "former_owner=%s expired_at=%s now=%s "
                    "reason=manual_intervention_required",
                    board_id,
                    recovery_manifest.get("owner_id"),
                    _iso(expires),
                    _iso(_now_epoch()),
                    extra={
                        "event": "kg.single_writer_lock.recovery_lock_stale",
                        "board_id": board_id,
                        "former_recovery_owner": recovery_manifest.get("owner_id"),
                        "former_recovery_operation": recovery_manifest.get(
                            "operation"
                        ),
                        "expired_at": _iso(expires),
                    },
                )
                _bump_lock_counter(
                    board_id=board_id,
                    operation=operation,
                    outcome="stale_recovery_failed",
                    stale_recovered=False,
                )
                raise SingleWriterLockError(
                    SingleWriterLockErrorCode.STALE_LOCK_RECOVERY_FAILED,
                    retryable=False,
                    reason=(
                        "recovery_lock_stale_manual_intervention_required: "
                        f"former_owner={recovery_manifest.get('owner_id')} "
                        f"expired_at={_iso(expires)}"
                    ),
                )
            # Live recovery-lock — another recoverer is in critical
            # section. Report contention with whatever the primary
            # manifest shows now.
            current = _read_manifest(path)
            logger.info(
                "kg.single_writer_lock.recovery_contention board=%s "
                "current_owner=%s reason=recovery_lock_busy",
                board_id, current.owner_id if current else "unknown",
            )
            _bump_lock_counter(
                board_id=board_id,
                operation=operation,
                outcome="contention",
                stale_recovered=False,
            )
            if current is None:
                return LockAcquisition(
                    acquired=False, owner_token=None,
                    expires_at=None, current_owner=None,
                )
            return LockAcquisition(
                acquired=False,
                owner_token=None,
                expires_at=_iso(current.expires_at_epoch),
                current_owner=current.owner_id,
                admin_lane=current.admin_lane,
            )

        try:
            stale_token = manifest.owner_token
            stale_expires = manifest.expires_at_epoch
            stale_owner = manifest.owner_id
            logger.warning(
                "kg.single_writer_lock.stale_recovery board=%s former_owner=%s "
                "operation=%s expired_at=%s now=%s",
                board_id, manifest.owner_id, manifest.operation,
                _iso(manifest.expires_at_epoch), _iso(_now_epoch()),
                extra={
                    "event": "kg.single_writer_lock.stale_recovery",
                    "board_id": board_id,
                    "former_owner": manifest.owner_id,
                    "former_operation": manifest.operation,
                    "former_admin_lane": manifest.admin_lane,
                    "expired_at": _iso(manifest.expires_at_epoch),
                },
            )

            revalidated = _read_manifest(path)
            if revalidated is None:
                # Someone else cleaned it up first. Try fresh acquire.
                acquisition = self._try_create(
                    path=path,
                    operation=operation,
                    owner_id=owner_id,
                    ttl_seconds=ttl_seconds,
                    admin_lane=admin_lane,
                )
                if acquisition is not None:
                    _bump_lock_counter(
                        board_id=board_id,
                        operation=operation,
                        outcome="acquired",
                        stale_recovered=True,
                    )
                    return acquisition
                return self._contention_after_race(
                    path, board_id, operation
                )

            if (
                revalidated.owner_token != stale_token
                or revalidated.expires_at_epoch != stale_expires
                or revalidated.owner_id != stale_owner
            ):
                # Either someone bypassed the recovery-lock (shouldn't
                # happen under our protocol) OR a previous recoverer's
                # fresh lock is still in place and we read it as "stale"
                # incorrectly. Either way: NOT safe to unlink. Treat as
                # contention against the actual current owner.
                logger.info(
                    "kg.single_writer_lock.stale_recovery_aborted board=%s "
                    "new_owner=%s reason=manifest_changed_under_us",
                    board_id, revalidated.owner_id,
                )
                _bump_lock_counter(
                    board_id=board_id,
                    operation=operation,
                    outcome="contention",
                    stale_recovered=False,
                )
                if revalidated.expires_at_epoch <= _now_epoch():
                    # Defensive: if what we re-read is also stale but
                    # different, surface as stale_recovery_failed rather
                    # than silently looping. Operator can rerun.
                    raise SingleWriterLockError(
                        SingleWriterLockErrorCode.STALE_LOCK_RECOVERY_FAILED,
                        retryable=True,
                        reason="manifest_changed_to_another_stale_lock",
                    )
                return LockAcquisition(
                    acquired=False,
                    owner_token=None,
                    expires_at=_iso(revalidated.expires_at_epoch),
                    current_owner=revalidated.owner_id,
                    admin_lane=revalidated.admin_lane,
                )

            try:
                os.unlink(path)
            except FileNotFoundError:
                pass  # Acceptable — primary already gone.
            except OSError as exc:
                _bump_lock_counter(
                    board_id=board_id,
                    operation=operation,
                    outcome="stale_recovery_failed",
                    stale_recovered=False,
                )
                raise SingleWriterLockError(
                    SingleWriterLockErrorCode.STALE_LOCK_RECOVERY_FAILED,
                    retryable=True,
                    reason=f"unlink_failed: {exc}",
                ) from exc

            acquisition = self._try_create(
                path=path,
                operation=operation,
                owner_id=owner_id,
                ttl_seconds=ttl_seconds,
                admin_lane=admin_lane,
            )
            if acquisition is not None:
                _bump_lock_counter(
                    board_id=board_id,
                    operation=operation,
                    outcome="acquired",
                    stale_recovered=True,
                )
                return acquisition
            return self._contention_after_race(path, board_id, operation)
        finally:
            self._release_recovery_lock(recovery_path)

    def _contention_after_race(
        self, path: Path, board_id: str, operation: str
    ) -> LockAcquisition:
        manifest = _read_manifest(path)
        if manifest is None:
            _bump_lock_counter(
                board_id=board_id,
                operation=operation,
                outcome="stale_recovery_failed",
                stale_recovered=False,
            )
            raise SingleWriterLockError(
                SingleWriterLockErrorCode.STALE_LOCK_RECOVERY_FAILED,
                retryable=True,
                reason="race_during_recovery_and_manifest_gone",
            )
        _bump_lock_counter(
            board_id=board_id,
            operation=operation,
            outcome="contention",
            stale_recovered=False,
        )
        return LockAcquisition(
            acquired=False,
            owner_token=None,
            expires_at=_iso(manifest.expires_at_epoch),
            current_owner=manifest.owner_id,
            admin_lane=manifest.admin_lane,
        )

    def release(self, *, board_id: str, owner_token: str) -> bool:
        """Release the lock if ``owner_token`` matches the current holder.

        Returns True if the sentinel was removed; False if the lock had
        already expired/been recovered or if the token doesn't match.
        Never raises for the common cases — a worker that crashed and
        restarted shouldn't crash again trying to clean up.
        """
        board_dir = self._resolve_board_dir(board_id)
        path = _lock_path(board_dir)
        manifest = _read_manifest(path)
        if manifest is None:
            _bump_lock_counter(
                board_id=board_id,
                operation="release",
                outcome="release_noop",
                stale_recovered=False,
            )
            return False
        if manifest.owner_token != owner_token:
            logger.warning(
                "kg.single_writer_lock.release_token_mismatch board=%s "
                "passed_owner_token_prefix=%s current_owner_id=%s",
                board_id, owner_token[:8], manifest.owner_id,
            )
            _bump_lock_counter(
                board_id=board_id,
                operation=manifest.operation,
                outcome="release_token_mismatch",
                stale_recovered=False,
            )
            return False
        try:
            os.unlink(path)
        except FileNotFoundError:
            _bump_lock_counter(
                board_id=board_id,
                operation=manifest.operation,
                outcome="release_noop",
                stale_recovered=False,
            )
            return False
        _bump_lock_counter(
            board_id=board_id,
            operation=manifest.operation,
            outcome="released",
            stale_recovered=False,
        )
        return True

    def inspect(self, *, board_id: str) -> LockManifest | None:
        """Read the current manifest without acquiring — observability."""
        board_dir = self._resolve_board_dir(board_id)
        return _read_manifest(_lock_path(board_dir))

    # --- internals ---------------------------------------------------------

    def _resolve_board_dir(self, board_id: str) -> Path:
        if self._resolver is not None:
            return Path(self._resolver(board_id))
        assert self._base_dir is not None
        return self._base_dir / board_id

    def _try_create(
        self,
        *,
        path: Path,
        operation: str,
        owner_id: str,
        ttl_seconds: int,
        admin_lane: bool,
    ) -> LockAcquisition | None:
        """Atomic create. Returns the acquisition on success, None if
        the sentinel already exists. Other OSErrors propagate as
        ``SingleWriterLockError``.
        """
        token = secrets.token_urlsafe(24)
        acquired_at = _now_epoch()
        expires_at = acquired_at + ttl_seconds
        manifest = LockManifest(
            owner_token=token,
            owner_id=owner_id,
            operation=operation,
            acquired_at_epoch=acquired_at,
            expires_at_epoch=expires_at,
            admin_lane=admin_lane,
        )
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            fd = os.open(path, flags, 0o644)
        except FileExistsError:
            return None
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                return None
            raise SingleWriterLockError(
                SingleWriterLockErrorCode.STALE_LOCK_RECOVERY_FAILED,
                retryable=True,
                reason=f"open_failed: {exc}",
            ) from exc
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(manifest.to_disk_dict(), fh, indent=2)
        except Exception:
            # Failed to write the manifest after creating the file —
            # undo so we don't leave a corrupt sentinel.
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
            raise
        logger.info(
            "kg.single_writer_lock.acquired path=%s owner_id=%s "
            "operation=%s admin_lane=%s ttl_s=%d",
            path, owner_id, operation, admin_lane, ttl_seconds,
        )
        return LockAcquisition(
            acquired=True,
            owner_token=token,
            expires_at=_iso(expires_at),
            current_owner=owner_id,
            admin_lane=admin_lane,
        )

    def _try_create_recovery_lock(
        self,
        recovery_path: Path,
        board_id: str,
        operation: str,
        owner_id: str,
    ) -> Path | None:
        """Serialise stale recovery via a second sentinel.

        Returns ``recovery_path`` if this caller is now the unique
        recoverer for the board; returns ``None`` if another recoverer
        is already running.

        IMPORTANT (val_189fe864): we DO NOT auto-reclaim a stale
        recovery-lock. The previous version did (read-stale → unlink →
        recreate) which itself contained the same class of race as the
        primary stale recovery: a concurrent recoverer could place a
        fresh recovery-lock between our read and our unlink, and we
        would delete it. The validator's reproduction confirmed that
        two recoverers could both enter the critical section and one
        could end up deleting the other's fresh primary lock.

        Instead, an existing recovery-lock — whether live or stale —
        ALWAYS blocks the caller. The caller in ``acquire`` decides
        whether to surface contention (live recovery-lock) or
        ``STALE_LOCK_RECOVERY_FAILED`` (stale recovery-lock requires
        manual cleanup by an operator). This is the validator-approved
        Option 1: simpler and provably safe.
        """
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        manifest = {
            "owner_id": owner_id,
            "operation": operation,
            "board_id": board_id,
            "acquired_at_epoch": _now_epoch(),
            "expires_at_epoch": _now_epoch() + RECOVERY_LOCK_TTL_SECONDS,
        }
        try:
            fd = os.open(recovery_path, flags, 0o644)
        except FileExistsError:
            return None
        except OSError:
            return None
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(manifest, fh, indent=2)
        except Exception:
            try:
                os.unlink(recovery_path)
            except FileNotFoundError:
                pass
            raise
        return recovery_path

    def _read_recovery_manifest(
        self, recovery_path: Path
    ) -> dict[str, Any] | None:
        """Best-effort read of the recovery-lock manifest for diagnosis.

        Returns None when the file is missing/corrupt; never raises.
        """
        try:
            with recovery_path.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        except (FileNotFoundError, OSError, ValueError):
            return None

    def _release_recovery_lock(self, recovery_path: Path) -> None:
        """Always-safe release of the recovery-lock sentinel."""
        try:
            os.unlink(recovery_path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            logger.warning(
                "kg.single_writer_lock.recovery_release_failed path=%s err=%s",
                recovery_path, exc,
            )


__all__ = [
    "DEFAULT_TTL_SECONDS",
    "KGSingleWriterLock",
    "LockAcquisition",
    "LockManifest",
    "LOCK_FILENAME",
    "MAX_TTL_SECONDS",
    "RECOVERY_LOCK_FILENAME",
    "RECOVERY_LOCK_TTL_SECONDS",
    "SingleWriterLockError",
    "SingleWriterLockErrorCode",
    "get_lock_counter",
    "get_lock_counter_labels",
    "get_lock_counter_samples",
    "reset_lock_counter",
]
