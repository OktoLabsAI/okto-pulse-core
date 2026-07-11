"""Core single-writer lock contract and port-backed facade.

The core owns the KG write-lock policy, typed outcomes and observability
counters. Concrete coordination mechanics belong to an edition adapter exposed
through ``WriteLockPort``.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from okto_pulse.core.ports.coordination import (
    WriteLockHandle,
    WriteLockPort,
    get_write_lock_port,
)


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
    return _LOCK_COUNTER_LABELS


def reset_lock_counter() -> None:
    with _lock_counter_lock:
        _lock_counter.clear()


LOCK_FILENAME = ".write.lock"
RECOVERY_LOCK_FILENAME = ".write.lock.recovering"
DEFAULT_TTL_SECONDS = 300
MAX_TTL_SECONDS = 3600
RECOVERY_LOCK_TTL_SECONDS = 30


class SingleWriterLockErrorCode(str, Enum):
    LOCK_CONTENTION = "lock_contention"
    STALE_LOCK_RECOVERY_FAILED = "stale_lock_recovery_failed"


class SingleWriterLockError(Exception):
    """Raised when the write-lock adapter cannot return a safe decision."""

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
    acquired: bool
    owner_token: str | None
    expires_at: str | None
    current_owner: str | None
    admin_lane: bool = False
    stale_recovered: bool = False


@dataclass(frozen=True, slots=True)
class LockManifest:
    owner_token: str
    owner_id: str
    operation: str
    acquired_at_epoch: float
    expires_at_epoch: float
    admin_lane: bool

    @staticmethod
    def from_disk_dict(data: dict[str, Any]) -> "LockManifest":
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
            "acquired_at": datetime.fromtimestamp(
                self.acquired_at_epoch, tz=timezone.utc
            ).isoformat(),
            "expires_at": datetime.fromtimestamp(
                self.expires_at_epoch, tz=timezone.utc
            ).isoformat(),
        }


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


class KGSingleWriterLock:
    """Compatibility facade over the edition-owned ``WriteLockPort``.

    Historical callers pass ``base_dir`` or ``board_dir_resolver``. Those values
    are retained only as adapter hints; the core never interprets them as a
    filesystem contract.
    """

    artifact_id = "kg_single_writer"

    def __init__(
        self,
        base_dir: object | None = None,
        *,
        board_dir_resolver: Any | None = None,
        write_lock_port: WriteLockPort | None = None,
    ) -> None:
        self._write_lock_port = write_lock_port
        self._base_dir_hint = str(base_dir) if base_dir is not None else None
        self._board_dir_resolver = board_dir_resolver

    def acquire(
        self,
        *,
        board_id: str,
        operation: str,
        owner_id: str,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        admin_lane: bool = False,
    ) -> LockAcquisition:
        if ttl_seconds < 1:
            raise ValueError("ttl_seconds must be >= 1")
        if ttl_seconds > MAX_TTL_SECONDS:
            raise ValueError(
                f"ttl_seconds={ttl_seconds} exceeds MAX_TTL_SECONDS={MAX_TTL_SECONDS}"
            )

        port = self._port()
        if hasattr(port, "acquire_single_writer_sync"):
            result = port.acquire_single_writer_sync(
                board_id=board_id,
                artifact_id=self.artifact_id,
                operation=operation,
                owner_id=owner_id,
                ttl_seconds=ttl_seconds,
                admin_lane=admin_lane,
                base_dir_hint=self._base_dir_hint,
                board_dir_resolver=self._board_dir_resolver,
            )
            acquisition = self._coerce_acquisition(result, admin_lane=admin_lane)
        else:
            handle = port.acquire_sync(
                board_id,
                self.artifact_id,
                owner_token=owner_id,
            )
            acquisition = LockAcquisition(
                acquired=True,
                owner_token=handle.owner_token,
                expires_at=None,
                current_owner=owner_id,
                admin_lane=admin_lane,
            )

        _bump_lock_counter(
            board_id=board_id,
            operation=operation,
            outcome="acquired" if acquisition.acquired else "contention",
            stale_recovered=acquisition.stale_recovered,
        )
        return acquisition

    def release(self, *, board_id: str, owner_token: str) -> bool:
        current = self.inspect(board_id=board_id)
        port = self._port()
        if hasattr(port, "release_single_writer_sync"):
            released = bool(
                port.release_single_writer_sync(
                    board_id=board_id,
                    artifact_id=self.artifact_id,
                    owner_token=owner_token,
                    base_dir_hint=self._base_dir_hint,
                    board_dir_resolver=self._board_dir_resolver,
                )
            )
        else:
            handle = WriteLockHandle(
                board_id=board_id,
                artifact_id=self.artifact_id,
                owner_token=owner_token,
                fencing_token=owner_token,
            )
            port.release_sync(handle)
            released = not port.is_locked(board_id, self.artifact_id)

        _bump_lock_counter(
            board_id=board_id,
            operation=current.operation if current is not None else "release",
            outcome=(
                "released"
                if released
                else "release_token_mismatch"
                if current is not None
                else "release_noop"
            ),
            stale_recovered=False,
        )
        return released

    def inspect(self, *, board_id: str) -> LockManifest | None:
        port = self._port()
        if not hasattr(port, "inspect_single_writer_sync"):
            return None
        result = port.inspect_single_writer_sync(
            board_id=board_id,
            artifact_id=self.artifact_id,
            base_dir_hint=self._base_dir_hint,
            board_dir_resolver=self._board_dir_resolver,
        )
        return self._coerce_manifest(result)

    def is_owner(self, board_id: str, owner_token: str) -> bool:
        manifest = self.inspect(board_id=board_id)
        return manifest is not None and manifest.owner_token == owner_token

    def _port(self) -> WriteLockPort:
        return self._write_lock_port or get_write_lock_port()

    @staticmethod
    def _coerce_acquisition(result: Any, *, admin_lane: bool) -> LockAcquisition:
        if isinstance(result, LockAcquisition):
            return result
        if isinstance(result, dict):
            return LockAcquisition(
                acquired=bool(result.get("acquired")),
                owner_token=result.get("owner_token"),
                expires_at=result.get("expires_at"),
                current_owner=result.get("current_owner"),
                admin_lane=bool(result.get("admin_lane", admin_lane)),
                stale_recovered=bool(result.get("stale_recovered", False)),
            )
        if isinstance(result, WriteLockHandle):
            return LockAcquisition(
                acquired=True,
                owner_token=result.owner_token,
                expires_at=None,
                current_owner=None,
                admin_lane=admin_lane,
            )
        raise TypeError(f"unsupported write-lock acquisition result: {type(result)!r}")

    @staticmethod
    def _coerce_manifest(result: Any) -> LockManifest | None:
        if result is None:
            return None
        if isinstance(result, LockManifest):
            return result
        if isinstance(result, dict):
            return LockManifest.from_disk_dict(result)
        raise TypeError(f"unsupported write-lock manifest result: {type(result)!r}")


__all__ = [
    "DEFAULT_TTL_SECONDS",
    "KGSingleWriterLock",
    "LOCK_FILENAME",
    "LockAcquisition",
    "LockManifest",
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
