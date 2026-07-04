from __future__ import annotations

import asyncio
import json
import shutil
import time
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from okto_pulse.core.ports.coordination import LeaseHandle, WriteLockHandle
from okto_pulse.core.kg.providers.testing.memory_rebuild_audit_storage import (
    InMemoryRebuildAuditArtifactStore,
)


class FakeLeaseProvider:
    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._handles: dict[str, LeaseHandle] = {}
        self.released: list[LeaseHandle] = []

    def _lock_for(self, resource: str) -> asyncio.Lock:
        lock = self._locks.get(resource)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[resource] = lock
        return lock

    async def try_acquire(
        self,
        resource: str,
        *,
        ttl_seconds: int | None = None,
        owner_token: str | None = None,
    ) -> LeaseHandle | None:
        lock = self._lock_for(resource)
        if lock.locked():
            return None
        await lock.acquire()
        handle = LeaseHandle(
            resource=resource,
            owner_token=owner_token or uuid.uuid4().hex,
            fencing_token=uuid.uuid4().hex,
            expires_at=(
                datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
                if ttl_seconds
                else None
            ),
        )
        self._handles[resource] = handle
        return handle

    async def release(self, handle: LeaseHandle) -> None:
        if self._handles.get(handle.resource) != handle:
            return
        self.released.append(handle)
        self._handles.pop(handle.resource, None)
        lock = self._lock_for(handle.resource)
        if lock.locked():
            lock.release()

    def is_held(self, resource: str) -> bool:
        return self._lock_for(resource).locked()


class FakeWriteLockPort:
    def __init__(self) -> None:
        self._async_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._sync_locks: dict[tuple[str, str], threading.Lock] = {}
        self._async_handles: dict[tuple[str, str], WriteLockHandle] = {}
        self._sync_handles: dict[tuple[str, str], WriteLockHandle] = {}
        self._single_writer_locks: dict[tuple[str, str, str], dict[str, object]] = {}
        self.acquired_async: list[tuple[str, str]] = []
        self.released_async: list[tuple[str, str]] = []

    @staticmethod
    def _key(board_id: str, artifact_id: str) -> tuple[str, str]:
        return (board_id, artifact_id)

    def _async_lock_for(self, board_id: str, artifact_id: str) -> asyncio.Lock:
        key = self._key(board_id, artifact_id)
        lock = self._async_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._async_locks[key] = lock
        return lock

    def _sync_lock_for(self, board_id: str, artifact_id: str) -> threading.Lock:
        key = self._key(board_id, artifact_id)
        lock = self._sync_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            self._sync_locks[key] = lock
        return lock

    async def acquire(
        self,
        board_id: str,
        artifact_id: str,
        *,
        owner_token: str | None = None,
    ) -> WriteLockHandle:
        await self._async_lock_for(board_id, artifact_id).acquire()
        self.acquired_async.append((board_id, artifact_id))
        handle = WriteLockHandle(
            board_id=board_id,
            artifact_id=artifact_id,
            owner_token=owner_token or uuid.uuid4().hex,
            fencing_token=uuid.uuid4().hex,
        )
        self._async_handles[self._key(board_id, artifact_id)] = handle
        return handle

    async def release(self, handle: WriteLockHandle) -> None:
        key = self._key(handle.board_id, handle.artifact_id)
        if self._async_handles.get(key) != handle:
            return
        self._async_handles.pop(key, None)
        lock = self._async_lock_for(handle.board_id, handle.artifact_id)
        if lock.locked():
            lock.release()
        self.released_async.append((handle.board_id, handle.artifact_id))

    def acquire_sync(
        self,
        board_id: str,
        artifact_id: str,
        *,
        owner_token: str | None = None,
    ) -> WriteLockHandle:
        self._sync_lock_for(board_id, artifact_id).acquire()
        handle = WriteLockHandle(
            board_id=board_id,
            artifact_id=artifact_id,
            owner_token=owner_token or uuid.uuid4().hex,
            fencing_token=uuid.uuid4().hex,
        )
        self._sync_handles[self._key(board_id, artifact_id)] = handle
        return handle

    def release_sync(self, handle: WriteLockHandle) -> None:
        key = self._key(handle.board_id, handle.artifact_id)
        if self._sync_handles.get(key) != handle:
            return
        self._sync_handles.pop(key, None)
        lock = self._sync_lock_for(handle.board_id, handle.artifact_id)
        if lock.locked():
            lock.release()

    def is_locked(self, board_id: str, artifact_id: str) -> bool:
        return (
            self._async_lock_for(board_id, artifact_id).locked()
            or self._sync_lock_for(board_id, artifact_id).locked()
        )

    @staticmethod
    def _single_writer_scope(base_dir_hint: str | None) -> str:
        return base_dir_hint or "default"

    def _single_writer_key(
        self, board_id: str, artifact_id: str, base_dir_hint: str | None
    ) -> tuple[str, str, str]:
        return (self._single_writer_scope(base_dir_hint), board_id, artifact_id)

    @staticmethod
    def _single_writer_iso(epoch: float) -> str:
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()

    def acquire_single_writer_sync(
        self,
        *,
        board_id: str,
        artifact_id: str,
        operation: str,
        owner_id: str,
        ttl_seconds: int,
        admin_lane: bool = False,
        base_dir_hint: str | None = None,
        board_dir_resolver=None,
    ) -> dict[str, object]:
        del board_dir_resolver
        key = self._single_writer_key(board_id, artifact_id, base_dir_hint)
        now = time.time()
        current = self._single_writer_locks.get(key)
        stale_recovered = False
        if current is not None and float(current["expires_at_epoch"]) > now:
            return {
                "acquired": False,
                "owner_token": None,
                "expires_at": current["expires_at"],
                "current_owner": current["owner_id"],
                "admin_lane": current["admin_lane"],
                "stale_recovered": False,
            }
        if current is not None:
            stale_recovered = True

        owner_token = uuid.uuid4().hex
        expires_at_epoch = now + ttl_seconds
        manifest = {
            "owner_token": owner_token,
            "owner_id": owner_id,
            "operation": operation,
            "acquired_at_epoch": now,
            "expires_at_epoch": expires_at_epoch,
            "acquired_at": self._single_writer_iso(now),
            "expires_at": self._single_writer_iso(expires_at_epoch),
            "admin_lane": admin_lane,
        }
        self._single_writer_locks[key] = manifest
        return {
            "acquired": True,
            "owner_token": owner_token,
            "expires_at": manifest["expires_at"],
            "current_owner": owner_id,
            "admin_lane": admin_lane,
            "stale_recovered": stale_recovered,
        }

    def release_single_writer_sync(
        self,
        *,
        board_id: str,
        artifact_id: str,
        owner_token: str,
        base_dir_hint: str | None = None,
        board_dir_resolver=None,
    ) -> bool:
        del board_dir_resolver
        key = self._single_writer_key(board_id, artifact_id, base_dir_hint)
        current = self._single_writer_locks.get(key)
        if current is None or current["owner_token"] != owner_token:
            return False
        del self._single_writer_locks[key]
        return True

    def inspect_single_writer_sync(
        self,
        *,
        board_id: str,
        artifact_id: str,
        base_dir_hint: str | None = None,
        board_dir_resolver=None,
    ) -> dict[str, object] | None:
        del board_dir_resolver
        current = self._single_writer_locks.get(
            self._single_writer_key(board_id, artifact_id, base_dir_hint)
        )
        return dict(current) if current is not None else None

    def reset_for_tests(self) -> None:
        self._async_locks.clear()
        self._sync_locks.clear()
        self._async_handles.clear()
        self._sync_handles.clear()
        self._single_writer_locks.clear()
        self.acquired_async.clear()
        self.released_async.clear()


class FakeRuntimeSettingsProvider:
    def __init__(self, payload: dict[str, int] | None = None) -> None:
        self.payload = dict(payload or {})
        self.read_scopes: list[str] = []
        self.validated_values: list[dict[str, int]] = []

    async def read_runtime_settings(self, scope: str = "global") -> dict[str, int]:
        self.read_scopes.append(scope)
        return dict(self.payload)

    def validate_runtime_settings(self, values: dict[str, int]) -> None:
        self.validated_values.append(dict(values))


class FakeRebuildAuditArtifactStore(InMemoryRebuildAuditArtifactStore):
    def __init__(self, base_dir: Path) -> None:
        super().__init__()
        self._base_dir = Path(base_dir)

    def quarantine_paths(
        self,
        *,
        board_id: str,
        graph_type: str,
        affected_paths,
        reason: str,
        reason_bucket: str,
        correlation_ids,
        kg_generation_id: str | None,
        retention_days: int,
        scope_roots,
        base_dir_hint: str | None = None,
    ) -> dict:
        roots = [Path(root).resolve() for root in scope_roots]
        resolved = [Path(path).resolve() for path in affected_paths]
        for path in resolved:
            if not any(_is_relative_to(path, root) for root in roots):
                from okto_pulse.core.kg.quarantine import (
                    QuarantineError,
                    QuarantineErrorCode,
                )

                raise QuarantineError(
                    QuarantineErrorCode.AFFECTED_PATH_OUT_OF_SCOPE,
                    retryable=False,
                    reason=f"path {path} is not under any KG storage root",
                )

        qid = f"q_{uuid.uuid4().hex}"
        quarantine_dir = self._quarantine_base(base_dir_hint) / "quarantine" / qid
        quarantine_dir.mkdir(parents=True, exist_ok=False)
        moved: list[str] = []
        files_moved = 0
        for source in resolved:
            moved.append(source.name)
            if source.exists():
                shutil.move(str(source), str(quarantine_dir / source.name))
                files_moved += 1

        now = datetime.now(timezone.utc)
        manifest = {
            "quarantine_id": qid,
            "board_id": board_id,
            "graph_type": graph_type,
            "reason": reason,
            "reason_bucket": reason_bucket,
            "correlation_ids": list(correlation_ids),
            "affected_paths_relative": moved,
            "kg_generation_id": kg_generation_id,
            "software_version": "test",
            "quarantined_at": now.isoformat(),
            "retention_until": (
                now + timedelta(days=retention_days)
            ).isoformat(),
            "files_moved": files_moved,
        }
        manifest_path = quarantine_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return {**manifest, "manifest_ref": str(manifest_path)}

    def list_quarantine_manifests(
        self,
        *,
        active_after_iso: str | None = None,
        base_dir_hint: str | None = None,
    ):
        root = self._quarantine_base(base_dir_hint) / "quarantine"
        if not root.exists():
            return []
        rows = []
        for path in sorted(root.glob("*/manifest.json")):
            rows.append(json.loads(path.read_text(encoding="utf-8")))
        return rows

    def read_quarantine_manifest(
        self,
        *,
        quarantine_id: str,
        base_dir_hint: str | None = None,
    ):
        path = (
            self._quarantine_base(base_dir_hint)
            / "quarantine"
            / quarantine_id
            / "manifest.json"
        )
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def _quarantine_base(self, base_dir_hint: str | None) -> Path:
        return Path(base_dir_hint) if base_dir_hint else self._base_dir


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False
