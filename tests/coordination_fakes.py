from __future__ import annotations

import asyncio
import threading
import uuid
from datetime import datetime, timedelta, timezone

from okto_pulse.core.ports.coordination import LeaseHandle, WriteLockHandle


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

    def reset_for_tests(self) -> None:
        self._async_locks.clear()
        self._sync_locks.clear()
        self._async_handles.clear()
        self._sync_handles.clear()
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
