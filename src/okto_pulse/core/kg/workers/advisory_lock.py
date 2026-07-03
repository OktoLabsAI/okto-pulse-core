"""Write-lock facade for KG workers.

The concrete local-first lock implementation lives in an edition adapter. This
module preserves the historical import surface (`get_async_lock`,
`advisory_lock`, etc.) while delegating every operation through
``WriteLockPort``.
"""

from __future__ import annotations

import logging
import weakref
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator

from okto_pulse.core.ports.coordination import (
    WriteLockHandle,
    get_write_lock_port,
)

logger = logging.getLogger("okto_pulse.kg.advisory_lock")


class _AsyncWriteLockProxy:
    def __init__(self, board_id: str, artifact_id: str) -> None:
        self.board_id = board_id
        self.artifact_id = artifact_id
        self._handle: WriteLockHandle | None = None

    def locked(self) -> bool:
        return get_write_lock_port().is_locked(self.board_id, self.artifact_id)

    async def acquire(self) -> bool:
        self._handle = await get_write_lock_port().acquire(
            self.board_id,
            self.artifact_id,
        )
        return True

    def release(self) -> None:
        if self._handle is None:
            raise RuntimeError("lock is not held by this proxy")
        handle = self._handle
        self._handle = None
        port = get_write_lock_port()
        release = port.release(handle)
        try:
            release.send(None)
        except StopIteration:
            return
        raise RuntimeError("async lock release must be awaited via advisory_lock")

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._handle is None:
            return
        handle = self._handle
        self._handle = None
        await get_write_lock_port().release(handle)


class _SyncWriteLockProxy:
    def __init__(self, board_id: str, artifact_id: str) -> None:
        self.board_id = board_id
        self.artifact_id = artifact_id
        self._handle: WriteLockHandle | None = None

    def locked(self) -> bool:
        return get_write_lock_port().is_locked(self.board_id, self.artifact_id)

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        if not blocking or timeout not in (-1, None):
            raise NotImplementedError(
                "non-blocking/timeout sync lock acquisition is adapter-specific"
            )
        self._handle = get_write_lock_port().acquire_sync(
            self.board_id,
            self.artifact_id,
        )
        return True

    def release(self) -> None:
        if self._handle is None:
            raise RuntimeError("lock is not held by this proxy")
        handle = self._handle
        self._handle = None
        get_write_lock_port().release_sync(handle)

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


_ASYNC_PROXIES: "weakref.WeakValueDictionary[tuple[str, str], _AsyncWriteLockProxy]" = (
    weakref.WeakValueDictionary()
)
_SYNC_PROXIES: "weakref.WeakValueDictionary[tuple[str, str], _SyncWriteLockProxy]" = (
    weakref.WeakValueDictionary()
)


def _key(board_id: str, artifact_id: str) -> tuple[str, str]:
    return (board_id, artifact_id)


def get_async_lock(board_id: str, artifact_id: str) -> _AsyncWriteLockProxy:
    """Return a stable proxy for the board/artifact write lock."""

    key = _key(board_id, artifact_id)
    lock = _ASYNC_PROXIES.get(key)
    if lock is None:
        lock = _AsyncWriteLockProxy(board_id, artifact_id)
        _ASYNC_PROXIES[key] = lock
    return lock


def get_sync_lock(board_id: str, artifact_id: str) -> _SyncWriteLockProxy:
    """Return a stable proxy for the board/artifact sync write lock."""

    key = _key(board_id, artifact_id)
    lock = _SYNC_PROXIES.get(key)
    if lock is None:
        lock = _SyncWriteLockProxy(board_id, artifact_id)
        _SYNC_PROXIES[key] = lock
    return lock


@asynccontextmanager
async def advisory_lock(
    board_id: str, artifact_id: str,
) -> AsyncIterator[None]:
    """Async context manager for a board/artifact write lock."""

    lock = get_async_lock(board_id, artifact_id)
    async with lock:
        logger.info(
            "advisory_lock.acquired board=%s artifact=%s",
            board_id,
            artifact_id,
            extra={
                "event": "advisory_lock.acquired",
                "board_id": board_id,
                "artifact_id": artifact_id,
            },
        )
        try:
            yield
        finally:
            logger.info(
                "advisory_lock.released board=%s artifact=%s",
                board_id,
                artifact_id,
                extra={
                    "event": "advisory_lock.released",
                    "board_id": board_id,
                    "artifact_id": artifact_id,
                },
            )


@contextmanager
def advisory_lock_sync(
    board_id: str, artifact_id: str,
) -> Iterator[None]:
    """Sync counterpart for worker calls that are not in an event loop."""

    lock = get_sync_lock(board_id, artifact_id)
    with lock:
        logger.info(
            "advisory_lock.acquired_sync board=%s artifact=%s",
            board_id,
            artifact_id,
            extra={
                "event": "advisory_lock.acquired_sync",
                "board_id": board_id,
                "artifact_id": artifact_id,
            },
        )
        yield


def reset_locks_for_tests() -> None:
    """Reset proxy caches and delegate adapter state reset when available."""

    _ASYNC_PROXIES.clear()
    _SYNC_PROXIES.clear()
    try:
        get_write_lock_port().reset_for_tests()
    except Exception:
        pass


__all__ = [
    "advisory_lock",
    "advisory_lock_sync",
    "get_async_lock",
    "get_sync_lock",
    "reset_locks_for_tests",
]
