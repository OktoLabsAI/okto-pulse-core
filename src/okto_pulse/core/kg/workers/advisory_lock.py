"""In-process advisory lock keyed by ``(board_id, artifact_id)``.

Kùzu does not expose native advisory locks (unlike PostgreSQL). For the
Layer 1 deterministic worker + cognitive session isolation invariant
(spec c48a5c33 TR ``tr_f2bca830``) we need a single-process mutex so that
two concurrent consolidations on the same artifact can't both write. This
module is the cheapest implementation that still honours the contract:

- Lock is re-entrant-free: the same ``asyncio`` task trying to acquire the
  same key twice will deadlock (by design; surface the bug rather than hide
  it).
- Locks live in a ``WeakValueDictionary`` so the GC can collect them after
  the last ``release()``. Memory usage stays bounded regardless of artifact
  cardinality.
- Works across async callers (cognitive session) and sync callers (Layer 1
  worker called from a thread) via ``AdvisoryLock.acquire_sync``.

For multi-process deployments a real fcntl/advisory lock on a per-artifact
sentinel file would be the next step — out of scope here since the MVP is
single-process.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import weakref
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator

logger = logging.getLogger("okto_pulse.kg.advisory_lock")


_ASYNC_LOCKS: "weakref.WeakValueDictionary[tuple[str, str], asyncio.Lock]" = (
    weakref.WeakValueDictionary()
)
_SYNC_LOCKS: "weakref.WeakValueDictionary[tuple[str, str], threading.Lock]" = (
    weakref.WeakValueDictionary()
)
_REGISTRY_LOCK = threading.Lock()


def _key(board_id: str, artifact_id: str) -> tuple[str, str]:
    return (board_id, artifact_id)


def get_async_lock(board_id: str, artifact_id: str) -> asyncio.Lock:
    """Return the shared asyncio.Lock for this (board, artifact).

    Two callers asking for the same pair get the same instance; distinct
    pairs get distinct locks (fine-grained, avoids head-of-line blocking).
    """
    key = _key(board_id, artifact_id)
    with _REGISTRY_LOCK:
        lock = _ASYNC_LOCKS.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _ASYNC_LOCKS[key] = lock
        return lock


def get_sync_lock(board_id: str, artifact_id: str) -> threading.Lock:
    """Return the shared threading.Lock for this (board, artifact)."""
    key = _key(board_id, artifact_id)
    with _REGISTRY_LOCK:
        lock = _SYNC_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _SYNC_LOCKS[key] = lock
        return lock


@asynccontextmanager
async def advisory_lock(
    board_id: str, artifact_id: str,
) -> AsyncIterator[None]:
    """Async context manager — acquires the per-artifact lock for the block.

    Usage:
        async with advisory_lock(board_id, artifact_id):
            # safe to write to Kùzu for this artifact
            ...
    """
    lock = get_async_lock(board_id, artifact_id)
    acquired_at = asyncio.get_event_loop().time()
    await lock.acquire()
    try:
        held_for = asyncio.get_event_loop().time() - acquired_at
        logger.info(
            "advisory_lock.acquired board=%s artifact=%s wait_s=%.4f",
            board_id, artifact_id, held_for,
            extra={"event": "advisory_lock.acquired",
                   "board_id": board_id, "artifact_id": artifact_id,
                   "wait_s": held_for},
        )
        yield
    finally:
        lock.release()
        logger.info(
            "advisory_lock.released board=%s artifact=%s",
            board_id, artifact_id,
            extra={"event": "advisory_lock.released",
                   "board_id": board_id, "artifact_id": artifact_id},
        )


@contextmanager
def advisory_lock_sync(
    board_id: str, artifact_id: str,
) -> Iterator[None]:
    """Sync counterpart — for Layer 1 worker calls that aren't in an event loop."""
    lock = get_sync_lock(board_id, artifact_id)
    lock.acquire()
    try:
        logger.info(
            "advisory_lock.acquired_sync board=%s artifact=%s",
            board_id, artifact_id,
            extra={"event": "advisory_lock.acquired_sync",
                   "board_id": board_id, "artifact_id": artifact_id},
        )
        yield
    finally:
        lock.release()


def reset_locks_for_tests() -> None:
    """Drop every registered lock — test hook only. Never call in prod."""
    with _REGISTRY_LOCK:
        _ASYNC_LOCKS.clear()
        _SYNC_LOCKS.clear()
