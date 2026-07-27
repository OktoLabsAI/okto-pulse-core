"""Per-board serialization and bounded retry policy for graph commits."""

from __future__ import annotations

import asyncio
import logging
import random
import threading
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, TypeVar

from okto_pulse.core.kg.interfaces.graph_errors import GraphLockContention
from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)

logger = logging.getLogger("okto_pulse.kg.commit_coordinator")

RETRY_BACKOFFS_MS: tuple[int, ...] = (100, 200, 400)
JITTER_MAX_MS = 50
LOCK_RETRY_WINDOW_SECONDS = 300
_RUNTIME_KEY = "kg.commit_coordinator"

T = TypeVar("T")


class CommitCoordinator:
    """Instance-owned locks, retry telemetry and graph commit policy."""

    def __init__(self) -> None:
        self._commit_locks: defaultdict[str, asyncio.Lock] = defaultdict(
            asyncio.Lock
        )
        self._retry_timestamps: deque[datetime] = deque()
        self._retry_lock = threading.Lock()

    def acquire(self, board_id: str) -> asyncio.Lock:
        return self._commit_locks[board_id]

    def record_retry(self, now: datetime | None = None) -> None:
        timestamp = now or datetime.now(timezone.utc)
        cutoff = timestamp - timedelta(seconds=LOCK_RETRY_WINDOW_SECONDS)
        with self._retry_lock:
            while self._retry_timestamps and self._retry_timestamps[0] < cutoff:
                self._retry_timestamps.popleft()
            self._retry_timestamps.append(timestamp)

    def retries_in_window(self, now: datetime | None = None) -> int:
        timestamp = now or datetime.now(timezone.utc)
        cutoff = timestamp - timedelta(seconds=LOCK_RETRY_WINDOW_SECONDS)
        with self._retry_lock:
            while self._retry_timestamps and self._retry_timestamps[0] < cutoff:
                self._retry_timestamps.popleft()
            return len(self._retry_timestamps)

    async def run(
        self,
        board_id: str,
        coro_factory: Callable[[], Awaitable[T]],
    ) -> T:
        total_attempts = len(RETRY_BACKOFFS_MS) + 1
        async with self.acquire(board_id):
            for attempt in range(1, total_attempts + 1):
                try:
                    return await coro_factory()
                except GraphLockContention:
                    if attempt >= total_attempts:
                        logger.error(
                            "kg.commit.lock_exhausted board=%s attempts=%d",
                            board_id,
                            total_attempts,
                            extra={
                                "event": "kg.commit.lock_exhausted",
                                "board_id": board_id,
                                "attempts": total_attempts,
                            },
                        )
                        raise
                    backoff_ms = (
                        RETRY_BACKOFFS_MS[attempt - 1]
                        + random.uniform(0, JITTER_MAX_MS)
                    )
                    self.record_retry()
                    logger.warning(
                        "kg.commit.lock_retry board=%s attempt=%d backoff_ms=%.1f",
                        board_id,
                        attempt,
                        backoff_ms,
                        extra={
                            "event": "kg.commit.lock_retry",
                            "board_id": board_id,
                            "attempt": attempt,
                            "backoff_ms": backoff_ms,
                        },
                    )
                    await asyncio.sleep(backoff_ms / 1000.0)
        raise RuntimeError("unreachable")  # pragma: no cover

    def reset(self) -> None:
        self._commit_locks.clear()
        with self._retry_lock:
            self._retry_timestamps.clear()


def register_commit_coordinator(coordinator: CommitCoordinator) -> None:
    register_runtime_value(_RUNTIME_KEY, coordinator)


def require_commit_coordinator() -> CommitCoordinator:
    return require_runtime_value(
        _RUNTIME_KEY,
        "graph_commit_coordinator_not_configured",
    )


def acquire_commit_lock(board_id: str) -> asyncio.Lock:
    return require_commit_coordinator().acquire(board_id)


def record_graph_lock_retry(now: datetime | None = None) -> None:
    require_commit_coordinator().record_retry(now)


def graph_lock_retries_5m(now: datetime | None = None) -> int:
    return require_commit_coordinator().retries_in_window(now)


async def run_with_commit_lock_and_retry(
    board_id: str,
    coro_factory: Callable[[], Awaitable[T]],
) -> T:
    return await require_commit_coordinator().run(board_id, coro_factory)


def reset_commit_coordinator_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)
    register_commit_coordinator(CommitCoordinator())


__all__ = [
    "CommitCoordinator",
    "JITTER_MAX_MS",
    "RETRY_BACKOFFS_MS",
    "acquire_commit_lock",
    "graph_lock_retries_5m",
    "record_graph_lock_retry",
    "register_commit_coordinator",
    "require_commit_coordinator",
    "reset_commit_coordinator_for_tests",
    "run_with_commit_lock_and_retry",
]
