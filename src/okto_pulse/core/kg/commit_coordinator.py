"""Per-board serialisation + retry for Kùzu commit operations.

Kùzu holds an exclusive-writer file lock on each per-board ``.kuzu`` directory.
When two ``kg_commit_consolidation`` calls target the same board concurrently,
the second one races the first at the OS file-lock level and crashes with
``RuntimeError: IO exception: Could not set lock on file``. Field reports
confirmed 6/37 parallel commits failing this way during a single delivery
session — all recoverable via manual retry, none signalling real corruption.

This module adds two defences the tool handler wraps around
``primitives.commit_consolidation``:

1. **Per-board asyncio.Lock** — a module-level ``defaultdict`` returns the
   same lock instance for the same ``board_id``. Intra-process commits on
   the same board serialise automatically; commits on distinct boards keep
   running in parallel.

2. **Bounded retry with exponential backoff + jitter** — inside the critical
   section, a Kùzu lock error caused by *another process* (CLI, second MCP
   server, IDE) is retried up to 3 times with 100 / 200 / 400 ms bases plus
   0–50 ms jitter. Other exceptions propagate immediately so real bugs are
   not masked.

The wrapper is transparent to agents: they can parallelise commit calls as
freely as every other MCP tool, and the handler takes care of the rest.
Release tracking: spec 194583e5 (released within 0.1.4, no version bump).
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections import defaultdict
from typing import Awaitable, Callable, TypeVar

logger = logging.getLogger("okto_pulse.kg.commit_coordinator")

_KUZU_LOCK_ERROR_SUBSTRING = "Could not set lock on file"

# Retry policy — exposed as module constants so tests can patch them without
# monkey-patching the function body. Changing these numbers counts as a
# behavioural change and must update the spec ACs.
RETRY_BACKOFFS_MS: tuple[int, ...] = (100, 200, 400)
JITTER_MAX_MS: int = 50

T = TypeVar("T")

# Module-level registry of per-board locks. The defaultdict gives us
# lazy instantiation (first touch creates the Lock) and stable identity
# (subsequent touches return the exact same Lock object).
#
# Unbounded growth is acceptable here — the key space is ``board_id``
# strings, and boards are long-lived. Even with thousands of boards the
# memory overhead is negligible (few hundred bytes per Lock).
_commit_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)


def acquire_commit_lock(board_id: str) -> asyncio.Lock:
    """Return the singleton :class:`asyncio.Lock` for a given board.

    Calls with the same ``board_id`` always return the same object (identity
    check via ``is`` is safe in tests). Calls with distinct ``board_id``
    strings return distinct locks, preserving cross-board parallelism.
    """
    return _commit_locks[board_id]


def _is_kuzu_lock_error(exc: BaseException) -> bool:
    """Classify an exception as Kùzu's transient file-lock error.

    Kùzu's Python binding does not expose typed error codes, so we match on
    the exception message substring. This is documented in spec TR3 as a
    known fragility to revisit when Kùzu ships structured errors.
    """
    return isinstance(exc, RuntimeError) and _KUZU_LOCK_ERROR_SUBSTRING in str(exc)


async def run_with_commit_lock_and_retry(
    board_id: str,
    coro_factory: Callable[[], Awaitable[T]],
) -> T:
    """Serialise and retry a commit-style coroutine for one board.

    Parameters
    ----------
    board_id:
        Key used to select the per-board lock. Two calls with the same
        ``board_id`` serialise; calls with different ``board_id`` values run
        concurrently.
    coro_factory:
        Zero-argument callable that returns a fresh coroutine each time it
        is invoked. A factory (instead of a coroutine object) is required
        because every retry needs a fresh awaitable — coroutines can only be
        awaited once.

    Returns the coroutine's value on success.

    Raises the last exception if every attempt hits a Kùzu lock error, or
    propagates immediately when the factory raises any non-lock exception.
    """
    lock = acquire_commit_lock(board_id)
    total_attempts = len(RETRY_BACKOFFS_MS) + 1

    async with lock:
        for attempt in range(1, total_attempts + 1):
            try:
                return await coro_factory()
            except BaseException as exc:  # noqa: BLE001 — we re-raise after classification
                if not _is_kuzu_lock_error(exc):
                    # Real bug — propagate immediately, no retry.
                    raise
                if attempt >= total_attempts:
                    logger.error(
                        "kg.commit.lock_exhausted board=%s attempts=%d",
                        board_id, total_attempts,
                        extra={
                            "event": "kg.commit.lock_exhausted",
                            "board_id": board_id,
                            "attempts": total_attempts,
                        },
                    )
                    raise
                base_ms = RETRY_BACKOFFS_MS[attempt - 1]
                jitter_ms = random.uniform(0, JITTER_MAX_MS)
                backoff_ms = base_ms + jitter_ms
                logger.warning(
                    "kg.commit.lock_retry board=%s attempt=%d backoff_ms=%.1f",
                    board_id, attempt, backoff_ms,
                    extra={
                        "event": "kg.commit.lock_retry",
                        "board_id": board_id,
                        "attempt": attempt,
                        "backoff_ms": backoff_ms,
                    },
                )
                await asyncio.sleep(backoff_ms / 1000.0)
        # Unreachable — either a return happened in the loop or the final
        # iteration raised. This pragma is here purely so static type
        # checkers see a terminator for every branch.
        raise RuntimeError("unreachable")  # pragma: no cover


def reset_commit_locks_for_tests() -> None:
    """Drop the module-level lock registry — only for tests that need a
    fresh defaultdict between cases. Production code never calls this."""
    _commit_locks.clear()
