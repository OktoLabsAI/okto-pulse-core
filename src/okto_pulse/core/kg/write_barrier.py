"""Write-time barrier that enforces "no embedded graph backend mutation without lifecycle".

KG-01 FR5/FR6 require every write path (commit_consolidation,
kg_tick force_full_rebuild, discovery writes, KG-02 rebuild) to go
through KGSingleWriterLock + KGSafeWriteLifecycle. The original
rejection on val_75dee856 noted that having primitives without
enforcement leaves a gap: a future developer can call the storage
driver directly and silently bypass the safety net.

This module provides a cheap, deterministic barrier:

    @under_safe_write(board_id, owner_token, operation)  # context manager
    def apply():
        # writes inside here are "guarded"
        ...

    def commit_consolidation(...):
        require_write_token(board_id)  # raises if no guard is active
        ...

Implementation: a single ``ContextVar`` holds the per-async-task
"active write guards" stack. Workers that are inside a safe lifecycle
push a guard; mutation primitives check it. ``require_write_token``
operates in two modes:

* STRICT (default in tests, opt-in in production) — raises
  ``WriteLifecycleViolation`` if no active guard for the board.
* SOFT — logs ``kg.write_barrier.unguarded`` at WARNING and bumps the
  ``kg_unguarded_write_total`` counter, but allows the call. This is
  the conservative rollout path while individual call sites are still
  being wired.

The barrier ships with both modes because the spec explicitly accepts
"flagged" as equivalent to "blocked" when wiring is in-flight — see
val_75dee856 "Wirear esses call sites, ou introduzir uma barreira
enforcement equivalente que torne mutacao direta fora do lifecycle
impossivel/flagged".
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator

from okto_pulse.core.runtime_context import register_runtime_value, resolve_runtime_value

logger = logging.getLogger("okto_pulse.kg.write_barrier")


class BarrierMode:
    SOFT = "soft"
    STRICT = "strict"


# Pseudo-board sentinel used by writes that target the global discovery
# graph (`global graph` at `~/.okto-pulse/global/`). Per-board writers
# pass their real board_id; the global graph has no real board, so we
# canonicalise this sentinel so that the barrier surface stays uniform
# and the counter labels remain explicit. KG-01.3.1 rework.
GLOBAL_DISCOVERY_BOARD_SENTINEL = "_global_discovery"


_MODE_KEY = "kg.write_barrier.mode"


@dataclass(frozen=True, slots=True)
class WriteGuard:
    board_id: str
    owner_token: str
    operation: str


_active_guards: ContextVar[tuple[WriteGuard, ...]] = ContextVar(
    "okto_pulse_kg_active_write_guards", default=()
)


class WriteLifecycleViolation(Exception):
    """Raised in STRICT mode when a write occurs outside a guard.

    Carries the board_id so observability can join violation events
    back to the call site. NOT a contract error — purely an internal
    invariant check that should never trigger in correct code.
    """

    def __init__(self, board_id: str, *, reason: str) -> None:
        super().__init__(f"unguarded_write board={board_id} reason={reason}")
        self.board_id = board_id
        self.reason = reason


# --- Counter (paired with kg_unguarded_write_total observability slice) ------

_UNGUARDED_LABELS = ("board_id", "mode")
class WriteBarrierRuntime:
    """Instance-owned barrier policy and observability state."""

    def __init__(self, mode: str = BarrierMode.SOFT) -> None:
        self.set_mode(mode)
        self._counter: dict[tuple[str, str], int] = {}
        self._lock = threading.Lock()

    @property
    def mode(self) -> str:
        return self._mode

    def set_mode(self, mode: str) -> None:
        if mode not in (BarrierMode.SOFT, BarrierMode.STRICT):
            raise ValueError(f"unknown barrier mode: {mode}")
        self._mode = mode

    def bump(self, board_id: str, mode: str) -> None:
        with self._lock:
            key = (board_id, mode)
            self._counter[key] = self._counter.get(key, 0) + 1

    def count(self, board_id: str, mode: str | None = None) -> int:
        with self._lock:
            return sum(
                value
                for (candidate_board, candidate_mode), value in self._counter.items()
                if candidate_board == board_id
                and (mode is None or candidate_mode == mode)
            )

    def samples(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                {"board_id": board, "mode": mode, "count": value}
                for (board, mode), value in self._counter.items()
            ]

    def reset_counter(self) -> None:
        with self._lock:
            self._counter.clear()


def configure_write_barrier_runtime(runtime: WriteBarrierRuntime) -> None:
    register_runtime_value(_MODE_KEY, runtime)


def _runtime() -> WriteBarrierRuntime:
    runtime = resolve_runtime_value(_MODE_KEY)
    if runtime is None:
        runtime = WriteBarrierRuntime()
        configure_write_barrier_runtime(runtime)
    return runtime


def _bump_unguarded(board_id: str, mode: str) -> None:
    _runtime().bump(board_id, mode)


def get_unguarded_count(board_id: str, *, mode: str | None = None) -> int:
    return _runtime().count(board_id, mode)


def get_unguarded_samples() -> list[dict[str, Any]]:
    return _runtime().samples()


def get_unguarded_counter_labels() -> tuple[str, ...]:
    return _UNGUARDED_LABELS


def reset_unguarded_counter() -> None:
    _runtime().reset_counter()


# --- Mode control --------------------------------------------------------------


def set_barrier_mode(mode: str) -> None:
    """Switch the active composed barrier runtime mode."""
    _runtime().set_mode(mode)


def get_barrier_mode() -> str:
    return _runtime().mode


# --- Public API ----------------------------------------------------------------


@contextmanager
def under_safe_write(
    board_id: str, owner_token: str, operation: str
) -> Iterator[WriteGuard]:
    """Push a write guard for the duration of the context.

    Workers that have a valid lifecycle-bound write to perform call this
    BEFORE the storage mutation, so ``require_write_token`` downstream
    sees the active guard. The guard is per-async-task via ContextVar,
    so concurrent boards in the same process never collide.

    Usage::

        with under_safe_write(board_id, token, "consolidate"):
            await commit_consolidation(req, agent_id=agent_id, db=db)
    """
    if not owner_token:
        raise ValueError("owner_token is required to enter under_safe_write")
    guard = WriteGuard(board_id=board_id, owner_token=owner_token, operation=operation)
    current = _active_guards.get()
    token_handle = _active_guards.set(current + (guard,))
    try:
        yield guard
    finally:
        _active_guards.reset(token_handle)


def require_write_token(
    board_id: str, *, expected_owner_token: str | None = None
) -> WriteGuard | None:
    """Assert that a write guard is active for ``board_id``.

    Behaviour depends on the global barrier mode:

    * STRICT — raises ``WriteLifecycleViolation`` if no guard is active.
      Also raises if ``expected_owner_token`` is supplied and does not
      match the topmost guard's token (token forgery defence).
    * SOFT — logs ``kg.write_barrier.unguarded`` and bumps
      ``kg_unguarded_write_total{board_id, mode=soft}``, then returns
      ``None``.

    Returns the matching ``WriteGuard`` on success.
    """
    guards = _active_guards.get()
    for guard in reversed(guards):
        if guard.board_id != board_id:
            continue
        if expected_owner_token is not None and guard.owner_token != expected_owner_token:
            mode = get_barrier_mode()
            if mode == BarrierMode.STRICT:
                raise WriteLifecycleViolation(
                    board_id, reason="owner_token_mismatch"
                )
            logger.warning(
                "kg.write_barrier.token_mismatch board=%s expected_prefix=%s "
                "actual_prefix=%s",
                board_id,
                (expected_owner_token or "")[:8],
                guard.owner_token[:8],
            )
            _bump_unguarded(board_id, BarrierMode.SOFT)
            return None
        return guard

    mode = get_barrier_mode()
    if mode == BarrierMode.STRICT:
        raise WriteLifecycleViolation(board_id, reason="no_active_guard")
    logger.warning(
        "kg.write_barrier.unguarded board=%s mode=%s",
        board_id, mode,
        extra={"event": "kg.write_barrier.unguarded", "board_id": board_id},
    )
    _bump_unguarded(board_id, mode)
    return None


def has_active_guard(board_id: str) -> bool:
    """Cheap check used by storage adapters that prefer no exceptions."""
    for guard in _active_guards.get():
        if guard.board_id == board_id:
            return True
    return False


# --- Global discovery helpers (KG-01.3.1 rework: val_441ad311) ---------------
#
# Global discovery writers (bootstrap_global_discovery, gc_orphans, …)
# operate on `global graph` with no per-board scope. They piggyback on
# the same barrier infrastructure via the GLOBAL_DISCOVERY_BOARD_SENTINEL.


def under_global_safe_write(
    owner_token: str, operation: str
):
    """Push a guard for a global-discovery write.

    Thin wrapper around ``under_safe_write`` with the canonical sentinel
    pre-bound. Use this from KG-02 admin rebuild paths and from the
    bootstrap/gc paths that operate on the global graph.
    """
    return under_safe_write(GLOBAL_DISCOVERY_BOARD_SENTINEL, owner_token, operation)


def require_global_write_token(
    *, expected_owner_token: str | None = None
) -> WriteGuard | None:
    """Assert that a global-discovery write guard is active.

    Same semantics as ``require_write_token`` but scoped to the global
    discovery sentinel. STRICT mode raises ``WriteLifecycleViolation``;
    SOFT logs ``kg.write_barrier.unguarded`` and bumps
    ``kg_unguarded_write_total{board_id=_global_discovery, mode=soft}``.
    """
    return require_write_token(
        GLOBAL_DISCOVERY_BOARD_SENTINEL,
        expected_owner_token=expected_owner_token,
    )


def has_active_global_guard() -> bool:
    return has_active_guard(GLOBAL_DISCOVERY_BOARD_SENTINEL)


__all__ = [
    "BarrierMode",
    "GLOBAL_DISCOVERY_BOARD_SENTINEL",
    "WriteGuard",
    "WriteBarrierRuntime",
    "WriteLifecycleViolation",
    "configure_write_barrier_runtime",
    "get_barrier_mode",
    "get_unguarded_count",
    "get_unguarded_counter_labels",
    "get_unguarded_samples",
    "has_active_global_guard",
    "has_active_guard",
    "require_global_write_token",
    "require_write_token",
    "reset_unguarded_counter",
    "set_barrier_mode",
    "under_global_safe_write",
    "under_safe_write",
]
