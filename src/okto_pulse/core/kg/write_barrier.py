"""Write-time barrier that enforces "no LadybugDB mutation without lifecycle".

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
import os
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator

from okto_pulse.core.runtime_context import register_runtime_value, resolve_runtime_value

logger = logging.getLogger("okto_pulse.kg.write_barrier")


# Env knob to switch the default mode in production. Tests use
# `set_barrier_mode("strict")` directly.
_BARRIER_MODE_ENV_VAR = "OKTO_PULSE_KG_WRITE_BARRIER_MODE"


class BarrierMode:
    SOFT = "soft"
    STRICT = "strict"


# Pseudo-board sentinel used by writes that target the global discovery
# graph (`discovery.lbug` at `~/.okto-pulse/global/`). Per-board writers
# pass their real board_id; the global graph has no real board, so we
# canonicalise this sentinel so that the barrier surface stays uniform
# and the counter labels remain explicit. KG-01.3.1 rework.
GLOBAL_DISCOVERY_BOARD_SENTINEL = "_global_discovery"


_DEFAULT_MODE = (
    os.environ.get(_BARRIER_MODE_ENV_VAR, BarrierMode.SOFT).strip().lower()
    or BarrierMode.SOFT
)
_DEFAULT_RESOLVED_MODE = (
    BarrierMode.STRICT
    if _DEFAULT_MODE == BarrierMode.STRICT
    else BarrierMode.SOFT
)
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
_unguarded_counter: dict[tuple[str, str], int] = {}
_unguarded_counter_lock = threading.Lock()


def _bump_unguarded(board_id: str, mode: str) -> None:
    with _unguarded_counter_lock:
        key = (board_id, mode)
        _unguarded_counter[key] = _unguarded_counter.get(key, 0) + 1


def get_unguarded_count(board_id: str, *, mode: str | None = None) -> int:
    with _unguarded_counter_lock:
        total = 0
        for (b, m), value in _unguarded_counter.items():
            if b != board_id:
                continue
            if mode is not None and m != mode:
                continue
            total += value
        return total


def get_unguarded_samples() -> list[dict[str, Any]]:
    with _unguarded_counter_lock:
        return [
            {"board_id": b, "mode": m, "count": v}
            for (b, m), v in _unguarded_counter.items()
        ]


def get_unguarded_counter_labels() -> tuple[str, ...]:
    return _UNGUARDED_LABELS


def reset_unguarded_counter() -> None:
    with _unguarded_counter_lock:
        _unguarded_counter.clear()


# --- Mode control --------------------------------------------------------------


def set_barrier_mode(mode: str) -> None:
    """Switch the global barrier mode. Tests call this with 'strict'."""
    if mode not in (BarrierMode.SOFT, BarrierMode.STRICT):
        raise ValueError(f"unknown barrier mode: {mode}")
    register_runtime_value(_MODE_KEY, mode)


def get_barrier_mode() -> str:
    return resolve_runtime_value(_MODE_KEY) or _DEFAULT_RESOLVED_MODE


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
# operate on `discovery.lbug` with no per-board scope. They piggyback on
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
    "WriteLifecycleViolation",
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
