"""RefinementCognitiveCompletionGuard (KG-03.7 / api_0cd358dd).

Fail-closed guard invoked before a refinement transitions to ``done``.
Reads the cognitive item ledger for the refinement's source_ref and
returns whether the transition may proceed.

Resolution rules (br_f6fc7cbc + br_929c2164 + tr_e2b4e9de):

    - target_status != "done"            → invalid_target_status error.
    - cognitive item store unavailable   → fail-closed
                                           allowed=False,
                                           reason=cognitive_status_unavailable.
    - any active item (pending /
      in_progress / failed) for the
      refinement source_ref              → allowed=False,
                                           reason=cognitive_consolidation_pending,
                                           blocking_items=[…].
    - all items terminal (consolidated /
      skipped) OR no items at all        → allowed=True,
                                           reason=no_active_cognitive_items.

Counter ``kg_refinement_cognitive_done_guard_total`` (or_fb5bbbe7):

    Labels: ``board_id_hash``, ``outcome``, ``reason``.

    Forbidden labels: refinement_id, source_ref, item_id, raw status
    enum outside the bounded set.

Coupled invariant (KG-03.1 + tr_9521d8fd + ir_dbf036d8): rebuild /
``CognitivePendingMarker`` cannot write terminal statuses. The guard
relies on this invariant — the ledger contains only ``pending`` items
created by deterministic code; terminal transitions are exclusively
MCP-owned.
"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from okto_pulse.core.kg.rebuild_audit import (
    ACTIVE_ITEM_STATUSES,
    CognitiveConsolidationItem,
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
)
from okto_pulse.core.observability.sample_buffer import runtime_counter_sample_buffer
from okto_pulse.core.runtime_context import runtime_lock


# ---------------------------------------------------------------------------
# Bounded enums for response + counter
# ---------------------------------------------------------------------------


class RefinementGuardReason(str, Enum):
    """Bounded reasons per api_0cd358dd response_success.reason."""

    NO_ACTIVE_COGNITIVE_ITEMS = "no_active_cognitive_items"
    COGNITIVE_CONSOLIDATION_PENDING = "cognitive_consolidation_pending"
    COGNITIVE_STATUS_UNAVAILABLE = "cognitive_status_unavailable"


class RefinementGuardOutcome(str, Enum):
    """Bounded outcomes for the guard counter."""

    ALLOWED = "allowed"
    BLOCKED = "blocked"
    UNAVAILABLE = "unavailable"
    INVALID_TARGET_STATUS = "invalid_target_status"


_ACTIVE_STATUSES = ACTIVE_ITEM_STATUSES


@dataclass(frozen=True, slots=True)
class GuardBlockingItem:
    """Per-item blocking detail in the guard response (api_0cd358dd)."""

    item_id: str
    status: str  # one of pending | in_progress | failed

    def to_api(self) -> dict[str, Any]:
        return {"item_id": self.item_id, "status": self.status}


@dataclass(frozen=True, slots=True)
class RefinementGuardResult:
    """Frozen guard response shape — api_0cd358dd response_success."""

    allowed: bool
    reason: str
    blocking_items: tuple[GuardBlockingItem, ...] = field(default_factory=tuple)

    def to_api(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "reason": self.reason,
            "blocking_items": [item.to_api() for item in self.blocking_items],
        }


class RefinementGuardError(Exception):
    """Raised when the caller violates the contract pre-conditions
    (e.g. ``target_status`` is not ``done``)."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


# ---------------------------------------------------------------------------
# Counter — kg_refinement_cognitive_done_guard_total (or_fb5bbbe7)
# ---------------------------------------------------------------------------


_GUARD_LABELS = ("board_id_hash", "outcome", "reason")
_guard_samples = runtime_counter_sample_buffer(
    "kg.refinement_cognitive_guard",
    _GUARD_LABELS,
)
_guard_samples_lock = runtime_lock("kg.refinement_cognitive_guard.samples")


def _board_id_hash(board_id: str) -> str:
    return hashlib.sha256(board_id.encode("utf-8")).hexdigest()[:16]


def _emit_guard_sample(
    *, board_id_hash: str, outcome: str, reason: str
) -> None:
    with _guard_samples_lock:
        _guard_samples.append({
            "board_id_hash": board_id_hash,
            "outcome": outcome,
            "reason": reason,
        })


def get_guard_counter_labels() -> tuple[str, ...]:
    return _GUARD_LABELS


def get_guard_event_count(
    *,
    board_id_hash: str | None = None,
    outcome: str | None = None,
    reason: str | None = None,
) -> int:
    with _guard_samples_lock:
        return _guard_samples.count(
            board_id_hash=board_id_hash,
            outcome=outcome,
            reason=reason,
        )


def get_guard_samples() -> list[dict[str, Any]]:
    with _guard_samples_lock:
        return _guard_samples.snapshot()


def reset_guard_counter() -> None:
    with _guard_samples_lock:
        _guard_samples.clear()


# ---------------------------------------------------------------------------
# Counter — kg_cognitive_marker_terminal_write_blocked_total (or_303fbcb3)
# ---------------------------------------------------------------------------
#
# Alert metric: any non-zero value in production means deterministic
# code tried to perform cognitive judgement (write a terminal status).
# Labels: board_id_hash, attempted_status, outcome.


class TerminalWriteBlockOutcome(str, Enum):
    BLOCKED = "blocked"
    ALLOWED = "allowed"  # purely defensive — must always be ``blocked``.


_TERMINAL_BLOCK_LABELS = ("board_id_hash", "attempted_status", "outcome")
_terminal_block_samples = runtime_counter_sample_buffer(
    "kg.refinement_cognitive_terminal_block",
    _TERMINAL_BLOCK_LABELS,
)
_terminal_block_lock = runtime_lock(
    "kg.refinement_cognitive_terminal_block.samples"
)

_TERMINAL_ITEM_STATUSES: frozenset[str] = frozenset({
    CognitiveItemStatus.CONSOLIDATED.value,
    CognitiveItemStatus.SKIPPED.value,
    CognitiveItemStatus.FAILED.value,
    CognitiveItemStatus.IN_PROGRESS.value,
})


def _emit_terminal_block_sample(
    *, board_id_hash: str, attempted_status: str, outcome: str
) -> None:
    with _terminal_block_lock:
        _terminal_block_samples.append({
            "board_id_hash": board_id_hash,
            "attempted_status": attempted_status,
            "outcome": outcome,
        })


def get_terminal_block_counter_labels() -> tuple[str, ...]:
    return _TERMINAL_BLOCK_LABELS


def get_terminal_block_event_count(
    *,
    board_id_hash: str | None = None,
    attempted_status: str | None = None,
    outcome: str | None = None,
) -> int:
    with _terminal_block_lock:
        return _terminal_block_samples.count(
            board_id_hash=board_id_hash,
            attempted_status=attempted_status,
            outcome=outcome,
        )


def get_terminal_block_samples() -> list[dict[str, Any]]:
    with _terminal_block_lock:
        return _terminal_block_samples.snapshot()


def reset_terminal_block_counter() -> None:
    with _terminal_block_lock:
        _terminal_block_samples.clear()


def assert_deterministic_only_pending(
    *,
    board_id: str,
    items: Sequence[CognitiveConsolidationItem],
) -> None:
    """Defensive check called from deterministic write paths
    (CognitivePendingMarker → CognitiveConsolidationItemStore._write_initial).

    Walks the per-item list and emits ``kg_cognitive_marker_terminal_write_blocked_total``
    + raises ``RefinementGuardError(code="terminal_write_blocked")`` if
    any item carries a non-``pending`` status. This enforces tr_9521d8fd
    + ir_dbf036d8 at the storage boundary so a future bug in the
    materialization path cannot silently flip an item to a terminal
    status (the marker can only OPEN cognitive debt).
    """

    board_hash = _board_id_hash(board_id)
    for item in items:
        if item.status == CognitiveItemStatus.PENDING.value:
            continue
        _emit_terminal_block_sample(
            board_id_hash=board_hash,
            attempted_status=item.status
            if item.status in _TERMINAL_ITEM_STATUSES
            else CognitiveItemStatus.PENDING.value,
            outcome=TerminalWriteBlockOutcome.BLOCKED.value,
        )
        raise RefinementGuardError(
            code="terminal_write_blocked",
            message=(
                "deterministic write path attempted to set terminal "
                f"cognitive status {item.status!r} for item "
                f"{item.item_id!r}; only ``pending`` is allowed"
            ),
        )


# ---------------------------------------------------------------------------
# Guard
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RefinementCognitiveCompletionGuard:
    """Fail-closed guard. The store is injected so tests + production
    can wire different base_dirs; the guard does not touch the file
    system directly."""

    store: CognitiveConsolidationItemStore

    def evaluate(
        self,
        *,
        board_id: str,
        refinement_id: str,
        target_status: str,
        source_ref: str | None = None,
        kg_generation_id: str | None = None,
    ) -> RefinementGuardResult:
        """Evaluate whether the refinement transition is allowed.

        Per the contract, the guard ONLY applies to ``target_status="done"``.
        Any other value raises ``RefinementGuardError(code="invalid_target_status")``.
        """

        if target_status != "done":
            _emit_guard_sample(
                board_id_hash=_board_id_hash(board_id),
                outcome=RefinementGuardOutcome.INVALID_TARGET_STATUS.value,
                reason=RefinementGuardReason.NO_ACTIVE_COGNITIVE_ITEMS.value,
            )
            raise RefinementGuardError(
                code="invalid_target_status",
                message="Guard only applies to target_status done",
            )

        board_hash = _board_id_hash(board_id)
        resolved_source_ref = source_ref or f"refinement:{refinement_id}"

        try:
            resolved_generation = (
                kg_generation_id
                if kg_generation_id
                else self.store.latest_generation(board_id)
            )
            items: list[CognitiveConsolidationItem] = (
                self.store.list_items(board_id, resolved_generation)
                if resolved_generation
                else []
            )
        except Exception:
            # fail-closed: block the transition + emit the unavailable
            # outcome so the operator sees the upstream failure.
            _emit_guard_sample(
                board_id_hash=board_hash,
                outcome=RefinementGuardOutcome.UNAVAILABLE.value,
                reason=RefinementGuardReason.COGNITIVE_STATUS_UNAVAILABLE.value,
            )
            return RefinementGuardResult(
                allowed=False,
                reason=RefinementGuardReason.COGNITIVE_STATUS_UNAVAILABLE.value,
            )

        # Filter to items matching the refinement's source_ref AND in
        # an active status. Terminal statuses do not block.
        active_items = [
            item
            for item in items
            if item.source_ref == resolved_source_ref
            and item.status in _ACTIVE_STATUSES
        ]

        if not active_items:
            _emit_guard_sample(
                board_id_hash=board_hash,
                outcome=RefinementGuardOutcome.ALLOWED.value,
                reason=RefinementGuardReason.NO_ACTIVE_COGNITIVE_ITEMS.value,
            )
            return RefinementGuardResult(
                allowed=True,
                reason=RefinementGuardReason.NO_ACTIVE_COGNITIVE_ITEMS.value,
            )

        blocking = tuple(
            GuardBlockingItem(item_id=item.item_id, status=item.status)
            for item in active_items
        )
        _emit_guard_sample(
            board_id_hash=board_hash,
            outcome=RefinementGuardOutcome.BLOCKED.value,
            reason=RefinementGuardReason.COGNITIVE_CONSOLIDATION_PENDING.value,
        )
        return RefinementGuardResult(
            allowed=False,
            reason=RefinementGuardReason.COGNITIVE_CONSOLIDATION_PENDING.value,
            blocking_items=blocking,
        )


__all__ = [
    "GuardBlockingItem",
    "RefinementCognitiveCompletionGuard",
    "RefinementGuardError",
    "RefinementGuardOutcome",
    "RefinementGuardReason",
    "RefinementGuardResult",
    "TerminalWriteBlockOutcome",
    "assert_deterministic_only_pending",
    "get_guard_counter_labels",
    "get_guard_event_count",
    "get_guard_samples",
    "get_terminal_block_counter_labels",
    "get_terminal_block_event_count",
    "get_terminal_block_samples",
    "reset_guard_counter",
    "reset_terminal_block_counter",
]
