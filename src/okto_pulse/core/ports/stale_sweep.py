"""Durable board-scoped stale-sweep scheduling boundary.

The Core owns the sweep policy and candidate ordering.  Concrete editions own
the relational transaction that creates synthetic deletion identities, queues
their reconcile intents and advances the sweep checkpoint.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


STALE_SWEEP_WORK_KIND = "stale_sweep"
STALE_SWEEP_ARTIFACT_TYPE = "board"
STALE_SWEEP_CATCHUP_EPOCH = 1
GOVERNED_SWEEP_ARTIFACT_TYPES = frozenset(
    {"card", "ideation", "refinement", "spec", "sprint"}
)


def _decode_cursor_identity(cursor: str) -> tuple[str, str] | None:
    if not isinstance(cursor, str):
        raise ValueError("stale_sweep_cursor_invalid")
    if cursor == "":
        return None
    try:
        raw = json.loads(cursor)
    except (TypeError, ValueError) as exc:
        raise ValueError("stale_sweep_cursor_invalid") from exc
    if (
        not isinstance(raw, list)
        or len(raw) != 2
        or raw[0] not in GOVERNED_SWEEP_ARTIFACT_TYPES
        or not isinstance(raw[1], str)
        or not raw[1]
        or raw[1] != raw[1].strip()
    ):
        raise ValueError("stale_sweep_cursor_invalid")
    return (str(raw[0]), raw[1])


@dataclass(frozen=True, order=True, slots=True)
class StaleSweepCandidate:
    """One deleted source discovered by the graph/source anti-join."""

    artifact_type: str
    artifact_id: str

    def __post_init__(self) -> None:
        if (
            self.artifact_type not in GOVERNED_SWEEP_ARTIFACT_TYPES
            or not isinstance(self.artifact_id, str)
            or not self.artifact_id
            or self.artifact_id != self.artifact_id.strip()
        ):
            raise ValueError("stale_sweep_candidate_invalid")

    @property
    def source_ref(self) -> str:
        return f"{self.artifact_type}:{self.artifact_id}"

    def synthetic_delete_event_id(
        self,
        *,
        board_id: str,
        epoch: int = STALE_SWEEP_CATCHUP_EPOCH,
    ) -> str:
        if not board_id or isinstance(epoch, bool) or epoch < 1:
            raise ValueError("stale_sweep_synthetic_identity_invalid")
        return (
            f"catchup:{board_id}:{self.artifact_type}:{self.artifact_id}:epoch:{epoch}"
        )


@dataclass(frozen=True, slots=True)
class StaleSweepScheduleRequest:
    board_id: str
    budget: int
    now: datetime

    def __post_init__(self) -> None:
        if (
            not self.board_id
            or isinstance(self.budget, bool)
            or not isinstance(self.budget, int)
            or self.budget < 1
            or not isinstance(self.now, datetime)
        ):
            raise ValueError("stale_sweep_schedule_request_invalid")


@dataclass(frozen=True, slots=True)
class StaleSweepScheduleReceipt:
    board_id: str
    sweep_id: str | None
    scheduled: bool
    board_present: bool
    cursor: str
    budget: int
    attempt: int


@dataclass(frozen=True, slots=True)
class StaleSweepBatchRequest:
    entry_id: str
    claim_token: str
    board_id: str
    cursor: str
    budget: int
    attempt: int
    candidates: tuple[StaleSweepCandidate, ...]
    next_cursor: str
    has_more: bool
    now: datetime

    def __post_init__(self) -> None:
        if (
            not self.entry_id
            or not self.claim_token
            or not self.board_id
            or not isinstance(self.cursor, str)
            or isinstance(self.budget, bool)
            or not isinstance(self.budget, int)
            or self.budget < 1
            or isinstance(self.attempt, bool)
            or not isinstance(self.attempt, int)
            or self.attempt < 0
            or len(self.candidates) > self.budget
            or not isinstance(self.next_cursor, str)
            or (self.candidates and not self.next_cursor)
            or not isinstance(self.now, datetime)
        ):
            raise ValueError("stale_sweep_batch_request_invalid")
        if tuple(sorted(set(self.candidates))) != self.candidates:
            raise ValueError("stale_sweep_candidates_not_strictly_ordered")
        try:
            current = _decode_cursor_identity(self.cursor)
            following = _decode_cursor_identity(self.next_cursor)
        except ValueError as exc:
            raise ValueError("stale_sweep_batch_cursor_invalid") from exc
        current_candidate = (
            StaleSweepCandidate(*current) if current is not None else None
        )
        following_candidate = (
            StaleSweepCandidate(*following) if following is not None else None
        )
        if (
            (
                current_candidate is not None
                and (
                    following_candidate is None
                    or following_candidate < current_candidate
                )
            )
            or (
                self.has_more
                and (
                    following_candidate is None
                    or following_candidate == current_candidate
                )
            )
            or any(
                (current_candidate is not None and candidate <= current_candidate)
                or (following_candidate is not None and candidate > following_candidate)
                for candidate in self.candidates
            )
        ):
            raise ValueError("stale_sweep_batch_cursor_contract_invalid")


@dataclass(frozen=True, slots=True)
class StaleSweepRescheduleRequest:
    entry_id: str
    claim_token: str
    board_id: str
    cursor: str
    budget: int
    attempt: int
    retry_at: datetime
    reason: str

    def __post_init__(self) -> None:
        if (
            not self.entry_id
            or not self.claim_token
            or not self.board_id
            or not isinstance(self.cursor, str)
            or isinstance(self.budget, bool)
            or not isinstance(self.budget, int)
            or self.budget < 1
            or isinstance(self.attempt, bool)
            or not isinstance(self.attempt, int)
            or self.attempt < 0
            or not isinstance(self.retry_at, datetime)
            or not self.reason
        ):
            raise ValueError("stale_sweep_reschedule_request_invalid")
        try:
            _decode_cursor_identity(self.cursor)
        except ValueError as exc:
            raise ValueError("stale_sweep_reschedule_cursor_invalid") from exc


class StaleSweepRunAction(StrEnum):
    ADVANCED = "advanced"
    COMPLETED = "completed"
    RESCHEDULED = "rescheduled"


@dataclass(frozen=True, slots=True)
class StaleSweepRunReceipt:
    entry_id: str
    board_id: str
    action: StaleSweepRunAction
    cursor: str
    budget: int
    attempt: int
    enqueued: int
    has_more: bool
    reason: str | None = None


class StaleSweepClaimConflict(RuntimeError):
    """The sweep row is no longer owned by the claim that staged the run."""


class StaleSweepPort(Protocol):
    async def schedule_stale_sweep(
        self,
        context: Any,
        request: StaleSweepScheduleRequest,
    ) -> StaleSweepScheduleReceipt:
        """Stage at most one active sweep for a board; never commit."""
        ...

    async def stage_stale_sweep_batch(
        self,
        context: Any,
        request: StaleSweepBatchRequest,
    ) -> StaleSweepRunReceipt:
        """Atomically enqueue candidates and then advance/delete checkpoint."""
        ...

    async def reschedule_stale_sweep(
        self,
        context: Any,
        request: StaleSweepRescheduleRequest,
    ) -> StaleSweepRunReceipt:
        """Preserve checkpoint and defer one degraded-board run."""
        ...


_RUNTIME_KEY = "ports.stale_sweep.port"


def register_stale_sweep_port(port: StaleSweepPort) -> None:
    register_runtime_value(_RUNTIME_KEY, port)


def get_stale_sweep_port() -> StaleSweepPort:
    return require_runtime_value(_RUNTIME_KEY, "stale_sweep_port_not_configured")


def reset_stale_sweep_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "GOVERNED_SWEEP_ARTIFACT_TYPES",
    "STALE_SWEEP_ARTIFACT_TYPE",
    "STALE_SWEEP_CATCHUP_EPOCH",
    "STALE_SWEEP_WORK_KIND",
    "StaleSweepBatchRequest",
    "StaleSweepCandidate",
    "StaleSweepClaimConflict",
    "StaleSweepPort",
    "StaleSweepRescheduleRequest",
    "StaleSweepRunAction",
    "StaleSweepRunReceipt",
    "StaleSweepScheduleReceipt",
    "StaleSweepScheduleRequest",
    "get_stale_sweep_port",
    "register_stale_sweep_port",
    "reset_stale_sweep_port_for_tests",
]
