"""Storage snapshots required by Core queue-health policy."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


@dataclass(frozen=True, slots=True)
class QueueHealthStorageSnapshot:
    queue_depth: int
    oldest_pending_at: datetime | None
    claimed_count: int
    claimed_boards: tuple[str, ...]
    dead_letter_count: int


@dataclass(frozen=True, slots=True)
class ActiveConsolidationWorkItemSnapshot:
    queue_id: str
    status: str
    work_kind: str
    artifact_type: str
    artifact_id: str
    attempts: int
    triggered_at: datetime
    claimed_at: datetime | None = None
    claim_timeout_at: datetime | None = None
    next_retry_at: datetime | None = None
    last_error: str | None = None


@dataclass(frozen=True, slots=True)
class ActiveQueueStorageSnapshot:
    consolidation_by_status: dict[str, int]
    consolidation_by_category: dict[str, int]
    consolidation_oldest_at: datetime | None
    outbox_depth: int
    outbox_oldest_at: datetime | None
    consolidation_ready_count: int
    consolidation_scheduled_retry_count: int
    consolidation_claimed_count: int
    consolidation_overdue_claimed_count: int
    consolidation_ready_oldest_at: datetime | None
    consolidation_overdue_claimed_oldest_at: datetime | None
    consolidation_next_retry_at: datetime | None
    consolidation_by_work_kind: dict[str, int]
    consolidation_max_attempts: int
    consolidation_items: tuple[ActiveConsolidationWorkItemSnapshot, ...]


@dataclass(frozen=True, slots=True)
class GlobalOutboxDeadLetterRowSnapshot:
    event_id: str
    board_id: str
    event_type: str
    retry_count: int
    created_at: datetime
    last_error: str | None


@dataclass(frozen=True, slots=True)
class GlobalOutboxDeadLetterStorageSnapshot:
    total_count: int
    oldest_created_at: datetime | None
    rows: tuple[GlobalOutboxDeadLetterRowSnapshot, ...]


class QueueHealthReadPort(Protocol):
    async def health_snapshot(
        self, context: object
    ) -> QueueHealthStorageSnapshot: ...

    async def active_snapshot(
        self,
        context: object,
        *,
        board_id: str | None,
        active_statuses: tuple[str, ...],
        max_outbox_retries: int,
        dead_letter_retry_sentinel: int,
        now: datetime,
        stuck_before: datetime,
        item_limit: int,
        include_code_traceability: bool = True,
    ) -> ActiveQueueStorageSnapshot: ...

    async def global_outbox_dead_letter_snapshot(
        self,
        context: object,
        *,
        board_id: str | None,
        limit: int,
        max_outbox_retries: int,
        dead_letter_retry_sentinel: int,
    ) -> GlobalOutboxDeadLetterStorageSnapshot: ...


_RUNTIME_KEY = "ports.queue_health.reader"


def register_queue_health_read_port(reader: QueueHealthReadPort) -> None:
    register_runtime_value(_RUNTIME_KEY, reader)


def get_queue_health_read_port() -> QueueHealthReadPort:
    return require_runtime_value(_RUNTIME_KEY, "queue_health_read_port_not_configured")


def reset_queue_health_read_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ActiveConsolidationWorkItemSnapshot",
    "ActiveQueueStorageSnapshot",
    "GlobalOutboxDeadLetterRowSnapshot",
    "GlobalOutboxDeadLetterStorageSnapshot",
    "QueueHealthReadPort",
    "QueueHealthStorageSnapshot",
    "get_queue_health_read_port",
    "register_queue_health_read_port",
    "reset_queue_health_read_port_for_tests",
]
