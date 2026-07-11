"""Storage snapshots required by Core queue-health policy."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True, slots=True)
class QueueHealthStorageSnapshot:
    queue_depth: int
    oldest_pending_at: datetime | None
    claimed_count: int
    claimed_boards: tuple[str, ...]
    dead_letter_count: int


@dataclass(frozen=True, slots=True)
class ActiveQueueStorageSnapshot:
    consolidation_by_status: dict[str, int]
    consolidation_by_category: dict[str, int]
    consolidation_oldest_at: datetime | None
    outbox_depth: int
    outbox_oldest_at: datetime | None


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
    ) -> ActiveQueueStorageSnapshot: ...


_RUNTIME_KEY = "ports.queue_health.reader"


def register_queue_health_read_port(reader: QueueHealthReadPort) -> None:
    register_runtime_value(_RUNTIME_KEY, reader)


def get_queue_health_read_port() -> QueueHealthReadPort:
    return require_runtime_value(_RUNTIME_KEY, "queue_health_read_port_not_configured")


def reset_queue_health_read_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ActiveQueueStorageSnapshot",
    "QueueHealthReadPort",
    "QueueHealthStorageSnapshot",
    "get_queue_health_read_port",
    "register_queue_health_read_port",
    "reset_queue_health_read_port_for_tests",
]
