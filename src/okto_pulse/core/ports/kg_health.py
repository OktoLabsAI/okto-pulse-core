"""Relational read boundary for KG health diagnostics."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, Sequence


@dataclass(frozen=True, slots=True)
class KGHealthQueueSnapshot:
    board_exists: bool
    queue_depth: int
    oldest_triggered_at: datetime | None
    dead_letter_count: int


@dataclass(frozen=True, slots=True)
class KGTickRunFact:
    started_at: datetime
    completed_at: datetime | None
    nodes_recomputed: int
    boards_processed: int
    boards_failed: int
    error: str | None


class KGHealthReadPort(Protocol):
    async def queue_snapshot(
        self, context: Any, *, board_id: str
    ) -> KGHealthQueueSnapshot: ...

    async def list_tick_runs(self, context: Any) -> Sequence[KGTickRunFact]: ...

    async def has_materialized_history(
        self, context: Any, *, board_id: str
    ) -> bool: ...

    async def count_partition_debt(
        self,
        context: Any,
        *,
        board_id: str,
        target_status: str,
        open_states: Sequence[str],
    ) -> int: ...


_RUNTIME_KEY = "ports.kg_health.reader"


def register_kg_health_read_port(reader: KGHealthReadPort) -> None:
    register_runtime_value(_RUNTIME_KEY, reader)


def get_kg_health_read_port() -> KGHealthReadPort:
    return require_runtime_value(_RUNTIME_KEY, "kg_health_read_port_not_configured")


def reset_kg_health_read_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "KGHealthQueueSnapshot",
    "KGHealthReadPort",
    "KGTickRunFact",
    "get_kg_health_read_port",
    "register_kg_health_read_port",
    "reset_kg_health_read_port_for_tests",
]
