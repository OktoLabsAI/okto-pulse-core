"""Dialect-neutral relational side-effect port.

Core handlers may request logical persistence effects through this port, but
the SQL dialect, SQLAlchemy metadata construction and concrete upsert mechanics
belong to an edition adapter.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, Sequence, runtime_checkable


class RelationalEffectsProviderMissing(RuntimeError):
    """Raised when a required relational effects provider is not registered."""

    code = "relational_effects_provider_missing"

    def __init__(self) -> None:
        super().__init__(
            "relational_effects_provider_missing: required provider not supplied"
        )


@dataclass(frozen=True, slots=True)
class ConsolidationQueueUpsert:
    board_id: str
    artifact_type: str
    artifact_id: str
    priority: str
    source: str
    triggered_by_event: str


@dataclass(frozen=True, slots=True)
class KGTickRunUpsert:
    tick_id: str
    started_at: datetime
    completed_at: datetime
    nodes_recomputed: int
    duration_ms: float
    boards_processed: int
    boards_failed: int = 0
    error: str | None = None


@runtime_checkable
class RelationalEffectsPort(Protocol):
    """Logical relational operations required by core runtime handlers."""

    async def count_active_consolidation_queue(
        self,
        session: Any,
        *,
        board_id: str,
    ) -> int:
        ...

    async def upsert_consolidation_queue_unless_tombstoned(
        self,
        session: Any,
        upsert: ConsolidationQueueUpsert,
    ) -> bool:
        """Atomically admit legacy work unless fenced.

        Return ``True`` only when a row is inserted or a terminal row is
        reopened. Active-row coalescing and tombstone suppression return
        ``False`` without mutating the queue.
        """
        ...

    async def list_board_ids(self, session: Any) -> Sequence[str]:
        ...

    async def read_latest_kg_tick_completed_at(
        self,
        session: Any,
    ) -> datetime | None:
        ...

    async def upsert_kg_tick_run(
        self,
        session: Any,
        upsert: KGTickRunUpsert,
    ) -> None:
        ...


_RUNTIME_KEY = "ports.relational_effects.port"


def register_relational_effects_port(port: RelationalEffectsPort) -> None:
    """Register the edition-owned relational effects port."""

    register_runtime_value(_RUNTIME_KEY, port)


def get_relational_effects_port() -> RelationalEffectsPort:
    """Return the registered relational effects port, fail-closed if absent."""

    port = resolve_runtime_value(_RUNTIME_KEY)
    if port is None:
        raise RelationalEffectsProviderMissing()
    return port


def reset_relational_effects_port_for_tests() -> None:
    """Drop the registered port for test isolation."""

    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ConsolidationQueueUpsert",
    "KGTickRunUpsert",
    "RelationalEffectsPort",
    "RelationalEffectsProviderMissing",
    "get_relational_effects_port",
    "register_relational_effects_port",
    "reset_relational_effects_port_for_tests",
]
