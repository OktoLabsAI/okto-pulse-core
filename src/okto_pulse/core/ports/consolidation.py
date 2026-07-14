"""Relational persistence boundary for the consolidation processor."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, Sequence


@dataclass(slots=True)
class ConsolidationQueueRecord:
    id: str
    board_id: str
    artifact_type: str
    artifact_id: str
    status: str
    attempts: int
    last_error: str | None
    next_retry_at: datetime | None
    claimed_at: datetime | None
    claim_timeout_at: datetime | None
    worker_id: str | None
    claimed_by_session_id: str | None
    triggered_at: datetime | None
    priority: str


@dataclass(frozen=True, slots=True)
class ConsolidationPoisonRow:
    id: str
    attempts: int


class ConsolidationPersistencePort(Protocol):
    async def load_artifact(
        self,
        context: Any,
        *,
        artifact_type: str,
        artifact_id: str,
    ) -> Any | None: ...

    async def list_artifacts(
        self,
        context: Any,
        *,
        artifact_type: str,
        artifact_ids: Sequence[str],
        board_id: str | None = None,
    ) -> tuple[Any, ...]: ...

    async def list_stale_claims(
        self,
        context: Any,
        *,
        now: datetime,
        legacy_cutoff: datetime,
    ) -> tuple[ConsolidationQueueRecord, ...]: ...

    async def count_pending(self, context: Any) -> int: ...

    async def list_claimed_board_ids(self, context: Any) -> frozenset[str]: ...

    async def list_ready_pending(
        self, context: Any, *, now: datetime
    ) -> tuple[ConsolidationQueueRecord, ...]: ...

    async def get_queue_entry(
        self, context: Any, *, entry_id: str
    ) -> ConsolidationQueueRecord | None: ...

    async def save_queue_entries(
        self, context: Any, entries: Sequence[ConsolidationQueueRecord]
    ) -> None: ...

    async def delete_queue_entry(self, context: Any, *, entry_id: str) -> None: ...

    async def discard_artifact_work(
        self,
        context: Any,
        *,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
    ) -> None:
        """Discard transient/terminal KG work for an intentionally deleted artifact."""
        ...

    async def board_exists(self, context: Any, *, board_id: str) -> bool: ...

    async def list_dlq_auto_drain_board_ids(
        self, context: Any
    ) -> tuple[str, ...]: ...

    async def count_dead_letters(self, context: Any, *, board_id: str) -> int: ...

    async def delete_poison_dead_letters(
        self, context: Any, *, board_id: str, max_attempts: int
    ) -> tuple[ConsolidationPoisonRow, ...]: ...

    async def commit(self, context: Any) -> None: ...

    async def rollback(self, context: Any) -> None: ...


_RUNTIME_KEY = "ports.consolidation.port"


def register_consolidation_persistence_port(
    port: ConsolidationPersistencePort,
) -> None:
    register_runtime_value(_RUNTIME_KEY, port)


def get_consolidation_persistence_port() -> ConsolidationPersistencePort:
    return require_runtime_value(_RUNTIME_KEY, "consolidation_persistence_port_not_configured")


def reset_consolidation_persistence_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ConsolidationPersistencePort",
    "ConsolidationPoisonRow",
    "ConsolidationQueueRecord",
    "get_consolidation_persistence_port",
    "register_consolidation_persistence_port",
    "reset_consolidation_persistence_port_for_tests",
]
