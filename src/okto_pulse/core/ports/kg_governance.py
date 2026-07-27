"""Relational persistence boundary for KG governance policies."""

from __future__ import annotations

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, Sequence


@dataclass(slots=True)
class HistoricalBoardRecord:
    id: str
    settings: dict[str, Any]


@dataclass(frozen=True, slots=True)
class HistoricalArtifactFact:
    artifact_type: str
    artifact_id: str


@dataclass(frozen=True, slots=True)
class HistoricalQueueFact:
    id: str
    artifact_type: str
    artifact_id: str
    source: str
    status: str


@dataclass(frozen=True, slots=True)
class HistoricalQueueInsert:
    id: str
    board_id: str
    artifact_type: str
    artifact_id: str
    priority: str = "low"
    source: str = "historical_backfill"
    status: str = "pending"


@dataclass(frozen=True, slots=True)
class GovernanceUndoFact:
    session_id: str
    undo_status: str
    node_ids: tuple[str, ...]
    blocking_sessions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BoostAuditRecord:
    session_id: str
    board_id: str
    artifact_id: str
    agent_id: str
    started_at: datetime
    committed_at: datetime


@dataclass(frozen=True, slots=True)
class BoardErasureJobFact:
    """Durable continuation for physical erasure after the source commit."""

    board_id: str
    actor_id: str
    attempts: int
    last_error: str | None
    next_attempt_at: datetime


class KGGovernanceStore(Protocol):
    async def get_board(
        self, context: Any, *, board_id: str
    ) -> HistoricalBoardRecord | None: ...

    async def save_board(self, context: Any, board: HistoricalBoardRecord) -> None: ...

    async def queue_counts(self, context: Any, *, board_id: str) -> dict[str, int]: ...

    async def list_historical_artifacts(
        self, context: Any, *, board_id: str
    ) -> tuple[HistoricalArtifactFact, ...]: ...

    async def list_live_queue(
        self, context: Any, *, board_id: str
    ) -> tuple[HistoricalQueueFact, ...]: ...

    async def delete_terminal_queue(self, context: Any, *, board_id: str) -> None: ...

    async def add_queue_entries(
        self, context: Any, entries: Sequence[HistoricalQueueInsert]
    ) -> None: ...

    async def update_historical_status(
        self,
        context: Any,
        *,
        board_id: str,
        old_status: str,
        new_status: str,
    ) -> None: ...

    async def delete_historical_pending(
        self, context: Any, *, board_id: str
    ) -> int: ...

    async def purge_stale_metadata(self, context: Any, *, board_id: str) -> None: ...

    async def get_undo_fact(
        self, context: Any, *, board_id: str, session_id: str
    ) -> GovernanceUndoFact | None: ...

    async def mark_session_undone(
        self, context: Any, *, session_id: str, undone_at: datetime
    ) -> None: ...

    async def purge_expired_audit(
        self, context: Any, *, board_id: str, cutoff: datetime
    ) -> int: ...

    async def purge_board_metadata(self, context: Any, *, board_id: str) -> None: ...

    async def stage_board_erasure_job(
        self,
        context: Any,
        *,
        board_id: str,
        actor_id: str,
    ) -> BoardErasureJobFact: ...

    async def get_board_erasure_job(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> BoardErasureJobFact | None: ...

    async def list_due_board_erasure_jobs(
        self,
        context: Any,
        *,
        now: datetime,
        limit: int,
    ) -> tuple[BoardErasureJobFact, ...]: ...

    async def record_board_erasure_failure(
        self,
        context: Any,
        *,
        board_id: str,
        error: str,
        next_attempt_at: datetime,
    ) -> None: ...

    async def complete_board_erasure_job(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> bool: ...

    def add_boost_audit(self, context: Any, audit: BoostAuditRecord) -> None: ...

    async def commit(self, context: Any) -> None: ...


_RUNTIME_KEY = "ports.kg_governance.store"


def register_kg_governance_store(store: KGGovernanceStore) -> None:
    register_runtime_value(_RUNTIME_KEY, store)


def get_kg_governance_store() -> KGGovernanceStore:
    return require_runtime_value(_RUNTIME_KEY, "kg_governance_store_not_configured")


def reset_kg_governance_store_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "BoardErasureJobFact",
    "BoostAuditRecord",
    "GovernanceUndoFact",
    "HistoricalArtifactFact",
    "HistoricalBoardRecord",
    "HistoricalQueueFact",
    "HistoricalQueueInsert",
    "KGGovernanceStore",
    "get_kg_governance_store",
    "register_kg_governance_store",
    "reset_kg_governance_store_for_tests",
]
