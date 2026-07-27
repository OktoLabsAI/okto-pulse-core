"""Relational persistence boundary for global discovery outbox processing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, Sequence

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


GLOBAL_OUTBOX_DEAD_LETTER_SENTINEL = -1
GLOBAL_OUTBOX_MAX_RETRIES = 5


@dataclass(slots=True)
class GlobalOutboxEventRecord:
    id: str
    event_id: str
    board_id: str
    session_id: str | None
    payload: dict[str, Any]
    retry_count: int
    last_error: str | None
    processed_at: datetime | None
    created_at: datetime
    event_type: str = "consolidation_committed"


@dataclass(frozen=True, slots=True)
class GlobalOutboxDeadLetterCursor:
    """Stable keyset cursor for an ordered dead-letter recovery scan."""

    created_at: datetime
    id: str


@dataclass(frozen=True, slots=True)
class GlobalOutboxNodeRefFact:
    graph_node_id: str
    graph_node_type: str


class GlobalOutboxMutationConflict(RuntimeError):
    """A terminal row changed after validation but before guarded requeue."""


class GlobalOutboxStore(Protocol):
    async def materialize_claimed(
        self, context: Any, claimed: Sequence[Any]
    ) -> tuple[GlobalOutboxEventRecord, ...]: ...

    async def list_dead_letters(
        self,
        context: Any,
        *,
        limit: int,
        error_markers: Sequence[str],
        after: GlobalOutboxDeadLetterCursor | None = None,
    ) -> tuple[GlobalOutboxEventRecord, ...]: ...

    async def list_terminal_events(
        self,
        context: Any,
        *,
        limit: int,
        after: GlobalOutboxDeadLetterCursor | None = None,
    ) -> tuple[GlobalOutboxEventRecord, ...]: ...

    async def get_events_by_ids(
        self,
        context: Any,
        *,
        ids: tuple[str, ...],
    ) -> tuple[GlobalOutboxEventRecord, ...]: ...

    async def requeue_terminal_events(
        self,
        context: Any,
        events: Sequence[GlobalOutboxEventRecord],
    ) -> None: ...

    async def list_added_node_refs(
        self,
        context: Any,
        *,
        session_id: str,
        board_id: str,
        node_types: Sequence[str],
    ) -> tuple[GlobalOutboxNodeRefFact, ...]: ...

    async def save_events(
        self, context: Any, events: Sequence[GlobalOutboxEventRecord]
    ) -> None: ...

    async def commit(self, context: Any) -> None: ...


_RUNTIME_KEY = "ports.global_outbox.store"


def register_global_outbox_store(store: GlobalOutboxStore) -> None:
    register_runtime_value(_RUNTIME_KEY, store)


def get_global_outbox_store() -> GlobalOutboxStore:
    return require_runtime_value(_RUNTIME_KEY, "global_outbox_store_not_configured")


def reset_global_outbox_store_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "GLOBAL_OUTBOX_DEAD_LETTER_SENTINEL",
    "GLOBAL_OUTBOX_MAX_RETRIES",
    "GlobalOutboxDeadLetterCursor",
    "GlobalOutboxEventRecord",
    "GlobalOutboxMutationConflict",
    "GlobalOutboxNodeRefFact",
    "GlobalOutboxStore",
    "get_global_outbox_store",
    "register_global_outbox_store",
    "reset_global_outbox_store_for_tests",
]
