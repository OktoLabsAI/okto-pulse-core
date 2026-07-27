"""Persistence contract for canonical-debt domain records."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Protocol, Sequence


@dataclass(slots=True)
class CanonicalDebtRecord:
    board_id: str
    artifact_type: str
    artifact_id: str
    source_ref: str
    content_hash: str
    target_status: str
    canonical_state: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source_version: str | None = None
    graph_layer: str = "canonical"
    maturity_status: str | None = None
    failure_reason: str | None = None
    last_error: str | None = None
    retry_count: int = 0
    next_retry_at: datetime | None = None
    last_attempt_at: datetime | None = None
    owner_agent_id: str | None = None
    correlation_id: str | None = None
    queue_ref: str | None = None
    dlq_ref: str | None = None
    evidence_ref: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class CanonicalDebtStore(Protocol):
    async def counts_by_state(
        self, context: object, *, board_id: str
    ) -> dict[str, int]: ...

    async def list_records(
        self,
        context: object,
        *,
        board_id: str,
        artifact_type: str | None,
        state: str | None,
        limit: int,
        offset: int,
    ) -> tuple[int, Sequence[CanonicalDebtRecord]]: ...

    async def find_by_identity(
        self,
        context: object,
        *,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
        target_status: str,
        content_hash: str,
    ) -> CanonicalDebtRecord | None: ...

    async def get(
        self, context: object, *, debt_id: str
    ) -> CanonicalDebtRecord | None: ...

    async def find_open_by_evidence(
        self,
        context: object,
        *,
        board_id: str,
        source_ref: str,
        content_hash: str,
        open_states: Sequence[str],
    ) -> Sequence[CanonicalDebtRecord]: ...

    async def find_open_for_artifact(
        self,
        context: object,
        *,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
        target_status: str,
        open_states: Sequence[str],
    ) -> Sequence[CanonicalDebtRecord]: ...

    async def save(
        self,
        context: object,
        record: CanonicalDebtRecord,
        *,
        commit: bool = False,
    ) -> CanonicalDebtRecord: ...


_RUNTIME_KEY = "ports.canonical_debt.store"


def register_canonical_debt_store(store: CanonicalDebtStore) -> None:
    register_runtime_value(_RUNTIME_KEY, store)


def get_canonical_debt_store() -> CanonicalDebtStore:
    return require_runtime_value(_RUNTIME_KEY, "canonical_debt_store_not_configured")


def reset_canonical_debt_store_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "CanonicalDebtRecord",
    "CanonicalDebtStore",
    "get_canonical_debt_store",
    "register_canonical_debt_store",
    "reset_canonical_debt_store_for_tests",
]
