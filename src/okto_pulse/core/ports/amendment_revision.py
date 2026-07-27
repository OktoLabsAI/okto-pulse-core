"""Persistence contract for amendment/hotfix revision records."""

from __future__ import annotations

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol, Sequence

from okto_pulse.core.domain.amendment_eligibility import (
    AmendmentLineageState,
    AmendmentRevisionStatus,
)


@dataclass(slots=True)
class AmendmentRevisionRecord:
    board_id: str
    original_spec_id: str
    origin_bug_id: str
    created_by: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    origin_task_ids: list[str] = field(default_factory=list)
    affected_task_ids: list[str] = field(default_factory=list)
    revision_spec_id: str | None = None
    regression_scenario_ids: list[str] = field(default_factory=list)
    regression_test_task_ids: list[str] = field(default_factory=list)
    automated_regression_refs: list[str] = field(default_factory=list)
    status: AmendmentRevisionStatus = AmendmentRevisionStatus.DRAFT
    lineage_state: AmendmentLineageState = AmendmentLineageState.INCOMPLETE
    validation_metadata: dict[str, Any] | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True, slots=True)
class AmendmentAuditRecord:
    action: str
    actor: str
    details: dict[str, Any]


class AmendmentRevisionStore(Protocol):
    async def get(
        self, context: object, *, amendment_id: str
    ) -> AmendmentRevisionRecord | None: ...

    async def list_for_bug(
        self,
        context: object,
        *,
        board_id: str,
        original_spec_id: str,
        origin_bug_id: str,
    ) -> Sequence[AmendmentRevisionRecord]: ...

    async def save(
        self,
        context: object,
        record: AmendmentRevisionRecord,
        *,
        audit: AmendmentAuditRecord,
    ) -> AmendmentRevisionRecord: ...


_RUNTIME_KEY = "ports.amendment_revision.store"


def register_amendment_revision_store(store: AmendmentRevisionStore) -> None:
    register_runtime_value(_RUNTIME_KEY, store)


def get_amendment_revision_store() -> AmendmentRevisionStore:
    return require_runtime_value(
        _RUNTIME_KEY, "amendment_revision_store_not_configured"
    )


def reset_amendment_revision_store_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "AmendmentAuditRecord",
    "AmendmentRevisionRecord",
    "AmendmentRevisionStore",
    "get_amendment_revision_store",
    "register_amendment_revision_store",
    "reset_amendment_revision_store_for_tests",
]
