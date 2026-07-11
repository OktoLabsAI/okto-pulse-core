"""Persistence boundary for Design System catalog, links and gate audits."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


@dataclass(slots=True)
class DesignSystemRecord:
    id: str
    scope: str
    board_id: str | None
    title: str
    payload: dict[str, Any] | None
    version: int
    status: str
    owner_id: str
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class BoardDesignSystemRecord:
    board_id: str
    design_system_id: str
    design_system_version: int


@dataclass(frozen=True, slots=True)
class DesignSystemGateAuditRecord:
    board_id: str
    entity_type: str | None
    entity_id: str | None
    mockup_id: str | None
    mode: str
    outcome: str
    reason: str
    expected_design_system_id: str | None
    expected_design_system_version: int | None
    provided_ref: dict[str, Any] | None


class DesignSystemStore(Protocol):
    async def create(
        self, context: Any, record: DesignSystemRecord
    ) -> DesignSystemRecord: ...

    async def list_catalog(
        self, context: Any, *, scope: str, board_id: str | None
    ) -> tuple[DesignSystemRecord, ...]: ...

    async def get(
        self, context: Any, *, design_system_id: str
    ) -> DesignSystemRecord | None: ...

    async def save(
        self, context: Any, record: DesignSystemRecord
    ) -> DesignSystemRecord: ...

    async def delete(self, context: Any, *, design_system_id: str) -> bool: ...

    async def upsert_board_link(
        self,
        context: Any,
        *,
        board_id: str,
        design_system_id: str,
        design_system_version: int,
    ) -> BoardDesignSystemRecord: ...

    async def get_board_link(
        self, context: Any, *, board_id: str
    ) -> BoardDesignSystemRecord | None: ...

    async def delete_board_link(self, context: Any, *, board_id: str) -> bool: ...

    async def get_board_snapshot(
        self, context: Any, *, board_id: str
    ) -> dict[str, Any] | None: ...

    async def get_board_settings(
        self, context: Any, *, board_id: str
    ) -> dict[str, Any]: ...

    def marked_screen_ids(self, context: Any) -> set[str]: ...

    def add_gate_audit(
        self, context: Any, audit: DesignSystemGateAuditRecord
    ) -> None: ...


_RUNTIME_KEY = "ports.design_system.store"


def register_design_system_store(store: DesignSystemStore) -> None:
    register_runtime_value(_RUNTIME_KEY, store)


def get_design_system_store() -> DesignSystemStore:
    return require_runtime_value(_RUNTIME_KEY, "design_system_store_not_configured")


def reset_design_system_store_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "BoardDesignSystemRecord",
    "DesignSystemGateAuditRecord",
    "DesignSystemRecord",
    "DesignSystemStore",
    "get_design_system_store",
    "register_design_system_store",
    "reset_design_system_store_for_tests",
]
