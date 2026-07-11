"""Read contracts for the Discovery catalog and history."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, Sequence


@dataclass(frozen=True, slots=True)
class DiscoveryIntentRecord:
    id: str
    name: str
    label: str
    description: str | None
    category: str
    tool_binding: str
    params_schema: dict[str, Any] | None
    renderer: str
    min_permission: str | None
    active: bool
    is_seed: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class DiscoverySavedSearchRecord:
    id: str
    board_id: str
    name: str
    query: str | None
    intent_id: str | None
    filters_json: dict[str, Any] | None
    created_by: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class DiscoverySearchHistoryRecord:
    id: str
    board_id: str
    user_id: str
    query: str | None
    intent_id: str | None
    result_count: int
    searched_at: datetime


class DiscoveryCatalogReadPort(Protocol):
    async def list_active_intents(
        self, context: object
    ) -> Sequence[DiscoveryIntentRecord]: ...

    async def list_saved_searches(
        self, context: object, *, board_id: str
    ) -> Sequence[DiscoverySavedSearchRecord]: ...

    async def list_search_history(
        self, context: object, *, board_id: str, user_id: str, limit: int
    ) -> Sequence[DiscoverySearchHistoryRecord]: ...

    async def get_intent(
        self, context: object, *, intent_id: str
    ) -> DiscoveryIntentRecord | None: ...

    async def can_read_board(
        self, context: object, *, board_id: str, user_id: str
    ) -> bool: ...


_RUNTIME_KEY = "ports.discovery_catalog.reader"


def register_discovery_catalog_read_port(reader: DiscoveryCatalogReadPort) -> None:
    register_runtime_value(_RUNTIME_KEY, reader)


def get_discovery_catalog_read_port() -> DiscoveryCatalogReadPort:
    return require_runtime_value(_RUNTIME_KEY, "discovery_catalog_read_port_not_configured")


def reset_discovery_catalog_read_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "DiscoveryCatalogReadPort",
    "DiscoveryIntentRecord",
    "DiscoverySavedSearchRecord",
    "DiscoverySearchHistoryRecord",
    "get_discovery_catalog_read_port",
    "register_discovery_catalog_read_port",
    "reset_discovery_catalog_read_port_for_tests",
]
