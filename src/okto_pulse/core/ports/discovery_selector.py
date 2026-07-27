"""Persistence-neutral discovery selector read boundary."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class SelectorSpecFact:
    id: str
    board_id: str
    title: str
    status: Any
    version: int | str | None
    functional_requirements: tuple[Any, ...]
    business_rules: tuple[Any, ...]
    technical_requirements: tuple[Any, ...]
    decisions: tuple[Any, ...]
    acceptance_criteria: tuple[Any, ...]
    api_contracts: tuple[Any, ...]
    integration_requirements: tuple[Any, ...]
    observability_requirements: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class SelectorCardFact:
    id: str
    board_id: str
    title: str
    status: Any
    priority: Any
    card_type: Any
    spec_id: str | None
    sprint_id: str | None
    position: int | None


class DiscoverySelectorReadPort(Protocol):
    async def list_specs(
        self,
        context: Any,
        *,
        board_id: str,
        status: str | None,
    ) -> tuple[SelectorSpecFact, ...]: ...

    async def list_cards(
        self,
        context: Any,
        *,
        board_id: str,
        status: str | None,
    ) -> tuple[SelectorCardFact, ...]: ...

    async def get_spec(
        self,
        context: Any,
        *,
        board_id: str,
        spec_id: str,
    ) -> SelectorSpecFact | None: ...


_RUNTIME_KEY = "ports.discovery_selector.reader"


def register_discovery_selector_read_port(reader: DiscoverySelectorReadPort) -> None:
    register_runtime_value(_RUNTIME_KEY, reader)


def get_discovery_selector_read_port() -> DiscoverySelectorReadPort:
    return require_runtime_value(_RUNTIME_KEY, "discovery_selector_read_port_not_configured")


def reset_discovery_selector_read_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "DiscoverySelectorReadPort",
    "SelectorCardFact",
    "SelectorSpecFact",
    "get_discovery_selector_read_port",
    "register_discovery_selector_read_port",
    "reset_discovery_selector_read_port_for_tests",
]
