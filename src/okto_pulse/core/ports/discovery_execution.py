"""Persistence-neutral reads used by discovery intent execution."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, Sequence


@dataclass(frozen=True, slots=True)
class DiscoverySpecFact:
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
    test_scenarios: tuple[Any, ...]
    skip_rules_coverage: bool
    skip_test_coverage: bool
    skip_trs_coverage: bool
    skip_contract_coverage: bool
    skip_ir_coverage: bool
    skip_or_coverage: bool
    skip_decisions_coverage: bool
    skip_code_evidence_coverage: bool


@dataclass(frozen=True, slots=True)
class DiscoveryCardFact:
    id: str
    board_id: str
    title: str
    status: Any
    priority: Any
    spec_id: str | None
    sprint_id: str | None
    archived: bool
    updated_at: datetime | None


@dataclass(frozen=True, slots=True)
class DiscoverySprintFact:
    id: str
    board_id: str
    title: str
    status: Any


@dataclass(frozen=True, slots=True)
class DiscoveryDependencyFact:
    card_id: str
    depends_on_id: str
    created_at: datetime | None


@dataclass(frozen=True, slots=True)
class DiscoveryActivityFact:
    id: str
    action: str
    details: dict[str, Any]
    card_id: str | None
    actor_id: str
    actor_type: str
    actor_name: str | None
    created_at: datetime | None


@dataclass(frozen=True, slots=True)
class DiscoveryDependentCardFact:
    dependency: DiscoveryDependencyFact
    card: DiscoveryCardFact


@dataclass(frozen=True, slots=True)
class DiscoveryMentionFact:
    id: str
    content: str
    author_id: str
    created_at: datetime | None
    card: DiscoveryCardFact


class DiscoveryExecutionReadPort(Protocol):
    async def get_spec_by_id(
        self, context: Any, *, spec_id: str
    ) -> DiscoverySpecFact | None: ...

    async def get_card_by_id(
        self, context: Any, *, card_id: str
    ) -> DiscoveryCardFact | None: ...

    async def list_recent_activity(
        self, context: Any, *, board_id: str, limit: int
    ) -> tuple[DiscoveryActivityFact, ...]: ...

    async def resolve_entity_titles(
        self,
        context: Any,
        *,
        refs: Sequence[tuple[str, str]],
    ) -> dict[tuple[str, str], str]: ...

    async def list_sprints(
        self, context: Any, *, board_id: str
    ) -> tuple[DiscoverySprintFact, ...]: ...

    async def list_cards_for_sprints(
        self,
        context: Any,
        *,
        board_id: str,
        sprint_ids: Sequence[str],
    ) -> tuple[DiscoveryCardFact, ...]: ...

    async def list_dependencies_for_cards(
        self, context: Any, *, card_ids: Sequence[str]
    ) -> tuple[DiscoveryDependencyFact, ...]: ...

    async def list_cards_by_ids(
        self, context: Any, *, card_ids: Sequence[str]
    ) -> tuple[DiscoveryCardFact, ...]: ...

    async def list_card_dependents(
        self, context: Any, *, card_id: str
    ) -> tuple[DiscoveryDependentCardFact, ...]: ...

    async def list_mentions(
        self,
        context: Any,
        *,
        board_id: str,
        mention_token: str,
        limit: int,
    ) -> tuple[DiscoveryMentionFact, ...]: ...

    async def list_specs(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> tuple[DiscoverySpecFact, ...]: ...

    async def get_board_settings(
        self, context: Any, *, board_id: str
    ) -> dict[str, Any]: ...

    async def list_board_cards(
        self, context: Any, *, board_id: str
    ) -> tuple[DiscoveryCardFact, ...]: ...


_RUNTIME_KEY = "ports.discovery_execution.reader"


def register_discovery_execution_read_port(
    reader: DiscoveryExecutionReadPort,
) -> None:
    register_runtime_value(_RUNTIME_KEY, reader)


def get_discovery_execution_read_port() -> DiscoveryExecutionReadPort:
    return require_runtime_value(_RUNTIME_KEY, "discovery_execution_read_port_not_configured")


def reset_discovery_execution_read_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "DiscoveryActivityFact",
    "DiscoveryCardFact",
    "DiscoveryDependencyFact",
    "DiscoveryDependentCardFact",
    "DiscoveryExecutionReadPort",
    "DiscoveryMentionFact",
    "DiscoverySpecFact",
    "DiscoverySprintFact",
    "get_discovery_execution_read_port",
    "register_discovery_execution_read_port",
    "reset_discovery_execution_read_port_for_tests",
]
