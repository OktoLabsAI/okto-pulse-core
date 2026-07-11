"""Persistence boundary for spec resource propagation."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from typing import Any, Protocol, Sequence


@dataclass(frozen=True, slots=True)
class ResourcePropagationBoardFact:
    id: str
    settings: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ResourcePropagationSpecFact:
    id: str
    board_id: str
    screen_mockups: tuple[Any, ...]


@dataclass(slots=True)
class ResourcePropagationCardRecord:
    id: str
    board_id: str
    knowledge_bases: list[Any]
    screen_mockups: list[Any]


@dataclass(frozen=True, slots=True)
class ResourcePropagationKnowledgeBaseFact:
    id: str
    title: str
    description: str | None
    content: str
    mime_type: str


class SpecResourcePropagationStore(Protocol):
    async def get_board(
        self, context: Any, *, board_id: str
    ) -> ResourcePropagationBoardFact | None: ...

    async def get_spec(
        self, context: Any, *, spec_id: str
    ) -> ResourcePropagationSpecFact | None: ...

    async def get_card(
        self, context: Any, *, card_id: str
    ) -> ResourcePropagationCardRecord | None: ...

    async def list_card_ids(
        self, context: Any, *, board_id: str, spec_id: str
    ) -> tuple[str, ...]: ...

    async def list_spec_ids(
        self, context: Any, *, board_id: str
    ) -> tuple[str, ...]: ...

    async def list_spec_knowledge_bases(
        self, context: Any, *, spec_id: str
    ) -> tuple[ResourcePropagationKnowledgeBaseFact, ...]: ...

    async def save_card(
        self,
        context: Any,
        record: ResourcePropagationCardRecord,
        *,
        changed_fields: Sequence[str],
    ) -> None: ...

    async def record_audit(
        self,
        context: Any,
        *,
        board_id: str,
        card_id: str,
        actor_id: str,
        details: dict[str, Any],
    ) -> None: ...


_RUNTIME_KEY = "ports.spec_resource_propagation.store"


def register_spec_resource_propagation_store(
    store: SpecResourcePropagationStore,
) -> None:
    register_runtime_value(_RUNTIME_KEY, store)


def get_spec_resource_propagation_store() -> SpecResourcePropagationStore:
    return require_runtime_value(_RUNTIME_KEY, "spec_resource_propagation_store_not_configured")


def reset_spec_resource_propagation_store_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ResourcePropagationBoardFact",
    "ResourcePropagationCardRecord",
    "ResourcePropagationKnowledgeBaseFact",
    "ResourcePropagationSpecFact",
    "SpecResourcePropagationStore",
    "get_spec_resource_propagation_store",
    "register_spec_resource_propagation_store",
    "reset_spec_resource_propagation_store_for_tests",
]
