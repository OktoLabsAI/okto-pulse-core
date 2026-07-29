"""Persistence boundary for default board configuration templates."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


@dataclass(slots=True)
class DefaultBoardTemplateRecord:
    id: str
    version: int
    status: str
    is_active: bool
    scope: str
    # ``None`` is retained for legacy/null projections.  Writers validate new
    # versions to dictionaries, but readers must not collapse persisted null to
    # an empty template.
    settings_payload: dict[str, Any] | None
    guideline_default_refs: list[Any] | None
    design_system_default_ref: dict[str, Any] | None
    created_by: str
    # Human-owned default for the separate per-board checklist binding.  It is
    # deliberately outside settings_payload so Board.settings can never drift
    # from the binding that actually enforces Spec validation.
    spec_checklist_mode: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class DefaultGuidelineFact:
    id: str
    title: str
    scope: str
    board_id: str | None
    owner_id: str | None
    version: int | None


@dataclass(frozen=True, slots=True)
class DefaultDesignSystemFact:
    id: str
    scope: str
    board_id: str | None
    status: str


@dataclass(frozen=True, slots=True)
class DefaultBoardTemplateAudit:
    template_id: str
    template_version: int
    event_type: str
    actor_id: str
    scope: str
    payload: dict[str, Any] | None


class DefaultBoardConfigurationStore(Protocol):
    async def resolve_active(
        self, context: Any, *, scope: str
    ) -> DefaultBoardTemplateRecord | None: ...

    async def get_template(
        self, context: Any, *, template_id: str
    ) -> DefaultBoardTemplateRecord | None: ...

    async def next_version(self, context: Any, *, scope: str) -> int: ...

    async def create_template(
        self, context: Any, record: DefaultBoardTemplateRecord
    ) -> DefaultBoardTemplateRecord: ...

    async def save_template(
        self, context: Any, record: DefaultBoardTemplateRecord
    ) -> DefaultBoardTemplateRecord: ...

    async def list_active_others(
        self, context: Any, *, scope: str, exclude_template_id: str
    ) -> tuple[DefaultBoardTemplateRecord, ...]: ...

    async def list_templates(
        self, context: Any, *, scope: str
    ) -> tuple[DefaultBoardTemplateRecord, ...]: ...

    async def get_guideline(
        self, context: Any, *, guideline_id: str
    ) -> DefaultGuidelineFact | None: ...

    async def list_global_guidelines(
        self, context: Any, *, owner_id: str | None
    ) -> tuple[DefaultGuidelineFact, ...]: ...

    async def get_design_system(
        self, context: Any, *, design_system_id: str
    ) -> DefaultDesignSystemFact | None: ...

    def add_audit(
        self, context: Any, audit: DefaultBoardTemplateAudit
    ) -> None: ...


_RUNTIME_KEY = "ports.default_board_configuration.store"


def register_default_board_configuration_store(
    store: DefaultBoardConfigurationStore,
) -> None:
    register_runtime_value(_RUNTIME_KEY, store)


def get_default_board_configuration_store() -> DefaultBoardConfigurationStore:
    return require_runtime_value(_RUNTIME_KEY, "default_board_configuration_store_not_configured")


def reset_default_board_configuration_store_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "DefaultBoardConfigurationStore",
    "DefaultBoardTemplateAudit",
    "DefaultBoardTemplateRecord",
    "DefaultDesignSystemFact",
    "DefaultGuidelineFact",
    "get_default_board_configuration_store",
    "register_default_board_configuration_store",
    "reset_default_board_configuration_store_for_tests",
]
