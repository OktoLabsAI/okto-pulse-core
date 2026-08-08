"""Persistence boundary for default board configuration templates."""

from __future__ import annotations

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


DEFAULT_GUIDELINE_REF_NATIVE_FIELDS = frozenset(
    {
        "guideline_id",
        "priority",
        "revision_id",
        "revision_number",
        "semantic_version",
        "revision_digest",
    }
)
"""Closed native payload written by current default-template mutations."""

DEFAULT_GUIDELINE_REF_COMPATIBILITY_FIELDS = frozenset(
    {
        "guideline_version",
        "legacy_version",
        "legacy_version_unresolvable",
    }
)
"""Read/migration aliases accepted without widening the native contract."""

DEFAULT_GUIDELINE_REF_ALLOWED_FIELDS = (
    DEFAULT_GUIDELINE_REF_NATIVE_FIELDS
    | DEFAULT_GUIDELINE_REF_COMPATIBILITY_FIELDS
)


@dataclass(frozen=True, slots=True)
class DefaultGuidelineRevisionRef:
    """Canonical immutable revision pin stored by a default template.

    Native writes provide the complete pin explicitly; they never select the
    current head implicitly. Compatibility/import aliases are deliberately not
    part of this native contract.
    """

    guideline_id: str
    priority: int
    revision_id: str
    revision_number: int
    semantic_version: str
    revision_digest: str

    def to_dict(self) -> dict[str, str | int]:
        return {
            "guideline_id": self.guideline_id,
            "priority": self.priority,
            "revision_id": self.revision_id,
            "revision_number": self.revision_number,
            "semantic_version": self.semantic_version,
            "revision_digest": self.revision_digest,
        }


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
    revision_id: str | None = None
    semantic_version: str | None = None
    revision_digest: str | None = None
    revision_number: int | None = None
    retired: bool = False


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

    async def get_guideline_revision(
        self,
        context: Any,
        *,
        guideline_id: str,
        revision_id: str | None = None,
        revision_number: int | None = None,
    ) -> DefaultGuidelineFact | None:
        """Resolve one immutable revision of a guideline identity.

        Exactly one selector is supplied by the service.  This lookup is
        deliberately separate from ``get_guideline`` (the current head) so a
        copied or imported default template can preserve a historical pin
        instead of silently drifting to the latest revision.
        """

        ...

    async def list_global_guidelines(
        self, context: Any, *, owner_id: str | None
    ) -> tuple[DefaultGuidelineFact, ...]: ...

    async def get_design_system(
        self, context: Any, *, design_system_id: str
    ) -> DefaultDesignSystemFact | None: ...

    def add_audit(self, context: Any, audit: DefaultBoardTemplateAudit) -> None: ...


_RUNTIME_KEY = "ports.default_board_configuration.store"


def register_default_board_configuration_store(
    store: DefaultBoardConfigurationStore,
) -> None:
    register_runtime_value(_RUNTIME_KEY, store)


def get_default_board_configuration_store() -> DefaultBoardConfigurationStore:
    return require_runtime_value(
        _RUNTIME_KEY, "default_board_configuration_store_not_configured"
    )


def reset_default_board_configuration_store_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "DEFAULT_GUIDELINE_REF_ALLOWED_FIELDS",
    "DEFAULT_GUIDELINE_REF_COMPATIBILITY_FIELDS",
    "DEFAULT_GUIDELINE_REF_NATIVE_FIELDS",
    "DefaultBoardConfigurationStore",
    "DefaultBoardTemplateAudit",
    "DefaultBoardTemplateRecord",
    "DefaultDesignSystemFact",
    "DefaultGuidelineFact",
    "DefaultGuidelineRevisionRef",
    "get_default_board_configuration_store",
    "register_default_board_configuration_store",
    "reset_default_board_configuration_store_for_tests",
]
