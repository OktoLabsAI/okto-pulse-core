"""Edition-owned relational application adapters.

The Core owns the commands, state gates and DTOs below. An edition owns the
SQL/ORM work needed to satisfy them and registers one adapter at composition
time. This keeps application services from reaching into a concrete adapter.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from .mcp_auth import AgentAuthSession

if TYPE_CHECKING:
    from .checklist import ChecklistPersistencePort
    from .guideline_policy import (
        GuidelinePolicyPersistencePort,
        SemanticGuidelineAssessmentPersistencePort,
    )
    from .quality_assessment import QualityAssessmentPersistencePort
    from .quality_assessment_lifecycle import (
        QualityAssessmentLifecyclePersistencePort,
    )
    from .research_decision_ledger import ResearchDecisionLedgerPersistencePort


class RelationalApplicationAdapterMissing(RuntimeError):
    """Raised when a relational application adapter was not composed."""


@dataclass(frozen=True)
class EffectivePermissions:
    """Transport-neutral effective permissions for one board."""

    board_id: str
    preset_name: str | None
    flags: dict[str, Any]
    owner_review_required: bool = False
    review_reason: str | None = None


@dataclass(frozen=True)
class PermissionPresetView:
    """DTO returned by the permission-preset persistence port."""

    id: str
    owner_id: str | None
    name: str
    description: str | None
    is_builtin: bool
    base_preset_id: str | None
    flags: dict[str, Any] | None
    owner_review_required: bool = False
    review_reason: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class AgentPermissionContext:
    """Resolved agent identity plus the effective MCP permission set."""

    agent_id: str
    agent_name: str
    permissions: Any


@runtime_checkable
class PermissionPresetGateway(Protocol):
    """Persistence operations used by the preset application use cases."""

    async def get_effective_permissions(
        self, *, user_id: str, board_id: str
    ) -> EffectivePermissions: ...

    async def list_presets(self, *, user_id: str) -> list[PermissionPresetView]: ...

    async def create_preset(
        self,
        *,
        user_id: str,
        name: str,
        description: str,
        flags: dict[str, Any] | None,
    ) -> PermissionPresetView: ...

    async def clone_preset(
        self,
        *,
        source_preset_id: str,
        user_id: str,
        name: str,
        description: str,
        flags: dict[str, Any] | None,
    ) -> PermissionPresetView | None: ...

    async def update_preset(
        self,
        *,
        preset_id: str,
        user_id: str,
        name: str | None,
        description: str | None,
        flags: dict[str, Any] | None,
    ) -> PermissionPresetView | None: ...

    async def delete_preset(self, *, preset_id: str, user_id: str) -> bool: ...


@runtime_checkable
class AgentAuthenticationGateway(Protocol):
    """Edition-owned credential and board-access lookup operations."""

    async def authenticate_agent_by_api_key(
        self, api_key: str, *, credential_source: str
    ) -> AgentAuthSession | None: ...

    async def list_accessible_board_ids_for_agent(self, agent_id: str) -> list[str]: ...

    async def agent_has_board_access(self, agent_id: str, board_id: str) -> bool: ...

    async def resolve_agent_permission_context(
        self, agent_id: str, *, board_id: str | None = None
    ) -> AgentPermissionContext | None: ...


@runtime_checkable
class RelationalApplicationAdapter(Protocol):
    """Factory bundle scoped to one edition-owned persistence session."""

    def permission_presets(self, session: Any) -> PermissionPresetGateway: ...

    def quality_assessments(
        self,
        session: Any,
    ) -> "QualityAssessmentPersistencePort":
        """Return the transaction-bound SK-A assessment persistence port."""
        ...

    def quality_assessment_lifecycle(
        self,
        session: Any,
    ) -> "QualityAssessmentLifecyclePersistencePort":
        """Return the transaction-bound SK-A lifecycle and purge port."""
        ...

    def checklists(self, session: Any) -> "ChecklistPersistencePort":
        """Return the transaction-bound A3 checklist persistence port."""
        ...

    def research_decisions(
        self,
        session: Any,
    ) -> "ResearchDecisionLedgerPersistencePort":
        """Return the transaction-bound SK-A RDL persistence port."""
        ...

    def guideline_policy(
        self,
        session: Any,
    ) -> "GuidelinePolicyPersistencePort":
        """Return the transaction-bound SK-B guideline policy authority."""

        ...

    def semantic_guideline_assessments(
        self,
        session: Any,
    ) -> "SemanticGuidelineAssessmentPersistencePort":
        """Return the transaction-bound SK-B3 evidence authority."""

        ...

    def amendment_revision_backend(self, session: Any) -> Any: ...

    def agent_authentication(self, session: Any) -> AgentAuthenticationGateway: ...


_RUNTIME_KEY = "ports.relational_application.adapter"


def register_relational_application_adapter(
    adapter: RelationalApplicationAdapter,
) -> None:
    """Register the relational adapter bundle selected by the edition."""

    register_runtime_value(_RUNTIME_KEY, adapter)


def reset_relational_application_adapter_for_tests() -> None:
    """Clear the registered bundle for isolated tests."""

    reset_runtime_values(_RUNTIME_KEY)


def require_relational_application_adapter() -> RelationalApplicationAdapter:
    """Resolve the composed adapter bundle, failing before an operation starts."""

    adapter = resolve_runtime_value(_RUNTIME_KEY)
    if adapter is None:
        raise RelationalApplicationAdapterMissing(
            "No relational application adapter is registered. The edition "
            "composition root must register one before relational application "
            "commands are executed."
        )
    return adapter


__all__ = [
    "AgentAuthenticationGateway",
    "AgentPermissionContext",
    "EffectivePermissions",
    "PermissionPresetGateway",
    "PermissionPresetView",
    "RelationalApplicationAdapter",
    "RelationalApplicationAdapterMissing",
    "register_relational_application_adapter",
    "reset_relational_application_adapter_for_tests",
    "require_relational_application_adapter",
]
