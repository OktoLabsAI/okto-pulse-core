"""In-memory SaaS-shaped implementation of the relational application port.

This fake deliberately accepts an opaque session object.  It proves Core use
cases depend on the adapter contract rather than on Community, SQLAlchemy or a
Local First database lifecycle.
"""

from __future__ import annotations

import copy
import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.ports.mcp_auth import AgentAuthSession
from okto_pulse.core.ports.permission_policy import (
    PermissionPresetLineageNode,
    builtin_preset_name,
    explicit_permission_overrides,
    flatten_permission_flags,
    get_permission_flag,
    resolve_effective_permissions,
    resolve_preset_lineage,
    set_permission_flag,
)
from okto_pulse.core.ports.relational_application import (
    AgentPermissionContext,
    EffectivePermissions,
    PermissionPresetView,
)


@dataclass
class _FakeAgent:
    agent_id: str
    name: str
    api_key_hash: str
    is_active: bool = True
    permissions: Any = field(default_factory=dict)
    board_ids: set[str] = field(default_factory=set)
    preset_id: str | None = None


@dataclass
class _FakePreset:
    id: str
    owner_id: str | None
    name: str
    description: str | None
    is_builtin: bool
    base_preset_id: str | None
    flags: Any
    created_at: datetime
    updated_at: datetime


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _lineage_nodes(
    presets: list[_FakePreset] | tuple[_FakePreset, ...],
) -> tuple[PermissionPresetLineageNode, ...]:
    return tuple(
        PermissionPresetLineageNode(
            id=preset.id,
            base_preset_id=preset.base_preset_id,
            flags=copy.deepcopy(preset.flags),
        )
        for preset in presets
    )


def _preset_view(
    preset: _FakePreset,
    *,
    flags: dict[str, Any] | None = None,
    owner_review_required: bool = False,
    review_reason: str | None = None,
) -> PermissionPresetView:
    return PermissionPresetView(
        id=preset.id,
        owner_id=preset.owner_id,
        name=preset.name,
        description=preset.description,
        is_builtin=preset.is_builtin,
        base_preset_id=preset.base_preset_id,
        flags=(
            copy.deepcopy(flags)
            if flags is not None
            else copy.deepcopy(preset.flags)
            if preset.flags is not None
            else None
        ),
        owner_review_required=owner_review_required,
        review_reason=review_reason,
        created_at=preset.created_at,
        updated_at=preset.updated_at,
    )


class _FakePermissionPresetGateway:
    def __init__(self, adapter: "FakeSaaSRelationalApplicationAdapter") -> None:
        self._adapter = adapter

    async def get_effective_permissions(
        self, *, user_id: str, board_id: str
    ) -> EffectivePermissions:
        agent = self._adapter._agents.get(user_id)
        agent_flags = (
            agent.permissions
            if isinstance(getattr(agent, "permissions", None), dict)
            else None
        )
        preset = None
        preset_flags = None
        owner_review_required = False
        review_reason = None
        if agent is not None and agent.preset_id:
            preset = self._adapter._presets.get(agent.preset_id)
            lineage = resolve_preset_lineage(
                agent.preset_id,
                _lineage_nodes(list(self._adapter._presets.values())),
            )
            preset_flags = lineage.flags
            owner_review_required = lineage.owner_review_required
            review_reason = lineage.review_reason
        resolved = resolve_effective_permissions(
            agent_flags,
            preset_flags,
            None,
            owner_review_required=owner_review_required,
            review_reason=review_reason,
        )
        return EffectivePermissions(
            board_id=board_id,
            preset_name=(
                preset.name
                if preset is not None
                else builtin_preset_name(resolved.flags)
            ),
            flags=copy.deepcopy(resolved.flags),
            owner_review_required=resolved.owner_review_required,
            review_reason=resolved.review_reason,
        )

    async def list_presets(self, *, user_id: str) -> list[PermissionPresetView]:
        presets = [
            preset
            for preset in self._adapter._presets.values()
            if preset.is_builtin or preset.owner_id == user_id
        ]
        presets.sort(key=lambda preset: (not preset.is_builtin, preset.name))
        nodes = _lineage_nodes(list(self._adapter._presets.values()))
        views: list[PermissionPresetView] = []
        for preset in presets:
            lineage = resolve_preset_lineage(preset.id, nodes)
            views.append(
                _preset_view(
                    preset,
                    flags=lineage.flags,
                    owner_review_required=lineage.owner_review_required,
                    review_reason=lineage.review_reason,
                )
            )
        return views

    async def get_preset(self, *, preset_id: str) -> PermissionPresetView | None:
        preset = self._adapter._presets.get(preset_id)
        return _preset_view(preset) if preset is not None else None

    async def create_preset(
        self,
        *,
        user_id: str,
        name: str,
        description: str,
        flags: dict[str, Any] | None,
        preset_id: str | None = None,
    ) -> PermissionPresetView:
        created_at = _now()
        preset = _FakePreset(
            id=preset_id or str(uuid.uuid4()),
            owner_id=user_id,
            name=name,
            description=description or None,
            is_builtin=False,
            base_preset_id=None,
            flags=copy.deepcopy(flags) if flags is not None else {},
            created_at=created_at,
            updated_at=created_at,
        )
        self._adapter._presets[preset.id] = preset
        lineage = resolve_preset_lineage(
            preset.id,
            _lineage_nodes([preset]),
        )
        return _preset_view(preset, flags=lineage.flags)

    async def clone_preset(
        self,
        *,
        source_preset_id: str,
        user_id: str,
        name: str,
        description: str,
        flags: dict[str, Any] | None,
    ) -> PermissionPresetView | None:
        source = self._adapter._presets.get(source_preset_id)
        if source is None:
            return None
        source_lineage = resolve_preset_lineage(
            source.id,
            _lineage_nodes(list(self._adapter._presets.values())),
        )
        desired_flags = copy.deepcopy(source_lineage.flags)
        if flags is not None:
            for path in flatten_permission_flags(flags):
                value = get_permission_flag(flags, path)
                if value is not None:
                    set_permission_flag(desired_flags, path, value)
        cloned_flags = explicit_permission_overrides(
            source_lineage.flags,
            desired_flags,
        )
        created_at = _now()
        preset = _FakePreset(
            id=str(uuid.uuid4()),
            owner_id=user_id,
            name=name,
            description=description or source.description,
            is_builtin=False,
            base_preset_id=source.id,
            flags=cloned_flags,
            created_at=created_at,
            updated_at=created_at,
        )
        self._adapter._presets[preset.id] = preset
        lineage = resolve_preset_lineage(
            preset.id,
            _lineage_nodes(list(self._adapter._presets.values())),
        )
        return _preset_view(
            preset,
            flags=lineage.flags,
            owner_review_required=lineage.owner_review_required,
            review_reason=lineage.review_reason,
        )

    async def update_preset(
        self,
        *,
        preset_id: str,
        user_id: str,
        name: str | None,
        description: str | None,
        flags: dict[str, Any] | None,
        replace: bool = False,
    ) -> PermissionPresetView | None:
        preset = self._adapter._presets.get(preset_id)
        if preset is None:
            return None
        if preset.is_builtin:
            raise PermissionError("Built-in presets cannot be modified or deleted")
        if preset.owner_id != user_id:
            raise PermissionError("You can only modify your own presets")
        if name is not None:
            preset.name = name
        if replace or description is not None:
            preset.description = description
        if replace or flags is not None:
            if preset.base_preset_id is None:
                preset.flags = copy.deepcopy(flags)
            else:
                base_lineage = resolve_preset_lineage(
                    preset.base_preset_id,
                    _lineage_nodes(list(self._adapter._presets.values())),
                )
                preset.flags = explicit_permission_overrides(
                    base_lineage.flags,
                    flags,
                )
        preset.updated_at = _now()
        lineage = resolve_preset_lineage(
            preset.id,
            _lineage_nodes(list(self._adapter._presets.values())),
        )
        return _preset_view(
            preset,
            flags=lineage.flags,
            owner_review_required=lineage.owner_review_required,
            review_reason=lineage.review_reason,
        )

    async def delete_preset(self, *, preset_id: str, user_id: str) -> bool:
        preset = self._adapter._presets.get(preset_id)
        if preset is None:
            return False
        if preset.is_builtin:
            raise PermissionError("Built-in presets cannot be modified or deleted")
        if preset.owner_id != user_id:
            raise PermissionError("You can only delete your own presets")
        del self._adapter._presets[preset_id]
        return True


class _FakeAgentAuthenticationGateway:
    def __init__(self, adapter: "FakeSaaSRelationalApplicationAdapter") -> None:
        self._adapter = adapter

    async def authenticate_agent_by_api_key(
        self, api_key: str, *, credential_source: str
    ) -> AgentAuthSession | None:
        digest = hashlib.sha256(api_key.encode()).hexdigest()
        for agent in self._adapter._agents.values():
            if agent.api_key_hash == digest and agent.is_active:
                return AgentAuthSession(
                    agent_id=agent.agent_id,
                    agent_name=agent.name,
                    is_active=True,
                    metadata={"credential_source": credential_source},
                )
        return None

    async def list_accessible_board_ids_for_agent(self, agent_id: str) -> list[str]:
        agent = self._adapter._agents.get(agent_id)
        return sorted(agent.board_ids) if agent is not None and agent.is_active else []

    async def agent_has_board_access(self, agent_id: str, board_id: str) -> bool:
        agent = self._adapter._agents.get(agent_id)
        return bool(
            agent is not None and agent.is_active and board_id in agent.board_ids
        )

    async def resolve_agent_permission_context(
        self, agent_id: str, *, board_id: str | None = None
    ) -> AgentPermissionContext | None:
        agent = self._adapter._agents.get(agent_id)
        if agent is None or not agent.is_active:
            return None
        if board_id is not None and board_id not in agent.board_ids:
            return None
        preset_flags = None
        owner_review_required = False
        review_reason = None
        if agent.preset_id:
            lineage = resolve_preset_lineage(
                agent.preset_id,
                _lineage_nodes(list(self._adapter._presets.values())),
            )
            preset_flags = lineage.flags
            owner_review_required = lineage.owner_review_required
            review_reason = lineage.review_reason
        permissions = resolve_effective_permissions(
            agent.permissions if isinstance(agent.permissions, dict) else None,
            preset_flags,
            None,
            owner_review_required=owner_review_required,
            review_reason=review_reason,
        )
        return AgentPermissionContext(
            agent_id=agent.agent_id,
            agent_name=agent.name,
            permissions=permissions,
        )


class FakeSaaSRelationalApplicationAdapter:
    """Opaque-session fake representing a future tenant-aware SaaS adapter."""

    def __init__(self) -> None:
        self._agents: dict[str, _FakeAgent] = {}
        self._presets: dict[str, _FakePreset] = {}
        self._permission_gateway = _FakePermissionPresetGateway(self)
        self._authentication_gateway = _FakeAgentAuthenticationGateway(self)

    def add_agent(
        self,
        *,
        agent_id: str,
        name: str,
        api_key: str,
        board_ids: set[str] | None = None,
        permissions: Any = None,
        preset_id: str | None = None,
        is_active: bool = True,
    ) -> None:
        self._agents[agent_id] = _FakeAgent(
            agent_id=agent_id,
            name=name,
            api_key_hash=hashlib.sha256(api_key.encode()).hexdigest(),
            is_active=is_active,
            permissions=copy.deepcopy(permissions) if permissions is not None else {},
            board_ids=set(board_ids or ()),
            preset_id=preset_id,
        )

    def permission_presets(self, session: Any) -> _FakePermissionPresetGateway:
        _ = session
        return self._permission_gateway

    def quality_assessments(self, session: Any) -> Any:
        _ = session
        from okto_pulse.core.ports.quality_assessment import (
            QualityAssessmentAdapterMissing,
        )

        raise QualityAssessmentAdapterMissing(
            "The SaaS fake does not model quality-assessment persistence."
        )

    def quality_assessment_lifecycle(self, session: Any) -> Any:
        _ = session
        from okto_pulse.core.ports.quality_assessment_lifecycle import (
            AssessmentLifecycleAdapterMissing,
        )

        raise AssessmentLifecycleAdapterMissing(
            "The SaaS fake does not model quality lifecycle persistence."
        )

    def code_investigations(self, session: Any) -> Any:
        _ = session
        from okto_pulse.core.ports.code_investigation import (
            CodeInvestigationAdapterMissing,
        )

        raise CodeInvestigationAdapterMissing(
            "The SaaS fake does not model code-investigation persistence."
        )

    def code_traceability(self, session: Any) -> Any:
        _ = session
        from okto_pulse.core.ports.code_traceability import (
            CodeTraceabilityAdapterMissing,
        )

        raise CodeTraceabilityAdapterMissing(
            "The SaaS fake does not model code-traceability persistence."
        )

    def code_traceability_read(self, session: Any) -> Any:
        _ = session
        from okto_pulse.core.ports.code_traceability import (
            CodeTraceabilityAdapterMissing,
        )

        raise CodeTraceabilityAdapterMissing(
            "The SaaS fake does not model code-traceability projections."
        )

    def research_decisions(self, session: Any) -> Any:
        _ = session
        from okto_pulse.core.ports.research_decision_ledger import (
            ResearchDecisionAdapterMissing,
        )

        raise ResearchDecisionAdapterMissing(
            "The SaaS fake does not model research-decision persistence."
        )

    def spec_dependencies(self, session: Any) -> Any:
        _ = session
        raise NotImplementedError(
            "The SaaS fake does not model Spec-dependency persistence."
        )

    def checklists(self, session: Any) -> Any:
        _ = session
        from okto_pulse.core.ports.checklist import ChecklistAdapterMissing

        raise ChecklistAdapterMissing(
            "The SaaS fake does not model checklist persistence."
        )

    def guideline_policy(self, session: Any) -> Any:
        _ = session
        from okto_pulse.core.ports.guideline_policy import (
            GuidelinePolicyAdapterMissing,
        )

        raise GuidelinePolicyAdapterMissing(
            "The SaaS fake does not model guideline-policy persistence."
        )

    def semantic_guideline_assessments(self, session: Any) -> Any:
        _ = session
        from okto_pulse.core.ports.guideline_policy import (
            GuidelinePolicyAdapterMissing,
        )

        raise GuidelinePolicyAdapterMissing(
            "The SaaS fake does not model semantic-guideline persistence."
        )

    def amendment_revision_backend(self, session: Any) -> Any:
        _ = session
        raise NotImplementedError("The SaaS fake does not model amendment persistence.")

    def agent_authentication(self, session: Any) -> _FakeAgentAuthenticationGateway:
        _ = session
        return self._authentication_gateway


__all__ = ["FakeSaaSRelationalApplicationAdapter"]
