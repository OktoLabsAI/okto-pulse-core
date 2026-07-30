"""MCP residual admin, validation and analytics use cases.

These use cases are intentionally MCP-scoped: the wrappers already resolve
agent permissions and the board id is the operational scope. The goal is to
move remaining MCP tools off direct MCP database sessions without changing
their legacy JSON envelopes or service semantics.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from datetime import datetime
from typing import Any

from okto_pulse.core.application.scope import ActorScope
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    commit,
)
from okto_pulse.core.application.use_cases.policy_governance import (
    ADOPTION_MANAGE,
    require_policy_governance_capabilities,
)
from okto_pulse.core.services.analytics_contract import parse_analytics_datetime


class _DataResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


def _parse_dt(value: str, *, end_exclusive: bool = False) -> datetime | None:
    """Compatibility wrapper over the canonical UTC half-open contract."""

    return parse_analytics_datetime(value, end_exclusive=end_exclusive)


def _query_scope_for_actor(actor: ActorContext, *, board_id: str | None = None) -> Any:
    return ActorScope.from_context(actor).query_scope(target_board_id=board_id)


# --- analytics and blockers -------------------------------------------------


class McpGetAnalyticsCommand:
    __slots__ = ("board_id", "metric_type", "from_date", "to_date")

    def __init__(
        self,
        board_id: str,
        *,
        metric_type: str = "overview",
        from_date: str = "",
        to_date: str = "",
    ) -> None:
        self.board_id = board_id
        self.metric_type = metric_type
        self.from_date = from_date
        self.to_date = to_date


class McpGetAnalyticsUseCase:
    """Legacy MCP analytics surface over the MCP UnitOfWork."""

    async def execute(
        self, command: McpGetAnalyticsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:
        return _DataResult(
            await uow.services.analytics.mcp_board_analytics(
                command.board_id,
                metric_type=command.metric_type,
                dt_from=_parse_dt(command.from_date),
                dt_to=_parse_dt(command.to_date, end_exclusive=True),
            )
        )


class McpListBlockersCommand:
    __slots__ = ("board_id", "stale_hours", "filter_type")

    def __init__(
        self, board_id: str, *, stale_hours: int = 72, filter_type: str | None = None
    ) -> None:
        self.board_id = board_id
        self.stale_hours = stale_hours
        self.filter_type = filter_type


class McpListBlockersUseCase:
    async def execute(
        self, command: McpListBlockersCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:
        data = await uow.services.analytics.blockers(
            command.board_id,
            stale_hours=command.stale_hours,
            filter_type=command.filter_type,
        )
        return _DataResult(data)


# --- default guideline and design-system admin surfaces ----------------------


class McpListDefaultGuidelineCandidatesCommand:
    __slots__ = ("board_id", "scope", "template_id")

    def __init__(
        self, board_id: str, *, scope: str = "global", template_id: str | None = None
    ) -> None:
        self.board_id = board_id
        self.scope = scope
        self.template_id = template_id


class McpListDefaultGuidelineCandidatesUseCase:
    async def execute(
        self,
        command: McpListDefaultGuidelineCandidatesCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:
        data = await uow.services.default_board_config.list_default_candidates(
            scope=command.scope,
            template_id=command.template_id,
            actor=actor.actor_id,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id),
        )
        return _DataResult(data)


class McpUpdateDefaultGuidelineRefsCommand:
    __slots__ = ("board_id", "template_id", "guideline_default_refs")

    def __init__(
        self,
        board_id: str,
        *,
        template_id: str,
        guideline_default_refs: list | None = None,
    ) -> None:
        self.board_id = board_id
        self.template_id = template_id
        self.guideline_default_refs = guideline_default_refs


class McpUpdateDefaultGuidelineRefsUseCase:
    async def execute(
        self,
        command: McpUpdateDefaultGuidelineRefsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> _DataResult:
        require_policy_governance_capabilities(actor, ADOPTION_MANAGE)
        data = await uow.services.default_board_config.update_template_guidelines(
            template_id=command.template_id,
            guideline_default_refs=command.guideline_default_refs,
            actor=actor.actor_id,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id),
        )
        await commit(uow)
        return _DataResult(data)


class McpSetDefaultDesignSystemCommand:
    __slots__ = ("template_id", "design_system_id", "gate_mode", "version", "snapshot")

    def __init__(
        self,
        *,
        template_id: str,
        design_system_id: str,
        gate_mode: str = "off",
        version: int | None = None,
        snapshot: dict | None = None,
    ) -> None:
        self.template_id = template_id
        self.design_system_id = design_system_id
        self.gate_mode = gate_mode
        self.version = version
        self.snapshot = snapshot


class McpSetDefaultDesignSystemUseCase:
    async def execute(
        self, command: McpSetDefaultDesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:

        data = await uow.services.default_board_config.set_template_design_system(
            template_id=command.template_id,
            design_system_id=command.design_system_id,
            actor=actor.actor_id,
            version=command.version,
            snapshot=command.snapshot,
            gate_mode=command.gate_mode,
        )
        await commit(uow)
        return _DataResult(data)


class McpListDesignSystemsCommand:
    __slots__ = ("board_id", "scope", "limit", "cursor", "profile")

    def __init__(
        self,
        board_id: str,
        *,
        scope: str = "global",
        limit: int = 50,
        cursor: str | None = None,
        profile: str = "summary",
    ) -> None:
        self.board_id = board_id
        self.scope = scope
        self.limit = limit
        self.cursor = cursor
        self.profile = profile


async def _require_mcp_design_system_board(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
    *,
    design_system_id: str | None = None,
) -> None:
    from okto_pulse.core.services.design_system import DesignSystemError

    if await load_accessible_board(uow, board_id, actor) is not None:
        return
    if design_system_id is not None:
        raise DesignSystemError(
            "design_system_not_found",
            f"Design System '{design_system_id}' was not found.",
            404,
            {"design_system_id": design_system_id},
        )
    raise DesignSystemError(
        "board_not_found",
        f"Board '{board_id}' not found.",
        404,
        {"board_id": board_id},
    )


class McpListDesignSystemsUseCase:
    async def execute(
        self, command: McpListDesignSystemsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:
        from okto_pulse.core.services.design_system import DesignSystemError

        await _require_mcp_design_system_board(
            uow,
            command.board_id,
            actor,
        )
        if command.profile != "summary":
            raise DesignSystemError(
                "design_system_invalid_profile",
                "Catalog lists support only profile='summary'; use get_design_system for the full payload.",
                422,
            )
        page = await uow.services.design_systems.list_catalog_page(
            scope=command.scope,
            board_id=command.board_id,
            limit=command.limit,
            cursor=command.cursor,
            owner_id=actor.actor_id if command.scope == "global" else None,
        )
        return _DataResult(page)


class McpGetDesignSystemCommand:
    __slots__ = ("board_id", "design_system_id", "profile")

    def __init__(
        self,
        board_id: str,
        design_system_id: str,
        *,
        profile: str = "full",
    ) -> None:
        self.board_id = board_id
        self.design_system_id = design_system_id
        self.profile = profile


class McpGetDesignSystemUseCase:
    async def execute(
        self, command: McpGetDesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:
        from okto_pulse.core.services.design_system import (
            serialize_design_system_profile,
        )

        await _require_mcp_design_system_board(
            uow,
            command.board_id,
            actor,
            design_system_id=command.design_system_id,
        )
        item = await uow.services.design_systems.require_authorized_design_system(
            command.design_system_id,
            actor.actor_id,
            board_id=command.board_id,
            board_access_authorized=True,
            allow_owned_global_without_link=True,
        )
        return _DataResult(
            serialize_design_system_profile(item, profile=command.profile)
        )


class McpCreateDesignSystemCommand:
    __slots__ = ("board_id", "title", "scope", "payload", "status")

    def __init__(
        self,
        board_id: str,
        *,
        title: str,
        scope: str = "global",
        payload: dict | None = None,
        status: str = "active",
    ) -> None:
        self.board_id = board_id
        self.title = title
        self.scope = scope
        self.payload = payload
        self.status = status


class McpCreateDesignSystemUseCase:
    async def execute(
        self, command: McpCreateDesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:
        from okto_pulse.core.services.design_system import (
            serialize_design_system,
        )

        await _require_mcp_design_system_board(
            uow,
            command.board_id,
            actor,
        )
        item = await uow.services.design_systems.create_design_system(
            actor.actor_id,
            title=command.title,
            scope=command.scope,
            board_id=command.board_id if command.scope == "inline" else None,
            payload=command.payload,
            status=command.status,
        )
        await commit(uow)
        return _DataResult(serialize_design_system(item))


class McpUpdateDesignSystemCommand:
    __slots__ = ("board_id", "design_system_id", "title", "payload", "status")

    def __init__(
        self,
        board_id: str,
        design_system_id: str,
        *,
        title: str | None = None,
        payload: dict | None = None,
        status: str | None = None,
    ) -> None:
        self.board_id = board_id
        self.design_system_id = design_system_id
        self.title = title
        self.payload = payload
        self.status = status


class McpUpdateDesignSystemUseCase:
    async def execute(
        self, command: McpUpdateDesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:
        from okto_pulse.core.services.design_system import (
            serialize_design_system,
        )

        await _require_mcp_design_system_board(
            uow,
            command.board_id,
            actor,
            design_system_id=command.design_system_id,
        )
        kwargs = {
            key: value
            for key, value in (
                ("title", command.title),
                ("payload", command.payload),
                ("status", command.status),
            )
            if value is not None
        }
        item = await uow.services.design_systems.update_design_system(
            command.design_system_id,
            actor.actor_id,
            board_id=command.board_id,
            board_access_authorized=True,
            allow_owned_global_without_link=True,
            **kwargs,
        )
        await commit(uow)
        return _DataResult(serialize_design_system(item))


class McpDeleteDesignSystemCommand:
    __slots__ = ("board_id", "design_system_id")

    def __init__(self, board_id: str, design_system_id: str) -> None:
        self.board_id = board_id
        self.design_system_id = design_system_id


class McpDeleteDesignSystemUseCase:
    async def execute(
        self, command: McpDeleteDesignSystemCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> _DataResult:
        await _require_mcp_design_system_board(
            uow,
            command.board_id,
            actor,
            design_system_id=command.design_system_id,
        )
        deleted = await uow.services.design_systems.delete_design_system(
            command.design_system_id,
            actor.actor_id,
            board_id=command.board_id,
            board_access_authorized=True,
            allow_owned_global_without_link=True,
        )
        if not deleted:
            return _DataResult(
                {"error": "design_system_not_found", "code": "design_system_not_found"}
            )
        await commit(uow)
        return _DataResult({"deleted": True, "id": command.design_system_id})
