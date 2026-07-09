"""MCP residual admin, validation and analytics use cases.

These use cases are intentionally MCP-scoped: the wrappers already resolve
agent permissions and the board id is the operational scope. The goal is to
move remaining MCP tools off direct MCP database sessions without changing
their legacy JSON envelopes or service semantics.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.application.scope import ActorScope
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    commit,
    session_of,
)


class _DataResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


def _parse_dt(value: str) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (TypeError, ValueError):
        return None


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
        self, command: McpGetAnalyticsCommand, *, actor: ActorContext, uow: Any
    ) -> _DataResult:
        from okto_pulse.core.services.analytics_service import compute_mcp_board_analytics

        return _DataResult(
            await compute_mcp_board_analytics(
                session_of(uow),
                command.board_id,
                metric_type=command.metric_type,
                dt_from=_parse_dt(command.from_date),
                dt_to=_parse_dt(command.to_date),
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
        self, command: McpListBlockersCommand, *, actor: ActorContext, uow: Any
    ) -> _DataResult:
        from okto_pulse.core.services.analytics_service import compute_blockers

        data = await compute_blockers(
            session_of(uow),
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
        uow: Any,
    ) -> _DataResult:
        from okto_pulse.core.services.default_board_config_api import (
            DefaultBoardConfigApiService,
        )

        data = await DefaultBoardConfigApiService(
            session_of(uow)
        ).list_default_candidates(
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
        uow: Any,
    ) -> _DataResult:
        from okto_pulse.core.services.default_board_config_api import (
            DefaultBoardConfigApiService,
        )

        data = await DefaultBoardConfigApiService(session_of(uow)).update_template_guidelines(
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
        self, command: McpSetDefaultDesignSystemCommand, *, actor: ActorContext, uow: Any
    ) -> _DataResult:
        from okto_pulse.core.services.default_board_config_api import (
            DefaultBoardConfigApiService,
        )

        data = await DefaultBoardConfigApiService(session_of(uow)).set_template_design_system(
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
    __slots__ = ("board_id", "scope")

    def __init__(self, board_id: str, *, scope: str = "global") -> None:
        self.board_id = board_id
        self.scope = scope


class McpListDesignSystemsUseCase:
    async def execute(
        self, command: McpListDesignSystemsCommand, *, actor: ActorContext, uow: Any
    ) -> _DataResult:
        from okto_pulse.core.services.design_system import (
            DesignSystemService,
            serialize_design_system,
        )

        items = await DesignSystemService(session_of(uow)).list_catalog(
            scope=command.scope, board_id=command.board_id
        )
        return _DataResult([serialize_design_system(item) for item in items])


class McpGetDesignSystemCommand:
    __slots__ = ("design_system_id",)

    def __init__(self, design_system_id: str) -> None:
        self.design_system_id = design_system_id


class McpGetDesignSystemUseCase:
    async def execute(
        self, command: McpGetDesignSystemCommand, *, actor: ActorContext, uow: Any
    ) -> _DataResult:
        from okto_pulse.core.services.design_system import (
            DesignSystemService,
            serialize_design_system,
        )

        item = await DesignSystemService(session_of(uow)).require_design_system(
            command.design_system_id
        )
        return _DataResult(serialize_design_system(item))


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
        self, command: McpCreateDesignSystemCommand, *, actor: ActorContext, uow: Any
    ) -> _DataResult:
        from okto_pulse.core.services.design_system import (
            DesignSystemService,
            serialize_design_system,
        )

        item = await DesignSystemService(session_of(uow)).create_design_system(
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
    __slots__ = ("design_system_id", "title", "payload", "status")

    def __init__(
        self,
        design_system_id: str,
        *,
        title: str | None = None,
        payload: dict | None = None,
        status: str | None = None,
    ) -> None:
        self.design_system_id = design_system_id
        self.title = title
        self.payload = payload
        self.status = status


class McpUpdateDesignSystemUseCase:
    async def execute(
        self, command: McpUpdateDesignSystemCommand, *, actor: ActorContext, uow: Any
    ) -> _DataResult:
        from okto_pulse.core.services.design_system import (
            DesignSystemService,
            serialize_design_system,
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
        item = await DesignSystemService(session_of(uow)).update_design_system(
            command.design_system_id, actor.actor_id, **kwargs
        )
        await commit(uow)
        return _DataResult(serialize_design_system(item))


class McpDeleteDesignSystemCommand:
    __slots__ = ("design_system_id",)

    def __init__(self, design_system_id: str) -> None:
        self.design_system_id = design_system_id


class McpDeleteDesignSystemUseCase:
    async def execute(
        self, command: McpDeleteDesignSystemCommand, *, actor: ActorContext, uow: Any
    ) -> _DataResult:
        from okto_pulse.core.services.design_system import DesignSystemService

        deleted = await DesignSystemService(session_of(uow)).delete_design_system(
            command.design_system_id, actor.actor_id
        )
        if not deleted:
            return _DataResult(
                {"error": "design_system_not_found", "code": "design_system_not_found"}
            )
        await commit(uow)
        return _DataResult({"deleted": True, "id": command.design_system_id})
