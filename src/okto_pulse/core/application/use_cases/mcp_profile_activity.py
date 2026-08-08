"""MCP profile, mention and activity use cases for AF35-S4.

The MCP wrapper remains responsible for authentication, permission checks,
parameter coercion and JSON envelope mapping. These use cases own the relational
work through the MCP UnitOfWork path so the tool bodies do not open
``get_db_for_mcp`` directly.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from datetime import datetime
from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.services.analytics_contract import encode_activity_cursor


class McpUpdateMyProfileCommand:
    __slots__ = ("description", "objective")

    def __init__(
        self,
        *,
        description: str | None = None,
        objective: str | None = None,
    ) -> None:
        self.description = description
        self.objective = objective


class McpUpdateMyProfileResult:
    __slots__ = ("agent",)

    def __init__(self, agent: Any) -> None:
        self.agent = agent


class McpUpdateMyProfileUseCase:
    async def execute(
        self,
        command: McpUpdateMyProfileCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpUpdateMyProfileResult:

        service = uow.services.agents
        agent = await service.get_agent(actor.actor_id)
        if not agent:
            raise EntityNotFoundError("agent", actor.actor_id)

        # ``None`` means omitted while an empty string is an intentional clear.
        # MCP callers therefore can round-trip a reversible profile mutation.
        if command.description is not None:
            agent.description = command.description
        if command.objective is not None:
            agent.objective = command.objective

        await commit(uow)
        return McpUpdateMyProfileResult(agent)


class McpListMyBoardsCommand:
    __slots__ = ()


class McpListMyBoardsResult:
    __slots__ = ("boards",)

    def __init__(self, boards: list[Any]) -> None:
        self.boards = boards


class McpListMyBoardsUseCase:
    async def execute(
        self,
        command: McpListMyBoardsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpListMyBoardsResult:

        boards = await uow.services.agents.list_boards_for_agent(actor.actor_id)
        await commit(uow)
        return McpListMyBoardsResult(boards)


class McpListMyMentionsCommand:
    __slots__ = ("board_id", "include_seen")

    def __init__(self, board_id: str, *, include_seen: bool = False) -> None:
        self.board_id = board_id
        self.include_seen = include_seen


class McpListMyMentionsResult:
    __slots__ = ("mentions", "show_all", "unseen_count")

    def __init__(self, mentions: list[dict[str, Any]], *, show_all: bool) -> None:
        self.mentions = mentions
        self.show_all = show_all
        self.unseen_count = sum(
            1 for mention in mentions if not bool(mention.get("seen", False))
        )


class McpListMyMentionsUseCase:
    async def execute(
        self,
        command: McpListMyMentionsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpListMyMentionsResult:
        mentions, show_all = await uow.services.list_my_mentions(
            board_id=command.board_id,
            agent_id=actor.actor_id,
            agent_name=actor.actor_name,
            include_seen=command.include_seen,
        )
        await commit(uow)
        return McpListMyMentionsResult(mentions, show_all=show_all)


class McpMarkMentionsSeenCommand:
    __slots__ = ("board_id", "item_ids")

    def __init__(self, board_id: str, item_ids: list[str]) -> None:
        self.board_id = board_id
        self.item_ids = item_ids


class McpMarkMentionsSeenResult:
    __slots__ = ("marked_count", "total_requested")

    def __init__(self, *, marked_count: int, total_requested: int) -> None:
        self.marked_count = marked_count
        self.total_requested = total_requested


class McpMarkMentionsSeenUseCase:
    async def execute(
        self,
        command: McpMarkMentionsSeenCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpMarkMentionsSeenResult:
        marked, total = await uow.services.mark_mentions_seen(
            board_id=command.board_id,
            agent_id=actor.actor_id,
            agent_name=actor.actor_name,
            item_ids=command.item_ids,
        )
        await commit(uow)

        return McpMarkMentionsSeenResult(marked_count=marked, total_requested=total)


class McpGetUnseenSummaryCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class McpGetUnseenSummaryResult:
    __slots__ = ("payload",)

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload


class McpGetUnseenSummaryUseCase:
    async def execute(
        self,
        command: McpGetUnseenSummaryCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpGetUnseenSummaryResult:
        payload = await uow.services.get_unseen_summary(
            board_id=command.board_id,
            agent_id=actor.actor_id,
            agent_name=actor.actor_name,
        )
        await commit(uow)
        return McpGetUnseenSummaryResult(payload)


class McpListAgentsCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class McpListAgentsResult:
    __slots__ = ("agents",)

    def __init__(self, agents: list[Any]) -> None:
        self.agents = agents


class McpListAgentsUseCase:
    async def execute(
        self,
        command: McpListAgentsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpListAgentsResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "agent.entity.read",
                legacy_operation="board.read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        agents = await uow.services.agents.list_agents(command.board_id)
        await commit(uow)
        return McpListAgentsResult(agents)


class McpGetActivityLogCommand:
    __slots__ = (
        "board_id",
        "limit",
        "cursor_pair",
        "effective_offset",
        "action",
        "card_id",
        "include_details",
    )

    def __init__(
        self,
        board_id: str,
        *,
        limit: int,
        cursor_pair: tuple[datetime, str] | None,
        effective_offset: int,
        action: str = "",
        card_id: str = "",
        include_details: bool = False,
    ) -> None:
        self.board_id = board_id
        self.limit = limit
        self.cursor_pair = cursor_pair
        self.effective_offset = effective_offset
        self.action = action
        self.card_id = card_id
        self.include_details = include_details


class McpGetActivityLogResult:
    __slots__ = ("rows", "next_cursor")

    def __init__(self, *, rows: list[dict[str, Any]], next_cursor: str | None) -> None:
        self.rows = rows
        self.next_cursor = next_cursor


def _encode_activity_cursor(
    created_at: datetime, row_id: str, *, board_id: str = ""
) -> str:
    return encode_activity_cursor(created_at, row_id, board_id=board_id)


class McpGetActivityLogUseCase:
    async def execute(
        self,
        command: McpGetActivityLogCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpGetActivityLogResult:
        rows, next_cursor_pair = await uow.services.get_activity_log_rows(
            board_id=command.board_id,
            limit=command.limit,
            cursor_pair=command.cursor_pair,
            effective_offset=command.effective_offset,
            action=command.action,
            card_id=command.card_id,
            include_details=command.include_details,
        )
        await commit(uow)

        next_cursor: str | None = None
        if next_cursor_pair:
            next_cursor = encode_activity_cursor(
                *next_cursor_pair,
                board_id=command.board_id,
                action=command.action,
                card_id=command.card_id,
            )

        return McpGetActivityLogResult(rows=rows, next_cursor=next_cursor)
