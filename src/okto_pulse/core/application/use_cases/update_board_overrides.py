"""update_board_overrides use case (SaaS Refactor spec R01A IMP4, REST write).

Behavior-preserving, transport-free reimplementation of the owner-only
``PATCH /api/v1/agents/{agent_id}/boards/{board_id}``
(``api/agents.py:update_board_overrides``). Delegates to the existing
``AgentService`` so the ownership check (404 when not owner), the
board-access-not-found check (404), the override update, the commit and the
returned ``AgentBoard`` are unchanged.

As with ``update_agent``, the MCP permission-cache invalidation
(``invalidate_agent_cache``) stays in the REST adapter AFTER success — this use
case does NOT touch it, so the proven invalidation point is preserved exactly
(ac_8e695cf2). Public signature is transport-neutral (no ``AsyncSession``).
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
    session_of,
)
from okto_pulse.core.services import AgentService


class UpdateBoardOverridesCommand:
    """Input for :class:`UpdateBoardOverridesUseCase`."""

    __slots__ = ("agent_id", "board_id", "permission_overrides")

    def __init__(
        self, agent_id: str, board_id: str, permission_overrides: Any
    ) -> None:
        self.agent_id = agent_id
        self.board_id = board_id
        self.permission_overrides = permission_overrides


class UpdateBoardOverridesResult:
    """Output — the updated ``AgentBoard``, ready for ``AgentBoardResponse``."""

    __slots__ = ("agent_board",)

    def __init__(self, agent_board: Any) -> None:
        self.agent_board = agent_board


class UpdateBoardOverridesUseCase:
    """Update an agent's per-board permission overrides, transport-free.

    Raises :class:`EntityNotFoundError` with ``entity_type='agent'`` when the
    agent is missing/not owned, or ``entity_type='board_access'`` when the agent
    has no access row on the board — the REST adapter maps both to the legacy
    404s with their respective details.
    """

    async def execute(
        self, command: UpdateBoardOverridesCommand, *, actor: ActorContext, uow: Any
    ) -> UpdateBoardOverridesResult:
        session = session_of(uow)
        service = AgentService(session)
        agent = await service.get_agent(command.agent_id)
        if not agent or agent.created_by != actor.actor_id:
            raise EntityNotFoundError("agent", command.agent_id)

        agent_board = await service.update_board_overrides(
            command.agent_id, command.board_id, command.permission_overrides
        )
        if not agent_board:
            raise EntityNotFoundError("board_access", command.board_id)
        await commit(uow)
        return UpdateBoardOverridesResult(agent_board=agent_board)
