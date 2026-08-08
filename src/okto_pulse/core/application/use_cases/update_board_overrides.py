"""update_board_overrides use case (SaaS Refactor spec R01A IMP4, REST write).

Behavior-preserving, transport-free reimplementation of the owner-only
``PATCH /api/v1/agents/{agent_id}/boards/{board_id}``
(``api/agents.py:update_board_overrides``). It proves ownership of both the
agent and board before delegating to ``AgentService``; board-access-not-found,
the override update, commit and returned ``AgentBoard`` remain unchanged.

As with ``update_agent``, the MCP permission-cache invalidation
(``invalidate_agent_cache``) stays in the REST adapter AFTER success — this use
case does NOT touch it, so the proven invalidation point is preserved exactly
(ac_8e695cf2). Public signature is transport-neutral (no ``AsyncSession``).
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)


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
        self, command: UpdateBoardOverridesCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> UpdateBoardOverridesResult:
        board = await load_accessible_board(
            uow,
            command.board_id,
            actor,
            allowed_share_permissions=(),
        )
        if board is None or getattr(board, "owner_id", None) != actor.actor_id:
            raise EntityNotFoundError("board", command.board_id)

        service = uow.services.agents
        agent = await service.get_agent(command.agent_id)
        if not agent or agent.created_by != actor.actor_id:
            raise EntityNotFoundError("agent", command.agent_id)

        await require_authorization(
            actor,
            PermissionRequirement(
                "agent.board_access.edit",
                legacy_operation="board.read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        agent_board = await service.update_board_overrides(
            command.agent_id, command.board_id, command.permission_overrides
        )
        if not agent_board:
            raise EntityNotFoundError("board_access", command.board_id)
        await commit(uow)
        return UpdateBoardOverridesResult(agent_board=agent_board)
