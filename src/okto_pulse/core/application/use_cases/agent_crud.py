"""Agent CRUD + board-access use cases (SaaS Refactor spec R01A REST-FU1).

Transport-free reimplementations of the remaining ``api/agents.py`` endpoints
that still opened a raw ``AsyncSession``/``get_db`` — create / list (user+board) /
get / regenerate-key / delete / grant / revoke. Each uses the UoW repositories
and ``AgentService`` while preserving structured errors, commit/refetch and the
transaction; the REST adapter maps transport-neutral errors back to 404/409.
Grant administration explicitly requires ownership of both the agent and board.

Cache invalidation is intentionally NOT added here: only ``update_agent`` and
``update_board_overrides`` are proven invalidation points (ac_8e695cf2). Grant /
revoke / delete must stay WITHOUT ``invalidate_agent_cache`` — see
[[feedback_register_critical_mutation_governance]] and the TEST-TEETH 1 guard.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    ConflictError,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)


async def _require_owned_board(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
) -> Any:
    """Return an actor-owned board; shares never authorize grant management."""
    board = await load_accessible_board(
        uow,
        board_id,
        actor,
        allowed_share_permissions=(),
    )
    if board is None or getattr(board, "owner_id", None) != actor.actor_id:
        raise EntityNotFoundError("board", board_id)
    return board


# --- create -----------------------------------------------------------------


class CreateAgentCommand:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class CreateAgentResult:
    __slots__ = ("agent", "reveal_once_secret")

    def __init__(self, agent: Any, reveal_once_secret: str) -> None:
        self.agent = agent
        self.reveal_once_secret = reveal_once_secret


class CreateAgentUseCase:
    """Create a global agent owned by the actor (write — commits, then refetches)."""

    async def execute(
        self, command: CreateAgentCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CreateAgentResult:
        service = uow.services.agents
        await require_authorization(
            actor,
            PermissionRequirement(
                "agent.entity.create",
                legacy_operation="profile.update",
            ),
            uow=uow,
        )
        agent, reveal_once_secret = await service.create_agent(actor.actor_id, command.data)
        await commit(uow)
        refetched = await service.get_agent(agent.id)
        return CreateAgentResult(agent=refetched, reveal_once_secret=reveal_once_secret)


# --- list (user) ------------------------------------------------------------


class ListAgentsForUserCommand:
    __slots__ = ()


class ListAgentsForUserResult:
    __slots__ = ("agents",)

    def __init__(self, agents: list[Any]) -> None:
        self.agents = agents


class ListAgentsForUserUseCase:
    """List the actor's owned agents (read)."""

    async def execute(
        self, command: ListAgentsForUserCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListAgentsForUserResult:
        service = uow.services.agents
        await require_authorization(
            actor,
            PermissionRequirement(
                "agent.entity.read",
                legacy_operation="board.read",
            ),
            uow=uow,
        )
        return ListAgentsForUserResult(await service.list_agents_for_user(actor.actor_id))


# --- list (board) -----------------------------------------------------------


class ListAgentsForBoardCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class ListAgentsForBoardResult:
    __slots__ = ("agents",)

    def __init__(self, agents: list[Any]) -> None:
        self.agents = agents


class ListAgentsForBoardUseCase:
    """List agents with access to a board the actor owns (read).

    Raises :class:`EntityNotFoundError` ("board") when the board is missing/not
    owned — the REST adapter maps that to the legacy 404."""

    async def execute(
        self, command: ListAgentsForBoardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListAgentsForBoardResult:
        await _require_owned_board(uow, command.board_id, actor)
        await require_authorization(
            actor,
            PermissionRequirement(
                "agent.board_access.read",
                legacy_operation="board.read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        agents = await uow.services.agents.list_agents_for_board(command.board_id)
        return ListAgentsForBoardResult(agents)


# --- get --------------------------------------------------------------------


class GetAgentCommand:
    __slots__ = ("agent_id",)

    def __init__(self, agent_id: str) -> None:
        self.agent_id = agent_id


class GetAgentResult:
    __slots__ = ("agent",)

    def __init__(self, agent: Any) -> None:
        self.agent = agent


class GetAgentUseCase:
    """Fetch an owned agent (read). Raises ``EntityNotFoundError`` when missing or
    not owned by the actor (REST → 404)."""

    async def execute(
        self, command: GetAgentCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetAgentResult:
        service = uow.services.agents
        agent = await service.get_agent(command.agent_id)
        if not agent or agent.created_by != actor.actor_id:
            raise EntityNotFoundError("agent", command.agent_id)
        await require_authorization(
            actor,
            PermissionRequirement(
                "agent.entity.read",
                legacy_operation="board.read",
            ),
            uow=uow,
        )
        return GetAgentResult(agent)


# --- regenerate key ---------------------------------------------------------


class RegenerateAgentKeyCommand:
    __slots__ = ("agent_id",)

    def __init__(self, agent_id: str) -> None:
        self.agent_id = agent_id


class RegenerateAgentKeyResult:
    __slots__ = ("agent", "reveal_once_secret")

    def __init__(self, agent: Any, reveal_once_secret: str) -> None:
        self.agent = agent
        self.reveal_once_secret = reveal_once_secret


class RegenerateAgentKeyUseCase:
    """Rotate an owned agent's API key (write — commits). 404 when not owned."""

    async def execute(
        self, command: RegenerateAgentKeyCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> RegenerateAgentKeyResult:
        service = uow.services.agents
        agent = await service.get_agent(command.agent_id)
        if not agent or agent.created_by != actor.actor_id:
            raise EntityNotFoundError("agent", command.agent_id)
        await require_authorization(
            actor,
            PermissionRequirement(
                "agent.api_key.rotate",
                legacy_operation="profile.update",
            ),
            uow=uow,
        )
        updated, reveal_once_secret = await service.regenerate_key(command.agent_id)
        await commit(uow)
        return RegenerateAgentKeyResult(agent=updated, reveal_once_secret=reveal_once_secret)


# --- delete -----------------------------------------------------------------


class DeleteAgentCommand:
    __slots__ = ("agent_id",)

    def __init__(self, agent_id: str) -> None:
        self.agent_id = agent_id


class DeleteAgentResult:
    __slots__ = ()


class DeleteAgentUseCase:
    """Delete an owned agent (write — commits). 404 when not owned. NO cache
    invalidation (not a proven invalidation point)."""

    async def execute(
        self, command: DeleteAgentCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeleteAgentResult:
        service = uow.services.agents
        agent = await service.get_agent(command.agent_id)
        if not agent or agent.created_by != actor.actor_id:
            raise EntityNotFoundError("agent", command.agent_id)
        await require_authorization(
            actor,
            PermissionRequirement(
                "agent.entity.delete",
                legacy_operation="profile.update",
            ),
            uow=uow,
        )
        await service.delete_agent(command.agent_id)
        await commit(uow)
        return DeleteAgentResult()


# --- grant board access -----------------------------------------------------


class GrantBoardAccessCommand:
    __slots__ = ("agent_id", "board_id")

    def __init__(self, agent_id: str, board_id: str) -> None:
        self.agent_id = agent_id
        self.board_id = board_id


class GrantBoardAccessResult:
    __slots__ = ("grant",)

    def __init__(self, grant: Any) -> None:
        self.grant = grant


class GrantBoardAccessUseCase:
    """Grant an owned agent access to an owned board (write — commits).

    Raises ``EntityNotFoundError`` ("agent"/"board") for the 404s and
    ``ConflictError`` ("agent_board") for the existing-grant 409. NO cache
    invalidation."""

    async def execute(
        self, command: GrantBoardAccessCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GrantBoardAccessResult:
        await _require_owned_board(uow, command.board_id, actor)
        service = uow.services.agents
        agent = await service.get_agent(command.agent_id)
        if not agent or agent.created_by != actor.actor_id:
            raise EntityNotFoundError("agent", command.agent_id)
        if await service.agent_has_board_access(command.agent_id, command.board_id):
            raise ConflictError("agent_board", f"{command.agent_id}:{command.board_id}")
        await require_authorization(
            actor,
            PermissionRequirement(
                "agent.board_access.grant",
                legacy_operation="board.read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        grant = await service.grant_board_access(
            command.agent_id, command.board_id, actor.actor_id
        )
        await commit(uow)
        return GrantBoardAccessResult(grant)


# --- revoke board access ----------------------------------------------------


class RevokeBoardAccessCommand:
    __slots__ = ("agent_id", "board_id")

    def __init__(self, agent_id: str, board_id: str) -> None:
        self.agent_id = agent_id
        self.board_id = board_id


class RevokeBoardAccessResult:
    __slots__ = ()


class RevokeBoardAccessUseCase:
    """Revoke access when the actor owns both the agent and board (commits).

    Raises ``EntityNotFoundError`` ("agent") when not owned and ("access") when
    there was no grant to revoke. NO cache invalidation."""

    async def execute(
        self, command: RevokeBoardAccessCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> RevokeBoardAccessResult:
        await _require_owned_board(uow, command.board_id, actor)
        service = uow.services.agents
        agent = await service.get_agent(command.agent_id)
        if not agent or agent.created_by != actor.actor_id:
            raise EntityNotFoundError("agent", command.agent_id)
        await require_authorization(
            actor,
            PermissionRequirement(
                "agent.board_access.revoke",
                legacy_operation="board.read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        revoked = await service.revoke_board_access(command.agent_id, command.board_id)
        if not revoked:
            raise EntityNotFoundError("access", f"{command.agent_id}:{command.board_id}")
        await commit(uow)
        return RevokeBoardAccessResult()
