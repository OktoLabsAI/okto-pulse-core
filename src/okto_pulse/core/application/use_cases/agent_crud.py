"""Agent CRUD + board-access use cases (SaaS Refactor spec R01A REST-FU1).

Transport-free reimplementations of the remaining ``api/agents.py`` endpoints
that still opened a raw ``AsyncSession``/``get_db`` — create / list (user+board) /
get / regenerate-key / delete / grant / revoke. Each delegates to the existing
``AgentService`` / ``BoardService`` so ownership checks (404), the structured
errors, the commit/refetch and the transaction are unchanged; the REST adapter
maps the transport-neutral errors back to 404/409.

Cache invalidation is intentionally NOT added here: only ``update_agent`` and
``update_board_overrides`` are proven invalidation points (ac_8e695cf2). Grant /
revoke / delete must stay WITHOUT ``invalidate_agent_cache`` — see
[[feedback_register_critical_mutation_governance]] and the TEST-TEETH 1 guard.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    ConflictError,
    EntityNotFoundError,
    commit,
)


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
        board = await uow.services.boards.get_board(command.board_id, actor.actor_id)
        if not board:
            raise EntityNotFoundError("board", command.board_id)
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
        service = uow.services.agents
        agent = await service.get_agent(command.agent_id)
        if not agent or agent.created_by != actor.actor_id:
            raise EntityNotFoundError("agent", command.agent_id)
        board = await uow.services.boards.get_board(command.board_id, actor.actor_id)
        if not board:
            raise EntityNotFoundError("board", command.board_id)
        if await service.agent_has_board_access(command.agent_id, command.board_id):
            raise ConflictError("agent_board", f"{command.agent_id}:{command.board_id}")
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
    """Revoke an owned agent's board access (write — commits).

    Raises ``EntityNotFoundError`` ("agent") when not owned and ("access") when
    there was no grant to revoke. NO cache invalidation."""

    async def execute(
        self, command: RevokeBoardAccessCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> RevokeBoardAccessResult:
        service = uow.services.agents
        agent = await service.get_agent(command.agent_id)
        if not agent or agent.created_by != actor.actor_id:
            raise EntityNotFoundError("agent", command.agent_id)
        revoked = await service.revoke_board_access(command.agent_id, command.board_id)
        if not revoked:
            raise EntityNotFoundError("access", f"{command.agent_id}:{command.board_id}")
        await commit(uow)
        return RevokeBoardAccessResult()
