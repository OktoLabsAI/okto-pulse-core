"""update_agent use case (SaaS Refactor spec R01A IMP4, REST write).

Behavior-preserving, transport-free reimplementation of the owner-only
``PATCH /api/v1/agents/{agent_id}`` (``api/agents.py:update_agent``). Delegates to
the existing ``AgentService`` so the ownership check (404 when not owner), the
update, the commit and the re-fetch are unchanged.

The MCP permission-cache invalidation (``invalidate_agent_cache``) is a transport
concern and stays in the REST adapter AFTER a successful update — this use case
does NOT touch it, so the proven invalidation point is preserved exactly
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
from okto_pulse.core.models import AgentUpdate
from okto_pulse.core.services import AgentService


class UpdateAgentCommand:
    """Input for :class:`UpdateAgentUseCase`."""

    __slots__ = ("agent_id", "data")

    def __init__(self, agent_id: str, data: AgentUpdate) -> None:
        self.agent_id = agent_id
        self.data = data


class UpdateAgentResult:
    """Output — the re-fetched agent, ready for ``AgentResponse``."""

    __slots__ = ("agent",)

    def __init__(self, agent: Any) -> None:
        self.agent = agent


class UpdateAgentUseCase:
    """Update an owner's agent without any transport dependency.

    Raises :class:`EntityNotFoundError` when the agent does not exist or is not
    owned by the actor — the REST adapter maps that to the legacy 404.
    """

    async def execute(
        self, command: UpdateAgentCommand, *, actor: ActorContext, uow: Any
    ) -> UpdateAgentResult:
        session = session_of(uow)
        service = AgentService(session)
        agent = await service.get_agent(command.agent_id)
        if not agent or agent.created_by != actor.actor_id:
            raise EntityNotFoundError("agent", command.agent_id)

        await service.update_agent(command.agent_id, command.data)
        await commit(uow)
        updated = await service.get_agent(command.agent_id)
        return UpdateAgentResult(agent=updated)
