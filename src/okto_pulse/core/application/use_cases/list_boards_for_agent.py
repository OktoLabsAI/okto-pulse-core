"""list_boards_for_agent use case (SaaS Refactor spec R01A MCP-FU4, read-only).

Transport-free reimplementation of the single SQL touch shared by the KG query
MCP tools — ``kg_query_tools._get_user_boards`` — which resolves the accessible
board ids for an authenticated agent via ``AgentService.list_boards_for_agent``.
Migrating this one helper strangles the relational coupling of the whole
``register_kg_query_tools`` family at once. Behaviour (the board ids returned and
the trailing commit) is preserved; the public signature is transport-neutral.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


from okto_pulse.core.application.use_cases.base import ActorContext, commit


class ListBoardsForAgentCommand:
    """Input for :class:`ListBoardsForAgentUseCase`."""

    __slots__ = ("agent_id",)

    def __init__(self, agent_id: str) -> None:
        self.agent_id = agent_id


class ListBoardsForAgentResult:
    """Output — the accessible board ids (same list the helper returned)."""

    __slots__ = ("board_ids",)

    def __init__(self, board_ids: list[str]) -> None:
        self.board_ids = board_ids


class ListBoardsForAgentUseCase:
    """Resolve an agent's accessible board ids, transport-free."""

    async def execute(
        self, command: ListBoardsForAgentCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListBoardsForAgentResult:

        boards = await uow.services.agents.list_boards_for_agent(command.agent_id)
        await commit(uow)
        return ListBoardsForAgentResult(board_ids=[b.id for b in boards])
