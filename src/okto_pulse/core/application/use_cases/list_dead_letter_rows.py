"""list_dead_letter_rows use case (SaaS Refactor spec R01A IMP2, REST read-only).

Behavior-preserving, transport-free reimplementation of
``GET /api/v1/kg/queue/dead-letter`` (``api/dead_letter.py``). Delegates to the
existing ``list_dead_letter_rows`` service so the DLQ Inspector payload, totals
and per-attempt error history are byte-identical to the legacy handler; the REST
inbound adapter builds the command/actor and serializes via
``DeadLetterListResponse``.

Read-only: no commit. The public signature is transport-neutral — it never
exposes ``AsyncSession``/``get_db`` (the relational session is reached through
the typed KG capability), so this use case stays inside the purity-/boundary-gated
``application/use_cases`` package.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


class DeadLetterBoardNotFoundError(EntityNotFoundError):
    def __init__(self, board_id: str) -> None:
        super().__init__("board", board_id)


class ListDeadLetterRowsCommand:
    """Input for :class:`ListDeadLetterRowsUseCase`."""

    __slots__ = ("board_id", "limit", "offset")

    def __init__(self, board_id: str, *, limit: int = 50, offset: int = 0) -> None:
        self.board_id = board_id
        self.limit = limit
        self.offset = offset


class ListDeadLetterRowsResult:
    """Output — the ``{rows, total, limit, offset, ...}`` mapping the legacy
    handler fed straight into ``DeadLetterListResponse``."""

    __slots__ = ("data",)

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


class ListDeadLetterRowsUseCase:
    """List dead-lettered consolidation rows without any transport dependency."""

    async def execute(
        self,
        command: ListDeadLetterRowsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListDeadLetterRowsResult:
        if await load_accessible_board(uow, command.board_id, actor) is None:
            raise DeadLetterBoardNotFoundError(command.board_id)
        data = await uow.services.kg.list_dead_letter_rows(
            command.board_id,
            limit=command.limit,
            offset=command.offset,
        )
        return ListDeadLetterRowsResult(data=data)
