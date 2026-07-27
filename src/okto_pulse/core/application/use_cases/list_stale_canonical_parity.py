"""list_stale_canonical_parity use case (SaaS Refactor spec R01A IMP4, REST read-only).

Behavior-preserving, transport-free reimplementation of
``GET /api/v1/kg/{board_id}/stale-canonical-parity`` (``api/kg_stale_canonical_parity.py``).
Delegates to the existing ``list_stale_canonical_parity`` reader so the drilldown
payload is byte-identical to the legacy handler. Read-only: no commit. The public
signature is transport-neutral — it never exposes ``AsyncSession``/``get_db``.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError


class ListStaleCanonicalParityCommand:
    """Input for :class:`ListStaleCanonicalParityUseCase`."""

    __slots__ = ("board_id", "limit", "offset")

    def __init__(self, board_id: str, *, limit: int = 50, offset: int = 0) -> None:
        self.board_id = board_id
        self.limit = limit
        self.offset = offset


class ListStaleCanonicalParityResult:
    """Output — the drilldown mapping the legacy handler returned directly."""

    __slots__ = ("data",)

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


class ListStaleCanonicalParityUseCase:
    """List stale-canonical parity signals without any transport dependency."""

    async def execute(
        self,
        command: ListStaleCanonicalParityCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListStaleCanonicalParityResult:
        if await load_accessible_board(uow, command.board_id, actor) is None:
            raise EntityNotFoundError("board", command.board_id)
        data = await uow.services.kg.list_stale_canonical_parity(
            board_id=command.board_id,
            limit=command.limit,
            offset=command.offset,
        )
        return ListStaleCanonicalParityResult(data=data)
