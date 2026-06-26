"""create_board use case (spec #09 first cut, REST-only).

Behavior-preserving transport-free reimplementation of
``POST /api/v1/boards`` (``api/boards.py:create_board``). Delegates to the
existing ``BoardService`` so the Community semantics, audit and default-config
snapshot are unchanged; the inbound adapter (spec #09 card 77188b6f) builds the
command/actor and serializes the result via ``BoardResponse``.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    commit,
    session_of,
)
from okto_pulse.core.models import BoardCreate
from okto_pulse.core.services import BoardService
from okto_pulse.core.services.board_governance import BoardGovernanceService


class CreateBoardCommand:
    """Input for :class:`CreateBoardUseCase`."""

    __slots__ = ("data",)

    def __init__(self, data: BoardCreate) -> None:
        self.data = data


class CreateBoardResult:
    """Output of :class:`CreateBoardUseCase` — the persisted board, re-fetched
    with relationships and effective settings, ready for ``BoardResponse``."""

    __slots__ = ("board",)

    def __init__(self, board: Any) -> None:
        self.board = board


class CreateBoardUseCase:
    """Create a board without any transport dependency."""

    async def execute(
        self, command: CreateBoardCommand, *, actor: ActorContext, uow: Any
    ) -> CreateBoardResult:
        session = session_of(uow)
        service = BoardService(session)
        board = await service.create_board(
            actor.actor_id, command.data, realm_id=actor.realm_id
        )
        await commit(uow)
        # Re-fetch with relationships loaded (mirrors api/boards.py:create_board).
        board = await service.get_board(board.id)
        board.__dict__["agents"] = []
        board.__dict__["settings"] = BoardGovernanceService.normalize_settings(
            getattr(board, "settings", None)
        )
        return CreateBoardResult(board=board)
