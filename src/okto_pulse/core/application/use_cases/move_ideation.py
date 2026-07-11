"""move_ideation use case (spec #09 first cut, paired REST/MCP).

Behavior-preserving transport-free reimplementation of
``POST /api/v1/ideations/{ideation_id}/move`` (``api/ideations.py:move_ideation``)
and the ``okto_pulse_move_ideation`` MCP tool. Delegates to the existing
``IdeationService``; the transition guard / ambiguity gate semantics are
unchanged. ``AmbiguityGateError`` raised by the service propagates unchanged so
the inbound adapter can map it (HTTP 400 / MCP error); a missing ideation is
surfaced as :class:`EntityNotFoundError` (adapter maps to HTTP 404).
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.models import IdeationMove


class MoveIdeationCommand:
    """Input for :class:`MoveIdeationUseCase`."""

    __slots__ = ("ideation_id", "data")

    def __init__(self, ideation_id: str, data: IdeationMove) -> None:
        self.ideation_id = ideation_id
        self.data = data


class MoveIdeationResult:
    """Output of :class:`MoveIdeationUseCase` — the moved ideation, re-fetched,
    ready for ``IdeationResponse``."""

    __slots__ = ("ideation",)

    def __init__(self, ideation: Any) -> None:
        self.ideation = ideation


class MoveIdeationUseCase:
    """Change an ideation's status without any transport dependency."""

    async def execute(
        self, command: MoveIdeationCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> MoveIdeationResult:
        service = uow.services.ideations
        # AmbiguityGateError (and any transition guard error) propagates to the
        # adapter unchanged — mirrors api/ideations.py:move_ideation ordering.
        # actor.actor_name is None for REST (the service resolves it) and the MCP
        # agent name for MCP — preserving both surfaces' audit behavior.
        ideation = await service.move_ideation(
            command.ideation_id,
            actor.actor_id,
            command.data,
            actor_name=actor.actor_name,
        )
        if ideation is None:
            raise EntityNotFoundError("ideation", command.ideation_id)
        await commit(uow)
        ideation = await service.get_ideation(command.ideation_id)
        return MoveIdeationResult(ideation=ideation)
