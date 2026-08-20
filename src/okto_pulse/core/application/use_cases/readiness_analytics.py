"""Permission-scoped Spec and policy/resource readiness projections."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsFoundationQuery,
    AnalyticsUtcWindow,
    require_utc_datetime,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


@dataclass(frozen=True, slots=True)
class ReadinessAnalyticsCommand:
    board_id: str
    window: AnalyticsUtcWindow
    as_of: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.board_id, str) or not self.board_id.strip():
            raise ValueError("readiness_board_id_required")
        if not isinstance(self.window, AnalyticsUtcWindow):
            raise ValueError("readiness_window_required")
        object.__setattr__(
            self,
            "as_of",
            require_utc_datetime(self.as_of, field="readiness_as_of"),
        )


@dataclass(frozen=True, slots=True)
class ReadinessAnalyticsResult:
    data: dict[str, object]


def _query(command: ReadinessAnalyticsCommand, actor: ActorContext):
    return AnalyticsFoundationQuery(
        board_id=command.board_id,
        actor_scope_ref=f"actor:{actor.actor_id}",
        window=command.window,
        as_of=command.as_of,
    )


async def _authorize(
    command: ReadinessAnalyticsCommand,
    *,
    actor: ActorContext,
    uow: PulseUnitOfWork,
) -> None:
    if await load_accessible_board(uow, command.board_id, actor) is None:
        raise EntityNotFoundError("board", command.board_id)


class SpecReadinessAnalyticsUseCase:
    async def execute(
        self,
        command: ReadinessAnalyticsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ReadinessAnalyticsResult:
        await _authorize(command, actor=actor, uow=uow)
        projection = await uow.services.analytics.canonical_spec_readiness(
            query=_query(command, actor),
            as_of=command.as_of,
        )
        return ReadinessAnalyticsResult(projection.canonical_dict())


class PolicyResourceReadinessAnalyticsUseCase:
    async def execute(
        self,
        command: ReadinessAnalyticsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ReadinessAnalyticsResult:
        await _authorize(command, actor=actor, uow=uow)
        projection = await uow.services.analytics.canonical_policy_resource_readiness(
            query=_query(command, actor),
            as_of=command.as_of,
        )
        return ReadinessAnalyticsResult(projection.canonical_dict())


__all__ = [
    "PolicyResourceReadinessAnalyticsUseCase",
    "ReadinessAnalyticsCommand",
    "ReadinessAnalyticsResult",
    "SpecReadinessAnalyticsUseCase",
]
