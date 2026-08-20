"""Permission-scoped Board KG Analytics application boundary."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    AnalyticsUtcWindow,
    require_utc_datetime,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


@dataclass(frozen=True, slots=True)
class BoardKgAnalyticsCommand:
    board_id: str
    window: AnalyticsUtcWindow
    as_of: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.board_id, str) or not self.board_id.strip():
            raise ValueError("board_kg_analytics_board_id_required")
        if not isinstance(self.window, AnalyticsUtcWindow):
            raise ValueError("board_kg_analytics_window_required")
        object.__setattr__(
            self,
            "as_of",
            require_utc_datetime(self.as_of, field="board_kg_analytics_as_of"),
        )


@dataclass(frozen=True, slots=True)
class BoardKgAnalyticsResult:
    data: dict[str, object]


class BoardKgAnalyticsUseCase:
    async def execute(
        self,
        command: BoardKgAnalyticsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> BoardKgAnalyticsResult:
        if await load_accessible_board(uow, command.board_id, actor) is None:
            raise EntityNotFoundError("board", command.board_id)

        actor_scope_ref = f"actor:{actor.actor_id}"
        query = AnalyticsFoundationQuery(
            board_id=command.board_id,
            actor_scope_ref=actor_scope_ref,
            window=command.window,
            as_of=command.as_of,
        )
        projection = await uow.services.analytics.board_kg(
            query=query,
            as_of=command.as_of,
            population_scope=AnalyticsPopulationScope(actor_scope_ref, 1),
            exclusions=AnalyticsExclusionSummary(),
        )
        return BoardKgAnalyticsResult(projection.canonical_dict())


__all__ = [
    "BoardKgAnalyticsCommand",
    "BoardKgAnalyticsResult",
    "BoardKgAnalyticsUseCase",
]
