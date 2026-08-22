"""Permission-scoped canonical Flow Health projection."""

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
class FlowHealthAnalyticsCommand:
    board_id: str
    window: AnalyticsUtcWindow
    as_of: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.board_id, str) or not self.board_id.strip():
            raise ValueError("flow_health_board_id_required")
        if not isinstance(self.window, AnalyticsUtcWindow):
            raise ValueError("flow_health_window_required")
        object.__setattr__(
            self,
            "as_of",
            require_utc_datetime(self.as_of, field="flow_health_as_of"),
        )


@dataclass(frozen=True, slots=True)
class FlowHealthAnalyticsResult:
    data: dict[str, object]


class FlowHealthAnalyticsUseCase:
    async def execute(
        self,
        command: FlowHealthAnalyticsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> FlowHealthAnalyticsResult:
        if await load_accessible_board(uow, command.board_id, actor) is None:
            raise EntityNotFoundError("board", command.board_id)
        query = AnalyticsFoundationQuery(
            board_id=command.board_id,
            actor_scope_ref=f"actor:{actor.actor_id}",
            window=command.window,
            as_of=command.as_of,
        )
        projection = await uow.services.analytics.canonical_flow_health(
            query=query,
            as_of=command.as_of,
        )
        return FlowHealthAnalyticsResult(projection.canonical_dict())


__all__ = [
    "FlowHealthAnalyticsCommand",
    "FlowHealthAnalyticsResult",
    "FlowHealthAnalyticsUseCase",
]
