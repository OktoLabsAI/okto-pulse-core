"""Permission-scoped Board KG Analytics application boundary."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import cast

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsExclusionSummary,
    AnalyticsFilterClause,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    AnalyticsUtcWindow,
    require_utc_datetime,
)
from okto_pulse.core.ports.board_kg_analytics import (
    BoardKgEffectivenessProjection,
    BoardKgAnalyticsQuery,
    BoardKgCognitiveStatus,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


@dataclass(frozen=True, slots=True)
class BoardKgAnalyticsCommand:
    board_id: str
    window: AnalyticsUtcWindow
    as_of: datetime
    filters: tuple[AnalyticsFilterClause, ...] = ()
    cognitive_status: tuple[str, ...] = ()
    artifact_types: tuple[str, ...] = ()
    cursor: str | None = None
    limit: int = 100
    historical_as_of: datetime | None = None

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
        if not isinstance(self.filters, tuple) or any(
            not isinstance(item, AnalyticsFilterClause) for item in self.filters
        ):
            raise ValueError("board_kg_analytics_filters_invalid")
        if not isinstance(self.cognitive_status, tuple):
            raise ValueError("board_kg_analytics_cognitive_status_invalid")
        try:
            statuses = tuple(
                BoardKgCognitiveStatus(item).value for item in self.cognitive_status
            )
        except ValueError as exc:
            raise ValueError("board_kg_analytics_cognitive_status_invalid") from exc
        if tuple(sorted(set(statuses))) != statuses:
            raise ValueError("board_kg_analytics_cognitive_status_not_canonical")
        object.__setattr__(self, "cognitive_status", statuses)
        if not isinstance(self.artifact_types, tuple) or any(
            not isinstance(item, str) or not item.strip()
            for item in self.artifact_types
        ):
            raise ValueError("board_kg_analytics_artifact_types_invalid")
        artifact_types = tuple(item.strip() for item in self.artifact_types)
        if tuple(sorted(set(artifact_types))) != artifact_types:
            raise ValueError("board_kg_analytics_artifact_types_not_canonical")
        object.__setattr__(self, "artifact_types", artifact_types)
        if self.cursor is not None and (
            not isinstance(self.cursor, str) or not self.cursor.strip()
        ):
            raise ValueError("board_kg_analytics_cursor_invalid")
        if (
            isinstance(self.limit, bool)
            or not isinstance(self.limit, int)
            or not 1 <= self.limit <= 500
        ):
            raise ValueError("board_kg_analytics_limit_invalid")
        if self.historical_as_of is not None:
            object.__setattr__(
                self,
                "historical_as_of",
                require_utc_datetime(
                    self.historical_as_of, field="board_kg_historical_as_of"
                ),
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
        foundation = AnalyticsFoundationQuery(
            board_id=command.board_id,
            actor_scope_ref=actor_scope_ref,
            window=command.window,
            filters=command.filters,
            as_of=command.as_of,
        )
        query = BoardKgAnalyticsQuery(
            foundation=foundation,
            cognitive_status=tuple(
                BoardKgCognitiveStatus(item) for item in command.cognitive_status
            ),
            artifact_types=command.artifact_types,
            cursor=command.cursor,
            limit=command.limit,
            historical_as_of=command.historical_as_of,
        )
        projection = cast(
            BoardKgEffectivenessProjection,
            await uow.services.analytics.board_kg(
                query=query,
                as_of=command.as_of,
                population_scope=AnalyticsPopulationScope(actor_scope_ref, 1),
                exclusions=AnalyticsExclusionSummary(),
            ),
        )
        return BoardKgAnalyticsResult(projection.canonical_dict())


__all__ = [
    "BoardKgAnalyticsCommand",
    "BoardKgAnalyticsResult",
    "BoardKgAnalyticsUseCase",
]
