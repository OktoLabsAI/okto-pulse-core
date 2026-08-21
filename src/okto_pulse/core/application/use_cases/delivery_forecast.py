"""Permission-scoped Delivery Forecast application boundary."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import cast

from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsFilterClause,
    AnalyticsFoundationQuery,
    AnalyticsUtcWindow,
    require_utc_datetime,
)
from okto_pulse.core.ports.delivery_forecast import (
    DEFAULT_FORECAST_CONFIDENCE_LEVEL,
    DEFAULT_FORECAST_HORIZON,
    DEFAULT_FORECAST_METHOD_VERSION,
    ForecastReadinessQuery,
    DeliveryForecastProjection,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


@dataclass(frozen=True, slots=True)
class DeliveryForecastCommand:
    board_id: str
    window: AnalyticsUtcWindow
    as_of: datetime
    horizon: str = DEFAULT_FORECAST_HORIZON
    confidence_level: float = DEFAULT_FORECAST_CONFIDENCE_LEVEL
    method_version: str = DEFAULT_FORECAST_METHOD_VERSION
    filters: tuple[AnalyticsFilterClause, ...] = ()
    historical_as_of: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.board_id, str) or not self.board_id.strip():
            raise ValueError("delivery_forecast_board_id_required")
        if not isinstance(self.window, AnalyticsUtcWindow):
            raise ValueError("delivery_forecast_window_required")
        object.__setattr__(
            self,
            "as_of",
            require_utc_datetime(self.as_of, field="delivery_forecast_as_of"),
        )
        if not isinstance(self.filters, tuple) or any(
            not isinstance(item, AnalyticsFilterClause) for item in self.filters
        ):
            raise ValueError("delivery_forecast_filters_invalid")
        if self.historical_as_of is not None:
            object.__setattr__(
                self,
                "historical_as_of",
                require_utc_datetime(
                    self.historical_as_of,
                    field="delivery_forecast_historical_as_of",
                ),
            )


@dataclass(frozen=True, slots=True)
class DeliveryForecastResult:
    data: dict[str, object]


class DeliveryForecastUseCase:
    async def execute(
        self,
        command: DeliveryForecastCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DeliveryForecastResult:
        if await load_accessible_board(uow, command.board_id, actor) is None:
            raise EntityNotFoundError("board", command.board_id)
        foundation = AnalyticsFoundationQuery(
            board_id=command.board_id,
            actor_scope_ref=f"actor:{actor.actor_id}",
            window=command.window,
            filters=command.filters,
            as_of=command.as_of,
        )
        projection = cast(
            DeliveryForecastProjection,
            await uow.services.analytics.delivery_forecast(
                query=ForecastReadinessQuery(
                    foundation=foundation,
                    horizon=command.horizon,
                    confidence_level=command.confidence_level,
                    method_version=command.method_version,
                    historical_as_of=command.historical_as_of,
                )
            ),
        )
        return DeliveryForecastResult(projection.canonical_dict())


__all__ = [
    "DeliveryForecastCommand",
    "DeliveryForecastResult",
    "DeliveryForecastUseCase",
]
