from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
)
from okto_pulse.core.application.use_cases.delivery_forecast import (
    DeliveryForecastCommand,
    DeliveryForecastUseCase,
)
from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsFilterClause,
    AnalyticsUtcWindow,
)


NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)


class _Projection:
    def canonical_dict(self) -> dict[str, object]:
        return {"result_state": "partial", "readiness": {"ready": False}}


class _Analytics:
    def __init__(self) -> None:
        self.query = None

    async def delivery_forecast(self, *, query):  # noqa: ANN001, ANN201
        self.query = query
        return _Projection()


class _Boards:
    def __init__(self, board: object | None) -> None:
        self.board = board

    async def get(self, board_id: str) -> object | None:
        return self.board if board_id == "board-1" else None


def _uow(*, accessible: bool = True):
    analytics = _Analytics()
    board = SimpleNamespace(owner_id="user-1", realm_id="local") if accessible else None
    return SimpleNamespace(
        boards=_Boards(board),
        services=SimpleNamespace(analytics=analytics),
    ), analytics


def _command() -> DeliveryForecastCommand:
    return DeliveryForecastCommand(
        board_id="board-1",
        window=AnalyticsUtcWindow(NOW - timedelta(days=90), NOW),
        as_of=NOW,
        filters=(AnalyticsFilterClause("sprint_id", "in", ("sprint-1",)),),
        horizon="next_sprint",
        confidence_level=0.8,
        method_version="empirical-quantile-v1",
    )


@pytest.mark.asyncio
async def test_use_case_builds_authorized_forecast_query() -> None:
    uow, analytics = _uow()

    result = await DeliveryForecastUseCase().execute(
        _command(),
        actor=ActorContext("user-1", "rest"),
        uow=uow,
    )

    assert result.data == {
        "result_state": "partial",
        "readiness": {"ready": False},
    }
    assert analytics.query.foundation.board_id == "board-1"
    assert analytics.query.foundation.actor_scope_ref == "actor:user-1"
    assert analytics.query.foundation.filters[0].canonical_dict() == {
        "field": "sprint_id",
        "operator": "in",
        "value": ["sprint-1"],
    }


@pytest.mark.asyncio
async def test_use_case_hides_missing_or_denied_board() -> None:
    uow, _analytics = _uow(accessible=False)

    with pytest.raises(EntityNotFoundError):
        await DeliveryForecastUseCase().execute(
            _command(),
            actor=ActorContext("user-1", "rest"),
            uow=uow,
        )
