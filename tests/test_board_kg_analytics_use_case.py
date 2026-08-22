from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
)
from okto_pulse.core.application.use_cases.board_kg_analytics import (
    BoardKgAnalyticsCommand,
    BoardKgAnalyticsUseCase,
)
from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
    McpGetAnalyticsCommand,
    McpGetAnalyticsUseCase,
)
from okto_pulse.core.ports.analytics_foundation import AnalyticsUtcWindow


NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


class _Projection:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def canonical_dict(self) -> dict[str, object]:
        return self.payload


class _Boards:
    def __init__(self, board: object | None) -> None:
        self.board = board

    async def get(self, board_id: str) -> object | None:
        return self.board if board_id == "board-1" else None


class _Analytics:
    def __init__(self) -> None:
        self.arguments: dict[str, object] | None = None

    async def board_kg(self, **arguments: object) -> _Projection:
        self.arguments = arguments
        return _Projection(
            {"result_state": "available", "health": {"state": "healthy"}}
        )


def _uow(*, accessible: bool = True):
    analytics = _Analytics()
    board = SimpleNamespace(owner_id="user-1", realm_id="local") if accessible else None
    return SimpleNamespace(
        boards=_Boards(board),
        services=SimpleNamespace(analytics=analytics),
    ), analytics


def _command() -> BoardKgAnalyticsCommand:
    return BoardKgAnalyticsCommand(
        board_id="board-1",
        window=AnalyticsUtcWindow(NOW - timedelta(days=1), NOW + timedelta(seconds=1)),
        as_of=NOW,
    )


@pytest.mark.asyncio
async def test_board_kg_use_case_scopes_projection_to_accessible_actor() -> None:
    uow, analytics = _uow()

    result = await BoardKgAnalyticsUseCase().execute(
        _command(), actor=ActorContext("user-1", "rest"), uow=uow
    )

    assert result.data == {
        "result_state": "available",
        "health": {"state": "healthy"},
    }
    assert analytics.arguments is not None
    query = analytics.arguments["query"]
    assert query.board_id == "board-1"
    assert query.actor_scope_ref == "actor:user-1"
    assert analytics.arguments["population_scope"].accessible_count == 1


@pytest.mark.asyncio
async def test_board_kg_use_case_hides_missing_or_denied_board() -> None:
    uow, _analytics = _uow(accessible=False)

    with pytest.raises(EntityNotFoundError):
        await BoardKgAnalyticsUseCase().execute(
            _command(), actor=ActorContext("user-1", "rest"), uow=uow
        )


@pytest.mark.asyncio
async def test_mcp_board_kg_uses_same_canonical_application_boundary() -> None:
    uow, _analytics = _uow()

    result = await McpGetAnalyticsUseCase().execute(
        McpGetAnalyticsCommand(
            "board-1",
            metric_type="board_kg",
            from_date="2026-08-19T00:00:00Z",
            to_date="2026-08-20T00:00:00Z",
        ),
        actor=ActorContext("user-1", "mcp", board_id="board-1"),
        uow=uow,
    )

    assert result.data["health"] == {"state": "healthy"}
