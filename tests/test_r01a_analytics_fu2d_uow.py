"""Spec R01A REST-FU2d — analytics sprints/agents + board export on the UoW path.

The three remaining analytics endpoints (board_sprints_analytics, board_agents,
board_analytics_export) now route through transport-free use cases +
``get_unit_of_work``. The two heavy inline-SQL ones became readers
(compute_sprints_analytics / compute_agents) reusing the FU2b helpers; the CSV
export now gets its data from the SAME funnel/quality/velocity use cases (it
called the migrated endpoints with a raw ``db=`` before — a latent break this
card fixes). Oracles: payload + board 404 + CSV shape + golden parity + AST.
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import analytics as analytics_api
from okto_pulse.community.api.analytics import router as analytics_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu2d-user"
OTHER = "r01a-fu2d-other"
PREFIX = "/api/v1"
_ENDPOINTS = ("board_sprints_analytics", "board_agents", "board_analytics_export")


def _client(user: str = USER) -> TestClient:
    app = FastAPI()
    app.include_router(analytics_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: user
    return TestClient(app)


async def _seed_board(owner: str = USER) -> str:
    from sqlalchemy_test_models import Board

    bid = f"board-fu2d-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu2d", owner_id=owner))
        await db.commit()
    return bid


@pytest.mark.asyncio
async def test_sprints_analytics_200_and_board_404() -> None:
    board_id = await _seed_board()
    ok = _client().get(f"{PREFIX}/boards/{board_id}/analytics/sprints")
    assert ok.status_code == 200, ok.text
    assert isinstance(ok.json(), dict)
    miss = _client(OTHER).get(f"{PREFIX}/boards/{board_id}/analytics/sprints")
    assert miss.status_code == 404 and miss.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_agents_200_and_board_404() -> None:
    board_id = await _seed_board()
    ok = _client().get(f"{PREFIX}/boards/{board_id}/analytics/agents")
    assert ok.status_code == 200, ok.text
    miss = _client(OTHER).get(f"{PREFIX}/boards/{board_id}/analytics/agents")
    assert miss.status_code == 404


@pytest.mark.asyncio
async def test_board_export_csv_and_404() -> None:
    """The CSV export works again (it called migrated endpoints with db= before)
    and still 404s for a non-owned board."""
    board_id = await _seed_board()
    resp = _client().get(f"{PREFIX}/boards/{board_id}/analytics/export")
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("text/csv")
    text = resp.text
    assert "Funnel Stage" in text and "Week" in text
    miss = _client(OTHER).get(f"{PREFIX}/boards/{board_id}/analytics/export")
    assert miss.status_code == 404


@pytest.mark.asyncio
async def test_sprints_use_case_matches_reader() -> None:
    from okto_pulse.core.application.use_cases import (
        BoardSprintsAnalyticsCommand,
        BoardSprintsAnalyticsUseCase,
    )
    from okto_pulse.core.application.use_cases.base import ActorContext
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    from okto_pulse.core.services.analytics_service import compute_sprints_analytics

    board_id = await _seed_board()
    async with get_session_factory()() as db:
        baseline = await compute_sprints_analytics(db, board_id, dt_from=None, dt_to=None)

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    async with uowf(actor=actor) as uow:
        result = await BoardSprintsAnalyticsUseCase().execute(
            BoardSprintsAnalyticsCommand(board_id), actor=actor, uow=uow
        )
    assert result.data == baseline


def test_fu2d_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(analytics_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def test_fu2d_endpoint_bodies_have_no_raw_session() -> None:
    """AST: the three migrated FU2d handlers carry no get_db/AsyncSession/select
    (the entity endpoints in FU2e are still pending)."""
    import ast
    from pathlib import Path

    tree = ast.parse(Path(analytics_api.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name in _ENDPOINTS:
            names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
            assert "get_db" not in names, node.name
            assert "AsyncSession" not in names, node.name
            assert "select" not in names, node.name
