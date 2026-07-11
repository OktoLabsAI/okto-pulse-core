"""Spec R01A REST-FU2a — analytics helper endpoints on the UnitOfWork path.

The four helper-delegating analytics endpoints (blockers / funnel / velocity /
coverage) now route through transport-free use cases + ``get_unit_of_work``
instead of a raw ``AsyncSession``. Oracles: payload + filters + the velocity 400
+ the board-ownership 404 are preserved, the use case output equals the
``compute_*`` helper (golden), and the four handlers carry no raw session.
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

USER = "r01a-fu2a-user"
OTHER = "r01a-fu2a-other"
PREFIX = "/api/v1"

_ENDPOINTS = ("board_blockers", "board_funnel", "board_velocity", "board_coverage")


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

    bid = f"board-fu2a-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu2a", owner_id=owner))
        await db.commit()
    return bid


def _url(board_id: str, leaf: str) -> str:
    return f"{PREFIX}/boards/{board_id}/analytics/{leaf}"


# --- happy path (owned board → 200 + payload) -------------------------------


@pytest.mark.asyncio
async def test_blockers_200_with_filter() -> None:
    board_id = await _seed_board()
    client = _client()
    resp = client.get(_url(board_id, "blockers"), params={"stale_hours": 24})
    assert resp.status_code == 200, resp.text
    assert isinstance(resp.json(), dict)
    # the filter_type passthrough does not 500
    filtered = client.get(_url(board_id, "blockers"), params={"filter_type": "on_hold"})
    assert filtered.status_code == 200, filtered.text


@pytest.mark.asyncio
async def test_funnel_200_with_date_filters() -> None:
    board_id = await _seed_board()
    resp = _client().get(
        _url(board_id, "funnel"), params={"from": "2026-01-01", "to": "2026-12-31"}
    )
    assert resp.status_code == 200, resp.text
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_velocity_200_and_invalid_granularity_400() -> None:
    board_id = await _seed_board()
    client = _client()
    ok = client.get(_url(board_id, "velocity"), params={"granularity": "day", "weeks": 4})
    assert ok.status_code == 200, ok.text
    bad = client.get(_url(board_id, "velocity"), params={"granularity": "month"})
    assert bad.status_code == 400
    assert "granularity must be" in bad.json()["detail"]


@pytest.mark.asyncio
async def test_coverage_200() -> None:
    board_id = await _seed_board()
    resp = _client().get(_url(board_id, "coverage"))
    assert resp.status_code == 200, resp.text
    assert isinstance(resp.json(), list)


# --- permission: 404 for a missing / non-owned board ------------------------


@pytest.mark.asyncio
async def test_all_helpers_404_for_non_owned_board() -> None:
    board_id = await _seed_board(owner=OTHER)
    client = _client(USER)
    for leaf in ("blockers", "funnel", "velocity", "coverage"):
        resp = client.get(_url(board_id, leaf))
        assert resp.status_code == 404, (leaf, resp.text)
        assert resp.json()["detail"] == "Board not found", leaf


@pytest.mark.asyncio
async def test_helpers_404_for_missing_board() -> None:
    missing = f"missing-{uuid.uuid4().hex[:8]}"
    resp = _client().get(_url(missing, "blockers"))
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


# --- golden parity: use case == compute_* helper ----------------------------


@pytest.mark.asyncio
async def test_blockers_use_case_matches_compute_helper() -> None:
    from okto_pulse.core.application.use_cases import (
        BoardBlockersCommand,
        BoardBlockersUseCase,
    )
    from okto_pulse.core.application.use_cases.base import ActorContext
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    from okto_pulse.core.services.analytics_service import compute_blockers

    board_id = await _seed_board()
    async with get_session_factory()() as db:
        baseline = await compute_blockers(db, board_id, stale_hours=72, filter_type=None)

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    async with uowf(actor=actor) as uow:
        result = await BoardBlockersUseCase().execute(
            BoardBlockersCommand(board_id), actor=actor, uow=uow
        )
    assert result.data == baseline


# --- AST strangler proof ----------------------------------------------------


def test_migrated_analytics_helpers_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(analytics_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def test_migrated_analytics_helpers_have_no_raw_session_in_body() -> None:
    import ast
    from pathlib import Path

    tree = ast.parse(Path(analytics_api.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name in _ENDPOINTS:
            names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
            assert "get_db" not in names, node.name
            assert "AsyncSession" not in names, node.name
            assert "_ensure_board" not in names, node.name
