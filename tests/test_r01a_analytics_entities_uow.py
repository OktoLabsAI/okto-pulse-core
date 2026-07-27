"""Spec R01A REST-FU2e — analytics entity list on the UnitOfWork path.

``board_entities`` (dispatch-by-type list) now routes through
``BoardEntitiesUseCase`` + ``get_unit_of_work``; the three list readers
(_list_card/ideation/spec_entities) moved verbatim to ``analytics_service``
(reusing the FU2b helpers). Oracles: per-type payload + search + pagination +
invalid-type 400 + board-ownership 404 + golden parity + AST.
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

USER = "r01a-fu2e-user"
OTHER = "r01a-fu2e-other"
PREFIX = "/api/v1"
ENTITIES = "{prefix}/boards/{bid}/analytics/entities"


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

    bid = f"board-fu2e-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu2e", owner_id=owner))
        await db.commit()
    return bid


def _url(bid: str) -> str:
    return ENTITIES.format(prefix=PREFIX, bid=bid)


@pytest.mark.asyncio
async def test_entities_each_type_200() -> None:
    bid = await _seed_board()
    client = _client()
    for entity_type in ("card", "ideation", "spec"):
        resp = client.get(_url(bid), params={"type": entity_type})
        assert resp.status_code == 200, (entity_type, resp.text)
        assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_entities_search_and_pagination() -> None:
    bid = await _seed_board()
    client = _client()
    searched = client.get(_url(bid), params={"type": "card", "search": "x"})
    assert searched.status_code == 200, searched.text
    paged = client.get(_url(bid), params={"type": "spec", "offset": 0, "limit": 5})
    assert paged.status_code == 200, paged.text


@pytest.mark.asyncio
async def test_entities_invalid_type_400() -> None:
    bid = await _seed_board()
    resp = _client().get(_url(bid), params={"type": "bogus"})
    assert resp.status_code == 400
    assert "type must be one of" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_entities_board_404_for_non_owner() -> None:
    bid = await _seed_board(owner=OTHER)
    resp = _client(USER).get(_url(bid), params={"type": "card"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_entities_use_case_matches_reader() -> None:
    from okto_pulse.core.application.use_cases import (
        BoardEntitiesCommand,
        BoardEntitiesUseCase,
    )
    from okto_pulse.core.application.use_cases.base import ActorContext
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    from okto_pulse.core.services.analytics_service import _list_card_entities

    bid = await _seed_board()
    async with get_session_factory()() as db:
        baseline = await _list_card_entities(db, bid, 0, 50, None, None, "")

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    async with uowf(actor=actor) as uow:
        result = await BoardEntitiesUseCase().execute(
            BoardEntitiesCommand(bid, type="card"), actor=actor, uow=uow
        )
    assert result.data == baseline


def test_board_entities_takes_uow_not_raw_session() -> None:
    sig = inspect.signature(analytics_api.board_entities)
    assert "db" not in sig.parameters
    assert "uow" in sig.parameters
    assert sig.parameters["uow"].default.dependency is get_unit_of_work
