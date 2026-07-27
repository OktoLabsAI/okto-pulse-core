"""Spec R01A REST-FU2f — analytics entity detail + export on the UoW path.

``board_entity_detail`` (dispatch-by-entity_type) and ``board_entity_detail_export``
now route through ``BoardEntityDetailUseCase`` + ``get_unit_of_work``. The five
detail readers (_spec/_ideation/_card/_refinement/_sprint_detail) moved to
``analytics_service`` with their ``HTTPException(404)`` rewritten to ``return None``
— the use case maps a ``None`` to ``EntityNotFoundError(entity_type)`` and the
adapter renders "<Type> not found". This card finishes the strangle:
``api/analytics.py`` carries 0 relational call-sites. Oracles: detail 200 +
per-type 404 + board 404 + invalid-type 400 + export CSV + golden parity + AST +
the fully-strangled inventory invariant.
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

USER = "r01a-fu2f-user"
OTHER = "r01a-fu2f-other"
PREFIX = "/api/v1"
_ENDPOINTS = ("board_entity_detail", "board_entity_detail_export")


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

    bid = f"board-fu2f-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu2f", owner_id=owner))
        await db.commit()
    return bid


async def _seed_spec(board_id: str) -> str:
    from okto_pulse.core.models.schemas import SpecCreate
    from okto_pulse.core.services import SpecService

    async with get_session_factory()() as db:
        spec = await SpecService(db).create_spec(
            board_id, USER, SpecCreate(title=f"fu2f-{uuid.uuid4().hex[:6]}")
        )
        await db.commit()
        return spec.id


def _detail(bid: str, etype: str, eid: str) -> str:
    return f"{PREFIX}/boards/{bid}/analytics/entity/{etype}/{eid}"


@pytest.mark.asyncio
async def test_spec_detail_200() -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    resp = _client().get(_detail(bid, "spec", sid))
    assert resp.status_code == 200, resp.text
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_detail_404_per_type() -> None:
    """A missing entity on an owned board → "<Type> not found" (None → 404)."""
    bid = await _seed_board()
    client = _client()
    expected = {
        "spec": "Spec not found",
        "ideation": "Ideation not found",
        "card": "Card not found",
        "refinement": "Refinement not found",
        "sprint": "Sprint not found",
    }
    for etype, detail in expected.items():
        resp = client.get(_detail(bid, etype, f"missing-{uuid.uuid4().hex[:6]}"))
        assert resp.status_code == 404, (etype, resp.text)
        assert resp.json()["detail"] == detail, etype


@pytest.mark.asyncio
async def test_detail_invalid_type_400() -> None:
    bid = await _seed_board()
    resp = _client().get(_detail(bid, "bogus", "x"))
    assert resp.status_code == 400
    assert "entity_type must be one of" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_detail_board_404_for_non_owner() -> None:
    # board ownership is checked before the entity lookup, so a dummy id is enough
    bid = await _seed_board(owner=OTHER)
    resp = _client(USER).get(_detail(bid, "spec", "any-spec-id"))
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_detail_export_csv() -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    resp = _client().get(_detail(bid, "spec", sid) + "/export")
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("text/csv")
    assert "Field" in resp.text and "Value" in resp.text


@pytest.mark.asyncio
async def test_detail_use_case_matches_reader() -> None:
    from okto_pulse.core.application.use_cases import (
        BoardEntityDetailCommand,
        BoardEntityDetailUseCase,
    )
    from okto_pulse.core.application.use_cases.base import ActorContext
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    from okto_pulse.core.services.analytics_service import _spec_detail

    bid = await _seed_board()
    sid = await _seed_spec(bid)
    async with get_session_factory()() as db:
        baseline = await _spec_detail(db, bid, sid)

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    async with uowf(actor=actor) as uow:
        result = await BoardEntityDetailUseCase().execute(
            BoardEntityDetailCommand(bid, "spec", sid), actor=actor, uow=uow
        )
    assert result.data == baseline


@pytest.mark.asyncio
async def test_detail_reader_returns_none_for_missing() -> None:
    from okto_pulse.core.services.analytics_service import _spec_detail

    bid = await _seed_board()
    async with get_session_factory()() as db:
        assert await _spec_detail(db, bid, "no-such-spec") is None


def test_fu2f_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(analytics_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def test_analytics_adapter_fully_strangled() -> None:
    """Terminal invariant: with FU2f done the analytics REST adapter carries no
    direct relational call-site — every endpoint routes through a use case."""
    from okto_pulse.core.repositories.relational_consumer_inventory import (
        build_relational_consumer_inventory,
    )

    inv = build_relational_consumer_inventory()
    sites = [c for c in inv.consumers if c.file == "core/api/analytics.py"]
    assert sites == [], [c.symbol for c in sites]
