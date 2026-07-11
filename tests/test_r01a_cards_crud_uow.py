"""Spec R01A REST-FU4-S1 — Card CRUD thin delegates on the UnitOfWork.

The three thin ``api/cards.py`` CRUD endpoints (get/update/delete) now route
through ``GetCardUseCase``/``UpdateCardUseCase``/``DeleteCardUseCase`` +
``get_unit_of_work``; each adapter only maps the result/errors to HTTP. Oracles:
get 200 + 404, update 200 (re-fetched body) + 404, delete 204 + 404, the use
case raising ``EntityNotFoundError`` for a missing card, and an AST signature
check proving the endpoints take ``uow`` (not a raw ``AsyncSession``).
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import cards as cards_api
from okto_pulse.community.api.cards import router as cards_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu4-s1-user"
PREFIX = "/api/v1/cards"
_ENDPOINTS = ("get_card", "update_card", "delete_card")


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(cards_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    return TestClient(app)


async def _seed_card(title: str = "fu4-s1-card") -> str:
    # Seed Board + Spec + Card via raw models. We exercise get/update/delete, not
    # create, so this deliberately bypasses CardService.create_card's spec-status
    # gate (a normal card needs an approved/in_progress/done spec) — the card just
    # needs to satisfy the "every card belongs to a spec" invariant.
    from sqlalchemy_test_models import Board, Card, Spec

    bid = f"board-fu4s1-{uuid.uuid4().hex[:8]}"
    sid = f"spec-fu4s1-{uuid.uuid4().hex[:8]}"
    cid = f"card-fu4s1-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu4s1", owner_id=USER))
        db.add(Spec(id=sid, board_id=bid, title="fu4s1-spec", created_by=USER))
        db.add(
            Card(
                id=cid,
                board_id=bid,
                spec_id=sid,
                title=f"{title}-{uuid.uuid4().hex[:6]}",
                created_by=USER,
            )
        )
        await db.commit()
        return cid


def _missing() -> str:
    return f"card-missing-{uuid.uuid4().hex[:8]}"


# --- get --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_card_200(client) -> None:
    card_id = await _seed_card()
    resp = client.get(f"{PREFIX}/{card_id}")
    assert resp.status_code == 200, resp.text
    assert resp.json()["id"] == card_id


@pytest.mark.asyncio
async def test_get_card_404(client) -> None:
    resp = client.get(f"{PREFIX}/{_missing()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Card not found"


# --- update -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_card_200_refetched_body(client) -> None:
    card_id = await _seed_card()
    resp = client.patch(f"{PREFIX}/{card_id}", json={"title": "fu4-s1-renamed"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == card_id
    assert body["title"] == "fu4-s1-renamed"


@pytest.mark.asyncio
async def test_update_card_404(client) -> None:
    resp = client.patch(f"{PREFIX}/{_missing()}", json={"title": "nope"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Card not found"


# --- delete -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_card_204_then_404(client) -> None:
    card_id = await _seed_card()
    resp = client.delete(f"{PREFIX}/{card_id}")
    assert resp.status_code == 204, resp.text
    # Gone now — a second delete is a 404.
    gone = client.delete(f"{PREFIX}/{card_id}")
    assert gone.status_code == 404
    assert client.get(f"{PREFIX}/{card_id}").status_code == 404


@pytest.mark.asyncio
async def test_delete_card_404(client) -> None:
    resp = client.delete(f"{PREFIX}/{_missing()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Card not found"


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_get_card_use_case_raises_for_missing_card() -> None:
    from okto_pulse.core.application.use_cases import GetCardCommand, GetCardUseCase
    from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await GetCardUseCase().execute(
                GetCardCommand(_missing()), actor=actor, uow=uow
            )


def test_fu4_s1_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(cards_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
