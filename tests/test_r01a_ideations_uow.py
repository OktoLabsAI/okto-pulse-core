"""Spec R01A REST-FU6-S1 — ideations REST endpoints on the UnitOfWork path.

Every ``api/ideations.py`` endpoint now routes through a transport-free use case
(``application/use_cases/ideations_crud.py``) over a ``PulseUnitOfWork`` — no
endpoint binds ``get_db`` / a raw ``AsyncSession`` anymore (move_ideation reuses
the pre-existing ``MoveIdeationUseCase``). Oracle written during the R01A
containment (the authoring agent stopped before emitting it): CRUD + knowledge +
Q&A happy/404 paths through ``TestClient`` + AST signature + fully-strangled.
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.ideations import router as ideations_router
from okto_pulse.community.api.auth_deps import get_realm_id, require_user
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu6-s1-user"
PREFIX = "/api/v1"


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(ideations_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    app.dependency_overrides[get_realm_id] = lambda: LOCAL_REALM_ID
    return TestClient(app)


def _missing() -> str:
    return f"missing-{uuid.uuid4().hex[:8]}"


async def _seed_board() -> str:
    from sqlalchemy_test_models import Board

    bid = f"board-fu6s1-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=bid,
                name="fu6s1",
                owner_id=USER,
                realm_id=LOCAL_REALM_ID,
            )
        )
        await db.commit()
    return bid


async def _seed_ideation(board_id: str) -> str:
    from okto_pulse.core.models.schemas import IdeationCreate
    from okto_pulse.core.services import IdeationService

    async with get_session_factory()() as db:
        ideation = await IdeationService(db).create_ideation(
            board_id, USER, IdeationCreate(title=f"idea-{uuid.uuid4().hex[:6]}")
        )
        await db.commit()
        return ideation.id


# --- ideation CRUD ----------------------------------------------------------


@pytest.mark.asyncio
async def test_create_list_get_ideation(client) -> None:
    board_id = await _seed_board()
    created = client.post(
        f"{PREFIX}/boards/{board_id}/ideations", json={"title": "First idea"}
    )
    assert created.status_code == 201, created.text
    ideation_id = created.json()["id"]

    listed = client.get(f"{PREFIX}/boards/{board_id}/ideations")
    assert listed.status_code == 200, listed.text
    assert any(i["id"] == ideation_id for i in listed.json())

    got = client.get(f"{PREFIX}/ideations/{ideation_id}")
    assert got.status_code == 200 and got.json()["id"] == ideation_id


@pytest.mark.asyncio
async def test_create_ideation_missing_board_404(client) -> None:
    resp = client.post(f"{PREFIX}/boards/{_missing()}/ideations", json={"title": "x"})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_ideation_404(client) -> None:
    resp = client.get(f"{PREFIX}/ideations/{_missing()}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_ideation_200_and_404(client) -> None:
    board_id = await _seed_board()
    ideation_id = await _seed_ideation(board_id)
    ok = client.patch(f"{PREFIX}/ideations/{ideation_id}", json={"title": "Renamed"})
    assert ok.status_code == 200, ok.text
    assert ok.json()["title"] == "Renamed"
    assert ok.json()["version"] == 2
    miss = client.patch(f"{PREFIX}/ideations/{_missing()}", json={"title": "y"})
    assert miss.status_code == 404


@pytest.mark.asyncio
async def test_delete_ideation_then_404(client) -> None:
    board_id = await _seed_board()
    ideation_id = await _seed_ideation(board_id)
    deleted = client.delete(f"{PREFIX}/ideations/{ideation_id}")
    assert deleted.status_code in (200, 204), deleted.text
    gone = client.get(f"{PREFIX}/ideations/{ideation_id}")
    assert gone.status_code == 404
    miss = client.delete(f"{PREFIX}/ideations/{_missing()}")
    assert miss.status_code == 404


# --- knowledge --------------------------------------------------------------


@pytest.mark.asyncio
async def test_knowledge_create_list_get_delete(client) -> None:
    board_id = await _seed_board()
    ideation_id = await _seed_ideation(board_id)

    created = client.post(
        f"{PREFIX}/ideations/{ideation_id}/knowledge",
        json={"title": "KB", "content": "body"},
    )
    assert created.status_code in (200, 201), created.text
    kb_id = created.json()["id"]

    listed = client.get(f"{PREFIX}/ideations/{ideation_id}/knowledge")
    assert listed.status_code == 200 and any(
        k["id"] == kb_id for k in listed.json()
    )

    got = client.get(f"{PREFIX}/ideations/{ideation_id}/knowledge/{kb_id}")
    assert got.status_code == 200
    miss = client.get(f"{PREFIX}/ideations/{ideation_id}/knowledge/{_missing()}")
    assert miss.status_code == 404

    deleted = client.delete(f"{PREFIX}/ideations/{ideation_id}/knowledge/{kb_id}")
    assert deleted.status_code in (200, 204)


# --- Q&A --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_qa_create_and_list(client) -> None:
    board_id = await _seed_board()
    ideation_id = await _seed_ideation(board_id)

    created = client.post(
        f"{PREFIX}/ideations/{ideation_id}/qa",
        json={"question": "Why?", "question_type": "open"},
    )
    assert created.status_code in (200, 201), created.text

    listed = client.get(f"{PREFIX}/ideations/{ideation_id}/qa")
    assert listed.status_code == 200


# --- strangler proofs -------------------------------------------------------


def test_ideations_endpoints_take_uow_not_raw_session() -> None:
    for route in ideations_router.routes:
        endpoint = getattr(route, "endpoint", None)
        if endpoint is None:
            continue
        sig = inspect.signature(endpoint)
        assert "db" not in sig.parameters, f"{endpoint.__name__} still takes db"


def test_ideations_router_has_no_endpoint_on_get_db() -> None:
    from okto_pulse.core.infra.database import get_db as _get_db

    checked = 0
    for route in ideations_router.routes:
        endpoint = getattr(route, "endpoint", None)
        if endpoint is None:
            continue
        checked += 1
        for param in inspect.signature(endpoint).parameters.values():
            assert getattr(param.default, "dependency", None) is not _get_db, (
                f"{endpoint.__name__} still depends on get_db"
            )
    assert checked > 0
