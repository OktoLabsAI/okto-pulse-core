from __future__ import annotations

import uuid

import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.guidelines import router as guidelines_router
from okto_pulse.community.api import auth_deps as _auth_mod
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
from sqlalchemy_test_models import Board


USER_ID = "guidelines-api-user"


@pytest_asyncio.fixture
async def _client_and_board():
    db_factory = get_session_factory()
    board_id = f"guidelines-board-{uuid.uuid4()}"

    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Guidelines API Board",
                owner_id=USER_ID,
                realm_id="local",
            )
        )
        await db.commit()

    app = FastAPI()
    app.include_router(guidelines_router, prefix="/api/v1")

    async def _override_uow():
        async with db_factory() as session:
            try:
                yield resolve_unit_of_work_factory().wrap(session)
                await session.commit()
            except BaseException:
                await session.rollback()
                raise

    app.dependency_overrides[get_unit_of_work] = _override_uow
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID
    app.dependency_overrides[_auth_mod.get_realm_id] = lambda: "local"

    return TestClient(app), board_id


def test_create_inline_board_guideline(_client_and_board):
    client, board_id = _client_and_board

    created = client.post(
        f"/api/v1/boards/{board_id}/guidelines",
        json={
            "title": "Keep specs actionable",
            "content": "Every spec must include acceptance criteria.",
            "tags": ["specs", "quality"],
        },
    )

    assert created.status_code == 201, created.text
    assert created.json()["scope"] == "inline"

    listed = client.get(f"/api/v1/boards/{board_id}/guidelines")
    assert listed.status_code == 200
    body = listed.json()
    assert len(body) == 1
    assert body[0]["scope"] == "inline"
    assert body[0]["guideline"]["title"] == "Keep specs actionable"
    assert body[0]["guideline"]["board_id"] == board_id


def test_link_global_board_guideline_still_works(_client_and_board):
    client, board_id = _client_and_board

    global_resp = client.post(
        "/api/v1/guidelines",
        json={
            "title": "Global review rule",
            "content": "Review all API contracts before approval.",
            "tags": ["review"],
            "scope": "global",
        },
    )
    assert global_resp.status_code == 201, global_resp.text
    guideline_id = global_resp.json()["id"]

    linked = client.post(
        f"/api/v1/boards/{board_id}/guidelines",
        json={"guideline_id": guideline_id, "priority": 2},
    )

    assert linked.status_code == 201, linked.text
    assert linked.json()["guideline_id"] == guideline_id
    assert linked.json()["priority"] == 2
