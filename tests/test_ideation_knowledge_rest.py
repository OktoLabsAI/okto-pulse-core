"""REST tests for ideation knowledge base endpoints."""

from __future__ import annotations

import uuid

import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.ideations import router as ideations_router
from okto_pulse.community.api import auth_deps as _auth_mod
from okto_pulse.core.infra.database import get_db, get_session_factory
from sqlalchemy_test_models import Board, Ideation


USER_ID = "ideation-kb-rest-user"


@pytest_asyncio.fixture
async def _client_and_ideation():
    db_factory = get_session_factory()
    board_id = f"ideation-kb-board-{uuid.uuid4()}"
    ideation_id = f"ideation-kb-{uuid.uuid4()}"

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Ideation KB REST Board", owner_id=USER_ID))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Ideation KB REST",
                created_by=USER_ID,
            )
        )
        await db.commit()

    app = FastAPI()
    app.include_router(ideations_router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID
    app.dependency_overrides[_auth_mod.get_realm_id] = lambda: "local"

    return TestClient(app), ideation_id


def test_create_list_get_delete_ideation_knowledge(_client_and_ideation):
    client, ideation_id = _client_and_ideation
    payload = {
        "title": "Discovery notes",
        "description": "context",
        "content": "Important ideation context",
    }

    created_resp = client.post(f"/api/v1/ideations/{ideation_id}/knowledge", json=payload)
    assert created_resp.status_code == 201, created_resp.text
    created = created_resp.json()
    assert created["title"] == "Discovery notes"
    assert created["content"] == "Important ideation context"

    listing_resp = client.get(f"/api/v1/ideations/{ideation_id}/knowledge")
    assert listing_resp.status_code == 200
    listing = listing_resp.json()
    assert any(item["id"] == created["id"] for item in listing)

    got_resp = client.get(f"/api/v1/ideations/{ideation_id}/knowledge/{created['id']}")
    assert got_resp.status_code == 200
    assert got_resp.json()["description"] == "context"

    deleted_resp = client.delete(f"/api/v1/ideations/{ideation_id}/knowledge/{created['id']}")
    assert deleted_resp.status_code == 204

    after_resp = client.get(f"/api/v1/ideations/{ideation_id}/knowledge")
    assert after_resp.status_code == 200
    assert created["id"] not in [item["id"] for item in after_resp.json()]
