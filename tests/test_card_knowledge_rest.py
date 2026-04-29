"""TC-2 (TS2) — REST endpoints for card.knowledge_bases including the
markdown download (Content-Disposition: attachment).

Uses FastAPI's TestClient + the test database factory wired by conftest.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from okto_pulse.core.api.cards import router as cards_router
from okto_pulse.core.infra import auth as _auth_mod
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.db import Board, Card, CardStatus, CardType, Spec, SpecStatus


BOARD_ID = "card-kb-rest-board-001"
USER_ID = "card-kb-rest-agent-001"


@pytest_asyncio.fixture
async def _client_and_card():
    from fastapi import FastAPI
    from okto_pulse.core.infra.database import get_session_factory

    db_factory = get_session_factory()

    spec_id = str(uuid.uuid4())
    card_id = str(uuid.uuid4())
    async with db_factory() as db:
        if await db.get(Board, BOARD_ID) is None:
            db.add(Board(id=BOARD_ID, name="Card KB REST", owner_id=USER_ID))
            await db.flush()
        db.add(Spec(
            id=spec_id, board_id=BOARD_ID, title="Card KB REST Spec",
            status=SpecStatus.APPROVED, created_by=USER_ID,
            functional_requirements=["FR1"], acceptance_criteria=["AC1"],
            test_scenarios=[], business_rules=[], api_contracts=[],
        ))
        db.add(Card(
            id=card_id, board_id=BOARD_ID, spec_id=spec_id,
            title="Card for REST tests", status=CardStatus.NOT_STARTED,
            card_type=CardType.NORMAL, created_by=USER_ID,
        ))
        await db.commit()

    app = FastAPI()
    app.include_router(cards_router, prefix="/api/v1/cards")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID

    return TestClient(app), card_id


def test_create_then_get_card_knowledge(_client_and_card):
    client, card_id = _client_and_card
    payload = {"title": "REST KE", "content": "body via REST", "description": "desc"}
    resp = client.post(f"/api/v1/cards/{card_id}/knowledge", json=payload)
    assert resp.status_code == 201, resp.text
    kb = resp.json()
    assert kb["id"].startswith("kb_")
    assert kb["title"] == "REST KE"

    got = client.get(f"/api/v1/cards/{card_id}/knowledge/{kb['id']}")
    assert got.status_code == 200
    assert got.json()["content"] == "body via REST"


def test_list_returns_inserted_kb(_client_and_card):
    client, card_id = _client_and_card
    resp = client.post(
        f"/api/v1/cards/{card_id}/knowledge",
        json={"title": "L1", "content": "c1"},
    )
    assert resp.status_code == 201
    listing = client.get(f"/api/v1/cards/{card_id}/knowledge")
    assert listing.status_code == 200
    body = listing.json()
    assert body["card_id"] == card_id
    assert any(k["title"] == "L1" for k in body["knowledge"])


def test_patch_updates_inline(_client_and_card):
    client, card_id = _client_and_card
    created = client.post(
        f"/api/v1/cards/{card_id}/knowledge",
        json={"title": "old", "content": "old-content"},
    ).json()
    patched = client.patch(
        f"/api/v1/cards/{card_id}/knowledge/{created['id']}",
        json={"title": "new"},
    )
    assert patched.status_code == 200
    assert patched.json()["title"] == "new"
    assert patched.json()["content"] == "old-content"


def test_delete_removes_kb(_client_and_card):
    client, card_id = _client_and_card
    created = client.post(
        f"/api/v1/cards/{card_id}/knowledge",
        json={"title": "del", "content": "x"},
    ).json()
    rem = client.delete(f"/api/v1/cards/{card_id}/knowledge/{created['id']}")
    assert rem.status_code == 204

    after = client.get(f"/api/v1/cards/{card_id}/knowledge").json()
    assert created["id"] not in [k["id"] for k in after["knowledge"]]


def test_download_returns_markdown_with_attachment_header(_client_and_card):
    client, card_id = _client_and_card
    created = client.post(
        f"/api/v1/cards/{card_id}/knowledge",
        json={
            "title": "Auth design",
            "description": "summary",
            "content": "## body\n\nMarkdown body here",
        },
    ).json()
    dl = client.get(f"/api/v1/cards/{card_id}/knowledge/{created['id']}/download")
    assert dl.status_code == 200
    assert dl.headers["content-type"].startswith("text/markdown")
    cd = dl.headers["content-disposition"]
    assert "attachment" in cd
    assert ".md" in cd
    body = dl.text
    assert body.startswith("# Auth design")
    assert "Markdown body here" in body
