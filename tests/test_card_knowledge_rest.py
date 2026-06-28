"""REST endpoints for card.knowledge_bases governed snapshots.

Uses FastAPI's TestClient + the test database factory wired by conftest.
"""

from __future__ import annotations

import uuid

import pytest_asyncio
from fastapi.testclient import TestClient

from okto_pulse.core.api.cards import router as cards_router
from okto_pulse.core.infra import auth as _auth_mod
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.db import Board, Card, CardStatus, CardType, Spec, SpecStatus


BOARD_ID = "card-kb-rest-board-001"
USER_ID = "card-kb-rest-agent-001"
SEEDED_KB_ID = "cardkb_existing"
READ_ONLY_MESSAGE = "Card resources are read-only governed snapshots."


def _seeded_knowledge() -> dict:
    return {
        "id": SEEDED_KB_ID,
        "title": "Auth design",
        "description": "summary",
        "content": "## body\n\nMarkdown body here",
        "mime_type": "text/markdown",
        "source": "copied_from_spec:spec_source:kb_source",
        "source_kb_id": "kb_source",
        "author_id": USER_ID,
    }


def _assert_read_only(response):
    assert response.status_code == 409, response.text
    assert READ_ONLY_MESSAGE in response.json()["detail"]


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
            knowledge_bases=[_seeded_knowledge()],
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


def test_get_card_knowledge_snapshot(_client_and_card):
    client, card_id = _client_and_card
    got = client.get(f"/api/v1/cards/{card_id}/knowledge/{SEEDED_KB_ID}")
    assert got.status_code == 200
    body = got.json()
    assert body["id"] == SEEDED_KB_ID
    assert body["title"] == "Auth design"
    assert body["content"] == "## body\n\nMarkdown body here"


def test_list_returns_seeded_card_knowledge_snapshot(_client_and_card):
    client, card_id = _client_and_card
    listing = client.get(f"/api/v1/cards/{card_id}/knowledge")
    assert listing.status_code == 200
    body = listing.json()
    assert body["card_id"] == card_id
    assert [k["id"] for k in body["knowledge"]] == [SEEDED_KB_ID]


def test_create_card_knowledge_is_read_only(_client_and_card):
    client, card_id = _client_and_card
    created = client.post(
        f"/api/v1/cards/{card_id}/knowledge",
        json={"title": "Direct", "content": "blocked"},
    )
    _assert_read_only(created)


def test_patch_card_knowledge_is_read_only(_client_and_card):
    client, card_id = _client_and_card
    patched = client.patch(
        f"/api/v1/cards/{card_id}/knowledge/{SEEDED_KB_ID}",
        json={"title": "new"},
    )
    _assert_read_only(patched)

    got = client.get(f"/api/v1/cards/{card_id}/knowledge/{SEEDED_KB_ID}")
    assert got.status_code == 200
    assert got.json()["title"] == "Auth design"


def test_delete_card_knowledge_is_read_only(_client_and_card):
    client, card_id = _client_and_card
    rem = client.delete(f"/api/v1/cards/{card_id}/knowledge/{SEEDED_KB_ID}")
    _assert_read_only(rem)

    after = client.get(f"/api/v1/cards/{card_id}/knowledge").json()
    assert SEEDED_KB_ID in [k["id"] for k in after["knowledge"]]


def test_download_returns_markdown_with_attachment_header(_client_and_card):
    client, card_id = _client_and_card
    dl = client.get(f"/api/v1/cards/{card_id}/knowledge/{SEEDED_KB_ID}/download")
    assert dl.status_code == 200
    assert dl.headers["content-type"].startswith("text/markdown")
    cd = dl.headers["content-disposition"]
    assert "attachment" in cd
    assert ".md" in cd
    body = dl.text
    assert body.startswith("# Auth design")
    assert "Markdown body here" in body
