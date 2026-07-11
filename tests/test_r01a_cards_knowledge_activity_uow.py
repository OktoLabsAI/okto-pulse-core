"""Spec R01A REST-FU4-S4 — Card activity / seen-status / knowledge on the
UnitOfWork. This card CLOSES ``api/cards.py``: afterwards no endpoint there binds
``get_db``.

The eight remaining ``api/cards.py`` endpoints now route through the ``card_crud``
use cases + ``get_unit_of_work``; each adapter only maps the result/errors to HTTP.
The activity + seen SQL that ran inline on the request session moved to the
transport-free readers ``compute_card_activity`` / ``compute_card_seen_status`` in
``services/main.py`` (the ``compute_*`` pattern) so the strangled use-case layer
never touches ``select``/ORM (the relational ratchet gate). Oracles assert the
legacy observable contract end-to-end via TestClient (status codes + body shape):

* get_card_activity     — 200 (newest-first projection), empty list (unknown card),
                          ``limit`` honored
* get_card_seen_status  — 200 (``{"items": {...}}`` grouped by item id),
                          ``{"items": {}}`` when the card has no comment/QA items
* list_card_knowledge   — 200 (``{card_id, knowledge}`` envelope), 404 (missing card)
* get_card_knowledge    — 200 (entry), 404 (missing card), 404 (unknown kb id)
* download_card_knowledge — 200 (markdown body + Content-Disposition), 404 (card),
                          404 (unknown kb id)
* create/update/delete_card_knowledge — 409 (read-only governed snapshots, blocked)

Plus a use-case-level ``EntityNotFoundError`` probe (the ``entity_type`` "card" vs
"card_knowledge" that drives get/download's two distinct 404 details), an AST
signature check proving every migrated endpoint takes ``uow`` (not a raw
``AsyncSession``), and a whole-module assertion that ZERO ``api/cards.py``
endpoints still bind ``get_db`` (the card's closing claim).
"""

from __future__ import annotations

import inspect
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import cards as cards_api
from okto_pulse.community.api.cards import router as cards_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu4-s4-user"
PREFIX = "/api/v1/cards"
_ENDPOINTS = (
    "get_card_activity",
    "get_card_seen_status",
    "list_card_knowledge",
    "create_card_knowledge",
    "get_card_knowledge",
    "update_card_knowledge",
    "delete_card_knowledge",
    "download_card_knowledge",
)

_T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


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


def _missing() -> str:
    return f"card-missing-{uuid.uuid4().hex[:8]}"


async def _seed_card(*, knowledge_bases: list | None = None) -> tuple[str, str]:
    """Seed Board + Spec + Card via raw models. Returns (board_id, card_id).

    The card just needs to satisfy the "every card belongs to a spec" invariant —
    these endpoints read activity/seen/knowledge, not create, so this deliberately
    bypasses ``CardService.create_card``'s spec-status gate.
    """
    from sqlalchemy_test_models import Board, Card, CardStatus, Spec

    bid = f"board-fu4s4-{uuid.uuid4().hex[:8]}"
    sid = f"spec-fu4s4-{uuid.uuid4().hex[:8]}"
    cid = f"card-fu4s4-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu4s4", owner_id=USER))
        db.add(Spec(id=sid, board_id=bid, title="fu4s4-spec", created_by=USER))
        db.add(
            Card(
                id=cid,
                board_id=bid,
                spec_id=sid,
                title=f"fu4-s4-card-{uuid.uuid4().hex[:6]}",
                created_by=USER,
                status=CardStatus.NOT_STARTED,
                knowledge_bases=knowledge_bases,
            )
        )
        await db.commit()
    return bid, cid


async def _seed_activity(board_id: str, card_id: str, action: str, *, created_at: datetime) -> str:
    from sqlalchemy_test_models import ActivityLog

    log_id = f"act-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            ActivityLog(
                id=log_id,
                board_id=board_id,
                card_id=card_id,
                action=action,
                actor_type="user",
                actor_id=USER,
                actor_name="Owner",
                details={"title": "demo", "status": "not_started"},
                created_at=created_at,
            )
        )
        await db.commit()
    return log_id


async def _seed_comment_and_seen(card_id: str) -> tuple[str, str, str]:
    """Seed a comment on the card + an agent that has seen it. Returns
    (comment_id, agent_id, agent_name)."""
    from sqlalchemy_test_models import Agent, AgentSeenItem, Comment

    comment_id = f"cmt-{uuid.uuid4().hex[:8]}"
    agent_id = f"agent-{uuid.uuid4().hex[:8]}"
    agent_name = f"Seer-{uuid.uuid4().hex[:4]}"
    async with get_session_factory()() as db:
        db.add(Comment(id=comment_id, card_id=card_id, content="hello", author_id=USER))
        db.add(
            Agent(
                id=agent_id,
                name=agent_name,
                api_key=f"key-{uuid.uuid4().hex}",
                api_key_hash=f"hash-{uuid.uuid4().hex}",
                created_by=USER,
            )
        )
        db.add(
            AgentSeenItem(
                id=f"seen-{uuid.uuid4().hex[:8]}",
                agent_id=agent_id,
                item_type="comment",
                item_id=comment_id,
                seen_at=_T0,
            )
        )
        await db.commit()
    return comment_id, agent_id, agent_name


# --- activity ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_activity_200_newest_first(client) -> None:
    board_id, card_id = await _seed_card()
    older = await _seed_activity(board_id, card_id, "card_created", created_at=_T0)
    newer = await _seed_activity(
        board_id, card_id, "card_updated", created_at=_T0 + timedelta(hours=1)
    )
    resp = client.get(f"{PREFIX}/{card_id}/activity")
    assert resp.status_code == 200, resp.text
    rows = resp.json()
    assert [r["id"] for r in rows] == [newer, older]
    # Projection carries the derived presentation fields.
    assert rows[0]["card_id"] == card_id
    assert rows[0]["action"] == "card_updated"
    assert "summary" in rows[0] and "details" in rows[0]


@pytest.mark.asyncio
async def test_activity_200_empty_for_unknown_card(client) -> None:
    # Unknown card id yields an empty list (no 404), exactly as the legacy endpoint.
    resp = client.get(f"{PREFIX}/{_missing()}/activity")
    assert resp.status_code == 200, resp.text
    assert resp.json() == []


@pytest.mark.asyncio
async def test_activity_limit_is_honored(client) -> None:
    board_id, card_id = await _seed_card()
    for i in range(3):
        await _seed_activity(
            board_id, card_id, "card_updated", created_at=_T0 + timedelta(hours=i)
        )
    resp = client.get(f"{PREFIX}/{card_id}/activity", params={"limit": 2})
    assert resp.status_code == 200, resp.text
    assert len(resp.json()) == 2


# --- seen status ------------------------------------------------------------


@pytest.mark.asyncio
async def test_seen_200_grouped_by_item(client) -> None:
    _, card_id = await _seed_card()
    comment_id, agent_id, agent_name = await _seed_comment_and_seen(card_id)
    resp = client.get(f"{PREFIX}/{card_id}/seen")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body["items"].keys()) == {comment_id}
    entry = body["items"][comment_id]
    # seen_at is asserted tz-tolerantly: SQLite does not round-trip tzinfo, so the
    # serialized value is naive under the test DB while Postgres keeps the offset.
    assert len(entry) == 1
    assert entry[0]["agent_id"] == agent_id
    assert entry[0]["agent_name"] == agent_name
    assert entry[0]["seen_at"].startswith("2026-01-01T12:00:00")


@pytest.mark.asyncio
async def test_seen_200_empty_when_no_items(client) -> None:
    # A card with no comments/QA returns {"items": {}} (also covers unknown card).
    _, card_id = await _seed_card()
    resp = client.get(f"{PREFIX}/{card_id}/seen")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"items": {}}

    unknown = client.get(f"{PREFIX}/{_missing()}/seen")
    assert unknown.status_code == 200, unknown.text
    assert unknown.json() == {"items": {}}


# --- knowledge: list --------------------------------------------------------


@pytest.mark.asyncio
async def test_list_knowledge_200_envelope(client) -> None:
    kbs = [
        {"id": "kb-1", "title": "Alpha", "content": "a", "description": "first"},
        {"id": "kb-2", "title": "Beta", "content": "b", "description": "second"},
    ]
    _, card_id = await _seed_card(knowledge_bases=kbs)
    resp = client.get(f"{PREFIX}/{card_id}/knowledge")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"card_id": card_id, "knowledge": kbs}


@pytest.mark.asyncio
async def test_list_knowledge_200_empty(client) -> None:
    _, card_id = await _seed_card()
    resp = client.get(f"{PREFIX}/{card_id}/knowledge")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"card_id": card_id, "knowledge": []}


@pytest.mark.asyncio
async def test_list_knowledge_404_missing_card(client) -> None:
    resp = client.get(f"{PREFIX}/{_missing()}/knowledge")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Card not found"


# --- knowledge: get single --------------------------------------------------


@pytest.mark.asyncio
async def test_get_knowledge_200_and_404s(client) -> None:
    kb = {"id": "kb-1", "title": "Alpha", "content": "a", "description": "first"}
    _, card_id = await _seed_card(knowledge_bases=[kb])

    found = client.get(f"{PREFIX}/{card_id}/knowledge/kb-1")
    assert found.status_code == 200, found.text
    assert found.json() == kb

    # Unknown kb id on an existing card → "Knowledge entry not found".
    missing_kb = client.get(f"{PREFIX}/{card_id}/knowledge/nope")
    assert missing_kb.status_code == 404
    assert missing_kb.json()["detail"] == "Knowledge entry not found"

    # Missing card → "Card not found" (the other 404 detail).
    missing_card = client.get(f"{PREFIX}/{_missing()}/knowledge/kb-1")
    assert missing_card.status_code == 404
    assert missing_card.json()["detail"] == "Card not found"


# --- knowledge: download ----------------------------------------------------


@pytest.mark.asyncio
async def test_download_knowledge_200_markdown(client) -> None:
    kb = {"id": "kb-1", "title": "My Title", "content": "body text", "description": "desc"}
    _, card_id = await _seed_card(knowledge_bases=[kb])
    resp = client.get(f"{PREFIX}/{card_id}/knowledge/kb-1/download")
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("text/markdown")
    assert resp.headers["content-disposition"] == 'attachment; filename="My_Title.md"'
    assert resp.text == "# My Title\n\n> desc\n\nbody text\n"


@pytest.mark.asyncio
async def test_download_knowledge_404s(client) -> None:
    _, card_id = await _seed_card(knowledge_bases=[{"id": "kb-1", "title": "x", "content": "y"}])

    missing_kb = client.get(f"{PREFIX}/{card_id}/knowledge/nope/download")
    assert missing_kb.status_code == 404
    assert missing_kb.json()["detail"] == "Knowledge entry not found"

    missing_card = client.get(f"{PREFIX}/{_missing()}/knowledge/kb-1/download")
    assert missing_card.status_code == 404
    assert missing_card.json()["detail"] == "Card not found"


# --- knowledge: blocked writes (409) ----------------------------------------


@pytest.mark.asyncio
async def test_create_knowledge_409_blocked(client) -> None:
    _, card_id = await _seed_card()
    resp = client.post(
        f"{PREFIX}/{card_id}/knowledge",
        json={"title": "t", "content": "c"},
    )
    assert resp.status_code == 409, resp.text


@pytest.mark.asyncio
async def test_update_knowledge_409_blocked(client) -> None:
    _, card_id = await _seed_card(knowledge_bases=[{"id": "kb-1", "title": "x", "content": "y"}])
    resp = client.patch(f"{PREFIX}/{card_id}/knowledge/kb-1", json={"title": "new"})
    assert resp.status_code == 409, resp.text


@pytest.mark.asyncio
async def test_delete_knowledge_409_blocked(client) -> None:
    _, card_id = await _seed_card(knowledge_bases=[{"id": "kb-1", "title": "x", "content": "y"}])
    resp = client.delete(f"{PREFIX}/{card_id}/knowledge/kb-1")
    assert resp.status_code == 409, resp.text


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_get_knowledge_use_case_entity_type_discrimination() -> None:
    """The ``entity_type`` ("card" vs "card_knowledge") drives the adapter's two
    distinct 404 details for get/download."""
    from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
    from okto_pulse.core.application.use_cases.card_crud import (
        GetCardKnowledgeCommand,
        GetCardKnowledgeUseCase,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")

    # Missing card → entity_type "card".
    with pytest.raises(EntityNotFoundError) as missing_card:
        async with uowf(actor=actor) as uow:
            await GetCardKnowledgeUseCase().execute(
                GetCardKnowledgeCommand(_missing(), "kb-1"), actor=actor, uow=uow
            )
    assert missing_card.value.entity_type == "card"

    # Existing card, unknown kb → entity_type "card_knowledge".
    _, card_id = await _seed_card(knowledge_bases=[{"id": "kb-1", "title": "x", "content": "y"}])
    with pytest.raises(EntityNotFoundError) as missing_kb:
        async with uowf(actor=actor) as uow:
            await GetCardKnowledgeUseCase().execute(
                GetCardKnowledgeCommand(card_id, "ghost"), actor=actor, uow=uow
            )
    assert missing_kb.value.entity_type == "card_knowledge"


def test_fu4_s4_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(cards_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def test_cards_api_has_zero_get_db_endpoints() -> None:
    """The card's closing claim: NO ``api/cards.py`` route endpoint binds ``get_db``.

    Walk every registered route on the router and assert no endpoint parameter
    defaults to ``Depends(get_db)`` — every persistence-bound endpoint now depends
    on ``get_unit_of_work`` instead.
    """
    checked = 0
    for route in cards_router.routes:
        endpoint = getattr(route, "endpoint", None)
        if endpoint is None:
            continue
        checked += 1
        for param in inspect.signature(endpoint).parameters.values():
            default = param.default
            if isinstance(default, type(Depends(get_db))):
                assert default.dependency is not get_db, (
                    f"{endpoint.__name__} still binds get_db"
                )
    assert checked >= len(_ENDPOINTS)
