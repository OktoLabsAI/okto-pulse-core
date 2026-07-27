"""Spec R01A REST-FU7-S4 — Discovery read/execute endpoints on the UnitOfWork.

The five ``api/discovery.py`` endpoints (intent catalog, board selector options,
saved searches, per-user search history, intent execution) now route through the
transport-free discovery use cases + ``get_unit_of_work``; each adapter only maps
the result/errors to HTTP. The inline SQL moved to ``DiscoveryCatalogReader`` and
the selector read policy to ``DiscoverySelectorRestAccessPolicy``.

Oracles: intents 200 (active-only), saved-searches 200 (board-scoped),
search-history 200 (user-scoped), selector-options 403 (no board read) + 200
(owner, empty board), execute 400 (missing board_id) + 404 (missing/inactive
intent), the use cases raising the typed errors for the adapter to map, and an
AST signature check proving every endpoint takes ``uow`` (not a raw
``AsyncSession``).

NOTE: NOT executed by this card — written as the dedicated oracle.
"""

from __future__ import annotations

import inspect
import uuid
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import discovery as discovery_api
from okto_pulse.community.api.discovery import router as discovery_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu7-s4-user"
OTHER_USER = "r01a-fu7-s4-other"
PREFIX = "/api/v1"
_ENDPOINTS = (
    "list_discovery_intents",
    "list_discovery_selector_options",
    "list_saved_searches",
    "list_search_history",
    "execute_discovery_intent",
)


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(discovery_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    return TestClient(app)


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _seed_intent(*, active: bool = True, category: str = "explore") -> str:
    from sqlalchemy_test_models import DiscoveryIntent

    iid = f"intent-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            DiscoveryIntent(
                id=iid,
                name=f"name-{uuid.uuid4().hex[:8]}",
                label="Intent label",
                category=category,
                tool_binding="okto_pulse_list_my_mentions",
                params_schema=None,
                active=active,
                created_at=_now(),
                updated_at=_now(),
            )
        )
        await db.commit()
        return iid


async def _seed_board(owner_id: str = USER) -> str:
    from sqlalchemy_test_models import Board

    bid = f"board-fu7s4-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu7s4", owner_id=owner_id))
        await db.commit()
        return bid


async def _seed_saved_search(board_id: str) -> str:
    from sqlalchemy_test_models import DiscoverySavedSearch

    sid = f"saved-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            DiscoverySavedSearch(
                id=sid,
                board_id=board_id,
                name="My saved search",
                query="alpha",
                created_by=USER,
                created_at=_now(),
            )
        )
        await db.commit()
        return sid


async def _seed_history(board_id: str, user_id: str) -> str:
    from sqlalchemy_test_models import DiscoverySearchHistory

    hid = f"hist-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            DiscoverySearchHistory(
                id=hid,
                board_id=board_id,
                user_id=user_id,
                query="beta",
                result_count=3,
                searched_at=_now(),
            )
        )
        await db.commit()
        return hid


def _missing() -> str:
    return f"missing-{uuid.uuid4().hex[:8]}"


# --- intent catalog ---------------------------------------------------------


@pytest.mark.asyncio
async def test_list_intents_returns_active_only(client) -> None:
    active_id = await _seed_intent(active=True)
    inactive_id = await _seed_intent(active=False)
    resp = client.get(f"{PREFIX}/discovery/intents")
    assert resp.status_code == 200, resp.text
    ids = {row["id"] for row in resp.json()}
    assert active_id in ids
    assert inactive_id not in ids


# --- saved searches ---------------------------------------------------------


@pytest.mark.asyncio
async def test_list_saved_searches_scoped_to_board(client) -> None:
    board_id = await _seed_board()
    saved_id = await _seed_saved_search(board_id)
    other_board = await _seed_board()
    await _seed_saved_search(other_board)

    resp = client.get(f"{PREFIX}/discovery/boards/{board_id}/saved-searches")
    assert resp.status_code == 200, resp.text
    ids = {row["id"] for row in resp.json()}
    assert ids == {saved_id}


@pytest.mark.asyncio
async def test_list_saved_searches_foreign_board_is_non_enumerable(client) -> None:
    board_id = await _seed_board(owner_id=OTHER_USER)
    saved_id = await _seed_saved_search(board_id)

    resp = client.get(f"{PREFIX}/discovery/boards/{board_id}/saved-searches")

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"
    assert saved_id not in resp.text


# --- search history ---------------------------------------------------------


@pytest.mark.asyncio
async def test_list_search_history_scoped_to_user(client) -> None:
    board_id = await _seed_board()
    mine = await _seed_history(board_id, USER)
    theirs = await _seed_history(board_id, OTHER_USER)

    resp = client.get(f"{PREFIX}/discovery/boards/{board_id}/search-history")
    assert resp.status_code == 200, resp.text
    ids = {row["id"] for row in resp.json()}
    assert mine in ids
    assert theirs not in ids


@pytest.mark.asyncio
async def test_list_search_history_foreign_board_is_non_enumerable(client) -> None:
    board_id = await _seed_board(owner_id=OTHER_USER)
    history_id = await _seed_history(board_id, USER)

    resp = client.get(f"{PREFIX}/discovery/boards/{board_id}/search-history")

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"
    assert history_id not in resp.text


# --- selector options -------------------------------------------------------


@pytest.mark.asyncio
async def test_selector_options_forbidden_without_board_read(client) -> None:
    # No board (or no permission) → explicit pre-check denies → 403.
    resp = client.get(
        f"{PREFIX}/discovery/boards/{_missing()}/selector-options"
    )
    assert resp.status_code == 403
    assert resp.json()["detail"] == {"error": "selector_access_denied"}


@pytest.mark.asyncio
async def test_selector_options_owner_empty_board_ok(client) -> None:
    board_id = await _seed_board(owner_id=USER)
    resp = client.get(
        f"{PREFIX}/discovery/boards/{board_id}/selector-options"
    )
    assert resp.status_code == 200, resp.text
    assert isinstance(resp.json(), dict)


# --- execute intent ---------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_intent_missing_board_id_400(client) -> None:
    intent_id = await _seed_intent(active=True)
    resp = client.post(f"{PREFIX}/discovery/intents/{intent_id}/execute", json={})
    assert resp.status_code == 400
    assert resp.json()["detail"] == "board_id is required"


@pytest.mark.asyncio
async def test_execute_intent_unknown_intent_404(client) -> None:
    resp = client.post(
        f"{PREFIX}/discovery/intents/{_missing()}/execute",
        json={"board_id": "some-board"},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Intent not found"


@pytest.mark.asyncio
async def test_execute_intent_inactive_intent_404(client) -> None:
    intent_id = await _seed_intent(active=False)
    resp = client.post(
        f"{PREFIX}/discovery/intents/{intent_id}/execute",
        json={"board_id": "some-board"},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Intent not found"


# --- use case level ---------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_use_case_raises_for_missing_intent() -> None:
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.application.use_cases.discovery_crud import (
        ExecuteDiscoveryIntentCommand,
        ExecuteDiscoveryIntentUseCase,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await ExecuteDiscoveryIntentUseCase().execute(
                ExecuteDiscoveryIntentCommand(_missing(), "some-board", {}),
                actor=actor,
                uow=uow,
            )


@pytest.mark.asyncio
async def test_list_intents_use_case_returns_active_only() -> None:
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.discovery_crud import (
        ListDiscoveryIntentsCommand,
        ListDiscoveryIntentsUseCase,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    active_id = await _seed_intent(active=True)
    inactive_id = await _seed_intent(active=False)
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    async with uowf(actor=actor) as uow:
        result = await ListDiscoveryIntentsUseCase().execute(
            ListDiscoveryIntentsCommand(), actor=actor, uow=uow
        )
    ids = {intent.id for intent in result.intents}
    assert active_id in ids
    assert inactive_id not in ids


# --- AST signature ----------------------------------------------------------


def test_fu7_s4_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(discovery_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
