"""Spec R01A REST-FU7-S1 — Board endpoints on the UnitOfWork.

The ``api/boards.py`` endpoints that still bound a raw request session — list /
get / update / delete board, create-card, columns, archive/restore tree, and the
board-share CRUD — now route through the ``boards_crud`` use cases +
``get_unit_of_work``; each adapter only maps the result/errors to HTTP. Oracles
exercise the migrated status codes + bodies (list 200, get 200/404, update 200
re-fetched body / 404, delete 204→404, create-card 404 / 400, columns 200 / 404,
archive+restore 200 + bad-entity 400, share 201 / 403-self, list-shares 200 /
404, update-share 200 / 403, revoke-share 204 / 403), the use case raising
``EntityNotFoundError`` for a missing board, and an AST signature check proving
every board endpoint takes ``uow`` (not a raw ``AsyncSession``).
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import boards as boards_api
from okto_pulse.community.api.boards import router as boards_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import get_realm_id, require_user
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu7-s1-user"
OTHER = "r01a-fu7-s1-other"
PREFIX = "/api/v1/boards"

# Every board endpoint must take ``uow`` (the whole module is get_db-free now).
_ENDPOINTS = (
    "create_board",
    "list_boards",
    "get_board",
    "update_board",
    "delete_board",
    "create_card",
    "get_board_columns",
    "archive_tree",
    "restore_tree",
    "share_board",
    "list_board_shares",
    "update_board_share",
    "revoke_board_share",
)


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(boards_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    app.dependency_overrides[get_realm_id] = lambda: LOCAL_REALM_ID
    return TestClient(app)


async def _seed_board(owner: str = USER, name: str = "fu7s1") -> str:
    from sqlalchemy_test_models import Board

    bid = f"board-fu7s1-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=bid,
                name=name,
                owner_id=owner,
                realm_id=LOCAL_REALM_ID,
            )
        )
        await db.commit()
        return bid


async def _seed_board_spec_card() -> tuple[str, str, str]:
    """Board + Spec + Card via raw models (bypasses CardService.create_card's
    spec-status gate — these endpoints read/group/archive, not create)."""
    from sqlalchemy_test_models import Board, Card, Spec

    bid = f"board-fu7s1-{uuid.uuid4().hex[:8]}"
    sid = f"spec-fu7s1-{uuid.uuid4().hex[:8]}"
    cid = f"card-fu7s1-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=bid,
                name="fu7s1",
                owner_id=USER,
                realm_id=LOCAL_REALM_ID,
            )
        )
        db.add(Spec(id=sid, board_id=bid, title="fu7s1-spec", created_by=USER))
        db.add(Card(id=cid, board_id=bid, spec_id=sid, title="fu7s1-card", created_by=USER))
        await db.commit()
        return bid, sid, cid


def _missing(kind: str = "board") -> str:
    return f"{kind}-missing-{uuid.uuid4().hex[:8]}"


# --- list -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_boards_200(client) -> None:
    board_id = await _seed_board()
    resp = client.get(PREFIX)
    assert resp.status_code == 200, resp.text
    assert board_id in {b["id"] for b in resp.json()}


# --- get --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_board_200(client) -> None:
    board_id = await _seed_board(name="fu7s1-get")
    resp = client.get(f"{PREFIX}/{board_id}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == board_id
    assert body["name"] == "fu7s1-get"


@pytest.mark.asyncio
async def test_get_board_compact_200(client) -> None:
    board_id = await _seed_board()
    resp = client.get(f"{PREFIX}/{board_id}", params={"compact": "true"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["id"] == board_id


@pytest.mark.asyncio
async def test_get_board_404(client) -> None:
    resp = client.get(f"{PREFIX}/{_missing()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


# --- update -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_board_200_refetched_body(client) -> None:
    board_id = await _seed_board()
    resp = client.patch(f"{PREFIX}/{board_id}", json={"name": "fu7s1-renamed"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == board_id
    assert body["name"] == "fu7s1-renamed"


@pytest.mark.asyncio
async def test_update_board_404(client) -> None:
    resp = client.patch(f"{PREFIX}/{_missing()}", json={"name": "nope"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


# --- delete -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_board_204_then_404(client) -> None:
    board_id = await _seed_board()
    resp = client.delete(f"{PREFIX}/{board_id}")
    assert resp.status_code == 204, resp.text
    gone = client.delete(f"{PREFIX}/{board_id}")
    assert gone.status_code == 404
    assert client.get(f"{PREFIX}/{board_id}").status_code == 404


@pytest.mark.asyncio
async def test_delete_board_404(client) -> None:
    resp = client.delete(f"{PREFIX}/{_missing()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


# --- create card ------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_card_board_not_found_404(client) -> None:
    # Valid CardCreate payload, but the board does not exist → create_card
    # returns None → EntityNotFoundError → the legacy 404 detail.
    resp = client.post(
        f"{PREFIX}/{_missing()}/cards",
        json={"title": "fu7s1-card", "spec_id": _missing("spec")},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found or not owned by user"


@pytest.mark.asyncio
async def test_create_card_missing_spec_400(client) -> None:
    # Board exists/owned, but no spec_id → CardService raises ValueError → 400.
    board_id = await _seed_board()
    resp = client.post(f"{PREFIX}/{board_id}/cards", json={"title": "fu7s1-card"})
    assert resp.status_code == 400, resp.text
    assert "spec" in resp.json()["detail"].lower()


# --- columns ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_board_columns_200(client) -> None:
    board_id, _spec_id, card_id = await _seed_board_spec_card()
    resp = client.get(f"{PREFIX}/{board_id}/columns")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["board_id"] == board_id
    all_card_ids = {c["id"] for col in body["columns"].values() for c in col}
    assert card_id in all_card_ids


@pytest.mark.asyncio
async def test_get_board_columns_404(client) -> None:
    resp = client.get(f"{PREFIX}/{_missing()}/columns")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


# --- archive / restore ------------------------------------------------------


@pytest.mark.asyncio
async def test_archive_then_restore_spec_200(client) -> None:
    board_id, spec_id, _card_id = await _seed_board_spec_card()

    arch = client.post(f"{PREFIX}/{board_id}/archive/spec/{spec_id}")
    assert arch.status_code == 200, arch.text
    arch_body = arch.json()
    assert arch_body["success"] is True
    assert arch_body["archived_count"]["specs"] >= 1

    rest = client.post(f"{PREFIX}/{board_id}/restore/spec/{spec_id}")
    assert rest.status_code == 200, rest.text
    rest_body = rest.json()
    assert rest_body["success"] is True
    assert rest_body["restored_count"]["specs"] >= 1


@pytest.mark.asyncio
async def test_archive_invalid_entity_type_400(client) -> None:
    board_id = await _seed_board()
    resp = client.post(f"{PREFIX}/{board_id}/archive/bogus/{_missing('x')}")
    assert resp.status_code == 400, resp.text
    assert "entity_type" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_restore_invalid_entity_type_400(client) -> None:
    board_id = await _seed_board()
    resp = client.post(f"{PREFIX}/{board_id}/restore/bogus/{_missing('x')}")
    assert resp.status_code == 400, resp.text
    assert "entity_type" in resp.json()["detail"]


# --- shares -----------------------------------------------------------------


def _share(client, board_id: str, target: str, permission: str = "viewer"):
    return client.post(
        f"{PREFIX}/{board_id}/shares",
        json={"user_id": target, "permission": permission},
    )


@pytest.mark.asyncio
async def test_share_board_201(client) -> None:
    board_id = await _seed_board()
    resp = _share(client, board_id, OTHER, "viewer")
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["user_id"] == OTHER
    assert body["permission"] == "viewer"
    assert body["board_id"] == board_id


@pytest.mark.asyncio
async def test_share_board_with_self_403(client) -> None:
    board_id = await _seed_board()
    resp = _share(client, board_id, USER)
    assert resp.status_code == 403
    assert resp.json()["detail"] == (
        "Not authorized to share this board or invalid target user"
    )


@pytest.mark.asyncio
async def test_list_board_shares_200(client) -> None:
    board_id = await _seed_board()
    _share(client, board_id, OTHER, "editor")
    resp = client.get(f"{PREFIX}/{board_id}/shares")
    assert resp.status_code == 200, resp.text
    assert OTHER in {s["user_id"] for s in resp.json()}


@pytest.mark.asyncio
async def test_list_board_shares_no_access_404(client) -> None:
    # Board owned by someone else, no share for USER → no permission → 404.
    foreign_board = await _seed_board(owner=OTHER)
    resp = client.get(f"{PREFIX}/{foreign_board}/shares")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_update_board_share_200(client) -> None:
    board_id = await _seed_board()
    share_id = _share(client, board_id, OTHER, "viewer").json()["id"]
    resp = client.patch(
        f"{PREFIX}/{board_id}/shares/{share_id}", json={"permission": "admin"}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["permission"] == "admin"


@pytest.mark.asyncio
async def test_update_board_share_missing_403(client) -> None:
    board_id = await _seed_board()
    resp = client.patch(
        f"{PREFIX}/{board_id}/shares/{_missing('share')}", json={"permission": "admin"}
    )
    assert resp.status_code == 403
    assert resp.json()["detail"] == "Not authorized to update this share"


@pytest.mark.asyncio
async def test_revoke_board_share_204(client) -> None:
    board_id = await _seed_board()
    share_id = _share(client, board_id, OTHER).json()["id"]
    resp = client.delete(f"{PREFIX}/{board_id}/shares/{share_id}")
    assert resp.status_code == 204, resp.text


@pytest.mark.asyncio
async def test_revoke_board_share_missing_403(client) -> None:
    board_id = await _seed_board()
    resp = client.delete(f"{PREFIX}/{board_id}/shares/{_missing('share')}")
    assert resp.status_code == 403
    assert resp.json()["detail"] == "Not authorized to revoke this share"


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_get_board_use_case_raises_for_missing_board() -> None:
    from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
    from okto_pulse.core.application.use_cases.boards_crud import (
        GetBoardCommand,
        GetBoardUseCase,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest", realm_id=LOCAL_REALM_ID)
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await GetBoardUseCase().execute(
                GetBoardCommand(_missing()), actor=actor, uow=uow
            )


def test_fu7_s1_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(boards_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
