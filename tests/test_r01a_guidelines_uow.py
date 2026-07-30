"""Spec R01A REST-FU7-S3 — Guideline endpoints on the UnitOfWork.

The ``api/guidelines.py`` endpoints that still bound a raw request session — the
global guideline CRUD (list / create / get / update / delete) and the board
guideline family (board listing, link-or-create inline, unlink, priority update)
— now route through the ``guidelines_crud`` use cases + ``get_unit_of_work``;
each adapter only maps the result/errors to HTTP. Oracles exercise the migrated
status codes + bodies (list 200, create 201, get 200/404, update 200/404 +
not-owned 404, delete 204→404, board-list 200 / board-404 / foreign-board-404,
governed-link 409 without mutation, inline-create 201, the inline-create 422
with the EXACT legacy detail, link-board-404, unlink 204→404, governed-priority
409 without mutation), the use case raising ``EntityNotFoundError`` for a
missing board, and an AST signature check proving every guideline endpoint
takes ``uow`` (not a raw ``AsyncSession``).

The legacy guideline write endpoints are exercised with an explicit principal
that has both the introduced SK-B capabilities and their historical authority
ceilings.  Read scoping still relies on board ownership
(``BoardService.get_board`` → 404 when missing/not-owned/not-shared), exercised
here by ``test_get_board_guidelines_foreign_board_404`` (a board owned by
another user, not shared, returns the legacy "Board not found").
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import guidelines as guidelines_api
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.guidelines import router as guidelines_router
from okto_pulse.community.api.auth_deps import (
    get_realm_id,
    require_principal,
    require_user,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.ports.authentication import Principal

USER = "r01a-fu7-s3-user"
OTHER = "r01a-fu7-s3-other"
PREFIX = "/api/v1"

# Every guideline endpoint must take ``uow`` (the whole module is get_db-free now).
_ENDPOINTS = (
    "list_guidelines",
    "create_guideline",
    "get_guideline",
    "update_guideline",
    "delete_guideline",
    "get_board_guidelines",
    "link_or_create_board_guideline",
    "unlink_board_guideline",
    "update_board_guideline_priority",
)


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(guidelines_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    # get_unit_of_work depends on get_db, so overriding get_db keeps the UoW path.
    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    app.dependency_overrides[require_principal] = lambda: Principal(
        subject=USER,
        realm_id=LOCAL_REALM_ID,
        claims={
            "permissions": {
                "guidelines": {
                    "delete": True,
                    "revisions": {
                        "create": True,
                        "retire": True,
                    },
                    "adoption": {"manage": True},
                },
                # Historical ceiling shared by revisions.create and
                # adoption.manage.  Introduced leaves deliberately fail closed
                # without both sides of this authority bridge.
                "spec": {"entity": {"edit_fields": True}},
            }
        },
    )
    app.dependency_overrides[get_realm_id] = lambda: LOCAL_REALM_ID
    return TestClient(app)


async def _seed_board(owner: str = USER, name: str = "fu7s3") -> str:
    from sqlalchemy_test_models import Board

    bid = f"board-fu7s3-{uuid.uuid4().hex[:8]}"
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


async def _seed_guideline(owner: str = OTHER, *, scope: str = "global") -> str:
    """Create an authoritative guideline owned by ``owner``."""
    from okto_pulse.core.models.schemas import GuidelineCreate
    from okto_pulse.core.services.main import GuidelineService

    async with get_session_factory()() as db:
        guideline = await GuidelineService(db).create_guideline(
            owner,
            GuidelineCreate(
                title="foreign rule",
                content="owned by someone else",
                tags=["x"],
                scope=scope,
            ),
        )
        await db.commit()
        return guideline.id


def _missing(kind: str = "guideline") -> str:
    return f"{kind}-missing-{uuid.uuid4().hex[:8]}"


def _create_global(client, title: str = "Keep specs actionable") -> str:
    resp = client.post(
        f"{PREFIX}/guidelines",
        json={
            "title": title,
            "content": "Every spec must include acceptance criteria.",
            "tags": ["specs"],
            "scope": "global",
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


async def _link_guideline(
    board_id: str,
    guideline_id: str,
    *,
    priority: int = 0,
) -> None:
    """Seed a governed binding through its authoritative preview/adopt flow."""
    from okto_pulse.core.domain.guideline_policy import GuidelineEnforcement
    from okto_pulse.core.services.main import GuidelineService

    nonce = uuid.uuid4().hex
    async with get_session_factory()() as db:
        service = GuidelineService(db)
        receipt = await service.preview_guideline_revision_impact(
            board_id=board_id,
            guideline_id=guideline_id,
            proposed_priority=priority,
            proposed_default_enforcement=GuidelineEnforcement.ADVISORY,
            requested_by=USER,
            idempotency_key=f"r01a-preview:{nonce}",
        )
        binding, consumed_receipt = await service.adopt_guideline_revision(
            board_id=board_id,
            guideline_id=guideline_id,
            impact_receipt_id=receipt.impact_receipt_id,
            impact_digest=receipt.impact_digest,
            actor_id=USER,
            actor_type="user",
            idempotency_key=f"r01a-adopt:{nonce}",
        )
        assert binding.guideline_id == guideline_id
        assert consumed_receipt.impact_receipt_id == receipt.impact_receipt_id
        await db.commit()


def _assert_preview_required(resp) -> None:
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["code"] == "guideline_impact_preview_required"
    assert detail["next_action"] == "preview_then_adopt"


# --- global: list / create --------------------------------------------------


@pytest.mark.asyncio
async def test_list_guidelines_200(client) -> None:
    gid = _create_global(client, title="listed-rule")
    resp = client.get(f"{PREFIX}/guidelines")
    assert resp.status_code == 200, resp.text
    assert gid in {g["id"] for g in resp.json()}


@pytest.mark.asyncio
async def test_create_guideline_201(client) -> None:
    resp = client.post(
        f"{PREFIX}/guidelines",
        json={"title": "new-rule", "content": "body", "scope": "global"},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["title"] == "new-rule"
    assert body["owner_id"] == USER
    assert body["scope"] == "global"


# --- global: get ------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_guideline_200(client) -> None:
    gid = _create_global(client)
    resp = client.get(f"{PREFIX}/guidelines/{gid}")
    assert resp.status_code == 200, resp.text
    assert resp.json()["id"] == gid


@pytest.mark.asyncio
async def test_get_guideline_404(client) -> None:
    resp = client.get(f"{PREFIX}/guidelines/{_missing()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Guideline not found"


# --- global: update ---------------------------------------------------------


@pytest.mark.asyncio
async def test_update_guideline_200(client) -> None:
    gid = _create_global(client)
    resp = client.patch(f"{PREFIX}/guidelines/{gid}", json={"title": "renamed-rule"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["title"] == "renamed-rule"


@pytest.mark.asyncio
async def test_update_guideline_404(client) -> None:
    resp = client.patch(f"{PREFIX}/guidelines/{_missing()}", json={"title": "x"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Guideline not found or not owned by user"


@pytest.mark.asyncio
async def test_update_guideline_not_owned_404(client) -> None:
    # Guideline exists but is owned by OTHER → service returns None → the legacy
    # "not owned" 404, proving the owner gate is preserved (not dropped).
    foreign = await _seed_guideline(owner=OTHER)
    resp = client.patch(f"{PREFIX}/guidelines/{foreign}", json={"title": "hijack"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Guideline not found or not owned by user"


# --- global: delete ---------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_guideline_204_then_404(client) -> None:
    gid = _create_global(client)
    resp = client.delete(f"{PREFIX}/guidelines/{gid}")
    assert resp.status_code == 204, resp.text
    gone = client.delete(f"{PREFIX}/guidelines/{gid}")
    assert gone.status_code == 404
    assert gone.json()["detail"] == "Guideline not found or not owned by user"


# --- board: list ------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_board_guidelines_200(client) -> None:
    board_id = await _seed_board()
    created = client.post(
        f"{PREFIX}/boards/{board_id}/guidelines",
        json={"title": "inline rule", "content": "do the thing", "tags": ["q"]},
    )
    assert created.status_code == 201, created.text

    resp = client.get(f"{PREFIX}/boards/{board_id}/guidelines")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body) == 1
    assert body[0]["scope"] == "inline"
    assert body[0]["guideline"]["title"] == "inline rule"
    assert body[0]["guideline"]["board_id"] == board_id


@pytest.mark.asyncio
async def test_get_board_guidelines_board_404(client) -> None:
    resp = client.get(f"{PREFIX}/boards/{_missing('board')}/guidelines")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_get_board_guidelines_foreign_board_404(client) -> None:
    # Board owned by OTHER, not shared with USER → get_board returns None → 404.
    foreign_board = await _seed_board(owner=OTHER)
    resp = client.get(f"{PREFIX}/boards/{foreign_board}/guidelines")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


# --- board: link or create --------------------------------------------------


@pytest.mark.asyncio
async def test_link_global_requires_preview_without_mutation(client) -> None:
    board_id = await _seed_board()
    gid = _create_global(client, title="global-to-link")
    resp = client.post(
        f"{PREFIX}/boards/{board_id}/guidelines",
        json={"guideline_id": gid, "priority": 2},
    )
    _assert_preview_required(resp)
    board_guidelines = client.get(f"{PREFIX}/boards/{board_id}/guidelines")
    assert board_guidelines.status_code == 200
    assert board_guidelines.json() == []


@pytest.mark.asyncio
async def test_create_inline_board_guideline_201(client) -> None:
    board_id = await _seed_board()
    resp = client.post(
        f"{PREFIX}/boards/{board_id}/guidelines",
        json={"title": "inline only", "content": "c", "priority": 5},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["scope"] == "inline"
    assert body["priority"] == 5
    assert body["guideline_id"] == body["id"]


@pytest.mark.asyncio
async def test_link_or_create_missing_fields_422(client) -> None:
    board_id = await _seed_board()
    resp = client.post(f"{PREFIX}/boards/{board_id}/guidelines", json={"priority": 1})
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == (
        "Provide guideline_id to link a global guideline, or title and content to "
        "create an inline guideline."
    )


@pytest.mark.asyncio
async def test_link_missing_global_requires_preview_without_enumeration(client) -> None:
    board_id = await _seed_board()
    resp = client.post(
        f"{PREFIX}/boards/{board_id}/guidelines",
        json={"guideline_id": _missing(), "priority": 1},
    )
    _assert_preview_required(resp)


@pytest.mark.asyncio
async def test_link_or_create_board_404(client) -> None:
    resp = client.post(
        f"{PREFIX}/boards/{_missing('board')}/guidelines",
        json={"title": "x", "content": "y"},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


# --- board: unlink ----------------------------------------------------------


@pytest.mark.asyncio
async def test_unlink_board_guideline_204_then_404(client) -> None:
    board_id = await _seed_board()
    gid = _create_global(client, title="to-unlink")
    await _link_guideline(board_id, gid)
    resp = client.delete(f"{PREFIX}/boards/{board_id}/guidelines/{gid}")
    assert resp.status_code == 204, resp.text
    gone = client.delete(f"{PREFIX}/boards/{board_id}/guidelines/{gid}")
    assert gone.status_code == 404
    assert gone.json()["detail"] == "Link not found"


@pytest.mark.asyncio
async def test_unlink_missing_link_404(client) -> None:
    board_id = await _seed_board()
    resp = client.delete(f"{PREFIX}/boards/{board_id}/guidelines/{_missing()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Link not found"


# --- board: priority --------------------------------------------------------


@pytest.mark.asyncio
async def test_update_priority_requires_preview_without_mutation(client) -> None:
    board_id = await _seed_board()
    gid = _create_global(client, title="prio")
    await _link_guideline(board_id, gid)
    resp = client.patch(
        f"{PREFIX}/boards/{board_id}/guidelines/{gid}", json={"priority": 9}
    )
    _assert_preview_required(resp)
    board_guidelines = client.get(f"{PREFIX}/boards/{board_id}/guidelines")
    assert board_guidelines.status_code == 200
    body = board_guidelines.json()
    assert len(body) == 1
    assert body[0]["guideline"]["id"] == gid
    assert body[0]["priority"] == 0


@pytest.mark.asyncio
async def test_update_priority_missing_link_requires_preview_without_enumeration(
    client,
) -> None:
    board_id = await _seed_board()
    resp = client.patch(
        f"{PREFIX}/boards/{board_id}/guidelines/{_missing()}", json={"priority": 3}
    )
    _assert_preview_required(resp)


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_get_board_guidelines_use_case_raises_for_missing_board() -> None:
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )
    from okto_pulse.core.application.use_cases.guidelines_crud import (
        GetBoardGuidelinesCommand,
        GetBoardGuidelinesUseCase,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest", realm_id=LOCAL_REALM_ID)
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await GetBoardGuidelinesUseCase().execute(
                GetBoardGuidelinesCommand(_missing("board")), actor=actor, uow=uow
            )


def test_fu7_s3_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(guidelines_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
