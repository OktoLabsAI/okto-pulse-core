"""Spec R01A REST-FU3a — specs CRUD + history endpoints on the UnitOfWork path.

The seven CRUD/history endpoints (create / list / get / update / move / delete /
history) now route through transport-free use cases + ``get_unit_of_work``
instead of a raw ``AsyncSession``. ``update_spec`` keeps its permission gate and
deprecation header: the permission RESOLVER (inline SQL) moved to a transport-free
service, the use case raises ``PermissionDeniedError`` (adapter → 403), and the
permission-requirements helper chain moved next to the use case. Oracles: payload
+ ownership/404 + the gate 409/400 + the permission 403 + AST + inventory.
"""

from __future__ import annotations

import inspect
import uuid
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import specs as specs_api
from okto_pulse.community.api.specs import router as specs_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu3a-user"
OTHER = "r01a-fu3a-other"
PREFIX = "/api/v1"

_ENDPOINTS = (
    "create_spec", "list_specs", "get_spec", "update_spec",
    "move_spec", "delete_spec", "list_spec_history",
)


def _client(user: str = USER) -> TestClient:
    app = FastAPI()
    app.include_router(specs_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: user
    return TestClient(app)


async def _seed_board(owner: str = USER) -> str:
    from sqlalchemy_test_models import Board

    bid = f"board-fu3a-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu3a", owner_id=owner))
        await db.commit()
    return bid


async def _seed_spec(board_id: str, owner: str = USER) -> str:
    from okto_pulse.core.models.schemas import SpecCreate
    from okto_pulse.core.services import SpecService

    async with get_session_factory()() as db:
        spec = await SpecService(db).create_spec(
            board_id, owner, SpecCreate(title=f"fu3a-{uuid.uuid4().hex[:6]}")
        )
        await db.commit()
        return spec.id


async def _spec_exists(spec_id: str) -> bool:
    from okto_pulse.core.services import SpecService

    async with get_session_factory()() as db:
        return await SpecService(db).get_spec(spec_id) is not None


# --- create / list / get ----------------------------------------------------


@pytest.mark.asyncio
async def test_create_spec_201_persists_and_404_for_missing_board() -> None:
    board_id = await _seed_board()
    resp = _client().post(f"{PREFIX}/boards/{board_id}/specs", json={"title": "fu3a-new"})
    assert resp.status_code == 201, resp.text
    assert await _spec_exists(resp.json()["id"])

    missing = _client().post(
        f"{PREFIX}/boards/missing-{uuid.uuid4().hex[:6]}/specs", json={"title": "x"}
    )
    assert missing.status_code == 404
    assert missing.json()["detail"] == "Board not found or not owned by user"


@pytest.mark.asyncio
async def test_list_specs_200_and_404_missing_board() -> None:
    board_id = await _seed_board()
    spec_id = await _seed_spec(board_id)
    ok = _client().get(f"{PREFIX}/boards/{board_id}/specs")
    assert ok.status_code == 200, ok.text
    assert spec_id in {s["id"] for s in ok.json()}
    miss = _client().get(f"{PREFIX}/boards/missing-{uuid.uuid4().hex[:6]}/specs")
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_get_spec_200_and_404() -> None:
    board_id = await _seed_board()
    spec_id = await _seed_spec(board_id)
    assert _client().get(f"{PREFIX}/specs/{spec_id}").status_code == 200
    miss = _client().get(f"{PREFIX}/specs/missing-{uuid.uuid4().hex[:6]}")
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Spec not found"


# --- update (incl. permission 403) ------------------------------------------


@pytest.mark.asyncio
async def test_update_spec_200_simple_and_404() -> None:
    board_id = await _seed_board()
    spec_id = await _seed_spec(board_id)
    resp = _client().patch(f"{PREFIX}/specs/{spec_id}", json={"title": "fu3a-renamed"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["title"] == "fu3a-renamed"
    miss = _client().patch(f"{PREFIX}/specs/missing-{uuid.uuid4().hex[:6]}", json={"title": "x"})
    assert miss.status_code == 404


_IR_CHANGE = {"integration_requirements": [{"id": "ir-fu3a-1", "title": "svc", "description": "d"}]}


@pytest.mark.asyncio
async def test_update_spec_403_when_permission_denied() -> None:
    """A change that requires a permission (integration_requirements) is rejected
    with 403 when the actor lacks it — the migrated use case raises
    PermissionDeniedError (via check_permission) and the adapter maps it. The
    best-effort permission model is default-allow, so we simulate the denial at
    ``check_permission`` exactly where the legacy ``_require_permissions`` did."""
    board_id = await _seed_board()
    spec_id = await _seed_spec(board_id)
    with patch(
        "okto_pulse.core.infra.permissions.check_permission",
        return_value="permission_denied: spec.integration_requirements.create",
    ):
        resp = _client().patch(f"{PREFIX}/specs/{spec_id}", json=_IR_CHANGE)
    assert resp.status_code == 403, resp.text
    assert "spec.integration_requirements.create" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_update_spec_permission_use_case_raises_permission_denied() -> None:
    from okto_pulse.core.application.use_cases import (
        PermissionDeniedError,
        UpdateSpecCommand,
        UpdateSpecUseCase,
    )
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.models.schemas import SpecUpdate
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    board_id = await _seed_board()
    spec_id = await _seed_spec(board_id)
    actor = ActorContext(USER, "rest")
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    with patch(
        "okto_pulse.core.infra.permissions.check_permission",
        return_value="permission_denied",
    ):
        with pytest.raises(PermissionDeniedError):
            async with uowf(actor=actor) as uow:
                await UpdateSpecUseCase().execute(
                    UpdateSpecCommand(spec_id, SpecUpdate(**_IR_CHANGE)),
                    actor=actor, uow=uow,
                )


# --- move / delete / history ------------------------------------------------


@pytest.mark.asyncio
async def test_move_spec_404_for_missing() -> None:
    resp = _client().post(
        f"{PREFIX}/specs/missing-{uuid.uuid4().hex[:6]}/move", json={"status": "review"}
    )
    assert resp.status_code in (400, 404), resp.text


@pytest.mark.asyncio
async def test_delete_spec_204_persists_and_404() -> None:
    board_id = await _seed_board()
    spec_id = await _seed_spec(board_id)
    resp = _client().delete(f"{PREFIX}/specs/{spec_id}")
    assert resp.status_code == 204, resp.text
    assert not await _spec_exists(spec_id)
    miss = _client().delete(f"{PREFIX}/specs/missing-{uuid.uuid4().hex[:6]}")
    assert miss.status_code == 404


@pytest.mark.asyncio
async def test_list_spec_history_200() -> None:
    board_id = await _seed_board()
    spec_id = await _seed_spec(board_id)
    resp = _client().get(f"{PREFIX}/specs/{spec_id}/history")
    assert resp.status_code == 200, resp.text
    assert isinstance(resp.json(), list)


# --- strangler + Clean Core proofs ------------------------------------------


def test_spec_crud_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(specs_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def test_spec_crud_use_case_module_has_no_api_or_session_in_public_surface() -> None:
    """Clean Core: the use case module imports no okto_pulse.community.api and exposes
    no AsyncSession/select/get_db in its source."""
    import ast
    from pathlib import Path

    from okto_pulse.core.application.use_cases import spec_crud

    src = Path(spec_crud.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            assert not (node.module or "").startswith("okto_pulse.community.api"), node.module
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    assert "AsyncSession" not in names
    assert "get_db" not in names
    assert "select" not in names


def test_specs_inline_permission_sql_moved_to_service() -> None:
    """The inline Agent/PermissionPreset SQL is gone from api/specs.py (delegated
    to the transport-free service resolver)."""
    import ast
    from pathlib import Path

    src = Path(specs_api.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    selects = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "select"
    ]
    assert selects == [], "api/specs.py should no longer issue inline select()"
