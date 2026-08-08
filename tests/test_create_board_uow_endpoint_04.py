"""Spec #04 card 3537363a — REST create_board migrated to the UnitOfWork path.

Proves the REAL endpoint (not an isolated harness): POST /api/v1/boards drives
endpoint -> get_unit_of_work (request-scoped PulseUnitOfWork bound to the
session) -> transport-free use case -> SQLAlchemyUnitOfWork -> service/repository
adapter, preserving the Community behavior (201 / payload / owner / effective
settings / persistence) and keeping the FastAPI ``get_db`` dependency override
intact. The handler no longer takes a raw ``AsyncSession``.
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import boards as boards_api
from okto_pulse.community.api.boards import router as boards_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import require_principal
from okto_pulse.core.domain.permissions import PERMISSION_REGISTRY
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.ports.authentication import Principal
from okto_pulse.core.repositories import PulseUnitOfWork

USER = "uow-endpoint-04"


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(boards_router, prefix="/api/v1/boards")
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_principal] = lambda: Principal(
        subject=USER,
        realm_id=LOCAL_REALM_ID,
        actor_kind="human",
        claims={"permissions": PERMISSION_REGISTRY},
    )
    return TestClient(app)


def test_create_board_endpoint_uses_uow_and_preserves_behavior():
    client = _client()
    resp = client.post("/api/v1/boards", json={"name": "Prod04 Board"})
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["name"] == "Prod04 Board"
    assert body["owner_id"] == USER
    assert body.get("settings") is not None  # effective settings preserved
    board_id = body["id"]

    # The board persisted on the overridden session (the override flowed through
    # get_unit_of_work -> get_db) and is readable on a subsequent request.
    got = client.get(f"/api/v1/boards/{board_id}")
    assert got.status_code == 200
    assert got.json()["name"] == "Prod04 Board"


def test_create_board_handler_depends_on_unit_of_work_not_raw_session():
    sig = inspect.signature(boards_api.create_board)
    assert "db" not in sig.parameters  # no raw AsyncSession in the migrated handler
    uow_param = sig.parameters["uow"]
    assert uow_param.default.dependency is get_unit_of_work


@pytest.mark.asyncio
async def test_get_unit_of_work_owns_the_request_transaction():
    # F12: the edition factory owns creation, realm resolution and teardown.
    fake_request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    dependency = get_unit_of_work(
        request=fake_request,
        principal=Principal(
            subject=USER,
            realm_id=LOCAL_REALM_ID,
            claims={"roles": ["admin"], "permissions": ("*",)},
            actor_kind="human",
        ),
    )
    try:
        uow = await anext(dependency)
        assert isinstance(uow, PulseUnitOfWork)  # port-shaped, not concrete-locked
        assert not hasattr(uow, "session")
        assert uow.services is not None
    finally:
        await dependency.aclose()
