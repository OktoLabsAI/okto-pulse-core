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

from okto_pulse.core.api import boards as boards_api
from okto_pulse.core.api.boards import router as boards_router
from okto_pulse.core.api.deps import get_unit_of_work
from okto_pulse.core.infra.auth import get_realm_id, require_user
from okto_pulse.core.infra.database import get_db, get_session_factory
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
    app.dependency_overrides[require_user] = lambda: USER
    app.dependency_overrides[get_realm_id] = lambda: None
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
async def test_get_unit_of_work_binds_the_request_session(db_factory):
    # R01B FR3: with no app.state.runtime_composition the dependency resolves the
    # process-level seam (the conftest-registered provider) and wraps the request
    # session port-shaped — no core concrete is constructed by get_unit_of_work.
    fake_request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    async with db_factory() as session:
        uow = await get_unit_of_work(request=fake_request, db=session)
        assert isinstance(uow, PulseUnitOfWork)  # port-shaped, not concrete-locked
        # Bound to the request session (preserving scope/override), not a fresh one.
        assert uow.session is session
