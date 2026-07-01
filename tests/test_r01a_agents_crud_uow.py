"""Spec R01A REST-FU1 — remaining api/agents.py endpoints on the UnitOfWork path.

The eight remaining agents endpoints (create / list-user / list-board / get /
regenerate-key / delete / grant / revoke) now route through transport-free use
cases + ``get_unit_of_work`` instead of a raw ``AsyncSession``. These oracles
prove, per endpoint, that payload / structured error / permission-404 / 409
conflict / transaction are preserved, and that the cache-invalidation rule is
unchanged: grant / revoke / delete must NOT invalidate (only update_agent /
update_board_overrides do — ac_8e695cf2).
"""

from __future__ import annotations

import inspect
import uuid
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api import agents as agents_api
from okto_pulse.core.api.agents import router as agents_router
from okto_pulse.core.api.deps import get_unit_of_work
from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu1-user"
OTHER = "r01a-fu1-other"

_ENDPOINTS = (
    "create_agent",
    "list_my_agents",
    "list_agents_for_board",
    "get_agent",
    "regenerate_agent_key",
    "delete_agent",
    "grant_board_access",
    "revoke_board_access",
)


def _client(user: str = USER) -> TestClient:
    app = FastAPI()
    app.include_router(agents_router, prefix="/api/v1/agents")
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: user
    return TestClient(app)


async def _seed_agent(owner: str = USER) -> str:
    from okto_pulse.core.models import AgentCreate
    from okto_pulse.core.services import AgentService

    async with get_session_factory()() as db:
        agent, _key = await AgentService(db).create_agent(
            owner, AgentCreate(name=f"fu1-{uuid.uuid4().hex[:6]}")
        )
        await db.commit()
        return agent.id


async def _seed_board(owner: str = USER) -> str:
    from okto_pulse.core.models.db import Board

    bid = f"board-fu1-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu1", owner_id=owner))
        await db.commit()
    return bid


async def _agent_exists(agent_id: str) -> bool:
    from okto_pulse.core.services import AgentService

    async with get_session_factory()() as db:
        return await AgentService(db).get_agent(agent_id) is not None


async def _has_access(agent_id: str, board_id: str) -> bool:
    from okto_pulse.core.services import AgentService

    async with get_session_factory()() as db:
        return await AgentService(db).agent_has_board_access(agent_id, board_id)


# --- create / list / get (read + create) -----------------------------------


@pytest.mark.asyncio
async def test_create_agent_returns_api_key_and_persists() -> None:
    client = _client()
    resp = client.post("/api/v1/agents", json={"name": "fu1-created"})
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["name"] == "fu1-created"
    assert body["api_key"]
    assert await _agent_exists(body["id"])


@pytest.mark.asyncio
async def test_list_my_agents_returns_owned() -> None:
    agent_id = await _seed_agent()
    client = _client()
    resp = client.get("/api/v1/agents")
    assert resp.status_code == 200, resp.text
    assert agent_id in {a["id"] for a in resp.json()}


@pytest.mark.asyncio
async def test_get_agent_owned_200_and_non_owner_404() -> None:
    agent_id = await _seed_agent(owner=OTHER)
    # owner sees it
    assert _client(OTHER).get(f"/api/v1/agents/{agent_id}").status_code == 200
    # a non-owner gets the legacy 404
    resp = _client(USER).get(f"/api/v1/agents/{agent_id}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Agent not found"


@pytest.mark.asyncio
async def test_list_agents_for_board_404_when_board_missing() -> None:
    resp = _client().get(f"/api/v1/agents/board/missing-{uuid.uuid4().hex[:8]}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_list_agents_for_board_owned_200() -> None:
    board_id = await _seed_board()
    resp = _client().get(f"/api/v1/agents/board/{board_id}")
    assert resp.status_code == 200, resp.text
    assert isinstance(resp.json(), list)


# --- regenerate-key / delete (write) ----------------------------------------


@pytest.mark.asyncio
async def test_regenerate_key_returns_new_key_and_404_for_non_owner() -> None:
    agent_id = await _seed_agent()
    resp = _client().post(f"/api/v1/agents/{agent_id}/regenerate-key")
    assert resp.status_code == 200, resp.text
    assert resp.json()["api_key"]
    # non-owner
    other = _client(OTHER).post(f"/api/v1/agents/{agent_id}/regenerate-key")
    assert other.status_code == 404


@pytest.mark.asyncio
async def test_delete_agent_204_persists_and_does_not_invalidate() -> None:
    agent_id = await _seed_agent()
    with patch("okto_pulse.core.mcp.server.invalidate_agent_cache") as mock_inval:
        resp = _client().delete(f"/api/v1/agents/{agent_id}")
    assert resp.status_code == 204, resp.text
    assert not await _agent_exists(agent_id)
    mock_inval.assert_not_called()


@pytest.mark.asyncio
async def test_delete_agent_404_for_non_owner() -> None:
    agent_id = await _seed_agent(owner=OTHER)
    resp = _client(USER).delete(f"/api/v1/agents/{agent_id}")
    assert resp.status_code == 404
    assert await _agent_exists(agent_id)  # untouched


# --- grant / revoke board access (write) ------------------------------------


@pytest.mark.asyncio
async def test_grant_board_access_201_persists_and_no_invalidate() -> None:
    agent_id = await _seed_agent()
    board_id = await _seed_board()
    with patch("okto_pulse.core.mcp.server.invalidate_agent_cache") as mock_inval:
        resp = _client().post(f"/api/v1/agents/{agent_id}/boards/{board_id}")
    assert resp.status_code == 201, resp.text
    assert await _has_access(agent_id, board_id)
    mock_inval.assert_not_called()


@pytest.mark.asyncio
async def test_grant_board_access_409_when_already_granted() -> None:
    agent_id = await _seed_agent()
    board_id = await _seed_board()
    client = _client()
    assert client.post(f"/api/v1/agents/{agent_id}/boards/{board_id}").status_code == 201
    dup = client.post(f"/api/v1/agents/{agent_id}/boards/{board_id}")
    assert dup.status_code == 409
    assert dup.json()["detail"] == "Access already granted"


@pytest.mark.asyncio
async def test_grant_board_access_404_agent_then_board() -> None:
    # missing agent
    miss_agent = _client().post(
        f"/api/v1/agents/missing-{uuid.uuid4().hex[:6]}/boards/x"
    )
    assert miss_agent.status_code == 404
    assert miss_agent.json()["detail"] == "Agent not found"
    # real agent, missing board
    agent_id = await _seed_agent()
    miss_board = _client().post(
        f"/api/v1/agents/{agent_id}/boards/missing-{uuid.uuid4().hex[:6]}"
    )
    assert miss_board.status_code == 404
    assert miss_board.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_revoke_board_access_204_and_no_invalidate() -> None:
    agent_id = await _seed_agent()
    board_id = await _seed_board()
    client = _client()
    client.post(f"/api/v1/agents/{agent_id}/boards/{board_id}")
    with patch("okto_pulse.core.mcp.server.invalidate_agent_cache") as mock_inval:
        resp = client.delete(f"/api/v1/agents/{agent_id}/boards/{board_id}")
    assert resp.status_code == 204, resp.text
    assert not await _has_access(agent_id, board_id)
    mock_inval.assert_not_called()


@pytest.mark.asyncio
async def test_revoke_board_access_404_when_no_grant() -> None:
    agent_id = await _seed_agent()
    board_id = await _seed_board()
    resp = _client().delete(f"/api/v1/agents/{agent_id}/boards/{board_id}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Access not found"


# --- cache rule + strangler proofs ------------------------------------------


def _funcs_calling(symbol: str) -> set[str]:
    import ast
    from pathlib import Path

    tree = ast.parse(Path(agents_api.__file__).read_text(encoding="utf-8"))
    out: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and any(
            isinstance(c, ast.Call)
            and isinstance(c.func, ast.Name)
            and c.func.id == symbol
            for c in ast.walk(node)
        ):
            out.add(node.name)
    return out


def test_cache_invalidation_points_unchanged() -> None:
    """ac_8e695cf2: ONLY update_agent + update_board_overrides invalidate; the
    REST-FU1 writes (grant/revoke/delete) did NOT add a new invalidation point."""
    assert _funcs_calling("invalidate_agent_cache") == {
        "update_agent",
        "update_board_overrides",
    }


def test_all_agents_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(agents_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def test_agents_module_is_fully_strangled_in_inventory() -> None:
    from okto_pulse.core.repositories.relational_consumer_inventory import (
        build_relational_consumer_inventory,
    )

    inv = build_relational_consumer_inventory()
    agents_sites = [c for c in inv.consumers if c.file == "core/api/agents.py"]
    assert agents_sites == [], agents_sites
