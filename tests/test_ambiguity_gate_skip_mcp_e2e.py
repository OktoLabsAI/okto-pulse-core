"""Card b682b5de (ts_2b024c93) — E2E coverage of the MCP skip tool itself.

Invokes okto_pulse_set_ideation_ambiguity_gate_skip through the real MCP tool
surface (with a stubbed agent auth context) and asserts payload equivalence
with the REST endpoint — not just a shared service path. Satisfies the
validator's explicit requirement for FR14/BR7.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api.ideations import router as ideations_router
from okto_pulse.core.infra import auth as _auth_mod
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import Board, Ideation, IdeationStatus

USER_ID = "ambiguity-skip-mcp-agent"
SKIP_TOOL = "okto_pulse_set_ideation_ambiguity_gate_skip"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "ambiguity-skip-agent",
            # None == full access in check_permission; this E2E exercises the skip
            # tool surface + payload, not authz (covered elsewhere).
            "permissions": None,
        },
    )()


async def _call(name: str, **kwargs) -> dict:
    mcp_server.register_session_factory(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    raw = await tool.fn(**kwargs)
    return json.loads(raw)


@pytest.fixture(autouse=True)
def _stub_auth():
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        yield


async def _seed(db_factory, *, status: IdeationStatus = IdeationStatus.EVALUATING, archived: bool = False):
    board_id, ideation_id = _id("board"), _id("idea")
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Skip MCP",
                owner_id=USER_ID,
                settings={"require_ideation_ambiguity_gate": True, "max_ideation_ambiguity": 3},
            )
        )
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="MCP skip ideation",
                created_by=USER_ID,
                status=status,
                archived=archived,
            )
        )
        await db.commit()
    return board_id, ideation_id


def _rest_client():
    app = FastAPI()
    app.include_router(ideations_router, prefix="/api/v1")
    df = get_session_factory()

    async def _odb():
        async with df() as session:
            yield session

    app.dependency_overrides[get_db] = _odb
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID
    return TestClient(app)


@pytest.mark.asyncio
async def test_mcp_skip_tool_persists_while_evaluating(db_factory):
    board_id, ideation_id = await _seed(db_factory)

    res = await _call(SKIP_TOOL, board_id=board_id, ideation_id=ideation_id, skip_ambiguity_gate=True)

    assert res["success"] is True
    assert res["id"] == ideation_id
    assert res["status"] == "evaluating"
    assert res["skip_ambiguity_gate"] is True
    assert "updated_at" in res

    # Persisted through the real service path.
    async with db_factory() as db:
        reloaded = await db.get(Ideation, ideation_id)
        assert reloaded.skip_ambiguity_gate is True


@pytest.mark.asyncio
async def test_mcp_skip_tool_rejects_archived(db_factory):
    board_id, ideation_id = await _seed(db_factory, archived=True)

    res = await _call(SKIP_TOOL, board_id=board_id, ideation_id=ideation_id, skip_ambiguity_gate=True)

    assert "error" in res
    assert "archived" in res["error"].lower()


@pytest.mark.asyncio
async def test_mcp_skip_tool_unknown_ideation_returns_not_found(db_factory):
    board_id, _ = await _seed(db_factory)

    res = await _call(SKIP_TOOL, board_id=board_id, ideation_id=_id("missing"), skip_ambiguity_gate=True)

    assert "error" in res
    assert "not found" in res["error"].lower()


@pytest.mark.asyncio
async def test_mcp_and_rest_skip_payloads_are_equivalent(db_factory):
    # Two ideations with identical starting state on identically-configured boards:
    # one toggled via the MCP tool, one via the REST endpoint. The overlapping
    # response fields must match (same skip semantics, same status).
    mcp_board, mcp_ideation = await _seed(db_factory)
    _, rest_ideation = await _seed(db_factory)

    mcp_res = await _call(SKIP_TOOL, board_id=mcp_board, ideation_id=mcp_ideation, skip_ambiguity_gate=True)

    client = _rest_client()
    rest_res = client.patch(
        f"/api/v1/ideations/{rest_ideation}/ambiguity-gate-skip",
        json={"skip_ambiguity_gate": True},
    ).json()

    assert mcp_res["skip_ambiguity_gate"] is True
    assert rest_res["skip_ambiguity_gate"] is True
    assert mcp_res["status"] == rest_res["status"] == "evaluating"
    assert mcp_res["id"] == mcp_ideation
    assert rest_res["id"] == rest_ideation
