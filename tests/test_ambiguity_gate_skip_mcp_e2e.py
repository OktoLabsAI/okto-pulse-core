"""Card b682b5de (ts_2b024c93) — E2E coverage of the MCP skip tool itself.

Invokes okto_pulse_set_ideation_ambiguity_gate_skip through the real MCP tool
surface (with a stubbed agent auth context).

R5-IMP1 (card e9a6b251) supersedes the original agent-mutable contract: the
ambiguity-gate skip is now a HUMAN-only control. The agent-facing MCP tool fails
closed with ``human_control_required`` (no skip_ambiguity_gate mutation); the
human REST surface (PATCH /ambiguity-gate-skip via require_user) remains the path
and still persists the skip. These E2E tests assert the new contract: MCP refuses
fail-closed, REST persists.
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
async def test_mcp_skip_tool_is_human_only_and_does_not_persist(db_factory):
    # R5-IMP1: the agent-facing MCP tool fails closed and never persists the skip.
    board_id, ideation_id = await _seed(db_factory)

    res = await _call(SKIP_TOOL, board_id=board_id, ideation_id=ideation_id, skip_ambiguity_gate=True)

    assert res["code"] == "human_control_required"
    d = res["details"]
    assert d["mutation_allowed"] is False
    assert d["state_changed"] is False
    assert d["required_actor"] == "human"
    assert d["required_surface"] == "ui|human_rest"
    assert d["target_ref"] == f"ideation:{ideation_id}"

    # TEETH: skip_ambiguity_gate was NOT mutated by the agent surface.
    async with db_factory() as db:
        reloaded = await db.get(Ideation, ideation_id)
        assert reloaded.skip_ambiguity_gate is False


@pytest.mark.asyncio
async def test_mcp_skip_tool_archived_still_human_only(db_factory):
    # R5-IMP1: the MCP tool refuses BEFORE the service, so even an archived ideation
    # surfaces human_control_required (the archived guard now runs on the human path).
    board_id, ideation_id = await _seed(db_factory, archived=True)

    res = await _call(SKIP_TOOL, board_id=board_id, ideation_id=ideation_id, skip_ambiguity_gate=True)

    assert res["code"] == "human_control_required"
    assert res["details"]["mutation_allowed"] is False


@pytest.mark.asyncio
async def test_mcp_skip_tool_unknown_ideation_still_human_only(db_factory):
    # R5-IMP1: fail-closed BEFORE any lookup — an unknown ideation also refuses
    # (the agent cannot mutate regardless of existence).
    board_id, _ = await _seed(db_factory)

    res = await _call(SKIP_TOOL, board_id=board_id, ideation_id=_id("missing"), skip_ambiguity_gate=True)

    assert res["code"] == "human_control_required"


@pytest.mark.asyncio
async def test_rest_persists_while_mcp_refuses(db_factory):
    # R5-IMP1: the human REST surface still persists the skip; the agent MCP surface
    # refuses fail-closed. No payload parity (by design).
    mcp_board, mcp_ideation = await _seed(db_factory)
    _, rest_ideation = await _seed(db_factory)

    mcp_res = await _call(SKIP_TOOL, board_id=mcp_board, ideation_id=mcp_ideation, skip_ambiguity_gate=True)

    client = _rest_client()
    rest_res = client.patch(
        f"/api/v1/ideations/{rest_ideation}/ambiguity-gate-skip",
        json={"skip_ambiguity_gate": True},
    ).json()

    # REST (human) persists ...
    assert rest_res["skip_ambiguity_gate"] is True
    assert rest_res["status"] == "evaluating"
    assert rest_res["id"] == rest_ideation
    # ... MCP (agent) refuses, and its ideation was NOT mutated.
    assert mcp_res["code"] == "human_control_required"
    async with db_factory() as db:
        reloaded = await db.get(Ideation, mcp_ideation)
        assert reloaded.skip_ambiguity_gate is False
