"""Spec #04 card b969406b — MCP okto_pulse_move_ideation migrated to the UoW path.

Proves the REAL MCP tool (not a harness): okto_pulse_move_ideation obtains a
PulseUnitOfWork from the MCP UnitOfWorkFactory
(get_unit_of_work_factory_for_mcp over _mcp_session_factory) and drives the
transport-free use case — it no longer calls get_db_for_mcp directly — while
preserving the compact payload, board pre-check, error envelopes, commit and
side effects (ts_929452f1).
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import ast
import inspect
import json
import textwrap
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, Ideation, IdeationStatus

USER_ID = "mcp-uow-04"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {"agent_id": USER_ID, "agent_name": "mcp-uow-agent", "permissions": None},
    )()


async def _call(name: str, **kwargs) -> dict:
    register_mcp_test_runtime(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    return json.loads(await tool.fn(**kwargs))


@pytest.fixture
def _stub_auth():
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        yield


async def _seed(db_factory) -> tuple[str, str]:
    board_id, ideation_id = _id("board"), _id("idea")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="MCP UoW", owner_id=USER_ID, settings={}))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="MCP UoW ideation",
                created_by=USER_ID,
                status=IdeationStatus.DRAFT,
            )
        )
        await db.commit()
    return board_id, ideation_id


@pytest.mark.asyncio
async def test_mcp_move_ideation_uow_preserves_payload_and_persists(db_factory, _stub_auth):
    board_id, ideation_id = await _seed(db_factory)
    result = await _call(
        "okto_pulse_move_ideation",
        board_id=board_id,
        ideation_id=ideation_id,
        status="review",
    )
    assert result == {
        "success": True,
        "ideation_id": ideation_id,
        "from_status": "draft",
        "to_status": "review",
    }
    # committed through the UoW (factory session) and visible on a fresh session
    async with db_factory() as db:
        row = (
            await db.execute(select(Ideation).where(Ideation.id == ideation_id))
        ).scalar_one()
        assert row.status == IdeationStatus.REVIEW


@pytest.mark.asyncio
async def test_mcp_move_ideation_uow_preserves_error_envelopes(db_factory, _stub_auth):
    board_id, ideation_id = await _seed(db_factory)
    not_found = await _call(
        "okto_pulse_move_ideation",
        board_id=board_id,
        ideation_id="missing-mcp-04",
        status="review",
    )
    assert not_found == {"error": "Ideation not found"}
    bad_status = await _call(
        "okto_pulse_move_ideation",
        board_id=board_id,
        ideation_id=ideation_id,
        status="not-a-status",
    )
    assert "error" in bad_status and "Invalid status" in bad_status["error"]


@pytest.mark.asyncio
async def test_mcp_move_ideation_does_not_call_get_db_for_mcp():
    tool = await mcp_server.mcp.get_tool("okto_pulse_move_ideation")
    tree = ast.parse(textwrap.dedent(inspect.getsource(tool.fn)))
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    # AST (not a string match — comments mention the old call by design):
    assert "get_db_for_mcp" not in called  # the migrated rule no longer opens it
    assert "get_unit_of_work_factory_for_mcp" in called  # uses the MCP UoW factory


def test_get_unit_of_work_factory_for_mcp_resolves_registered_provider():
    # R01B FR3: the MCP path resolves the edition-registered UnitOfWorkFactory from
    # the process-level seam (port-shaped) — it no longer constructs a core
    # SQLAlchemyUnitOfWorkFactory inline. conftest registers a core-only provider;
    # production registers the Community factory.
    from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory

    factory = mcp_server.get_unit_of_work_factory_for_mcp()
    # Port-shaped: a callable factory exposing the request-scoped wrap bridge,
    # asserted by contract, not by concrete class.
    assert callable(factory)
    assert hasattr(factory, "wrap")
    # It is exactly the registered provider (no inline construction).
    assert factory is resolve_unit_of_work_factory()
