"""Spec R01A IMP5 — MCP KG queue/parity inspector cluster migrated to the UoW path.

A small, cohesive cluster of two read-only KG inspector tools — the MCP pair of
the REST batch migrated in R01A IMP4 — now route through the SAME transport-free
use cases (``ListStaleCanonicalParityUseCase`` / ``GetQueueDrilldownUseCase``) via
the MCP ``UnitOfWorkFactory``, so neither tool opens a raw ``get_db_for_mcp()``
session:

- ``okto_pulse_kg_stale_canonical_parity_list`` (auth via ``_get_agent_ctx``)
- ``okto_pulse_kg_queue_drilldown`` (auth + ``BOARD_READ`` permission)

Golden output, the auth/permission baselines and the transactional read path are
unchanged. The cluster-specific assertions are complemented by a server-wide
guard that prevents raw ``get_db_for_mcp`` access from being reintroduced.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import ast
import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.mcp import server as mcp_server

STALE_TOOL = "okto_pulse_kg_stale_canonical_parity_list"
DRILL_TOOL = "okto_pulse_kg_queue_drilldown"
MIGRATED_TOOLS = (STALE_TOOL, DRILL_TOOL)


def _stub_ctx(permissions=("board:read", "kg.admin.settings_read")):
    return type(
        "Ctx",
        (),
        {
            "agent_id": "imp5-mcp-agent",
            "agent_name": "imp5-mcp-agent",
            "permissions": list(permissions),
            "realm_id": LOCAL_REALM_ID,
        },
    )()


async def _call(tool_name: str, **kwargs) -> str:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    tool = await mcp_server.mcp.get_tool(tool_name)
    return await tool.fn(**kwargs)


async def _seed_board(board_id: str) -> None:
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import Board

    factory = get_session_factory()
    async with factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(
                Board(
                    id=board_id,
                    name="imp5",
                    owner_id="imp5-owner",
                    realm_id=LOCAL_REALM_ID,
                )
            )
            await db.commit()


# --- golden output parity (tool == shared reader) --------------------------


@pytest.mark.asyncio
async def test_stale_canonical_parity_payload_matches_reader() -> None:
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.kg.stale_canonical_parity import list_stale_canonical_parity

    board_id = f"imp5-stale-{uuid.uuid4().hex[:8]}"
    await _seed_board(board_id)

    async with get_session_factory()() as db:
        baseline = await list_stale_canonical_parity(
            db, board_id=board_id, limit=25, offset=0
        )

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        raw = await _call(STALE_TOOL, board_id=board_id, limit=25, offset=0)

    assert json.loads(raw) == json.loads(json.dumps(baseline, default=str))


@pytest.mark.asyncio
async def test_queue_drilldown_payload_matches_reader() -> None:
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.queue_health_service import get_active_queue_drilldown

    board_id = f"imp5-drill-{uuid.uuid4().hex[:8]}"
    await _seed_board(board_id)

    async with get_session_factory()() as db:
        baseline = await get_active_queue_drilldown(db, board_id)

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        raw = await _call(DRILL_TOOL, board_id=board_id)

    assert json.loads(raw) == json.loads(json.dumps(baseline, default=str))


# --- auth / permission negative paths --------------------------------------


@pytest.mark.asyncio
async def test_stale_parity_auth_gates_before_use_case() -> None:
    """ctx None → unchanged _auth_error() envelope and the use case never runs."""
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    expected = mcp_server._auth_error()

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=None)), patch(
        "okto_pulse.core.application.use_cases.list_stale_canonical_parity."
        "ListStaleCanonicalParityUseCase.execute",
        AsyncMock(side_effect=AssertionError("use case must not run when auth fails")),
    ):
        tool = await mcp_server.mcp.get_tool(STALE_TOOL)
        raw = await tool.fn(board_id="any", limit=10, offset=0)

    assert raw == expected


@pytest.mark.asyncio
async def test_queue_drilldown_permission_denied_before_use_case() -> None:
    """A ctx without BOARD_READ → unchanged _perm_error envelope, use case skipped."""
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())

    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(permissions=()))
    ), patch(
        "okto_pulse.core.application.use_cases.queue_health."
        "GetQueueDrilldownUseCase.execute",
        AsyncMock(side_effect=AssertionError("use case must not run when denied")),
    ):
        tool = await mcp_server.mcp.get_tool(DRILL_TOOL)
        raw = await tool.fn(board_id="any")

    payload = json.loads(raw)
    assert "error" in payload or "permission" in raw.lower()


# --- AST strangler proofs --------------------------------------------------


def _tool_node(name: str) -> ast.AST:
    tree = ast.parse(Path(mcp_server.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"{name} not found in mcp/server.py")


def test_migrated_tool_bodies_have_no_relational_coupling() -> None:
    for name in MIGRATED_TOOLS:
        names = {n.id for n in ast.walk(_tool_node(name)) if isinstance(n, ast.Name)}
        assert "get_db_for_mcp" not in names, name
        assert "AsyncSession" not in names, name
        assert "get_db" not in names, name
        assert "_get_agent_ctx" in names, name
        assert "get_unit_of_work_factory_for_mcp" in names, name


def test_mcp_server_has_no_raw_get_db_for_mcp_access() -> None:
    """Every MCP handler must enter persistence through the application UoW."""
    tree = ast.parse(Path(mcp_server.__file__).read_text(encoding="utf-8"))
    raw_uses = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id == "get_db_for_mcp"
    ]
    assert raw_uses == []
