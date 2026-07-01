"""Spec R01A IMP3 — MCP DLQ inspector tool migrated to the UnitOfWork path.

Pairs with R01A IMP2 (REST): ``okto_pulse_kg_dead_letter_list`` now routes
through the SAME transport-free ``ListDeadLetterRowsUseCase`` via the MCP
``UnitOfWorkFactory``, so the tool no longer opens a raw ``get_db_for_mcp()``
session. The ``_get_agent_ctx`` permission-cache path is unchanged (auth still
gates BEFORE the use case), the JSON payload is identical to the service, and the
migration is limited to this one tool — ``mcp/server.py`` was not swept.
"""

from __future__ import annotations

import ast
import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server

TOOL = "okto_pulse_kg_dead_letter_list"
USER_ID = "dlq-mcp-r01a-agent"


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {"agent_id": USER_ID, "agent_name": "dlq-mcp-agent", "permissions": ["board.read"]},
    )()


async def _insert_dlq_row(db, board_id: str, idx: int) -> str:
    from okto_pulse.core.models.db import ConsolidationDeadLetter

    row_id = f"dlq_r01a_{uuid.uuid4().hex[:8]}_{idx}"
    db.add(
        ConsolidationDeadLetter(
            id=row_id,
            board_id=board_id,
            artifact_type="spec",
            artifact_id=f"spec-{idx}-{uuid.uuid4().hex[:8]}",
            original_queue_id=f"q-{idx}",
            attempts=5,
            errors=[
                {
                    "attempt": n,
                    "occurred_at": "2026-04-27T10:00:00",
                    "error_type": "TestError",
                    "message": f"failure {n}",
                    "traceback": None,
                }
                for n in range(1, 4)
            ],
        )
    )
    await db.flush()
    return row_id


async def _call_tool(**kwargs) -> str:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    tool = await mcp_server.mcp.get_tool(TOOL)
    return await tool.fn(**kwargs)


@pytest.mark.asyncio
async def test_mcp_tool_payload_matches_service() -> None:
    """Success path: the REAL MCP tool returns the SAME JSON as the service
    (parity), routed through the use case + MCP UnitOfWorkFactory."""
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.dead_letter_inspector_service import (
        list_dead_letter_rows,
    )

    board_id = f"board-mcp-r01a-{uuid.uuid4().hex[:8]}"
    factory = get_session_factory()
    async with factory() as db:
        for i in range(3):
            await _insert_dlq_row(db, board_id, i)
        await db.commit()

    async with factory() as db:
        baseline = await list_dead_letter_rows(db, board_id, limit=10, offset=0)

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        raw = await _call_tool(board_id=board_id, limit=10, offset=0)
    payload = json.loads(raw)

    # Byte-identical to the service payload (the MCP tool json.dumps(default=str)).
    assert payload == json.loads(json.dumps(baseline, default=str))
    assert payload["total"] == 3
    assert len(payload["rows"]) == 3
    assert len(payload["rows"][0]["errors"]) == 3


@pytest.mark.asyncio
async def test_mcp_tool_auth_gates_before_use_case() -> None:
    """Permission baseline: when ``_get_agent_ctx`` returns None the tool returns
    the unchanged ``_auth_error()`` envelope and NEVER reaches the use case — the
    auth check is still on the path before the relational work."""
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    expected = mcp_server._auth_error()

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=None)), patch(
        "okto_pulse.core.application.use_cases.list_dead_letter_rows."
        "ListDeadLetterRowsUseCase.execute",
        AsyncMock(side_effect=AssertionError("use case must not run when auth fails")),
    ):
        tool = await mcp_server.mcp.get_tool(TOOL)
        raw = await tool.fn(board_id="any-board", limit=10, offset=0)

    assert raw == expected


def _tool_function_node() -> ast.AST:
    src = Path(mcp_server.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == TOOL:
            return node
    raise AssertionError(f"{TOOL} not found in mcp/server.py")


def test_tool_body_has_no_direct_relational_coupling() -> None:
    """AST: the migrated tool body no longer references get_db_for_mcp /
    AsyncSession / get_db, but DOES still go through _get_agent_ctx and the MCP
    UnitOfWorkFactory + the shared use case."""
    node = _tool_function_node()
    names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
    assert "get_db_for_mcp" not in names
    assert "AsyncSession" not in names
    assert "get_db" not in names
    assert "_get_agent_ctx" in names
    assert "get_unit_of_work_factory_for_mcp" in names
    assert "ListDeadLetterRowsUseCase" in names


def test_migration_is_limited_to_this_tool() -> None:
    """TR4: the migration is scoped to one tool — mcp/server.py was NOT swept;
    get_db_for_mcp still appears in other handlers."""
    src = Path(mcp_server.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    other_uses = 0
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
            and node.name != TOOL
        ):
            other_uses += sum(
                1
                for n in ast.walk(node)
                if isinstance(n, ast.Name) and n.id == "get_db_for_mcp"
            )
    assert other_uses > 0


def test_permission_cache_invalidation_baseline_intact() -> None:
    """Permission-cache witness (IMP3 title guardrail / ts_4160b5c5): the
    invalidation points are unchanged — update_agent and update_board_overrides
    invalidate the cache; grant/revoke/delete do NOT — and the cached
    ``_get_agent_ctx`` path the tool still depends on exists."""
    assert callable(getattr(mcp_server, "invalidate_agent_cache", None))
    assert callable(getattr(mcp_server, "_get_agent_ctx", None))

    from okto_pulse.core.api import agents as agents_api

    tree = ast.parse(Path(agents_api.__file__).read_text(encoding="utf-8"))
    invalidating: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            called = {
                c.func.id if isinstance(c.func, ast.Name) else getattr(c.func, "attr", None)
                for c in ast.walk(node)
                if isinstance(c, ast.Call)
            }
            if "invalidate_agent_cache" in called:
                invalidating.add(node.name)

    assert "update_agent" in invalidating
    assert "update_board_overrides" in invalidating
    for not_invalidating in ("grant_board_access", "revoke_board_access", "delete_agent"):
        assert not_invalidating not in invalidating
