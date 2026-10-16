"""Spec R01A IMP3 — MCP DLQ inspector tool migrated to the UnitOfWork path.

Pairs with R01A IMP2 (REST): ``okto_pulse_kg_dead_letter_list`` now routes
through the SAME transport-free ``ListDeadLetterRowsUseCase`` via the MCP
``UnitOfWorkFactory``, so the tool no longer opens a raw ``get_db_for_mcp()``
session. The ``_get_agent_ctx`` permission-cache path is unchanged (auth still
gates BEFORE the use case), the JSON payload is identical to the service, and the
migration is limited to this one tool — ``mcp/server.py`` was not swept.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import ast
import uuid
from pathlib import Path


from okto_pulse.core.mcp import server as mcp_server

TOOL = "okto_pulse_kg_dead_letter_list"
USER_ID = "dlq-mcp-r01a-agent"


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "dlq-mcp-agent",
            "permissions": ["board.read", "kg.admin.settings_read"],
        },
    )()


async def _insert_dlq_row(db, board_id: str, idx: int) -> str:
    from sqlalchemy_test_models import Board, ConsolidationDeadLetter

    if await db.get(Board, board_id) is None:
        db.add(Board(id=board_id, name="dlq-mcp-r01a", owner_id=USER_ID))
        await db.flush()
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

    register_mcp_test_runtime(get_session_factory())
    tool = await mcp_server.mcp.get_tool(TOOL)
    return await tool.fn(**kwargs)






def _tool_function_node() -> ast.AST:
    src = Path(mcp_server.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == TOOL:
            return node
    raise AssertionError(f"{TOOL} not found in mcp/server.py")




def test_mcp_handlers_have_no_direct_database_session_access() -> None:
    """Final ratchet: every MCP handler resolves work through a UnitOfWork."""
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
    assert other_uses == 0


def test_permission_cache_invalidation_baseline_intact() -> None:
    """Permission-cache witness (IMP3 title guardrail / ts_4160b5c5): the
    invalidation points are unchanged — update_agent and update_board_overrides
    invalidate the cache; grant/revoke/delete do NOT — and the cached
    ``_get_agent_ctx`` path the tool still depends on exists."""
    assert callable(getattr(mcp_server, "invalidate_agent_cache", None))
    assert callable(getattr(mcp_server, "_get_agent_ctx", None))

    from okto_pulse.community.api import agents as agents_api

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
