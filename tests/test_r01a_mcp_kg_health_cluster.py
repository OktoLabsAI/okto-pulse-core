"""Spec R01A MCP-FU4 — MCP KG health read sub-family migrated to the UoW path.

A small, cohesive READ-ONLY sub-family of the kg query/health/schema family — the
two health diagnostics — now route through transport-free use cases
(``GetKgHealthUseCase`` / ``GetKgHealthReadinessUseCase``) via the MCP
``UnitOfWorkFactory``, so neither tool opens a raw ``get_db_for_mcp()`` session:

- ``okto_pulse_kg_health`` (slim/full projection, ``BoardNotFoundError`` envelope)
- ``okto_pulse_kg_health_readiness`` (``InvalidProfileError`` / ``BoardNotFoundError``)

Golden output is asserted against the reader recomputed independently (so parity
holds for data, board-not-found and invalid-profile alike); the auth path and the
MCP projection are unchanged; the migration is limited to these two tools.
"""

from __future__ import annotations

import ast
import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server

HEALTH_TOOL = "okto_pulse_kg_health"
READINESS_TOOL = "okto_pulse_kg_health_readiness"
MIGRATED_TOOLS = (HEALTH_TOOL, READINESS_TOOL)


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {"agent_id": "fu4-mcp-agent", "agent_name": "fu4-mcp-agent", "permissions": ["board:read"]},
    )()


async def _call(tool_name: str, **kwargs) -> str:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    tool = await mcp_server.mcp.get_tool(tool_name)
    return await tool.fn(**kwargs)


async def _seed_board(board_id: str) -> None:
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.models.db import Board

    factory = get_session_factory()
    async with factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="fu4", owner_id="fu4-owner"))
            await db.commit()


async def _expected_health(board_id: str, profile: str) -> dict:
    """Recompute the tool's expected output straight from the reader + projection."""
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.mcp.kg_query_safety import KGHealthMCPProjection
    from okto_pulse.core.services.kg_health_service import BoardNotFoundError, get_kg_health

    async with get_session_factory()() as db:
        try:
            data = await get_kg_health(board_id, db)
        except BoardNotFoundError as exc:
            return {"error": str(exc)}
    projected = KGHealthMCPProjection().project(data, profile=profile)
    return json.loads(json.dumps(projected, default=str))


async def _expected_readiness(board_id: str, profile: str, artifact_ref: str) -> dict:
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.kg_health_readiness_service import (
        InvalidProfileError,
        build_health_readiness,
    )
    from okto_pulse.core.services.kg_health_service import BoardNotFoundError

    async with get_session_factory()() as db:
        try:
            data = await build_health_readiness(
                board_id, db, profile=profile, surface="mcp",
                artifact_ref=(artifact_ref or None),
            )
        except InvalidProfileError:
            return {"error": "invalid_profile"}
        except BoardNotFoundError as exc:
            return {"error": str(exc)}
    return json.loads(json.dumps(data, default=str))


# --- golden output parity (tool == independently recomputed reader) --------


def _key_shape(obj):
    """Recursive key/leaf structure of a payload, ignoring scalar values and list
    contents. The migration changes only the session source — never the reader or
    the projection — so the health snapshot's CONTRACT shape must be identical even
    though its live values (correlation id, timestamps, evolving byte sizes,
    materialisation-dependent state) are not deterministic across calls."""
    if isinstance(obj, dict):
        return {k: _key_shape(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return "list"
    return "scalar"


@pytest.mark.asyncio
async def test_kg_health_matches_reader_shape_for_seeded_board() -> None:
    board_id = f"fu4-health-{uuid.uuid4().hex[:8]}"
    await _seed_board(board_id)
    # Warm up: the first read bootstraps a fresh board graph, so materialise once.
    await _expected_health(board_id, "summary")

    expected = await _expected_health(board_id, "summary")
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        raw = await _call(HEALTH_TOOL, board_id=board_id, profile="summary")
    payload = json.loads(raw)

    assert "error" not in payload
    # Same summary stop-rule contract as the reader+projection (live values aside).
    assert _key_shape(payload) == _key_shape(expected)
    for stop_rule_field in ("overall_state", "graph_state", "discovery_state"):
        assert stop_rule_field in payload


@pytest.mark.asyncio
async def test_kg_health_matches_reader_for_unknown_board() -> None:
    """Error parity: an unknown board yields the SAME envelope as the reader path."""
    board_id = f"fu4-missing-{uuid.uuid4().hex[:8]}"
    expected = await _expected_health(board_id, "summary")
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        raw = await _call(HEALTH_TOOL, board_id=board_id, profile="summary")
    assert json.loads(raw) == expected


@pytest.mark.asyncio
async def test_kg_health_readiness_matches_reader_for_seeded_board() -> None:
    board_id = f"fu4-ready-{uuid.uuid4().hex[:8]}"
    await _seed_board(board_id)
    expected = await _expected_readiness(board_id, "summary", "")
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        raw = await _call(READINESS_TOOL, board_id=board_id, profile="summary", artifact_ref="")
    assert json.loads(raw) == expected


@pytest.mark.asyncio
async def test_kg_health_readiness_invalid_profile_parity() -> None:
    """Error parity for a bad profile: tool == reader envelope (invalid_profile when
    the service rejects it)."""
    board_id = f"fu4-badprofile-{uuid.uuid4().hex[:8]}"
    await _seed_board(board_id)
    expected = await _expected_readiness(board_id, "definitely-not-a-profile", "")
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        raw = await _call(
            READINESS_TOOL, board_id=board_id,
            profile="definitely-not-a-profile", artifact_ref="",
        )
    assert json.loads(raw) == expected


# --- auth negative path ----------------------------------------------------


@pytest.mark.asyncio
async def test_kg_health_auth_gates_before_use_case() -> None:
    """ctx None → unchanged _auth_error() envelope and the use case never runs."""
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    expected = mcp_server._auth_error()

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=None)), patch(
        "okto_pulse.core.application.use_cases.kg_health.GetKgHealthUseCase.execute",
        AsyncMock(side_effect=AssertionError("use case must not run when auth fails")),
    ):
        tool = await mcp_server.mcp.get_tool(HEALTH_TOOL)
        raw = await tool.fn(board_id="any", profile="summary")

    assert raw == expected


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


def test_migration_is_limited_to_the_cluster() -> None:
    """TR4: only the two health tools were migrated — get_db_for_mcp still appears
    in other handlers; mcp/server.py was not swept."""
    tree = ast.parse(Path(mcp_server.__file__).read_text(encoding="utf-8"))
    other_uses = 0
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
            and node.name not in MIGRATED_TOOLS
        ):
            other_uses += sum(
                1
                for n in ast.walk(node)
                if isinstance(n, ast.Name) and n.id == "get_db_for_mcp"
            )
    assert other_uses > 0
