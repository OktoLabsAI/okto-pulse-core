"""Spec R01A MCP-FU4 / R06 — KG query auth path.

The standalone ``ListBoardsForAgentUseCase`` still proves the R01A UoW path.
After R06, the MCP ``_get_user_boards`` helper no longer uses that path as an
invisible ACL fallback: it consumes registered AuthContext or fails closed.
The ``register_kg_power_tools`` graph-only escape hatches keep their unused
``get_db`` injection removed.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import ast
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.mcp import kg_power_tools, kg_query_tools
from okto_pulse.core.mcp import server as mcp_server


class _Agent:
    def __init__(self, id_: str, name: str = "fu4-query-agent") -> None:
        self.id = id_
        self.name = name


async def _seed_agent_boards(agent_id: str, n: int = 2) -> list[str]:
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import Agent, AgentBoard, Board

    factory = get_session_factory()
    board_ids: list[str] = []
    async with factory() as db:
        if await db.get(Agent, agent_id) is None:
            db.add(Agent(
                id=agent_id,
                name="fu4-query-agent",
                api_key=f"fixture-{agent_id}",
                api_key_hash=f"fixture-hash-{agent_id}",
                created_by="fu4-owner",
            ))
            await db.flush()
        for i in range(n):
            bid = f"fu4q-board-{uuid.uuid4().hex[:8]}"
            db.add(
                Board(
                    id=bid,
                    name=f"zz-{i}-{bid}",
                    owner_id="fu4-owner",
                    realm_id=LOCAL_REALM_ID,
                )
            )
            await db.flush()
            db.add(AgentBoard(agent_id=agent_id, board_id=bid, granted_by="fu4-owner"))
            board_ids.append(bid)
        await db.commit()
    return board_ids


async def _baseline_boards(agent_id: str) -> list[str]:
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.main import AgentService

    async with get_session_factory()() as db:
        return [b.id for b in await AgentService(db).list_boards_for_agent(agent_id)]


# --- parity: the use case == the reader -----------------------------------


@pytest.mark.asyncio
async def test_list_boards_for_agent_use_case_parity() -> None:
    from okto_pulse.core.application.use_cases import (
        ListBoardsForAgentCommand,
        ListBoardsForAgentUseCase,
    )
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.infra.database import get_session_factory

    agent_id = f"fu4q-agent-{uuid.uuid4().hex[:8]}"
    seeded = await _seed_agent_boards(agent_id, 2)
    baseline = await _baseline_boards(agent_id)
    assert sorted(baseline) == sorted(seeded)

    register_mcp_test_runtime(get_session_factory())
    actor = ActorContext(agent_id, "mcp", realm_id=LOCAL_REALM_ID)
    async with mcp_server.get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await ListBoardsForAgentUseCase().execute(
            ListBoardsForAgentCommand(agent_id), actor=actor, uow=uow
        )
    assert result.board_ids == baseline


@pytest.mark.asyncio
async def test_get_user_boards_without_auth_context_fails_closed() -> None:
    """The shared helper must not use get_agent/get_uow as a hidden ACL fallback."""
    from okto_pulse.core.infra.database import get_session_factory

    agent_id = f"fu4q-agent-{uuid.uuid4().hex[:8]}"
    await _seed_agent_boards(agent_id, 2)
    register_mcp_test_runtime(get_session_factory())

    async def _get_agent():
        raise AssertionError("R06 forbids get_agent fallback")

    with patch.object(kg_query_tools, "_get_auth_context", AsyncMock(return_value=None)):
        agent, boards = await kg_query_tools._get_user_boards(
            get_agent=_get_agent,
            get_uow=mcp_server.get_unit_of_work_factory_for_mcp,
        )
    assert agent is None and boards == []


@pytest.mark.asyncio
async def test_get_user_boards_fallback_empty_when_no_agent() -> None:
    """Behaviour preserved: no authenticated agent → (None, [])."""
    with patch.object(kg_query_tools, "_get_auth_context", AsyncMock(return_value=None)):
        agent, boards = await kg_query_tools._get_user_boards(
            get_agent=AsyncMock(return_value=None),
            get_uow=mcp_server.get_unit_of_work_factory_for_mcp,
        )
    assert agent is None and boards == []


# --- AST strangler proofs (codex's three invariants) -----------------------


def _fn(mod, name):
    tree = ast.parse(Path(mod.__file__).read_text(encoding="utf-8"))
    for n in ast.walk(tree):
        if isinstance(n, (ast.AsyncFunctionDef, ast.FunctionDef)) and n.name == name:
            return n
    raise AssertionError(f"{name} not found in {mod.__file__}")


def test_query_register_injects_uow_not_get_db() -> None:
    params = [a.arg for a in _fn(kg_query_tools, "register_kg_query_tools").args.kwonlyargs]
    assert "get_db" not in params
    assert "get_uow" in params


def test_get_user_boards_has_no_relational_session() -> None:
    names = {n.id for n in ast.walk(_fn(kg_query_tools, "_get_user_boards")) if isinstance(n, ast.Name)}
    assert "get_db" not in names
    assert "ListBoardsForAgentUseCase" not in names


def test_power_register_drops_dead_get_db_injection() -> None:
    params = [a.arg for a in _fn(kg_power_tools, "register_kg_power_tools").args.kwonlyargs]
    assert "get_db" not in params


def test_query_and_power_modules_have_no_get_db_for_mcp() -> None:
    for mod in (kg_query_tools, kg_power_tools):
        assert "get_db_for_mcp" not in Path(mod.__file__).read_text(encoding="utf-8")


def test_server_registration_injects_uow_not_get_db_for_mcp() -> None:
    tree = ast.parse(Path(mcp_server.__file__).read_text(encoding="utf-8"))

    def registration(name: str) -> ast.Call:
        matches = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == name
        ]
        assert len(matches) == 1, name
        return matches[0]

    query = registration("_register_kg_query_tools")
    assert [arg.id for arg in query.args if isinstance(arg, ast.Name)] == ["mcp"]
    query_kwargs = {
        keyword.arg: keyword.value.id
        for keyword in query.keywords
        if keyword.arg and isinstance(keyword.value, ast.Name)
    }
    assert query_kwargs == {
        "get_agent": "_get_authenticated_agent",
        "get_uow": "get_unit_of_work_factory_for_mcp",
    }

    power = registration("_register_kg_power_tools")
    assert [arg.id for arg in power.args if isinstance(arg, ast.Name)] == ["mcp"]
    power_kwargs = {
        keyword.arg: keyword.value.id
        for keyword in power.keywords
        if keyword.arg and isinstance(keyword.value, ast.Name)
    }
    assert power_kwargs == {"get_agent": "_get_authenticated_agent"}
