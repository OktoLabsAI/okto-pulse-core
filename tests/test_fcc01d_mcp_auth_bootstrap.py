"""FCC-01D-A MCP auth_bootstrap migration coverage."""

from __future__ import annotations

import ast
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import select

from okto_pulse.core.infra.permissions import Permissions, map_legacy_permissions
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import Agent, AgentBoard, Board, PermissionPreset
from okto_pulse.core.ports import AgentAuthSession, McpCredential
from okto_pulse.core.services.main import AgentService


AUTH_BOOTSTRAP_HELPERS = {
    "_authenticate_mcp_credential",
    "_get_agent_ctx_for_credential",
    "_get_global_agent_ctx",
}


class _StaticMcpAuthenticator:
    def __init__(self, sessions: dict[str, AgentAuthSession | None]) -> None:
        self.sessions = sessions
        self.seen: list[McpCredential | None] = []

    async def authenticate(self, credential):
        self.seen.append(credential)
        if credential is None:
            return None
        return self.sessions.get(credential.value)


def _server_tree() -> ast.Module:
    return ast.parse(Path(mcp_server.__file__).read_text(encoding="utf-8"))


def _function_node(name: str) -> ast.AsyncFunctionDef | ast.FunctionDef:
    for node in ast.walk(_server_tree()):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"missing function node: {name}")


def _callee_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _calls(node: ast.AST, callee: str) -> bool:
    return any(
        isinstance(candidate, ast.Call) and _callee_name(candidate) == callee
        for candidate in ast.walk(node)
    )


def _opens_direct_get_db_for_mcp(node: ast.AST) -> bool:
    for candidate in ast.walk(node):
        if not isinstance(candidate, ast.AsyncWith):
            continue
        for item in candidate.items:
            if (
                isinstance(item.context_expr, ast.Call)
                and _callee_name(item.context_expr) == "get_db_for_mcp"
            ):
                return True
    return False


def test_auth_bootstrap_helpers_do_not_open_direct_mcp_db_sessions() -> None:
    forbidden_import_modules = {"sqlalchemy", "okto_pulse.core.models.db"}
    forbidden_names = {"AgentService", "AgentBoard", "PermissionPreset", "select", "sa_select"}

    for helper in AUTH_BOOTSTRAP_HELPERS:
        node = _function_node(helper)
        assert not _opens_direct_get_db_for_mcp(node), helper
        assert not _calls(node, "get_db_for_mcp"), helper
        assert not _calls(node, "AgentService"), helper
        imported_modules = {
            imported.module
            for imported in ast.walk(node)
            if isinstance(imported, ast.ImportFrom)
        }
        assert imported_modules.isdisjoint(forbidden_import_modules), helper
        names = {candidate.id for candidate in ast.walk(node) if isinstance(candidate, ast.Name)}
        assert names.isdisjoint(forbidden_names), helper

    assert _calls(_function_node("_authenticate_mcp_session"), "get_mcp_authenticator_for_mcp")
    assert _calls(_function_node("_get_agent_ctx_for_credential"), "get_unit_of_work_factory_for_mcp")
    assert _calls(_function_node("_get_global_agent_ctx"), "get_unit_of_work_factory_for_mcp")


@pytest.mark.asyncio
async def test_authenticate_mcp_credential_uses_registered_authenticator(monkeypatch) -> None:
    secret = "fcc01d-secret"
    authenticator = _StaticMcpAuthenticator(
        {
            secret: AgentAuthSession(
                agent_id="agent-fcc01d",
                agent_name="FCC01D Agent",
                is_active=True,
                metadata={"credential_source": "query_param"},
            ),
            "inactive": AgentAuthSession(
                agent_id="inactive-agent",
                agent_name="Inactive",
                is_active=False,
            ),
        }
    )
    monkeypatch.setattr(mcp_server, "_mcp_authenticator", authenticator)

    resolved = await mcp_server._authenticate_mcp_credential(
        McpCredential(source="query_param", value=secret)
    )

    assert resolved is not None
    assert resolved.id == "agent-fcc01d"
    assert resolved.name == "FCC01D Agent"
    assert resolved.api_key != secret
    assert secret not in repr(resolved)
    assert secret not in str(resolved.metadata)
    assert authenticator.seen[-1].source == "query_param"

    assert await mcp_server._authenticate_mcp_credential(None) is None
    assert await mcp_server._authenticate_mcp_credential(
        McpCredential(source="x_api_key_header", value="missing")
    ) is None
    assert await mcp_server._authenticate_mcp_credential(
        McpCredential(source="authorization_bearer", value="inactive")
    ) is None


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


async def _seed_context_fixture(db_factory) -> dict[str, str]:
    board_id = _id("board-fcc01d")
    legacy_agent_id = _id("agent-legacy")
    granular_agent_id = _id("agent-granular")
    denied_agent_id = _id("agent-denied")
    preset_id = _id("preset-fcc01d")
    now = datetime.now(timezone.utc)

    async with db_factory() as db:
        db.add(Board(id=board_id, name="FCC01D Board", owner_id="owner"))
        db.add(
            PermissionPreset(
                id=preset_id,
                name="FCC01D Preset",
                owner_id="owner",
                flags=map_legacy_permissions(
                    [Permissions.BOARD_READ, Permissions.CARDS_CREATE]
                ),
            )
        )
        db.add(
            Agent(
                id=legacy_agent_id,
                name="Legacy Agent",
                api_key=AgentService.credential_marker(
                    AgentService.hash_api_key("legacy-key")
                ),
                api_key_hash=AgentService.hash_api_key("legacy-key"),
                permissions=[Permissions.BOARD_READ],
                permission_flags=None,
                is_active=True,
                created_by="owner",
                created_at=now,
            )
        )
        db.add(
            Agent(
                id=granular_agent_id,
                name="Granular Agent",
                api_key=AgentService.credential_marker(
                    AgentService.hash_api_key("granular-key")
                ),
                api_key_hash=AgentService.hash_api_key("granular-key"),
                permissions=[],
                permission_flags={"card": {"entity": {"create": True}}},
                preset_id=preset_id,
                is_active=True,
                created_by="owner",
                created_at=now,
            )
        )
        db.add(
            Agent(
                id=denied_agent_id,
                name="Denied Agent",
                api_key=AgentService.credential_marker(
                    AgentService.hash_api_key("denied-key")
                ),
                api_key_hash=AgentService.hash_api_key("denied-key"),
                permissions=[Permissions.BOARD_READ],
                permission_flags=None,
                is_active=True,
                created_by="owner",
                created_at=now,
            )
        )
        db.add(AgentBoard(agent_id=legacy_agent_id, board_id=board_id, granted_by="owner"))
        db.add(
            AgentBoard(
                agent_id=granular_agent_id,
                board_id=board_id,
                granted_by="owner",
                permission_overrides={"card": {"entity": {"create": False}}},
            )
        )
        await db.commit()

    return {
        "board_id": board_id,
        "legacy_agent_id": legacy_agent_id,
        "granular_agent_id": granular_agent_id,
        "denied_agent_id": denied_agent_id,
    }


@pytest.mark.asyncio
async def test_agent_context_board_and_global_parity(
    db_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ids = await _seed_context_fixture(db_factory)
    authenticator = _StaticMcpAuthenticator(
        {
            "legacy-key": AgentAuthSession(
                agent_id=ids["legacy_agent_id"],
                agent_name="Legacy Agent",
                is_active=True,
            ),
            "granular-key": AgentAuthSession(
                agent_id=ids["granular_agent_id"],
                agent_name="Granular Agent",
                is_active=True,
            ),
            "denied-key": AgentAuthSession(
                agent_id=ids["denied_agent_id"],
                agent_name="Denied Agent",
                is_active=True,
            ),
        }
    )
    mcp_server.register_session_factory(
        db_factory,
        mcp_authenticator=authenticator,
    )

    def _unexpected_db_fallback():
        raise AssertionError("auth_bootstrap helpers must not call get_db_for_mcp")

    monkeypatch.setattr(mcp_server, "get_db_for_mcp", _unexpected_db_fallback)

    board_id = ids["board_id"]
    legacy_credential = McpCredential(source="query_param", value="legacy-key")
    legacy_ctx = await mcp_server._get_agent_ctx_for_credential(
        board_id,
        legacy_credential,
    )
    assert legacy_ctx is not None
    assert legacy_ctx.agent_id == ids["legacy_agent_id"]
    assert legacy_ctx.board_id == board_id
    assert legacy_ctx.permissions == [Permissions.BOARD_READ]

    granular_ctx = await mcp_server._get_agent_ctx_for_credential(
        board_id,
        McpCredential(source="x_api_key_header", value="granular-key"),
    )
    assert granular_ctx is not None
    assert granular_ctx.permissions.has("board.read")
    assert not granular_ctx.permissions.has("card.entity.create")

    denied_ctx = await mcp_server._get_agent_ctx_for_credential(
        board_id,
        McpCredential(source="authorization_bearer", value="denied-key"),
    )
    assert denied_ctx is None

    from okto_pulse.core.services import application_agents

    original_resolver = application_agents.resolve_agent_permission_context

    async def _should_not_resolve_permissions(*_args, **_kwargs):
        raise AssertionError("cached AgentContext should bypass permission resolution")

    monkeypatch.setattr(
        application_agents,
        "resolve_agent_permission_context",
        _should_not_resolve_permissions,
    )
    cached_ctx = await mcp_server._get_agent_ctx_for_credential(
        board_id,
        legacy_credential,
    )
    assert cached_ctx is legacy_ctx

    async with db_factory() as db:
        grant = (
            await db.execute(
                select(AgentBoard).where(
                    AgentBoard.agent_id == ids["legacy_agent_id"],
                    AgentBoard.board_id == board_id,
                )
            )
        ).scalar_one()
        await db.delete(grant)
        await db.commit()

    revoked_ctx = await mcp_server._get_agent_ctx_for_credential(
        board_id,
        legacy_credential,
    )
    assert revoked_ctx is None

    monkeypatch.setattr(
        application_agents,
        "resolve_agent_permission_context",
        original_resolver,
    )
    monkeypatch.setattr(
        mcp_server,
        "active_api_key_credential",
        lambda: McpCredential(source="query_param", value="granular-key"),
    )
    global_ctx = await mcp_server._get_global_agent_ctx()
    assert global_ctx is not None
    assert global_ctx.agent_id == ids["granular_agent_id"]
    assert global_ctx.board_id == ""
    assert global_ctx.permissions.has("board.read")
    assert global_ctx.permissions.has("card.entity.create")

    async with db_factory() as db:
        agents = (
            await db.execute(
                select(Agent).where(
                    Agent.id.in_(
                        [
                            ids["legacy_agent_id"],
                            ids["granular_agent_id"],
                            ids["denied_agent_id"],
                        ]
                    )
                )
            )
        ).scalars().all()
        assert {agent.last_used_at for agent in agents} == {None}
