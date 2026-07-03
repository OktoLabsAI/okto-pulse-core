"""AF13 — ActorScope/QueryScope on KG REST and MCP surfaces."""

from __future__ import annotations

import ast
import json
import inspect
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.application.scope import ActorScope
from okto_pulse.core.application.use_cases import ActorContext


CORE_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"


def _source(relative: str) -> str:
    return (CORE_ROOT / relative).read_text(encoding="utf-8")


def test_core_kg_surfaces_do_not_hardcode_local_user() -> None:
    for relative in (
        "api/kg_routes.py",
        "kg/governance.py",
        "mcp/kg_query_tools.py",
        "mcp/server.py",
    ):
        assert "local-user" not in _source(relative), relative


def test_actor_scope_preserves_non_mapping_permission_objects() -> None:
    class PermissionLike:
        def check(self, _permission: str) -> None:
            return None

    permissions = PermissionLike()
    actor = ActorContext("agent-af13", "mcp", permissions=permissions)
    scope = ActorScope.from_context(actor)

    assert scope.permissions is permissions


def test_rest_kg_board_routes_require_board_actor_dependency() -> None:
    from fastapi.params import Depends as DependsParam

    from okto_pulse.core.api import kg_routes

    route_dependencies = {
        route.endpoint.__name__: {
            param.default.dependency
            for param in inspect.signature(route.endpoint).parameters.values()
            if isinstance(param.default, DependsParam)
        }
        for route in kg_routes.router.routes
        if getattr(route, "path", "").startswith("/kg/")
    }

    for route in kg_routes.router.routes:
        path = getattr(route, "path", "")
        if "/boards/{board_id}" not in path:
            continue
        deps = route_dependencies[route.endpoint.__name__]
        assert (
            kg_routes.require_kg_board_actor in deps
            or kg_routes.require_kg_admin_board_actor in deps
        ), path

    migrate_deps = route_dependencies["post_migrate_schema"]
    assert kg_routes.require_kg_admin_board_actor in migrate_deps


@pytest.mark.asyncio
async def test_global_search_passes_only_actor_visible_boards(monkeypatch) -> None:
    from okto_pulse.core.application.use_cases.kg_routes_crud import (
        GlobalSearchCommand,
        GlobalSearchUseCase,
    )
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.models.db import Board
    from okto_pulse.core.repositories import SQLAlchemyUnitOfWork
    from okto_pulse.core.services import application_kg

    owner_id = f"user-af13-{uuid.uuid4().hex[:8]}"
    allowed_board = f"board-af13-allowed-{uuid.uuid4().hex[:8]}"
    forbidden_board = f"board-af13-forbidden-{uuid.uuid4().hex[:8]}"
    captured: dict[str, list[str]] = {}

    def _fake_query_global(_query: str, *, user_boards: list[str], **_kwargs):
        captured["user_boards"] = user_boards
        return [
            {"board_id": board_id, "id": f"node-{board_id}", "title": board_id}
            for board_id in user_boards
        ]

    monkeypatch.setattr(application_kg, "query_global", _fake_query_global)

    async with get_session_factory()() as db:
        db.add_all(
            [
                Board(id=allowed_board, name="allowed", owner_id=owner_id),
                Board(id=forbidden_board, name="forbidden", owner_id="other-user"),
            ]
        )
        await db.commit()

    actor = ActorContext(owner_id, "rest")
    async with get_session_factory()() as db:
        result = await GlobalSearchUseCase().execute(
            GlobalSearchCommand(
                q="auth scope",
                limit=20,
                min_similarity=0.3,
                graph_layer="canonical",
            ),
            actor=actor,
            uow=SQLAlchemyUnitOfWork(db),
        )

    assert captured["user_boards"] == [allowed_board]
    assert {row["board_id"] for row in result.results} == {allowed_board}
    assert forbidden_board not in captured["user_boards"]


def test_mcp_kg_inventory_uses_scope_and_admin_gates() -> None:
    server_source = _source("mcp/server.py")
    query_tools_source = _source("mcp/kg_query_tools.py")

    migrate_fn = _function_source(server_source, "okto_pulse_kg_migrate_schema")
    query_global_fn = _function_source(query_tools_source, "okto_pulse_kg_query_global")

    assert "ActorScope.from_context" in migrate_fn
    assert "kg.admin.historical_consolidation" in migrate_fn
    assert "allowed_board_ids=" in migrate_fn
    assert "svc.check_board_access(boards, board_id)" in query_global_fn


@pytest.mark.asyncio
async def test_mcp_migrate_all_boards_denies_non_admin_before_listing(monkeypatch) -> None:
    from okto_pulse.core.mcp import server as mcp_server

    class _Ctx:
        agent_id = "agent-af13"
        agent_name = "AF13"
        permissions: list[str] = []

    async def _ctx():
        return _Ctx()

    def _db_forbidden():
        raise AssertionError("all_boards must not list boards before admin gate")

    monkeypatch.setattr(mcp_server, "_get_global_agent_ctx", _ctx)
    monkeypatch.setattr(mcp_server, "get_db_for_mcp", _db_forbidden)

    payload = json.loads(
        await mcp_server.okto_pulse_kg_migrate_schema.fn(all_boards=True)
    )

    assert "kg.admin.historical_consolidation" in payload["error"]


def _function_source(source: str, name: str) -> str:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == name:
            return ast.get_source_segment(source, node) or ""
    raise AssertionError(f"{name} not found")
