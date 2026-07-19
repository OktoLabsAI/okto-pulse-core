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
from okto_pulse.core.domain.realm import LOCAL_REALM_ID


CORE_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"


def _source(relative: str) -> str:
    return (CORE_ROOT / relative).read_text(encoding="utf-8")


def _kg_tool_policy_violations(source: str) -> list[str]:
    tree = ast.parse(source)
    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            continue
        if not node.name.startswith("okto_pulse_kg_"):
            continue
        body = ast.get_source_segment(source, node) or ""
        if "board_id" not in {arg.arg for arg in node.args.args}:
            continue
        has_auth = "_get_user_boards(" in body or "ActorScope.from_context" in body
        has_scope = (
            "svc.check_board_access(" in body
            or "target_boards" in body
            or "allowed_board_ids=" in body
        )
        has_admin_gate = "kg.admin.historical_consolidation" in body
        if not has_auth:
            violations.append(f"{node.name}:missing_auth")
        if not (has_scope or has_admin_gate):
            violations.append(f"{node.name}:missing_scope_or_capability")
    return violations


def test_core_kg_surfaces_do_not_hardcode_local_user() -> None:
    for relative in (
        "kg/governance.py",
        "mcp/kg_query_tools.py",
        "mcp/server.py",
    ):
        assert "local-user" not in _source(relative), relative

    from okto_pulse.community.api import kg_routes

    assert "local-user" not in Path(kg_routes.__file__).read_text(encoding="utf-8")


def test_actor_scope_preserves_non_mapping_permission_objects() -> None:
    class PermissionLike:
        def check(self, _permission: str) -> None:
            return None

    permissions = PermissionLike()
    actor = ActorContext(
        "agent-af13", "mcp", realm_id=LOCAL_REALM_ID, permissions=permissions
    )
    scope = ActorScope.from_context(actor)

    assert scope.permissions is permissions


def test_rest_kg_board_routes_require_board_actor_dependency() -> None:
    from fastapi.params import Depends as DependsParam

    from okto_pulse.community.api import kg_routes

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
            or kg_routes.require_kg_board_writer_actor in deps
            or kg_routes.require_kg_admin_board_actor in deps
            or kg_routes.require_kg_stream_board_actor in deps
        ), path

    migrate_deps = route_dependencies["post_migrate_schema"]
    assert kg_routes.require_kg_admin_board_actor in migrate_deps
    stream_deps = route_dependencies["stream_kg_events"]
    assert kg_routes.require_kg_stream_board_actor in stream_deps


@pytest.mark.asyncio
async def test_global_search_passes_only_actor_visible_boards(monkeypatch) -> None:
    from okto_pulse.core.application.use_cases.kg_routes_crud import (
        GlobalSearchCommand,
        GlobalSearchUseCase,
    )
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import Board
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWork
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
                Board(
                    id=allowed_board,
                    name="allowed",
                    owner_id=owner_id,
                    realm_id=LOCAL_REALM_ID,
                ),
                Board(
                    id=forbidden_board,
                    name="forbidden",
                    owner_id="other-user",
                    realm_id=LOCAL_REALM_ID,
                ),
            ]
        )
        await db.commit()

    actor = ActorContext(owner_id, "rest", realm_id=LOCAL_REALM_ID)
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


@pytest.mark.asyncio
async def test_global_search_with_zero_visible_boards_never_falls_back_to_all(
    monkeypatch,
) -> None:
    from okto_pulse.core.application.use_cases.kg_routes_crud import (
        GlobalSearchCommand,
        GlobalSearchUseCase,
    )
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import Board
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWork
    from okto_pulse.core.services import application_kg

    actor_id = f"user-af13-empty-{uuid.uuid4().hex[:8]}"
    forbidden_board = f"board-af13-hidden-{uuid.uuid4().hex[:8]}"
    captured: dict[str, list[str]] = {}

    def _fake_query_global(_query: str, *, user_boards: list[str], **_kwargs):
        captured["user_boards"] = user_boards
        return [
            {"board_id": board_id, "id": f"node-{board_id}", "title": board_id}
            for board_id in user_boards
        ]

    monkeypatch.setattr(application_kg, "query_global", _fake_query_global)

    async with get_session_factory()() as db:
        db.add(
            Board(
                id=forbidden_board,
                name="hidden",
                owner_id="other-user",
                realm_id=LOCAL_REALM_ID,
            )
        )
        await db.commit()

    actor = ActorContext(actor_id, "rest", realm_id=LOCAL_REALM_ID)
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

    assert captured["user_boards"] == []
    assert result.results == []
    assert forbidden_board not in captured["user_boards"]


@pytest.mark.asyncio
async def test_kg_boost_audit_uses_non_default_actor(monkeypatch) -> None:
    from datetime import datetime, timezone

    from sqlalchemy import select

    from okto_pulse.core.application.use_cases.kg_routes_crud import (
        BoostNodeCommand,
        BoostNodeUseCase,
    )
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import Board, ConsolidationAudit
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    from okto_pulse.core.services import application_kg

    actor_id = f"agent-af13-real-{uuid.uuid4().hex[:8]}"
    board_id = f"board-af13-audit-{uuid.uuid4().hex[:8]}"
    node_id = f"node-af13-audit-{uuid.uuid4().hex[:8]}"

    async def _fake_boost_node(db, board_id: str, node_id: str, *, actor_id: str):
        now = datetime.now(timezone.utc)
        db.add(
            ConsolidationAudit(
                session_id=f"boost-{uuid.uuid4().hex[:30]}",
                board_id=board_id,
                artifact_id=node_id,
                artifact_type="boost",
                agent_id=actor_id,
                started_at=now,
                committed_at=now,
                nodes_added=0,
                edges_added=0,
            )
        )
        return {
            "node_id": node_id,
            "node_type": "Entity",
            "score_before": 0.4,
            "score_after": 0.7,
            "boosted_at": now.isoformat(),
            "boosted_by": actor_id,
        }

    monkeypatch.setattr(application_kg, "boost_node", _fake_boost_node)

    async with get_session_factory()() as db:
        db.add(
            Board(
                id=board_id,
                name="AF13 audit",
                owner_id=actor_id,
                realm_id=LOCAL_REALM_ID,
            )
        )
        await db.commit()

    actor = ActorContext(actor_id, "mcp", realm_id=LOCAL_REALM_ID)
    async with SQLAlchemyUnitOfWorkFactory(get_session_factory())(actor=actor) as uow:
        result = await BoostNodeUseCase().execute(
            BoostNodeCommand(board_id, node_id),
            actor=actor,
            uow=uow,
        )

    assert result.payload["boosted_by"] == actor_id

    async with get_session_factory()() as db:
        row = (
            await db.execute(
                select(ConsolidationAudit).where(
                    ConsolidationAudit.board_id == board_id,
                    ConsolidationAudit.artifact_id == node_id,
                    ConsolidationAudit.artifact_type == "boost",
                )
            )
        ).scalar_one()

    assert row.agent_id == actor_id


def test_mcp_kg_inventory_uses_scope_and_admin_gates() -> None:
    server_source = _source("mcp/server.py")
    query_tools_source = _source("mcp/kg_query_tools.py")

    migrate_fn = _function_source(server_source, "okto_pulse_kg_migrate_schema")
    query_global_fn = _function_source(query_tools_source, "okto_pulse_kg_query_global")

    assert "ActorScope.from_context" in migrate_fn
    assert "kg.admin.historical_consolidation" in migrate_fn
    assert "allowed_board_ids=" in migrate_fn
    assert "svc.check_board_access(boards, board_id)" in query_global_fn
    assert _kg_tool_policy_violations(query_tools_source) == []


def test_kg_tool_inventory_guard_bites_missing_scope_policy() -> None:
    bad_source = """
async def okto_pulse_kg_query_new(board_id: str) -> str:
    return "{}"
"""

    assert _kg_tool_policy_violations(bad_source) == [
        "okto_pulse_kg_query_new:missing_auth",
        "okto_pulse_kg_query_new:missing_scope_or_capability",
    ]


@pytest.mark.asyncio
async def test_mcp_migrate_all_boards_denies_non_admin_before_listing(monkeypatch) -> None:
    from okto_pulse.core.mcp import server as mcp_server

    class _Ctx:
        agent_id = "agent-af13"
        agent_name = "AF13"
        permissions: list[str] = []

    async def _ctx():
        return _Ctx()

    def _uow_forbidden():
        raise AssertionError("all_boards must not list boards before admin gate")

    monkeypatch.setattr(mcp_server, "_get_global_agent_ctx", _ctx)
    monkeypatch.setattr(
        mcp_server, "get_unit_of_work_factory_for_mcp", _uow_forbidden
    )

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
