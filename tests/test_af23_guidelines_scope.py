"""AF23 Guidelines scope regressions.

These tests capture the realm-ready boundary for Guidelines before the
implementation changes the service/use-case contract away from raw owner_id.
"""

from __future__ import annotations

import ast
import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.default_board_config import router as default_board_config_router
from okto_pulse.community.api.guidelines import router as guidelines_router
from okto_pulse.core.application.scope import QueryScope
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.models.schemas import GuidelineUpdate

USER = "af23-guidelines-user"
OTHER = "af23-guidelines-other"
PREFIX = "/api/v1"
CORE_SRC_PATH = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"
MCP_SERVER_PATH = CORE_SRC_PATH / "mcp" / "server.py"
GUIDELINE_SERVICE_PATH = CORE_SRC_PATH / "services" / "main.py"
GUIDELINES_REST_PATH = CORE_SRC_PATH / "api" / "guidelines.py"
GUIDELINES_USE_CASE_PATH = CORE_SRC_PATH / "application" / "use_cases" / "guidelines_crud.py"
MCP_BOARD_USE_CASE_PATH = CORE_SRC_PATH / "application" / "use_cases" / "mcp_board_crud.py"
DEFAULT_CONFIG_SERVICE_PATH = CORE_SRC_PATH / "services" / "default_board_configuration.py"
DEFAULT_CONFIG_API_SERVICE_PATH = CORE_SRC_PATH / "services" / "default_board_config_api.py"
DEFAULT_CONFIG_REST_PATH = CORE_SRC_PATH / "api" / "default_board_config.py"


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(default_board_config_router, prefix=PREFIX)
    app.include_router(guidelines_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    return TestClient(app)


async def _seed_board(owner: str) -> str:
    from sqlalchemy_test_models import Board

    board_id = f"af23-board-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name=board_id, owner_id=owner))
        await db.commit()
    return board_id


async def _seed_guideline(
    owner: str,
    *,
    scope: str = "global",
    board_id: str | None = None,
) -> str:
    from sqlalchemy_test_models import Guideline

    guideline_id = f"af23-guideline-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Guideline(
                id=guideline_id,
                title="foreign guideline",
                content="must stay scoped",
                tags=["af23"],
                scope=scope,
                board_id=board_id,
                owner_id=owner,
            )
        )
        await db.commit()
    return guideline_id


async def _link_guideline(board_id: str, guideline_id: str, priority: int = 1) -> None:
    from sqlalchemy_test_models import BoardGuideline

    async with get_session_factory()() as db:
        db.add(
            BoardGuideline(
                board_id=board_id,
                guideline_id=guideline_id,
                priority=priority,
            )
        )
        await db.commit()


@pytest.mark.asyncio
async def test_ts1_foreign_global_guideline_get_is_fail_closed(client: TestClient) -> None:
    guideline_id = await _seed_guideline(OTHER)

    response = client.get(f"{PREFIX}/guidelines/{guideline_id}")

    assert response.status_code == 404
    assert response.json()["detail"] == "Guideline not found"


@pytest.mark.asyncio
async def test_ts1_foreign_board_unlink_and_priority_are_fail_closed(client: TestClient) -> None:
    board_id = await _seed_board(OTHER)
    guideline_id = await _seed_guideline(OTHER)
    await _link_guideline(board_id, guideline_id)

    unlink = client.delete(f"{PREFIX}/boards/{board_id}/guidelines/{guideline_id}")
    priority = client.patch(
        f"{PREFIX}/boards/{board_id}/guidelines/{guideline_id}",
        json={"priority": 7},
    )

    assert unlink.status_code == 404
    assert unlink.json()["detail"] == "Board not found"
    assert priority.status_code == 404
    assert priority.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_ts2_owner_floor_is_preserved_when_query_scope_is_none() -> None:
    from okto_pulse.core.services import GuidelineService

    guideline_id = await _seed_guideline(USER)

    async with get_session_factory()() as db:
        service = GuidelineService(db)
        guidelines = await service.list_guidelines(USER, query_scope=None)
        updated = await service.update_guideline(
            guideline_id,
            USER,
            GuidelineUpdate(title="local owner update"),
            query_scope=None,
        )

    assert any(guideline.id == guideline_id for guideline in guidelines)
    assert updated is not None
    assert updated.title == "local owner update"


@pytest.mark.asyncio
async def test_ts2_query_scope_actor_is_authoritative_when_present() -> None:
    from okto_pulse.core.services import GuidelineService

    owner_guideline = await _seed_guideline(USER)
    other_guideline = await _seed_guideline(OTHER)
    query_scope = QueryScope(actor_id=USER, source="test")

    async with get_session_factory()() as db:
        service = GuidelineService(db)
        scoped_guidelines = await service.list_guidelines(
            OTHER,
            query_scope=query_scope,
        )
        owner_visible = await service.get_guideline(
            owner_guideline,
            owner_id=OTHER,
            query_scope=query_scope,
        )
        other_hidden = await service.get_guideline(
            other_guideline,
            owner_id=OTHER,
            query_scope=query_scope,
        )

    assert any(guideline.id == owner_guideline for guideline in scoped_guidelines)
    assert all(guideline.owner_id == USER for guideline in scoped_guidelines)
    assert owner_visible is not None
    assert other_hidden is None


def _async_function_source(path: Path, function_name: str) -> str:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    lines = source.splitlines()
    for node in tree.body:
        if isinstance(node, ast.AsyncFunctionDef) and node.name == function_name:
            return "\n".join(lines[node.lineno - 1 : node.end_lineno])
    raise AssertionError(f"{function_name} not found in {path}")


def _class_source(path: Path, class_name: str) -> str:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    lines = source.splitlines()
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return "\n".join(lines[node.lineno - 1 : node.end_lineno])
    raise AssertionError(f"{class_name} not found in {path}")


async def _call_mcp_tool(tool_name: str, **kwargs) -> dict:
    from okto_pulse.core.mcp import server as mcp_server

    ctx = mcp_server.AgentContext(
        agent_id=USER,
        agent_name="AF23 scoped actor",
        board_id=kwargs.get("board_id", ""),
        permissions=None,
    )
    mcp_server.register_session_factory(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=ctx)):
        return json.loads(await getattr(mcp_server, tool_name).fn(**kwargs))


def test_ts3_mcp_guideline_tools_use_scoped_use_cases_not_board_owner() -> None:
    tool_names = [
        "okto_pulse_get_board_guidelines",
        "okto_pulse_list_guidelines",
        "okto_pulse_create_guideline",
        "okto_pulse_update_guideline",
        "okto_pulse_delete_guideline",
        "okto_pulse_link_guideline_to_board",
        "okto_pulse_unlink_guideline_from_board",
        "okto_pulse_update_board_guideline_priority",
    ]

    combined = "\n\n".join(_async_function_source(MCP_SERVER_PATH, name) for name in tool_names)

    assert "board.owner_id" not in combined
    assert "db.get(Board" not in combined
    assert combined.count("MCPAdapterContract.actor") == len(tool_names)
    assert combined.count("get_unit_of_work_factory_for_mcp") == len(tool_names)


@pytest.mark.asyncio
async def test_ts4_default_candidates_are_scoped_and_global_only(client: TestClient) -> None:
    from okto_pulse.core.services.default_board_configuration import (
        DefaultBoardConfigurationService,
    )

    visible_guideline = await _seed_guideline(USER)
    foreign_guideline = await _seed_guideline(OTHER)
    board_id = await _seed_board(USER)
    inline_guideline = await _seed_guideline(USER, scope="inline", board_id=board_id)
    template_scope = f"af23-scope-{uuid.uuid4().hex[:8]}"

    async with get_session_factory()() as db:
        await DefaultBoardConfigurationService(db).create_version(
            settings_payload=None,
            actor=USER,
            scope=template_scope,
            guideline_default_refs=[
                {"guideline_id": visible_guideline, "priority": 3}
            ],
            activate=True,
        )
        await db.commit()

    response = client.get(
        f"{PREFIX}/guidelines/default-candidates",
        params={"scope": template_scope},
    )

    assert response.status_code == 200
    candidates = response.json()["candidates"]
    candidate_by_id = {item["guideline_id"]: item for item in candidates}
    assert visible_guideline in candidate_by_id
    assert foreign_guideline not in candidate_by_id
    assert inline_guideline not in candidate_by_id
    assert candidate_by_id[visible_guideline]["is_default"] is True
    assert candidate_by_id[visible_guideline]["priority"] == 3


@pytest.mark.asyncio
async def test_ts4_default_guideline_refs_reject_inline_missing_and_foreign(
    client: TestClient,
) -> None:
    from okto_pulse.core.services.default_board_configuration import (
        DefaultBoardConfigurationService,
    )

    visible_guideline = await _seed_guideline(USER)
    foreign_guideline = await _seed_guideline(OTHER)
    board_id = await _seed_board(USER)
    inline_guideline = await _seed_guideline(USER, scope="inline", board_id=board_id)
    template_scope = f"af23-scope-{uuid.uuid4().hex[:8]}"

    async with get_session_factory()() as db:
        template = await DefaultBoardConfigurationService(db).create_version(
            settings_payload=None,
            actor=USER,
            scope=template_scope,
        )
        await db.commit()
        template_id = template.id

    ok = client.post(
        f"{PREFIX}/default-board-configurations/{template_id}/guidelines",
        json={"guideline_default_refs": [{"guideline_id": visible_guideline}]},
    )

    assert ok.status_code == 200
    assert ok.json()["guideline_default_refs"][0]["guideline_id"] == visible_guideline

    invalid_cases = [
        ({"guideline_id": foreign_guideline}, "default_guideline_not_found"),
        ({"guideline_id": inline_guideline}, "default_guideline_not_global"),
        ({"guideline_id": "missing-guideline"}, "default_guideline_not_found"),
        ({"title": "inline default"}, "default_guideline_inline_not_allowed"),
    ]
    for ref, expected_code in invalid_cases:
        response = client.post(
            f"{PREFIX}/default-board-configurations/{template_id}/guidelines",
            json={"guideline_default_refs": [ref]},
        )
        assert response.status_code == 422
        assert response.json()["detail"]["code"] == expected_code


def test_ts4_mcp_default_guideline_tools_pass_query_scope() -> None:
    tool_names = [
        "okto_pulse_list_default_guideline_candidates",
        "okto_pulse_update_default_guideline_refs",
    ]

    combined = "\n\n".join(_async_function_source(MCP_SERVER_PATH, name) for name in tool_names)

    assert combined.count("MCPAdapterContract.actor") == len(tool_names)
    assert combined.count("ActorScope.from_context(actor).query_scope") == len(tool_names)
    assert combined.count("query_scope=query_scope") == len(tool_names)


@pytest.mark.asyncio
async def test_ts5_mcp_cross_scope_priority_update_is_denied() -> None:
    from sqlalchemy import select

    from sqlalchemy_test_models import BoardGuideline

    board_id = await _seed_board(OTHER)
    guideline_id = await _seed_guideline(OTHER)
    await _link_guideline(board_id, guideline_id, priority=1)

    response = await _call_mcp_tool(
        "okto_pulse_update_board_guideline_priority",
        board_id=board_id,
        guideline_id=guideline_id,
        priority="9",
    )

    assert response == {"error": "Link not found"}
    async with get_session_factory()() as db:
        persisted = (
            await db.execute(
                select(BoardGuideline).where(
                    BoardGuideline.board_id == board_id,
                    BoardGuideline.guideline_id == guideline_id,
                )
            )
        ).scalar_one()
        assert persisted is not None
        assert persisted.priority == 1


def test_ts5_guidelines_scope_gate_blocks_raw_owner_auth_and_boardshare_drift() -> None:
    guideline_service_source = _class_source(GUIDELINE_SERVICE_PATH, "GuidelineService")
    scoped_sources = {
        "guidelines_rest": GUIDELINES_REST_PATH,
        "guidelines_use_cases": GUIDELINES_USE_CASE_PATH,
        "mcp_board_use_cases": MCP_BOARD_USE_CASE_PATH,
        "default_config_service": DEFAULT_CONFIG_SERVICE_PATH,
        "default_config_api_service": DEFAULT_CONFIG_API_SERVICE_PATH,
        "default_config_rest": DEFAULT_CONFIG_REST_PATH,
        "mcp_server": MCP_SERVER_PATH,
    }

    for label, path in scoped_sources.items():
        source = path.read_text(encoding="utf-8")
        assert "BoardShare" not in source, label
        assert "ShareService" not in source, label

    assert "BoardShare" not in guideline_service_source
    assert "ShareService" not in guideline_service_source
    assert "inbound adapters must pass an explicit scope" in guideline_service_source
    assert "query_scope" in guideline_service_source
    assert "board.owner_id" not in guideline_service_source
    assert "db.get(Board" not in guideline_service_source

    for label, path in {
        "guidelines_use_cases": GUIDELINES_USE_CASE_PATH,
        "mcp_board_use_cases": MCP_BOARD_USE_CASE_PATH,
        "default_config_service": DEFAULT_CONFIG_SERVICE_PATH,
        "default_config_api_service": DEFAULT_CONFIG_API_SERVICE_PATH,
        "default_config_rest": DEFAULT_CONFIG_REST_PATH,
        "mcp_server": MCP_SERVER_PATH,
    }.items():
        assert "query_scope" in path.read_text(encoding="utf-8"), label
