"""AF23 Guidelines scope regressions.

These tests capture the realm-ready boundary for Guidelines before the
implementation changes the service/use-case contract away from raw owner_id.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import ast
import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import default_board_config as default_board_config_api
from okto_pulse.community.api import guidelines as guidelines_api
from okto_pulse.community.api.default_board_config import (
    router as default_board_config_router,
)
from okto_pulse.community.api.guidelines import router as guidelines_router
from okto_pulse.core.application.scope import QueryScope
from okto_pulse.community.api.auth_deps import (
    get_realm_id,
    require_principal,
    require_user,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.models.schemas import GuidelineCreate, GuidelineUpdate
from okto_pulse.core.ports.authentication import Principal

USER = "af23-guidelines-user"
OTHER = "af23-guidelines-other"
PREFIX = "/api/v1"
CORE_SRC_PATH = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"
MCP_SERVER_PATH = CORE_SRC_PATH / "mcp" / "server.py"
GUIDELINE_SERVICE_PATH = CORE_SRC_PATH / "services" / "main.py"
GUIDELINES_REST_PATH = Path(guidelines_api.__file__)
GUIDELINES_USE_CASE_PATH = (
    CORE_SRC_PATH / "application" / "use_cases" / "guidelines_crud.py"
)
MCP_BOARD_USE_CASE_PATH = (
    CORE_SRC_PATH / "application" / "use_cases" / "mcp_board_crud.py"
)
DEFAULT_CONFIG_SERVICE_PATH = (
    CORE_SRC_PATH / "services" / "default_board_configuration.py"
)
DEFAULT_CONFIG_API_SERVICE_PATH = (
    CORE_SRC_PATH / "services" / "default_board_config_api.py"
)
DEFAULT_CONFIG_REST_PATH = Path(default_board_config_api.__file__)


@pytest_asyncio.fixture(autouse=True)
async def _af23_release_domain_event_referents():
    """Clean this file's guideline-impact rows from the SHARED test database.

    The adopt/unlink/retire flows exercised here persist rows in
    guideline_impact_adoptions / guideline_impact_unlinks /
    guideline_retirement_impacts whose event FKs are ondelete=RESTRICT into
    domain_events. Later tests (e.g. test_kg_relevance_dynamic) legitimately
    wipe domain_events for clean counting; leftover af23 referents turn that
    wipe into a FOREIGN KEY failure. Polluter pays: delete the rows this
    file's boards created, then their events.
    """

    yield
    from sqlalchemy import text

    session_factory = get_session_factory()
    engine = session_factory.kw["bind"]
    # guideline_board_bindings <-> guideline_impact_adoptions/unlinks
    # reference each other with RESTRICT in BOTH directions, so no delete
    # order satisfies the constraints. This wipes the whole af23-owned
    # subgraph, so relaxing FK enforcement is sound: nothing outside af23
    # boards references these rows. AUTOCOMMIT is required — the PRAGMA is
    # a no-op inside a transaction — and enforcement is restored on the
    # SAME connection before it returns to the pool.
    async with engine.connect() as connection:
        autocommit = await connection.execution_options(
            isolation_level="AUTOCOMMIT"
        )
        await autocommit.execute(text("PRAGMA foreign_keys=OFF"))
        try:
            for statement in (
                "DELETE FROM guideline_board_bindings "
                "WHERE board_id LIKE 'af23-%'",
                "DELETE FROM guideline_impact_adoptions "
                "WHERE board_id LIKE 'af23-%'",
                "DELETE FROM guideline_impact_unlinks "
                "WHERE board_id LIKE 'af23-%'",
                "DELETE FROM guideline_retirement_impacts "
                "WHERE board_id LIKE 'af23-%'",
                "DELETE FROM domain_events WHERE board_id LIKE 'af23-%'",
            ):
                await autocommit.execute(text(statement))
        finally:
            await autocommit.execute(text("PRAGMA foreign_keys=ON"))


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
    app.dependency_overrides[require_principal] = lambda: Principal(
        subject=USER,
        realm_id=LOCAL_REALM_ID,
        claims={"roles": ["admin"], "permissions": ["*"]},
    )
    app.dependency_overrides[get_realm_id] = lambda: LOCAL_REALM_ID
    return TestClient(app)


async def _seed_board(owner: str) -> str:
    from sqlalchemy_test_models import Board

    board_id = f"af23-board-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=board_id,
                name=board_id,
                owner_id=owner,
                realm_id=LOCAL_REALM_ID,
            )
        )
        await db.commit()
    return board_id


async def _seed_guideline(
    owner: str,
    *,
    scope: str = "global",
    board_id: str | None = None,
    tags: list[str] | None = None,
) -> str:
    from okto_pulse.core.services.main import GuidelineService

    async with get_session_factory()() as db:
        guideline = await GuidelineService(db).create_guideline(
            owner,
            GuidelineCreate(
                title="foreign guideline",
                content="must stay scoped",
                tags=["af23"] if tags is None else tags,
                scope=scope,
                board_id=board_id,
            ),
        )
        await db.commit()
    return guideline.id


async def _native_default_ref(guideline_id: str, *, priority: int = 0) -> dict:
    from okto_pulse.core.services.main import GuidelineService

    async with get_session_factory()() as db:
        guideline = await GuidelineService(db).get_guideline(
            guideline_id,
            owner_id=None,
        )
        assert guideline is not None
        return {
            "guideline_id": guideline.id,
            "priority": priority,
            "revision_id": guideline.revision_id,
            "revision_number": guideline.version,
            "semantic_version": guideline.semantic_version,
            "revision_digest": guideline.revision_digest,
        }


async def _link_guideline(board_id: str, guideline_id: str, priority: int = 1) -> None:
    from okto_pulse.core.domain.guideline_policy import GuidelineEnforcement
    from okto_pulse.core.services.main import GuidelineService

    async with get_session_factory()() as db:
        service = GuidelineService(db)
        seed_nonce = uuid.uuid4().hex
        receipt = await service.preview_guideline_revision_impact(
            board_id=board_id,
            guideline_id=guideline_id,
            proposed_priority=priority,
            proposed_enforcement=GuidelineEnforcement.ADVISORY,
            proposed_minimum_confidence=70,
            proposed_metric_threshold_overrides={},
            requested_by="af23-authoritative-seed",
            idempotency_key=f"af23-preview:{seed_nonce}",
        )
        linked, consumed_receipt = await service.adopt_guideline_revision(
            board_id=board_id,
            guideline_id=guideline_id,
            impact_receipt_id=receipt.impact_receipt_id,
            impact_digest=receipt.impact_digest,
            actor_id="af23-authoritative-seed",
            actor_type="system",
            idempotency_key=f"af23-adopt:{seed_nonce}",
        )
        assert linked is not None
        assert consumed_receipt.impact_receipt_id == receipt.impact_receipt_id
        await db.commit()


async def _share_board(board_id: str, *, permission: str) -> None:
    from sqlalchemy_test_models import BoardShare

    async with get_session_factory()() as db:
        db.add(
            BoardShare(
                board_id=board_id,
                user_id=USER,
                realm_id=LOCAL_REALM_ID,
                permission=permission,
                shared_by=OTHER,
            )
        )
        await db.commit()


@pytest.mark.asyncio
async def test_ts1_foreign_global_guideline_get_is_fail_closed(
    client: TestClient,
) -> None:
    guideline_id = await _seed_guideline(OTHER)

    response = client.get(f"{PREFIX}/guidelines/{guideline_id}")

    assert response.status_code == 404
    assert response.json()["detail"] == "Guideline not found"


@pytest.mark.asyncio
async def test_ts1_foreign_board_unlink_and_priority_are_fail_closed(
    client: TestClient,
) -> None:
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
async def test_shared_viewer_reads_guidelines_but_cannot_mutate(
    client: TestClient,
) -> None:
    from okto_pulse.core.ports.relational_application import (
        require_relational_application_adapter,
    )

    board_id = await _seed_board(OTHER)
    guideline_id = await _seed_guideline(OTHER)
    await _link_guideline(board_id, guideline_id, priority=1)
    await _share_board(board_id, permission="viewer")

    listing = client.get(f"{PREFIX}/boards/{board_id}/guidelines")
    mutation = client.patch(
        f"{PREFIX}/boards/{board_id}/guidelines/{guideline_id}",
        json={"priority": 9},
    )

    assert listing.status_code == 200, listing.text
    assert {item["id"] for item in listing.json()} == {guideline_id}
    assert mutation.status_code == 404
    assert mutation.json()["detail"] == "Board not found"
    async with get_session_factory()() as db:
        persisted = (
            await require_relational_application_adapter()
            .guideline_policy(db)
            .get_binding(board_id=board_id, guideline_id=guideline_id)
        )
        assert persisted is not None
        assert persisted.priority == 1


@pytest.mark.asyncio
async def test_shared_editor_reaches_b08_preview_gate_for_priority(
    client: TestClient,
) -> None:
    from okto_pulse.core.ports.relational_application import (
        require_relational_application_adapter,
    )

    board_id = await _seed_board(OTHER)
    guideline_id = await _seed_guideline(OTHER)
    await _link_guideline(board_id, guideline_id, priority=1)
    await _share_board(board_id, permission="editor")

    response = client.patch(
        f"{PREFIX}/boards/{board_id}/guidelines/{guideline_id}",
        json={"priority": 7},
    )
    assert response.status_code == 409
    assert (
        response.json()["detail"]["error_code"]
        == "guideline_impact_preview_required"
    )
    assert response.json()["detail"]["next_action"] == "preview_then_adopt"

    async with get_session_factory()() as db:
        persisted = (
            await require_relational_application_adapter()
            .guideline_policy(db)
            .get_binding(board_id=board_id, guideline_id=guideline_id)
        )
        assert persisted is not None
        assert persisted.priority == 1


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
    query_scope = QueryScope(actor_id=USER, source="test", realm_id=LOCAL_REALM_ID)

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


@pytest.mark.asyncio
async def test_guideline_tag_filter_matches_exact_member_in_multi_tag_arrays() -> None:
    from okto_pulse.core.services import GuidelineService

    tag = f"af23-alpha-{uuid.uuid4().hex}"
    single = await _seed_guideline(USER, tags=[tag])
    multi = await _seed_guideline(USER, tags=["before", tag, "after"])
    await _seed_guideline(USER, tags=[f"{tag}-suffix"])

    async with get_session_factory()() as db:
        guidelines = await GuidelineService(db).list_guidelines(USER, tag=tag)

    assert {guideline.id for guideline in guidelines} == {single, multi}


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
    from okto_pulse.core.domain.permissions import (
        PermissionSet,
        get_builtin_presets,
    )

    full_control = next(
        preset["flags"]
        for preset in get_builtin_presets()
        if preset["name"] == "Full Control"
    )
    ctx = mcp_server.AgentContext(
        agent_id=USER,
        agent_name="AF23 scoped actor",
        board_id=kwargs.get("board_id", ""),
        permissions=PermissionSet(full_control),
        realm_id=LOCAL_REALM_ID,
    )
    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=ctx)):
        return json.loads(await getattr(mcp_server, tool_name).fn(**kwargs))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("offset", "limit"),
    (
        (-1, 50),
        (0, 0),
        (0, 250.5),
        ("0", 50),
        (2**63, 50),
    ),
)
async def test_mcp_list_guidelines_rejects_invalid_page_windows(
    offset: int,
    limit: int,
) -> None:
    payload = await _call_mcp_tool(
        "okto_pulse_list_guidelines",
        board_id="board-guideline-window",
        offset=offset,
        limit=limit,
    )

    assert payload["error_code"] == "page_request_invalid_window"


def test_mcp_list_guidelines_schema_uses_integer_page_fields() -> None:
    from okto_pulse.core.mcp import server as mcp_server

    tool = mcp_server.mcp._tool_manager._tools["okto_pulse_list_guidelines"]

    assert tool.parameters["properties"]["offset"]["type"] == "integer"
    assert tool.parameters["properties"]["limit"]["type"] == "integer"


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

    combined = "\n\n".join(
        _async_function_source(MCP_SERVER_PATH, name) for name in tool_names
    )

    assert "board.owner_id" not in combined
    assert "db.get(Board" not in combined
    assert combined.count("MCPAdapterContract.actor") == 2
    assert combined.count("_authorize_legacy_guideline_mcp") == 6
    assert combined.count("get_unit_of_work_factory_for_mcp") == len(tool_names)


@pytest.mark.asyncio
async def test_ts4_default_candidates_are_scoped_and_global_only(
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
    visible_ref = await _native_default_ref(visible_guideline, priority=3)

    async with get_session_factory()() as db:
        await DefaultBoardConfigurationService(db).create_version(
            settings_payload=None,
            actor=USER,
            scope=template_scope,
            guideline_default_refs=[visible_ref],
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
    visible_ref = await _native_default_ref(visible_guideline)
    foreign_ref = await _native_default_ref(foreign_guideline)
    inline_ref = await _native_default_ref(inline_guideline)

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
        json={"guideline_default_refs": [visible_ref]},
    )

    assert ok.status_code == 200
    assert ok.json()["guideline_default_refs"][0]["guideline_id"] == visible_guideline

    invalid_cases = [
        (foreign_ref, "default_guideline_not_found"),
        (inline_ref, "default_guideline_not_global"),
        (
            {
                "guideline_id": "missing-guideline",
                "priority": 0,
                "revision_id": str(uuid.uuid4()),
                "revision_number": 1,
                "semantic_version": "1.0.0",
                "revision_digest": "0" * 64,
            },
            "default_guideline_not_found",
        ),
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

    combined = "\n\n".join(
        _async_function_source(MCP_SERVER_PATH, name) for name in tool_names
    )

    assert combined.count("MCPAdapterContract.actor") == len(tool_names)
    assert combined.count("ActorScope.from_context(actor).query_scope") == len(
        tool_names
    )
    assert combined.count("query_scope=query_scope") == len(tool_names)


@pytest.mark.asyncio
async def test_ts5_mcp_priority_update_without_board_grant_is_denied() -> None:
    from okto_pulse.core.ports.relational_application import (
        require_relational_application_adapter,
    )

    board_id = await _seed_board(OTHER)
    guideline_id = await _seed_guideline(OTHER)
    await _link_guideline(board_id, guideline_id, priority=1)

    from okto_pulse.core.mcp import server as mcp_server

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=None)):
        response = json.loads(
            await mcp_server.okto_pulse_update_board_guideline_priority.fn(
                board_id=board_id,
                guideline_id=guideline_id,
                priority="9",
            )
        )

    assert response == {"error": "Authentication failed or board access denied"}
    async with get_session_factory()() as db:
        persisted = (
            await require_relational_application_adapter()
            .guideline_policy(db)
            .get_binding(board_id=board_id, guideline_id=guideline_id)
        )
        assert persisted is not None
        assert persisted.priority == 1


@pytest.mark.asyncio
async def test_ts5_mcp_same_scope_reaches_b08_gate_then_unlinks() -> None:
    from okto_pulse.core.domain.guideline_policy import GuidelineBindingState
    from okto_pulse.core.ports.relational_application import (
        require_relational_application_adapter,
    )

    board_id = await _seed_board(USER)
    guideline_id = await _seed_guideline(USER)
    await _link_guideline(board_id, guideline_id, priority=1)

    response = await _call_mcp_tool(
        "okto_pulse_update_board_guideline_priority",
        board_id=board_id,
        guideline_id=guideline_id,
        priority="9",
    )
    assert response["error_code"] == "guideline_impact_preview_required"
    assert response["next_action"] == "preview_then_adopt"

    async with get_session_factory()() as db:
        active = (
            await require_relational_application_adapter()
            .guideline_policy(db)
            .get_binding(board_id=board_id, guideline_id=guideline_id)
        )
        assert active is not None
        assert active.priority == 1

    unlinked = await _call_mcp_tool(
        "okto_pulse_unlink_guideline_from_board",
        board_id=board_id,
        guideline_id=guideline_id,
    )
    assert unlinked == {"success": True}

    async with get_session_factory()() as db:
        policy = require_relational_application_adapter().guideline_policy(db)
        persisted = await policy.get_binding(
            board_id=board_id,
            guideline_id=guideline_id,
        )
        assert persisted is not None
        assert persisted.state is GuidelineBindingState.UNLINKED
        assert await policy.list_bindings(board_id=board_id) == ()


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
