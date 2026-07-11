"""R01A MCP-FU6 (family: board) — strangler oracle.

Proves the board MCP tools were migrated off ``get_db_for_mcp`` / direct service
construction onto the MCP UnitOfWork + ``mcp_board_crud`` use cases without
behavior drift (Codex option-A-with-adapter-envelope), and makes the family
BOUNDARY explicit: the guideline/design-system DEFAULT/CRUD tools stay OUT.

Covered: AST delegation + no raw session for all 15 board tools; the family
boundary for default guideline/design-system tools; the get_board /
list_board_members aggregations + "Board not found";
the list_by_board dispatcher (per-type + required-filter + unsupported envelopes);
the default-board-config read.
"""

from __future__ import annotations

import ast
import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import delete

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    Board,
    BoardDesignSystem,
    BoardGuideline,
    DesignSystem,
    Guideline,
)

BOARD_ID = "r01a-mcpboard"
USER_ID = "r01a-mcpboard-agent"

_TOOL_USE_CASE = {
    "get_board": "McpGetBoardUseCase",
    "list_board_members": "McpListBoardMembersUseCase",
    "get_active_default_board_config": "McpGetActiveDefaultBoardConfigUseCase",
    "list_default_board_config_versions": "McpListDefaultBoardConfigVersionsUseCase",
    "get_board_default_config_diff": "McpGetBoardDefaultConfigDiffUseCase",
    "create_default_board_config_version": "McpCreateDefaultBoardConfigVersionUseCase",
    "activate_default_board_config_version": "McpActivateDefaultBoardConfigVersionUseCase",
    "deactivate_default_board_config_version": "McpDeactivateDefaultBoardConfigVersionUseCase",
    "get_board_guidelines": "McpGetBoardGuidelinesUseCase",
    "link_guideline_to_board": "McpLinkGuidelineToBoardUseCase",
    "unlink_guideline_from_board": "McpUnlinkGuidelineFromBoardUseCase",
    "link_board_design_system": "McpLinkBoardDesignSystemUseCase",
    "unlink_board_design_system": "McpUnlinkBoardDesignSystemUseCase",
    "get_board_design_system": "McpGetBoardDesignSystemUseCase",
    "list_by_board": "McpListByBoardUseCase",
}

# Codex boundary: guideline/design-system DEFAULT/CRUD stay OUT of MCP-FU6.
# AF35-S4 later migrated these tools through its residual admin family, so this
# oracle now asserts ownership boundary rather than raw-session status.
_AF35_S4_RESIDUAL_ADMIN_TOOLS = [
    "list_default_guideline_candidates",
    "update_default_guideline_refs",
    "set_default_design_system",
]


# --- AST proofs (no DB) -----------------------------------------------------


def test_board_tools_strangled_and_delegate():
    src = Path(mcp_server.__file__).read_text(encoding="utf-8")
    seen = set()
    for n in ast.walk(ast.parse(src)):
        if isinstance(n, ast.AsyncFunctionDef):
            short = n.name.replace("okto_pulse_", "")
            if short not in _TOOL_USE_CASE:
                continue
            seen.add(short)
            block = ast.get_source_segment(src, n) or ""
            assert "async with get_db_for_mcp" not in block, (
                f"{short} still opens get_db_for_mcp"
            )
            assert _TOOL_USE_CASE[short] in block, (
                f"{short} must delegate to {_TOOL_USE_CASE[short]}"
            )
    assert seen == set(_TOOL_USE_CASE), f"missing: {set(_TOOL_USE_CASE) - seen}"


def test_boundary_guideline_designsystem_defaults_stay_out_of_board_family():
    """The guideline/design-system DEFAULT tools remain outside the board family.
    AF35-S4 owns their UoW migration, so they should not delegate to board-family
    use cases while also avoiding raw MCP DB sessions."""
    src = Path(mcp_server.__file__).read_text(encoding="utf-8")
    found = set()
    for n in ast.walk(ast.parse(src)):
        if isinstance(n, ast.AsyncFunctionDef):
            short = n.name.replace("okto_pulse_", "")
            if short in _AF35_S4_RESIDUAL_ADMIN_TOOLS:
                found.add(short)
                block = ast.get_source_segment(src, n) or ""
                assert "McpGetBoard" not in block and "McpListByBoardUseCase" not in block
                assert "async with get_db_for_mcp" not in block, (
                    f"{short} must stay off raw MCP DB sessions after AF35-S4"
                )
                assert "get_unit_of_work_factory_for_mcp" in block
    assert found == set(_AF35_S4_RESIDUAL_ADMIN_TOOLS), (
        f"missing OUT tools: {set(_AF35_S4_RESIDUAL_ADMIN_TOOLS) - found}"
    )


# --- runtime harness --------------------------------------------------------


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {"agent_id": USER_ID, "agent_name": "mcp-board-test", "permissions": ["*"]},
    )()


@pytest.fixture(autouse=True)
def _auth():
    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())
    ), patch.object(mcp_server, "check_permission", return_value=None):
        yield


@pytest.fixture
async def _seed():
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        if await db.get(Board, BOARD_ID) is None:
            db.add(Board(id=BOARD_ID, name="MCP Board", owner_id=USER_ID))
        await db.commit()
    return BOARD_ID


async def _call(tool: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    t = await mcp_server.mcp.get_tool(tool)
    return json.loads(await t.fn(**kwargs))


# --- aggregations -----------------------------------------------------------


@pytest.mark.asyncio
async def test_get_board_returns_overview_payload(_seed):
    payload = await _call("okto_pulse_get_board", board_id=BOARD_ID)
    assert payload["id"] == BOARD_ID
    assert "counts" in payload
    assert "design_system" in payload  # FR1-FR4 effective DS surface preserved


@pytest.mark.asyncio
async def test_get_board_missing_returns_board_not_found():
    payload = await _call("okto_pulse_get_board", board_id="does-not-exist-board")
    assert payload == {"error": "Board not found"}


@pytest.mark.asyncio
async def test_list_board_members(_seed):
    payload = await _call("okto_pulse_list_board_members", board_id=BOARD_ID)
    assert payload["owner"]["id"] == USER_ID
    assert "agents" in payload


# --- list_by_board dispatcher ----------------------------------------------


@pytest.mark.asyncio
async def test_list_by_board_spec_dispatch(_seed):
    payload = await _call(
        "okto_pulse_list_by_board", board_id=BOARD_ID, entity_type="spec"
    )
    assert payload["entity_type"] == "spec"
    assert "items" in payload and "total" in payload


@pytest.mark.asyncio
async def test_list_by_board_refinement_requires_ideation_id(_seed):
    payload = await _call(
        "okto_pulse_list_by_board", board_id=BOARD_ID, entity_type="refinement"
    )
    assert "ideation_id" in json.dumps(payload), payload


@pytest.mark.asyncio
async def test_list_by_board_sprint_requires_spec_id(_seed):
    payload = await _call(
        "okto_pulse_list_by_board", board_id=BOARD_ID, entity_type="sprint"
    )
    assert "spec_id" in json.dumps(payload), payload


@pytest.mark.asyncio
async def test_list_by_board_unsupported_entity(_seed):
    payload = await _call(
        "okto_pulse_list_by_board", board_id=BOARD_ID, entity_type="banana"
    )
    assert "banana" in json.dumps(payload) or "unsupported" in json.dumps(payload).lower()


# --- default board config (read + error envelope path) ----------------------


@pytest.mark.asyncio
async def test_get_active_default_board_config(_seed):
    payload = await _call(
        "okto_pulse_get_active_default_board_config", board_id=BOARD_ID
    )
    # Either the scoped active-config payload, or the legacy DefaultBoardConfigurationError
    # envelope (e.to_dict()) — both are dicts the adapter renders, never an exception.
    assert isinstance(payload, dict)


@pytest.mark.asyncio
async def test_get_board_design_system_read(_seed):
    payload = await _call("okto_pulse_get_board_design_system", board_id=BOARD_ID)
    assert payload.get("board_id") == BOARD_ID
    assert "effective" in payload


@pytest.mark.asyncio
async def test_get_board_guidelines_honors_mcp_board_grant_for_non_owner_board():
    from okto_pulse.core.infra.database import get_session_factory

    owner_id = "r01a-human-board-owner"
    board_id = f"r01a-mcp-guidelines-{uuid.uuid4().hex[:8]}"
    linked_id = f"r01a-linked-guideline-{uuid.uuid4().hex[:8]}"
    inline_id = f"r01a-inline-guideline-{uuid.uuid4().hex[:8]}"

    factory = get_session_factory()
    async with factory() as db:
        db.add(Board(id=board_id, name="MCP granted guidelines", owner_id=owner_id))
        db.add_all(
            [
                Guideline(
                    id=linked_id,
                    title="Linked global rule",
                    content="Global board guidance must be visible to board agents.",
                    tags=["r01a"],
                    scope="global",
                    owner_id=owner_id,
                ),
                Guideline(
                    id=inline_id,
                    title="Inline board rule",
                    content="Inline board guidance must be visible to board agents.",
                    tags=["r01a"],
                    scope="inline",
                    board_id=board_id,
                    owner_id=owner_id,
                ),
            ]
        )
        await db.flush()
        db.add(BoardGuideline(board_id=board_id, guideline_id=linked_id, priority=5))
        await db.commit()

    try:
        payload = await _call("okto_pulse_get_board_guidelines", board_id=board_id)
        titles = {item["guideline"]["title"] for item in payload["guidelines"]}

        assert payload["board_id"] == board_id
        assert payload["count"] == 2
        assert titles == {"Linked global rule", "Inline board rule"}
    finally:
        async with factory() as db:
            await db.execute(delete(BoardGuideline).where(BoardGuideline.board_id == board_id))
            await db.execute(delete(Guideline).where(Guideline.id.in_([linked_id, inline_id])))
            await db.execute(delete(Board).where(Board.id == board_id))
            await db.commit()


@pytest.mark.asyncio
async def test_get_board_design_system_honors_mcp_board_grant_for_non_owner_board():
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.design_system import DesignSystemService

    owner_id = "r01a-human-board-owner"
    board_id = f"r01a-mcp-ds-{uuid.uuid4().hex[:8]}"
    ds_id: str | None = None

    factory = get_session_factory()
    async with factory() as db:
        db.add(Board(id=board_id, name="MCP granted design system", owner_id=owner_id))
        await db.flush()
        service = DesignSystemService(db)
        ds = await service.create_design_system(
            owner_id,
            title="Granted MCP Design System",
            scope="global",
        )
        ds_id = ds.id
        await service.link_design_system_to_board(board_id, ds.id)
        await db.commit()

    try:
        payload = await _call("okto_pulse_get_board_design_system", board_id=board_id)

        assert payload["board_id"] == board_id
        assert payload["effective"]["source"] == "board_link"
        assert payload["effective"]["design_system_id"] == ds_id
        assert payload["effective"]["title"] == "Granted MCP Design System"
    finally:
        async with factory() as db:
            await db.execute(delete(BoardDesignSystem).where(BoardDesignSystem.board_id == board_id))
            if ds_id is not None:
                await db.execute(delete(DesignSystem).where(DesignSystem.id == ds_id))
            await db.execute(delete(Board).where(Board.id == board_id))
            await db.commit()
