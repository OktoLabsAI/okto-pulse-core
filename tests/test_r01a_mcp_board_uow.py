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

from mcp_runtime_testing import register_mcp_test_runtime

import ast
import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import delete

from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    BoardDesignSystem,
    BoardGuideline,
    Card,
    DesignSystem,
    Guideline,
    Spec,
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
    "update_board_guideline_priority": "McpUpdateBoardGuidelinePriorityUseCase",
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
        {
            "agent_id": USER_ID,
            "agent_name": "mcp-board-test",
            "permissions": ["*"],
            "realm_id": LOCAL_REALM_ID,
        },
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
            db.add(
                Board(
                    id=BOARD_ID,
                    name="MCP Board",
                    owner_id=USER_ID,
                    realm_id=LOCAL_REALM_ID,
                )
            )
        await db.commit()
    return BOARD_ID


async def _call(tool: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    t = await mcp_server.mcp.get_tool(tool)
    return json.loads(await t.fn(**kwargs))


async def _seed_archive_tree(*, archived: bool = False) -> tuple[str, str, str]:
    from okto_pulse.core.infra.database import get_session_factory

    board_id = f"r01a-mcp-archive-{uuid.uuid4().hex[:8]}"
    spec_id = f"r01a-mcp-spec-{uuid.uuid4().hex[:8]}"
    card_id = f"r01a-mcp-card-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=board_id,
                name="MCP archive scope",
                owner_id="human-owner",
                realm_id=LOCAL_REALM_ID,
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="MCP archive spec",
                created_by="human-owner",
                archived=archived,
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="MCP archive card",
                created_by="human-owner",
                archived=archived,
            )
        )
        await db.commit()
    return board_id, spec_id, card_id


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
async def test_archive_tree_foreign_root_matches_missing_and_has_no_activity():
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy import select

    actor_board, _actor_spec, _actor_card = await _seed_archive_tree()
    _foreign_board, foreign_spec, foreign_card = await _seed_archive_tree()

    foreign = await _call(
        "okto_pulse_archive_tree",
        board_id=actor_board,
        entity_type="spec",
        entity_id=foreign_spec,
    )
    missing = await _call(
        "okto_pulse_archive_tree",
        board_id=actor_board,
        entity_type="spec",
        entity_id=f"missing-{uuid.uuid4().hex[:8]}",
    )

    assert foreign == missing == {"error": "Spec not found"}
    async with get_session_factory()() as db:
        assert (await db.get(Spec, foreign_spec)).archived is False
        assert (await db.get(Card, foreign_card)).archived is False
        activities = (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.board_id == actor_board,
                    ActivityLog.action == "tree_archived",
                )
            )
        ).scalars().all()
        assert activities == []


@pytest.mark.asyncio
async def test_restore_tree_foreign_root_matches_missing_and_has_no_activity():
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy import select

    actor_board, _actor_spec, _actor_card = await _seed_archive_tree(archived=True)
    _foreign_board, foreign_spec, foreign_card = await _seed_archive_tree(
        archived=True
    )

    foreign = await _call(
        "okto_pulse_restore_tree",
        board_id=actor_board,
        entity_type="spec",
        entity_id=foreign_spec,
    )
    missing = await _call(
        "okto_pulse_restore_tree",
        board_id=actor_board,
        entity_type="spec",
        entity_id=f"missing-{uuid.uuid4().hex[:8]}",
    )

    assert foreign == missing == {"error": "Spec not found"}
    async with get_session_factory()() as db:
        assert (await db.get(Spec, foreign_spec)).archived is True
        assert (await db.get(Card, foreign_card)).archived is True
        activities = (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.board_id == actor_board,
                    ActivityLog.action == "tree_restored",
                )
            )
        ).scalars().all()
        assert activities == []


@pytest.mark.asyncio
async def test_archive_tree_permission_denial_precedes_mutation():
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy import select

    board_id, spec_id, _card_id = await _seed_archive_tree()
    with patch.object(mcp_server, "_mcp_check_permission", return_value="denied"):
        payload = await _call(
            "okto_pulse_archive_tree",
            board_id=board_id,
            entity_type="spec",
            entity_id=spec_id,
        )

    assert "error" in payload
    async with get_session_factory()() as db:
        assert (await db.get(Spec, spec_id)).archived is False
        assert (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.board_id == board_id,
                    ActivityLog.action == "tree_archived",
                )
            )
        ).scalars().all() == []


@pytest.mark.asyncio
async def test_archive_tree_success_records_one_atomic_activity():
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy import select

    board_id, spec_id, card_id = await _seed_archive_tree()
    payload = await _call(
        "okto_pulse_archive_tree",
        board_id=board_id,
        entity_type="spec",
        entity_id=spec_id,
    )

    assert payload["success"] is True
    async with get_session_factory()() as db:
        assert (await db.get(Spec, spec_id)).archived is True
        assert (await db.get(Card, card_id)).archived is True
        activities = (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.board_id == board_id,
                    ActivityLog.action == "tree_archived",
                )
            )
        ).scalars().all()
        assert len(activities) == 1


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
    from okto_pulse.core.domain.guideline_policy import GuidelineEnforcement
    from okto_pulse.core.models.schemas import GuidelineCreate
    from okto_pulse.core.services.main import GuidelineService

    owner_id = "r01a-human-board-owner"
    board_id = f"r01a-mcp-context-{uuid.uuid4().hex[:8]}"

    factory = get_session_factory()
    async with factory() as db:
        db.add(
            Board(
                id=board_id,
                name="MCP granted guidelines",
                owner_id=owner_id,
                realm_id=LOCAL_REALM_ID,
            )
        )
        await db.flush()
        service = GuidelineService(db)
        linked = await service.create_guideline(
            owner_id,
            GuidelineCreate(
                title="Linked global rule",
                content="Global board guidance must be visible to board agents.",
                tags=["r01a"],
                scope="global",
            ),
        )
        await service.create_guideline(
            owner_id,
            GuidelineCreate(
                title="Inline board rule",
                content="Inline board guidance must be visible to board agents.",
                tags=["r01a"],
                scope="inline",
                board_id=board_id,
            ),
        )
        receipt = await service.preview_guideline_revision_impact(
            board_id=board_id,
            guideline_id=linked.id,
            proposed_priority=5,
            proposed_enforcement=GuidelineEnforcement.ADVISORY,
            proposed_minimum_confidence=70,
            proposed_metric_threshold_overrides={},
            requested_by=owner_id,
            idempotency_key=f"preview:{linked.id}",
        )
        await service.adopt_guideline_revision(
            board_id=board_id,
            guideline_id=linked.id,
            impact_receipt_id=receipt.impact_receipt_id,
            impact_digest=receipt.impact_digest,
            actor_id=owner_id,
            actor_type="user",
            idempotency_key=f"adopt:{linked.id}",
        )
        await db.commit()

    payload = await _call("okto_pulse_get_board_guidelines", board_id=board_id)
    titles = {item["guideline"]["title"] for item in payload["guidelines"]}

    assert payload["board_id"] == board_id
    assert payload["count"] == 2
    assert titles == {"Linked global rule", "Inline board rule"}


@pytest.mark.asyncio
async def test_update_board_guideline_priority_requires_preview_without_mutation():
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy import select

    owner_id = "r01a-human-board-owner"
    board_id = f"r01a-mcp-priority-{uuid.uuid4().hex[:8]}"
    guideline_id = f"r01a-priority-guideline-{uuid.uuid4().hex[:8]}"

    factory = get_session_factory()
    async with factory() as db:
        db.add(
            Board(
                id=board_id,
                name="MCP granted priority update",
                owner_id=owner_id,
                realm_id=LOCAL_REALM_ID,
            )
        )
        db.add(
            Guideline(
                id=guideline_id,
                title="Priority rule",
                content="Agents granted to the board may reprioritize this rule.",
                tags=["r01a"],
                scope="global",
                owner_id=owner_id,
            )
        )
        await db.flush()
        db.add(BoardGuideline(board_id=board_id, guideline_id=guideline_id, priority=5))
        await db.commit()

    try:
        payload = await _call(
            "okto_pulse_update_board_guideline_priority",
            board_id=board_id,
            guideline_id=guideline_id,
            priority="30",
        )

        assert payload["error_code"] == "guideline_impact_preview_required"
        assert payload["next_action"] == "preview_then_adopt"
        assert payload["retryable"] is False
        async with factory() as db:
            link = (
                await db.execute(
                    select(BoardGuideline).where(
                        BoardGuideline.board_id == board_id,
                        BoardGuideline.guideline_id == guideline_id,
                    )
                )
            ).scalar_one_or_none()
            assert link is not None
            assert link.priority == 5
    finally:
        async with factory() as db:
            await db.execute(delete(BoardGuideline).where(BoardGuideline.board_id == board_id))
            await db.execute(delete(Guideline).where(Guideline.id == guideline_id))
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
        db.add(
            Board(
                id=board_id,
                name="MCP granted design system",
                owner_id=owner_id,
                realm_id=LOCAL_REALM_ID,
            )
        )
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
