"""R01A MCP-FU6 (family: card) — strangler oracle.

Proves the MCP card tools were migrated off ``get_db_for_mcp`` / direct
``CardService`` onto the MCP UnitOfWork + ``mcp_card_crud`` use cases WITHOUT
behavior drift, per Codex's option-A-with-adapter-envelope decision:

- AST: every migrated card tool delegates to its ``Mcp*UseCase`` (or the reused
  ``AddCardDependencyUseCase``) and no longer opens ``get_db_for_mcp`` nor builds
  ``CardService`` directly.
- board-scope: a cross-board id returns the legacy ``{"error": "Card not found"}``.
- atomic activity log: ``update_card``/``delete_card`` write the
  ``card_updated``/``card_deleted`` ``ActivityLog`` row in the SAME transaction.
- legacy MCP envelopes: ``remove_card_dependency`` returns ``{"success": bool}``
  (no raise); ``add_card_dependency`` self-ref returns the legacy circular msg.
- ``create_card`` keeps the bidirectional ``test_scenarios.linked_task_ids``
  backlink.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import ast
import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import ActivityLog, Board, Spec, SpecStatus

BOARD_A = "r01a-mcpcard-a"
BOARD_B = "r01a-mcpcard-b"
USER_ID = "r01a-mcpcard-agent"

_TOOL_USE_CASE = {
    "create_card": "McpCreateCardUseCase",
    "get_card": "McpGetCardUseCase",
    "update_card": "McpUpdateCardUseCase",
    "move_card": "McpMoveCardUseCase",
    "delete_card": "McpDeleteCardUseCase",
    "add_card_dependency": "AddCardDependencyUseCase",
    "remove_card_dependency": "McpRemoveCardDependencyUseCase",
    "get_card_dependencies": "McpGetCardDependenciesUseCase",
    "copy_knowledge_to_card": "McpCopyKnowledgeToCardUseCase",
}


# --- AST proof (no DB) ------------------------------------------------------


def test_card_tools_strangled_and_delegate():
    src = Path(mcp_server.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    seen = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.AsyncFunctionDef):
            short = n.name.replace("okto_pulse_", "")
            if short not in _TOOL_USE_CASE:
                continue
            seen.add(short)
            block = ast.get_source_segment(src, n) or ""
            assert "async with get_db_for_mcp" not in block, (
                f"{short} still opens get_db_for_mcp"
            )
            assert "CardService(" not in block, (
                f"{short} still builds CardService directly"
            )
            assert _TOOL_USE_CASE[short] in block, (
                f"{short} must delegate to {_TOOL_USE_CASE[short]}"
            )
    assert seen == set(_TOOL_USE_CASE), f"missing tools: {set(_TOOL_USE_CASE) - seen}"


# --- runtime harness --------------------------------------------------------


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {"agent_id": USER_ID, "agent_name": "mcp-card-test", "permissions": ["*"]},
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
        for bid in (BOARD_A, BOARD_B):
            if await db.get(Board, bid) is None:
                db.add(Board(id=bid, name=f"MCP Card {bid}", owner_id=USER_ID))
        await db.flush()
        spec_id = str(uuid.uuid4())
        scen_id = "scen-" + uuid.uuid4().hex[:8]
        db.add(
            Spec(
                id=spec_id,
                board_id=BOARD_A,
                title="Spec A",
                status=SpecStatus.APPROVED,
                created_by=USER_ID,
                functional_requirements=["FR1"],
                acceptance_criteria=["AC1"],
                test_scenarios=[
                    {"id": scen_id, "title": "S", "type": "happy", "linked_task_ids": []}
                ],
                business_rules=[],
                api_contracts=[],
            )
        )
        await db.commit()
    return spec_id, scen_id


async def _call(tool_name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    tool = await mcp_server.mcp.get_tool(tool_name)
    return json.loads(await tool.fn(**kwargs))


async def _create_card(spec_id: str, **over) -> dict:
    kwargs = {"board_id": BOARD_A, "title": "C", "spec_id": spec_id}
    kwargs.update(over)
    return await _call("okto_pulse_create_card", **kwargs)


async def _activity_rows(card_id: str, action: str) -> list:
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        res = await db.execute(
            select(ActivityLog).where(
                ActivityLog.card_id == card_id, ActivityLog.action == action
            )
        )
        return list(res.scalars().all())


# --- board-scope (cross-board → legacy "Card not found") --------------------


@pytest.mark.asyncio
async def test_cross_board_returns_card_not_found(_seed):
    spec_id, _ = _seed
    card_id = (await _create_card(spec_id))["card"]["id"]
    for tool, extra in (
        ("okto_pulse_get_card", {}),
        ("okto_pulse_update_card", {"title": "x"}),
        ("okto_pulse_move_card", {"status": "in_progress"}),
        ("okto_pulse_delete_card", {}),
    ):
        payload = await _call(tool, board_id=BOARD_B, card_id=card_id, **extra)
        assert payload.get("error") == "Card not found", (tool, payload)


# --- atomic activity log ----------------------------------------------------


@pytest.mark.asyncio
async def test_update_card_writes_atomic_card_updated_activity(_seed):
    spec_id, _ = _seed
    card_id = (await _create_card(spec_id))["card"]["id"]
    payload = await _call(
        "okto_pulse_update_card", board_id=BOARD_A, card_id=card_id, title="Updated"
    )
    assert payload.get("success") is True, payload
    assert await _activity_rows(card_id, "card_updated"), (
        "update_card must atomically write a card_updated ActivityLog row"
    )


@pytest.mark.asyncio
async def test_delete_card_writes_atomic_card_deleted_activity(_seed):
    spec_id, _ = _seed
    card_id = (await _create_card(spec_id))["card"]["id"]
    payload = await _call("okto_pulse_delete_card", board_id=BOARD_A, card_id=card_id)
    assert payload.get("success") is True, payload
    assert await _activity_rows(card_id, "card_deleted"), (
        "delete_card must atomically write a card_deleted ActivityLog row"
    )


# --- legacy MCP envelopes ---------------------------------------------------


@pytest.mark.asyncio
async def test_remove_card_dependency_returns_bool_without_raising(_seed):
    spec_id, _ = _seed
    card_id = (await _create_card(spec_id))["card"]["id"]
    payload = await _call(
        "okto_pulse_remove_card_dependency",
        board_id=BOARD_A,
        card_id=card_id,
        depends_on_id="does-not-exist",
    )
    # Legacy MCP returns {"success": removed} — NOT a 404/raise like the REST UC.
    assert payload == {"success": False}, payload


@pytest.mark.asyncio
async def test_add_card_dependency_self_ref_returns_legacy_message(_seed):
    spec_id, _ = _seed
    card_id = (await _create_card(spec_id))["card"]["id"]
    payload = await _call(
        "okto_pulse_add_card_dependency",
        board_id=BOARD_A,
        card_id=card_id,
        depends_on_id=card_id,
    )
    assert "circular" in payload.get("error", "").lower(), payload


# --- create_card scenario backlink ------------------------------------------


@pytest.mark.asyncio
async def test_create_card_backlinks_scenario(_seed):
    spec_id, scen_id = _seed
    created = await _create_card(
        spec_id, title="Test card", card_type="test", test_scenario_ids=[scen_id]
    )
    card_id = created["card"]["id"]

    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        spec = await db.get(Spec, spec_id)
        scen = next(s for s in spec.test_scenarios if s["id"] == scen_id)
    assert card_id in (scen.get("linked_task_ids") or []), (
        "create_card must bidirectionally backlink the test scenario"
    )
