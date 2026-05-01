"""MCP tests for Architecture Design tools and task context."""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import Board, Card, CardStatus, CardType, Spec, SpecStatus


USER_ID = "architecture-mcp-agent"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _stub_ctx(board_id: str):
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "architecture-mcp-agent",
            "board_id": board_id,
            "permissions": ["board:read", "cards:update", "specs:update"],
        },
    )()


def _stub_ctx_with_permissions(board_id: str, permissions):
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "architecture-mcp-agent",
            "board_id": board_id,
            "permissions": permissions,
        },
    )()


def _architecture_diagrams() -> list[dict]:
    return [
        {
            "id": "diagram-mcp",
            "title": "MCP diagram",
            "diagram_type": "context",
            "format": "excalidraw_json",
            "adapter_payload": {
                "type": "excalidraw",
                "version": 2,
                "elements": [{"id": "mcp-shape", "type": "rectangle"}],
                "appState": {},
                "files": {},
            },
        }
    ]


@pytest_asyncio.fixture
async def _seed_spec_card():
    from okto_pulse.core.infra.database import get_session_factory

    db_factory = get_session_factory()
    board_id = _id("architecture-mcp-board")
    spec_id = _id("architecture-mcp-spec")
    card_id = _id("architecture-mcp-card")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Architecture MCP Board", owner_id=USER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Architecture MCP Spec",
                status=SpecStatus.APPROVED,
                created_by=USER_ID,
                functional_requirements=["FR"],
                acceptance_criteria=["AC"],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Architecture MCP Card",
                status=CardStatus.STARTED,
                card_type=CardType.NORMAL,
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, spec_id, card_id


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    raw = await tool.fn(**kwargs)
    return json.loads(raw)


@pytest_asyncio.fixture(autouse=True)
async def _stub_auth(_seed_spec_card):
    board_id, _, _ = _seed_spec_card
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))), \
         patch.object(mcp_server, "check_permission", return_value=None):
        yield


@pytest.mark.asyncio
async def test_mcp_add_list_get_import_and_dump_architecture(_seed_spec_card):
    board_id, spec_id, _ = _seed_spec_card

    created = await _call(
        "okto_pulse_add_architecture_design",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Spec Architecture",
        global_description="Architecture exposed through MCP.",
        diagrams=json.dumps(_architecture_diagrams()),
    )
    assert created.get("success") is True, created
    design_id = created["architecture_design"]["id"]

    listed = await _call(
        "okto_pulse_list_architecture_designs",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
    )
    assert listed.get("success") is True
    assert listed["architecture_designs"][0]["diagrams_count"] == 1

    loaded = await _call(
        "okto_pulse_get_architecture_design",
        board_id=board_id,
        design_id=design_id,
        include_payloads="true",
    )
    assert loaded.get("success") is True
    assert loaded["architecture_design"]["diagrams"][0]["adapter_payload"]["elements"][0]["id"] == "mcp-shape"

    imported = await _call(
        "okto_pulse_import_excalidraw_architecture_diagram",
        board_id=board_id,
        design_id=design_id,
        title="Imported via MCP",
        payload_json=json.dumps(
            {
                "type": "excalidraw",
                "version": 2,
                "elements": [{"id": "imported-shape", "type": "text", "text": "MCP"}],
                "appState": {},
                "files": {},
            }
        ),
    )
    assert imported.get("success") is True
    assert len(imported["architecture_design"]["diagrams"]) == 2

    dumped = await _call(
        "okto_pulse_dump_architecture_diagram",
        board_id=board_id,
        design_id=design_id,
        diagram_id="diagram-mcp",
    )
    assert dumped.get("success") is True
    assert "mcp-shape" in dumped["dump"]


@pytest.mark.asyncio
async def test_mcp_copy_architecture_to_card_and_task_context(_seed_spec_card):
    board_id, spec_id, card_id = _seed_spec_card
    created = await _call(
        "okto_pulse_add_architecture_design",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Spec Architecture",
        global_description="Architecture copied into card context.",
        diagrams=json.dumps(_architecture_diagrams()),
    )
    source_id = created["architecture_design"]["id"]

    copied = await _call(
        "okto_pulse_copy_architecture_to_card",
        board_id=board_id,
        spec_id=spec_id,
        card_id=card_id,
    )
    assert copied.get("success") is True, copied
    assert copied["copied"] == 1
    assert copied["architecture_designs"][0]["source_design_id"] == source_id

    context = await _call(
        "okto_pulse_get_task_context",
        board_id=board_id,
        card_id=card_id,
        include_architecture="true",
    )
    assert "architecture_designs" in context["card"]
    assert context["card"]["architecture_designs"][0]["source_design_id"] == source_id
    assert context["spec"]["architecture_designs"][0]["id"] == source_id


@pytest.mark.asyncio
async def test_mcp_spec_lock_blocks_architecture_update(_seed_spec_card):
    from okto_pulse.core.infra.database import get_session_factory

    board_id, spec_id, _ = _seed_spec_card
    created = await _call(
        "okto_pulse_add_architecture_design",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Lockable Architecture",
        global_description="Architecture before validation.",
    )
    design_id = created["architecture_design"]["id"]

    db_factory = get_session_factory()
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        spec.validations = [{"id": "val-success", "outcome": "success"}]
        spec.current_validation_id = "val-success"
        await db.commit()

    updated = await _call(
        "okto_pulse_update_architecture_design",
        board_id=board_id,
        design_id=design_id,
        global_description="Should be blocked.",
    )
    assert "error" in updated
    assert "locked" in updated["error"]


@pytest.mark.asyncio
async def test_mcp_architecture_tools_enforce_granular_permissions(_seed_spec_card):
    from okto_pulse.core.infra.permissions import _build_preset_flags, resolve_permissions

    board_id, spec_id, card_id = _seed_spec_card
    created = await _call(
        "okto_pulse_add_architecture_design",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Permissioned Architecture",
        global_description="Architecture created with legacy-compatible permissions.",
    )
    assert created.get("success") is True, created

    read_only = resolve_permissions(
        None,
        _build_preset_flags(["board.read", "spec.architecture.read", "card.architecture.read"]),
        None,
    )
    with patch.object(
        mcp_server,
        "_get_agent_ctx",
        AsyncMock(return_value=_stub_ctx_with_permissions(board_id, read_only)),
    ):
        updated = await _call(
            "okto_pulse_update_architecture_design",
            board_id=board_id,
            design_id=created["architecture_design"]["id"],
            global_description="This should be denied.",
        )
        copied = await _call(
            "okto_pulse_copy_architecture_to_card",
            board_id=board_id,
            spec_id=spec_id,
            card_id=card_id,
        )

    assert "spec.architecture.edit" in updated["error"]
    assert "card.copy_from_spec.architecture" in copied["error"]
