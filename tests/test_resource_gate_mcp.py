from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import Board, Ideation


USER_ID = "resource-gate-mcp-agent"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "resource-gate-agent",
            "permissions": [
                "ideation.entity.read",
                "ideation.entity.edit_fields",
                "spec.entity.read",
                "spec.entity.edit_fields",
                "card.entity.read",
                "card.entity.edit_fields",
            ],
        },
    )()


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    raw = await tool.fn(**kwargs)
    return json.loads(raw)


@pytest.fixture(autouse=True)
def _stub_auth():
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        yield


@pytest.fixture
async def _seed_ideation(db_factory):
    board_id = _id("board")
    ideation_id = _id("idea")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate MCP", owner_id=USER_ID))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="MCP Resource Gate ideation",
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, ideation_id


@pytest.mark.asyncio
async def test_resource_gate_mcp_summary_mark_and_clear_na(_seed_ideation):
    board_id, ideation_id = _seed_ideation

    summary = await _call(
        "okto_pulse_get_resource_gate_summary",
        board_id=board_id,
        entity_type="ideation",
        entity_id=ideation_id,
    )
    assert summary["success"] is True
    assert summary["blocking"] is True

    missing = {
        item["resource_type"]: item["state"]
        for item in summary["resources"]
    }
    assert missing["architecture"] == "missing"

    no_reason = await _call(
        "okto_pulse_mark_resource_not_applicable",
        board_id=board_id,
        entity_type="ideation",
        entity_id=ideation_id,
        resource_type="architecture",
    )
    assert no_reason["success"] is False
    assert no_reason["code"] == "justification_required"

    marked = await _call(
        "okto_pulse_mark_resource_not_applicable",
        board_id=board_id,
        entity_type="ideation",
        entity_id=ideation_id,
        resource_type="architecture",
        justification="Architecture does not apply to this text-only operational note.",
    )
    assert marked["success"] is True
    assert marked["warning"]
    by_type = {
        item["resource_type"]: item
        for item in marked["summary"]["resources"]
    }
    assert by_type["architecture"]["state"] == "not_applicable"
    assert by_type["architecture"]["na_mark"]["effective"] is True

    cleared = await _call(
        "okto_pulse_clear_resource_not_applicable",
        board_id=board_id,
        entity_type="ideation",
        entity_id=ideation_id,
        resource_type="architecture",
        reason="Architecture became applicable after clarification.",
    )
    assert cleared["success"] is True
    assert cleared["cleared"] == 1
    by_type = {
        item["resource_type"]: item
        for item in cleared["summary"]["resources"]
    }
    assert by_type["architecture"]["state"] == "missing"
