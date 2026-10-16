"""Current scenario listing refuses unsupported stored taxonomy."""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.services.test_scenario_lifecycle import InvalidScenarioTypeError
from sqlalchemy_test_models import Board, Spec, SpecStatus

pytestmark = pytest.mark.asyncio

USER_ID = "scenario-report-agent"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _stub_ctx(board_id: str):
    return type(
        "Ctx", (),
        {"agent_id": USER_ID, "agent_name": USER_ID, "board_id": board_id,
         "permissions": ["board:read", "specs:update"]},
    )()


async def _seed(db_factory, scenarios) -> tuple[str, str]:
    board_id, spec_id = _id("rep-board"), _id("rep-spec")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Report Board", owner_id=USER_ID, settings={}))
        db.add(Spec(id=spec_id, board_id=board_id, title="Report Spec",
                    status=SpecStatus.DRAFT, created_by=USER_ID, acceptance_criteria=[],
                    test_scenarios=scenarios, functional_requirements=[],
                    business_rules=[], api_contracts=[]))
        await db.commit()
    return board_id, spec_id


async def _list(db_factory, board_id, spec_id, *, scenario_type=None):
    register_mcp_test_runtime(db_factory)
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))), \
         patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool("okto_pulse_list_test_scenarios")
        arguments = {
            "board_id": board_id,
            "spec_id": spec_id,
        }
        if scenario_type is not None:
            arguments["scenario_type"] = scenario_type
        return json.loads(
            await tool.fn(**arguments)
        )


async def test_list_refuses_unsupported_stored_types(db_factory):
    board_id, spec_id = await _seed(db_factory, [
        {"id": "ts_a", "title": "a", "scenario_type": "unit", "status": "draft"},
        {"id": "ts_b", "title": "b", "scenario_type": "integration", "status": "draft"},
        {"id": "ts_c", "title": "c", "scenario_type": "regression", "status": "draft"},
        {"id": "ts_d", "title": "d", "scenario_type": "exploratory", "status": "draft"},
    ])
    with pytest.raises(InvalidScenarioTypeError):
        await _list(db_factory, board_id, spec_id)


async def test_list_summary_no_unsupported_when_all_valid(db_factory):
    board_id, spec_id = await _seed(db_factory, [
        {"id": "ts_a", "title": "a", "scenario_type": "e2e", "status": "draft"},
        {"id": "ts_b", "title": "b", "scenario_type": "manual", "status": "draft"},
    ])
    summary = (await _list(db_factory, board_id, spec_id))["summary"]
    assert "unsupported_types" not in summary
    assert summary["by_type"] == {"e2e": 1, "manual": 1}


async def test_list_omitted_filter_refuses_invalid_type(db_factory):
    # An omitted filter cannot hide invalid stored values.
    board_id, spec_id = await _seed(db_factory, [
        {"id": "ts_a", "title": "a", "scenario_type": "regression", "status": "draft"},
    ])
    with pytest.raises(InvalidScenarioTypeError):
        await _list(db_factory, board_id, spec_id)


async def test_list_exact_filter_enumerates_current_type(db_factory):
    board_id, spec_id = await _seed(db_factory, [
        {"id": "ts_a", "title": "a", "scenario_type": "negative", "status": "draft"},
        {"id": "ts_b", "title": "b", "scenario_type": "manual", "status": "draft"},
    ])
    listed = await _list(
        db_factory,
        board_id,
        spec_id,
        scenario_type="manual",
    )
    assert listed["filtered_count"] == 1
    assert [item["id"] for item in listed["scenarios"]] == ["ts_b"]
    assert listed["scenarios"][0]["scenario_type"] == "manual"
