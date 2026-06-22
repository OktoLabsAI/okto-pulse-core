"""Historical-invalid scenario_type reporting (spec ac16b3c9, IMP card bf52c32f).

okto_pulse_list_test_scenarios must surface persisted scenario_types that are
OUTSIDE the supported enum EXPLICITLY (FR5/AC5) — a stale value like
``regression``/``exploratory`` is reported in ``summary.unsupported_types`` instead
of being silently folded into a supported bucket or dropped. Supported counts
stay in ``summary.by_type``. Reads stay tolerant (the list never rejects).

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_scenario_type_reporting.py
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import Board, Spec, SpecStatus

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


async def _list(db_factory, board_id, spec_id):
    mcp_server.register_session_factory(db_factory)
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))), \
         patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool("okto_pulse_list_test_scenarios")
        return json.loads(await tool.fn(board_id=board_id, spec_id=spec_id))


async def test_list_summary_reports_unsupported_historical_types(db_factory):
    board_id, spec_id = await _seed(db_factory, [
        {"id": "ts_a", "title": "a", "scenario_type": "unit", "status": "draft"},
        {"id": "ts_b", "title": "b", "scenario_type": "integration", "status": "draft"},
        {"id": "ts_c", "title": "c", "scenario_type": "regression", "status": "draft"},
        {"id": "ts_d", "title": "d", "scenario_type": "exploratory", "status": "draft"},
    ])
    summary = (await _list(db_factory, board_id, spec_id))["summary"]
    # supported types counted normally; invalid NOT folded into a supported bucket.
    assert summary["by_type"] == {"unit": 1, "integration": 1}
    # historical/invalid persisted types surfaced explicitly (sorted keys).
    assert summary["unsupported_types"] == {"exploratory": 1, "regression": 1}


async def test_list_summary_no_unsupported_when_all_valid(db_factory):
    board_id, spec_id = await _seed(db_factory, [
        {"id": "ts_a", "title": "a", "scenario_type": "e2e", "status": "draft"},
        {"id": "ts_b", "title": "b", "scenario_type": "manual", "status": "draft"},
    ])
    summary = (await _list(db_factory, board_id, spec_id))["summary"]
    assert summary["unsupported_types"] == {}
    assert summary["by_type"] == {"e2e": 1, "manual": 1}


async def test_list_does_not_reject_on_invalid_historical_type(db_factory):
    # read-tolerant: a spec full of invalid types still lists without error.
    board_id, spec_id = await _seed(db_factory, [
        {"id": "ts_a", "title": "a", "scenario_type": "regression", "status": "draft"},
    ])
    listed = await _list(db_factory, board_id, spec_id)
    assert "error" not in listed
    assert listed["total_scenarios"] == 1
    assert listed["summary"]["unsupported_types"] == {"regression": 1}
