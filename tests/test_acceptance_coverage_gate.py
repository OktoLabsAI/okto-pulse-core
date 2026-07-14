from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, Spec, SpecStatus
from okto_pulse.core.models.schemas import SpecMove
from okto_pulse.core.services.main import SpecService


pytestmark = pytest.mark.asyncio

USER_ID = "coverage-gate-agent"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _stub_ctx(board_id: str):
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "coverage-gate-agent",
            "board_id": board_id,
            "permissions": ["board:read", "specs:update"],
        },
    )()


async def _seed_spec_with_indexed_criteria(db_factory) -> tuple[str, str]:
    board_id = _id("coverage-board")
    spec_id = _id("coverage-spec")
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Coverage Gate Board",
                owner_id=USER_ID,
                settings={"skip_test_coverage_global": False},
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Coverage Gate Spec",
                status=SpecStatus.IN_PROGRESS,
                created_by=USER_ID,
                skip_test_coverage=False,
                skip_qualitative_validation=True,
                functional_requirements=["FR1", "FR2"],
                acceptance_criteria=[
                    "AC1: first behavior is covered",
                    "AC2: second behavior is covered",
                ],
                test_scenarios=[
                    {
                        "id": "ts_first",
                        "title": "First behavior",
                        "given": "The first precondition",
                        "when": "The first action runs",
                        "then": "The first result is observed",
                        "scenario_type": "integration",
                        "linked_criteria": ["0"],
                        "status": "passed",
                        "linked_task_ids": [],
                    },
                    {
                        "id": "ts_second",
                        "title": "Second behavior",
                        "given": "The second precondition",
                        "when": "The second action runs",
                        "then": "The second result is observed",
                        "scenario_type": "integration",
                        "linked_criteria": [1],
                        "status": "passed",
                        "linked_task_ids": [],
                    },
                ],
                business_rules=[],
                api_contracts=[],
            )
        )
        await db.commit()
    return board_id, spec_id


async def test_move_spec_done_accepts_linked_criteria_by_index(db_factory):
    board_id, spec_id = await _seed_spec_with_indexed_criteria(db_factory)

    async with db_factory() as db:
        moved = await SpecService(db).move_spec(
            spec_id,
            USER_ID,
            SpecMove(status=SpecStatus.DONE),
        )
        await db.commit()

    assert board_id
    assert moved is not None
    assert moved.status == SpecStatus.DONE


async def test_list_test_scenarios_coverage_accepts_linked_criteria_by_index(db_factory):
    board_id, spec_id = await _seed_spec_with_indexed_criteria(db_factory)
    register_mcp_test_runtime(db_factory)

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))), \
         patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool("okto_pulse_list_test_scenarios")
        payload = json.loads(await tool.fn(board_id=board_id, spec_id=spec_id))

    assert payload["coverage"]["covered"] == 2
    assert payload["coverage"]["uncovered_indices"] == []
    assert payload["coverage"]["details"]["0"] == ["ts_first"]
    assert payload["coverage"]["details"]["1"] == ["ts_second"]
