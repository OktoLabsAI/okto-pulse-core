"""Cross-board containment for parent-filtered consolidated MCP lists."""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from mcp_runtime_testing import register_mcp_test_runtime
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.mcp_board_crud import (
    McpListByBoardCommand,
    McpListByBoardUseCase,
)
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    Board,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecStatus,
    Sprint,
    SprintLaneType,
    SprintStatus,
)


USER_ID = "list-scope-agent"


@pytest.fixture
async def list_scope_graph(db_factory):
    suffix = uuid.uuid4().hex[:8]
    ids = {
        name: f"list-scope-{name}-{suffix}"
        for name in (
            "board_a",
            "board_b",
            "ideation_a",
            "ideation_b",
            "refinement_a",
            "refinement_b",
            "spec_a",
            "spec_b",
            "sprint_a",
            "sprint_b",
        )
    }
    async with db_factory() as db:
        db.add_all(
            [
                Board(id=ids["board_a"], name="List scope A", owner_id=USER_ID),
                Board(id=ids["board_b"], name="List scope B", owner_id="owner-b"),
            ]
        )
        await db.flush()
        db.add_all(
            [
                Ideation(
                    id=ids["ideation_a"],
                    board_id=ids["board_a"],
                    title="Visible ideation A",
                    status=IdeationStatus.DRAFT,
                    created_by=USER_ID,
                ),
                Ideation(
                    id=ids["ideation_b"],
                    board_id=ids["board_b"],
                    title="SECRET ideation B",
                    status=IdeationStatus.DRAFT,
                    created_by="owner-b",
                ),
                Spec(
                    id=ids["spec_a"],
                    board_id=ids["board_a"],
                    title="Visible spec A",
                    status=SpecStatus.DRAFT,
                    created_by=USER_ID,
                ),
                Spec(
                    id=ids["spec_b"],
                    board_id=ids["board_b"],
                    title="SECRET spec B",
                    status=SpecStatus.DRAFT,
                    created_by="owner-b",
                ),
            ]
        )
        await db.flush()
        db.add_all(
            [
                Refinement(
                    id=ids["refinement_a"],
                    ideation_id=ids["ideation_a"],
                    board_id=ids["board_a"],
                    title="Visible refinement A",
                    status=RefinementStatus.DRAFT,
                    created_by=USER_ID,
                ),
                Refinement(
                    id=ids["refinement_b"],
                    ideation_id=ids["ideation_b"],
                    board_id=ids["board_b"],
                    title="SECRET refinement B",
                    status=RefinementStatus.DRAFT,
                    created_by="owner-b",
                ),
                Sprint(
                    id=ids["sprint_a"],
                    board_id=ids["board_a"],
                    spec_id=ids["spec_a"],
                    title="Visible sprint A",
                    status=SprintStatus.DRAFT,
                    lane_type=SprintLaneType.NORMAL,
                    created_by=USER_ID,
                ),
                Sprint(
                    id=ids["sprint_b"],
                    board_id=ids["board_b"],
                    spec_id=ids["spec_b"],
                    title="SECRET sprint B",
                    status=SprintStatus.DRAFT,
                    lane_type=SprintLaneType.NORMAL,
                    created_by="owner-b",
                ),
            ]
        )
        await db.commit()
    return ids


async def _call_list(db_factory, *, board_id: str, entity_type: str, filters: dict):
    ctx = SimpleNamespace(
        agent_id=USER_ID,
        agent_name=USER_ID,
        board_id=board_id,
        permissions=["*"],
    )
    register_mcp_test_runtime(db_factory)
    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=ctx)
    ), patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool("okto_pulse_list_by_board")
        return json.loads(
            await tool.fn(
                board_id=board_id,
                entity_type=entity_type,
                filters=filters,
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("entity_type", "filter_name", "foreign_parent", "local_parent", "local_child"),
    [
        (
            "refinement",
            "ideation_id",
            "ideation_b",
            "ideation_a",
            "refinement_a",
        ),
        ("sprint", "spec_id", "spec_b", "spec_a", "sprint_a"),
    ],
)
async def test_parent_filtered_lists_hide_foreign_and_missing_parents(
    db_factory,
    list_scope_graph,
    entity_type,
    filter_name,
    foreign_parent,
    local_parent,
    local_child,
):
    ids = list_scope_graph
    foreign = await _call_list(
        db_factory,
        board_id=ids["board_a"],
        entity_type=entity_type,
        filters={filter_name: ids[foreign_parent]},
    )
    missing = await _call_list(
        db_factory,
        board_id=ids["board_a"],
        entity_type=entity_type,
        filters={filter_name: f"missing-{uuid.uuid4().hex}"},
    )

    for payload in (foreign, missing):
        assert payload["total"] == 0
        assert payload["items"] == []
        assert "SECRET" not in json.dumps(payload)

    same_board = await _call_list(
        db_factory,
        board_id=ids["board_a"],
        entity_type=entity_type,
        filters={filter_name: ids[local_parent]},
    )
    assert [item["id"] for item in same_board["items"]] == [ids[local_child]]


@pytest.mark.asyncio
async def test_list_use_case_rejects_actor_command_board_spoof_before_parent_reads():
    ideations = SimpleNamespace(get_ideation=AsyncMock())
    refinements = SimpleNamespace(list_refinements=AsyncMock())
    specs = SimpleNamespace(get_spec=AsyncMock())
    sprints = SimpleNamespace(list_sprints=AsyncMock())
    uow = SimpleNamespace(
        services=SimpleNamespace(
            ideations=ideations,
            refinements=refinements,
            specs=specs,
            sprints=sprints,
        )
    )
    actor = ActorContext(USER_ID, "mcp", board_id="board-b")

    for entity_type, filters in (
        ("refinement", {"ideation_id": "ideation-a"}),
        ("sprint", {"spec_id": "spec-a"}),
    ):
        result = await McpListByBoardUseCase().execute(
            McpListByBoardCommand("board-a", entity_type, filters),
            actor=actor,
            uow=uow,
        )
        assert result.data.items == ()
        assert result.data.total_filtered == 0
        assert result.data.total_overall == 0
        assert result.data.offset == 0
        assert result.data.limit == 100

    ideations.get_ideation.assert_not_awaited()
    refinements.list_refinements.assert_not_awaited()
    specs.get_spec.assert_not_awaited()
    sprints.list_sprints.assert_not_awaited()
