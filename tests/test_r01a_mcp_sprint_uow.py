"""Remaining shared Sprint use-case scope proof during F3.

Dedicated MCP removal is covered by test_sprint_mcp_retirement.py.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import func, select

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
    Sprint,
    SprintHistory,
    SprintQAItem,
    SprintStatus,
)

BOARD_ID = "r01a-mcpsprint"
OTHER_BOARD_ID = "r01a-mcpsprint-other"
USER_ID = "r01a-mcpsprint-agent"
OTHER_USER = "r01a-mcpsprint-someone-else"



# --- runtime harness --------------------------------------------------------


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {"agent_id": USER_ID, "agent_name": "mcp-sprint-test", "permissions": ["*"]},
    )()


@pytest.fixture(autouse=True)
def _auth():
    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())
    ), patch.object(mcp_server, "check_permission", return_value=None):
        yield


@pytest.fixture
async def _seed():
    """Board(s) + a spec + a sprint in review with two evaluations (one owned by the
    caller, one by someone else) seeded directly into the JSON column."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        for bid in (BOARD_ID, OTHER_BOARD_ID):
            if await db.get(Board, bid) is None:
                db.add(Board(id=bid, name="MCP Sprint", owner_id=USER_ID))
        await db.flush()
        spec = Spec(
            board_id=BOARD_ID, title="Spec", status=SpecStatus.IN_PROGRESS,
            created_by=USER_ID,
        )
        db.add(spec)
        await db.flush()
        sprint = Sprint(
            board_id=BOARD_ID, spec_id=spec.id, title="Sprint A",
            status=SprintStatus.REVIEW, created_by=USER_ID,
            evaluations=[
                {"id": "ev-own", "evaluator_id": USER_ID, "recommendation": "approve",
                 "overall_score": 8},
                {"id": "ev-other", "evaluator_id": OTHER_USER,
                 "recommendation": "reject", "overall_score": 3},
            ],
        )
        db.add(sprint)
        await db.flush()
        sid = sprint.id
        await db.commit()
    return sid


@pytest.fixture
async def _foreign_sprint(_seed):
    """A Sprint graph whose full lineage belongs to the other board."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        spec = Spec(
            board_id=OTHER_BOARD_ID,
            title="Foreign Spec",
            status=SpecStatus.IN_PROGRESS,
            created_by=USER_ID,
        )
        db.add(spec)
        await db.flush()
        sprint = Sprint(
            board_id=OTHER_BOARD_ID,
            spec_id=spec.id,
            title="Foreign Sprint",
            status=SprintStatus.REVIEW,
            created_by=USER_ID,
            evaluations=[
                {
                    "id": "foreign-eval",
                    "evaluator_id": USER_ID,
                    "recommendation": "approve",
                    "overall_score": 9,
                }
            ],
        )
        db.add(sprint)
        await db.flush()
        unassigned = Card(
            board_id=OTHER_BOARD_ID,
            spec_id=spec.id,
            title="Foreign unassigned card",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.NORMAL,
            created_by=USER_ID,
        )
        assigned = Card(
            board_id=OTHER_BOARD_ID,
            spec_id=spec.id,
            sprint_id=sprint.id,
            title="Foreign assigned card",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.NORMAL,
            created_by=USER_ID,
        )
        seeded_sprint = await db.get(Sprint, _seed)
        assert seeded_sprint is not None
        inconsistent = Card(
            board_id=OTHER_BOARD_ID,
            spec_id=seeded_sprint.spec_id,
            sprint_id=seeded_sprint.id,
            title="Cross-board card with matching spec and sprint FKs",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.NORMAL,
            created_by=USER_ID,
        )
        qa = SprintQAItem(
            sprint_id=sprint.id,
            question="Foreign sprint question",
            asked_by=OTHER_USER,
        )
        db.add_all([unassigned, assigned, inconsistent, qa])
        await db.flush()
        result = {
            "spec_id": spec.id,
            "sprint_id": sprint.id,
            "unassigned_card_id": unassigned.id,
            "assigned_card_id": assigned.id,
            "inconsistent_card_id": inconsistent.id,
            "qa_id": qa.id,
        }
        await db.commit()
    return result


async def _sprint_count() -> int:
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        return int(await db.scalar(select(func.count()).select_from(Sprint)) or 0)


async def _sprint_snapshot(sprint_id: str) -> tuple[str, int]:
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        sprint = await db.get(Sprint, sprint_id)
        assert sprint is not None
        return sprint.title, sprint.version


async def _foreign_graph_snapshot(graph: dict[str, str]) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        sprint = await db.get(Sprint, graph["sprint_id"])
        qa = await db.get(SprintQAItem, graph["qa_id"])
        unassigned = await db.get(Card, graph["unassigned_card_id"])
        assigned = await db.get(Card, graph["assigned_card_id"])
        inconsistent = await db.get(Card, graph["inconsistent_card_id"])
        history_count = int(
            await db.scalar(
                select(func.count())
                .select_from(SprintHistory)
                .where(SprintHistory.sprint_id == graph["sprint_id"])
            )
            or 0
        )
        activity_counts_list = []
        for board_id in (BOARD_ID, OTHER_BOARD_ID):
            activity_counts_list.append(
                int(
                    await db.scalar(
                        select(func.count())
                        .select_from(ActivityLog)
                        .where(ActivityLog.board_id == board_id)
                    )
                    or 0
                )
            )
        activity_counts = tuple(activity_counts_list)
        return {
            "sprint": None
            if sprint is None
            else (
                sprint.status,
                sprint.version,
                tuple(
                    (entry.get("id"), entry.get("evaluator_id"))
                    for entry in (sprint.evaluations or [])
                ),
            ),
            "qa": None
            if qa is None
            else (qa.sprint_id, qa.answer, qa.answered_by, qa.answered_at),
            "unassigned_card_sprint": unassigned.sprint_id if unassigned else None,
            "assigned_card_sprint": assigned.sprint_id if assigned else None,
            "inconsistent_card_sprint": (
                inconsistent.sprint_id if inconsistent else None
            ),
            "history_count": history_count,
            "activity_counts": activity_counts,
        }


async def _call(tool: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    t = await mcp_server.mcp.get_tool(tool)
    return json.loads(await t.fn(**kwargs))


@pytest.mark.asyncio
async def test_shared_sprint_use_cases_enforce_actor_board(_foreign_sprint):
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )
    from okto_pulse.core.application.use_cases.sprints_crud import (
        AssignSprintTasksCommand,
        AssignSprintTasksUseCase,
        CreateSprintCommand,
        CreateSprintUseCase,
        DeleteSprintCommand,
        DeleteSprintUseCase,
        GetSprintCommand,
        GetSprintUseCase,
        MoveSprintCommand,
        MoveSprintUseCase,
        SubmitSprintEvaluationCommand,
        SubmitSprintEvaluationUseCase,
        UnassignSprintTasksCommand,
        UnassignSprintTasksUseCase,
        UpdateSprintCommand,
        UpdateSprintUseCase,
    )
    from okto_pulse.core.domain.realm import LOCAL_REALM_ID
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.models.schemas import SprintCreate, SprintMove, SprintUpdate
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory

    actor = ActorContext(
        USER_ID,
        "mcp",
        board_id=BOARD_ID,
        realm_id=LOCAL_REALM_ID,
    )
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    sprint_id = _foreign_sprint["sprint_id"]
    before = await _sprint_snapshot(sprint_id)
    count_before = await _sprint_count()

    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await GetSprintUseCase().execute(
                GetSprintCommand(sprint_id), actor=actor, uow=uow
            )


    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await UpdateSprintUseCase().execute(
                UpdateSprintCommand(sprint_id, SprintUpdate(title="blocked")),
                actor=actor,
                uow=uow,
            )

    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await CreateSprintUseCase().execute(
                CreateSprintCommand(
                    OTHER_BOARD_ID,
                    SprintCreate(
                        spec_id=_foreign_sprint["spec_id"],
                        title="blocked",
                    ),
                ),
                actor=actor,
                uow=uow,
            )

    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await MoveSprintUseCase().execute(
                MoveSprintCommand(
                    sprint_id,
                    SprintMove(
                        status=SprintStatus.CANCELLED,
                        cancellation_reason="blocked",
                    ),
                ),
                actor=actor,
                uow=uow,
            )

    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await SubmitSprintEvaluationUseCase().execute(
                SubmitSprintEvaluationCommand(
                    sprint_id,
                    {"overall_score": 8, "recommendation": "approve"},
                ),
                actor=actor,
                uow=uow,
            )

    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await AssignSprintTasksUseCase().execute(
                AssignSprintTasksCommand(
                    sprint_id,
                    [_foreign_sprint["unassigned_card_id"]],
                ),
                actor=actor,
                uow=uow,
            )

    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await UnassignSprintTasksUseCase().execute(
                UnassignSprintTasksCommand(
                    sprint_id,
                    [_foreign_sprint["assigned_card_id"]],
                ),
                actor=actor,
                uow=uow,
            )

    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await DeleteSprintUseCase().execute(
                DeleteSprintCommand(sprint_id),
                actor=actor,
                uow=uow,
            )

    assert await _sprint_snapshot(sprint_id) == before
    assert await _sprint_count() == count_before
    graph_after = await _foreign_graph_snapshot(_foreign_sprint)
    assert graph_after["unassigned_card_sprint"] is None
    assert graph_after["assigned_card_sprint"] == sprint_id
