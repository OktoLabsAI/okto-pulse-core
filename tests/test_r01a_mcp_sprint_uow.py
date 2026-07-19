"""R01A MCP-FU6 (family: sprint) — strangler oracle.

Proves the 13 sprint MCP tools were migrated off ``get_db_for_mcp`` / direct service
construction onto the MCP UnitOfWork + the transport-free ``mcp_sprint_crud`` use cases
(REUSE of the REST sprint use cases where the flow matches; MCP VARIANTs otherwise),
WITHOUT behavior drift — under the TIGHTER Clean Core rule Codex set for this block:
the MCP wrappers must be THIN (no composed ``uow.session`` query, no direct service);
ALL aggregation lives in the application layer.

Consolidated proofs:
- AST: all 13 migrated sprint tools strangled (no ``get_db_for_mcp``).
- AST thin: no wrapper has a ``uow.session`` composed query, a direct ``SprintService``/
  ``SprintQAService`` construction, or a raw ``db.get(Sprint`` read.
- AST purity: ``mcp_sprint_crud`` imports neither ``okto_pulse.core.mcp`` nor a server
  helper.
- AST option-A: the ``flag_modified`` ORM mutation lives in the NEW
  ``SprintService.delete_evaluation`` (service), NOT in the use case nor the adapter.
- AST: the delete-Q&A activity-log is ATOMIC in the use case (1 ``_log_activity`` in
  the module — answer_sprint_question intentionally has none).
- Runtime: get round-trip; get_context board-scope (cross-board not found); the
  evaluations cluster incl. the delete ownership gate; suggest; the Q&A self-answer +
  delete.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import ast
import json
from pathlib import Path
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

_MIGRATED = (
    "create_sprint", "get_sprint", "update_sprint", "move_sprint",
    "get_sprint_context", "assign_tasks_to_sprint", "submit_sprint_evaluation",
    "list_sprint_evaluations", "get_sprint_evaluation", "delete_sprint_evaluation",
    "answer_sprint_question", "delete_sprint_question", "suggest_sprints",
)


def _sprint_blocks() -> dict[str, str]:
    src = Path(mcp_server.__file__).read_text(encoding="utf-8")
    out: dict[str, str] = {}
    for n in ast.walk(ast.parse(src)):
        if isinstance(n, ast.AsyncFunctionDef):
            short = n.name.replace("okto_pulse_", "")
            if short in _MIGRATED:
                out[short] = ast.get_source_segment(src, n) or ""
    return out


# --- AST proofs (no DB) -----------------------------------------------------


def test_all_migrated_sprint_tools_strangled():
    blocks = _sprint_blocks()
    assert set(blocks) == set(_MIGRATED), (
        f"missing sprint tools: {set(_MIGRATED) - set(blocks)}"
    )
    still = [nm for nm, b in blocks.items() if "async with get_db_for_mcp" in b]
    assert not still, f"sprint tools still open get_db_for_mcp: {still}"


def test_sprint_wrappers_are_thin():
    """Clean Core: no wrapper carries a composed read query or a direct service."""
    blocks = _sprint_blocks()
    for nm, b in blocks.items():
        assert "uow.session" not in b, f"{nm} wrapper has a uow.session composed query"
        assert "SprintService(" not in b, f"{nm} wrapper constructs SprintService"
        assert "SprintQAService(" not in b, f"{nm} wrapper constructs SprintQAService"
        assert "db.get(Sprint" not in b, f"{nm} wrapper does a raw Sprint ORM read"


def test_mcp_sprint_crud_is_transport_free():
    from okto_pulse.core.application.use_cases import mcp_sprint_crud

    src = Path(mcp_sprint_crud.__file__).read_text(encoding="utf-8")
    bad = [
        (getattr(n, "module", None))
        for n in ast.walk(ast.parse(src))
        if isinstance(n, (ast.Import, ast.ImportFrom))
        and (getattr(n, "module", None) or "").startswith("okto_pulse.core.mcp")
    ]
    assert not bad, f"mcp_sprint_crud must not import the MCP transport package: {bad}"


def test_delete_evaluation_flag_modified_lives_in_service():
    """Option A: the ORM dirty-flag mutation stays in SprintService.delete_evaluation,
    not in the use-case layer nor the adapter."""
    from okto_pulse.core.application.use_cases import mcp_sprint_crud
    from okto_pulse.core.services import main as svc_main

    crud_src = Path(mcp_sprint_crud.__file__).read_text(encoding="utf-8")
    svc_src = Path(svc_main.__file__).read_text(encoding="utf-8")
    blocks = _sprint_blocks()
    assert "flag_modified" not in crud_src, "flag_modified must not be in the use case"
    assert "flag_modified" not in blocks["delete_sprint_evaluation"], (
        "flag_modified must not be in the adapter"
    )
    assert "async def delete_evaluation" in svc_src, "SprintService.delete_evaluation missing"


def test_delete_qa_activity_log_is_atomic_in_use_case():
    blocks = _sprint_blocks()
    assert "_log_activity" not in blocks["delete_sprint_question"]
    assert "_log_activity" not in blocks["answer_sprint_question"]
    from okto_pulse.core.application.use_cases import mcp_sprint_crud

    crud = Path(mcp_sprint_crud.__file__).read_text(encoding="utf-8")
    assert crud.count("_log_activity(") == 1, "only delete_sprint_question logs (atomic)"


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
async def test_get_sprint_roundtrip(_seed):
    got = await _call("okto_pulse_get_sprint", board_id=BOARD_ID, sprint_id=_seed)
    assert got["id"] == _seed
    assert got["status"] == "review"
    assert "cards" in got and "qa_items" in got


@pytest.mark.asyncio
async def test_get_sprint_cross_board_is_indistinguishable_from_missing(_foreign_sprint):
    got = await _call(
        "okto_pulse_get_sprint",
        board_id=BOARD_ID,
        sprint_id=_foreign_sprint["sprint_id"],
    )
    assert got == {"error": "Sprint not found"}


@pytest.mark.asyncio
async def test_cross_board_sprint_mutators_are_not_found_and_zero_write(_foreign_sprint):
    sprint_id = _foreign_sprint["sprint_id"]
    before = await _foreign_graph_snapshot(_foreign_sprint)

    moved = await _call(
        "okto_pulse_move_sprint",
        board_id=BOARD_ID,
        sprint_id=sprint_id,
        status="cancelled",
        cancellation_reason="must not persist",
    )
    assigned = await _call(
        "okto_pulse_assign_tasks_to_sprint",
        board_id=BOARD_ID,
        sprint_id=sprint_id,
        card_ids=[_foreign_sprint["unassigned_card_id"]],
    )
    evaluated = await _call(
        "okto_pulse_submit_sprint_evaluation",
        board_id=BOARD_ID,
        sprint_id=sprint_id,
        breakdown_completeness=8,
        breakdown_justification="blocked",
        granularity=8,
        granularity_justification="blocked",
        dependency_coherence=8,
        dependency_justification="blocked",
        test_coverage_quality=8,
        test_coverage_justification="blocked",
        overall_score=8,
        overall_justification="blocked",
        recommendation="approve",
    )

    assert moved == {"error": "Sprint not found"}
    assert assigned == {"error": "Sprint not found"}
    assert evaluated == {"error": "Sprint not found"}
    assert await _foreign_graph_snapshot(_foreign_sprint) == before


@pytest.mark.asyncio
async def test_assign_foreign_card_ids_is_missing_without_oracle_or_reparent(
    _seed,
    _foreign_sprint,
):
    before = await _foreign_graph_snapshot(_foreign_sprint)

    for card_id in (
        _foreign_sprint["unassigned_card_id"],
        _foreign_sprint["inconsistent_card_id"],
    ):
        out = await _call(
            "okto_pulse_assign_tasks_to_sprint",
            board_id=BOARD_ID,
            sprint_id=_seed,
            card_ids=[card_id],
        )
        assert out["error"] == "card_not_found"
        assert out["code"] == "card_not_found"
        rendered = json.dumps(out)
        assert "Foreign unassigned card" not in rendered
        assert "Cross-board card with matching spec" not in rendered
        assert _foreign_sprint["spec_id"] not in rendered
        assert OTHER_BOARD_ID not in rendered

    assert await _foreign_graph_snapshot(_foreign_sprint) == before


@pytest.mark.asyncio
async def test_unassign_foreign_card_id_is_legacy_zero_and_zero_mutation(
    _seed,
    _foreign_sprint,
):
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.sprints_crud import (
        UnassignSprintTasksCommand,
        UnassignSprintTasksUseCase,
    )
    from okto_pulse.core.domain.realm import LOCAL_REALM_ID
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory

    before = await _foreign_graph_snapshot(_foreign_sprint)
    actor = ActorContext(
        USER_ID,
        "mcp",
        board_id=BOARD_ID,
        realm_id=LOCAL_REALM_ID,
    )
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    async with uowf(actor=actor) as uow:
        result = await UnassignSprintTasksUseCase().execute(
            UnassignSprintTasksCommand(
                _seed,
                [_foreign_sprint["inconsistent_card_id"]],
            ),
            actor=actor,
            uow=uow,
        )

    assert result.unassigned == 0
    assert await _foreign_graph_snapshot(_foreign_sprint) == before


@pytest.mark.asyncio
async def test_cross_board_evaluation_reads_and_delete_are_not_found(_foreign_sprint):
    sprint_id = _foreign_sprint["sprint_id"]
    before = await _foreign_graph_snapshot(_foreign_sprint)

    listed = await _call(
        "okto_pulse_list_sprint_evaluations",
        board_id=BOARD_ID,
        sprint_id=sprint_id,
    )
    got = await _call(
        "okto_pulse_get_sprint_evaluation",
        board_id=BOARD_ID,
        sprint_id=sprint_id,
        evaluation_id="foreign-eval",
    )
    deleted = await _call(
        "okto_pulse_delete_sprint_evaluation",
        board_id=BOARD_ID,
        sprint_id=sprint_id,
        evaluation_id="foreign-eval",
    )

    assert listed == {"error": "Sprint not found"}
    assert got == {"error": "Sprint not found"}
    assert deleted == {"error": "Sprint not found"}
    assert await _foreign_graph_snapshot(_foreign_sprint) == before


@pytest.mark.asyncio
async def test_cross_board_sprint_qa_is_not_found_without_state_or_log(_foreign_sprint):
    sprint_id = _foreign_sprint["sprint_id"]
    qa_id = _foreign_sprint["qa_id"]
    before = await _foreign_graph_snapshot(_foreign_sprint)

    asked = await _call(
        "okto_pulse_ask_sprint_question",
        board_id=BOARD_ID,
        sprint_id=sprint_id,
        question="must not be created",
    )
    answered = await _call(
        "okto_pulse_answer_sprint_question",
        board_id=BOARD_ID,
        sprint_id=sprint_id,
        qa_id=qa_id,
        answer="must not persist",
    )
    deleted = await _call(
        "okto_pulse_delete_sprint_question",
        board_id=BOARD_ID,
        sprint_id=sprint_id,
        qa_id=qa_id,
    )

    assert asked == {"error": "Sprint not found"}
    assert answered == {"error": "Q&A item not found"}
    assert deleted == {"error": "Q&A item not found"}
    assert await _foreign_graph_snapshot(_foreign_sprint) == before


@pytest.mark.asyncio
async def test_update_sprint_cross_board_and_missing_are_zero_mutation(_foreign_sprint):
    sprint_id = _foreign_sprint["sprint_id"]
    before = await _sprint_snapshot(sprint_id)
    count_before = await _sprint_count()

    cross = await _call(
        "okto_pulse_update_sprint",
        board_id=BOARD_ID,
        sprint_id=sprint_id,
        title="must not persist",
    )
    missing = await _call(
        "okto_pulse_update_sprint",
        board_id=BOARD_ID,
        sprint_id="missing-sprint",
        title="must not persist either",
    )

    assert cross == {"error": "Sprint not found"}
    assert missing == {"error": "Sprint not found"}
    assert await _sprint_snapshot(sprint_id) == before
    assert await _sprint_count() == count_before


@pytest.mark.asyncio
async def test_create_sprint_rejects_foreign_spec_without_writing(_foreign_sprint):
    count_before = await _sprint_count()
    out = await _call(
        "okto_pulse_create_sprint",
        board_id=BOARD_ID,
        spec_id=_foreign_sprint["spec_id"],
        title="Cross-board Sprint",
    )
    assert out == {
        "error": "Failed to create sprint (spec not found or wrong board)"
    }
    assert await _sprint_count() == count_before


@pytest.mark.asyncio
async def test_create_and_update_sprint_same_board_still_succeed(_seed):
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        seeded = await db.get(Sprint, _seed)
        assert seeded is not None
        spec_id = seeded.spec_id

    created = await _call(
        "okto_pulse_create_sprint",
        board_id=BOARD_ID,
        spec_id=spec_id,
        title="Same-board Sprint",
    )
    assert created["success"] is True
    assert created["sprint"]["spec_id"] == spec_id

    updated = await _call(
        "okto_pulse_update_sprint",
        board_id=BOARD_ID,
        sprint_id=_seed,
        title="Same-board Update",
    )
    assert updated["success"] is True
    assert updated["sprint"]["title"] == "Same-board Update"
    assert (await _sprint_snapshot(_seed))[0] == "Same-board Update"


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
    from okto_pulse.core.application.use_cases.mcp_sprint_crud import (
        McpGetSprintContextCommand,
        McpGetSprintContextUseCase,
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
            await McpGetSprintContextUseCase().execute(
                McpGetSprintContextCommand(sprint_id, OTHER_BOARD_ID, True),
                actor=actor,
                uow=uow,
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


@pytest.mark.asyncio
async def test_get_sprint_context_cross_board_not_found(_seed):
    cross = await _call(
        "okto_pulse_get_sprint_context", board_id=OTHER_BOARD_ID, sprint_id=_seed
    )
    assert cross == {"error": "Sprint not found"}

    ok = await _call(
        "okto_pulse_get_sprint_context", board_id=BOARD_ID, sprint_id=_seed
    )
    assert ok["id"] == _seed and "spec" in ok  # include_spec defaults true
    assert ok["reviewer_separation"] == {
        "mode": "off",
        "allowed": True,
        "warning": False,
        "conflicts": ["sprint_creator"],
        "source": "legacy_absent_compat",
    }


@pytest.mark.asyncio
async def test_get_sprint_context_rejects_cross_board_parent_spec(_foreign_sprint):
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        inconsistent = Sprint(
            board_id=BOARD_ID,
            spec_id=_foreign_sprint["spec_id"],
            title="Inconsistent parent board",
            status=SprintStatus.DRAFT,
            created_by=USER_ID,
        )
        db.add(inconsistent)
        await db.commit()
        sprint_id = inconsistent.id

    out = await _call(
        "okto_pulse_get_sprint_context",
        board_id=BOARD_ID,
        sprint_id=sprint_id,
    )
    assert out == {"error": "Sprint not found"}


@pytest.mark.asyncio
async def test_assign_tasks_missing_card_is_explicit(_seed):
    result = await _call(
        "okto_pulse_assign_tasks_to_sprint",
        board_id=BOARD_ID,
        sprint_id=_seed,
        card_ids=["missing-card"],
    )
    assert result["error"] == "card_not_found"
    assert result["code"] == "card_not_found"
    assert result["facts"]["card_id"] == "missing-card"


@pytest.mark.asyncio
async def test_list_and_get_evaluations(_seed):
    listed = await _call(
        "okto_pulse_list_sprint_evaluations", board_id=BOARD_ID, sprint_id=_seed
    )
    assert listed["total"] == 2 and listed["approvals"] == 1
    assert listed["avg_score"] == 8  # only the approved one counts

    got = await _call(
        "okto_pulse_get_sprint_evaluation", board_id=BOARD_ID, sprint_id=_seed,
        evaluation_id="ev-own",
    )
    assert got["id"] == "ev-own"

    missing = await _call(
        "okto_pulse_get_sprint_evaluation", board_id=BOARD_ID, sprint_id=_seed,
        evaluation_id="nope",
    )
    assert missing == {"error": "Evaluation 'nope' not found"}


@pytest.mark.asyncio
async def test_delete_evaluation_ownership_gate(_seed):
    # someone else's evaluation -> refused (the ownership gate in the service)
    refused = await _call(
        "okto_pulse_delete_sprint_evaluation", board_id=BOARD_ID, sprint_id=_seed,
        evaluation_id="ev-other",
    )
    assert refused == {"error": "You can only delete your own evaluations"}

    # missing eval -> not found, no false positive
    missing = await _call(
        "okto_pulse_delete_sprint_evaluation", board_id=BOARD_ID, sprint_id=_seed,
        evaluation_id="nope",
    )
    assert missing == {"error": "Evaluation 'nope' not found"}

    # own evaluation -> deleted
    deleted = await _call(
        "okto_pulse_delete_sprint_evaluation", board_id=BOARD_ID, sprint_id=_seed,
        evaluation_id="ev-own",
    )
    assert deleted == {"success": True}

    # and it is gone (the JSON mutation + dirty-flag persisted via the service)
    listed = await _call(
        "okto_pulse_list_sprint_evaluations", board_id=BOARD_ID, sprint_id=_seed
    )
    assert listed["total"] == 1


@pytest.mark.asyncio
async def test_suggest_sprints_runs(_seed):
    out = await _call(
        "okto_pulse_suggest_sprints", board_id=BOARD_ID, spec_id="does-not-exist"
    )
    # spec not found / not ready -> ValueError mapped to {error: str} by the adapter;
    # a valid spec would return {suggestions, count}. Either proves the REUSE path runs.
    assert "error" in out or ("suggestions" in out and "count" in out)


@pytest.mark.asyncio
async def test_suggest_sprints_cross_board_spec_is_not_found(_foreign_sprint):
    out = await _call(
        "okto_pulse_suggest_sprints",
        board_id=BOARD_ID,
        spec_id=_foreign_sprint["spec_id"],
    )
    assert out == {"error": "Spec not found"}


@pytest.mark.asyncio
async def test_qa_ask_answer_delete(_seed):
    asked = await _call(
        "okto_pulse_ask_sprint_question", board_id=BOARD_ID, sprint_id=_seed,
        question="Is X in scope?",
    )
    qa_id = asked["qa"]["id"] if "qa" in asked else asked["id"]

    wrong_parent_answer = await _call(
        "okto_pulse_answer_sprint_question", board_id=BOARD_ID,
        sprint_id="another-sprint", qa_id=qa_id, answer="must not persist",
    )
    assert wrong_parent_answer == {"error": "Q&A item not found"}

    wrong_parent_delete = await _call(
        "okto_pulse_delete_sprint_question", board_id=BOARD_ID,
        sprint_id="another-sprint", qa_id=qa_id,
    )
    assert wrong_parent_delete == {"error": "Q&A item not found"}

    answered = await _call(
        "okto_pulse_answer_sprint_question", board_id=BOARD_ID, sprint_id=_seed,
        qa_id=qa_id, answer="yes",
    )
    # Same agent asked + answers -> QASelfAnsweringNotAllowedError caught + committed
    # in McpAnswerSprintQuestionUseCase -> {error, detail}.
    assert "error" in answered and "detail" in answered

    deleted = await _call(
        "okto_pulse_delete_sprint_question", board_id=BOARD_ID, sprint_id=_seed,
        qa_id=qa_id,
    )
    assert deleted == {"success": True}


@pytest.mark.asyncio
async def test_submit_evaluation_guard_returns_structured_envelope(_seed):
    """Adversarial (Codex contract): FullContextGuardError IS a ValueError subclass, so
    it must surface the MCP sibling's STRUCTURED envelope {error, reason, decision} (the
    guard's persisted decision audit), NOT be swallowed into the bare {error: str(e)} of
    the generic ValueError catch. move_sprint, by contrast, keeps the ValueError->{error}
    conversion (consistent with the validated move_* siblings)."""
    from unittest.mock import MagicMock

    from okto_pulse.core.services.critical_context_guard import FullContextGuardError
    from okto_pulse.core.services.main import SprintService

    decision = MagicMock()
    decision.audit_details.return_value = {
        "action": "sprint_submit_evaluation", "blocked": True,
    }
    guard_exc = FullContextGuardError("full context required", decision=decision)

    async def _raise(*_a, **_k):
        raise guard_exc

    with patch.object(SprintService, "submit_evaluation", _raise):
        out = await _call(
            "okto_pulse_submit_sprint_evaluation", board_id=BOARD_ID, sprint_id=_seed,
            breakdown_completeness=8, breakdown_justification="x",
            granularity=8, granularity_justification="x",
            dependency_coherence=8, dependency_justification="x",
            test_coverage_quality=8, test_coverage_justification="x",
            overall_score=8, overall_justification="x", recommendation="approve",
        )
    # structured envelope (mirrors submit_spec_evaluation), NOT the bare {error: str(e)}
    assert out.get("reason") == "full_context_unavailable"
    assert out.get("decision") == {"action": "sprint_submit_evaluation", "blocked": True}
    assert set(out) == {"error", "reason", "decision"}
