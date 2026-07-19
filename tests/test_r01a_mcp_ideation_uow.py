"""R01A MCP-FU6 (family: ideation) — strangler oracle.

Proves the 16 ideation MCP tools were migrated off ``get_db_for_mcp`` / direct
service construction onto the MCP UnitOfWork + the transport-free
``mcp_ideation_crud`` use cases, WITHOUT behavior drift.

Consolidated proofs (Codex-mandated):
- AST: all 16 migrated ideation tools strangled (no ``get_db_for_mcp``); the two
  story tools keep no ``StoryService(db)`` in the adapter.
- AST purity: ``mcp_ideation_crud`` imports neither ``okto_pulse.core.mcp`` nor a
  ``server.py`` helper.
- AST: the Q&A mutations carry the activity-log ATOMICALLY in the use case (the
  ``_log_activity`` calls live in ``mcp_ideation_crud``, not the adapter tools).
- Runtime: same-board, missing-parent, cross-board, wrong-parent-Q&A, and direct
  actor/command mismatch matrices across CRUD, snapshot, history, KB, Q&A, and
  empty-scope evaluation; plus the story tools' governed not-found envelopes.
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
    Ideation,
    IdeationHistory,
    IdeationKnowledgeBase,
    IdeationQAItem,
    IdeationSnapshot,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecStatus,
)

BOARD_ID = "r01a-mcpideation"
OTHER_BOARD_ID = "r01a-mcpideation-other"
USER_ID = "r01a-mcpideation-agent"
OTHER_USER_ID = "r01a-mcpideation-other-agent"

_MIGRATED = (
    "create_ideation", "get_ideation", "update_ideation", "delete_ideation",
    "get_ideation_context", "get_ideation_snapshot", "get_ideation_history",
    "get_ideation_knowledge", "add_ideation_knowledge", "delete_ideation_knowledge",
    "ask_ideation_choice_question", "answer_ideation_question",
    "delete_ideation_question", "evaluate_ideation",
    "link_story_to_ideation", "convert_stories_to_ideation",
)


def _ideation_blocks() -> dict[str, str]:
    src = Path(mcp_server.__file__).read_text(encoding="utf-8")
    out: dict[str, str] = {}
    for n in ast.walk(ast.parse(src)):
        if isinstance(n, ast.AsyncFunctionDef):
            short = n.name.replace("okto_pulse_", "")
            if short in _MIGRATED:
                out[short] = ast.get_source_segment(src, n) or ""
    return out


# --- AST proofs (no DB) -----------------------------------------------------


def test_all_migrated_ideation_tools_strangled():
    blocks = _ideation_blocks()
    assert set(blocks) == set(_MIGRATED), (
        f"missing ideation tools: {set(_MIGRATED) - set(blocks)}"
    )
    still = [nm for nm, b in blocks.items() if "async with get_db_for_mcp" in b]
    assert not still, f"ideation tools still open get_db_for_mcp: {still}"


def test_story_tools_no_direct_service_in_adapter():
    blocks = _ideation_blocks()
    for nm in ("link_story_to_ideation", "convert_stories_to_ideation"):
        assert "StoryService(db)" not in blocks[nm], (
            f"{nm} must not construct StoryService in the adapter"
        )


def test_mcp_ideation_crud_is_transport_free():
    from okto_pulse.core.application.use_cases import mcp_ideation_crud

    src = Path(mcp_ideation_crud.__file__).read_text(encoding="utf-8")
    bad = [
        (getattr(n, "module", None))
        for n in ast.walk(ast.parse(src))
        if isinstance(n, (ast.Import, ast.ImportFrom))
        and (getattr(n, "module", None) or "").startswith("okto_pulse.core.mcp")
    ]
    assert not bad, f"mcp_ideation_crud must not import the MCP transport package: {bad}"


def test_qa_activity_log_is_atomic_in_use_case():
    """The Q&A mutations must carry the ``_log_activity`` in the use case (atomic with
    the mutation), NOT in the adapter tools."""
    blocks = _ideation_blocks()
    for nm in (
        "ask_ideation_choice_question",
        "answer_ideation_question",
        "delete_ideation_question",
    ):
        assert "_log_activity" not in blocks[nm], f"{nm} must not log in the adapter"
    from okto_pulse.core.application.use_cases import mcp_ideation_crud

    crud = Path(mcp_ideation_crud.__file__).read_text(encoding="utf-8")
    assert crud.count("_log_activity(") == 3, "expected 3 atomic Q&A activity logs"


# --- runtime harness --------------------------------------------------------


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "mcp-ideation-test",
            "permissions": [
                "*",
                "story.links.ideation",
                "story.conversion.to_ideation",
                "specs:create",
            ],
        },
    )()


@pytest.fixture(autouse=True)
def _auth():
    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())
    ), patch.object(mcp_server, "check_permission", return_value=None), patch.object(
        mcp_server, "_mcp_check_permission", return_value=None
    ):
        yield


@pytest.fixture
async def _seed():
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        for bid in (BOARD_ID, OTHER_BOARD_ID):
            if await db.get(Board, bid) is None:
                db.add(Board(id=bid, name="MCP Ideation", owner_id=USER_ID))
        await db.commit()
    return BOARD_ID


async def _call(tool: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    t = await mcp_server.mcp.get_tool(tool)
    return json.loads(await t.fn(**kwargs))


@pytest.fixture
async def _board_scope_graph(_seed):
    """Same-board and foreign ideation graphs with every by-id child type."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        local = Ideation(
            board_id=BOARD_ID,
            title="Local ideation",
            status=IdeationStatus.DRAFT,
            created_by=USER_ID,
        )
        local_sibling = Ideation(
            board_id=BOARD_ID,
            title="Local sibling",
            status=IdeationStatus.DRAFT,
            created_by=USER_ID,
        )
        foreign = Ideation(
            board_id=OTHER_BOARD_ID,
            title="foreign-secret-ideation",
            status=IdeationStatus.DRAFT,
            created_by=OTHER_USER_ID,
        )
        db.add_all((local, local_sibling, foreign))
        await db.flush()

        rows: dict[str, dict[str, str]] = {}
        for label, ideation in (("local", local), ("foreign", foreign)):
            snapshot = IdeationSnapshot(
                ideation_id=ideation.id,
                version=1,
                title=f"{label}-secret-snapshot",
                description=f"{label} snapshot",
                problem_statement=f"{label} problem",
                proposed_approach=f"{label} approach",
                scope_assessment={},
                complexity=None,
                labels=[label],
                qa_snapshot=[],
                created_by=OTHER_USER_ID,
            )
            history = IdeationHistory(
                ideation_id=ideation.id,
                action=f"{label}-secret-history",
                actor_type="user",
                actor_id=OTHER_USER_ID,
                actor_name="Other User",
                changes=[],
                summary=f"{label} history",
                version=1,
            )
            kb = IdeationKnowledgeBase(
                ideation_id=ideation.id,
                title=f"{label}-secret-kb",
                content=f"{label}-secret-content",
                mime_type="text/markdown",
                created_by=OTHER_USER_ID,
            )
            qa = IdeationQAItem(
                ideation_id=ideation.id,
                question=f"{label}-secret-question",
                question_type="choice",
                choices=[{"id": "opt_0", "label": "Yes"}],
                allow_free_text=False,
                asked_by=OTHER_USER_ID,
            )
            db.add_all((snapshot, history, kb, qa))
            await db.flush()
            rows[label] = {
                "ideation_id": ideation.id,
                "snapshot_id": snapshot.id,
                "history_id": history.id,
                "kb_id": kb.id,
                "qa_id": qa.id,
            }

        rows["local_sibling"] = {"ideation_id": local_sibling.id}
        await db.commit()
    return rows


async def _ideation_graph_state(graph: dict[str, dict[str, str]]) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    foreign = graph["foreign"]
    async with get_session_factory()() as db:
        ideation = await db.get(Ideation, foreign["ideation_id"])
        qa = await db.get(IdeationQAItem, foreign["qa_id"])
        kb = await db.get(IdeationKnowledgeBase, foreign["kb_id"])
        return {
            "ideation": None
            if ideation is None
            else (
                ideation.title,
                ideation.version,
                ideation.status,
                ideation.scope_assessment,
                ideation.complexity,
            ),
            "qa": None
            if qa is None
            else (qa.answer, qa.selected, qa.answered_by, qa.answered_at),
            "kb": None if kb is None else (kb.ideation_id, kb.content),
            "ideation_count": int(
                await db.scalar(select(func.count()).select_from(Ideation)) or 0
            ),
            "kb_count": int(
                await db.scalar(
                    select(func.count()).select_from(IdeationKnowledgeBase)
                )
                or 0
            ),
            "qa_count": int(
                await db.scalar(select(func.count()).select_from(IdeationQAItem))
                or 0
            ),
            "activity_count": int(
                await db.scalar(select(func.count()).select_from(ActivityLog)) or 0
            ),
        }


@pytest.mark.asyncio
async def test_create_get_update_delete_roundtrip(_seed):
    created = await _call(
        "okto_pulse_create_ideation", board_id=BOARD_ID, title="Idea A",
        problem_statement="we need X",
    )
    assert created["success"] is True
    iid = created["ideation"]["id"]

    got = await _call("okto_pulse_get_ideation", board_id=BOARD_ID, ideation_id=iid)
    assert got["id"] == iid
    assert "refinements" in got and "qa_items" in got

    updated = await _call(
        "okto_pulse_update_ideation", board_id=BOARD_ID, ideation_id=iid,
        title="Idea A v2",
    )
    assert updated["ideation"]["title"] == "Idea A v2"

    deleted = await _call(
        "okto_pulse_delete_ideation", board_id=BOARD_ID, ideation_id=iid
    )
    assert deleted == {"success": True}


@pytest.mark.asyncio
async def test_delete_ideation_cascades_refinements_and_specs(_seed):
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        ideation = Ideation(
            board_id=BOARD_ID,
            title="Cascade root",
            status=IdeationStatus.DONE,
            created_by=USER_ID,
        )
        db.add(ideation)
        await db.flush()
        refinement = Refinement(
            board_id=BOARD_ID,
            ideation_id=ideation.id,
            title="Cascade refinement",
            status=RefinementStatus.DRAFT,
            created_by=USER_ID,
        )
        db.add(refinement)
        await db.flush()
        spec = Spec(
            board_id=BOARD_ID,
            ideation_id=ideation.id,
            refinement_id=refinement.id,
            title="Cascade spec",
            status=SpecStatus.DRAFT,
            created_by=USER_ID,
        )
        db.add(spec)
        await db.flush()
        ideation_id = ideation.id
        refinement_id = refinement.id
        spec_id = spec.id
        await db.commit()

    deleted = await _call(
        "okto_pulse_delete_ideation", board_id=BOARD_ID, ideation_id=ideation_id
    )

    assert deleted == {"success": True}
    async with factory() as db:
        assert await db.get(Ideation, ideation_id) is None
        assert await db.get(Refinement, refinement_id) is None
        assert await db.get(Spec, spec_id) is None


@pytest.mark.asyncio
async def test_get_ideation_cross_board_not_found(_seed):
    created = await _call(
        "okto_pulse_create_ideation", board_id=BOARD_ID, title="Scoped"
    )
    iid = created["ideation"]["id"]
    cross = await _call(
        "okto_pulse_get_ideation", board_id=OTHER_BOARD_ID, ideation_id=iid
    )
    assert cross == {"error": "Ideation not found"}


@pytest.mark.asyncio
async def test_ideation_cross_board_matrix_has_no_payload_write_or_log(
    _board_scope_graph,
):
    foreign = _board_scope_graph["foreign"]
    ideation_id = foreign["ideation_id"]
    before = await _ideation_graph_state(_board_scope_graph)

    results = {
        "update": await _call(
            "okto_pulse_update_ideation",
            board_id=BOARD_ID,
            ideation_id=ideation_id,
            title="must-not-persist",
        ),
        "delete": await _call(
            "okto_pulse_delete_ideation",
            board_id=BOARD_ID,
            ideation_id=ideation_id,
        ),
        "snapshot": await _call(
            "okto_pulse_get_ideation_snapshot",
            board_id=BOARD_ID,
            ideation_id=ideation_id,
            version="1",
        ),
        "history": await _call(
            "okto_pulse_get_ideation_history",
            board_id=BOARD_ID,
            ideation_id=ideation_id,
        ),
        "get_kb": await _call(
            "okto_pulse_get_ideation_knowledge",
            board_id=BOARD_ID,
            ideation_id=ideation_id,
            knowledge_id=foreign["kb_id"],
        ),
        "add_kb": await _call(
            "okto_pulse_add_ideation_knowledge",
            board_id=BOARD_ID,
            ideation_id=ideation_id,
            title="must-not-create",
            content="must-not-persist",
        ),
        "delete_kb": await _call(
            "okto_pulse_delete_ideation_knowledge",
            board_id=BOARD_ID,
            ideation_id=ideation_id,
            knowledge_id=foreign["kb_id"],
        ),
        "ask": await _call(
            "okto_pulse_ask_ideation_choice_question",
            board_id=BOARD_ID,
            ideation_id=ideation_id,
            question="must-not-create",
            options="A|B",
        ),
        "answer": await _call(
            "okto_pulse_answer_ideation_question",
            board_id=BOARD_ID,
            ideation_id=ideation_id,
            qa_id=foreign["qa_id"],
            selected="opt_0",
        ),
        "delete_qa": await _call(
            "okto_pulse_delete_ideation_question",
            board_id=BOARD_ID,
            ideation_id=ideation_id,
            qa_id=foreign["qa_id"],
        ),
        "evaluate_empty_scope": await _call(
            "okto_pulse_evaluate_ideation",
            board_id=BOARD_ID,
            ideation_id=ideation_id,
        ),
    }

    assert results == {
        "update": {"error": "Ideation not found"},
        "delete": {"error": "Ideation not found"},
        "snapshot": {"error": "Snapshot v1 not found"},
        "history": {"error": "Ideation not found"},
        "get_kb": {"error": "Ideation not found"},
        "add_kb": {"error": "Ideation not found"},
        "delete_kb": {"error": "Ideation not found"},
        "ask": {"error": "Ideation not found"},
        "answer": {"error": "Q&A item not found or invalid selection"},
        "delete_qa": {"error": "Q&A item not found"},
        "evaluate_empty_scope": {"error": "Ideation not found"},
    }
    assert "foreign-secret" not in json.dumps(results)
    assert await _ideation_graph_state(_board_scope_graph) == before


@pytest.mark.asyncio
async def test_ideation_missing_parent_matrix_is_not_found_and_zero_write(
    _board_scope_graph,
):
    before = await _ideation_graph_state(_board_scope_graph)
    missing = "missing-ideation"
    missing_qa = "missing-ideation-qa"
    missing_kb = "missing-ideation-kb"

    results = [
        await _call(
            "okto_pulse_update_ideation",
            board_id=BOARD_ID,
            ideation_id=missing,
            title="must-not-persist",
        ),
        await _call(
            "okto_pulse_delete_ideation",
            board_id=BOARD_ID,
            ideation_id=missing,
        ),
        await _call(
            "okto_pulse_get_ideation_snapshot",
            board_id=BOARD_ID,
            ideation_id=missing,
            version="1",
        ),
        await _call(
            "okto_pulse_get_ideation_history",
            board_id=BOARD_ID,
            ideation_id=missing,
        ),
        await _call(
            "okto_pulse_get_ideation_knowledge",
            board_id=BOARD_ID,
            ideation_id=missing,
            knowledge_id=missing_kb,
        ),
        await _call(
            "okto_pulse_add_ideation_knowledge",
            board_id=BOARD_ID,
            ideation_id=missing,
            title="must-not-create",
            content="must-not-persist",
        ),
        await _call(
            "okto_pulse_delete_ideation_knowledge",
            board_id=BOARD_ID,
            ideation_id=missing,
            knowledge_id=missing_kb,
        ),
        await _call(
            "okto_pulse_ask_ideation_choice_question",
            board_id=BOARD_ID,
            ideation_id=missing,
            question="must-not-create",
            options="A|B",
        ),
        await _call(
            "okto_pulse_answer_ideation_question",
            board_id=BOARD_ID,
            ideation_id=missing,
            qa_id=missing_qa,
            selected="opt_0",
        ),
        await _call(
            "okto_pulse_delete_ideation_question",
            board_id=BOARD_ID,
            ideation_id=missing,
            qa_id=missing_qa,
        ),
        await _call(
            "okto_pulse_evaluate_ideation",
            board_id=BOARD_ID,
            ideation_id=missing,
        ),
    ]

    assert results == [
        {"error": "Ideation not found"},
        {"error": "Ideation not found"},
        {"error": "Snapshot v1 not found"},
        {"error": "Ideation not found"},
        {"error": "Ideation not found"},
        {"error": "Ideation not found"},
        {"error": "Ideation not found"},
        {"error": "Ideation not found"},
        {"error": "Q&A item not found or invalid selection"},
        {"error": "Q&A item not found"},
        {"error": "Ideation not found"},
    ]
    assert await _ideation_graph_state(_board_scope_graph) == before


@pytest.mark.asyncio
async def test_ideation_direct_use_cases_compare_actor_and_command_board(
    _board_scope_graph,
):
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )
    from okto_pulse.core.application.use_cases.mcp_ideation_crud import (
        McpAskIdeationChoiceQuestionCommand,
        McpAskIdeationChoiceQuestionUseCase,
        McpGetIdeationCommand,
        McpGetIdeationSnapshotCommand,
        McpGetIdeationSnapshotUseCase,
        McpGetIdeationUseCase,
        McpUpdateIdeationCommand,
        McpUpdateIdeationUseCase,
    )
    from okto_pulse.core.domain.realm import LOCAL_REALM_ID
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.models.schemas import (
        IdeationQACreate,
        IdeationQAChoiceOption,
        IdeationUpdate,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory

    foreign = _board_scope_graph["foreign"]
    ideation_id = foreign["ideation_id"]
    actor = ActorContext(
        USER_ID,
        "mcp",
        board_id=BOARD_ID,
        realm_id=LOCAL_REALM_ID,
    )
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    before = await _ideation_graph_state(_board_scope_graph)

    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await McpGetIdeationUseCase().execute(
                McpGetIdeationCommand(ideation_id, OTHER_BOARD_ID),
                actor=actor,
                uow=uow,
            )
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await McpUpdateIdeationUseCase().execute(
                McpUpdateIdeationCommand(
                    ideation_id,
                    OTHER_BOARD_ID,
                    IdeationUpdate(title="must-not-persist"),
                ),
                actor=actor,
                uow=uow,
            )
    async with uowf(actor=actor) as uow:
        snapshot = await McpGetIdeationSnapshotUseCase().execute(
            McpGetIdeationSnapshotCommand(ideation_id, OTHER_BOARD_ID, 1),
            actor=actor,
            uow=uow,
        )
        asked = await McpAskIdeationChoiceQuestionUseCase().execute(
            McpAskIdeationChoiceQuestionCommand(
                OTHER_BOARD_ID,
                ideation_id,
                IdeationQACreate(
                    question="must-not-create",
                    question_type="choice",
                    choices=[IdeationQAChoiceOption(id="opt_0", label="A")],
                ),
            ),
            actor=actor,
            uow=uow,
        )

    assert snapshot.snapshot is None
    assert asked.ideation_not_found is True
    assert await _ideation_graph_state(_board_scope_graph) == before


@pytest.mark.asyncio
async def test_ideation_qa_rejects_same_board_wrong_parent_without_log(
    _board_scope_graph,
):
    local = _board_scope_graph["local"]
    sibling = _board_scope_graph["local_sibling"]["ideation_id"]
    before = await _ideation_graph_state(_board_scope_graph)

    answered = await _call(
        "okto_pulse_answer_ideation_question",
        board_id=BOARD_ID,
        ideation_id=sibling,
        qa_id=local["qa_id"],
        selected="opt_0",
    )
    deleted = await _call(
        "okto_pulse_delete_ideation_question",
        board_id=BOARD_ID,
        ideation_id=sibling,
        qa_id=local["qa_id"],
    )

    assert answered == {"error": "Q&A item not found or invalid selection"}
    assert deleted == {"error": "Q&A item not found"}
    assert await _ideation_graph_state(_board_scope_graph) == before


@pytest.mark.asyncio
async def test_ideation_same_board_matrix_preserves_all_capabilities(
    _board_scope_graph,
):
    local = _board_scope_graph["local"]
    ideation_id = local["ideation_id"]

    snapshot = await _call(
        "okto_pulse_get_ideation_snapshot",
        board_id=BOARD_ID,
        ideation_id=ideation_id,
        version="1",
    )
    history = await _call(
        "okto_pulse_get_ideation_history",
        board_id=BOARD_ID,
        ideation_id=ideation_id,
    )
    knowledge = await _call(
        "okto_pulse_get_ideation_knowledge",
        board_id=BOARD_ID,
        ideation_id=ideation_id,
        knowledge_id=local["kb_id"],
    )
    updated = await _call(
        "okto_pulse_update_ideation",
        board_id=BOARD_ID,
        ideation_id=ideation_id,
        title="Local scoped update",
    )
    answered = await _call(
        "okto_pulse_answer_ideation_question",
        board_id=BOARD_ID,
        ideation_id=ideation_id,
        qa_id=local["qa_id"],
        selected="opt_0",
    )
    deleted_qa = await _call(
        "okto_pulse_delete_ideation_question",
        board_id=BOARD_ID,
        ideation_id=ideation_id,
        qa_id=local["qa_id"],
    )
    deleted_kb = await _call(
        "okto_pulse_delete_ideation_knowledge",
        board_id=BOARD_ID,
        ideation_id=ideation_id,
        knowledge_id=local["kb_id"],
    )

    assert snapshot["title"] == "local-secret-snapshot"
    assert any(item["id"] == local["history_id"] for item in history["history"])
    assert knowledge["id"] == local["kb_id"]
    assert updated["ideation"]["title"] == "Local scoped update"
    assert answered["success"] is True
    assert deleted_qa == {"success": True}
    assert deleted_kb == {"success": True}


@pytest.mark.asyncio
async def test_knowledge_add_get_roundtrip(_seed):
    created = await _call(
        "okto_pulse_create_ideation", board_id=BOARD_ID, title="With KB"
    )
    iid = created["ideation"]["id"]
    added = await _call(
        "okto_pulse_add_ideation_knowledge", board_id=BOARD_ID, ideation_id=iid,
        title="Notes", content="some markdown",
    )
    assert added["success"] is True
    kb_id = added["knowledge"]["id"]
    got = await _call(
        "okto_pulse_get_ideation_knowledge", board_id=BOARD_ID, ideation_id=iid,
        knowledge_id=kb_id,
    )
    assert got["id"] == kb_id


@pytest.mark.asyncio
async def test_qa_ask_and_answer(_seed):
    created = await _call(
        "okto_pulse_create_ideation", board_id=BOARD_ID, title="With QA"
    )
    iid = created["ideation"]["id"]
    asked = await _call(
        "okto_pulse_ask_ideation_choice_question", board_id=BOARD_ID, ideation_id=iid,
        question="A or B?", options="A|B",
    )
    assert asked["success"] is True
    qa_id = asked["qa"]["id"]
    opt_id = asked["qa"]["choices"][0]["id"]
    answered = await _call(
        "okto_pulse_answer_ideation_question", board_id=BOARD_ID, ideation_id=iid,
        qa_id=qa_id, selected=opt_id,
    )
    # The SAME agent asked + answers -> McpAnswerIdeationQuestionUseCase catches
    # QASelfAnsweringNotAllowedError (committing, legacy parity) and returns the
    # error envelope. This exercises the use case's self-answer path.
    assert "error" in answered and "detail" in answered


@pytest.mark.asyncio
async def test_evaluate_ideation_classifies_submitted_scope_and_persists_it(_seed):
    created = await _call(
        "okto_pulse_create_ideation", board_id=BOARD_ID, title="Scope evaluation"
    )
    iid = created["ideation"]["id"]

    for status in ("review", "approved", "evaluating"):
        moved = await _call(
            "okto_pulse_move_ideation",
            board_id=BOARD_ID,
            ideation_id=iid,
            status=status,
        )
        assert moved["success"] is True

    evaluated = await _call(
        "okto_pulse_evaluate_ideation",
        board_id=BOARD_ID,
        ideation_id=iid,
        domains="3",
        domains_justification="Three application boundaries.",
        ambiguity="1",
        ambiguity_justification="The behavior is explicit.",
        dependencies="2",
        dependencies_justification="One external adapter is required.",
    )
    assert evaluated["complexity"] == "large"
    assert evaluated["scope_assessment"] == {
        "domains": 3,
        "domains_justification": "Three application boundaries.",
        "ambiguity": 1,
        "ambiguity_justification": "The behavior is explicit.",
        "dependencies": 2,
        "dependencies_justification": "One external adapter is required.",
    }

    persisted = await _call(
        "okto_pulse_get_ideation", board_id=BOARD_ID, ideation_id=iid
    )
    assert persisted["complexity"] == "large"
    assert persisted["scope_assessment"] == evaluated["scope_assessment"]


@pytest.mark.asyncio
async def test_link_story_missing_story_envelope(_seed):
    created = await _call(
        "okto_pulse_create_ideation", board_id=BOARD_ID, title="Target"
    )
    iid = created["ideation"]["id"]
    out = await _call(
        "okto_pulse_link_story_to_ideation", board_id=BOARD_ID,
        story_id="does-not-exist", ideation_id=iid,
    )
    assert out == {"error": "Story or Ideation not found"}


@pytest.mark.asyncio
async def test_convert_stories_missing_story_envelope(_seed):
    out = await _call(
        "okto_pulse_convert_stories_to_ideation", board_id=BOARD_ID,
        story_ids="does-not-exist", title="From stories",
    )
    assert out == {"error": "One or more Stories were not found in this board"}
