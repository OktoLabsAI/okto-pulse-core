"""R01A MCP-FU6 (family: refinement) — strangler oracle.

Proves the 14 refinement MCP tools were migrated off ``get_db_for_mcp`` / direct
service construction onto the MCP UnitOfWork + the transport-free
``mcp_refinement_crud`` use cases, WITHOUT behavior drift.

Consolidated proofs:
- AST: all 14 migrated refinement tools strangled (no ``get_db_for_mcp``).
- AST purity: ``mcp_refinement_crud`` imports neither ``okto_pulse.core.mcp`` nor a
  ``server.py`` helper.
- AST: the Q&A mutations carry the activity-log ATOMICALLY in the use case (3
  ``_log_activity`` in the module, none in the adapter tools).
- Runtime: same-board, missing-parent, cross-board, wrong-parent-Q&A, and direct
  actor/command mismatch matrices across CRUD, snapshot, history, KB, and Q&A.
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
    IdeationStatus,
    Refinement,
    RefinementHistory,
    RefinementKnowledgeBase,
    RefinementQAItem,
    RefinementSnapshot,
    RefinementStatus,
    Spec,
    SpecStatus,
)
from knowledge_governance_test_data import valid_governance_metadata

BOARD_ID = "r01a-mcprefinement"
OTHER_BOARD_ID = "r01a-mcprefinement-other"
USER_ID = "r01a-mcprefinement-agent"
OTHER_USER_ID = "r01a-mcprefinement-other-agent"

_MIGRATED = (
    "create_refinement",
    "get_refinement",
    "update_refinement",
    "move_refinement",
    "delete_refinement",
    "get_refinement_context",
    "get_refinement_snapshot",
    "get_refinement_history",
    "get_refinement_knowledge",
    "add_refinement_knowledge",
    "delete_refinement_knowledge",
    "ask_refinement_choice_question",
    "answer_refinement_question",
    "delete_refinement_question",
)


def _refinement_blocks() -> dict[str, str]:
    src = Path(mcp_server.__file__).read_text(encoding="utf-8")
    out: dict[str, str] = {}
    for n in ast.walk(ast.parse(src)):
        if isinstance(n, ast.AsyncFunctionDef):
            short = n.name.replace("okto_pulse_", "")
            if short in _MIGRATED:
                out[short] = ast.get_source_segment(src, n) or ""
    return out


# --- AST proofs (no DB) -----------------------------------------------------


def test_all_migrated_refinement_tools_strangled():
    blocks = _refinement_blocks()
    assert set(blocks) == set(_MIGRATED), (
        f"missing refinement tools: {set(_MIGRATED) - set(blocks)}"
    )
    still = [nm for nm, b in blocks.items() if "async with get_db_for_mcp" in b]
    assert not still, f"refinement tools still open get_db_for_mcp: {still}"


def test_mcp_refinement_crud_is_transport_free():
    from okto_pulse.core.application.use_cases import mcp_refinement_crud

    src = Path(mcp_refinement_crud.__file__).read_text(encoding="utf-8")
    bad = [
        (getattr(n, "module", None))
        for n in ast.walk(ast.parse(src))
        if isinstance(n, (ast.Import, ast.ImportFrom))
        and (getattr(n, "module", None) or "").startswith("okto_pulse.core.mcp")
    ]
    assert not bad, (
        f"mcp_refinement_crud must not import the MCP transport package: {bad}"
    )


def test_qa_activity_log_is_atomic_in_use_case():
    blocks = _refinement_blocks()
    for nm in (
        "ask_refinement_choice_question",
        "answer_refinement_question",
        "delete_refinement_question",
    ):
        assert "_log_activity" not in blocks[nm], f"{nm} must not log in the adapter"
    from okto_pulse.core.application.use_cases import mcp_refinement_crud

    crud = Path(mcp_refinement_crud.__file__).read_text(encoding="utf-8")
    assert crud.count("_log_activity(") == 3, "expected 3 atomic Q&A activity logs"


# --- runtime harness --------------------------------------------------------


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "mcp-refinement-test",
            "permissions": ["*"],
        },
    )()


@pytest.fixture(autouse=True)
def _auth():
    with (
        patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())),
        patch.object(mcp_server, "check_permission", return_value=None),
    ):
        yield


@pytest.fixture
async def _seed():
    """Board + a DONE ideation + a draft refinement on it (seeded directly to avoid the
    create-flow snapshot requirement)."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        for bid in (BOARD_ID, OTHER_BOARD_ID):
            if await db.get(Board, bid) is None:
                db.add(Board(id=bid, name="MCP Refinement", owner_id=USER_ID))
        await db.flush()
        ideation = Ideation(
            board_id=BOARD_ID,
            title="Parent",
            status=IdeationStatus.DONE,
            created_by=USER_ID,
        )
        db.add(ideation)
        await db.flush()
        refinement = Refinement(
            board_id=BOARD_ID,
            ideation_id=ideation.id,
            title="Refine A",
            status=RefinementStatus.DRAFT,
            created_by=USER_ID,
            # draft->review has a content gate (>=1 non-empty in_scope item).
            in_scope=["the in-scope item"],
        )
        db.add(refinement)
        await db.flush()
        rid = refinement.id
        await db.commit()
    return rid


async def _call(tool: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    t = await mcp_server.mcp.get_tool(tool)
    return json.loads(await t.fn(**kwargs))


@pytest.fixture
async def _create_parent_ideations():
    """DONE parents on the authorized board and on a different board."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        for bid in (BOARD_ID, OTHER_BOARD_ID):
            if await db.get(Board, bid) is None:
                db.add(Board(id=bid, name="MCP Refinement", owner_id=USER_ID))
        await db.flush()
        local = Ideation(
            board_id=BOARD_ID,
            title="Authorized parent",
            status=IdeationStatus.DONE,
            created_by=USER_ID,
        )
        foreign = Ideation(
            board_id=OTHER_BOARD_ID,
            title="Foreign parent",
            status=IdeationStatus.DONE,
            created_by=USER_ID,
        )
        db.add_all((local, foreign))
        await db.flush()
        parent_ids = (local.id, foreign.id)
        await db.commit()
    return parent_ids


async def _refinement_count() -> int:
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        return int(await db.scalar(select(func.count()).select_from(Refinement)) or 0)


@pytest.fixture
async def _board_scope_graph(_seed):
    """Same-board and foreign refinement graphs with every by-id child type."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        local = await db.get(Refinement, _seed)
        assert local is not None
        local_sibling = Refinement(
            board_id=BOARD_ID,
            ideation_id=local.ideation_id,
            title="Local sibling",
            status=RefinementStatus.DRAFT,
            created_by=USER_ID,
            in_scope=["sibling"],
        )
        foreign_parent = Ideation(
            board_id=OTHER_BOARD_ID,
            title="Foreign parent",
            status=IdeationStatus.DONE,
            created_by=OTHER_USER_ID,
        )
        db.add_all((local_sibling, foreign_parent))
        await db.flush()
        foreign = Refinement(
            board_id=OTHER_BOARD_ID,
            ideation_id=foreign_parent.id,
            title="foreign-secret-refinement",
            status=RefinementStatus.DRAFT,
            created_by=OTHER_USER_ID,
            in_scope=["foreign secret scope"],
        )
        db.add(foreign)
        await db.flush()

        rows: dict[str, dict[str, str]] = {}
        for label, refinement in (("local", local), ("foreign", foreign)):
            snapshot = RefinementSnapshot(
                refinement_id=refinement.id,
                version=1,
                title=f"{label}-secret-snapshot",
                description=f"{label} snapshot",
                in_scope=[label],
                out_of_scope=[],
                analysis=f"{label} analysis",
                decisions=[],
                labels=[label],
                qa_snapshot=[],
                created_by=OTHER_USER_ID,
            )
            history = RefinementHistory(
                refinement_id=refinement.id,
                action=f"{label}-secret-history",
                actor_type="user",
                actor_id=OTHER_USER_ID,
                actor_name="Other User",
                changes=[],
                summary=f"{label} history",
                version=1,
            )
            kb = RefinementKnowledgeBase(
                refinement_id=refinement.id,
                title=f"{label}-secret-kb",
                content=f"{label}-secret-content",
                mime_type="text/markdown",
                created_by=OTHER_USER_ID,
            )
            qa = RefinementQAItem(
                refinement_id=refinement.id,
                question=f"{label}-secret-question",
                question_type="choice",
                choices=[{"id": "opt_0", "label": "Yes"}],
                allow_free_text=False,
                asked_by=OTHER_USER_ID,
            )
            db.add_all((snapshot, history, kb, qa))
            await db.flush()
            rows[label] = {
                "refinement_id": refinement.id,
                "snapshot_id": snapshot.id,
                "history_id": history.id,
                "kb_id": kb.id,
                "qa_id": qa.id,
            }

        rows["local_sibling"] = {"refinement_id": local_sibling.id}
        await db.commit()
    return rows


async def _refinement_graph_state(graph: dict[str, dict[str, str]]) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    foreign = graph["foreign"]
    async with get_session_factory()() as db:
        refinement = await db.get(Refinement, foreign["refinement_id"])
        qa = await db.get(RefinementQAItem, foreign["qa_id"])
        kb = await db.get(RefinementKnowledgeBase, foreign["kb_id"])
        return {
            "refinement": None
            if refinement is None
            else (refinement.title, refinement.version, refinement.status),
            "qa": None
            if qa is None
            else (qa.answer, qa.selected, qa.answered_by, qa.answered_at),
            "kb": None if kb is None else (kb.refinement_id, kb.content),
            "refinement_count": int(
                await db.scalar(select(func.count()).select_from(Refinement)) or 0
            ),
            "kb_count": int(
                await db.scalar(
                    select(func.count()).select_from(RefinementKnowledgeBase)
                )
                or 0
            ),
            "qa_count": int(
                await db.scalar(select(func.count()).select_from(RefinementQAItem)) or 0
            ),
            "activity_count": int(
                await db.scalar(select(func.count()).select_from(ActivityLog)) or 0
            ),
        }


@pytest.mark.asyncio
async def test_create_refinement_same_board_succeeds(_create_parent_ideations):
    from okto_pulse.core.infra.database import get_session_factory

    local_ideation_id, _ = _create_parent_ideations
    created = await _call(
        "okto_pulse_create_refinement",
        board_id=BOARD_ID,
        ideation_id=local_ideation_id,
        title="Board-scoped refinement",
    )

    assert created["success"] is True
    async with get_session_factory()() as db:
        refinement = await db.get(Refinement, created["refinement"]["id"])
        assert refinement is not None
        assert refinement.board_id == BOARD_ID
        assert refinement.ideation_id == local_ideation_id


@pytest.mark.asyncio
async def test_create_refinement_cross_board_is_not_found_and_writes_nothing(
    _create_parent_ideations,
):
    _, foreign_ideation_id = _create_parent_ideations
    before = await _refinement_count()

    result = await _call(
        "okto_pulse_create_refinement",
        board_id=BOARD_ID,
        ideation_id=foreign_ideation_id,
        title="Must not cross boards",
    )

    assert result == {"error": "Failed to create refinement (ideation not found)"}
    assert await _refinement_count() == before


@pytest.mark.asyncio
async def test_create_refinement_missing_parent_is_not_found_and_writes_nothing(
    _create_parent_ideations,
):
    before = await _refinement_count()

    result = await _call(
        "okto_pulse_create_refinement",
        board_id=BOARD_ID,
        ideation_id="missing-ideation",
        title="Must not create without a parent",
    )

    assert result == {"error": "Failed to create refinement (ideation not found)"}
    assert await _refinement_count() == before


@pytest.mark.asyncio
async def test_get_update_move_delete_roundtrip(_seed):
    got = await _call(
        "okto_pulse_get_refinement", board_id=BOARD_ID, refinement_id=_seed
    )
    assert got["id"] == _seed

    updated = await _call(
        "okto_pulse_update_refinement",
        board_id=BOARD_ID,
        refinement_id=_seed,
        title="Refine A v2",
    )
    assert updated["refinement"]["title"] == "Refine A v2"

    moved = await _call(
        "okto_pulse_move_refinement",
        board_id=BOARD_ID,
        refinement_id=_seed,
        status="review",
    )
    assert moved["from_status"] == "draft" and moved["to_status"] == "review"

    deleted = await _call(
        "okto_pulse_delete_refinement", board_id=BOARD_ID, refinement_id=_seed
    )
    assert deleted["success"] is True
    assert deleted["takedown"]["artifact_type"] == "refinement"
    assert deleted["takedown"]["artifact_id"] == _seed


@pytest.mark.asyncio
async def test_delete_refinement_cascades_derived_specs(_seed):
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        refinement = await db.get(Refinement, _seed)
        spec = Spec(
            board_id=BOARD_ID,
            ideation_id=refinement.ideation_id,
            refinement_id=_seed,
            title="Cascade child",
            status=SpecStatus.DRAFT,
            created_by=USER_ID,
        )
        db.add(spec)
        await db.flush()
        spec_id = spec.id
        await db.commit()

    deleted = await _call(
        "okto_pulse_delete_refinement", board_id=BOARD_ID, refinement_id=_seed
    )

    assert deleted["success"] is True
    assert deleted["takedown"]["artifact_type"] == "refinement"
    descendants = deleted["takedown"]["descendant_deletions"]
    assert len(descendants) == 1
    assert descendants[0]["artifact_type"] == "spec"
    assert descendants[0]["artifact_id"] == spec_id
    async with factory() as db:
        assert await db.get(Refinement, _seed) is None
        assert await db.get(Spec, spec_id) is None


@pytest.mark.asyncio
async def test_get_refinement_cross_board_not_found(_seed):
    cross = await _call(
        "okto_pulse_get_refinement", board_id=OTHER_BOARD_ID, refinement_id=_seed
    )
    assert cross == {"error": "Refinement not found"}


@pytest.mark.asyncio
async def test_refinement_cross_board_matrix_has_no_payload_write_or_log(
    _board_scope_graph,
):
    foreign = _board_scope_graph["foreign"]
    refinement_id = foreign["refinement_id"]
    before = await _refinement_graph_state(_board_scope_graph)

    results = {
        "update": await _call(
            "okto_pulse_update_refinement",
            board_id=BOARD_ID,
            refinement_id=refinement_id,
            title="must-not-persist",
        ),
        "delete": await _call(
            "okto_pulse_delete_refinement",
            board_id=BOARD_ID,
            refinement_id=refinement_id,
        ),
        "snapshot": await _call(
            "okto_pulse_get_refinement_snapshot",
            board_id=BOARD_ID,
            refinement_id=refinement_id,
            version="1",
        ),
        "history": await _call(
            "okto_pulse_get_refinement_history",
            board_id=BOARD_ID,
            refinement_id=refinement_id,
        ),
        "get_kb": await _call(
            "okto_pulse_get_refinement_knowledge",
            board_id=BOARD_ID,
            refinement_id=refinement_id,
            knowledge_id=foreign["kb_id"],
        ),
        "add_kb": await _call(
            "okto_pulse_add_refinement_knowledge",
            board_id=BOARD_ID,
            refinement_id=refinement_id,
            title="must-not-create",
            content="must-not-persist",
        ),
        "delete_kb": await _call(
            "okto_pulse_delete_refinement_knowledge",
            board_id=BOARD_ID,
            refinement_id=refinement_id,
            knowledge_id=foreign["kb_id"],
        ),
        "ask": await _call(
            "okto_pulse_ask_refinement_choice_question",
            board_id=BOARD_ID,
            refinement_id=refinement_id,
            question="must-not-create",
            options="A|B",
        ),
        "answer": await _call(
            "okto_pulse_answer_refinement_question",
            board_id=BOARD_ID,
            refinement_id=refinement_id,
            qa_id=foreign["qa_id"],
            selected="opt_0",
        ),
        "delete_qa": await _call(
            "okto_pulse_delete_refinement_question",
            board_id=BOARD_ID,
            refinement_id=refinement_id,
            qa_id=foreign["qa_id"],
        ),
    }

    assert results == {
        "update": {"error": "Refinement not found"},
        "delete": {"error": "Refinement not found"},
        "snapshot": {"error": "Snapshot v1 not found"},
        "history": {"error": "Refinement not found"},
        "get_kb": {"error": "Refinement not found"},
        "add_kb": {
            "error": "Failed to create knowledge base item — refinement not found"
        },
        "delete_kb": {"error": "Refinement not found"},
        "ask": {"error": "Refinement not found"},
        "answer": {"error": "Q&A item not found or invalid selection"},
        "delete_qa": {"error": "Q&A item not found"},
    }
    assert "foreign-secret" not in json.dumps(results)
    assert await _refinement_graph_state(_board_scope_graph) == before


@pytest.mark.asyncio
async def test_refinement_missing_parent_matrix_is_not_found_and_zero_write(
    _board_scope_graph,
):
    before = await _refinement_graph_state(_board_scope_graph)
    missing = "missing-refinement"
    missing_qa = "missing-refinement-qa"
    missing_kb = "missing-refinement-kb"

    results = [
        await _call(
            "okto_pulse_update_refinement",
            board_id=BOARD_ID,
            refinement_id=missing,
            title="must-not-persist",
        ),
        await _call(
            "okto_pulse_delete_refinement",
            board_id=BOARD_ID,
            refinement_id=missing,
        ),
        await _call(
            "okto_pulse_get_refinement_snapshot",
            board_id=BOARD_ID,
            refinement_id=missing,
            version="1",
        ),
        await _call(
            "okto_pulse_get_refinement_history",
            board_id=BOARD_ID,
            refinement_id=missing,
        ),
        await _call(
            "okto_pulse_get_refinement_knowledge",
            board_id=BOARD_ID,
            refinement_id=missing,
            knowledge_id=missing_kb,
        ),
        await _call(
            "okto_pulse_add_refinement_knowledge",
            board_id=BOARD_ID,
            refinement_id=missing,
            title="must-not-create",
            content="must-not-persist",
        ),
        await _call(
            "okto_pulse_delete_refinement_knowledge",
            board_id=BOARD_ID,
            refinement_id=missing,
            knowledge_id=missing_kb,
        ),
        await _call(
            "okto_pulse_ask_refinement_choice_question",
            board_id=BOARD_ID,
            refinement_id=missing,
            question="must-not-create",
            options="A|B",
        ),
        await _call(
            "okto_pulse_answer_refinement_question",
            board_id=BOARD_ID,
            refinement_id=missing,
            qa_id=missing_qa,
            selected="opt_0",
        ),
        await _call(
            "okto_pulse_delete_refinement_question",
            board_id=BOARD_ID,
            refinement_id=missing,
            qa_id=missing_qa,
        ),
    ]

    assert results == [
        {"error": "Refinement not found"},
        {"error": "Refinement not found"},
        {"error": "Snapshot v1 not found"},
        {"error": "Refinement not found"},
        {"error": "Refinement not found"},
        {"error": "Failed to create knowledge base item — refinement not found"},
        {"error": "Refinement not found"},
        {"error": "Refinement not found"},
        {"error": "Q&A item not found or invalid selection"},
        {"error": "Q&A item not found"},
    ]
    assert await _refinement_graph_state(_board_scope_graph) == before


@pytest.mark.asyncio
async def test_refinement_direct_use_cases_compare_actor_and_command_board(
    _board_scope_graph,
):
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )
    from okto_pulse.core.application.use_cases.mcp_refinement_crud import (
        McpAskRefinementChoiceQuestionCommand,
        McpAskRefinementChoiceQuestionUseCase,
        McpGetRefinementCommand,
        McpGetRefinementSnapshotCommand,
        McpGetRefinementSnapshotUseCase,
        McpGetRefinementUseCase,
        McpUpdateRefinementCommand,
        McpUpdateRefinementUseCase,
    )
    from okto_pulse.core.domain.realm import LOCAL_REALM_ID
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.models.schemas import (
        RefinementQACreate,
        RefinementQAChoiceOption,
        RefinementUpdate,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory

    foreign = _board_scope_graph["foreign"]
    refinement_id = foreign["refinement_id"]
    actor = ActorContext(
        USER_ID,
        "mcp",
        board_id=BOARD_ID,
        realm_id=LOCAL_REALM_ID,
    )
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    before = await _refinement_graph_state(_board_scope_graph)

    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await McpGetRefinementUseCase().execute(
                McpGetRefinementCommand(refinement_id, OTHER_BOARD_ID),
                actor=actor,
                uow=uow,
            )
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await McpUpdateRefinementUseCase().execute(
                McpUpdateRefinementCommand(
                    refinement_id,
                    OTHER_BOARD_ID,
                    RefinementUpdate(title="must-not-persist"),
                ),
                actor=actor,
                uow=uow,
            )
    async with uowf(actor=actor) as uow:
        snapshot = await McpGetRefinementSnapshotUseCase().execute(
            McpGetRefinementSnapshotCommand(
                refinement_id,
                OTHER_BOARD_ID,
                1,
            ),
            actor=actor,
            uow=uow,
        )
        asked = await McpAskRefinementChoiceQuestionUseCase().execute(
            McpAskRefinementChoiceQuestionCommand(
                OTHER_BOARD_ID,
                refinement_id,
                RefinementQACreate(
                    question="must-not-create",
                    question_type="choice",
                    choices=[RefinementQAChoiceOption(id="opt_0", label="A")],
                ),
            ),
            actor=actor,
            uow=uow,
        )

    assert snapshot.snapshot is None
    assert asked.refinement_not_found is True
    assert await _refinement_graph_state(_board_scope_graph) == before


@pytest.mark.asyncio
async def test_refinement_qa_rejects_same_board_wrong_parent_without_log(
    _board_scope_graph,
):
    local = _board_scope_graph["local"]
    sibling = _board_scope_graph["local_sibling"]["refinement_id"]
    before = await _refinement_graph_state(_board_scope_graph)

    answered = await _call(
        "okto_pulse_answer_refinement_question",
        board_id=BOARD_ID,
        refinement_id=sibling,
        qa_id=local["qa_id"],
        selected="opt_0",
    )
    deleted = await _call(
        "okto_pulse_delete_refinement_question",
        board_id=BOARD_ID,
        refinement_id=sibling,
        qa_id=local["qa_id"],
    )

    assert answered == {"error": "Q&A item not found or invalid selection"}
    assert deleted == {"error": "Q&A item not found"}
    assert await _refinement_graph_state(_board_scope_graph) == before


@pytest.mark.asyncio
async def test_refinement_same_board_matrix_preserves_all_capabilities(
    _board_scope_graph,
):
    local = _board_scope_graph["local"]
    refinement_id = local["refinement_id"]

    snapshot = await _call(
        "okto_pulse_get_refinement_snapshot",
        board_id=BOARD_ID,
        refinement_id=refinement_id,
        version="1",
    )
    history = await _call(
        "okto_pulse_get_refinement_history",
        board_id=BOARD_ID,
        refinement_id=refinement_id,
    )
    knowledge = await _call(
        "okto_pulse_get_refinement_knowledge",
        board_id=BOARD_ID,
        refinement_id=refinement_id,
        knowledge_id=local["kb_id"],
    )
    updated = await _call(
        "okto_pulse_update_refinement",
        board_id=BOARD_ID,
        refinement_id=refinement_id,
        title="Local scoped update",
    )
    answered = await _call(
        "okto_pulse_answer_refinement_question",
        board_id=BOARD_ID,
        refinement_id=refinement_id,
        qa_id=local["qa_id"],
        selected="opt_0",
    )
    deleted_qa = await _call(
        "okto_pulse_delete_refinement_question",
        board_id=BOARD_ID,
        refinement_id=refinement_id,
        qa_id=local["qa_id"],
    )
    deleted_kb = await _call(
        "okto_pulse_delete_refinement_knowledge",
        board_id=BOARD_ID,
        refinement_id=refinement_id,
        knowledge_id=local["kb_id"],
    )

    assert snapshot["title"] == "local-secret-snapshot"
    assert any(item["id"] == local["history_id"] for item in history["history"])
    assert knowledge["id"] == local["kb_id"]
    assert updated["refinement"]["title"] == "Local scoped update"
    assert answered["success"] is True
    assert deleted_qa == {"success": True}
    assert deleted_kb == {"success": True}


@pytest.mark.asyncio
async def test_knowledge_add_get_roundtrip(_seed):
    added = await _call(
        "okto_pulse_add_refinement_knowledge",
        board_id=BOARD_ID,
        refinement_id=_seed,
        title="Notes",
        content="some markdown",
        governance_metadata=valid_governance_metadata(),
    )
    assert added["success"] is True
    assert added["knowledge"]["governance"]["metadata_status"] == "complete"
    kb_id = added["knowledge"]["id"]
    got = await _call(
        "okto_pulse_get_refinement_knowledge",
        board_id=BOARD_ID,
        refinement_id=_seed,
        knowledge_id=kb_id,
    )
    assert got["id"] == kb_id
    assert got["governance"] == added["knowledge"]["governance"]
    assert got == added["knowledge"]
    assert len(got["content_hash"]) == 64

    rejected = await _call(
        "okto_pulse_add_refinement_knowledge",
        board_id=BOARD_ID,
        refinement_id=_seed,
        title="Invalid",
        content="body",
        governance_metadata={},
    )
    assert rejected["code"] == "knowledge_governance_invalid_metadata"

    from okto_pulse.core.infra.database import get_session_factory

    async def kb_count() -> int:
        async with get_session_factory()() as db:
            return int(
                await db.scalar(
                    select(func.count())
                    .select_from(RefinementKnowledgeBase)
                    .where(RefinementKnowledgeBase.refinement_id == _seed)
                )
                or 0
            )

    before_blank = await kb_count()
    blank = await _call(
        "okto_pulse_add_refinement_knowledge",
        board_id=BOARD_ID,
        refinement_id=_seed,
        title="Blank metadata",
        content="body",
        governance_metadata="",
    )
    assert blank["code"] == "knowledge_governance_invalid_metadata"
    assert blank["issues"][0]["code"] == "invalid_json"
    assert await kb_count() == before_blank


@pytest.mark.asyncio
async def test_qa_ask_and_answer(_seed):
    asked = await _call(
        "okto_pulse_ask_refinement_choice_question",
        board_id=BOARD_ID,
        refinement_id=_seed,
        question="A or B?",
        options="A|B",
    )
    assert asked["success"] is True
    qa_id = asked["qa"]["id"]
    opt_id = asked["qa"]["choices"][0]["id"]
    answered = await _call(
        "okto_pulse_answer_refinement_question",
        board_id=BOARD_ID,
        refinement_id=_seed,
        qa_id=qa_id,
        selected=opt_id,
    )
    # Same agent asked + answers -> McpAnswerRefinementQuestionUseCase catches
    # QASelfAnsweringNotAllowedError (committing, legacy parity) -> error envelope.
    assert "error" in answered and "detail" in answered
