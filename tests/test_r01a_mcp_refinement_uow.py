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
- Runtime: get/update/move/delete round-trip on a seeded refinement; KB add/get; Q&A
  ask/answer (self-answer path); the board-scope asymmetry (cross-board not found).
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import (
    Board,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementStatus,
)

BOARD_ID = "r01a-mcprefinement"
OTHER_BOARD_ID = "r01a-mcprefinement-other"
USER_ID = "r01a-mcprefinement-agent"

_MIGRATED = (
    "create_refinement", "get_refinement", "update_refinement", "move_refinement",
    "delete_refinement", "get_refinement_context", "get_refinement_snapshot",
    "get_refinement_history", "get_refinement_knowledge", "add_refinement_knowledge",
    "delete_refinement_knowledge", "ask_refinement_choice_question",
    "answer_refinement_question", "delete_refinement_question",
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
    assert not bad, f"mcp_refinement_crud must not import the MCP transport package: {bad}"


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
        {"agent_id": USER_ID, "agent_name": "mcp-refinement-test", "permissions": ["*"]},
    )()


@pytest.fixture(autouse=True)
def _auth():
    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())
    ), patch.object(mcp_server, "check_permission", return_value=None):
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
            board_id=BOARD_ID, title="Parent", status=IdeationStatus.DONE,
            created_by=USER_ID,
        )
        db.add(ideation)
        await db.flush()
        refinement = Refinement(
            board_id=BOARD_ID, ideation_id=ideation.id, title="Refine A",
            status=RefinementStatus.DRAFT, created_by=USER_ID,
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

    mcp_server.register_session_factory(get_session_factory())
    t = await mcp_server.mcp.get_tool(tool)
    return json.loads(await t.fn(**kwargs))


@pytest.mark.asyncio
async def test_get_update_move_delete_roundtrip(_seed):
    got = await _call("okto_pulse_get_refinement", board_id=BOARD_ID, refinement_id=_seed)
    assert got["id"] == _seed

    updated = await _call(
        "okto_pulse_update_refinement", board_id=BOARD_ID, refinement_id=_seed,
        title="Refine A v2",
    )
    assert updated["refinement"]["title"] == "Refine A v2"

    moved = await _call(
        "okto_pulse_move_refinement", board_id=BOARD_ID, refinement_id=_seed,
        status="review",
    )
    assert moved["from_status"] == "draft" and moved["to_status"] == "review"

    deleted = await _call(
        "okto_pulse_delete_refinement", board_id=BOARD_ID, refinement_id=_seed
    )
    assert deleted == {"success": True}


@pytest.mark.asyncio
async def test_get_refinement_cross_board_not_found(_seed):
    cross = await _call(
        "okto_pulse_get_refinement", board_id=OTHER_BOARD_ID, refinement_id=_seed
    )
    assert cross == {"error": "Refinement not found"}


@pytest.mark.asyncio
async def test_knowledge_add_get_roundtrip(_seed):
    added = await _call(
        "okto_pulse_add_refinement_knowledge", board_id=BOARD_ID, refinement_id=_seed,
        title="Notes", content="some markdown",
    )
    assert added["success"] is True
    kb_id = added["knowledge"]["id"]
    got = await _call(
        "okto_pulse_get_refinement_knowledge", board_id=BOARD_ID, refinement_id=_seed,
        knowledge_id=kb_id,
    )
    assert got["id"] == kb_id


@pytest.mark.asyncio
async def test_qa_ask_and_answer(_seed):
    asked = await _call(
        "okto_pulse_ask_refinement_choice_question", board_id=BOARD_ID,
        refinement_id=_seed, question="A or B?", options="A|B",
    )
    assert asked["success"] is True
    qa_id = asked["qa"]["id"]
    opt_id = asked["qa"]["choices"][0]["id"]
    answered = await _call(
        "okto_pulse_answer_refinement_question", board_id=BOARD_ID,
        refinement_id=_seed, qa_id=qa_id, selected=opt_id,
    )
    # Same agent asked + answers -> McpAnswerRefinementQuestionUseCase catches
    # QASelfAnsweringNotAllowedError (committing, legacy parity) -> error envelope.
    assert "error" in answered and "detail" in answered
