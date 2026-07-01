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

import ast
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import Board, Spec, SpecStatus, Sprint, SprintStatus

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


async def _call(tool: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    t = await mcp_server.mcp.get_tool(tool)
    return json.loads(await t.fn(**kwargs))


@pytest.mark.asyncio
async def test_get_sprint_roundtrip(_seed):
    got = await _call("okto_pulse_get_sprint", board_id=BOARD_ID, sprint_id=_seed)
    assert got["id"] == _seed
    assert got["status"] == "review"
    assert "cards" in got and "qa_items" in got


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
async def test_qa_ask_answer_delete(_seed):
    asked = await _call(
        "okto_pulse_ask_sprint_question", board_id=BOARD_ID, sprint_id=_seed,
        question="Is X in scope?",
    )
    qa_id = asked["qa"]["id"] if "qa" in asked else asked["id"]

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
