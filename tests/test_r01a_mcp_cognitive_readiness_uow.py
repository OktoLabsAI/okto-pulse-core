"""Spec R01A MCP-FU3 — KG cognitive readiness read tools on the UnitOfWork path.

The three read-only cognitive tools (evaluate_bug_cognitive_closure,
list_cognitive_readiness_items, evaluate_cognitive_readiness) now route through
transport-free use cases + the MCP ``UnitOfWorkFactory`` instead of a raw
``get_db_for_mcp()`` session. Golden parity: each use case returns the SAME
verdict/payload as the central service (precedence never recomputed), and the
enforcement flag matches the extracted ``cognitive_enforcement_active`` reader.
Read-only — no commit.
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.application.use_cases import (
    EvaluateBugCognitiveClosureCommand,
    EvaluateBugCognitiveClosureUseCase,
    EvaluateCognitiveReadinessCommand,
    EvaluateCognitiveReadinessUseCase,
    ListCognitiveReadinessItemsCommand,
    ListCognitiveReadinessItemsUseCase,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
ACTOR = ActorContext("fu3-mcp-agent", "mcp", realm_id=LOCAL_REALM_ID)


def _board_actor(board_id: str) -> ActorContext:
    return ActorContext(
        ACTOR.actor_id,
        "mcp",
        board_id=board_id,
        realm_id=LOCAL_REALM_ID,
        permissions=["kg.admin.settings_read"],
    )


def _board() -> str:
    return f"board-fu3-{uuid.uuid4().hex[:8]}"


def _uowf():
    from okto_pulse.core.infra.database import get_session_factory

    return SQLAlchemyUnitOfWorkFactory(get_session_factory())


def _service():
    from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessService
    from memory_rebuild_audit_storage import (
        InMemoryRebuildAuditArtifactStore,
    )
    from okto_pulse.core.kg.rebuild_audit import CognitiveConsolidationItemStore

    return CognitiveReadinessService(
        CognitiveConsolidationItemStore(
            artifact_store=InMemoryRebuildAuditArtifactStore()
        )
    )


async def _seed_board(board_id: str) -> None:
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import Board

    async with get_session_factory()() as db:
        if await db.get(Board, board_id) is None:
            db.add(
                Board(
                    id=board_id,
                    name="fu3",
                    owner_id="fu3-owner",
                    realm_id=LOCAL_REALM_ID,
                )
            )
            await db.commit()


# --- golden parity: use case == central service ----------------------------


@pytest.mark.asyncio
async def test_evaluate_cognitive_readiness_parity() -> None:
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.main import cognitive_enforcement_active

    board_id = _board()
    await _seed_board(board_id)
    source_ref = f"spec:{uuid.uuid4().hex[:8]}"

    async with get_session_factory()() as db:
        baseline = await _service().evaluate_artifact(
            db, board_id=board_id, source_ref=source_ref,
            kg_generation_id=None, has_reusable_cognition=True,
        )
        baseline_enf = await cognitive_enforcement_active(db, board_id)

    actor = _board_actor(board_id)
    async with _uowf()(actor=actor) as uow:
        result = await EvaluateCognitiveReadinessUseCase().execute(
            EvaluateCognitiveReadinessCommand(board_id, source_ref=source_ref),
            actor=actor, uow=uow,
        )
    assert result.verdict.to_api() == baseline.to_api()
    assert result.enforcement_active == baseline_enf


@pytest.mark.asyncio
async def test_list_cognitive_readiness_items_parity() -> None:
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.kg.cognitive_action_center import CognitiveActionCenterReadModel
    from okto_pulse.core.services.main import cognitive_enforcement_active

    board_id = _board()
    await _seed_board(board_id)

    async with get_session_factory()() as db:
        baseline = await CognitiveActionCenterReadModel(_service()).list_signals(
            db, board_id=board_id, signal="all", artifact_id=None, source_ref=None,
            reason_code=None, status=None, search=None, limit=50, offset=0,
            kg_generation_id=None,
        )
        baseline_enf = await cognitive_enforcement_active(db, board_id)

    actor = _board_actor(board_id)
    async with _uowf()(actor=actor) as uow:
        result = await ListCognitiveReadinessItemsUseCase().execute(
            ListCognitiveReadinessItemsCommand(board_id), actor=actor, uow=uow
        )
    assert result.result == baseline
    assert result.enforcement_active == baseline_enf


@pytest.mark.asyncio
async def test_evaluate_bug_cognitive_closure_context_aware_fail_closed() -> None:
    """A direct policy call without context preserves the bounded legacy error;
    the transport use case assembles canonical context and reports the more
    precise missing source blocker without trusting caller evidence."""
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.kg.bug_cognitive_closure import evaluate_bug_cognitive_closure
    from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessError
    from okto_pulse.core.ports.bug_cognitive_context import (
        BugCognitiveContext,
        register_bug_cognitive_context_assembler,
    )

    board_id = _board()
    await _seed_board(board_id)
    bug_id = f"bug-{uuid.uuid4().hex[:8]}"

    with pytest.raises(CognitiveReadinessError) as svc_exc:
        async with get_session_factory()() as db:
            await evaluate_bug_cognitive_closure(
                _service(), db, board_id=board_id, bug_id=bug_id, evidence={},
                requested_action="evaluate", reason_code=None, actor=ACTOR.actor_id,
                justification=None, evidence_refs=None, revisit_at=None,
            )

    class _AbsentContextAssembler:
        async def assemble(self, _db, *, board_id, bug_id):  # noqa: ANN001
            return BugCognitiveContext(
                board_id=board_id,
                bug_id=bug_id,
                card_exists=False,
            )

    register_bug_cognitive_context_assembler(_AbsentContextAssembler())

    actor = _board_actor(board_id)
    async with _uowf()(actor=actor) as uow:
        result = await EvaluateBugCognitiveClosureUseCase().execute(
            EvaluateBugCognitiveClosureCommand(board_id, bug_id),
            actor=actor,
            uow=uow,
        )

    assert svc_exc.value.code == "missing_bug_evidence"
    assert result.data["status"] == "bug_source_not_found"
    assert result.data["blocking"] is True
    assert result.data["readiness_effect"] == "blocking_technical"


# --- AST strangler + enforcement extraction --------------------------------


def test_migrated_cognitive_tools_have_no_get_db_for_mcp() -> None:
    import ast
    from pathlib import Path

    from okto_pulse.core.mcp import server as mcp_server

    tree = ast.parse(Path(mcp_server.__file__).read_text(encoding="utf-8"))
    tools = {
        "okto_pulse_kg_evaluate_bug_cognitive_closure",
        "okto_pulse_kg_list_cognitive_readiness_items",
        "okto_pulse_kg_evaluate_cognitive_readiness",
    }
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name in tools:
            names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
            assert "get_db_for_mcp" not in names, node.name
            assert "get_unit_of_work_factory_for_mcp" in names, node.name


@pytest.mark.asyncio
async def test_enforcement_helper_extracted_and_delegated() -> None:
    """The server helper delegates to the transport-free reader (4 callers intact)."""
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.mcp import server as mcp_server
    from okto_pulse.core.services.main import cognitive_enforcement_active

    board_id = _board()
    await _seed_board(board_id)
    async with get_session_factory()() as db:
        direct = await cognitive_enforcement_active(db, board_id)
    async with _uowf()(actor=ACTOR) as uow:
        delegated = await mcp_server._cognitive_enforcement_active(
            uow.services.kg,
            board_id,
        )
    assert direct == delegated
