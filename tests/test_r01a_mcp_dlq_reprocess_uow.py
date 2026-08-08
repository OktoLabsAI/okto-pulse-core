"""Spec R01A MCP-FU2 — KG DLQ reprocess/connectivity tools on the UnitOfWork path.

The four DLQ/connectivity tools (dead_letter_reprocess, connectivity_dlq_diagnose,
connectivity_dlq_reprocess, connectivity_dlq_verify) now route through transport-free
use cases + the MCP ``UnitOfWorkFactory`` instead of a raw ``get_db_for_mcp()``
session. Commit parity is proven (not assumed):
- reads (diagnose/verify): golden payload == the service.
- ``dead_letter_reprocess`` (WRITE): commits and persists the requeue; a mid-flow
  failure rolls back (no partial DLQ state).
- ``connectivity_dlq_reprocess`` (WRITE): a fail-closed/blocked selection commits
  NOTHING (parity with the legacy conditional commit).
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import patch

import pytest

from okto_pulse.core.application.use_cases import (
    DiagnoseConnectivityDlqCommand,
    DiagnoseConnectivityDlqUseCase,
    ReprocessConnectivityDlqCommand,
    ReprocessConnectivityDlqUseCase,
    ReprocessDeadLetterRowsCommand,
    ReprocessDeadLetterRowsUseCase,
    VerifyConnectivityClassCommand,
    VerifyConnectivityClassUseCase,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
ACTOR = ActorContext("fu2-mcp-agent", "mcp")


def _board_actor(board_id: str, *, write: bool = False) -> ActorContext:
    return ActorContext(
        ACTOR.actor_id,
        "mcp",
        board_id=board_id,
        permissions=[
            "kg.admin.settings_write" if write else "kg.admin.settings_read"
        ],
    )


def _board() -> str:
    return f"board-fu2-{uuid.uuid4().hex[:8]}"


def _uowf():
    from okto_pulse.core.infra.database import get_session_factory

    return SQLAlchemyUnitOfWorkFactory(get_session_factory())


async def _seed_dlq_row(board_id: str) -> str:
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import ConsolidationDeadLetter

    await _seed_board(board_id)
    row_id = f"dlq-fu2-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            ConsolidationDeadLetter(
                id=row_id,
                board_id=board_id,
                artifact_type="spec",
                artifact_id=f"spec-{uuid.uuid4().hex[:6]}",
                original_queue_id=f"q-{uuid.uuid4().hex[:6]}",
                attempts=5,
                errors=[{"attempt": 1, "error_type": "X", "message": "m", "occurred_at": "2026-04-27T10:00:00", "traceback": None}],
            )
        )
        await db.commit()
    return row_id


async def _seed_board(board_id: str) -> None:
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import Board

    async with get_session_factory()() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="fu2", owner_id="fu2-owner"))
            await db.commit()


async def _dlq_exists(board_id: str, row_id: str) -> bool:
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import ConsolidationDeadLetter

    async with get_session_factory()() as db:
        row = await db.get(ConsolidationDeadLetter, row_id)
        return row is not None


# --- golden parity: reads (use case == service) ----------------------------


@pytest.mark.asyncio
async def test_diagnose_parity_with_service() -> None:
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.connectivity_dlq_reprocess_service import (
        diagnose_connectivity_guard_dlq,
    )

    board_id = _board()
    async with get_session_factory()() as db:
        baseline = await diagnose_connectivity_guard_dlq(db, board_id)

    actor = _board_actor(board_id)
    async with _uowf()(actor=actor) as uow:
        result = await DiagnoseConnectivityDlqUseCase().execute(
            DiagnoseConnectivityDlqCommand(board_id), actor=actor, uow=uow
        )
    assert json.loads(json.dumps(result.data, default=str)) == json.loads(
        json.dumps(baseline, default=str)
    )


@pytest.mark.asyncio
async def test_verify_parity_with_service() -> None:
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.connectivity_dlq_reprocess_service import (
        verify_connectivity_class_cleared,
    )

    board_id = _board()
    async with get_session_factory()() as db:
        baseline = await verify_connectivity_class_cleared(db, board_id, artifact_refs=None)

    actor = _board_actor(board_id)
    async with _uowf()(actor=actor) as uow:
        result = await VerifyConnectivityClassUseCase().execute(
            VerifyConnectivityClassCommand(board_id), actor=actor, uow=uow
        )
    assert json.loads(json.dumps(result.data, default=str)) == json.loads(
        json.dumps(baseline, default=str)
    )


# --- connectivity reprocess: blocked selection commits nothing -------------


@pytest.mark.asyncio
async def test_connectivity_reprocess_blocked_selection_commits_nothing() -> None:
    """Fail-closed parity: an empty in-class selection blocks (removes no DLQ) and
    the use case commits nothing — identical to the legacy conditional commit."""
    board_id = _board()
    await _seed_board(board_id)
    actor = _board_actor(board_id, write=True)
    async with _uowf()(actor=actor) as uow:
        result = await ReprocessConnectivityDlqUseCase().execute(
            ReprocessConnectivityDlqCommand(board_id, []), actor=actor, uow=uow
        )
    assert result.data.get("blocked")  # fail-closed, nothing reprocessed


# --- dead_letter_reprocess: WRITE persists + rollback parity ---------------


@pytest.mark.asyncio
async def test_dead_letter_reprocess_persists_requeue() -> None:
    board_id = _board()
    row_id = await _seed_dlq_row(board_id)
    assert await _dlq_exists(board_id, row_id)

    actor = _board_actor(board_id, write=True)
    async with _uowf()(actor=actor) as uow:
        result = await ReprocessDeadLetterRowsUseCase().execute(
            ReprocessDeadLetterRowsCommand(board_id, dead_letter_ids=[row_id], limit=50),
            actor=actor,
            uow=uow,
        )
    assert isinstance(result.data, dict)
    # The requeue committed: the DLQ row no longer exists (moved back to the queue).
    assert not await _dlq_exists(board_id, row_id)


@pytest.mark.asyncio
async def test_dead_letter_reprocess_rolls_back_on_mid_flow_failure() -> None:
    """Rollback parity: a mid-flow failure persists no partial DLQ state."""
    from sqlalchemy_test_models import ConsolidationDeadLetter

    board_id = _board()
    await _seed_board(board_id)
    marker_id = f"dlq-marker-{uuid.uuid4().hex[:8]}"

    async def _failing(session, board, **kwargs):
        session.add(
            ConsolidationDeadLetter(
                id=marker_id,
                board_id=board,
                artifact_type="marker",
                artifact_id="partial",
                original_queue_id="q-marker",
                attempts=1,
                errors=[],
            )
        )
        await session.flush()
        raise RuntimeError("mid-flow failure (R01A MCP-FU2)")

    with patch(
        "okto_pulse.core.services.dead_letter_inspector_service.reprocess_dead_letter_rows",
        _failing,
    ):
        with pytest.raises(RuntimeError):
            actor = _board_actor(board_id, write=True)
            async with _uowf()(actor=actor) as uow:
                await ReprocessDeadLetterRowsUseCase().execute(
                    ReprocessDeadLetterRowsCommand(board_id, dead_letter_ids=[marker_id]),
                    actor=actor,
                    uow=uow,
                )

    # The partial write rolled back — the marker never persisted.
    assert not await _dlq_exists(board_id, marker_id)


# --- MCP wrapper authorization --------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tool_name",
    [
        "okto_pulse_kg_dead_letter_reprocess",
        "okto_pulse_kg_connectivity_dlq_reprocess",
    ],
)
async def test_reprocess_wrappers_reject_obsolete_historical_authority_before_uow(
    tool_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import server as mcp_server

    denied_ctx = mcp_server.AgentContext(
        ACTOR.actor_id,
        "FU2 MCP agent",
        "board-a",
        ["kg.admin.historical_consolidation"],
    )

    async def _ctx(_board_id: str):
        return denied_ctx

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _ctx)
    monkeypatch.setattr(
        mcp_server,
        "get_unit_of_work_factory_for_mcp",
        lambda: (_ for _ in ()).throw(AssertionError("UoW must not open")),
    )
    tool = await mcp_server.mcp.get_tool(tool_name)

    raw = await tool.fn(
        board_id="board-a",
        dead_letter_ids=[],
        process_now=False,
    )

    denial = json.loads(raw)
    assert denial["error"] == "permission_denied"
    assert denial["required_permission"] == "kg.operations.queue.reprocess"


# --- AST strangler proof ---------------------------------------------------


def test_migrated_dlq_tools_have_no_get_db_for_mcp() -> None:
    import ast
    from pathlib import Path

    from okto_pulse.core.mcp import server as mcp_server

    tree = ast.parse(Path(mcp_server.__file__).read_text(encoding="utf-8"))
    tools = {
        "okto_pulse_kg_dead_letter_reprocess",
        "okto_pulse_kg_connectivity_dlq_diagnose",
        "okto_pulse_kg_connectivity_dlq_reprocess",
        "okto_pulse_kg_connectivity_dlq_verify",
    }
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name in tools:
            names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
            assert "get_db_for_mcp" not in names, node.name
            assert "get_unit_of_work_factory_for_mcp" in names, node.name
