"""Spec R01A MCP-FU3B — KG cognitive DLQ read tool on the UnitOfWork path.

``okto_pulse_kg_list_cognitive_dlq`` used to issue SQL inline
(``select(ConsolidationDeadLetter)``). The query is now a dedicated reader
(``list_cognitive_dlq_rows``) behind a transport-free use case + the MCP
``UnitOfWorkFactory``, so the tool no longer issues SQL or opens a raw
``get_db_for_mcp()`` session. Golden parity: the use case returns the SAME
``(total, rows)`` as the reader; the tool's row projection is unchanged.
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.application.use_cases import (
    ListCognitiveDlqCommand,
    ListCognitiveDlqUseCase,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
ACTOR = ActorContext("fu3b-mcp-agent", "mcp")


def _uowf():
    from okto_pulse.core.infra.database import get_session_factory

    return SQLAlchemyUnitOfWorkFactory(get_session_factory())


async def _seed_dlq(board_id: str, n: int) -> list[str]:
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import Board, ConsolidationDeadLetter

    ids: list[str] = []
    async with get_session_factory()() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="fu3b", owner_id="fu3b-owner"))
            await db.flush()
        for i in range(n):
            row_id = f"dlq-fu3b-{i}-{uuid.uuid4().hex[:8]}"
            db.add(
                ConsolidationDeadLetter(
                    id=row_id,
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id=f"spec-{i}-{uuid.uuid4().hex[:6]}",
                    original_queue_id=f"q-{i}",
                    attempts=3,
                    errors=[],
                )
            )
            ids.append(row_id)
        await db.commit()
    return ids


@pytest.mark.asyncio
async def test_list_cognitive_dlq_use_case_matches_reader() -> None:
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.dead_letter_inspector_service import (
        list_cognitive_dlq_rows,
    )

    board_id = f"board-fu3b-{uuid.uuid4().hex[:8]}"
    seeded = sorted(await _seed_dlq(board_id, 3))

    async with get_session_factory()() as db:
        base_total, base_rows = await list_cognitive_dlq_rows(
            db, board_id, limit=50, offset=0
        )

    async with _uowf()(actor=ACTOR) as uow:
        result = await ListCognitiveDlqUseCase().execute(
            ListCognitiveDlqCommand(board_id, limit=50, offset=0), actor=ACTOR, uow=uow
        )

    assert result.total == base_total == 3
    assert sorted(r.id for r in result.rows) == sorted(r.id for r in base_rows) == seeded


@pytest.mark.asyncio
async def test_list_cognitive_dlq_use_case_paginates() -> None:
    board_id = f"board-fu3b-{uuid.uuid4().hex[:8]}"
    await _seed_dlq(board_id, 5)

    async with _uowf()(actor=ACTOR) as uow:
        page = await ListCognitiveDlqUseCase().execute(
            ListCognitiveDlqCommand(board_id, limit=2, offset=0), actor=ACTOR, uow=uow
        )
    assert page.total == 5
    assert len(page.rows) == 2


def test_migrated_dlq_tool_has_no_inline_sql() -> None:
    import ast
    from pathlib import Path

    from okto_pulse.core.mcp import server as mcp_server

    tree = ast.parse(Path(mcp_server.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
            and node.name == "okto_pulse_kg_list_cognitive_dlq"
        ):
            names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
            assert "get_db_for_mcp" not in names
            assert "sa_select" not in names
            assert "get_unit_of_work_factory_for_mcp" in names
            return
    raise AssertionError("okto_pulse_kg_list_cognitive_dlq not found")
