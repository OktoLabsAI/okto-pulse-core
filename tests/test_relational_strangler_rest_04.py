"""Spec #04 card 416e317a — RelationalBoundaryGate negative + REST full-path.

ts_aa654a8a: the gate blocks direct relational coupling in a migrated use case,
reporting file/symbol/surface/remediation_hint.
ts_56f9dccb: a REST flow driven through the FULL relational path —
endpoint -> thin adapter -> transport-free use case -> UnitOfWorkFactory ->
SQLAlchemyUnitOfWork -> repository/adapter — preserves the Community-observable
behavior (payload + persisted state) vs the direct-service baseline, and the
migrated use case stays clean under the RelationalBoundaryGate. Uses the real
production components (CreateBoardUseCase + SQLAlchemyUnitOfWorkFactory +
BoardRepository); the thin adapter is constructed here, exactly as a migrated
handler would (sans the HTTP framework).
"""

from __future__ import annotations

import pytest

from okto_pulse.core.application.golden_inbound_replay import (
    GoldenInboundReplay,
    capture_outcome,
)
from okto_pulse.core.application.use_cases import (
    ActorContext,
    CreateBoardCommand,
    CreateBoardUseCase,
)
from okto_pulse.core.models import BoardCreate
from okto_pulse.core.repositories import (
    run_relational_boundary_gate,
)
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
from okto_pulse.core.services import BoardService
from okto_pulse.core.services.board_governance import BoardGovernanceService

ACTOR = "strangler04-actor"


def _norm_board(board) -> dict:
    return {
        "name": board.name,
        "owner_id": board.owner_id,
        "agents": getattr(board, "agents", None),
        "settings_present": getattr(board, "settings", None) is not None,
    }


async def _rest_full_path_create_board(uow_factory, *, name: str, user_id: str):
    """Thin adapter → use case → UnitOfWorkFactory → SQLAlchemyUnitOfWork → adapter."""
    actor = ActorContext(user_id, "rest")
    async with uow_factory(actor=actor) as uow:
        return await CreateBoardUseCase().execute(
            CreateBoardCommand(BoardCreate(name=name)), actor=actor, uow=uow
        )


async def _baseline_service_create_board(db_factory, *, name: str, user_id: str):
    async with db_factory() as db:
        service = BoardService(db)
        board = await service.create_board(user_id, BoardCreate(name=name), realm_id=None)
        await db.commit()
        board = await service.get_board(board.id)
        board.agents = []
        board.settings = BoardGovernanceService.normalize_settings(
            getattr(board, "settings", None)
        )
        return board


# --- ts_56f9dccb: full-path migration is behavior-equivalent to the baseline ---


@pytest.mark.asyncio
async def test_rest_full_path_create_board_equivalent_to_baseline(db_factory):
    factory = SQLAlchemyUnitOfWorkFactory(db_factory)
    name = "Strangler04 Board"
    after = await capture_outcome(
        lambda: _rest_full_path_create_board(factory, name=name, user_id=ACTOR),
        normalize=lambda r: _norm_board(r.board),
    )
    before = await capture_outcome(
        lambda: _baseline_service_create_board(db_factory, name=name, user_id=ACTOR),
        normalize=_norm_board,
    )
    report = GoldenInboundReplay("create_board", "rest").compare(before, after)
    assert report.equivalent, report.as_dict()
    assert after.payload["name"] == name
    assert after.payload["agents"] == []
    assert after.payload["settings_present"] is True


@pytest.mark.asyncio
async def test_rest_full_path_persists_and_reads_back_via_repository(db_factory):
    factory = SQLAlchemyUnitOfWorkFactory(db_factory)
    result = await _rest_full_path_create_board(
        factory, name="Strangler04 RT", user_id=ACTOR
    )
    board_id = result.board.id
    # Read back via the repository port on a fresh unit of work — the persistence
    # round-trip went endpoint -> use case -> UoW -> repository -> SQLAlchemy adapter.
    async with factory() as uow:
        persisted = await uow.boards.get(board_id)
        assert persisted is not None
        assert persisted.name == "Strangler04 RT"


def test_migrated_use_cases_pass_relational_gate():
    report = run_relational_boundary_gate()
    assert report.ok, report.as_dict()


# --- ts_aa654a8a: the gate blocks a use case with direct relational coupling ---


def test_gate_blocks_direct_relational_coupling(tmp_path):
    (tmp_path / "leaky_use_case.py").write_text(
        "from sqlalchemy.ext.asyncio import AsyncSession\n"
        "from okto_pulse.core.models.db import Board\n"
        "\n"
        "async def migrated(db: AsyncSession):\n"
        "    return Board\n",
        encoding="utf-8",
    )
    report = run_relational_boundary_gate(tmp_path)
    assert report.ok is False
    symbols = {v.symbol for v in report.violations}
    assert "AsyncSession" in symbols
    assert "orm:Board" in symbols
    # THEN: file, symbol, surface (camada) and remediation_hint present.
    for v in report.violations:
        assert v.file and v.line >= 1
        assert v.surface in ("rest", "mcp", "service")
        assert v.remediation_hint
