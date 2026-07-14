"""Spec #09 card 9c1f5d62 — application use cases, ActorContext, UseCase protocol
and ApplicationPurityGate.

Proves the impl-card deliverables directly: the transport-free use cases are
behavior-equivalent to the existing service path, the purity gate enforces the
transport boundary (positive + negative), and the command validation reproduces
the REST handler's input checks. The GoldenInboundReplay / paired REST↔MCP
scenarios live in the dedicated test cards (77188b6f and the #09 test cards).
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.application.purity_gate import run_application_purity_gate
from okto_pulse.core.application.use_cases import (
    ActorContext,
    CommandValidationError,
    CreateBoardCommand,
    CreateBoardUseCase,
    EntityNotFoundError,
    MoveIdeationCommand,
    MoveIdeationUseCase,
    SubmitSpecValidationCommand,
    SubmitSpecValidationUseCase,
    UseCase,
    commit,
)
from okto_pulse.core.models import BoardCreate, IdeationMove
from sqlalchemy_test_models import Board, Ideation, IdeationStatus
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory

ACTOR = "uc09-actor"

_VALID_VALIDATION_PAYLOAD = {
    "completeness": 95,
    "completeness_justification": "complete enough",
    "assertiveness": 95,
    "assertiveness_justification": "assertive enough",
    "ambiguity": 5,
    "ambiguity_justification": "low ambiguity",
    "general_justification": "general justification well over twenty chars",
    "recommendation": "approve",
}


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _wrap_uow(db):
    return resolve_unit_of_work_factory().wrap(db)


# --------------------------------------------------------------------------- #
# ApplicationPurityGate (tr_391ea356, fr_a016174a, ac_763bea1f, ts_96698fd9)
# --------------------------------------------------------------------------- #


def test_purity_gate_clean_on_real_use_cases():
    report = run_application_purity_gate()
    assert report.ok, report.as_dict()
    assert report.scanned_files >= 4


def test_purity_gate_flags_transport_leaks(tmp_path):
    leaky = tmp_path / "bad_use_case.py"
    leaky.write_text(
        "from fastapi import Depends, Request\n"
        "from contextvars import ContextVar\n"
        "_key = ContextVar('key')\n"
        "def handler(r: Request): ...\n",
        encoding="utf-8",
    )
    report = run_application_purity_gate(tmp_path)
    assert report.ok is False
    symbols = {v.symbol for v in report.violations}
    assert any("fastapi" in s for s in symbols)
    assert any("Depends" in s for s in symbols)
    assert any("Request" in s for s in symbols)
    assert any("ContextVar" in s for s in symbols)
    # Every violation carries file, symbol, layer and remediation_hint (ts_96698fd9 THEN).
    for violation in report.violations:
        assert violation.layer == "application/use_cases"
        assert violation.file and violation.line >= 1
        assert violation.remediation_hint


# --------------------------------------------------------------------------- #
# UseCase protocol + opaque UnitOfWork contract (tr_3d5b5204, tr_b18aefe5)
# --------------------------------------------------------------------------- #


def test_use_cases_satisfy_protocol():
    assert isinstance(CreateBoardUseCase(), UseCase)
    assert isinstance(MoveIdeationUseCase(), UseCase)
    assert isinstance(SubmitSpecValidationUseCase(), UseCase)


def test_actor_context_is_transport_neutral_data():
    actor = ActorContext("user-1", "mcp", board_id="b1", realm_id=None, roles=("owner",))
    assert actor.actor_id == "user-1"
    assert actor.source == "mcp"
    assert actor.board_id == "b1"
    assert actor.roles == ("owner",)


@pytest.mark.asyncio
async def test_uow_commit_uses_only_the_typed_transaction_capability():
    class _FakeUow:
        def __init__(self):
            self.commits = 0

        async def commit(self):
            self.commits += 1

    uow = _FakeUow()
    assert not hasattr(uow, "session")
    await commit(uow)
    assert uow.commits == 1


# --------------------------------------------------------------------------- #
# Command validation reproduces the REST handler input checks
# --------------------------------------------------------------------------- #


def test_submit_spec_validation_command_validation():
    SubmitSpecValidationCommand("s1", _VALID_VALIDATION_PAYLOAD).validate()  # no raise

    with pytest.raises(CommandValidationError, match="Missing required fields"):
        SubmitSpecValidationCommand("s1", {"completeness": 95}).validate()

    with pytest.raises(CommandValidationError, match="recommendation must be"):
        SubmitSpecValidationCommand(
            "s1", {**_VALID_VALIDATION_PAYLOAD, "recommendation": "maybe"}
        ).validate()

    with pytest.raises(CommandValidationError, match="completeness_justification must be at least"):
        SubmitSpecValidationCommand(
            "s1", {**_VALID_VALIDATION_PAYLOAD, "completeness_justification": "short"}
        ).validate()

    with pytest.raises(CommandValidationError, match="general_justification must be at least"):
        SubmitSpecValidationCommand(
            "s1", {**_VALID_VALIDATION_PAYLOAD, "general_justification": "too short"}
        ).validate()


# --------------------------------------------------------------------------- #
# Behavior equivalence against a real session
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_create_board_use_case_persists_and_shapes(db_factory):
    async with db_factory() as db:
        result = await CreateBoardUseCase().execute(
            CreateBoardCommand(BoardCreate(name="UC09 Board")),
            actor=ActorContext(ACTOR, "rest"),
            uow=_wrap_uow(db),
        )
        board = result.board
        assert board is not None
        assert board.name == "UC09 Board"
        assert board.owner_id == ACTOR
        # Same post-processing as api/boards.py:create_board.
        assert board.agents == []
        assert board.values["settings"] is not None


@pytest.mark.asyncio
async def test_move_ideation_use_case_changes_status(db_factory):
    async with db_factory() as db:
        board_id = _id("board")
        ideation_id = _id("idea")
        db.add(Board(id=board_id, name="UC09", owner_id=ACTOR, settings={}))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="UC09 ideation",
                created_by=ACTOR,
                status=IdeationStatus.DRAFT,
            )
        )
        await db.flush()

        result = await MoveIdeationUseCase().execute(
            MoveIdeationCommand(ideation_id, IdeationMove(status=IdeationStatus.REVIEW)),
            actor=ActorContext(ACTOR, "rest"),
            uow=_wrap_uow(db),
        )
        assert result.ideation is not None
        assert result.ideation.status == IdeationStatus.REVIEW


@pytest.mark.asyncio
async def test_move_ideation_missing_raises_not_found(db_factory):
    async with db_factory() as db:
        with pytest.raises(EntityNotFoundError) as exc_info:
            await MoveIdeationUseCase().execute(
                MoveIdeationCommand("missing-id", IdeationMove(status=IdeationStatus.REVIEW)),
                actor=ActorContext(ACTOR, "rest"),
                uow=_wrap_uow(db),
            )
        assert exc_info.value.entity_type == "ideation"


@pytest.mark.asyncio
async def test_submit_spec_validation_missing_spec_raises_not_found(db_factory):
    async with db_factory() as db:
        with pytest.raises(EntityNotFoundError) as exc_info:
            await SubmitSpecValidationUseCase().execute(
                SubmitSpecValidationCommand("missing-spec", _VALID_VALIDATION_PAYLOAD),
                actor=ActorContext(ACTOR, "rest"),
                uow=_wrap_uow(db),
            )
        assert exc_info.value.entity_type == "spec"
