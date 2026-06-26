"""Spec #04 card 3af67ea7 — PulseUnitOfWork ports + SQLAlchemy adapter + ORM debt.

Proves the persistence ports are real and behavior-correct: the SQLAlchemy
UnitOfWork round-trips via the repositories (add/commit/get + rollback), is
realm-ready without enforcement, satisfies the protocols, and — critically for
the strangler seam — powers an existing spec #09 use case unchanged via the
transitional ``session`` bridge. The ORM-return debt ledger is asserted against
the verified baseline.
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.application.use_cases import (
    ActorContext,
    CreateBoardCommand,
    CreateBoardUseCase,
)
from okto_pulse.core.models import BoardCreate
from okto_pulse.core.models.db import Board, Ideation, IdeationStatus, Spec
from okto_pulse.core.repositories import (
    ORM_BASE_CLASS_BASELINE,
    SESSION_BRIDGE_DEBT,
    PulseUnitOfWork,
    RepositoryCatalog,
    SQLAlchemyUnitOfWork,
    SQLAlchemyUnitOfWorkFactory,
    TransitionalDebt,
    is_orm_return_excepted,
)

ACTOR = "uow04-actor"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


# --------------------------------------------------------------------------- #
# ORM debt ledger (tr_cd0631cf / fr_802078e4)
# --------------------------------------------------------------------------- #


def test_orm_debt_baseline_and_exceptions():
    assert ORM_BASE_CLASS_BASELINE == 59
    # First-cut aggregates are registered ORM-return debt...
    assert is_orm_return_excepted("okto_pulse.core.models.db.Board")
    assert is_orm_return_excepted("okto_pulse.core.models.db.Ideation")
    assert is_orm_return_excepted("okto_pulse.core.models.db.Spec")
    # ...an un-migrated aggregate's ORM return is NOT excepted (gate must block it).
    assert not is_orm_return_excepted("okto_pulse.core.models.db.Card")
    assert not is_orm_return_excepted("okto_pulse.core.models.db.Sprint")
    # Pair-keyed: a non-migrated repo returning an already-excepted type is NOT
    # excepted (no false-negative for the boundary gate).
    board = "okto_pulse.core.models.db.Board"
    board_repo = "okto_pulse.core.repositories.interfaces.repositories.BoardRepository"
    card_repo = "okto_pulse.core.repositories.interfaces.repositories.CardRepository"
    assert is_orm_return_excepted(board, repository=board_repo)
    assert not is_orm_return_excepted(board, repository=card_repo)


def test_session_bridge_is_registered_debt():
    assert isinstance(SESSION_BRIDGE_DEBT, TransitionalDebt)
    assert SESSION_BRIDGE_DEBT.kind == "session_bridge"
    assert "SQLAlchemyUnitOfWork.session" in SESSION_BRIDGE_DEBT.location
    assert SESSION_BRIDGE_DEBT.owner
    assert SESSION_BRIDGE_DEBT.deadline
    assert SESSION_BRIDGE_DEBT.withdrawal_criterion


def test_base_class_count_matches_baseline():
    # Cross-check the documented baseline against the live mapped classes DEFINED
    # IN models/db.py (the spec scopes the baseline to that module). Filtering by
    # __module__ keeps the count stable against test-only models other tests
    # register on the shared Base. A model addition here surfaces the debt drift
    # (the gate card b37786d9 enforces it live).
    from okto_pulse.core.models.db import Base

    mapped = len(
        [
            m
            for m in Base.registry.mappers
            if m.class_.__module__ == "okto_pulse.core.models.db"
        ]
    )
    assert mapped == ORM_BASE_CLASS_BASELINE, (
        f"models/db.py has {mapped} mapped classes vs baseline "
        f"{ORM_BASE_CLASS_BASELINE}; re-baseline the ORM debt ledger."
    )


# --------------------------------------------------------------------------- #
# Transaction lifecycle — single rollback/close path, both entry styles
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_uow_rolls_back_on_exception_via_factory(db_factory):
    factory = SQLAlchemyUnitOfWorkFactory(db_factory)
    board_id = _id("board")
    with pytest.raises(RuntimeError):
        async with factory() as uow:
            await uow.boards.add(
                Board(id=board_id, name="Boom", owner_id=ACTOR, settings={})
            )
            raise RuntimeError("boom")
    async with factory() as verify:
        assert await verify.boards.get(board_id) is None


@pytest.mark.asyncio
async def test_direct_async_with_uow_commits_and_rolls_back(db_factory):
    # The port advertises `async with uow:`; the direct path must commit on
    # success and roll back + close on error (the same teardown as the factory).
    ok_id = _id("board")
    async with SQLAlchemyUnitOfWork(db_factory()) as uow:
        await uow.boards.add(Board(id=ok_id, name="Direct", owner_id=ACTOR, settings={}))
        await uow.commit()

    err_id = _id("board")
    with pytest.raises(RuntimeError):
        async with SQLAlchemyUnitOfWork(db_factory()) as uow:
            await uow.boards.add(
                Board(id=err_id, name="DirectBoom", owner_id=ACTOR, settings={})
            )
            raise RuntimeError("boom")

    async with SQLAlchemyUnitOfWorkFactory(db_factory)() as verify:
        assert await verify.boards.get(ok_id) is not None
        assert await verify.boards.get(err_id) is None


# --------------------------------------------------------------------------- #
# Protocol conformance + repository round-trip
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_uow_satisfies_protocols(db_factory):
    factory = SQLAlchemyUnitOfWorkFactory(db_factory)
    assert callable(factory)
    async with factory() as uow:
        assert isinstance(uow, PulseUnitOfWork)
        assert isinstance(uow, RepositoryCatalog)
        assert uow.boards is not None
        assert uow.ideations is not None
        assert uow.specs is not None


@pytest.mark.asyncio
async def test_board_repo_add_commit_get(db_factory):
    factory = SQLAlchemyUnitOfWorkFactory(db_factory)
    board_id = _id("board")
    async with factory() as uow:
        await uow.boards.add(Board(id=board_id, name="UoW04", owner_id=ACTOR, settings={}))
        # autoflush makes the pending add visible to the query within the txn
        within = await uow.boards.get(board_id)
        assert within is not None and within.name == "UoW04"
        await uow.commit()
    async with factory() as uow2:
        persisted = await uow2.boards.get(board_id)
        assert persisted is not None and persisted.name == "UoW04"


@pytest.mark.asyncio
async def test_ideation_and_spec_repo_get(db_factory):
    factory = SQLAlchemyUnitOfWorkFactory(db_factory)
    board_id, ideation_id, spec_id = _id("board"), _id("idea"), _id("spec")
    async with factory() as uow:
        await uow.boards.add(Board(id=board_id, name="UoW04", owner_id=ACTOR, settings={}))
        await uow.ideations.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="t",
                created_by=ACTOR,
                status=IdeationStatus.DRAFT,
            )
        )
        await uow.specs.add(Spec(id=spec_id, board_id=board_id, title="s", created_by=ACTOR))
        await uow.commit()
    async with factory() as uow2:
        assert await uow2.ideations.get(ideation_id) is not None
        assert await uow2.specs.get(spec_id) is not None
        assert await uow2.ideations.get("missing") is None


@pytest.mark.asyncio
async def test_uow_rollback_discards(db_factory):
    factory = SQLAlchemyUnitOfWorkFactory(db_factory)
    board_id = _id("board")
    async with factory() as uow:
        await uow.boards.add(Board(id=board_id, name="Rollback", owner_id=ACTOR, settings={}))
        await uow.rollback()
    async with factory() as uow2:
        assert await uow2.boards.get(board_id) is None


@pytest.mark.asyncio
async def test_realm_id_is_carried_not_enforced(db_factory):
    factory = SQLAlchemyUnitOfWorkFactory(db_factory)
    async with factory(realm_id="realm-1") as uow:
        assert uow.realm_id == "realm-1"  # realm-ready
        board_id = _id("board")
        # No realm column/filter applied this phase — a board persists & reads back
        # regardless of realm (enforcement is a separate axis, fr_cbfcb1aa).
        await uow.boards.add(Board(id=board_id, name="Realm", owner_id=ACTOR, settings={}))
        await uow.commit()
        assert await uow.boards.get(board_id) is not None


# --------------------------------------------------------------------------- #
# Strangler seam: a spec #09 use case runs unchanged on a real PulseUnitOfWork
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_uow_session_bridge_powers_spec09_use_case(db_factory):
    factory = SQLAlchemyUnitOfWorkFactory(db_factory)
    async with factory() as uow:
        result = await CreateBoardUseCase().execute(
            CreateBoardCommand(BoardCreate(name="Bridge04")),
            actor=ActorContext(ACTOR, "rest"),
            uow=uow,
        )
        board_id = result.board.id
        assert result.board.name == "Bridge04"
    async with factory() as uow2:
        assert await uow2.boards.get(board_id) is not None
