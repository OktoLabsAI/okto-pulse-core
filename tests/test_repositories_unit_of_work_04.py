"""Spec #04 card 3af67ea7 — PulseUnitOfWork ports and terminal ORM boundary.

Proves the persistence ports are real and behavior-correct: the SQLAlchemy
UnitOfWork round-trips via the repositories (add/commit/get + rollback), is
realm-ready without enforcement, satisfies the protocols, and powers an existing
spec #09 use case through typed application capabilities. The ORM-return debt
ledger is asserted at its terminal zero budget.
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
from sqlalchemy_test_models import Board, Ideation, IdeationStatus, Spec
from okto_pulse.core.repositories import (
    ORM_BASE_CLASS_BASELINE,
    PulseUnitOfWork,
    RepositoryCatalog,
    is_orm_return_excepted,
)
from sqlalchemy_test_unit_of_work import (
    SQLAlchemyUnitOfWork,
    SQLAlchemyUnitOfWorkFactory,
)

ACTOR = "uow04-actor"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


# --------------------------------------------------------------------------- #
# ORM debt ledger (tr_cd0631cf / fr_802078e4)
# --------------------------------------------------------------------------- #


def test_orm_debt_baseline_and_exceptions():
    assert ORM_BASE_CLASS_BASELINE == 0
    for orm_type in (
        "okto_pulse.core.models.db.Board",
        "okto_pulse.core.models.db.Ideation",
        "okto_pulse.core.models.db.Spec",
        "okto_pulse.core.models.db.Card",
        "okto_pulse.core.models.db.Sprint",
    ):
        assert not is_orm_return_excepted(orm_type)
    assert not is_orm_return_excepted(
        "okto_pulse.core.models.db.Board",
        repository=(
            "okto_pulse.core.repositories.interfaces.repositories.BoardRepository"
        ),
    )


def test_base_class_count_matches_baseline():
    # No mapped class may be defined by the removed Core ORM module. Community
    # mappings loaded by the test harness do not count toward this boundary.
    from sqlalchemy_test_models import Base

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
# Typed catalog: a spec #09 use case runs unchanged on a real PulseUnitOfWork
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_uow_typed_catalog_powers_spec09_use_case(db_factory):
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
