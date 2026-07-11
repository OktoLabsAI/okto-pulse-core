from __future__ import annotations

import ast
from pathlib import Path

import pytest

from okto_pulse.core.domain.entities import Board
from okto_pulse.core.domain.realm import RealmScope
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.testing.fake_saas_uow import FakeSaaSUnitOfWorkFactory


@pytest.mark.asyncio
async def test_f02_fake_uow_commits_once_and_isolates_transactions() -> None:
    factory = FakeSaaSUnitOfWorkFactory()
    realm = RealmScope.tenant("realm-a")
    board = Board(id="board-a", name="A", owner_id="owner")

    async with factory(realm_scope=realm) as writer:
        assert isinstance(writer, PulseUnitOfWork)
        assert not hasattr(writer, "session")
        await writer.boards.add(board)

        async with factory(realm_scope=realm) as concurrent_reader:
            assert await concurrent_reader.boards.get(board.id) is None

        await writer.commit()

    async with factory(realm_scope=realm) as committed_reader:
        assert await committed_reader.boards.get(board.id) == board

    assert factory.created[0].commit_calls == 1
    assert factory.created[0].rollback_calls == 0


@pytest.mark.asyncio
async def test_f02_fake_uow_rolls_back_each_modeled_failure() -> None:
    factory = FakeSaaSUnitOfWorkFactory()
    realm = RealmScope.tenant("realm-a")

    with pytest.raises(RuntimeError, match="modeled failure"):
        async with factory(realm_scope=realm) as uow:
            await uow.boards.add(Board(id="lost", name="Lost", owner_id="owner"))
            raise RuntimeError("modeled failure")

    failed = factory.created[0]
    assert failed.commit_calls == 0
    assert failed.rollback_calls == 1
    assert failed.close_calls == 1
    async with factory(realm_scope=realm) as reader:
        assert await reader.boards.get("lost") is None


def test_f02_fake_uow_has_no_orm_or_local_first_dependency() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "testing"
        / "fake_saas_uow.py"
    )
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules = {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    modules.update(
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    )
    assert not any(module.startswith("sqlalchemy") for module in modules)
    assert not any(module.startswith("okto_pulse.community") for module in modules)
