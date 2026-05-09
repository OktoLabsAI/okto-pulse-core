import asyncio

import pytest

from okto_pulse.core.infra import database


class FakeSession:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def commit(self) -> None:
        self.calls.append("commit")

    async def rollback(self) -> None:
        self.calls.append("rollback")

    async def close(self) -> None:
        self.calls.append("close")


@pytest.mark.asyncio
async def test_get_db_rolls_back_and_closes_on_cancelled_error(monkeypatch):
    session = FakeSession()
    monkeypatch.setattr(database, "get_session_factory", lambda: lambda: session)

    dependency = database.get_db()
    yielded = await dependency.__anext__()

    assert yielded is session
    with pytest.raises(asyncio.CancelledError):
        await dependency.athrow(asyncio.CancelledError())

    assert session.calls == ["rollback", "close"]


@pytest.mark.asyncio
async def test_get_db_session_rolls_back_and_closes_on_cancelled_error(monkeypatch):
    session = FakeSession()
    monkeypatch.setattr(database, "get_session_factory", lambda: lambda: session)

    with pytest.raises(asyncio.CancelledError):
        async with database.get_db_session() as yielded:
            assert yielded is session
            raise asyncio.CancelledError()

    assert session.calls == ["rollback", "close"]
