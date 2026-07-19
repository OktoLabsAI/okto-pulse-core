import asyncio
from contextlib import asynccontextmanager

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


class FakeRuntime:
    engine = object()

    def __init__(self, session: FakeSession) -> None:
        self._session = session
        self.session_factory = lambda: session

    @asynccontextmanager
    async def transactional_session(self):
        try:
            yield self._session
            await self._session.commit()
        except BaseException:
            await self._session.rollback()
            raise
        finally:
            await self._session.close()

    @asynccontextmanager
    async def cancel_safe_session_scope(self, session_factory=None):
        yield (session_factory or self.session_factory)()


@pytest.mark.asyncio
async def test_get_db_delegates_cancelled_cleanup_to_runtime():
    session = FakeSession()
    previous_runtime = database.resolve_database_runtime()
    database.configure_database_runtime(runtime=FakeRuntime(session))

    try:
        dependency = database.get_db()
        yielded = await dependency.__anext__()

        assert yielded is session
        with pytest.raises(asyncio.CancelledError):
            await dependency.athrow(asyncio.CancelledError())
    finally:
        database.configure_database_runtime(runtime=previous_runtime)

    assert session.calls == ["rollback", "close"]


@pytest.mark.asyncio
async def test_get_db_session_delegates_cancelled_cleanup_to_runtime():
    session = FakeSession()
    previous_runtime = database.resolve_database_runtime()
    database.configure_database_runtime(runtime=FakeRuntime(session))

    try:
        with pytest.raises(asyncio.CancelledError):
            async with database.get_db_session() as yielded:
                assert yielded is session
                raise asyncio.CancelledError()
    finally:
        database.configure_database_runtime(runtime=previous_runtime)

    assert session.calls == ["rollback", "close"]
