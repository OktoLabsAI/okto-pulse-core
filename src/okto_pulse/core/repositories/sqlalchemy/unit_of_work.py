"""SQLAlchemy adapter for PulseUnitOfWork / UnitOfWorkFactory (spec #04).

``SQLAlchemyUnitOfWork`` wraps an ``AsyncSession`` by composition, owns the
transaction boundary (commit/rollback/close) and exposes the repository catalog.
It also exposes a transitional ``session`` property so the spec #09 use cases
(which still delegate to the existing services via ``session_of``) keep working
when handed a real unit of work — the strangler seam.

``SQLAlchemyUnitOfWorkFactory`` is realm-ready: ``realm_id``/``actor`` are
accepted and carried, but NO realm filter/enforcement is applied this phase
(fr_cbfcb1aa). It produces a fresh session+UoW per ``__call__`` as an async
context manager and closes the session on exit.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.repositories.sqlalchemy.repositories import (
    SQLAlchemyBoardRepository,
    SQLAlchemyIdeationRepository,
    SQLAlchemySpecRepository,
)

if TYPE_CHECKING:
    from okto_pulse.core.application.use_cases.base import ActorContext


class SQLAlchemyUnitOfWork:
    """PulseUnitOfWork backed by a SQLAlchemy AsyncSession."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        realm_id: str | None = None,
        actor: "ActorContext | None" = None,
    ) -> None:
        self._session = session
        # realm-ready, NOT enforced this phase (fr_cbfcb1aa).
        self.realm_id = realm_id
        self.actor = actor
        self.boards = SQLAlchemyBoardRepository(session)
        self.ideations = SQLAlchemyIdeationRepository(session)
        self.specs = SQLAlchemySpecRepository(session)

    @property
    def session(self) -> AsyncSession:
        """Transitional bridge: the spec #09 use cases still delegate to services
        via a session (``session_of``). Removed when those flows migrate to the
        repositories."""
        return self._session

    async def __aenter__(self) -> "SQLAlchemyUnitOfWork":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        # Single, entry-style-independent teardown: roll back on error and ALWAYS
        # close the session. The factory context delegates here, and a direct
        # `async with uow:` reaches the same path — so neither style leaks the
        # connection (the port docstring advertises both).
        try:
            if exc is not None:
                await self.rollback()
        finally:
            await self.close()
        return None

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    async def close(self) -> None:
        await self._session.close()


class _UnitOfWorkContext:
    """Async context manager that creates a session + UoW and delegates teardown
    to the UoW, so the rollback/close path is identical whether the consumer
    enters via the factory or via ``async with uow:`` directly (one path)."""

    def __init__(
        self, session_factory: Any, *, realm_id: str | None, actor: "ActorContext | None"
    ) -> None:
        self._session_factory = session_factory
        self._realm_id = realm_id
        self._actor = actor
        self._uow: SQLAlchemyUnitOfWork | None = None

    async def __aenter__(self) -> SQLAlchemyUnitOfWork:
        session = self._session_factory()
        self._uow = SQLAlchemyUnitOfWork(
            session, realm_id=self._realm_id, actor=self._actor
        )
        return self._uow

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._uow is not None:
            await self._uow.__aexit__(exc_type, exc, tb)
        return None


class SQLAlchemyUnitOfWorkFactory:
    """UnitOfWorkFactory producing SQLAlchemy-backed units of work."""

    def __init__(self, session_factory: Any) -> None:
        self._session_factory = session_factory

    def __call__(
        self,
        *,
        realm_id: str | None = None,
        actor: "ActorContext | None" = None,
    ) -> AbstractAsyncContextManager["SQLAlchemyUnitOfWork"]:
        return _UnitOfWorkContext(
            self._session_factory, realm_id=realm_id, actor=actor
        )

    def wrap(
        self,
        session: AsyncSession,
        *,
        realm_id: str | None = None,
        actor: "ActorContext | None" = None,
    ) -> "SQLAlchemyUnitOfWork":
        """Request-scoped bridge (R01B FR3): wrap an EXTERNALLY-owned session
        (the REST ``Depends(get_db)`` session) in a unit of work WITHOUT taking
        over its lifecycle. The caller (``get_db``) still closes the session; the
        returned UoW is used as a plain object (the use case commits/rolls back),
        NOT entered as an ``async with`` context. Mirrors the historical
        ``SQLAlchemyUnitOfWork(db)`` the REST dependency returned."""
        return SQLAlchemyUnitOfWork(session, realm_id=realm_id, actor=actor)
