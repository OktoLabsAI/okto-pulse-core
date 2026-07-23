"""SQLAlchemy adapter for PulseUnitOfWork / UnitOfWorkFactory (spec #04).

``SQLAlchemyUnitOfWork`` wraps an ``AsyncSession`` by composition, owns the
transaction boundary (commit/rollback/close) and exposes only typed repositories
and application capabilities. The native session remains implementation-private.

``SQLAlchemyUnitOfWorkFactory`` is realm-ready: ``realm_id``/``actor`` are
accepted and carried, but NO realm filter/enforcement is applied this phase
(fr_cbfcb1aa). It produces a fresh session+UoW per ``__call__`` as an async
context manager and closes the session on exit.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.application.service_catalog import (
    build_application_service_catalog,
)
from okto_pulse.core.ports.application_persistence import (
    ApplicationRecord,
    get_application_persistence_port,
)
from okto_pulse.core.domain.realm import RealmScope
from sqlalchemy_test_repositories import (
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
        realm_scope: RealmScope | None = None,
        realm_id: str | None = None,
        actor: "ActorContext | None" = None,
    ) -> None:
        self._session = session
        # realm-ready, NOT enforced this phase (fr_cbfcb1aa).
        self.realm_scope = realm_scope or (
            RealmScope.tenant(realm_id) if realm_id else RealmScope.local()
        )
        self.realm_id = self.realm_scope.realm_id
        self.actor = actor
        self.boards = SQLAlchemyBoardRepository(session, self.realm_scope)
        self.ideations = SQLAlchemyIdeationRepository(session, self.realm_scope)
        self.specs = SQLAlchemySpecRepository(session, self.realm_scope)
        self.services = build_application_service_catalog(session)

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
        await get_application_persistence_port().commit(self._session)

    async def rollback(self) -> None:
        await get_application_persistence_port().rollback(self._session)

    async def synchronize(
        self,
        *,
        conflict_error: Exception | None = None,
    ) -> None:
        del conflict_error
        await get_application_persistence_port().flush(self._session)

    async def reload(
        self, entity: object, *, fields: tuple[str, ...] = ()
    ) -> None:
        if isinstance(entity, ApplicationRecord):
            await get_application_persistence_port().refresh(self._session, entity)
            return
        await self._session.refresh(
            entity,
            attribute_names=list(fields) if fields else None,
        )

    async def close(self) -> None:
        await self._session.close()


class _UnitOfWorkContext:
    """Async context manager that creates a session + UoW and delegates teardown
    to the UoW, so the rollback/close path is identical whether the consumer
    enters via the factory or via ``async with uow:`` directly (one path)."""

    def __init__(
        self,
        session_factory: Any,
        *,
        realm_scope: RealmScope,
        actor: "ActorContext | None",
    ) -> None:
        self._session_factory = session_factory
        self._realm_scope = realm_scope
        self._actor = actor
        self._uow: SQLAlchemyUnitOfWork | None = None

    async def __aenter__(self) -> SQLAlchemyUnitOfWork:
        session = self._session_factory()
        self._uow = SQLAlchemyUnitOfWork(
            session,
            realm_scope=self._realm_scope,
            actor=self._actor,
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

    def resolve_realm_scope(self) -> RealmScope:
        return RealmScope.local()

    def __call__(
        self,
        *,
        realm_scope: RealmScope | None = None,
        realm_id: str | None = None,
        actor: "ActorContext | None" = None,
    ) -> AbstractAsyncContextManager["SQLAlchemyUnitOfWork"]:
        resolved_scope = realm_scope or (
            RealmScope.tenant(realm_id) if realm_id else self.resolve_realm_scope()
        )
        return _UnitOfWorkContext(
            self._session_factory,
            realm_scope=resolved_scope,
            actor=actor,
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
