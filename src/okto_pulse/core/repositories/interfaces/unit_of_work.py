"""UnitOfWork ports for the relational strangler (SaaS Refactor spec #04).

``PulseUnitOfWork`` groups the repositories in a single transaction and is the
ONE place the transaction boundary is controlled (commit/rollback) — pulling it
out of the API/MCP handlers. ``UnitOfWorkFactory`` is realm-ready by a
keyword-only ``realm_id`` and an optional transport-neutral ``actor``; this
phase creates NO column, filter or realm enforcement (fr_cbfcb1aa) — the
parameters are accepted and carried only.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from okto_pulse.core.repositories.interfaces.repositories import RepositoryCatalog

if TYPE_CHECKING:
    # Type-only import to keep the persistence port free of a runtime dependency
    # on the application layer (which depends on persistence) — avoids a cycle.
    from okto_pulse.core.application.use_cases.base import ActorContext


@runtime_checkable
class PulseUnitOfWork(RepositoryCatalog, Protocol):
    """A transactional unit of work exposing the repository catalog.

    Usable as an async context manager; ``__aexit__`` rolls back on error. The
    SQLAlchemy adapter additionally exposes a transitional ``session`` property
    so the still-service-delegating spec #09 use cases keep working until they
    migrate to the repositories.
    """

    async def __aenter__(self) -> "PulseUnitOfWork":
        ...

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool | None:
        ...

    async def commit(self) -> None:
        ...

    async def rollback(self) -> None:
        ...

    async def close(self) -> None:
        ...


class UnitOfWorkFactory(Protocol):
    """Produces a :class:`PulseUnitOfWork` bound to a fresh session/transaction."""

    def __call__(
        self,
        *,
        realm_id: str | None = None,
        actor: "ActorContext | None" = None,
    ) -> AbstractAsyncContextManager[PulseUnitOfWork]:
        ...
