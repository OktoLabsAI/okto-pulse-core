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
from typing import Any, TYPE_CHECKING, Protocol, runtime_checkable

from okto_pulse.core.domain.realm import RealmScope
from okto_pulse.core.repositories.interfaces.repositories import RepositoryCatalog
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog

if TYPE_CHECKING:
    # Type-only import to keep the persistence port free of a runtime dependency
    # on the application layer (which depends on persistence) — avoids a cycle.
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.ports.semantic_subject_projection import (
        SemanticAssessmentV2PersistencePort,
        SemanticAssessmentV2ReadPort,
        SemanticAssessmentV2CapabilityPort,
        SemanticSubjectProjectionPort,
    )


class ConsistentReadContractError(RuntimeError):
    """A UoW cannot establish the requested transaction-wide read snapshot."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@runtime_checkable
class PulseUnitOfWork(RepositoryCatalog, Protocol):
    """A transactional unit of work exposing the repository catalog.

    Usable as an async context manager; ``__aexit__`` rolls back on error. The
    Concrete adapters may keep implementation-private transaction handles, but
    those handles are not part of this public contract.
    """

    services: ApplicationServiceCatalog
    realm_scope: RealmScope
    semantic_subject_projection: "SemanticSubjectProjectionPort"
    semantic_assessment_v2: "SemanticAssessmentV2PersistencePort"
    semantic_assessment_v2_reader: "SemanticAssessmentV2ReadPort"
    semantic_assessment_v2_capability: "SemanticAssessmentV2CapabilityPort"

    async def __aenter__(self) -> "PulseUnitOfWork": ...

    async def __aexit__(
        self, exc_type: object, exc: object, tb: object
    ) -> bool | None: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...

    async def begin_consistent_read(self) -> None:
        """Start (or reuse) one transaction-wide consistent read snapshot.

        Composite read use cases call this before their first repository or
        authorization lookup.  The adapter must configure the strongest
        transport-neutral equivalent of a repeatable snapshot *before* the
        first physical statement and fail closed when an already-active
        transaction cannot provide that guarantee.

        Calling this method again in the same active snapshot is idempotent.
        Core deliberately does not prescribe a database, dialect, SQL command
        or isolation-level spelling here.
        """

        ...

    async def synchronize(
        self,
        *,
        conflict_error: Exception | None = None,
    ) -> None:
        """Flush pending work and optionally translate a uniqueness conflict.

        ``conflict_error`` keeps persistence-specific exceptions out of Core
        use cases.  Adapters raise the supplied transport-neutral exception
        when the flush detects a creation race; existing callers omit it and
        retain the adapter's normal error behavior.
        """

        ...

    async def reload(
        self, entity: object, *, fields: tuple[str, ...] = ()
    ) -> None: ...

    async def close(self) -> None: ...


class UnitOfWorkFactory(Protocol):
    """Produces a :class:`PulseUnitOfWork` bound to a fresh session/transaction."""

    def resolve_realm_scope(self) -> RealmScope:
        """Resolve the request/task realm without exposing transport state.

        Local First editions return their local realm. A SaaS adapter resolves
        the tenant from its request context and must fail closed when that
        context has not been established.
        """

        ...

    def __call__(
        self,
        *,
        realm_scope: RealmScope,
        actor: "ActorContext | None" = None,
    ) -> AbstractAsyncContextManager[PulseUnitOfWork]: ...

    def wrap(
        self,
        context: Any,
        *,
        realm_scope: RealmScope,
        actor: "ActorContext | None" = None,
    ) -> PulseUnitOfWork: ...
