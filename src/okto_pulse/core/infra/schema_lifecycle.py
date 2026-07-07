"""Relational schema-lifecycle orchestrator seam (R01C FR3).

DORMANT composition seam. ``init_db`` owns the relational schema lifecycle
(migrations -> ``Base.metadata.create_all`` -> seeds) inline today. This module
lets the edition composition root *optionally* register an orchestrator that
takes that lifecycle over, so the concrete schema/migration/bootstrap ownership
can move to Community (FR3) WITHOUT changing core's public startup contract.

Fail-open by design: nothing is registered in production by this card (R01C
IMP1), so ``resolve_*`` returns ``None`` and ``init_db`` runs its unchanged
inline path. Activation — registering the Community
``RelationalSchemaMigrator``/``DataBootstrapper`` orchestrator — is R01C IMP2.

This is a leaf module: it imports only ``typing`` so it stays agnostic of the
SQLAlchemy ORM and of the relational provider (R01B owns
engine/session/pool/PRAGMA ownership; this seam never touches runtime creation).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class RelationalSchemaLifecycleOrchestrator(Protocol):
    """Duck-typed port that owns the relational schema lifecycle.

    A registered orchestrator fully replaces ``init_db``'s inline body: it must
    bring an empty database up to the same tables/indexes/columns and run the
    same idempotent migrations + seeds (AC2/AC3/TR1). Held opaquely — core never
    imports the concrete Community adapter.
    """

    async def initialize_schema(self) -> None:
        ...


#: Process-wide registered orchestrator. ``None`` => fail-open (inline default).
_orchestrator: RelationalSchemaLifecycleOrchestrator | None = None


def register_relational_schema_lifecycle_orchestrator(
    orchestrator: RelationalSchemaLifecycleOrchestrator,
) -> None:
    """Register the edition's schema-lifecycle orchestrator (composition root).

    Intentionally NOT called from production wiring in this card — registration
    is the IMP2 activation step. Tests use it to exercise the seam.
    """
    global _orchestrator
    _orchestrator = orchestrator


def resolve_relational_schema_lifecycle_orchestrator() -> (
    RelationalSchemaLifecycleOrchestrator | None
):
    """Return the registered orchestrator, or ``None`` (fail-open default)."""
    return _orchestrator


def reset_relational_schema_lifecycle_orchestrator() -> None:
    """Clear any registered orchestrator (restores the inline default)."""
    global _orchestrator
    _orchestrator = None
