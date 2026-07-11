"""Relational schema-lifecycle orchestrator seam (R01C FR3).

Mandatory composition seam. ``init_db`` delegates the relational schema
lifecycle (migrations -> ``Base.metadata.create_all`` -> seeds) to the edition
composition root. Core owns the ORM ``Base`` and domain models; concrete
schema/migration/bootstrap execution is Community-owned.

Fail-closed by design: if no orchestrator is registered, ``init_db`` raises
instead of running lifecycle SQL from core.

This is a leaf module: it imports only ``typing`` so it stays agnostic of the
SQLAlchemy ORM and of the relational provider (R01B owns
engine/session/pool/PRAGMA ownership; this seam never touches runtime creation).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value


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


#: Process-wide registered orchestrator. ``None`` => init_db fails closed.
_RUNTIME_KEY = "infra.schema_lifecycle.orchestrator"


def register_relational_schema_lifecycle_orchestrator(
    orchestrator: RelationalSchemaLifecycleOrchestrator,
) -> None:
    """Register the edition's schema-lifecycle orchestrator (composition root).

    Called by the edition composition root before ``init_db``.
    """
    register_runtime_value(_RUNTIME_KEY, orchestrator)


def resolve_relational_schema_lifecycle_orchestrator() -> (
    RelationalSchemaLifecycleOrchestrator | None
):
    """Return the registered orchestrator, or ``None`` when unregistered."""
    return resolve_runtime_value(_RUNTIME_KEY)


def reset_relational_schema_lifecycle_orchestrator() -> None:
    """Clear any registered orchestrator."""
    reset_runtime_values(_RUNTIME_KEY)
