"""Port for edition-owned relational schema lifecycle orchestration.

``init_db`` delegates migrations, schema creation and data bootstrap to the
active edition. Core defines only this protocol and its runtime registration;
concrete SQL and bootstrap behavior remains outside Core.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)


@runtime_checkable
class RelationalSchemaLifecycleOrchestrator(Protocol):
    """Bring the active relational store to its ready application state."""

    async def initialize_schema(self) -> None:
        ...


# Retain the established key so the compatibility facade and existing runtime
# snapshots share one registration during migration to the ports namespace.
_RUNTIME_KEY = "infra.schema_lifecycle.orchestrator"


def register_relational_schema_lifecycle_orchestrator(
    orchestrator: RelationalSchemaLifecycleOrchestrator,
) -> None:
    """Register the schema lifecycle supplied by the edition composition root."""

    register_runtime_value(_RUNTIME_KEY, orchestrator)


def resolve_relational_schema_lifecycle_orchestrator() -> (
    RelationalSchemaLifecycleOrchestrator | None
):
    """Return the active edition orchestrator, if one was registered."""

    return resolve_runtime_value(_RUNTIME_KEY)


def reset_relational_schema_lifecycle_orchestrator() -> None:
    """Clear the active orchestrator, primarily for isolated composition tests."""

    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "RelationalSchemaLifecycleOrchestrator",
    "register_relational_schema_lifecycle_orchestrator",
    "reset_relational_schema_lifecycle_orchestrator",
    "resolve_relational_schema_lifecycle_orchestrator",
]
