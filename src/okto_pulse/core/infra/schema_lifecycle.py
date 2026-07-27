"""Compatibility facade for the relational schema lifecycle port.

New consumers must import :mod:`okto_pulse.core.ports.schema_lifecycle`.
"""

from __future__ import annotations

from okto_pulse.core.ports.schema_lifecycle import (
    RelationalSchemaLifecycleOrchestrator,
    register_relational_schema_lifecycle_orchestrator,
    reset_relational_schema_lifecycle_orchestrator,
    resolve_relational_schema_lifecycle_orchestrator,
)

__all__ = [
    "RelationalSchemaLifecycleOrchestrator",
    "register_relational_schema_lifecycle_orchestrator",
    "reset_relational_schema_lifecycle_orchestrator",
    "resolve_relational_schema_lifecycle_orchestrator",
]
