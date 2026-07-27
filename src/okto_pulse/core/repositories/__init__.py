"""Lazy public facade for repository contracts and architecture gates."""

from __future__ import annotations

import importlib

_LAZY_EXPORTS: dict[str, str] = {
    # Pure contracts.
    "BoardRepository": "okto_pulse.core.repositories.interfaces",
    "IdeationRepository": "okto_pulse.core.repositories.interfaces",
    "SpecRepository": "okto_pulse.core.repositories.interfaces",
    "RepositoryCatalog": "okto_pulse.core.repositories.interfaces",
    "PulseUnitOfWork": "okto_pulse.core.repositories.interfaces",
    "UnitOfWorkFactory": "okto_pulse.core.repositories.interfaces",
    # Resource-gate contracts.
    "ENTITY_TYPES": "okto_pulse.core.services.resource_gate_contracts",
    "RESOURCE_TYPES": "okto_pulse.core.services.resource_gate_contracts",
    "SOURCE_CHANNELS": "okto_pulse.core.services.resource_gate_contracts",
    "EntityType": "okto_pulse.core.services.resource_gate_contracts",
    "ResourceGateError": "okto_pulse.core.services.resource_gate_contracts",
    "ResourceGateJustificationRequired": "okto_pulse.core.services.resource_gate_contracts",
    "ResourceGateNotFound": "okto_pulse.core.services.resource_gate_contracts",
    "ResourceGateViolation": "okto_pulse.core.services.resource_gate_contracts",
    "ResourceState": "okto_pulse.core.services.resource_gate_contracts",
    "ResourceType": "okto_pulse.core.services.resource_gate_contracts",
    "SourceChannel": "okto_pulse.core.services.resource_gate_contracts",
    # Debt and gates.
    "ORM_BASE_CLASS_BASELINE": "okto_pulse.core.repositories.debt",
    "ORM_RETURN_DEBT": "okto_pulse.core.repositories.debt",
    "OrmReturnDebt": "okto_pulse.core.repositories.debt",
    "TransitionalDebt": "okto_pulse.core.repositories.debt",
    "is_orm_return_excepted": "okto_pulse.core.repositories.debt",
    "run_relational_boundary_gate": "okto_pulse.core.repositories.relational_boundary_gate",
    "relational_baseline_report": "okto_pulse.core.repositories.relational_boundary_gate",
    "relational_coverage_counts": "okto_pulse.core.repositories.relational_boundary_gate",
    "relational_coverage_drift": "okto_pulse.core.repositories.relational_boundary_gate",
    "observe_relational_boundary_violations": "okto_pulse.core.repositories.relational_boundary_gate",
    "RelationalBoundaryReport": "okto_pulse.core.repositories.relational_boundary_gate",
    "RelationalViolation": "okto_pulse.core.repositories.relational_boundary_gate",
    "RELATIONAL_BASELINE": "okto_pulse.core.repositories.relational_boundary_gate",
    "RELATIONAL_BASELINE_R01B": "okto_pulse.core.repositories.relational_boundary_gate",
    "RELATIONAL_COVERAGE_BASELINE": "okto_pulse.core.repositories.relational_boundary_gate",
    "RELATIONAL_COVERAGE_SNAPSHOT_R01B": "okto_pulse.core.repositories.relational_boundary_gate",
    "RELATIONAL_COVERAGE_SNAPSHOT_TERMINAL": "okto_pulse.core.repositories.relational_boundary_gate",
    "METRIC_RELATIONAL_BOUNDARY_VIOLATIONS": "okto_pulse.core.repositories.relational_boundary_gate",
}

__all__ = list(_LAZY_EXPORTS)


def __getattr__(name: str):
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *_LAZY_EXPORTS})
