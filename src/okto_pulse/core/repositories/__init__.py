"""Persistence layer for the agnostic core (SaaS Refactor spec #04).

Defines the repository / unit-of-work PORTS (``interfaces``), the default
SQLAlchemy Community ADAPTER (``sqlalchemy``) and the transitional ORM-return
debt ledger (``debt``). Use cases depend on the ports; downstream editions
(Community SQLite, SaaS Postgres) supply the adapter. No schema, migration or
realm enforcement is introduced here (those are separate axes).
"""

from __future__ import annotations

from okto_pulse.core.repositories.debt import (
    ORM_BASE_CLASS_BASELINE,
    ORM_RETURN_DEBT,
    SESSION_BRIDGE_DEBT,
    OrmReturnDebt,
    TransitionalDebt,
    is_orm_return_excepted,
)
from okto_pulse.core.repositories.interfaces import (
    BoardRepository,
    IdeationRepository,
    PulseUnitOfWork,
    RepositoryCatalog,
    SpecRepository,
    UnitOfWorkFactory,
)
from okto_pulse.core.repositories.relational_boundary_gate import (
    METRIC_RELATIONAL_BOUNDARY_VIOLATIONS,
    RELATIONAL_BASELINE,
    RELATIONAL_BASELINE_R01B,
    RELATIONAL_COVERAGE_BASELINE,
    RELATIONAL_COVERAGE_SNAPSHOT_R01B,
    RelationalBoundaryReport,
    RelationalViolation,
    observe_relational_boundary_violations,
    relational_baseline_report,
    relational_coverage_counts,
    relational_coverage_drift,
    run_relational_boundary_gate,
)
from okto_pulse.core.repositories.sqlalchemy import (
    SQLAlchemyBoardRepository,
    SQLAlchemyIdeationRepository,
    SQLAlchemySpecRepository,
    SQLAlchemyUnitOfWork,
    SQLAlchemyUnitOfWorkFactory,
)
from okto_pulse.core.services.resource_gate_contracts import (
    ENTITY_TYPES,
    RESOURCE_TYPES,
    SOURCE_CHANNELS,
    EntityType,
    ResourceGateError,
    ResourceGateJustificationRequired,
    ResourceGateNotFound,
    ResourceGateViolation,
    ResourceState,
    ResourceType,
    SourceChannel,
)
from okto_pulse.core.repositories.sqlalchemy.runtime_settings_service import (
    DECAY_TICK_KEYS,
    EVENT_QUEUE_KEYS,
    GRAPH_DB_KEYS,
    RUNTIME_KEYS,
    SETTINGS_RUNTIME_EFFECT_PORTS,
    AppSetting,
    ConfigChangeBlocked,
    _apply_live_tick_settings,
    _resolve_legacy_env_aliases,
    apply_persisted_settings_to_core_settings,
    apply_tick_runtime_effects,
    get_runtime_settings,
    put_runtime_settings,
)
from okto_pulse.core.repositories.sqlalchemy.traceability_read_model import (
    TraceabilityReadError,
    build_lineage_graph,
    build_traceability_report,
    resolve_lineage_root,
    resolve_root_ideation_id,
    spec_coverage_summary,
)


def sqlalchemy_resource_gate_service_class() -> type:
    """Return the adapter-owned Resource Gate service class lazily."""
    from okto_pulse.core.repositories.sqlalchemy.resource_gate_service import (
        ResourceGateService,
    )

    return ResourceGateService


__all__ = [
    # ports
    "BoardRepository",
    "IdeationRepository",
    "SpecRepository",
    "RepositoryCatalog",
    "PulseUnitOfWork",
    "UnitOfWorkFactory",
    # adapter
    "SQLAlchemyBoardRepository",
    "SQLAlchemyIdeationRepository",
    "SQLAlchemySpecRepository",
    "SQLAlchemyUnitOfWork",
    "SQLAlchemyUnitOfWorkFactory",
    "ENTITY_TYPES",
    "RESOURCE_TYPES",
    "SOURCE_CHANNELS",
    "EntityType",
    "ResourceGateError",
    "ResourceGateJustificationRequired",
    "ResourceGateNotFound",
    "ResourceGateViolation",
    "ResourceState",
    "ResourceType",
    "SourceChannel",
    "DECAY_TICK_KEYS",
    "EVENT_QUEUE_KEYS",
    "GRAPH_DB_KEYS",
    "RUNTIME_KEYS",
    "SETTINGS_RUNTIME_EFFECT_PORTS",
    "AppSetting",
    "ConfigChangeBlocked",
    "_apply_live_tick_settings",
    "_resolve_legacy_env_aliases",
    "apply_persisted_settings_to_core_settings",
    "apply_tick_runtime_effects",
    "get_runtime_settings",
    "put_runtime_settings",
    "TraceabilityReadError",
    "build_lineage_graph",
    "build_traceability_report",
    "resolve_lineage_root",
    "resolve_root_ideation_id",
    "sqlalchemy_resource_gate_service_class",
    "spec_coverage_summary",
    # debt ledger
    "ORM_BASE_CLASS_BASELINE",
    "ORM_RETURN_DEBT",
    "OrmReturnDebt",
    "SESSION_BRIDGE_DEBT",
    "TransitionalDebt",
    "is_orm_return_excepted",
    # relational boundary gate
    "run_relational_boundary_gate",
    "relational_baseline_report",
    "relational_coverage_counts",
    "relational_coverage_drift",
    "observe_relational_boundary_violations",
    "RelationalBoundaryReport",
    "RelationalViolation",
    "RELATIONAL_BASELINE",
    "RELATIONAL_BASELINE_R01B",
    "RELATIONAL_COVERAGE_BASELINE",
    "RELATIONAL_COVERAGE_SNAPSHOT_R01B",
    "METRIC_RELATIONAL_BOUNDARY_VIOLATIONS",
]
