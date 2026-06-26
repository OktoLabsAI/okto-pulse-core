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
    RelationalBoundaryReport,
    RelationalViolation,
    observe_relational_boundary_violations,
    relational_baseline_report,
    run_relational_boundary_gate,
)
from okto_pulse.core.repositories.sqlalchemy import (
    SQLAlchemyBoardRepository,
    SQLAlchemyIdeationRepository,
    SQLAlchemySpecRepository,
    SQLAlchemyUnitOfWork,
    SQLAlchemyUnitOfWorkFactory,
)

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
    "observe_relational_boundary_violations",
    "RelationalBoundaryReport",
    "RelationalViolation",
    "RELATIONAL_BASELINE",
    "METRIC_RELATIONAL_BOUNDARY_VIOLATIONS",
]
