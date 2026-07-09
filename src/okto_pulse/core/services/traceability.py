"""Traceability service facade.

Relational reads live in the SQLAlchemy read-model adapter. This module keeps
the stable public service path and re-exports the neutral report functions used
by REST, MCP and tests.
"""

from __future__ import annotations

from okto_pulse.core.repositories import (
    TraceabilityReadError,
    build_lineage_graph,
    build_traceability_report,
    resolve_lineage_root,
    resolve_root_ideation_id,
    spec_coverage_summary,
)

__all__ = [
    "TraceabilityReadError",
    "build_lineage_graph",
    "build_traceability_report",
    "resolve_lineage_root",
    "resolve_root_ideation_id",
    "spec_coverage_summary",
]
