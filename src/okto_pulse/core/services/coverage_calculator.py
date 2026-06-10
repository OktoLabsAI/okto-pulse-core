"""Pure objective coverage calculator for Analytics surfaces.

This module intentionally has no database, REST, MCP, or evaluation-gate
dependencies. It computes objective coverage percentages used by Analytics
read models only; manual spec/sprint evaluation scores remain separate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class CoverageDimensionConfig:
    """Map a coverage payload dimension into the objective calculator."""

    key: str
    pct_key: str
    total_key: str
    skip_key: str | None = None


@dataclass(frozen=True)
class CoverageDimensionInput:
    """Normalized objective coverage input for one category."""

    key: str
    pct: float
    total: int
    skipped: bool = False
    covered: int | None = None
    uncovered_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class CoverageDimensionResult:
    """Computed result for one objective coverage category."""

    key: str
    pct: float
    total: int
    active: bool
    skipped: bool
    blocking: bool
    covered: int | None = None
    uncovered_ids: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "key": self.key,
            "pct": self.pct,
            "total": self.total,
            "active": self.active,
            "skipped": self.skipped,
            "blocking": self.blocking,
        }
        if self.covered is not None:
            out["covered"] = self.covered
        if self.uncovered_ids:
            out["uncovered_ids"] = list(self.uncovered_ids)
        return out


@dataclass(frozen=True)
class ObjectiveCoverageResult:
    """Equal-weight objective coverage across active, non-skipped categories."""

    objective_pct: float
    active_category_count: int
    skipped_category_keys: tuple[str, ...]
    blocking: tuple[str, ...]
    dimensions: tuple[CoverageDimensionResult, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "objective_pct": self.objective_pct,
            "active_category_count": self.active_category_count,
            "skipped_category_keys": list(self.skipped_category_keys),
            "blocking": list(self.blocking),
            "dimensions": [d.as_dict() for d in self.dimensions],
        }


SPEC_OBJECTIVE_COVERAGE_DIMENSIONS: tuple[CoverageDimensionConfig, ...] = (
    CoverageDimensionConfig(
        "acceptance_criteria", "ac_coverage_pct", "ac_total", "skip_test_coverage"
    ),
    CoverageDimensionConfig("functional_requirements", "fr_coverage_pct", "fr_total"),
    CoverageDimensionConfig(
        "scenarios",
        "scenario_task_linkage_pct",
        "scenarios_total",
        "skip_test_coverage",
    ),
    CoverageDimensionConfig(
        "rules", "br_task_linkage_pct", "brs_total", "skip_rules_coverage"
    ),
    CoverageDimensionConfig("contracts", "contract_task_linkage_pct", "contracts_total"),
    CoverageDimensionConfig("trs", "tr_task_linkage_pct", "trs_total"),
    CoverageDimensionConfig(
        "decisions",
        "decisions_coverage_pct",
        "decisions_total",
        "skip_decisions_coverage",
    ),
    CoverageDimensionConfig("irs", "ir_task_linkage_pct", "irs_total", "skip_ir_coverage"),
    CoverageDimensionConfig("ors", "or_task_linkage_pct", "ors_total", "skip_or_coverage"),
)


def compute_objective_coverage(
    dimensions: Iterable[CoverageDimensionInput],
) -> ObjectiveCoverageResult:
    """Compute equal-weight objective coverage across active dimensions.

    A dimension is active only when it has total > 0 and is not skipped. Skipped
    dimensions are tracked separately and never counted as covered. If no
    active dimension exists, the objective coverage is 100% by convention,
    matching the existing Analytics saturation envelope behavior.
    """

    results: list[CoverageDimensionResult] = []
    active_pcts: list[float] = []
    skipped: list[str] = []
    blocking: list[str] = []

    for dim in dimensions:
        _validate_dimension(dim)
        active = dim.total > 0 and not dim.skipped
        is_blocking = active and dim.pct < 100.0
        if active:
            active_pcts.append(dim.pct)
            if is_blocking:
                blocking.append(dim.key)
        elif dim.skipped:
            skipped.append(dim.key)
        results.append(
            CoverageDimensionResult(
                key=dim.key,
                pct=round(float(dim.pct), 1),
                total=dim.total,
                active=active,
                skipped=dim.skipped,
                blocking=is_blocking,
                covered=dim.covered,
                uncovered_ids=dim.uncovered_ids,
            )
        )

    objective_pct = 100.0 if not active_pcts else round(sum(active_pcts) / len(active_pcts), 1)
    return ObjectiveCoverageResult(
        objective_pct=objective_pct,
        active_category_count=len(active_pcts),
        skipped_category_keys=tuple(skipped),
        blocking=tuple(blocking),
        dimensions=tuple(results),
    )


def compute_spec_objective_coverage(
    coverage: Mapping[str, Any],
    *,
    configs: Iterable[CoverageDimensionConfig] = SPEC_OBJECTIVE_COVERAGE_DIMENSIONS,
) -> ObjectiveCoverageResult:
    """Compute objective spec coverage from ``spec_coverage_summary`` output."""

    return compute_objective_coverage(
        _dimension_from_payload(coverage, config) for config in configs
    )


def spec_saturation_envelope_from_coverage(coverage: Mapping[str, Any]) -> dict[str, Any]:
    """Return the compact Analytics saturation envelope for spec coverage."""

    if not coverage:
        return {"pct": 100.0, "blocking": []}
    result = compute_spec_objective_coverage(coverage)
    return {"pct": result.objective_pct, "blocking": list(result.blocking)}


def _dimension_from_payload(
    coverage: Mapping[str, Any],
    config: CoverageDimensionConfig,
) -> CoverageDimensionInput:
    total = int(coverage.get(config.total_key, 0) or 0)
    pct = float(coverage.get(config.pct_key, 0) or 0)
    skipped = bool(config.skip_key and coverage.get(config.skip_key))
    return CoverageDimensionInput(
        key=config.key,
        pct=pct,
        total=total,
        skipped=skipped,
    )


def _validate_dimension(dim: CoverageDimensionInput) -> None:
    if dim.total < 0:
        raise ValueError(f"coverage dimension {dim.key!r} has negative total")
    if dim.covered is not None:
        if dim.covered < 0:
            raise ValueError(f"coverage dimension {dim.key!r} has negative covered")
        if dim.covered > dim.total:
            raise ValueError(f"coverage dimension {dim.key!r} has covered > total")
    if dim.pct < 0 or dim.pct > 100:
        raise ValueError(f"coverage dimension {dim.key!r} pct must be between 0 and 100")
