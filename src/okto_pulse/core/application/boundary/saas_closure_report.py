"""F16 canonical ownership matrix and zero-budget closure report.

Core owns the scanner and report model but receives Community audit projections
as data. Importing this module never imports an edition package.
"""

from __future__ import annotations

import ast
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping, Sequence

from .af35_s5_relational_final_gate import run_af35_s5_relational_final_gate
from .conformance_matrix import build_conformance_matrix
from .dependency_ledger import CANONICAL_TEMPORARY_EXCEPTION_TOKENS
from .distribution_dependency_ownership import (
    COMMUNITY_DISTRIBUTION,
    CORE_DISTRIBUTION,
    DistributionDependency,
    audit_distribution_dependencies,
    build_distribution_dependency_ledger,
)
from .gates import IMPORT_BOUNDARY_BASELINE_LEDGER
from .graph_runtime_surface_gate import LEGACY_GRAPH_RUNTIME_COMPATIBILITY_LEDGER
from .layer_resolver import LayerResolver
from .rebuild_audit_storage_gate import (
    rebuild_audit_storage_fallback_ledger,
    run_rebuild_audit_storage_gate,
)
from .singleton_gate import (
    AntiSingletonGate,
    AntiSingletonGateInput,
    BASELINE_SINGLETONS,
)

REPORT_VERSION = "F16.1"
F16_IMPLEMENTATION_CARD = "675c43ee-7d91-4cc3-8f87-44eeb293f90c"
README_BEGIN = "<!-- F16-SAAS-CLOSURE:BEGIN -->"
README_END = "<!-- F16-SAAS-CLOSURE:END -->"

OwnershipSeverity = Literal["info", "warning", "blocking"]


@dataclass(frozen=True, slots=True)
class OwnershipRow:
    source_owner: str
    surface: str
    source: str
    target: str
    target_owner: str
    classification: str
    severity: OwnershipSeverity
    location: str


@dataclass(frozen=True, slots=True)
class TransitionalBudget:
    key: str
    current: int
    limit: int
    implementation_card_id: str
    removal_criterion: str
    expires_on: str | None = None

    @property
    def ok(self) -> bool:
        if self.current > self.limit:
            return False
        return (
            self.expires_on is None
            or date.fromisoformat(self.expires_on) >= date.today()
        )


@dataclass(frozen=True, slots=True)
class ClosureFinding:
    code: str
    surface: str
    location: str
    detail: str


@dataclass(frozen=True, slots=True)
class SaaSClosureReport:
    version: str
    rows: tuple[OwnershipRow, ...]
    budgets: tuple[TransitionalBudget, ...]
    findings: tuple[ClosureFinding, ...]
    evidence: Mapping[str, Any]

    @property
    def blockers(self) -> tuple[ClosureFinding, ...]:
        return self.findings

    @property
    def ok(self) -> bool:
        return not self.findings and all(budget.ok for budget in self.budgets)

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "ok": self.ok,
            "rows": [asdict(row) for row in self.rows],
            "budgets": [{**asdict(budget), "ok": budget.ok} for budget in self.budgets],
            "findings": [asdict(finding) for finding in self.findings],
            "evidence": dict(self.evidence),
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), indent=2, sort_keys=True)


def _core_package_root(repo: Path) -> Path:
    return repo / "src" / "okto_pulse" / "core"


def _import_targets(node: ast.AST) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if isinstance(node, ast.Import):
        return tuple((alias.name, ()) for alias in node.names)
    if isinstance(node, ast.ImportFrom):
        target = "." * node.level + (node.module or "")
        return ((target, tuple(alias.name for alias in node.names)),)
    return ()


def _distribution_by_import_token(
    ledger: Sequence[DistributionDependency],
) -> dict[str, DistributionDependency]:
    result: dict[str, DistributionDependency] = {}
    for entry in ledger:
        for token in entry.import_tokens:
            current = result.get(token)
            if current is None or entry.declared_by == CORE_DISTRIBUTION:
                result[token] = entry
    return result


def audit_core_import_ownership(
    core_repo: Path,
    *,
    dependency_ledger: Sequence[DistributionDependency] | None = None,
) -> tuple[OwnershipRow, ...]:
    """Classify every absolute/relative import statement in productive Core."""

    package_root = _core_package_root(core_repo)
    ledger = tuple(dependency_ledger or build_distribution_dependency_ledger())
    dependency_by_token = _distribution_by_import_token(ledger)
    resolver = LayerResolver()
    rows: list[OwnershipRow] = []

    for path in sorted(package_root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        relative = path.relative_to(core_repo / "src").as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            for target, symbols in _import_targets(node):
                line = int(getattr(node, "lineno", 1))
                location = f"{relative}:{line}"
                display_target = target
                if symbols:
                    display_target += ":" + ",".join(symbols)
                root = target.lstrip(".").split(".", 1)[0]
                if target.startswith("."):
                    owner = "core"
                    classification = "core_internal"
                    severity: OwnershipSeverity = "info"
                elif root in sys.stdlib_module_names:
                    owner = "python-stdlib"
                    classification = "stdlib"
                    severity = "info"
                elif target == "okto_pulse.community" or target.startswith(
                    "okto_pulse.community."
                ):
                    owner = "community"
                    classification = "edition_implementation_reach_in"
                    severity = "blocking"
                elif target == "okto_pulse" or target.startswith("okto_pulse.core"):
                    resolution = resolver.resolve_module(target.split(":", 1)[0])
                    owner = f"core:{resolution.layer}"
                    classification = "core_internal"
                    severity = "info"
                else:
                    entry = dependency_by_token.get(root)
                    if entry is None:
                        owner = "unowned"
                        classification = "unowned_external_import"
                        severity = "blocking"
                    elif entry.declared_by != CORE_DISTRIBUTION:
                        owner = entry.source_owner
                        classification = "edition_dependency_in_core"
                        severity = "blocking"
                    else:
                        owner = "core"
                        classification = "core_dependency"
                        severity = "info"
                rows.append(
                    OwnershipRow(
                        source_owner="core",
                        surface="source_import",
                        source=relative,
                        target=display_target,
                        target_owner=owner,
                        classification=classification,
                        severity=severity,
                        location=location,
                    )
                )
    return tuple(sorted(rows, key=_row_key))


def _distribution_rows(
    ledger: Sequence[DistributionDependency],
) -> tuple[OwnershipRow, ...]:
    rows = []
    for entry in ledger:
        rows.append(
            OwnershipRow(
                source_owner=entry.source_owner,
                surface="direct_dependency",
                source=entry.declared_by,
                target=entry.normalized_distribution,
                target_owner=entry.source_owner,
                classification="owned_dependency",
                severity="info",
                location=entry.source_paths[0],
            )
        )
    return tuple(sorted(rows, key=_row_key))


def _community_rows(report: Mapping[str, Any] | None) -> tuple[OwnershipRow, ...]:
    if report is None:
        return ()
    rows: list[OwnershipRow] = []
    for entry in report.get("full_inventory", ()):
        classification = str(entry.get("classification") or "unclassified")
        severity: OwnershipSeverity = (
            "info" if classification == "public_contract" else "blocking"
        )
        symbols = ",".join(str(value) for value in entry.get("symbols", ()))
        target = str(entry.get("module") or "")
        if symbols:
            target += ":" + symbols
        rows.append(
            OwnershipRow(
                source_owner="community",
                surface="community_to_core_import",
                source=str(entry.get("file_path") or ""),
                target=target,
                target_owner="core:public_contract"
                if severity == "info"
                else "core:private",
                classification=classification,
                severity=severity,
                location=f"{entry.get('file_path', '')}:{entry.get('line', 1)}",
            )
        )
    return tuple(sorted(rows, key=_row_key))


def _row_key(row: OwnershipRow) -> tuple[str, str, str, str, str]:
    return (row.surface, row.source, row.location, row.target, row.classification)


def _projection_failures(
    name: str,
    report: Mapping[str, Any] | None,
    fields: Iterable[str],
) -> list[ClosureFinding]:
    if report is None:
        return [
            ClosureFinding(
                "edition_projection_missing",
                name,
                name,
                f"The {name} projection is required and was not supplied.",
            )
        ]
    findings: list[ClosureFinding] = []
    for field in fields:
        value = report.get(field)
        if value:
            findings.append(
                ClosureFinding(
                    f"{name}_{field}",
                    name,
                    field,
                    f"{len(value)} unresolved item(s).",
                )
            )
    if not report.get("ok", False) and not findings:
        findings.append(
            ClosureFinding(
                f"{name}_not_ok",
                name,
                name,
                "Edition projection reported non-conformance.",
            )
        )
    return findings


def build_transitional_budgets(
    *,
    community_import_report: Mapping[str, Any] | None,
    community_provenance_report: Mapping[str, Any] | None,
    af35_residue_count: int,
) -> tuple[TransitionalBudget, ...]:
    import_occurrences = int((community_import_report or {}).get("occurrence_count", 0))
    bridge_count = int((community_provenance_report or {}).get("bridge_count", 0))
    values = (
        ("import_boundary_baseline", len(IMPORT_BOUNDARY_BASELINE_LEDGER)),
        ("singleton_baseline", len(BASELINE_SINGLETONS)),
        ("dependency_temporary_exceptions", len(CANONICAL_TEMPORARY_EXCEPTION_TOKENS)),
        ("graph_runtime_compatibility", len(LEGACY_GRAPH_RUNTIME_COMPATIBILITY_LEDGER)),
        (
            "rebuild_artifact_compatibility",
            len(rebuild_audit_storage_fallback_ledger()),
        ),
        ("community_private_reach_ins", import_occurrences),
        ("community_adapter_bridges", bridge_count),
        ("af35_relational_residue", af35_residue_count),
    )
    return tuple(
        TransitionalBudget(
            key=key,
            current=current,
            limit=0,
            implementation_card_id=F16_IMPLEMENTATION_CARD,
            removal_criterion=f"Remove every {key} allowance before F16 completion.",
        )
        for key, current in values
    )


def build_saas_closure_report(
    *,
    core_repo: str | Path,
    community_repo: str | Path,
    community_import_report: Mapping[str, Any] | None,
    community_provenance_report: Mapping[str, Any] | None,
    core_wheel: str | Path | None = None,
    community_wheel: str | Path | None = None,
) -> SaaSClosureReport:
    core = Path(core_repo).resolve()
    community = Path(community_repo).resolve()
    ledger = build_distribution_dependency_ledger()
    rows = list(audit_core_import_ownership(core, dependency_ledger=ledger))
    rows.extend(_distribution_rows(ledger))
    rows.extend(_community_rows(community_import_report))
    findings: list[ClosureFinding] = []

    distribution = audit_distribution_dependencies(
        core_repo=core,
        community_repo=community,
        core_wheel=Path(core_wheel) if core_wheel else None,
        community_wheel=Path(community_wheel) if community_wheel else None,
        ledger=ledger,
    )
    findings.extend(
        ClosureFinding(item.code, item.surface, item.dependency, item.detail)
        for item in distribution.findings
    )

    matrix = build_conformance_matrix(
        repo_root=core,
        pyproject_path=core / "pyproject.toml",
        lock_path=core / "uv.lock",
        source_root=core / "src",
        audit_wheel=False,
    )
    for row in matrix.rows:
        if row.severity == "blocking":
            findings.append(
                ClosureFinding(
                    row.diagnostic_code or "conformance_matrix_blocker",
                    row.surface,
                    row.module,
                    row.remediation or row.symbol_or_dependency,
                )
            )

    singleton = AntiSingletonGate().run(
        AntiSingletonGateInput(source_root=core / "src")
    )
    if singleton.status != "passed":
        findings.append(
            ClosureFinding(
                "singleton_gate_not_terminal",
                "core_runtime_state",
                "okto_pulse.core",
                f"AntiSingletonGate status is {singleton.status}.",
            )
        )

    af35 = run_af35_s5_relational_final_gate(_core_package_root(core))
    residue_count = sum(row.observed_count for row in af35.rows)
    if not af35.ok:
        findings.append(
            ClosureFinding(
                "af35_relational_gate_failed",
                "core_relational",
                "okto_pulse.core",
                "AF35-S5 reported unresolved relational ownership findings.",
            )
        )

    rebuild_violations = run_rebuild_audit_storage_gate(_core_package_root(core))
    findings.extend(
        ClosureFinding(item.rule, "rebuild_storage", item.path, item.detail)
        for item in rebuild_violations
    )
    findings.extend(
        _projection_failures(
            "community_import_boundary",
            community_import_report,
            (
                "violations",
                "stale_ledger",
                "incomplete_ledger",
                "invalid_allowlist_entries",
                "baseline_violations",
            ),
        )
    )
    findings.extend(
        _projection_failures(
            "community_adapter_provenance",
            community_provenance_report,
            ("violations", "registration_violations", "parse_violations"),
        )
    )

    budgets = build_transitional_budgets(
        community_import_report=community_import_report,
        community_provenance_report=community_provenance_report,
        af35_residue_count=residue_count,
    )
    for budget in budgets:
        if not budget.ok:
            findings.append(
                ClosureFinding(
                    "transitional_budget_nonzero",
                    "budget",
                    budget.key,
                    f"Observed {budget.current}; terminal limit is {budget.limit}.",
                )
            )

    findings.extend(
        ClosureFinding(
            "ownership_row_blocking",
            row.surface,
            row.location,
            f"{row.target} is classified as {row.classification}.",
        )
        for row in rows
        if row.severity == "blocking"
    )
    ordered_rows = tuple(sorted(set(rows), key=_row_key))
    ordered_findings = tuple(
        sorted(
            set(findings),
            key=lambda item: (item.code, item.surface, item.location, item.detail),
        )
    )
    return SaaSClosureReport(
        version=REPORT_VERSION,
        rows=ordered_rows,
        budgets=budgets,
        findings=ordered_findings,
        evidence={
            "core_import_rows": sum(
                row.surface == "source_import" for row in ordered_rows
            ),
            "community_to_core_import_rows": sum(
                row.surface == "community_to_core_import" for row in ordered_rows
            ),
            "dependency_rows": sum(
                row.surface == "direct_dependency" for row in ordered_rows
            ),
            "conformance_matrix_ok": matrix.ok,
            "distribution_report_ok": distribution.ok,
            "singleton_gate_status": singleton.status,
            "af35_s5_ok": af35.ok,
            "rebuild_storage_violation_count": len(rebuild_violations),
        },
    )


def render_saas_closure_readme(report: SaaSClosureReport) -> str:
    """Render the exact README fragment verified by F16 CI."""

    lines = [
        README_BEGIN,
        "| F16 executable surface | Owner | Observed | Terminal target |",
        "| --- | --- | ---: | ---: |",
        f"| Core import rows | Core | {report.evidence['core_import_rows']} | classified |",
        f"| Community-to-Core import rows | Community | {report.evidence['community_to_core_import_rows']} | classified |",
        f"| Direct dependency rows | Distribution owner | {report.evidence['dependency_rows']} | classified |",
    ]
    for budget in report.budgets:
        lines.append(
            f"| `{budget.key}` budget | `{budget.implementation_card_id}` | "
            f"{budget.current} | {budget.limit} |"
        )
    lines.append(README_END)
    return "\n".join(lines)


def validate_saas_closure_readmes(
    report: SaaSClosureReport,
    *,
    core_readme: str,
    community_readme: str,
) -> tuple[ClosureFinding, ...]:
    expected = render_saas_closure_readme(report)
    findings: list[ClosureFinding] = []
    for name, text in (
        ("core README", core_readme),
        ("community README", community_readme),
    ):
        start = text.find(README_BEGIN)
        end = text.find(README_END)
        actual = "" if start < 0 or end < 0 else text[start : end + len(README_END)]
        if actual != expected:
            findings.append(
                ClosureFinding(
                    "readme_closure_matrix_mismatch",
                    "documentation",
                    name,
                    "README F16 matrix differs from the executable closure report.",
                )
            )
    return tuple(findings)


__all__ = [
    "ClosureFinding",
    "OwnershipRow",
    "SaaSClosureReport",
    "TransitionalBudget",
    "audit_core_import_ownership",
    "build_saas_closure_report",
    "build_transitional_budgets",
    "render_saas_closure_readme",
    "validate_saas_closure_readmes",
]
