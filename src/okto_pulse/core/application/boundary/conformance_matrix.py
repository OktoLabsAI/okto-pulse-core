"""FCC-07A row-level conformance matrix for core dependency/import cleanup.

This module is an additive projection over existing boundary reports. It does
not replace ``dependency_conformance`` or the adapter readiness inventory; it
turns their structured outputs into one deterministic, reviewable matrix.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal

from .adapter_readiness_inventory import (
    REQUIRED_ADAPTER_KEYS,
    AdapterEvidence,
    AdapterInventoryEntry,
    AdapterReadinessVerdict,
    build_adapter_inventory,
    evaluate_adapter_readiness,
)
from .dependency_conformance import (
    AuditFinding,
    DependencyConformanceReport,
    _default_repo_root,
    _lock_core_direct_dependencies,
    _manifest_direct_dependencies,
    audit_dependency_conformance,
)
from .dependency_ledger import LedgerEntry, build_dependency_ledger, normalize_token
from .gates import ImportBoundaryGate, ImportBoundaryGateInput

MatrixClassification = Literal[
    "core_common",
    "community_owned",
    "future_adapter",
    "temporary_exception",
    "removed",
    "unknown",
    "test_only",
    "violation",
]
MatrixSeverity = Literal["blocking", "warning", "accepted", "info"]

TESTING_PROVIDER_PREFIX = "okto_pulse.core.kg.providers.testing"
CORE_COMMON_BASELINE_TOKENS: frozenset[str] = frozenset()
DEFERRED_RELATIONAL_SEAM_ADAPTER_KEYS = frozenset({"asyncpg_postgres_driver"})

# Bounded bridge from governed dependency tokens to the residual adapter inventory.
# Prefer inventory.packages when it already carries the token; these hints cover
# names whose current inventory evidence is semantic rather than package-derived.
_TOKEN_ADAPTER_HINTS: dict[str, tuple[str, ...]] = {
    "requests": ("local_telemetry_store",),
    "chardet": ("local_telemetry_store",),
    "apscheduler": ("singleton_scheduler_control",),
    "aiosqlite": ("asyncpg_postgres_driver",),
    "asyncpg": ("asyncpg_postgres_driver",),
    "sqlite3": ("board_source_store", "board_rebuild_ingestion_adapter"),
}


@dataclass(frozen=True)
class SourceImportReference:
    """A pre-parsed source import supplied by an existing gate or test harness."""

    imported: str
    location: str
    test_context: bool = False


@dataclass(frozen=True)
class ConformanceMatrixRow:
    """One row in the FCC-07A conformance matrix."""

    module: str
    symbol_or_dependency: str
    surface: str
    classification: MatrixClassification
    adapter_key: str | None
    owning_fcc_or_wave: str | None
    evidence_field_impact: str | None
    severity: MatrixSeverity
    remediation: str | None
    diagnostic_code: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "module": self.module,
            "symbol_or_dependency": self.symbol_or_dependency,
            "surface": self.surface,
            "classification": self.classification,
            "adapter_key": self.adapter_key,
            "owning_fcc_or_wave": self.owning_fcc_or_wave,
            "evidence_field_impact": self.evidence_field_impact,
            "severity": self.severity,
            "remediation": self.remediation,
            "diagnostic_code": self.diagnostic_code,
        }


@dataclass(frozen=True)
class ConformanceMatrixReport:
    """Deterministic, JSON-serialisable FCC-07A matrix report."""

    ok: bool
    rows: tuple[ConformanceMatrixRow, ...]
    dependency_report_ok: bool
    adapter_keys_required: tuple[str, ...]
    adapter_keys_covered: tuple[str, ...]
    adapter_keys_missing: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "dependency_report_ok": self.dependency_report_ok,
            "adapter_keys_required": list(self.adapter_keys_required),
            "adapter_keys_covered": list(self.adapter_keys_covered),
            "adapter_keys_missing": list(self.adapter_keys_missing),
            "rows": [row.as_dict() for row in self.rows],
        }


def build_conformance_matrix(
    *,
    repo_root: Path | None = None,
    pyproject_path: Path | None = None,
    lock_path: Path | None = None,
    source_root: Path | None = None,
    wheel_metadata_path: Path | None = None,
    audit_wheel: bool = True,
    dependency_report: DependencyConformanceReport | None = None,
    adapter_inventory: tuple[AdapterInventoryEntry, ...] | None = None,
    adapter_evidence: dict[str, AdapterEvidence] | None = None,
    source_import_references: Iterable[SourceImportReference] = (),
    required_adapter_keys: frozenset[str] = REQUIRED_ADAPTER_KEYS,
    include_import_boundary: bool = True,
    import_boundary_mode: str = "blocking",
) -> ConformanceMatrixReport:
    """Build the FCC-07A matrix from existing structured reports.

    The only source parser this function invokes is the existing
    ``audit_dependency_conformance`` / ``ImportBoundaryGate`` machinery. Optional
    ``source_import_references`` are already parsed references, used for
    sanctioned test-only provider classification without embedding that policy
    into the dependency auditor.
    """

    dep_report = dependency_report or audit_dependency_conformance(
        repo_root=repo_root,
        pyproject_path=pyproject_path,
        lock_path=lock_path,
        source_root=source_root,
        wheel_metadata_path=wheel_metadata_path,
        audit_wheel=audit_wheel,
    )
    inventory = adapter_inventory or build_adapter_inventory()
    evidence = adapter_evidence or {}
    ledger = build_dependency_ledger()
    ledger_by_token = _ledger_by_token(ledger)
    entries_by_key = {entry.adapter_key: entry for entry in inventory}
    token_to_adapters = _token_to_adapter_keys(inventory)

    rows: list[ConformanceMatrixRow] = []
    for finding in dep_report.findings:
        rows.extend(
            _rows_from_dependency_finding(
                finding,
                ledger_by_token=ledger_by_token,
                entries_by_key=entries_by_key,
                token_to_adapters=token_to_adapters,
            )
        )

    rows.extend(
        _rows_from_core_common_baseline(
            dep_report,
            repo_root=repo_root,
            pyproject_path=pyproject_path,
            lock_path=lock_path,
        )
    )
    rows.extend(_rows_from_removed_dependencies(dep_report, ledger_by_token=ledger_by_token))

    if include_import_boundary:
        boundary_report = ImportBoundaryGate().run(
            ImportBoundaryGateInput(source_root=source_root, mode=import_boundary_mode)
        )
        rows.extend(
            _rows_from_import_boundary(
                boundary_report.evidence.get("violations") or (),
                entries_by_key=entries_by_key,
                token_to_adapters=token_to_adapters,
            )
        )

    for reference in source_import_references:
        rows.append(
            _row_from_source_reference(
                reference,
                entries_by_key=entries_by_key,
                token_to_adapters=token_to_adapters,
            )
        )

    rows.extend(
        _rows_from_adapter_inventory(
            inventory,
            evidence_by_key=evidence,
            ledger_by_token=ledger_by_token,
        )
    )

    missing = tuple(
        sorted(required_adapter_keys.difference({entry.adapter_key for entry in inventory}))
    )
    for adapter_key in missing:
        rows.append(
            ConformanceMatrixRow(
                module="adapter_readiness_inventory",
                symbol_or_dependency=adapter_key,
                surface="adapter_inventory",
                classification="violation",
                adapter_key=adapter_key,
                owning_fcc_or_wave=None,
                evidence_field_impact="missing_inventory_entry",
                severity="blocking",
                remediation=(
                    "Add a canonical adapter readiness inventory entry for this key "
                    "before FCC-07A can be considered complete."
                ),
                diagnostic_code="missing_required_adapter_key",
            )
        )

    ordered = tuple(sorted(rows, key=_row_sort_key))
    return ConformanceMatrixReport(
        ok=not any(row.severity == "blocking" for row in ordered),
        rows=ordered,
        dependency_report_ok=dep_report.ok,
        adapter_keys_required=tuple(sorted(required_adapter_keys)),
        adapter_keys_covered=tuple(
            sorted(required_adapter_keys.intersection({entry.adapter_key for entry in inventory}))
        ),
        adapter_keys_missing=missing,
    )


def render_matrix_report(report: ConformanceMatrixReport) -> str:
    """Render the matrix as deterministic text for CLI/release evidence."""

    status = "CONFORMANT" if report.ok else "NON-CONFORMANT"
    lines = [
        f"FCC-07A conformance matrix: {status}",
        f"  rows                 : {len(report.rows)}",
        f"  adapter_keys_required: {len(report.adapter_keys_required)}",
        f"  adapter_keys_covered : {len(report.adapter_keys_covered)}",
        f"  adapter_keys_missing : {len(report.adapter_keys_missing)}",
    ]
    if report.adapter_keys_missing:
        for key in report.adapter_keys_missing:
            lines.append(f"    - {key}")
    for row in report.rows:
        adapter = row.adapter_key or "-"
        owner = row.owning_fcc_or_wave or "-"
        lines.append(
            "  - "
            f"[{row.severity}] {row.surface} {row.classification} "
            f"{row.symbol_or_dependency} @ {row.module} "
            f"(adapter={adapter}, owner={owner})"
        )
        if row.evidence_field_impact:
            lines.append(f"      evidence: {row.evidence_field_impact}")
        if row.remediation:
            lines.append(f"      remediation: {row.remediation}")
    return "\n".join(lines)


def _ledger_by_token(ledger: tuple[LedgerEntry, ...]) -> dict[str, LedgerEntry]:
    out: dict[str, LedgerEntry] = {}
    for entry in ledger:
        out[normalize_token(entry.token)] = entry
        for alias in entry.aliases:
            out[normalize_token(alias)] = entry
    return out


def _token_to_adapter_keys(
    inventory: tuple[AdapterInventoryEntry, ...],
) -> dict[str, tuple[str, ...]]:
    out: dict[str, set[str]] = {}
    for token, adapter_keys in _TOKEN_ADAPTER_HINTS.items():
        out.setdefault(normalize_token(token), set()).update(adapter_keys)
    for entry in inventory:
        for package in entry.packages:
            token = normalize_token(_package_token(package))
            if token in {"stdlib", "stdlib(pathlib,shutil)"}:
                continue
            out.setdefault(token, set()).add(entry.adapter_key)
    return {token: tuple(sorted(keys)) for token, keys in out.items()}


def _package_token(package: str) -> str:
    token = package.strip()
    for sep in ("(", "[", ">", "<", "=", "!", "~", ";", " ", "@"):
        idx = token.find(sep)
        if idx != -1:
            token = token[:idx]
    return token


def _rows_from_dependency_finding(
    finding: AuditFinding,
    *,
    ledger_by_token: dict[str, LedgerEntry],
    entries_by_key: dict[str, AdapterInventoryEntry],
    token_to_adapters: dict[str, tuple[str, ...]],
) -> tuple[ConformanceMatrixRow, ...]:
    token = normalize_token(finding.token)
    if _is_core_common_baseline_finding(finding):
        return (
            ConformanceMatrixRow(
                module=finding.location,
                symbol_or_dependency=finding.token,
                surface=finding.surface,
                classification="core_common",
                adapter_key=None,
                owning_fcc_or_wave=None,
                evidence_field_impact=None,
                severity=finding.severity,
                remediation=None,
                diagnostic_code=finding.diagnostic_code,
            ),
        )
    adapter_keys = token_to_adapters.get(token, ())
    if not adapter_keys:
        return (
            ConformanceMatrixRow(
                module=finding.location,
                symbol_or_dependency=finding.token,
                surface=finding.surface,
                classification=_classification_from_finding(finding),
                adapter_key=None,
                owning_fcc_or_wave=None,
                evidence_field_impact=None,
                severity=finding.severity,
                remediation=finding.remediation,
                diagnostic_code=finding.diagnostic_code,
            ),
        )
    rows: list[ConformanceMatrixRow] = []
    for adapter_key in adapter_keys:
        entry = entries_by_key.get(adapter_key)
        rows.append(
            ConformanceMatrixRow(
                module=finding.location,
                symbol_or_dependency=finding.token,
                surface=finding.surface,
                classification=_classification_from_finding(finding),
                adapter_key=adapter_key,
                owning_fcc_or_wave=entry.wave if entry else None,
                evidence_field_impact=_adapter_evidence_impact(entry) if entry else None,
                severity=finding.severity,
                remediation=finding.remediation or (entry.removal_criterion if entry else None),
                diagnostic_code=finding.diagnostic_code,
            )
        )
    return tuple(rows)


def _is_core_common_baseline_finding(finding: AuditFinding) -> bool:
    return (
        normalize_token(finding.token) in CORE_COMMON_BASELINE_TOKENS
        and finding.severity == "accepted"
        and finding.surface in {"manifest", "lock", "source", "wheel"}
    )


def _classification_from_finding(finding: AuditFinding) -> MatrixClassification:
    if _is_core_common_baseline_finding(finding):
        return "core_common"
    if finding.classification in {
        "community_owned",
        "future_adapter",
        "temporary_exception",
        "removed",
    }:
        return finding.classification  # type: ignore[return-value]
    if finding.severity == "blocking":
        return "unknown"
    if finding.severity == "accepted":
        return "core_common"
    return "violation"


def _rows_from_core_common_baseline(
    dep_report: DependencyConformanceReport,
    *,
    repo_root: Path | None,
    pyproject_path: Path | None,
    lock_path: Path | None,
) -> tuple[ConformanceMatrixRow, ...]:
    repo = repo_root or _default_repo_root()
    pyproject = pyproject_path or (repo / "pyproject.toml")
    lock = lock_path or (repo / "uv.lock")
    already_projected = {
        (finding.surface, normalize_token(finding.token))
        for finding in dep_report.findings
    }
    rows: list[ConformanceMatrixRow] = []
    manifest_deps = _manifest_direct_dependencies(pyproject)
    for token in sorted(CORE_COMMON_BASELINE_TOKENS.intersection(manifest_deps)):
        if ("manifest", token) not in already_projected:
            rows.append(_core_common_row(token, "manifest"))
    lock_deps = _lock_core_direct_dependencies(lock)
    if lock_deps is not None:
        for token in sorted(CORE_COMMON_BASELINE_TOKENS.intersection(lock_deps)):
            if ("lock", token) not in already_projected:
                rows.append(_core_common_row(token, "lock"))
    return tuple(rows)


def _core_common_row(token: str, surface: str) -> ConformanceMatrixRow:
    return ConformanceMatrixRow(
        module=surface,
        symbol_or_dependency=token,
        surface=surface,
        classification="core_common",
        adapter_key=None,
        owning_fcc_or_wave=None,
        evidence_field_impact=None,
        severity="accepted",
        remediation=None,
        diagnostic_code="core_common_baseline",
    )


def _rows_from_removed_dependencies(
    dep_report: DependencyConformanceReport,
    *,
    ledger_by_token: dict[str, LedgerEntry],
) -> tuple[ConformanceMatrixRow, ...]:
    rows: list[ConformanceMatrixRow] = []
    for token in dep_report.removed_dependencies:
        entry = ledger_by_token.get(normalize_token(token))
        rows.append(
            ConformanceMatrixRow(
                module="manifest+lock+source",
                symbol_or_dependency=token,
                surface="dependency_conformance",
                classification="removed",
                adapter_key=None,
                owning_fcc_or_wave=entry.owner_wave if entry else None,
                evidence_field_impact="confirmed_absent_from_manifest_lock_source",
                severity="accepted",
                remediation=None,
                diagnostic_code="removed_dependency_absent",
            )
        )
    return tuple(rows)


def _rows_from_import_boundary(
    violations: Iterable[object],
    *,
    entries_by_key: dict[str, AdapterInventoryEntry],
    token_to_adapters: dict[str, tuple[str, ...]],
) -> tuple[ConformanceMatrixRow, ...]:
    rows: list[ConformanceMatrixRow] = []
    for raw in violations:
        if not isinstance(raw, dict):
            continue
        imported = str(raw.get("imported") or "")
        module = str(raw.get("file") or "")
        line = raw.get("line")
        location = f"{module}:{line}" if line else module
        token = normalize_token(imported.split(".")[0])
        adapter_keys = token_to_adapters.get(token, ())
        classification = (
            "community_owned"
            if imported == "okto_pulse.community"
            or imported.startswith("okto_pulse.community.")
            else "violation"
        )
        severity: MatrixSeverity = (
            "blocking" if str(raw.get("status")) in {"blocking", "reject"} else "warning"
        )
        if not adapter_keys:
            rows.append(
                ConformanceMatrixRow(
                    module=location,
                    symbol_or_dependency=imported,
                    surface="source",
                    classification=classification,
                    adapter_key=None,
                    owning_fcc_or_wave=None,
                    evidence_field_impact=None,
                    severity=severity,
                    remediation=str(raw.get("remediation_hint") or ""),
                    diagnostic_code=str(raw.get("rule") or "import_boundary_violation"),
                )
            )
            continue
        for adapter_key in adapter_keys:
            entry = entries_by_key.get(adapter_key)
            rows.append(
                ConformanceMatrixRow(
                    module=location,
                    symbol_or_dependency=imported,
                    surface="source",
                    classification=classification,
                    adapter_key=adapter_key,
                    owning_fcc_or_wave=entry.wave if entry else None,
                    evidence_field_impact=_adapter_evidence_impact(entry) if entry else None,
                    severity=severity,
                    remediation=str(raw.get("remediation_hint") or ""),
                    diagnostic_code=str(raw.get("rule") or "import_boundary_violation"),
                )
            )
    return tuple(rows)


def _row_from_source_reference(
    reference: SourceImportReference,
    *,
    entries_by_key: dict[str, AdapterInventoryEntry],
    token_to_adapters: dict[str, tuple[str, ...]],
) -> ConformanceMatrixRow:
    imported = reference.imported
    is_testing_provider = (
        imported == TESTING_PROVIDER_PREFIX
        or imported.startswith(TESTING_PROVIDER_PREFIX + ".")
    )
    classification: MatrixClassification
    severity: MatrixSeverity
    remediation: str | None
    if is_testing_provider and reference.test_context:
        classification = "test_only"
        severity = "info"
        remediation = None
    elif is_testing_provider:
        classification = "violation"
        severity = "blocking"
        remediation = (
            "Testing providers are sanctioned only under explicit test fixtures; "
            "production composition must use a registered runtime adapter."
        )
    elif imported == "okto_pulse.community" or imported.startswith("okto_pulse.community."):
        classification = "community_owned"
        severity = "blocking"
        remediation = "Move Community implementation import out of productive core code."
    else:
        classification = "violation"
        severity = "blocking"
        remediation = "Route the concrete import through an existing boundary gate."

    token = normalize_token(imported.split(".")[0])
    adapter_key = None
    owner = None
    evidence_impact = None
    adapter_keys = token_to_adapters.get(token, ())
    if adapter_keys:
        adapter_key = adapter_keys[0]
        entry = entries_by_key.get(adapter_key)
        owner = entry.wave if entry else None
        evidence_impact = _adapter_evidence_impact(entry) if entry else None
    return ConformanceMatrixRow(
        module=reference.location,
        symbol_or_dependency=imported,
        surface="source",
        classification=classification,
        adapter_key=adapter_key,
        owning_fcc_or_wave=owner,
        evidence_field_impact=evidence_impact,
        severity=severity,
        remediation=remediation,
        diagnostic_code="source_import_reference",
    )


def _rows_from_adapter_inventory(
    inventory: tuple[AdapterInventoryEntry, ...],
    *,
    evidence_by_key: dict[str, AdapterEvidence],
    ledger_by_token: dict[str, LedgerEntry],
) -> tuple[ConformanceMatrixRow, ...]:
    rows: list[ConformanceMatrixRow] = []
    for entry in inventory:
        evidence = evidence_by_key.get(entry.adapter_key, AdapterEvidence())
        verdict = evaluate_adapter_readiness(entry, evidence)
        structural_violations = _adapter_structural_violations(entry)
        rows.append(
            ConformanceMatrixRow(
                module=entry.current_module,
                symbol_or_dependency=entry.adapter_key,
                surface="adapter_inventory",
                classification=_classification_from_adapter(entry, ledger_by_token),
                adapter_key=entry.adapter_key,
                owning_fcc_or_wave=entry.wave,
                evidence_field_impact=(
                    "structural_violation:" + ",".join(structural_violations)
                    if structural_violations
                    else _verdict_evidence_impact(verdict)
                ),
                severity=(
                    "blocking"
                    if structural_violations
                    else ("info" if verdict.is_ready else "warning")
                ),
                remediation=entry.removal_criterion,
                diagnostic_code=(
                    "adapter_readiness:relational_seam_contract_violation"
                    if structural_violations
                    else f"adapter_readiness:{verdict.status}"
                ),
            )
        )
    return tuple(rows)


def _adapter_structural_violations(entry: AdapterInventoryEntry) -> tuple[str, ...]:
    """Fail closed when a residual relational seam loses its deferred contract."""
    if entry.adapter_key not in DEFERRED_RELATIONAL_SEAM_ADAPTER_KEYS:
        return ()

    text = " ".join(
        (
            entry.owner,
            entry.current_module,
            entry.port_ref,
            entry.wave,
            entry.target_destination,
            entry.removal_criterion,
            " ".join(entry.packages),
            " ".join(entry.oracles_required),
        )
    ).lower()
    missing: list[str] = []
    if entry.status != "deferred":
        missing.append("status:deferred")
    if not entry.owner.strip():
        missing.append("owner")
    if "community" not in text and "future" not in text:
        missing.append("target_owner")
    if not entry.predecessor_refs:
        missing.append("predecessor_refs")
    if not entry.oracles_required:
        missing.append("oracles_required")
    if not entry.removal_criterion.strip():
        missing.append("removal_criterion")
    return tuple(missing)


def _classification_from_adapter(
    entry: AdapterInventoryEntry,
    ledger_by_token: dict[str, LedgerEntry],
) -> MatrixClassification:
    if entry.adapter_key in DEFERRED_RELATIONAL_SEAM_ADAPTER_KEYS:
        return "future_adapter"

    for package in entry.packages:
        ledger_entry = ledger_by_token.get(normalize_token(_package_token(package)))
        if ledger_entry and ledger_entry.classification in {
            "community_owned",
            "future_adapter",
            "temporary_exception",
            "removed",
        }:
            return ledger_entry.classification  # type: ignore[return-value]
    haystack = " ".join(
        (
            entry.owner,
            entry.target_destination,
            entry.current_module,
            entry.wave,
            entry.adapter_key,
        )
    ).lower()
    if "community" in haystack:
        return "community_owned"
    if entry.status == "ready":
        return "core_common"
    return "future_adapter"


def _adapter_evidence_impact(entry: AdapterInventoryEntry | None) -> str | None:
    if entry is None:
        return None
    return ",".join(entry.oracles_required) or None


def _verdict_evidence_impact(verdict: AdapterReadinessVerdict) -> str | None:
    if verdict.missing_evidence:
        return "missing:" + ",".join(verdict.missing_evidence)
    if verdict.failed_evidence:
        return "failed:" + ",".join(verdict.failed_evidence)
    return ",".join(verdict.reasons) or None


def _row_sort_key(row: ConformanceMatrixRow) -> tuple[str, str, str, str, str]:
    return (
        row.surface,
        row.module,
        row.symbol_or_dependency,
        row.classification,
        row.adapter_key or "",
    )


__all__ = [
    "MatrixClassification",
    "MatrixSeverity",
    "TESTING_PROVIDER_PREFIX",
    "DEFERRED_RELATIONAL_SEAM_ADAPTER_KEYS",
    "SourceImportReference",
    "ConformanceMatrixRow",
    "ConformanceMatrixReport",
    "build_conformance_matrix",
    "render_matrix_report",
]
