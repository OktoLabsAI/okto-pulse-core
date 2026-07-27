"""Packaging ownership gate (FCC-07C-IMP1) — REUSES the FCC-07A conformance matrix.

This gate does NOT re-derive any dependency classification. It runs
``build_conformance_matrix`` over the four packaging surfaces
(``manifest`` / ``lock`` / ``source`` / ``wheel``), reads every row's
``MatrixClassification``, and turns the matrix into a deterministic OWNERSHIP
verdict for the CORE:

  * ``community_owned`` / ``future_adapter`` / ``removed`` / ``unknown`` /
    ``violation`` declared in the productive (runtime) core packaging FAIL
    (blocking) — UNLESS the matrix already classifies the token as a ledgered
    ``temporary_exception`` (the dependency ledger carries its
    owner / removal_criterion / validation oracle, so the exception is honoured
    without a second, parallel ledger here — FR4).
  * ``temporary_exception`` (ledgered) and ``core_common`` stay VISIBLE but
    non-blocking (FR4 / TR5 — ``sqlalchemy`` is never name-banned: any restriction
    flows from the MatrixClassification, never from the token spelling).
  * the SAME ownership violation that appears ONLY as an optional / dev / test
    extra (not in the runtime deps, not in the runtime wheel metadata) is
    reported with an explicit ``optional`` scope and does NOT block; declared in
    the runtime manifest / wheel it blocks (FR7).

Single classification authority: the ``MatrixClassification`` produced by
``conformance_matrix`` (which itself delegates to ``dependency_ledger`` /
``audit_dependency_conformance``). This module adds ONLY the ownership policy
(which classifications block) and the runtime/optional SCOPE axis; it never
embeds the token->classification decision nor the ``test_only`` / ``violation``
disambiguation. Static analysis only; importable in isolation.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .conformance_matrix import (
    ConformanceMatrixReport,
    ConformanceMatrixRow,
    MatrixClassification,
    MatrixSeverity,
    build_conformance_matrix,
)
from .dependency_conformance import (
    DependencyConformanceReport,
    audit_dependency_conformance,
)
from .dependency_ledger import normalize_token
from .report import GateReport, GateSeverity, GateStatus

#: The four packaging surfaces this gate is responsible for (FR1). Rows on any
#: OTHER matrix surface (``adapter_inventory`` readiness, ``ledger`` integrity)
#: belong to FCC-07A / R05-A and are not this gate's ownership verdict.
OWNERSHIP_SURFACES: frozenset[str] = frozenset({"manifest", "lock", "source", "wheel"})

#: Classifications that are an OWNERSHIP violation when declared in the core
#: (FR4 community_owned/future_adapter, FR5 removed, plus fail-closed unknown /
#: violation). ``temporary_exception`` / ``core_common`` / ``test_only`` are NOT
#: here — they are visible-non-blocking.
BLOCKING_CLASSIFICATIONS: frozenset[str] = frozenset(
    {"community_owned", "future_adapter", "removed", "unknown", "violation"}
)

#: Dependency-conformance ``origin`` values that mean "ships in the productive
#: runtime/wheel" (runtime scope). Anything else (an optional-dependency extra)
#: is treated as out-of-runtime ``optional`` scope. Fail-closed: an unknown
#: origin defaults to runtime.
_OPTIONAL_ORIGINS: frozenset[str] = frozenset(
    {"optional_dependency", "wheel_optional_dependency"}
)

DependencyScope = Literal["runtime", "optional"]
OwnershipAction = Literal[
    "block",
    "temporary_exception",
    "core_common",
    "scoped_out",
    "accepted",
]


@dataclass(frozen=True)
class PackagingOwnershipGateInput:
    """Parameterizable inputs (TR2) — every path is overridable for fixtures."""

    repo_root: Path | None = None
    pyproject_path: Path | None = None
    lock_path: Path | None = None
    source_root: Path | None = None
    wheel_metadata_path: Path | None = None
    audit_wheel: bool = True
    #: A pre-built dependency report (e.g. with a synthetic ledger for AC5). When
    #: supplied it is the single source for BOTH the matrix rows and the
    #: runtime/optional scope axis, so the two never disagree.
    dependency_report: DependencyConformanceReport | None = None
    include_import_boundary: bool = True
    import_boundary_mode: str = "blocking"


@dataclass(frozen=True)
class PackagingOwnershipRow:
    """One packaging-surface row with its ownership action + runtime/optional scope."""

    surface: str
    symbol: str
    classification: MatrixClassification
    scope: DependencyScope
    action: OwnershipAction
    severity: MatrixSeverity
    adapter_key: str | None
    owning_fcc_or_wave: str | None
    diagnostic_code: str | None
    remediation: str | None

    @property
    def blocking(self) -> bool:
        return self.action == "block"

    def as_dict(self) -> dict[str, object]:
        return {
            "surface": self.surface,
            "symbol": self.symbol,
            "classification": self.classification,
            "scope": self.scope,
            "action": self.action,
            "severity": self.severity,
            "adapter_key": self.adapter_key,
            "owning_fcc_or_wave": self.owning_fcc_or_wave,
            "diagnostic_code": self.diagnostic_code,
            "remediation": self.remediation,
        }


@dataclass(frozen=True)
class PackagingOwnershipReport:
    """Deterministic packaging-ownership verdict for the core (FCC-07C-IMP1)."""

    ok: bool
    status: GateStatus
    rows: tuple[PackagingOwnershipRow, ...]
    blocking: tuple[PackagingOwnershipRow, ...]
    temporary_exceptions: tuple[PackagingOwnershipRow, ...]
    scoped_out: tuple[PackagingOwnershipRow, ...]
    ledger_integrity_ok: bool
    matrix_ok: bool
    surfaces_audited: tuple[str, ...]
    adapter_keys_missing: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "status": self.status,
            "ledger_integrity_ok": self.ledger_integrity_ok,
            "matrix_ok": self.matrix_ok,
            "surfaces_audited": list(self.surfaces_audited),
            "adapter_keys_missing": list(self.adapter_keys_missing),
            "scope_summary": {
                "runtime": sum(1 for r in self.rows if r.scope == "runtime"),
                "optional": sum(1 for r in self.rows if r.scope == "optional"),
            },
            "rows": [r.as_dict() for r in self.rows],
            "blocking": [r.as_dict() for r in self.blocking],
            "temporary_exceptions": [r.as_dict() for r in self.temporary_exceptions],
            "scoped_out": [r.as_dict() for r in self.scoped_out],
        }

    def to_gate_report(self) -> GateReport:
        """Bridge to the shared ``GateReport`` contract (CLI / suite consistency)."""
        severity: GateSeverity
        if self.status in ("blocking", "reject"):
            severity = "high"
        elif self.status == "passed":
            severity = "low"
        else:
            severity = "medium"
        remediation = None
        if self.blocking:
            remediation = self.blocking[0].remediation or (
                "Remove the out-of-core dependency/import from the productive core "
                "packaging, or ledger it as a temporary exception."
            )
        return GateReport(
            gate_id="packaging_ownership",
            subject="core packaging ownership (FCC-07C)",
            status=self.status,
            severity=severity,
            owner="okto-pulse-core/architecture" if self.status != "passed" else None,
            evidence=self.as_dict(),
            promotion_criteria=(
                "Resolve every blocking ownership violation (move it to the owning "
                "edition / rebuild the wheel) or ledger it as a temporary exception "
                "with owner + removal_criterion + validation oracle."
            )
            if self.status != "passed"
            else None,
            observed_value=[r.symbol for r in self.blocking],
            expected_value=[],
            remediation_hint=remediation,
        )


def _scope_for_origin(origin: str) -> DependencyScope:
    return "optional" if origin in _OPTIONAL_ORIGINS else "runtime"


def _scope_map(report: DependencyConformanceReport) -> dict[tuple[str, str], DependencyScope]:
    """Map ``(surface, normalized token)`` -> runtime/optional from the auditor.

    The auditor's ``origin`` already separates a runtime/default dependency from
    an optional-extra and from a runtime import — REUSE it instead of re-parsing
    the manifest, so the scope axis can never disagree with the classification.
    A token present at runtime on a surface wins over the same token as an extra
    (the auditor already collapses both into one finding per surface/token).
    """
    out: dict[tuple[str, str], DependencyScope] = {}
    for finding in report.findings:
        if finding.surface not in OWNERSHIP_SURFACES:
            continue
        key = (finding.surface, normalize_token(finding.token))
        scope = _scope_for_origin(finding.origin)
        if out.get(key) == "runtime":
            continue  # runtime is sticky — never downgrade to optional
        out[key] = scope
    return out


def _action_and_severity(
    classification: MatrixClassification, scope: DependencyScope
) -> tuple[OwnershipAction, MatrixSeverity]:
    if classification in BLOCKING_CLASSIFICATIONS:
        if scope == "optional":
            # FR7: an ownership violation confined to an optional/dev/test extra
            # that does not ship in the runtime/wheel is surfaced, not blocked.
            return "scoped_out", "warning"
        return "block", "blocking"
    if classification == "temporary_exception":
        return "temporary_exception", "accepted"
    if classification == "core_common":
        return "core_common", "accepted"
    return "accepted", "info"


def _remediation(row: ConformanceMatrixRow, action: OwnershipAction) -> str | None:
    if action != "block":
        return row.remediation
    if row.surface == "wheel":
        return (
            f"'{row.symbol_or_dependency}' is out of the core ownership boundary but "
            "is declared by the wheel metadata: rebuild the wheel from the cleaned "
            "source and reinstall it so the published artifact stops shipping it."
        )
    return row.remediation or (
        f"'{row.symbol_or_dependency}' ({row.classification}) is out of the core "
        f"ownership boundary on the {row.surface} surface: move it to the owning "
        "edition package or ledger it as a temporary exception."
    )


def _classify_status(
    *,
    blocking: tuple[PackagingOwnershipRow, ...],
    scoped_out: tuple[PackagingOwnershipRow, ...],
    temporary_exceptions: tuple[PackagingOwnershipRow, ...],
    ledger_integrity_ok: bool,
) -> GateStatus:
    if blocking or not ledger_integrity_ok:
        return "blocking"
    if scoped_out:
        return "xfail_advisory"
    if temporary_exceptions:
        return "baseline"
    return "passed"


class PackagingOwnershipGate:
    """Reuse-only ownership gate over the FCC-07A conformance matrix (FCC-07C-IMP1)."""

    gate_id = "packaging_ownership"

    def run(
        self, gate_input: PackagingOwnershipGateInput | None = None
    ) -> PackagingOwnershipReport:
        gate_input = gate_input or PackagingOwnershipGateInput()
        dep_report = gate_input.dependency_report or audit_dependency_conformance(
            repo_root=gate_input.repo_root,
            pyproject_path=gate_input.pyproject_path,
            lock_path=gate_input.lock_path,
            source_root=gate_input.source_root,
            wheel_metadata_path=gate_input.wheel_metadata_path,
            audit_wheel=gate_input.audit_wheel,
        )
        matrix: ConformanceMatrixReport = build_conformance_matrix(
            repo_root=gate_input.repo_root,
            pyproject_path=gate_input.pyproject_path,
            lock_path=gate_input.lock_path,
            source_root=gate_input.source_root,
            wheel_metadata_path=gate_input.wheel_metadata_path,
            audit_wheel=gate_input.audit_wheel,
            dependency_report=dep_report,
            include_import_boundary=gate_input.include_import_boundary,
            import_boundary_mode=gate_input.import_boundary_mode,
        )
        scope_by_key = _scope_map(dep_report)

        rows: list[PackagingOwnershipRow] = []
        for mrow in matrix.rows:  # already deterministically sorted by the matrix
            if mrow.surface not in OWNERSHIP_SURFACES:
                continue
            scope = scope_by_key.get(
                (mrow.surface, normalize_token(mrow.symbol_or_dependency)),
                "runtime",  # fail-closed: an unmapped packaging row is runtime
            )
            action, severity = _action_and_severity(mrow.classification, scope)
            rows.append(
                PackagingOwnershipRow(
                    surface=mrow.surface,
                    symbol=mrow.symbol_or_dependency,
                    classification=mrow.classification,
                    scope=scope,
                    action=action,
                    severity=severity,
                    adapter_key=mrow.adapter_key,
                    owning_fcc_or_wave=mrow.owning_fcc_or_wave,
                    diagnostic_code=mrow.diagnostic_code,
                    remediation=_remediation(mrow, action),
                )
            )

        ordered = tuple(rows)
        blocking = tuple(r for r in ordered if r.action == "block")
        temporary_exceptions = tuple(
            r for r in ordered if r.action == "temporary_exception"
        )
        scoped_out = tuple(r for r in ordered if r.action == "scoped_out")
        status = _classify_status(
            blocking=blocking,
            scoped_out=scoped_out,
            temporary_exceptions=temporary_exceptions,
            ledger_integrity_ok=dep_report.ledger_integrity_ok,
        )
        return PackagingOwnershipReport(
            ok=not blocking and dep_report.ledger_integrity_ok,
            status=status,
            rows=ordered,
            blocking=blocking,
            temporary_exceptions=temporary_exceptions,
            scoped_out=scoped_out,
            ledger_integrity_ok=dep_report.ledger_integrity_ok,
            matrix_ok=matrix.ok,
            surfaces_audited=dep_report.surfaces_audited,
            adapter_keys_missing=matrix.adapter_keys_missing,
        )


def run_packaging_ownership_gate(
    gate_input: PackagingOwnershipGateInput | None = None,
) -> PackagingOwnershipReport:
    """Functional wrapper around :class:`PackagingOwnershipGate` (CLI ergonomics)."""
    return PackagingOwnershipGate().run(gate_input)


def render_packaging_ownership_report(report: PackagingOwnershipReport) -> str:
    """Render the verdict as deterministic text for CLI / release evidence."""
    status = "CONFORMANT" if report.ok else "NON-CONFORMANT"
    lines = [
        f"FCC-07C packaging ownership: {status} (status={report.status})",
        f"  ledger_integrity_ok : {report.ledger_integrity_ok}",
        f"  surfaces_audited    : {', '.join(report.surfaces_audited)}",
        f"  ownership_rows      : {len(report.rows)}",
        f"  blocking            : {len(report.blocking)}",
        f"  scoped_out(optional): {len(report.scoped_out)}",
        f"  temporary_exception : {len(report.temporary_exceptions)}",
    ]
    for row in report.blocking:
        lines.append(
            "  - [BLOCK] "
            f"{row.surface}/{row.scope} {row.classification} {row.symbol} "
            f"(adapter={row.adapter_key or '-'}, code={row.diagnostic_code or '-'})"
        )
        if row.remediation:
            lines.append(f"      remediation: {row.remediation}")
    for row in report.scoped_out:
        lines.append(
            "  - [scoped-out/optional] "
            f"{row.surface} {row.classification} {row.symbol}"
        )
    for row in report.temporary_exceptions:
        lines.append(
            "  - [temporary-exception] "
            f"{row.surface} {row.symbol} (owner={row.owning_fcc_or_wave or '-'})"
        )
    return "\n".join(lines)


__all__ = [
    "OWNERSHIP_SURFACES",
    "BLOCKING_CLASSIFICATIONS",
    "DependencyScope",
    "OwnershipAction",
    "PackagingOwnershipGateInput",
    "PackagingOwnershipRow",
    "PackagingOwnershipReport",
    "PackagingOwnershipGate",
    "run_packaging_ownership_gate",
    "render_packaging_ownership_report",
]
