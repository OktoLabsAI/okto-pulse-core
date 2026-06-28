"""Community-manifest packaging audit + ``dependency_audit_passed`` projection
(FCC-07C-IMP2 — the COMMUNITY-AWARE half of the packaging gate).

The CORE half (FCC-07C-IMP1, :mod:`packaging_ownership_gate`) decides — over the
core packaging surfaces — whether a community-owned / removed / future-adapter
dependency LEAKED into the productive core. This module adds the complementary,
community-side question: for every Community-owned adapter in the readiness
inventory, is the dependency family the adapter needs actually DECLARED in the
Community edition's ``pyproject.toml``?

Two responsibilities (FR2/AC3 and FR6/AC7):

1. :func:`audit_community_manifest` — reads the Community ``pyproject.toml`` (by a
   PARAMETERIZABLE path, the same way IMP1 reads the core ``pyproject.toml``) and,
   for each Community-owned adapter, checks every required dependency FAMILY is
   declared. A required family is one the FCC-07A/R05-E
   :mod:`dependency_ledger` classifies ``community_owned`` (so the Community
   edition is the authoritative declaration site). Transitive/contractual
   packages (``stdlib`` / a token the ledger does NOT classify community-owned,
   e.g. ``torch`` pulled in by ``sentence-transformers``) are intentionally NOT
   required to be declared directly. A missing required family yields a BLOCKING
   finding carrying the ``dependency_family`` + ``adapter_key`` + a remediation
   pointing at the Community ``pyproject.toml`` (AC3). A family declared ONLY as
   an optional/dev extra (not a runtime dependency) is surfaced as a non-blocking
   ``warning`` — the runtime/optional/dev SCOPE differentiation (FR7).

2. :func:`project_dependency_audit_evidence` — the SINGLE authorized source of the
   ``dependency_audit_passed`` field consumed by FCC-07B's
   :class:`~okto_pulse.core.application.boundary.adapter_readiness_inventory.AdapterEvidence`.
   It CONSUMES the IMP1 ownership verdict (it does NOT re-audit the core
   classification) and combines it with the community-manifest audit:
   ``dependency_audit_passed[adapter_key]`` is ``True`` exactly when the IMP1
   packaging gate has NO blocking row for that ``adapter_key`` AND the
   community-manifest audit has no missing-declaration finding for it (AC7).

The CORE NEVER imports ``okto_pulse.community`` — the Community manifest enters
ONLY as a file read from a parameterizable path. Static analysis only
(``tomllib`` via the reused parser); importable in isolation; deterministic
(every collection is sorted by ``adapter_key`` / ``dependency_family``).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .adapter_readiness_inventory import (
    AdapterEvidence,
    AdapterInventoryEntry,
    build_adapter_inventory,
)
from .conformance_matrix import MatrixSeverity, _package_token
from .dependency_conformance import _manifest_direct_dependencies
from .dependency_ledger import LedgerEntry, ledger_index, normalize_token
from .packaging_ownership_gate import PackagingOwnershipReport

#: Substring (case-insensitive) in ``AdapterInventoryEntry.owner`` that marks an
#: adapter as owned by the Community edition (its dependency families must be
#: declared in the Community manifest, NOT the core's).
COMMUNITY_OWNER_MARKER = "community"

#: The ledger classification that makes a package a dependency FAMILY the Community
#: manifest is authoritative for. Anything else (unledgered transitive packages,
#: stdlib) is NOT required to be declared directly.
_DECLARABLE_CLASSIFICATION = "community_owned"

DependencyDeclScope = Literal["runtime", "optional", "absent"]

#: Stable diagnostic codes (fail-closed contract referenced by the tests).
DIAG_COMMUNITY_DEPENDENCY_DECLARED = "community_dependency_declared"
DIAG_COMMUNITY_DEPENDENCY_OPTIONAL_ONLY = "community_dependency_optional_only"
DIAG_COMMUNITY_DEPENDENCY_NOT_DECLARED = "community_dependency_not_declared"


@dataclass(frozen=True)
class CommunityDependencyFinding:
    """One (community-owned adapter, required dependency family) audit outcome."""

    adapter_key: str
    dependency_family: str
    owner: str
    wave: str
    declared: bool
    scope: DependencyDeclScope
    severity: MatrixSeverity
    diagnostic_code: str
    remediation: str | None

    @property
    def blocking(self) -> bool:
        return self.severity == "blocking"

    def as_dict(self) -> dict[str, object]:
        return {
            "adapter_key": self.adapter_key,
            "dependency_family": self.dependency_family,
            "owner": self.owner,
            "wave": self.wave,
            "declared": self.declared,
            "scope": self.scope,
            "severity": self.severity,
            "diagnostic_code": self.diagnostic_code,
            "remediation": self.remediation,
        }


@dataclass(frozen=True)
class CommunityManifestAuditReport:
    """Deterministic audit of the Community manifest for Community-owned adapters."""

    ok: bool
    community_pyproject_path: str
    community_pyproject_exists: bool
    findings: tuple[CommunityDependencyFinding, ...]
    blocking: tuple[CommunityDependencyFinding, ...]
    warnings: tuple[CommunityDependencyFinding, ...]
    audited_adapter_keys: tuple[str, ...]
    required_families: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "community_pyproject_path": self.community_pyproject_path,
            "community_pyproject_exists": self.community_pyproject_exists,
            "audited_adapter_keys": list(self.audited_adapter_keys),
            "required_families": list(self.required_families),
            "findings": [f.as_dict() for f in self.findings],
            "blocking": [f.as_dict() for f in self.blocking],
            "warnings": [f.as_dict() for f in self.warnings],
        }


def _is_community_owned(entry: AdapterInventoryEntry) -> bool:
    return COMMUNITY_OWNER_MARKER in entry.owner.lower()


def _required_families(
    entry: AdapterInventoryEntry, ledger_idx: dict[str, LedgerEntry]
) -> tuple[str, ...]:
    """The dependency families this adapter requires the Community to declare.

    REUSES the ledger classification (does NOT re-derive it): a package is a
    required family iff the ledger classifies its bare token ``community_owned``.
    ``stdlib`` and any unledgered transitive package (``torch`` / ``huggingface_hub``
    behind ``sentence-transformers``) are dropped — they are pulled in
    transitively or are purely contractual.
    """
    families: set[str] = set()
    for package in entry.packages:
        token = normalize_token(_package_token(package))
        led = ledger_idx.get(token)
        if led is not None and led.classification == _DECLARABLE_CLASSIFICATION:
            families.add(normalize_token(led.token))
    return tuple(sorted(families))


def audit_community_manifest(
    community_pyproject_path: Path,
    *,
    inventory: tuple[AdapterInventoryEntry, ...] | None = None,
    ledger: tuple[LedgerEntry, ...] | None = None,
) -> CommunityManifestAuditReport:
    """Audit the Community ``pyproject.toml`` for every Community-owned adapter.

    ``community_pyproject_path`` is PARAMETERIZABLE (a synthetic fixture or the
    real sibling ``okto-pulse/pyproject.toml``) — the core NEVER imports
    ``okto_pulse.community``; the manifest enters only as a file read.
    """
    inventory = inventory if inventory is not None else build_adapter_inventory()
    ledger_idx = ledger_index(ledger)
    path = Path(community_pyproject_path)
    exists = path.exists()
    # {normalized dependency name -> is_optional}; {} when the file is absent.
    manifest = _manifest_direct_dependencies(path)

    findings: list[CommunityDependencyFinding] = []
    audited_keys: set[str] = set()
    required_all: set[str] = set()

    for entry in inventory:
        if not _is_community_owned(entry):
            continue
        audited_keys.add(entry.adapter_key)
        for family in _required_families(entry, ledger_idx):
            required_all.add(family)
            findings.append(
                _evaluate_family(entry, family, manifest, path)
            )

    findings.sort(key=lambda f: (f.adapter_key, f.dependency_family))
    blocking = tuple(f for f in findings if f.severity == "blocking")
    warnings = tuple(f for f in findings if f.severity == "warning")
    return CommunityManifestAuditReport(
        ok=not blocking,
        community_pyproject_path=path.as_posix(),
        community_pyproject_exists=exists,
        findings=tuple(findings),
        blocking=blocking,
        warnings=warnings,
        audited_adapter_keys=tuple(sorted(audited_keys)),
        required_families=tuple(sorted(required_all)),
    )


def _evaluate_family(
    entry: AdapterInventoryEntry,
    family: str,
    manifest: dict[str, bool],
    path: Path,
) -> CommunityDependencyFinding:
    if family not in manifest:
        return CommunityDependencyFinding(
            adapter_key=entry.adapter_key,
            dependency_family=family,
            owner=entry.owner,
            wave=entry.wave,
            declared=False,
            scope="absent",
            severity="blocking",
            diagnostic_code=DIAG_COMMUNITY_DEPENDENCY_NOT_DECLARED,
            remediation=(
                f"Declare '{family}' as a runtime dependency in "
                f"{path.as_posix()} [project.dependencies]: it is required by the "
                f"Community-owned adapter '{entry.adapter_key}' ({entry.wave})."
            ),
        )
    is_optional = manifest[family]
    if is_optional:
        # FR7 scope differentiation: a RUNTIME adapter's family declared only as an
        # optional/dev extra is surfaced (not silently passed), but does NOT block —
        # the declaration exists, it is just mis-scoped for a runtime adapter.
        return CommunityDependencyFinding(
            adapter_key=entry.adapter_key,
            dependency_family=family,
            owner=entry.owner,
            wave=entry.wave,
            declared=True,
            scope="optional",
            severity="warning",
            diagnostic_code=DIAG_COMMUNITY_DEPENDENCY_OPTIONAL_ONLY,
            remediation=(
                f"'{family}' is declared only as an optional/dev extra in "
                f"{path.as_posix()}, but the Community-owned runtime adapter "
                f"'{entry.adapter_key}' needs it at runtime: promote it to "
                "[project.dependencies]."
            ),
        )
    return CommunityDependencyFinding(
        adapter_key=entry.adapter_key,
        dependency_family=family,
        owner=entry.owner,
        wave=entry.wave,
        declared=True,
        scope="runtime",
        severity="accepted",
        diagnostic_code=DIAG_COMMUNITY_DEPENDENCY_DECLARED,
        remediation=None,
    )


# =========================================================================== #
# dependency_audit_passed projection (FR6/AC7) — the ONLY authorized source.
# =========================================================================== #
@dataclass(frozen=True)
class DependencyAuditEvidenceRow:
    """The ``dependency_audit_passed`` evidence for ONE adapter_key, with reasons."""

    adapter_key: str
    owner: str
    community_owned: bool
    dependency_families: tuple[str, ...]
    dependency_audit_passed: bool
    core_ownership_blocking_symbols: tuple[str, ...]
    community_blocking_families: tuple[str, ...]
    community_optional_only_families: tuple[str, ...]
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "adapter_key": self.adapter_key,
            "owner": self.owner,
            "community_owned": self.community_owned,
            "dependency_families": list(self.dependency_families),
            "dependency_audit_passed": self.dependency_audit_passed,
            "core_ownership_blocking_symbols": list(self.core_ownership_blocking_symbols),
            "community_blocking_families": list(self.community_blocking_families),
            "community_optional_only_families": list(self.community_optional_only_families),
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class DependencyAuditProjection:
    """Deterministic ``{adapter_key: dependency_audit_passed}`` projection (AC7)."""

    ok: bool
    community_pyproject_path: str
    rows: tuple[DependencyAuditEvidenceRow, ...]

    def evidence_map(self) -> dict[str, bool]:
        """``{adapter_key -> dependency_audit_passed}`` — the bool consumed by FCC-07B."""
        return {row.adapter_key: row.dependency_audit_passed for row in self.rows}

    def as_adapter_evidence(self) -> dict[str, AdapterEvidence]:
        """``{adapter_key -> AdapterEvidence(dependency_audit_passed=...)}``.

        FCC-07B merges the ``dependency_audit_passed`` field of each
        :class:`AdapterEvidence` into the adapter's full evidence before calling
        ``evaluate_adapter_readiness``. This module is the SINGLE authorized source
        of that field — every other evidence field stays ``None`` here.
        """
        return {
            row.adapter_key: AdapterEvidence(
                dependency_audit_passed=row.dependency_audit_passed
            )
            for row in self.rows
        }

    def passed(self, adapter_key: str) -> bool | None:
        for row in self.rows:
            if row.adapter_key == adapter_key:
                return row.dependency_audit_passed
        return None

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "community_pyproject_path": self.community_pyproject_path,
            "evidence_map": self.evidence_map(),
            "rows": [row.as_dict() for row in self.rows],
        }


def project_dependency_audit_evidence(
    *,
    ownership_report: PackagingOwnershipReport,
    community_audit: CommunityManifestAuditReport,
    inventory: tuple[AdapterInventoryEntry, ...] | None = None,
) -> DependencyAuditProjection:
    """Project ``dependency_audit_passed`` per adapter_key (FR6/AC7).

    CONSUMES the IMP1 ``ownership_report`` verdict (does NOT re-audit the core
    classification) and the community-manifest audit. For each inventory adapter,
    ``dependency_audit_passed`` is ``True`` iff:

      * the IMP1 packaging-ownership gate has NO blocking row carrying this
        ``adapter_key`` (the community-owned dependency did not leak into the
        productive core), AND
      * the community-manifest audit has no BLOCKING (missing-declaration)
        finding for this ``adapter_key``.

    A non-blocking optional-only warning (FR7) is surfaced on the row but does NOT
    fail the audit.
    """
    inventory = inventory if inventory is not None else build_adapter_inventory()

    core_blocking_by_key: dict[str, list[str]] = {}
    for row in ownership_report.blocking:
        if row.adapter_key:
            core_blocking_by_key.setdefault(row.adapter_key, []).append(row.symbol)

    comm_block_by_key: dict[str, set[str]] = {}
    comm_optional_by_key: dict[str, set[str]] = {}
    comm_families_by_key: dict[str, set[str]] = {}
    for f in community_audit.findings:
        comm_families_by_key.setdefault(f.adapter_key, set()).add(f.dependency_family)
        if f.severity == "blocking":
            comm_block_by_key.setdefault(f.adapter_key, set()).add(f.dependency_family)
        elif f.severity == "warning":
            comm_optional_by_key.setdefault(f.adapter_key, set()).add(f.dependency_family)

    rows: list[DependencyAuditEvidenceRow] = []
    for entry in inventory:
        key = entry.adapter_key
        core_blocking = tuple(sorted(set(core_blocking_by_key.get(key, []))))
        comm_block = tuple(sorted(comm_block_by_key.get(key, set())))
        comm_optional = tuple(sorted(comm_optional_by_key.get(key, set())))
        families = tuple(sorted(comm_families_by_key.get(key, set())))
        passed = not core_blocking and not comm_block

        reasons: list[str] = []
        for sym in core_blocking:
            reasons.append(f"core_ownership_blocking:{sym}")
        for fam in comm_block:
            reasons.append(f"community_dependency_not_declared:{fam}")
        for fam in comm_optional:
            reasons.append(f"community_dependency_optional_only:{fam}")
        if passed and not reasons:
            reasons.append(
                "clean" if families else "no_governed_community_dependency"
            )
        elif passed:
            reasons.append("clean")

        rows.append(
            DependencyAuditEvidenceRow(
                adapter_key=key,
                owner=entry.owner,
                community_owned=_is_community_owned(entry),
                dependency_families=families,
                dependency_audit_passed=passed,
                core_ownership_blocking_symbols=core_blocking,
                community_blocking_families=comm_block,
                community_optional_only_families=comm_optional,
                reasons=tuple(reasons),
            )
        )

    rows.sort(key=lambda r: r.adapter_key)
    return DependencyAuditProjection(
        ok=all(r.dependency_audit_passed for r in rows),
        community_pyproject_path=community_audit.community_pyproject_path,
        rows=tuple(rows),
    )


def run_community_packaging_audit(
    *,
    community_pyproject_path: Path,
    ownership_report: PackagingOwnershipReport,
    inventory: tuple[AdapterInventoryEntry, ...] | None = None,
    ledger: tuple[LedgerEntry, ...] | None = None,
) -> tuple[CommunityManifestAuditReport, DependencyAuditProjection]:
    """Run the community-manifest audit and project the ``dependency_audit_passed``
    evidence against a (caller-supplied, IMP1) core ownership verdict.

    The ``ownership_report`` is produced by the IMP1
    :class:`~okto_pulse.core.application.boundary.packaging_ownership_gate.PackagingOwnershipGate`
    over the CORE packaging surfaces; this function never re-runs that gate's
    classification.
    """
    inventory = inventory if inventory is not None else build_adapter_inventory()
    community_audit = audit_community_manifest(
        community_pyproject_path, inventory=inventory, ledger=ledger
    )
    projection = project_dependency_audit_evidence(
        ownership_report=ownership_report,
        community_audit=community_audit,
        inventory=inventory,
    )
    return community_audit, projection


def render_community_packaging_audit(
    community_audit: CommunityManifestAuditReport,
    projection: DependencyAuditProjection,
) -> str:
    """Render the community audit + dependency_audit_passed projection as text."""
    status = "CONFORMANT" if community_audit.ok else "NON-CONFORMANT"
    lines = [
        f"FCC-07C community packaging audit: {status}",
        f"  community_pyproject : {community_audit.community_pyproject_path}",
        f"  exists              : {community_audit.community_pyproject_exists}",
        f"  audited_adapters    : {len(community_audit.audited_adapter_keys)}",
        f"  required_families   : {', '.join(community_audit.required_families) or '-'}",
        f"  blocking            : {len(community_audit.blocking)}",
        f"  warnings            : {len(community_audit.warnings)}",
    ]
    for f in community_audit.blocking:
        lines.append(
            f"  - [BLOCK] {f.adapter_key} needs '{f.dependency_family}' "
            f"(scope={f.scope})"
        )
        if f.remediation:
            lines.append(f"      remediation: {f.remediation}")
    for f in community_audit.warnings:
        lines.append(
            f"  - [warn/optional-only] {f.adapter_key} '{f.dependency_family}'"
        )
    lines.append("  dependency_audit_passed:")
    for row in projection.rows:
        if not row.community_owned and row.dependency_audit_passed:
            continue  # keep the projection summary focused on the audited surface
        lines.append(
            f"    - {row.adapter_key}: {row.dependency_audit_passed} "
            f"({', '.join(row.reasons)})"
        )
    return "\n".join(lines)


__all__ = [
    "COMMUNITY_OWNER_MARKER",
    "DependencyDeclScope",
    "DIAG_COMMUNITY_DEPENDENCY_DECLARED",
    "DIAG_COMMUNITY_DEPENDENCY_OPTIONAL_ONLY",
    "DIAG_COMMUNITY_DEPENDENCY_NOT_DECLARED",
    "CommunityDependencyFinding",
    "CommunityManifestAuditReport",
    "DependencyAuditEvidenceRow",
    "DependencyAuditProjection",
    "audit_community_manifest",
    "project_dependency_audit_evidence",
    "run_community_packaging_audit",
    "render_community_packaging_audit",
]
