"""R15A Community boundary evidence contract and phased hardening gates.

The core owns the common boundary policy. Community-specific proof enters as
data: a versioned evidence payload, plus a ledger of intentional Community
references. This module never imports ``okto_pulse.community``.
"""

from __future__ import annotations

import ast
import importlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from .import_matrix import COMMUNITY_PREFIXES
from .report import GateReport

EvidenceStatus = Literal["passed", "failed", "skipped"]
EvidenceSurface = Literal["boundary", "packaging", "readiness", "conformance"]
ReferenceKind = Literal["runtime_import", "type_checking_import", "string_ref", "evidence_schema"]

DIAG_EVIDENCE_MISSING = "evidence_missing"
DIAG_EVIDENCE_STALE = "evidence_stale"
DIAG_EVIDENCE_FAILING = "evidence_failing"
DIAG_EVIDENCE_MALFORMED = "evidence_malformed"
DIAG_EVIDENCE_MISMATCH = "evidence_mismatch"
DIAG_LEDGER_MISSING_REFERENCE = "ledger_missing_reference"
DIAG_LEDGER_RUNTIME_IMPORT = "ledger_runtime_import_not_allowed"
DIAG_LEDGER_MISSING_OWNER_VALIDITY = "ledger_missing_owner_or_validity"
DIAG_RELATIONAL_DEPENDENCIES_PENDING = "relational_hardening_dependencies_pending"
DIAG_RELATIONAL_VIOLATION = "relational_hardening_violation"

COMMUNITY_EVIDENCE_SCHEMA_VERSION = "1"
COMMUNITY_EVIDENCE_PRODUCER = "okto-pulse-community"
COMMUNITY_EVIDENCE_EDITION = "community"
R01_RELATIONAL_HARDENING_DEPENDENCIES: tuple[str, ...] = ("R01A", "R01B", "R01C")


@dataclass(frozen=True)
class BoundaryEvidenceCheck:
    """One check emitted by the Community evidence producer."""

    name: str
    surface: EvidenceSurface
    status: EvidenceStatus
    details: Mapping[str, Any]

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "BoundaryEvidenceCheck":
        return cls(
            name=str(raw.get("name") or ""),
            surface=str(raw.get("surface") or ""),  # type: ignore[arg-type]
            status=str(raw.get("status") or ""),  # type: ignore[arg-type]
            details=dict(raw.get("details") or {}),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "surface": self.surface,
            "status": self.status,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class CommunityBoundaryEvidence:
    """Structured Community-owned boundary evidence consumed by core gates."""

    schema_version: str
    producer: str
    edition: str
    generated_at: str
    max_age_seconds: int | None
    expires_at: str | None
    core_commit: str
    community_commit: str
    artifact_hash: str
    ledger_path: str
    checks: tuple[BoundaryEvidenceCheck, ...]

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "CommunityBoundaryEvidence":
        checks_raw = raw.get("checks") or ()
        if not isinstance(checks_raw, Sequence) or isinstance(checks_raw, (str, bytes)):
            checks_raw = ()
        checks = tuple(
            BoundaryEvidenceCheck.from_mapping(check)
            for check in checks_raw
            if isinstance(check, Mapping)
        )
        max_age_raw = raw.get("max_age_seconds")
        max_age = int(max_age_raw) if max_age_raw is not None else None
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            producer=str(raw.get("producer") or ""),
            edition=str(raw.get("edition") or ""),
            generated_at=str(raw.get("generated_at") or ""),
            max_age_seconds=max_age,
            expires_at=str(raw["expires_at"]) if raw.get("expires_at") else None,
            core_commit=str(raw.get("core_commit") or ""),
            community_commit=str(raw.get("community_commit") or ""),
            artifact_hash=str(raw.get("artifact_hash") or ""),
            ledger_path=str(raw.get("ledger_path") or ""),
            checks=checks,
        )

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "schema_version": self.schema_version,
            "producer": self.producer,
            "edition": self.edition,
            "generated_at": self.generated_at,
            "core_commit": self.core_commit,
            "community_commit": self.community_commit,
            "artifact_hash": self.artifact_hash,
            "ledger_path": self.ledger_path,
            "checks": [check.as_dict() for check in self.checks],
        }
        if self.max_age_seconds is not None:
            out["max_age_seconds"] = self.max_age_seconds
        if self.expires_at is not None:
            out["expires_at"] = self.expires_at
        return out


@dataclass(frozen=True)
class BoundaryFinding:
    """Fail-closed diagnostic shared by the R15A gates."""

    code: str
    message: str
    field: str | None = None
    file: str | None = None
    line: int | None = None
    reference: str | None = None
    remediation: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "field": self.field,
            "file": self.file,
            "line": self.line,
            "reference": self.reference,
            "remediation": self.remediation,
        }


@dataclass(frozen=True)
class CommunityBoundaryEvidenceReport:
    """Validation result for one Community boundary evidence payload."""

    ok: bool
    evidence: CommunityBoundaryEvidence | None
    findings: tuple[BoundaryFinding, ...]

    @property
    def blocking(self) -> tuple[BoundaryFinding, ...]:
        return self.findings

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "evidence": self.evidence.as_dict() if self.evidence else None,
            "findings": [finding.as_dict() for finding in self.findings],
        }


@dataclass(frozen=True)
class CommunityReferenceLedgerEntry:
    """Classified Community reference found in boundary/conformance surfaces."""

    file: str
    line: int
    reference: str
    kind: ReferenceKind
    owner: str | None = None
    valid_until: str | None = None
    action: str | None = None

    def semantic_key(self) -> tuple[str, str, str]:
        return (self.file.replace("\\", "/"), self.reference, self.kind)

    def as_dict(self) -> dict[str, Any]:
        return {
            "file": self.file,
            "line": self.line,
            "reference": self.reference,
            "kind": self.kind,
            "owner": self.owner,
            "valid_until": self.valid_until,
            "action": self.action,
        }


@dataclass(frozen=True)
class CommunityReferenceLedgerReport:
    """Completeness/ownership verdict for Community reference ledger entries."""

    ok: bool
    discovered: tuple[CommunityReferenceLedgerEntry, ...]
    ledger_entries: tuple[CommunityReferenceLedgerEntry, ...]
    findings: tuple[BoundaryFinding, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "discovered": [entry.as_dict() for entry in self.discovered],
            "ledger_entries": [entry.as_dict() for entry in self.ledger_entries],
            "findings": [finding.as_dict() for finding in self.findings],
        }


@dataclass(frozen=True)
class PhasedRelationalHardeningReport:
    """Relational hardening gate that only activates after R01A/R01B/R01C."""

    ok: bool
    dependencies_required: tuple[str, ...]
    dependencies_completed: tuple[str, ...]
    dependencies_missing: tuple[str, ...]
    findings: tuple[BoundaryFinding, ...]
    relational_report: Any | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "dependencies_required": list(self.dependencies_required),
            "dependencies_completed": list(self.dependencies_completed),
            "dependencies_missing": list(self.dependencies_missing),
            "findings": [finding.as_dict() for finding in self.findings],
            "relational_report": (
                self.relational_report.as_dict()
                if hasattr(self.relational_report, "as_dict")
                else None
            ),
        }


@dataclass(frozen=True)
class CommunityBoundaryEvidenceGateInput:
    """Input for the core GateReport wrapper around Community evidence data."""

    payload: Mapping[str, Any] | str | None
    now: datetime | None = None
    expected_core_commit: str | None = None
    expected_community_commit: str | None = None
    expected_artifact_hash: str | None = None
    required_surfaces: tuple[EvidenceSurface, ...] = ()


class CommunityBoundaryEvidenceGate:
    """Core gate that consumes Community evidence as structured data."""

    gate_id = "community_boundary_evidence"

    def run(self, gate_input: CommunityBoundaryEvidenceGateInput) -> GateReport:
        report = validate_community_boundary_evidence(
            gate_input.payload,
            now=gate_input.now,
            expected_core_commit=gate_input.expected_core_commit,
            expected_community_commit=gate_input.expected_community_commit,
            expected_artifact_hash=gate_input.expected_artifact_hash,
            required_surfaces=gate_input.required_surfaces,
        )
        status = "passed" if report.ok else "blocking"
        return GateReport(
            gate_id=self.gate_id,
            subject="Community boundary evidence",
            status=status,
            severity="high" if not report.ok else "medium",
            owner="okto-pulse-community/boundary-evidence",
            evidence=report.as_dict(),
            observed_value=len(report.findings),
            expected_value=0,
            remediation_hint=(
                "Produce fresh, passing CommunityBoundaryEvidence with the required "
                "schema, commit/hash binding and surfaces."
                if not report.ok
                else None
            ),
        )


def _load_payload(payload: Mapping[str, Any] | str) -> Mapping[str, Any]:
    if isinstance(payload, str):
        loaded = json.loads(payload)
        if not isinstance(loaded, Mapping):
            raise TypeError("Community boundary evidence JSON must decode to an object")
        return loaded
    return payload


def _parse_dt(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def validate_community_boundary_evidence(
    payload: Mapping[str, Any] | str | None,
    *,
    now: datetime | None = None,
    expected_core_commit: str | None = None,
    expected_community_commit: str | None = None,
    expected_artifact_hash: str | None = None,
    required_surfaces: Iterable[EvidenceSurface] = (),
) -> CommunityBoundaryEvidenceReport:
    """Validate Community evidence fail-closed.

    Missing, stale, failing, malformed or mismatched evidence all produce a
    blocking report. A valid payload remains pure data; no Community module is
    imported or executed.
    """
    if payload is None or payload == "":
        return CommunityBoundaryEvidenceReport(
            ok=False,
            evidence=None,
            findings=(
                BoundaryFinding(
                    code=DIAG_EVIDENCE_MISSING,
                    field="payload",
                    message="No Community boundary evidence was provided.",
                    remediation="Produce a CommunityBoundaryEvidence payload before closing the gate.",
                ),
            ),
        )
    try:
        evidence = CommunityBoundaryEvidence.from_mapping(_load_payload(payload))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        return CommunityBoundaryEvidenceReport(
            ok=False,
            evidence=None,
            findings=(
                BoundaryFinding(
                    code=DIAG_EVIDENCE_MALFORMED,
                    field="payload",
                    message=f"Community boundary evidence is malformed: {exc}",
                ),
            ),
        )

    findings: list[BoundaryFinding] = []
    _require_equal(
        findings,
        field="schema_version",
        actual=evidence.schema_version,
        expected=COMMUNITY_EVIDENCE_SCHEMA_VERSION,
    )
    _require_equal(
        findings,
        field="producer",
        actual=evidence.producer,
        expected=COMMUNITY_EVIDENCE_PRODUCER,
    )
    _require_equal(
        findings,
        field="edition",
        actual=evidence.edition,
        expected=COMMUNITY_EVIDENCE_EDITION,
    )
    for field_name, value in (
        ("generated_at", evidence.generated_at),
        ("core_commit", evidence.core_commit),
        ("community_commit", evidence.community_commit),
        ("artifact_hash", evidence.artifact_hash),
        ("ledger_path", evidence.ledger_path),
    ):
        if not value:
            findings.append(
                BoundaryFinding(
                    code=DIAG_EVIDENCE_MALFORMED,
                    field=field_name,
                    message=f"Community boundary evidence missing required field {field_name}.",
                )
            )
    if evidence.max_age_seconds is None and not evidence.expires_at:
        findings.append(
            BoundaryFinding(
                code=DIAG_EVIDENCE_MALFORMED,
                field="max_age_seconds",
                message="Evidence must declare max_age_seconds or expires_at.",
            )
        )
    if not evidence.checks:
        findings.append(
            BoundaryFinding(
                code=DIAG_EVIDENCE_MALFORMED,
                field="checks",
                message="Evidence must contain at least one check.",
            )
        )

    generated_at = _parse_dt(evidence.generated_at)
    if generated_at is None:
        findings.append(
            BoundaryFinding(
                code=DIAG_EVIDENCE_MALFORMED,
                field="generated_at",
                message="generated_at must be an ISO-8601 timestamp.",
            )
        )
    else:
        clock = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        if evidence.max_age_seconds is not None:
            age_seconds = (clock - generated_at).total_seconds()
            if age_seconds > evidence.max_age_seconds:
                findings.append(
                    BoundaryFinding(
                        code=DIAG_EVIDENCE_STALE,
                        field="generated_at",
                        message="Community boundary evidence is older than max_age_seconds.",
                    )
                )
        if evidence.expires_at:
            expires_at = _parse_dt(evidence.expires_at)
            if expires_at is None:
                findings.append(
                    BoundaryFinding(
                        code=DIAG_EVIDENCE_MALFORMED,
                        field="expires_at",
                        message="expires_at must be an ISO-8601 timestamp.",
                    )
                )
            elif clock > expires_at:
                findings.append(
                    BoundaryFinding(
                        code=DIAG_EVIDENCE_STALE,
                        field="expires_at",
                        message="Community boundary evidence is past expires_at.",
                    )
                )

    _require_expected(findings, "core_commit", evidence.core_commit, expected_core_commit)
    _require_expected(
        findings, "community_commit", evidence.community_commit, expected_community_commit
    )
    _require_expected(findings, "artifact_hash", evidence.artifact_hash, expected_artifact_hash)

    seen_surfaces: set[str] = set()
    for check in evidence.checks:
        if not check.name:
            findings.append(
                BoundaryFinding(
                    code=DIAG_EVIDENCE_MALFORMED,
                    field="checks.name",
                    message="Every evidence check must have a name.",
                )
            )
        if check.surface not in ("boundary", "packaging", "readiness", "conformance"):
            findings.append(
                BoundaryFinding(
                    code=DIAG_EVIDENCE_MALFORMED,
                    field="checks.surface",
                    message=f"Unknown evidence surface {check.surface!r}.",
                    reference=check.name,
                )
            )
        else:
            seen_surfaces.add(check.surface)
        if check.status not in ("passed", "failed", "skipped"):
            findings.append(
                BoundaryFinding(
                    code=DIAG_EVIDENCE_MALFORMED,
                    field="checks.status",
                    message=f"Unknown evidence status {check.status!r}.",
                    reference=check.name,
                )
            )
        elif check.status == "failed":
            findings.append(
                BoundaryFinding(
                    code=DIAG_EVIDENCE_FAILING,
                    field="checks.status",
                    message=f"Community evidence check {check.name!r} failed.",
                    reference=check.name,
                    remediation="Fix the Community-side check before consuming this evidence.",
                )
            )

    for surface in required_surfaces:
        if surface not in seen_surfaces:
            findings.append(
                BoundaryFinding(
                    code=DIAG_EVIDENCE_MALFORMED,
                    field="checks.surface",
                    message=f"Required evidence surface {surface!r} is missing.",
                )
            )

    return CommunityBoundaryEvidenceReport(
        ok=not findings,
        evidence=evidence,
        findings=tuple(findings),
    )


def _require_equal(
    findings: list[BoundaryFinding], *, field: str, actual: str, expected: str
) -> None:
    if actual != expected:
        findings.append(
            BoundaryFinding(
                code=DIAG_EVIDENCE_MALFORMED,
                field=field,
                message=f"Expected {field}={expected!r}, got {actual!r}.",
            )
        )


def _require_expected(
    findings: list[BoundaryFinding],
    field: str,
    actual: str,
    expected: str | None,
) -> None:
    if expected is not None and actual != expected:
        findings.append(
            BoundaryFinding(
                code=DIAG_EVIDENCE_MISMATCH,
                field=field,
                message=f"Evidence {field}={actual!r} does not match expected {expected!r}.",
            )
        )


def discover_community_references(
    roots: Sequence[str | Path],
) -> tuple[CommunityReferenceLedgerEntry, ...]:
    """Discover real and string Community references without importing code."""
    entries: list[CommunityReferenceLedgerEntry] = []
    for root_value in roots:
        root = Path(root_value)
        files = sorted(root.rglob("*.py")) if root.is_dir() else [root]
        for path in files:
            if "__pycache__" in path.parts or not path.exists():
                continue
            try:
                text = path.read_text(encoding="utf-8")
                tree = ast.parse(text, filename=str(path))
            except (OSError, SyntaxError, UnicodeDecodeError):
                continue
            try:
                rel = path.relative_to(root if root.is_dir() else root.parent).as_posix()
            except ValueError:
                rel = path.as_posix()
            entries.extend(_discover_in_tree(tree, rel))
    return tuple(sorted(entries, key=lambda e: (e.file, e.line, e.reference, e.kind)))


def _discover_in_tree(tree: ast.AST, rel: str) -> list[CommunityReferenceLedgerEntry]:
    entries: list[CommunityReferenceLedgerEntry] = []
    type_guarded = _type_checking_import_nodes(tree)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _is_community_module(alias.name):
                    kind: ReferenceKind = (
                        "type_checking_import" if id(node) in type_guarded else "runtime_import"
                    )
                    entries.append(
                        CommunityReferenceLedgerEntry(rel, node.lineno, alias.name, kind)
                    )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level == 0 and _is_community_module(module):
                kind = "type_checking_import" if id(node) in type_guarded else "runtime_import"
                entries.append(
                    CommunityReferenceLedgerEntry(rel, node.lineno, module, kind)
                )
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            for reference in _string_community_references(node.value):
                kind = "evidence_schema" if "evidence" in node.value.lower() else "string_ref"
                entries.append(
                    CommunityReferenceLedgerEntry(rel, getattr(node, "lineno", 0), reference, kind)
                )
    return entries


def validate_community_reference_ledger(
    *,
    roots: Sequence[str | Path] = (),
    ledger_entries: Iterable[CommunityReferenceLedgerEntry] = (),
    discovered: Iterable[CommunityReferenceLedgerEntry] | None = None,
    today: datetime | None = None,
) -> CommunityReferenceLedgerReport:
    """Check that every discovered Community reference is ledgered with owner/validity."""
    found = tuple(discovered) if discovered is not None else discover_community_references(roots)
    ledger = tuple(ledger_entries)
    ledger_by_key = {entry.semantic_key(): entry for entry in ledger}
    findings: list[BoundaryFinding] = []

    for entry in found:
        if entry.semantic_key() not in ledger_by_key:
            findings.append(
                BoundaryFinding(
                    code=DIAG_LEDGER_MISSING_REFERENCE,
                    file=entry.file,
                    line=entry.line,
                    reference=entry.reference,
                    message="Community reference is missing from the classification ledger.",
                    remediation="Add a ledger entry with kind, owner, validity and action.",
                )
            )

    clock = today or datetime.now(timezone.utc)
    for entry in ledger:
        if entry.kind == "runtime_import":
            findings.append(
                BoundaryFinding(
                    code=DIAG_LEDGER_RUNTIME_IMPORT,
                    file=entry.file,
                    line=entry.line,
                    reference=entry.reference,
                    message="Runtime import of okto_pulse.community is not allowed in core boundary surfaces.",
                    remediation="Replace the import with structured evidence data or move collection to Community.",
                )
            )
            continue
        if entry.kind in ("string_ref", "evidence_schema"):
            if not entry.owner or not entry.valid_until:
                findings.append(
                    BoundaryFinding(
                        code=DIAG_LEDGER_MISSING_OWNER_VALIDITY,
                        file=entry.file,
                        line=entry.line,
                        reference=entry.reference,
                        message="String/evidence Community references require owner and valid_until.",
                    )
                )
            elif _parse_dt(entry.valid_until) is None:
                findings.append(
                    BoundaryFinding(
                        code=DIAG_LEDGER_MISSING_OWNER_VALIDITY,
                        file=entry.file,
                        line=entry.line,
                        reference=entry.reference,
                        message="valid_until must be an ISO-8601 timestamp.",
                    )
                )
            elif _parse_dt(entry.valid_until) < clock.astimezone(timezone.utc):
                findings.append(
                    BoundaryFinding(
                        code=DIAG_LEDGER_MISSING_OWNER_VALIDITY,
                        file=entry.file,
                        line=entry.line,
                        reference=entry.reference,
                        message="Community reference ledger entry is past valid_until.",
                    )
                )

    return CommunityReferenceLedgerReport(
        ok=not findings,
        discovered=found,
        ledger_entries=ledger,
        findings=tuple(findings),
    )


def run_phased_relational_hardening(
    root: str | Path | None = None,
    *,
    completed_dependencies: Iterable[str] = (),
    required_dependencies: tuple[str, ...] = R01_RELATIONAL_HARDENING_DEPENDENCIES,
) -> PhasedRelationalHardeningReport:
    """Run relational hardening only after R01A/R01B/R01C are complete."""
    completed = tuple(sorted(set(completed_dependencies)))
    missing = tuple(dep for dep in required_dependencies if dep not in completed)
    if missing:
        return PhasedRelationalHardeningReport(
            ok=False,
            dependencies_required=required_dependencies,
            dependencies_completed=completed,
            dependencies_missing=missing,
            findings=(
                BoundaryFinding(
                    code=DIAG_RELATIONAL_DEPENDENCIES_PENDING,
                    field="completed_dependencies",
                    message=(
                        "Post-R01 relational hardening cannot run before all "
                        "R01A/R01B/R01C dependency waves are complete."
                    ),
                    remediation="Complete R01A, R01B and R01C before enabling core-wide hardening.",
                ),
            ),
        )

    relational_module = importlib.import_module(
        "okto_pulse.core.repositories.relational_boundary_gate"
    )
    relational = relational_module.run_relational_boundary_gate(root=root)
    findings = tuple(
        BoundaryFinding(
            code=DIAG_RELATIONAL_VIOLATION,
            file=v.file,
            line=v.line,
            reference=v.symbol,
            message=v.remediation_hint,
            remediation=v.remediation_hint,
        )
        for v in relational.violations
    )
    return PhasedRelationalHardeningReport(
        ok=relational.ok,
        dependencies_required=required_dependencies,
        dependencies_completed=completed,
        dependencies_missing=(),
        findings=findings,
        relational_report=relational,
    )


def _is_community_module(module: str) -> bool:
    return any(
        module == prefix or module.startswith(prefix + ".")
        for prefix in COMMUNITY_PREFIXES
    )


def _string_community_references(value: str) -> tuple[str, ...]:
    refs: set[str] = set()
    for prefix in COMMUNITY_PREFIXES:
        if prefix in value:
            refs.add(prefix)
    if "okto_pulse/community" in value:
        refs.add("okto_pulse/community")
    return tuple(sorted(refs))


def _type_checking_import_nodes(tree: ast.AST) -> set[int]:
    guarded: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and _is_type_checking_test(node.test):
            for child in node.body:
                for sub in ast.walk(child):
                    if isinstance(sub, (ast.Import, ast.ImportFrom)):
                        guarded.add(id(sub))
    return guarded


def _is_type_checking_test(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return node.id == "TYPE_CHECKING"
    if isinstance(node, ast.Attribute):
        return node.attr == "TYPE_CHECKING"
    return False


__all__ = [
    "COMMUNITY_EVIDENCE_EDITION",
    "COMMUNITY_EVIDENCE_PRODUCER",
    "COMMUNITY_EVIDENCE_SCHEMA_VERSION",
    "DIAG_EVIDENCE_FAILING",
    "DIAG_EVIDENCE_MALFORMED",
    "DIAG_EVIDENCE_MISMATCH",
    "DIAG_EVIDENCE_MISSING",
    "DIAG_EVIDENCE_STALE",
    "DIAG_LEDGER_MISSING_OWNER_VALIDITY",
    "DIAG_LEDGER_MISSING_REFERENCE",
    "DIAG_LEDGER_RUNTIME_IMPORT",
    "DIAG_RELATIONAL_DEPENDENCIES_PENDING",
    "DIAG_RELATIONAL_VIOLATION",
    "R01_RELATIONAL_HARDENING_DEPENDENCIES",
    "BoundaryEvidenceCheck",
    "BoundaryFinding",
    "CommunityBoundaryEvidence",
    "CommunityBoundaryEvidenceGate",
    "CommunityBoundaryEvidenceGateInput",
    "CommunityBoundaryEvidenceReport",
    "CommunityReferenceLedgerEntry",
    "CommunityReferenceLedgerReport",
    "PhasedRelationalHardeningReport",
    "discover_community_references",
    "run_phased_relational_hardening",
    "validate_community_boundary_evidence",
    "validate_community_reference_ledger",
]
