"""R05-E dependency conformance auditor — fail-closed (spec d9d30831, card 0c066d12).

Audits FOUR surfaces for non-agnostic technical dependencies / imports that are
NOT covered by the versioned dependency ledger (``dependency_ledger``):

  1. ``manifest`` — ``pyproject.toml`` ``[project.dependencies]`` + optional-deps
                    (the DIRECT dependencies the core package declares).
  2. ``lock``     — ``uv.lock``, the ``okto-pulse-core`` package's OWN dependency
                    list (DIRECT), never the transitive closure.
  3. ``source``   — an AST import scan of ``src/okto_pulse/core`` only (RUNTIME
                    imports), never ``tests/`` / ``docs/`` / frontend dist text.
  4. ``wheel``    — the installed/declared wheel metadata (``Requires-Dist``),
                    parsed via ``importlib.metadata`` or an explicit dist-info
                    ``METADATA`` path.

Fail-closed (br_3796a542 / fr_73119bd6): a concrete edition dependency/import
that appears in the core WITHOUT a complete ledger entry yields a deterministic
violation with diagnostic code ``unledgered_dependency`` or ``unledgered_import``.
A token the ledger classifies as ``removed`` (e.g. ``asyncpg``) that REAPPEARS as
a direct dependency or import yields ``removed_dependency_present`` /
``removed_import_present``.

Origin discrimination (tr_ee04e7b2 / ac_103ce235): only a DIRECT manifest/lock
dependency of ``okto-pulse-core``, or a RUNTIME import under
``src/okto_pulse/core``, is ledger-blocking. Transitive-only lock entries, and
any occurrence in tests, docs, or the frontend dist, NEVER reprove.

Static analysis only (``ast`` / ``tomllib`` / ``pathlib`` / ``importlib.metadata``)
— no heavy runtime import (tr_73ba5de5, tr_2796f890: Windows + offline; no
network / AWS / PyPI). Importable in isolation.
"""

from __future__ import annotations

import ast
import tomllib
from dataclasses import dataclass, field
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Literal

from .dependency_ledger import (
    LEDGER_VERSION,
    CANONICAL_TEMPORARY_EXCEPTION_TOKENS,
    LedgerEntry,
    build_dependency_ledger,
    entry_missing_fields,
    ledger_index,
    normalize_token,
)

#: PyPI / import tokens that are NON-AGNOSTIC technology (edition concerns) and
#: therefore REQUIRE a ledger entry when present as a core direct dep / import.
#: Reuses the persistence roots of the #12 import matrix and extends them with the
#: embedding / scheduler / telemetry-transport tokens R05-E governs. Transport
#: frameworks (fastapi / uvicorn / mcp / fastmcp / starlette) are the #12
#: import-matrix gate's concern and are intentionally NOT in this set, and the
#: agnostic-until-#04 ORM (``sqlalchemy``) is governed by #04, not R05-E.
GOVERNED_TECHNICAL_TOKENS: frozenset[str] = frozenset(
    normalize_token(t)
    for t in (
        "asyncpg",
        "aiosqlite",
        "ladybug",
        "kuzu",
        "numpy",
        "sentence-transformers",
        "torch",
        "apscheduler",
        "requests",
        "chardet",
    )
)

AuditSurface = Literal["manifest", "lock", "source", "wheel", "ledger"]
FindingSeverity = Literal["blocking", "warning", "accepted", "info"]

#: Stable diagnostic codes (fail-closed contract — referenced by ts_3a5d3275).
DIAG_UNLEDGERED_DEPENDENCY = "unledgered_dependency"
DIAG_UNLEDGERED_IMPORT = "unledgered_import"
DIAG_REMOVED_DEPENDENCY_PRESENT = "removed_dependency_present"
DIAG_REMOVED_IMPORT_PRESENT = "removed_import_present"
DIAG_LEDGER_ENTRY_INCOMPLETE = "ledger_entry_incomplete"
DIAG_MISSING_CANONICAL_EXCEPTION = "missing_canonical_exception"


@dataclass(frozen=True)
class AuditFinding:
    """One deterministic outcome for one (surface, token) pair."""

    surface: AuditSurface
    token: str
    diagnostic_code: str
    severity: FindingSeverity
    origin: str
    location: str
    classification: str | None = None
    remediation: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "surface": self.surface,
            "token": self.token,
            "diagnostic_code": self.diagnostic_code,
            "severity": self.severity,
            "origin": self.origin,
            "location": self.location,
            "classification": self.classification,
            "remediation": self.remediation,
        }


@dataclass(frozen=True)
class DependencyConformanceReport:
    """Deterministic, JSON-serialisable conformance report (fr_b916ccaf / OR)."""

    ledger_version: str
    ok: bool
    surfaces_audited: tuple[str, ...]
    analyzed_paths: tuple[str, ...]
    removed_dependencies: tuple[str, ...]
    accepted_exceptions: tuple[dict[str, object], ...]
    community_owned: tuple[str, ...]
    future_adapters: tuple[str, ...]
    violations: tuple[AuditFinding, ...]
    warnings: tuple[AuditFinding, ...]
    findings: tuple[AuditFinding, ...] = field(default_factory=tuple)
    ledger_integrity_ok: bool = True

    def as_dict(self) -> dict[str, object]:
        return {
            "ledger_version": self.ledger_version,
            "ok": self.ok,
            "surfaces_audited": list(self.surfaces_audited),
            "analyzed_paths": list(self.analyzed_paths),
            "removed_dependencies": list(self.removed_dependencies),
            "accepted_exceptions": list(self.accepted_exceptions),
            "community_owned": list(self.community_owned),
            "future_adapters": list(self.future_adapters),
            "violations": [f.as_dict() for f in self.violations],
            "warnings": [f.as_dict() for f in self.warnings],
            "ledger_integrity_ok": self.ledger_integrity_ok,
        }


# --------------------------------------------------------------------------- #
# Path defaults
# --------------------------------------------------------------------------- #
def _default_source_root() -> Path:
    # .../src/okto_pulse/core/application/boundary/dependency_conformance.py -> src/
    return Path(__file__).resolve().parents[4]


def _default_repo_root() -> Path:
    # src/ -> repo root (contains pyproject.toml + uv.lock)
    return _default_source_root().parent


# --------------------------------------------------------------------------- #
# Parsers (structured, never grep)
# --------------------------------------------------------------------------- #
def _dependency_name(spec: str) -> str:
    """Extract the bare distribution name from a PEP 508 requirement string."""
    token = spec.strip()
    for sep in ("[", ">", "<", "=", "!", "~", ";", " ", "(", ")", "@"):
        idx = token.find(sep)
        if idx != -1:
            token = token[:idx]
    return token.strip()


def _manifest_direct_dependencies(pyproject_path: Path) -> dict[str, bool]:
    """Direct deps declared by the core package -> ``{norm_name: is_optional}``.

    Reads ``[project.dependencies]`` (default) and every
    ``[project.optional-dependencies.*]`` group (optional). Returns the bare,
    normalized names — version specifiers / extras are stripped.
    """
    if not pyproject_path.exists():
        return {}
    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    project = data.get("project", {})
    result: dict[str, bool] = {}
    for spec in project.get("dependencies", []) or []:
        name = _dependency_name(spec)
        if name:
            result.setdefault(normalize_token(name), False)
    for group in (project.get("optional-dependencies", {}) or {}).values():
        for spec in group or []:
            name = _dependency_name(spec)
            if name:
                # optional unless it is also a default dependency
                result.setdefault(normalize_token(name), True)
    return result


def _lock_core_direct_dependencies(
    lock_path: Path, package_name: str = "okto-pulse-core"
) -> dict[str, bool] | None:
    """Direct deps the ``okto-pulse-core`` package declares IN the lock.

    Returns ``{norm_name: is_optional}`` for the package's own ``dependencies``
    and ``optional-dependencies`` ONLY — every OTHER ``[[package]]`` block is a
    transitive dependency and is deliberately ignored (tr_ee04e7b2). ``None`` when
    the lock or the package block is absent.
    """
    if not lock_path.exists():
        return None
    data = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    packages = data.get("package", []) or []
    target = next((p for p in packages if p.get("name") == package_name), None)
    if target is None:
        return None
    result: dict[str, bool] = {}
    for dep in target.get("dependencies", []) or []:
        name = dep.get("name")
        if name:
            result.setdefault(normalize_token(name), False)
    for group in (target.get("optional-dependencies", {}) or {}).values():
        for dep in group or []:
            name = dep.get("name")
            if name:
                result.setdefault(normalize_token(name), True)
    return result


def _scan_source_imports(core_dir: Path) -> list[tuple[str, str]]:
    """Every runtime import ROOT under ``core_dir`` -> ``[(norm_root, file:line)]``.

    Walks ALL ``ast.Import`` / ``ast.ImportFrom`` nodes (including lazy in-function
    imports). ``__pycache__`` is skipped. Only this directory is scanned — tests,
    docs and the frontend dist live outside it and never reprove (ac_103ce235).
    """
    out: list[tuple[str, str]] = []
    if not core_dir.exists():
        return out
    for path in sorted(core_dir.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        try:
            rel = path.as_posix()
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError, ValueError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root = alias.name.split(".")[0]
                    out.append((normalize_token(root), f"{rel}:{node.lineno}"))
            elif isinstance(node, ast.ImportFrom):
                if node.level == 0 and node.module:
                    root = node.module.split(".")[0]
                    out.append((normalize_token(root), f"{rel}:{node.lineno}"))
    return out


def _wheel_requires(
    *,
    wheel_metadata_path: Path | None,
    distribution_name: str,
) -> tuple[dict[str, bool] | None, str]:
    """Direct deps declared by the installed/built wheel -> ``{norm: is_optional}``.

    Parses ``Requires-Dist`` from an explicit dist-info ``METADATA`` file (or a
    dist-info directory) when given; otherwise from the installed distribution via
    ``importlib.metadata``. Returns ``(deps_or_None, source_descriptor)``; ``None``
    means the wheel metadata could not be located (surface skipped, never a silent
    pass).
    """
    requires: list[str] | None = None
    source = ""
    if wheel_metadata_path is not None:
        meta = Path(wheel_metadata_path)
        if meta.is_dir():
            meta = meta / "METADATA"
        if meta.exists():
            requires = [
                line[len("Requires-Dist:"):].strip()
                for line in meta.read_text(encoding="utf-8").splitlines()
                if line.startswith("Requires-Dist:")
            ]
            source = f"metadata:{meta.as_posix()}"
        else:
            return None, f"metadata_not_found:{meta.as_posix()}"
    else:
        try:
            requires = importlib_metadata.distribution(distribution_name).requires
            source = f"importlib.metadata:{distribution_name}"
        except importlib_metadata.PackageNotFoundError:
            return None, f"distribution_not_installed:{distribution_name}"

    result: dict[str, bool] = {}
    for spec in requires or []:
        is_optional = "extra ==" in spec
        name = _dependency_name(spec)
        if name:
            prev = result.get(normalize_token(name))
            result[normalize_token(name)] = is_optional if prev is None else (prev and is_optional)
    return result, source


# --------------------------------------------------------------------------- #
# Auditor
# --------------------------------------------------------------------------- #
def audit_dependency_conformance(
    *,
    repo_root: Path | None = None,
    pyproject_path: Path | None = None,
    lock_path: Path | None = None,
    source_root: Path | None = None,
    wheel_metadata_path: Path | None = None,
    audit_wheel: bool = True,
    distribution_name: str = "okto-pulse-core",
    lock_package_name: str = "okto-pulse-core",
    ledger: tuple[LedgerEntry, ...] | None = None,
    governed_tokens: frozenset[str] | None = None,
) -> DependencyConformanceReport:
    """Run the fail-closed R05-E dependency conformance audit over all surfaces.

    Every path is overridable so a synthetic fixture (an unledgered manifest /
    import) can prove the gate fails closed (tr_4dc2a75c). Defaults point at the
    real ``okto-pulse-core`` repo.
    """
    repo = repo_root or _default_repo_root()
    src = source_root or _default_source_root()
    pyproject = pyproject_path or (repo / "pyproject.toml")
    lock = lock_path or (repo / "uv.lock")
    core_dir = src / "okto_pulse" / "core"

    led = ledger if ledger is not None else build_dependency_ledger()
    index = ledger_index(led)
    governed = governed_tokens if governed_tokens is not None else GOVERNED_TECHNICAL_TOKENS

    findings: list[AuditFinding] = []
    surfaces: list[str] = []
    analyzed: list[str] = []

    # --- ledger integrity (fail-closed: the ledger itself must be complete) ----
    surfaces.append("ledger")
    integrity_ok = True
    for entry in led:
        missing = entry_missing_fields(entry)
        if missing:
            integrity_ok = False
            findings.append(
                AuditFinding(
                    surface="ledger",
                    token=entry.token,
                    diagnostic_code=DIAG_LEDGER_ENTRY_INCOMPLETE,
                    severity="blocking",
                    origin="ledger",
                    location=f"dependency_ledger:{entry.token}",
                    classification=entry.classification,
                    remediation=f"Fill the required ledger fields: {', '.join(missing)}.",
                )
            )
    for token in CANONICAL_TEMPORARY_EXCEPTION_TOKENS:
        if normalize_token(token) not in index:
            integrity_ok = False
            findings.append(
                AuditFinding(
                    surface="ledger",
                    token=token,
                    diagnostic_code=DIAG_MISSING_CANONICAL_EXCEPTION,
                    severity="blocking",
                    origin="ledger",
                    location="dependency_ledger",
                    classification=None,
                    remediation=(
                        f"Add a ledger entry for the temporary exception '{token}' "
                        "(owner_wave/current_owner/reason/removal_criterion/"
                        "validation_oracle)."
                    ),
                )
            )

    # --- manifest surface ------------------------------------------------------
    manifest_deps = _manifest_direct_dependencies(pyproject)
    if pyproject.exists():
        surfaces.append("manifest")
        analyzed.append(pyproject.as_posix())
        _audit_dependency_surface(
            "manifest", manifest_deps, governed, index, findings
        )

    # --- lock surface ----------------------------------------------------------
    lock_deps = _lock_core_direct_dependencies(lock, lock_package_name)
    if lock_deps is not None:
        surfaces.append("lock")
        analyzed.append(lock.as_posix())
        _audit_dependency_surface("lock", lock_deps, governed, index, findings)

    # --- source surface --------------------------------------------------------
    source_imports = _scan_source_imports(core_dir)
    if core_dir.exists():
        surfaces.append("source")
        analyzed.append(core_dir.as_posix())
        seen_import: set[tuple[str, str]] = set()
        for root, location in source_imports:
            if root not in governed:
                continue
            key = (root, location)
            if key in seen_import:
                continue
            seen_import.add(key)
            entry = index.get(root)
            if entry is None:
                findings.append(
                    AuditFinding(
                        surface="source",
                        token=root,
                        diagnostic_code=DIAG_UNLEDGERED_IMPORT,
                        severity="blocking",
                        origin="runtime_import",
                        location=location,
                        classification=None,
                        remediation=(
                            "Ledger this concrete edition import or move it behind a "
                            "port wired in the composition root."
                        ),
                    )
                )
            elif entry.classification == "removed":
                findings.append(
                    AuditFinding(
                        surface="source",
                        token=root,
                        diagnostic_code=DIAG_REMOVED_IMPORT_PRESENT,
                        severity="blocking",
                        origin="runtime_import",
                        location=location,
                        classification="removed",
                        remediation=(
                            f"'{root}' is ledgered as removed; it must not be "
                            "imported in src/okto_pulse/core."
                        ),
                    )
                )
            else:
                findings.append(
                    AuditFinding(
                        surface="source",
                        token=entry.token,
                        diagnostic_code="ledgered_import",
                        severity="accepted",
                        origin="runtime_import",
                        location=location,
                        classification=entry.classification,
                        remediation=None,
                    )
                )

    # --- wheel surface ---------------------------------------------------------
    if audit_wheel:
        wheel_deps, wheel_source = _wheel_requires(
            wheel_metadata_path=wheel_metadata_path,
            distribution_name=distribution_name,
        )
        if wheel_deps is not None:
            surfaces.append("wheel")
            analyzed.append(wheel_source)
            for token, is_optional in sorted(wheel_deps.items()):
                if token not in governed:
                    continue
                entry = index.get(token)
                origin = "wheel_optional_dependency" if is_optional else "wheel_metadata"
                if entry is None:
                    findings.append(
                        AuditFinding(
                            surface="wheel",
                            token=token,
                            diagnostic_code=DIAG_UNLEDGERED_DEPENDENCY,
                            severity="blocking",
                            origin=origin,
                            location=wheel_source,
                            classification=None,
                            remediation="Ledger this concrete edition dependency or remove it from the wheel.",
                        )
                    )
                elif entry.classification == "removed":
                    # An EXPLICIT wheel/dist-info METADATA artifact (``metadata:``
                    # source) is AUTHORITATIVE: a ``removed`` token in its
                    # ``Requires-Dist`` is a real published-boundary breach and
                    # fails closed. Only ``importlib.metadata`` of an installed
                    # distribution may be STALE (editable install before reinstall),
                    # so THAT case stays a non-blocking warning — the authoritative
                    # removal is proven by the manifest + lock.
                    if wheel_source.startswith("metadata:"):
                        findings.append(
                            AuditFinding(
                                surface="wheel",
                                token=token,
                                diagnostic_code=DIAG_REMOVED_DEPENDENCY_PRESENT,
                                severity="blocking",
                                origin=origin,
                                location=wheel_source,
                                classification="removed",
                                remediation=(
                                    f"'{token}' is ledgered as removed but declared "
                                    "by the explicit wheel metadata; the published "
                                    "artifact must not ship it (rebuild from the "
                                    "cleaned source)."
                                ),
                            )
                        )
                    else:
                        findings.append(
                            AuditFinding(
                                surface="wheel",
                                token=token,
                                diagnostic_code="stale_removed_in_wheel",
                                severity="warning",
                                origin=origin,
                                location=wheel_source,
                                classification="removed",
                                remediation=(
                                    f"'{token}' is removed from the source manifest but "
                                    "still declared by the installed wheel metadata; "
                                    "rebuild/reinstall the wheel to refresh it."
                                ),
                            )
                        )
                else:
                    findings.append(
                        AuditFinding(
                            surface="wheel",
                            token=entry.token,
                            diagnostic_code="ledgered_dependency",
                            severity="accepted",
                            origin=origin,
                            location=wheel_source,
                            classification=entry.classification,
                            remediation=None,
                        )
                    )
        else:
            analyzed.append(f"wheel_skipped({wheel_source})")

    # --- aggregation -----------------------------------------------------------
    violations = tuple(f for f in findings if f.severity == "blocking")
    warnings = tuple(f for f in findings if f.severity == "warning")

    # removed deps confirmed absent from the authoritative surfaces (manifest+lock+source)
    present_tokens = set(manifest_deps) | set(lock_deps or {}) | {
        r for r, _ in source_imports
    }
    removed_dependencies = tuple(
        sorted(
            normalize_token(e.token)
            for e in led
            if e.classification == "removed"
            and normalize_token(e.token) not in present_tokens
        )
    )

    # temporary exceptions actually present in an authoritative surface
    accepted_exceptions = tuple(
        {
            "token": e.token,
            "classification": e.classification,
            "kind": e.kind,
            "owner_wave": e.owner_wave,
            "current_owner": e.current_owner,
            "reason": e.reason,
            "removal_criterion": e.removal_criterion,
            "validation_oracle": e.validation_oracle,
            "direct_dep_no_import": e.direct_dep_no_import,
            "transitive_consumer": e.transitive_consumer,
        }
        for e in led
        if e.classification == "temporary_exception"
        and (
            normalize_token(e.token) in present_tokens
            or any(normalize_token(a) in present_tokens for a in e.aliases)
            or any(
                normalize_token(r) in present_tokens
                for r in e.expected_source_import_roots
            )
        )
    )
    community_owned = tuple(
        sorted(e.token for e in led if e.classification == "community_owned")
    )
    future_adapters = tuple(
        sorted(e.token for e in led if e.classification == "future_adapter")
    )

    ok = not violations and integrity_ok
    return DependencyConformanceReport(
        ledger_version=LEDGER_VERSION,
        ok=ok,
        surfaces_audited=tuple(surfaces),
        analyzed_paths=tuple(analyzed),
        removed_dependencies=removed_dependencies,
        accepted_exceptions=accepted_exceptions,
        community_owned=community_owned,
        future_adapters=future_adapters,
        violations=violations,
        warnings=warnings,
        findings=tuple(findings),
        ledger_integrity_ok=integrity_ok,
    )


def _audit_dependency_surface(
    surface: AuditSurface,
    deps: dict[str, bool],
    governed: frozenset[str],
    index: dict[str, LedgerEntry],
    findings: list[AuditFinding],
) -> None:
    """Classify each governed direct dependency on a manifest/lock surface."""
    for token, is_optional in sorted(deps.items()):
        if token not in governed:
            continue
        origin = "optional_dependency" if is_optional else "direct_dependency"
        entry = index.get(token)
        if entry is None:
            findings.append(
                AuditFinding(
                    surface=surface,
                    token=token,
                    diagnostic_code=DIAG_UNLEDGERED_DEPENDENCY,
                    severity="blocking",
                    origin=origin,
                    location=surface,
                    classification=None,
                    remediation=(
                        "Ledger this concrete edition dependency (owner_wave / "
                        "removal_criterion / validation_oracle) or remove it."
                    ),
                )
            )
        elif entry.classification == "removed":
            findings.append(
                AuditFinding(
                    surface=surface,
                    token=token,
                    diagnostic_code=DIAG_REMOVED_DEPENDENCY_PRESENT,
                    severity="blocking",
                    origin=origin,
                    location=surface,
                    classification="removed",
                    remediation=(
                        f"'{token}' is ledgered as removed; it must not be a core "
                        "direct dependency."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    surface=surface,
                    token=entry.token,
                    diagnostic_code="ledgered_dependency",
                    severity="accepted",
                    origin=origin,
                    location=surface,
                    classification=entry.classification,
                    remediation=None,
                )
            )


# --------------------------------------------------------------------------- #
# Human-readable report (OR-R05E-01 / fr_b916ccaf)
# --------------------------------------------------------------------------- #
def render_report(report: DependencyConformanceReport) -> str:
    """Render the conformance report as actionable, deterministic text."""
    lines: list[str] = []
    status = "CONFORMANT" if report.ok else "NON-CONFORMANT"
    lines.append(f"R05-E dependency conformance: {status}")
    lines.append(f"  ledger_version : {report.ledger_version}")
    lines.append(f"  surfaces       : {', '.join(report.surfaces_audited)}")
    lines.append("  analyzed_paths :")
    for path in report.analyzed_paths:
        lines.append(f"    - {path}")

    lines.append(f"  removed_dependencies ({len(report.removed_dependencies)}):")
    for token in report.removed_dependencies:
        lines.append(f"    - {token} (confirmed absent from manifest+lock+source)")

    lines.append(f"  temporary_exceptions ({len(report.accepted_exceptions)}):")
    for exc in report.accepted_exceptions:
        kind = "direct-dep/no-import" if exc["direct_dep_no_import"] else exc["kind"]
        lines.append(f"    - {exc['token']} [{kind}]")
        lines.append(f"        owner_wave        : {exc['owner_wave']}")
        lines.append(f"        current_owner     : {exc['current_owner']}")
        if exc["transitive_consumer"]:
            lines.append(f"        transitive_consumer: {exc['transitive_consumer']}")
        lines.append(f"        removal_criterion : {exc['removal_criterion']}")
        lines.append(f"        validation_oracle : {exc['validation_oracle']}")

    if report.community_owned:
        lines.append(f"  community_owned : {', '.join(report.community_owned)}")
    if report.future_adapters:
        lines.append(f"  future_adapters : {', '.join(report.future_adapters)}")

    lines.append(f"  violations ({len(report.violations)}):")
    for finding in report.violations:
        lines.append(
            f"    - [{finding.diagnostic_code}] {finding.token} "
            f"({finding.surface}/{finding.origin}) @ {finding.location}"
        )
        if finding.remediation:
            lines.append(f"        remediation: {finding.remediation}")

    if report.warnings:
        lines.append(f"  warnings ({len(report.warnings)}):")
        for finding in report.warnings:
            lines.append(
                f"    - [{finding.diagnostic_code}] {finding.token} "
                f"({finding.surface}) @ {finding.location}"
            )
            if finding.remediation:
                lines.append(f"        remediation: {finding.remediation}")

    return "\n".join(lines)


__all__ = [
    "GOVERNED_TECHNICAL_TOKENS",
    "DIAG_UNLEDGERED_DEPENDENCY",
    "DIAG_UNLEDGERED_IMPORT",
    "DIAG_REMOVED_DEPENDENCY_PRESENT",
    "DIAG_REMOVED_IMPORT_PRESENT",
    "DIAG_LEDGER_ENTRY_INCOMPLETE",
    "DIAG_MISSING_CANONICAL_EXCEPTION",
    "AuditFinding",
    "DependencyConformanceReport",
    "audit_dependency_conformance",
    "render_report",
]
