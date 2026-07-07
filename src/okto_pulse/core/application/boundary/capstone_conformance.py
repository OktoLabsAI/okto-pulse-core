"""AF33-S1 capstone conformance matrix and executable documentation guard.

This module is deliberately declarative. Core owns the contract matrix and the
checks, but it does not import Community or any concrete local adapter package.
Community tests may pass their reach-in ledger as data.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from .adapter_readiness_inventory import (
    AdapterInventoryEntry,
    build_adapter_inventory,
)
from .core_settings_defaults_gate import (
    run_core_settings_defaults_gate,
    run_public_config_stability_gate,
)
from .dependency_ledger import (
    LedgerEntry,
    build_dependency_ledger,
    entry_missing_fields,
)
from .rebuild_audit_storage_gate import run_rebuild_audit_storage_gate
from .relational_residue_gate import run_relational_residue_gate
from .scheduler_control_symbol_gate import SchedulerControlSymbolGate
from .conformance_suite import scheduler_signal_conformance
from .telemetry_product_ownership_gate import run_telemetry_product_ownership_gate
from .telemetry_sender_ownership_gate import run_telemetry_sender_ownership_gate
from .telemetry_store_ownership_gate import run_telemetry_store_ownership_gate

README_MATRIX_BEGIN = "<!-- AF33-CAPSTONE-MATRIX:BEGIN -->"
README_MATRIX_END = "<!-- AF33-CAPSTONE-MATRIX:END -->"

CAPSTONE_FORBIDDEN_CORE_IMPORT_ROOTS: tuple[str, ...] = (
    "okto_pulse.community",
    "apscheduler",
    "ladybug",
    "kuzu",
    "sentence_transformers",
)

REQUIRED_SWAP_TARGETS: tuple[str, ...] = (
    "SQLite -> Aurora/Postgres",
    "LadybugDB/Kuzu -> Neptune",
    "filesystem -> S3",
    "local telemetry files/API -> AWS telemetry API",
    "APScheduler local runtime -> runtime scheduler adapter",
)


@dataclass(frozen=True)
class CapstoneOwnershipRow:
    """One executable row in the AF33 ownership/swap matrix."""

    surface: str
    core_contract: str
    community_adapter: str
    saas_swap_target: str
    gate_refs: tuple[str, ...]


@dataclass(frozen=True)
class CapstoneViolation:
    """Structured violation emitted by AF33 capstone checks."""

    code: str
    surface: str
    location: str
    detail: str


@dataclass(frozen=True)
class CapstoneReadmeReport:
    ok: bool
    violations: tuple[CapstoneViolation, ...]
    expected_fragment: str


@dataclass(frozen=True)
class CapstoneImportReport:
    ok: bool
    violations: tuple[CapstoneViolation, ...]
    forbidden_roots: tuple[str, ...]


@dataclass(frozen=True)
class CapstoneLedgerReport:
    ok: bool
    violations: tuple[CapstoneViolation, ...]


@dataclass(frozen=True)
class CapstoneIntegrationGateResult:
    gate_id: str
    ok: bool
    detail: str


@dataclass(frozen=True)
class CapstoneIntegrationGateReport:
    ok: bool
    results: tuple[CapstoneIntegrationGateResult, ...]
    violations: tuple[CapstoneViolation, ...]


@dataclass(frozen=True)
class CapstoneConformanceReport:
    ok: bool
    readme: CapstoneReadmeReport
    imports: CapstoneImportReport
    ledgers: CapstoneLedgerReport
    integrations: CapstoneIntegrationGateReport


CAPSTONE_OWNERSHIP_MATRIX: tuple[CapstoneOwnershipRow, ...] = (
    CapstoneOwnershipRow(
        surface="Relational runtime",
        core_contract=(
            "repository/UoW and schema lifecycle ports; no ad-hoc dialect or "
            "engine/session factory bypass"
        ),
        community_adapter=(
            "SQLite/SQLAlchemy adapters in community.adapters.sqlalchemy_* and "
            "relational_schema_lifecycle"
        ),
        saas_swap_target="SQLite -> Aurora/Postgres",
        gate_refs=(
            "run_relational_residue_gate",
            "audit_dependency_conformance",
            "audit_community_core_import_boundary",
        ),
    ),
    CapstoneOwnershipRow(
        surface="KG graph runtime",
        core_contract=(
            "KG interfaces, policies and adapter-neutral schema compatibility helpers"
        ),
        community_adapter=(
            "LadybugDB/Kuzu adapters in community.adapters.kuzu_* and "
            "global_discovery_runtime"
        ),
        saas_swap_target="LadybugDB/Kuzu -> Neptune",
        gate_refs=(
            "audit_dependency_conformance",
            "ImportBoundaryGate",
            "audit_community_core_import_boundary",
        ),
    ),
    CapstoneOwnershipRow(
        surface="Durable files and artifacts",
        core_contract=(
            "StorageProvider, RebuildAuditArtifactStore and "
            "CognitivePendingWorkProvider contracts"
        ),
        community_adapter=(
            "filesystem storage, upload_dir, rebuild audit storage and "
            "cognitive-pending providers"
        ),
        saas_swap_target="filesystem -> S3",
        gate_refs=(
            "run_rebuild_audit_storage_gate",
            "run_core_settings_defaults_gate",
            "run_public_config_stability_gate",
        ),
    ),
    CapstoneOwnershipRow(
        surface="Telemetry effects",
        core_contract="TelemetryPort contracts, event schema and privacy policy",
        community_adapter=(
            "local JSONL store, state files, beacon sender and product telemetry adapters"
        ),
        saas_swap_target="local telemetry files/API -> AWS telemetry API",
        gate_refs=(
            "run_telemetry_store_ownership_gate",
            "run_telemetry_sender_ownership_gate",
            "run_telemetry_product_ownership_gate",
        ),
    ),
    CapstoneOwnershipRow(
        surface="Scheduler/runtime effects",
        core_contract="JobSpec, SchedulerControl and KG daily tick policy",
        community_adapter="APScheduler-backed SingletonSchedulerControl",
        saas_swap_target="APScheduler local runtime -> runtime scheduler adapter",
        gate_refs=("SchedulerControlSymbolGate", "scheduler_signal_conformance"),
    ),
    CapstoneOwnershipRow(
        surface="MCP resources and versions",
        core_contract=(
            "MCP instruction/resource/version provider ports and stable public catalog"
        ),
        community_adapter=(
            "Community resource catalog, capability descriptors and package version wiring"
        ),
        saas_swap_target="local catalog/version reads -> deployment provider",
        gate_refs=(
            "run_public_config_stability_gate",
            "register_instruction_provider",
            "register_package_version_provider",
        ),
    ),
)


def render_capstone_ownership_matrix(
    rows: Sequence[CapstoneOwnershipRow] = CAPSTONE_OWNERSHIP_MATRIX,
) -> str:
    """Render the canonical AF33 matrix for README inclusion."""

    lines = [
        README_MATRIX_BEGIN,
        "| Surface | Core contract | Community/local adapter | SaaS swap target | Executable gates |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                (
                    _cell(row.surface),
                    _cell(row.core_contract),
                    _cell(row.community_adapter),
                    _cell(row.saas_swap_target),
                    _cell(", ".join(row.gate_refs)),
                )
            )
            + " |"
        )
    lines.append(README_MATRIX_END)
    return "\n".join(lines)


def validate_capstone_readme_sync(
    *,
    core_readme: str,
    community_readme: str,
    expected_fragment: str | None = None,
) -> CapstoneReadmeReport:
    """Fail when either README drifts from the executable AF33 matrix."""

    expected = expected_fragment or render_capstone_ownership_matrix()
    fragments: dict[str, str] = {}
    violations: list[CapstoneViolation] = []
    for name, text in (("core README", core_readme), ("community README", community_readme)):
        fragment = _extract_marked_matrix(text)
        if fragment is None:
            violations.append(
                CapstoneViolation(
                    code="readme_matrix_missing",
                    surface="readme",
                    location=name,
                    detail=(
                        f"Missing {README_MATRIX_BEGIN} / {README_MATRIX_END} "
                        "capstone matrix markers."
                    ),
                )
            )
            continue
        fragments[name] = fragment
        if fragment != expected:
            violations.append(
                CapstoneViolation(
                    code="readme_matrix_mismatch",
                    surface="readme",
                    location=name,
                    detail="README matrix differs from CAPSTONE_OWNERSHIP_MATRIX.",
                )
            )
    if len(fragments) == 2 and fragments["core README"] != fragments["community README"]:
        violations.append(
            CapstoneViolation(
                code="readme_cross_repo_mismatch",
                surface="readme",
                location="core README <-> community README",
                detail="Core and Community README capstone matrices are not identical.",
            )
        )
    return CapstoneReadmeReport(
        ok=not violations,
        violations=tuple(violations),
        expected_fragment=expected,
    )


def audit_core_capstone_imports(
    source_root: str | Path,
    *,
    forbidden_roots: tuple[str, ...] = CAPSTONE_FORBIDDEN_CORE_IMPORT_ROOTS,
) -> CapstoneImportReport:
    """Scan production core source for Community/concrete local adapter imports."""

    root = _core_package_root(Path(source_root))
    violations: list[CapstoneViolation] = []
    for py_file in sorted(root.rglob("*.py")):
        if "__pycache__" in py_file.parts:
            continue
        rel = py_file.relative_to(root).as_posix()
        tree = ast.parse(py_file.read_text(encoding="utf-8"))
        visitor = _CoreImportVisitor(rel, forbidden_roots)
        visitor.visit(tree)
        violations.extend(visitor.violations)
    return CapstoneImportReport(
        ok=not violations,
        violations=tuple(violations),
        forbidden_roots=forbidden_roots,
    )


def audit_capstone_ledgers(
    *,
    dependency_ledger: Sequence[LedgerEntry] | None = None,
    adapter_inventory: Sequence[AdapterInventoryEntry] | None = None,
    community_reach_in_ledger: Iterable[Any] = (),
) -> CapstoneLedgerReport:
    """Validate owner/source/coverage/removal fields for AF33 ledger exceptions."""

    violations: list[CapstoneViolation] = []
    for entry in dependency_ledger if dependency_ledger is not None else build_dependency_ledger():
        for field_name in entry_missing_fields(entry):
            violations.append(
                CapstoneViolation(
                    code="dependency_ledger_missing_field",
                    surface="dependency_ledger",
                    location=entry.token,
                    detail=field_name,
                )
            )
    for entry in adapter_inventory if adapter_inventory is not None else build_adapter_inventory():
        for field_name in _missing_string_attrs(
            entry,
            (
                "adapter_key",
                "owner",
                "current_module",
                "port_ref",
                "wave",
                "target_destination",
                "removal_criterion",
                "status",
            ),
        ):
            violations.append(
                CapstoneViolation(
                    code="adapter_inventory_missing_field",
                    surface="adapter_readiness_inventory",
                    location=_value(entry, "adapter_key") or "<unknown>",
                    detail=field_name,
                )
            )
        if not _value(entry, "predecessor_refs") or not _value(entry, "oracles_required"):
            violations.append(
                CapstoneViolation(
                    code="adapter_inventory_missing_coverage",
                    surface="adapter_readiness_inventory",
                    location=_value(entry, "adapter_key") or "<unknown>",
                    detail="predecessor_refs/oracles_required",
                )
            )
    for entry in community_reach_in_ledger:
        location = (
            f"{_value(entry, 'file_path') or '<unknown>'}:"
            f"{_value(entry, 'scope') or '<unknown>'}"
        )
        for field_name in _missing_string_attrs(
            entry,
            (
                "file_path",
                "scope",
                "module",
                "category",
                "owner",
                "reason",
                "target_public_surface",
                "removal_path",
                "withdrawal_criterion",
            ),
        ):
            violations.append(
                CapstoneViolation(
                    code="community_reach_in_ledger_missing_field",
                    surface="community_core_reach_in_ledger",
                    location=location,
                    detail=field_name,
                )
            )
        if not _value(entry, "symbols"):
            violations.append(
                CapstoneViolation(
                    code="community_reach_in_ledger_missing_symbols",
                    surface="community_core_reach_in_ledger",
                    location=location,
                    detail="symbols",
                )
            )
    return CapstoneLedgerReport(ok=not violations, violations=tuple(violations))


def audit_capstone_integration_gates(
    *,
    core_source_root: str | Path,
    community_source_root: str | Path | None = None,
) -> CapstoneIntegrationGateReport:
    """Execute the AF29/AF30/AF31 gates the AF33 matrix depends on."""

    core_root = _core_package_root(Path(core_source_root))
    source_root = _source_root_for_core_package(core_root)
    community_root = (
        _community_package_root(Path(community_source_root))
        if community_source_root is not None
        else None
    )
    results: list[CapstoneIntegrationGateResult] = []
    violations: list[CapstoneViolation] = []

    def add(gate_id: str, ok: bool, detail: str) -> None:
        results.append(CapstoneIntegrationGateResult(gate_id=gate_id, ok=ok, detail=detail))
        if not ok:
            violations.append(
                CapstoneViolation(
                    code="capstone_integration_gate_failed",
                    surface="integration_gate",
                    location=gate_id,
                    detail=detail,
                )
            )

    rebuild_violations = run_rebuild_audit_storage_gate(core_root)
    add(
        "run_rebuild_audit_storage_gate",
        not rebuild_violations,
        f"violations={len(rebuild_violations)}",
    )

    relational = run_relational_residue_gate(
        core_root=core_root,
        community_root=community_root,
    )
    add(
        "run_relational_residue_gate",
        relational.ok,
        f"findings={len(relational.findings)}",
    )

    scheduler = SchedulerControlSymbolGate().run(source_root=source_root)
    add("SchedulerControlSymbolGate", _gate_report_ok(scheduler), f"status={scheduler.status}")

    signal = scheduler_signal_conformance()
    add("scheduler_signal_conformance", _gate_report_ok(signal), f"status={signal.status}")

    settings = run_core_settings_defaults_gate(source_root=source_root)
    add(
        "run_core_settings_defaults_gate",
        _gate_report_ok(settings),
        f"status={settings.status}",
    )

    public_config = run_public_config_stability_gate(source_root=source_root)
    add(
        "run_public_config_stability_gate",
        _gate_report_ok(public_config),
        f"status={public_config.status}",
    )

    telemetry_store = run_telemetry_store_ownership_gate(core_root)
    add(
        "run_telemetry_store_ownership_gate",
        telemetry_store.ok,
        f"violations={len(telemetry_store.violations)}",
    )

    telemetry_sender = run_telemetry_sender_ownership_gate(core_root)
    add(
        "run_telemetry_sender_ownership_gate",
        telemetry_sender.ok,
        f"violations={len(telemetry_sender.violations)}",
    )

    telemetry_product = run_telemetry_product_ownership_gate(core_root)
    add(
        "run_telemetry_product_ownership_gate",
        telemetry_product.ok,
        f"violations={len(telemetry_product.violations)}",
    )

    return CapstoneIntegrationGateReport(
        ok=not violations,
        results=tuple(results),
        violations=tuple(violations),
    )


def build_capstone_conformance_report(
    *,
    core_source_root: str | Path,
    core_readme: str,
    community_readme: str,
    community_source_root: str | Path | None = None,
    community_reach_in_ledger: Iterable[Any] = (),
) -> CapstoneConformanceReport:
    """Run the AF33 capstone checks that do not require importing Community."""

    readme = validate_capstone_readme_sync(
        core_readme=core_readme,
        community_readme=community_readme,
    )
    imports = audit_core_capstone_imports(core_source_root)
    ledgers = audit_capstone_ledgers(community_reach_in_ledger=community_reach_in_ledger)
    integrations = audit_capstone_integration_gates(
        core_source_root=core_source_root,
        community_source_root=community_source_root,
    )
    return CapstoneConformanceReport(
        ok=readme.ok and imports.ok and ledgers.ok and integrations.ok,
        readme=readme,
        imports=imports,
        ledgers=ledgers,
        integrations=integrations,
    )


def _cell(value: str) -> str:
    return " ".join(value.strip().replace("|", "/").split())


def _extract_marked_matrix(text: str) -> str | None:
    start = text.find(README_MATRIX_BEGIN)
    if start == -1:
        return None
    end = text.find(README_MATRIX_END, start)
    if end == -1:
        return None
    end += len(README_MATRIX_END)
    return text[start:end].strip()


def _core_package_root(source_root: Path) -> Path:
    candidate = source_root / "src" / "okto_pulse" / "core"
    if candidate.exists():
        return candidate
    candidate = source_root / "okto_pulse" / "core"
    if candidate.exists():
        return candidate
    return source_root


def _community_package_root(source_root: Path) -> Path:
    candidate = source_root / "src" / "okto_pulse" / "community"
    if candidate.exists():
        return candidate
    candidate = source_root / "okto_pulse" / "community"
    if candidate.exists():
        return candidate
    return source_root


def _source_root_for_core_package(core_root: Path) -> Path:
    if core_root.name == "core" and core_root.parent.name == "okto_pulse":
        return core_root.parent.parent
    return core_root


def _gate_report_ok(report: Any) -> bool:
    return str(getattr(report, "status", "")) not in {"blocking", "reject"}


def _missing_string_attrs(entry: Any, attrs: tuple[str, ...]) -> tuple[str, ...]:
    missing: list[str] = []
    for attr in attrs:
        value = _value(entry, attr)
        if not (isinstance(value, str) and value.strip()):
            missing.append(attr)
    return tuple(missing)


def _value(entry: Any, attr: str) -> Any:
    if isinstance(entry, dict):
        return entry.get(attr)
    return getattr(entry, attr, None)


def _matches_forbidden_root(imported: str, forbidden_roots: tuple[str, ...]) -> bool:
    return any(
        imported == root or imported.startswith(root + ".")
        for root in forbidden_roots
    )


class _CoreImportVisitor(ast.NodeVisitor):
    def __init__(self, rel_path: str, forbidden_roots: tuple[str, ...]) -> None:
        self.rel_path = rel_path
        self.forbidden_roots = forbidden_roots
        self.violations: list[CapstoneViolation] = []

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self._record_if_forbidden(alias.name, node.lineno)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module:
            self._record_if_forbidden(node.module, node.lineno)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        imported = _literal_dynamic_import(node)
        if imported:
            self._record_if_forbidden(imported, node.lineno)
        self.generic_visit(node)

    def _record_if_forbidden(self, imported: str, line: int) -> None:
        if not _matches_forbidden_root(imported, self.forbidden_roots):
            return
        self.violations.append(
            CapstoneViolation(
                code="core_concrete_adapter_import",
                surface="core_source",
                location=f"{self.rel_path}:{line}",
                detail=imported,
            )
        )


def _literal_dynamic_import(node: ast.Call) -> str | None:
    if not node.args:
        return None
    target = node.args[0]
    if not (isinstance(target, ast.Constant) and isinstance(target.value, str)):
        return None
    func = node.func
    if isinstance(func, ast.Name) and func.id == "__import__":
        return target.value
    if (
        isinstance(func, ast.Attribute)
        and func.attr == "import_module"
        and isinstance(func.value, ast.Name)
        and func.value.id == "importlib"
    ):
        return target.value
    if isinstance(func, ast.Name) and func.id == "import_module":
        return target.value
    return None


__all__ = [
    "README_MATRIX_BEGIN",
    "README_MATRIX_END",
    "CAPSTONE_FORBIDDEN_CORE_IMPORT_ROOTS",
    "CAPSTONE_OWNERSHIP_MATRIX",
    "REQUIRED_SWAP_TARGETS",
    "CapstoneOwnershipRow",
    "CapstoneViolation",
    "CapstoneReadmeReport",
    "CapstoneImportReport",
    "CapstoneLedgerReport",
    "CapstoneIntegrationGateResult",
    "CapstoneIntegrationGateReport",
    "CapstoneConformanceReport",
    "render_capstone_ownership_matrix",
    "validate_capstone_readme_sync",
    "audit_core_capstone_imports",
    "audit_capstone_ledgers",
    "audit_capstone_integration_gates",
    "build_capstone_conformance_report",
]
