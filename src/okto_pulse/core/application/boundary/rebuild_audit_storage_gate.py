"""AF16 conformance gate for rebuild audit storage ownership.

The core may define storage rules and logical ports. Runtime-specific durable
locations belong to edition adapters.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


_FORBIDDEN_REBUILD_ROOT_HELPER = "default_" + "rebuild_base_dir"
_FORBIDDEN_REBUILD_ROOT_SYMBOLS = frozenset({
    "_REBUILD_" + "BASE_DIR",
    "_LEGACY_" + "REBUILD_BASE_DIR_SEAM",
})
_ALLOWLISTED_TEMPDIR_SEAMS = frozenset({
    # Full-clean-core smoke checks whether the OS temp root is writable before
    # it creates a disposable venv. It is an ephemeral prerequisite probe.
    (
        "application/boundary/final_clean_core_full_smoke.py",
        "detect_full_prerequisites",
    ),
})
_PATH_ENV_KEYS = frozenset({"OKTO_PULSE_" + "REBUILD_BASE_DIR"})


@dataclass(frozen=True, slots=True)
class RebuildAuditStorageGateViolation:
    """One fail-closed storage-boundary violation."""

    rule: str
    path: str
    line: int
    symbol: str
    detail: str


@dataclass(frozen=True, slots=True)
class RebuildAuditStorageLedgerEntry:
    """One governed residual ``base_dir: Path`` compatibility seam."""

    path: str
    kind: str
    symbol: str
    classification: str
    owner: str
    reason: str
    removal_criterion: str

    def key(self) -> tuple[str, str, str]:
        return (self.path, self.kind, self.symbol)


_BASE_DIR_LEDGER: tuple[RebuildAuditStorageLedgerEntry, ...] = (
    RebuildAuditStorageLedgerEntry(
        path="kg/candidate_decision_store.py",
        kind="field",
        symbol="CandidateDecisionStore.base_dir",
        classification="legacy_compat_injection",
        owner="AF38-R1",
        reason=(
            "Explicit base_dir keeps historical local tests and migration callers "
            "working; production default resolves through RebuildAuditArtifactStore."
        ),
        removal_criterion=(
            "Remove when all candidate-decision callers pass artifact_store or use "
            "the edition registry."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/contingency.py",
        kind="field",
        symbol="KGStorageBackendContingency.base_dir",
        classification="legacy_compat_injection",
        owner="AF38-R1",
        reason=(
            "Explicit base_dir preserves compatibility for local contingency "
            "manifest tests; omitted base_dir now resolves the registered store."
        ),
        removal_criterion=(
            "Remove after Community and tests construct contingency storage only "
            "through artifact_store/provider."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/global_discovery_reindex.py",
        kind="field",
        symbol="GlobalDiscoveryReindexStatusStore.base_dir",
        classification="legacy_compat_injection",
        owner="AF38-R1",
        reason=(
            "Explicit base_dir preserves historical reindex status files; "
            "productive default uses the registered artifact store namespace."
        ),
        removal_criterion=(
            "Remove after all reindex status callers inject artifact_store or "
            "composition has no direct path constructors."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/quarantine.py",
        kind="arg",
        symbol="KGQuarantineService.__init__.base_dir",
        classification="quarantine_compat_bridge",
        owner="AF38-R1",
        reason=(
            "Quarantine still accepts an explicit local base for compatibility with "
            "existing local recovery flows while artifact refs use the store."
        ),
        removal_criterion=(
            "Remove when quarantine file movement is fully owned by Community or "
            "a dedicated quarantine port."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_audit.py",
        kind="arg",
        symbol="_require_base_dir.base_dir",
        classification="compat_guard_helper",
        owner="AF38-R1",
        reason=(
            "Helper fails closed when an explicit legacy base_dir path is used "
            "without a path; it does not resolve a local root."
        ),
        removal_criterion=(
            "Remove when legacy explicit-base branches are deleted from rebuild "
            "audit stores."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_audit.py",
        kind="arg",
        symbol="resolve_rebuild_audit_artifact_store.base_dir",
        classification="compat_guard_helper",
        owner="AF38-R1",
        reason=(
            "Resolver distinguishes explicit compatibility paths from productive "
            "registry-backed artifact IO."
        ),
        removal_criterion=(
            "Remove the parameter when no rebuild audit class accepts base_dir."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_audit.py",
        kind="field",
        symbol="KGRebuiltEventPublisher.base_dir",
        classification="legacy_compat_injection",
        owner="AF38-R1",
        reason=(
            "Explicit path branch preserves existing audit event layout tests; "
            "default construction requires the registry store."
        ),
        removal_criterion=(
            "Remove when tests and compatibility callers use RebuildAuditArtifactStore."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_audit.py",
        kind="field",
        symbol="CognitiveConsolidationItemStore.base_dir",
        classification="legacy_compat_injection",
        owner="AF38-R1",
        reason=(
            "Explicit base_dir keeps historical cognitive pending files readable; "
            "productive default uses the store namespace."
        ),
        removal_criterion=(
            "Remove after cognitive pending readers/writers use artifact_store or "
            "a Community provider exclusively."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_audit.py",
        kind="arg",
        symbol="record_cognitive_working_only_hold.base_dir",
        classification="legacy_compat_injection",
        owner="AF38-R1",
        reason=(
            "Function-level compatibility seam forwards explicit local paths to "
            "the cognitive pending store for old tests."
        ),
        removal_criterion=(
            "Remove after callers pass artifact_store or rely on the registry."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_audit.py",
        kind="field",
        symbol="CognitivePendingMarker.base_dir",
        classification="legacy_compat_injection",
        owner="AF38-R1",
        reason=(
            "Explicit path branch preserves old marker tests; default path is "
            "registry-backed and fail-closed."
        ),
        removal_criterion=(
            "Remove when marker compatibility tests use artifact_store."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_audit.py",
        kind="field",
        symbol="ConfirmationConsumptionAuditRecorder.base_dir",
        classification="legacy_compat_injection",
        owner="AF38-R1",
        reason=(
            "Explicit path branch preserves old confirmation audit tests; default "
            "path is registry-backed and fail-closed."
        ),
        removal_criterion=(
            "Remove when confirmation audit compatibility tests use artifact_store."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_confirmation.py",
        kind="field",
        symbol="RebuildConfirmationStore.base_dir",
        classification="legacy_compat_injection",
        owner="AF38-R1",
        reason=(
            "Explicit base_dir supports existing local confirmation token data; "
            "productive default resolves the registered artifact store."
        ),
        removal_criterion=(
            "Remove when confirmation store consumers use artifact refs only."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_generation.py",
        kind="field",
        symbol="KGGenerationRepository.base_dir",
        classification="legacy_filesystem_repository",
        owner="AF38-R1",
        reason=(
            "Legacy repository remains as explicit compatibility implementation; "
            "new productive default is RebuildAuditKGGenerationRepository."
        ),
        removal_criterion=(
            "Remove when no production or compatibility caller constructs "
            "KGGenerationRepository directly."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_report.py",
        kind="field",
        symbol="RebuildReportStore.base_dir",
        classification="legacy_compat_injection",
        owner="AF38-R1",
        reason=(
            "Explicit path branch keeps local report compatibility; productive "
            "default uses the artifact store."
        ),
        removal_criterion=(
            "Remove when report tests/callers use artifact_store exclusively."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_service.py",
        kind="field",
        symbol="KGRebuildService.base_dir",
        classification="orchestration_compat_bridge",
        owner="AF38-R1",
        reason=(
            "Service still carries an explicit compatibility base for old local "
            "artifact branches while store-backed paths are injected."
        ),
        removal_criterion=(
            "Remove after rebuild service no longer forwards base_dir to any "
            "legacy filesystem implementation."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_sources.py",
        kind="arg",
        symbol="_rebaseline_audit_path.base_dir",
        classification="legacy_compat_helper",
        owner="AF38-R1",
        reason=(
            "Path builder is confined to explicit compatibility branch for "
            "rebaseline audit files."
        ),
        removal_criterion=(
            "Remove with the explicit base_dir rebaseline audit branch."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_sources.py",
        kind="arg",
        symbol="_append_spec_manifest_rebaseline_audit.base_dir",
        classification="legacy_compat_injection",
        owner="AF38-R1",
        reason=(
            "Append helper first uses artifact_store when no explicit base_dir is "
            "passed; base_dir remains compatibility only."
        ),
        removal_criterion=(
            "Remove after rebaseline audit tests/callers use artifact_store."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_sources.py",
        kind="arg",
        symbol="read_spec_manifest_rebaseline_audit.base_dir",
        classification="legacy_compat_injection",
        owner="AF38-R1",
        reason=(
            "Reader helper first uses artifact_store when no explicit base_dir is "
            "passed; base_dir remains compatibility only."
        ),
        removal_criterion=(
            "Remove after rebaseline audit reads use artifact_store exclusively."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/rebuild_sources.py",
        kind="field",
        symbol="KGRebuildSourceManifest.base_dir",
        classification="legacy_compat_injection",
        owner="AF38-R1",
        reason=(
            "Explicit path branch preserves source manifest compatibility; "
            "productive default is registry-backed."
        ),
        removal_criterion=(
            "Remove when source manifest callers use artifact_store or registry."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/single_writer_lock.py",
        kind="arg",
        symbol="KGSingleWriterLock.__init__.base_dir",
        classification="coordination_lock_compat",
        owner="AF38-R1",
        reason=(
            "Single-writer lock path is a coordination compatibility seam, not a "
            "rebuild artifact store."
        ),
        removal_criterion=(
            "Remove when KG writer coordination is fully adapter-owned."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/stress_chaos_executor.py",
        kind="arg",
        symbol="KGChaosExecutor.__init__.base_dir",
        classification="stress_chaos_evidence",
        owner="AF38-R1",
        reason=(
            "Chaos executor writes disposable test/evidence files and is outside "
            "productive rebuild artifact storage."
        ),
        removal_criterion=(
            "Remove if chaos evidence is moved behind an explicit evidence store."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="kg/stress_runner.py",
        kind="field",
        symbol="KGStressProfileRunner.base_dir",
        classification="stress_chaos_evidence",
        owner="AF38-R1",
        reason=(
            "Stress runner writes disposable evidence directories, not productive "
            "rebuild artifacts."
        ),
        removal_criterion=(
            "Remove if stress evidence is moved behind an explicit evidence store."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="ports/mcp_instructions.py",
        kind="field",
        symbol="StaticFileMcpInstructionProvider.base_dir",
        classification="static_bundled_resource_path",
        owner="AF41-MCP-runtime",
        reason=(
            "MCP instruction path loading is confined to static bundled content, "
            "not rebuild artifact IO."
        ),
        removal_criterion=(
            "Remove when MCP instruction loading is fully package-metadata/provider "
            "based without path loaders."
        ),
    ),
    RebuildAuditStorageLedgerEntry(
        path="ports/mcp_resources.py",
        kind="field",
        symbol="McpResourceSpec.base_dir",
        classification="static_bundled_resource_path",
        owner="AF41-MCP-runtime",
        reason=(
            "MCP resource path loading is confined to static bundled content, not "
            "rebuild artifact IO."
        ),
        removal_criterion=(
            "Remove when MCP resources are loaded only through provider/package "
            "metadata without local path loaders."
        ),
    ),
)

_BASE_DIR_LEDGER_BY_KEY = {entry.key(): entry for entry in _BASE_DIR_LEDGER}


def _core_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _py_files(root: Path) -> Iterator[Path]:
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" not in path.parts:
            yield path


def _relative(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def _walk_with_stack(
    node: ast.AST,
    stack: tuple[ast.AST, ...] = (),
) -> Iterator[tuple[ast.AST, tuple[ast.AST, ...]]]:
    yield node, stack
    next_stack = stack + (node,)
    for child in ast.iter_child_nodes(node):
        yield from _walk_with_stack(child, next_stack)


def _nearest_function_name(stack: tuple[ast.AST, ...]) -> str | None:
    for parent in reversed(stack):
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return parent.name
    return None


def _nearest_class_name(stack: tuple[ast.AST, ...]) -> str | None:
    for parent in reversed(stack):
        if isinstance(parent, ast.ClassDef):
            return parent.name
    return None


def _base_dir_consumer_symbol(
    node: ast.AST,
    stack: tuple[ast.AST, ...],
) -> tuple[str, str] | None:
    if isinstance(node, ast.arg) and node.arg == "base_dir":
        if not _annotation_names_path(node.annotation):
            return None
        function_name = _nearest_function_name(stack) or "<module>"
        class_name = _nearest_class_name(stack)
        owner = f"{class_name}.{function_name}" if class_name else function_name
        return "arg", f"{owner}.base_dir"
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        if node.target.id != "base_dir" or not _annotation_names_path(node.annotation):
            return None
        class_name = _nearest_class_name(stack) or "<module>"
        return "field", f"{class_name}.base_dir"
    return None


def _is_real_core_root(scan_root: Path) -> bool:
    try:
        return scan_root.resolve() == _core_root().resolve()
    except OSError:
        return False


def _is_allowlisted_tempdir_seam(path: str, stack: tuple[ast.AST, ...]) -> bool:
    return (path, _nearest_function_name(stack)) in _ALLOWLISTED_TEMPDIR_SEAMS


def _annotation_names_path(annotation: ast.AST | None) -> bool:
    if annotation is None:
        return False
    try:
        return "Path" in ast.unparse(annotation)
    except Exception:
        return False


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _call_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return ""


def _forbidden_rebuild_root_symbol(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name) and node.id in _FORBIDDEN_REBUILD_ROOT_SYMBOLS:
        return node.id
    if (
        isinstance(node, ast.Attribute)
        and node.attr in _FORBIDDEN_REBUILD_ROOT_SYMBOLS
    ):
        return node.attr
    if isinstance(node, ast.alias) and node.name in _FORBIDDEN_REBUILD_ROOT_SYMBOLS:
        return node.name
    return None


def _ledger_integrity_violations() -> list[RebuildAuditStorageGateViolation]:
    violations: list[RebuildAuditStorageGateViolation] = []
    seen: set[tuple[str, str, str]] = set()
    for entry in _BASE_DIR_LEDGER:
        location = f"{entry.path}:{entry.symbol}"
        for field_name in (
            "path",
            "kind",
            "symbol",
            "classification",
            "owner",
            "reason",
            "removal_criterion",
        ):
            if not getattr(entry, field_name).strip():
                violations.append(
                    RebuildAuditStorageGateViolation(
                        rule="base_dir_ledger_missing_field",
                        path=entry.path or "<ledger>",
                        line=1,
                        symbol=entry.symbol or "<unknown>",
                        detail=f"{location} missing {field_name}",
                    )
                )
        key = entry.key()
        if key in seen:
            violations.append(
                RebuildAuditStorageGateViolation(
                    rule="base_dir_ledger_duplicate_key",
                    path=entry.path,
                    line=1,
                    symbol=entry.symbol,
                    detail=f"Duplicate base_dir ledger key: {key!r}",
                )
            )
        seen.add(key)
    return violations


def rebuild_audit_storage_fallback_ledger() -> tuple[RebuildAuditStorageLedgerEntry, ...]:
    """Return the governed residual AF38 filesystem compatibility ledger."""

    return _BASE_DIR_LEDGER


def run_rebuild_audit_storage_gate(
    root: str | Path | None = None,
    *,
    enforce_stale_ledger: bool | None = None,
) -> tuple[RebuildAuditStorageGateViolation, ...]:
    """Return all AF16 storage ownership violations under ``root``.

    ``root`` is expected to be the ``src/okto_pulse/core`` directory. Tests pass
    synthetic roots to prove the gate fails closed.
    """

    scan_root = Path(root) if root is not None else _core_root()
    violations: list[RebuildAuditStorageGateViolation] = _ledger_integrity_violations()
    observed_ledger_keys: set[tuple[str, str, str]] = set()
    if enforce_stale_ledger is None:
        enforce_stale_ledger = _is_real_core_root(scan_root)

    for path in _py_files(scan_root):
        rel = _relative(path, scan_root)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

        for node, stack in _walk_with_stack(tree):
            line = getattr(node, "lineno", 1)

            if isinstance(node, ast.ClassDef) and node.name == "DurableArtifactStore":
                violations.append(
                    RebuildAuditStorageGateViolation(
                        rule="parallel_durable_artifact_store",
                        path=rel,
                        line=line,
                        symbol=node.name,
                        detail=(
                            "Do not introduce DurableArtifactStore as a parallel "
                            "port; extend RebuildAuditArtifactStore instead."
                        ),
                    )
                )

            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == _FORBIDDEN_REBUILD_ROOT_HELPER
            ):
                violations.append(
                    RebuildAuditStorageGateViolation(
                        rule="legacy_rebuild_base_dir_helper",
                        path=rel,
                        line=line,
                        symbol=node.name,
                        detail=(
                            "Core must not own a rebuild base-dir resolver; "
                            "local roots belong to the edition adapter."
                        ),
                    )
                )

            forbidden_symbol = _forbidden_rebuild_root_symbol(node)
            if forbidden_symbol is not None:
                violations.append(
                    RebuildAuditStorageGateViolation(
                        rule="rebuild_root_symbol_in_core",
                        path=rel,
                        line=line,
                        symbol=forbidden_symbol,
                        detail=(
                            "Core must not retain rebuild root symbols; "
                            "local roots belong to the edition adapter."
                        ),
                    )
                )

            base_dir_consumer = _base_dir_consumer_symbol(node, stack)
            if base_dir_consumer is not None:
                kind, symbol = base_dir_consumer
                key = (rel, kind, symbol)
                if key in _BASE_DIR_LEDGER_BY_KEY:
                    observed_ledger_keys.add(key)
                else:
                    violations.append(
                        RebuildAuditStorageGateViolation(
                            rule="base_dir_path_consumer",
                            path=rel,
                            line=line,
                            symbol=symbol,
                            detail=(
                                "Path-backed rebuild/artifact consumers in core "
                                "must either move behind an edition adapter or be "
                                "recorded in the AF38 ledger with owner, "
                                "classification and removal criterion."
                            ),
                        )
                    )

            if isinstance(node, ast.Call):
                call_name = _call_name(node.func)
                if call_name.rsplit(".", 1)[-1] == _FORBIDDEN_REBUILD_ROOT_HELPER:
                    violations.append(
                        RebuildAuditStorageGateViolation(
                            rule="legacy_rebuild_base_dir_helper",
                            path=rel,
                            line=line,
                            symbol=call_name,
                            detail=(
                                "Core must use RebuildAuditArtifactStore from "
                                "the edition registry, not a rebuild base-dir "
                                "resolver."
                            ),
                        )
                    )
                tempdir_symbols = {
                    "tempfile." + "gettempdir",
                    "get" + "tempdir",
                }
                if call_name in tempdir_symbols:
                    if not _is_allowlisted_tempdir_seam(rel, stack):
                        violations.append(
                            RebuildAuditStorageGateViolation(
                                rule="durable_tempdir_in_core",
                                path=rel,
                                line=line,
                                symbol=call_name,
                                detail=(
                                    "Core must not choose a durable tempdir; "
                                    "resolve local filesystem roots in Community."
                                ),
                            )
                        )

            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if node.value in _PATH_ENV_KEYS:
                    violations.append(
                        RebuildAuditStorageGateViolation(
                            rule="durable_path_env_in_core",
                            path=rel,
                            line=line,
                            symbol=node.value,
                            detail=(
                                "Core must not read filesystem path env vars "
                                "for durable rebuild artifacts."
                            ),
                        )
                    )
                if node.value in _FORBIDDEN_REBUILD_ROOT_SYMBOLS:
                    violations.append(
                        RebuildAuditStorageGateViolation(
                            rule="rebuild_root_symbol_in_core",
                            path=rel,
                            line=line,
                            symbol=node.value,
                            detail=(
                                "Core must not retain rebuild root symbols; "
                                "local roots belong to the edition adapter."
                            ),
                        )
                    )

    if enforce_stale_ledger:
        for entry in _BASE_DIR_LEDGER:
            if entry.key() not in observed_ledger_keys:
                violations.append(
                    RebuildAuditStorageGateViolation(
                        rule="stale_base_dir_ledger_entry",
                        path=entry.path,
                        line=1,
                        symbol=entry.symbol,
                        detail=(
                            "AF38 base_dir ledger entry no longer matches source; "
                            "remove or update the ledger as the allowlist shrinks."
                        ),
                    )
                )

    return tuple(violations)


__all__ = [
    "RebuildAuditStorageLedgerEntry",
    "RebuildAuditStorageGateViolation",
    "rebuild_audit_storage_fallback_ledger",
    "run_rebuild_audit_storage_gate",
]
