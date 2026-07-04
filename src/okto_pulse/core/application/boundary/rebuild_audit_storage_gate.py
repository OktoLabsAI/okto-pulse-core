"""AF16 conformance gate for rebuild audit storage ownership.

The core may define storage rules and logical ports. Runtime-specific durable
locations belong to edition adapters, except for the named legacy seam that is
being retired in a later slice.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


_ALLOWLISTED_BASE_DIR_PATH_CONSUMERS = frozenset({
    "candidate_decision_store.py",
    "contingency.py",
    "global_discovery_reindex.py",
    "quarantine.py",
    "rebuild_audit.py",
    "rebuild_confirmation.py",
    "rebuild_generation.py",
    "rebuild_report.py",
    "rebuild_service.py",
    "rebuild_sources.py",
    # MCP resource path loading is a confined read contract for bundled docs,
    # not a rebuild/current-generation durable store.
    "mcp_resources.py",
    "single_writer_lock.py",
    "stress_chaos_executor.py",
    "stress_runner.py",
})

_LEGACY_REBUILD_BASE_DIR_SEAM = ("kg/rebuild_audit.py", "default_rebuild_base_dir")
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


def _is_legacy_rebuild_base_dir_seam(path: str, stack: tuple[ast.AST, ...]) -> bool:
    expected_path, expected_function = _LEGACY_REBUILD_BASE_DIR_SEAM
    return path == expected_path and _nearest_function_name(stack) == expected_function


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


def run_rebuild_audit_storage_gate(
    root: str | Path | None = None,
) -> tuple[RebuildAuditStorageGateViolation, ...]:
    """Return all AF16 storage ownership violations under ``root``.

    ``root`` is expected to be the ``src/okto_pulse/core`` directory. Tests pass
    synthetic roots to prove the gate fails closed.
    """

    scan_root = Path(root) if root is not None else _core_root()
    violations: list[RebuildAuditStorageGateViolation] = []

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

            if isinstance(node, ast.arg) and node.arg == "base_dir":
                if _annotation_names_path(node.annotation) and path.name not in (
                    _ALLOWLISTED_BASE_DIR_PATH_CONSUMERS
                ):
                    violations.append(
                        RebuildAuditStorageGateViolation(
                            rule="base_dir_path_consumer",
                            path=rel,
                            line=line,
                            symbol="base_dir",
                            detail=(
                                "New Path-backed durable storage consumers in core "
                                "must move behind an edition adapter."
                            ),
                        )
                    )

            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                if node.target.id == "base_dir" and _annotation_names_path(
                    node.annotation
                ):
                    if path.name not in _ALLOWLISTED_BASE_DIR_PATH_CONSUMERS:
                        violations.append(
                            RebuildAuditStorageGateViolation(
                                rule="base_dir_path_consumer",
                                path=rel,
                                line=line,
                                symbol="base_dir",
                                detail=(
                                    "New Path-backed durable storage consumers in "
                                    "core must move behind an edition adapter."
                                ),
                            )
                        )

            if isinstance(node, ast.Call):
                call_name = _call_name(node.func)
                if call_name in {"tempfile.gettempdir", "gettempdir"}:
                    if not (
                        _is_legacy_rebuild_base_dir_seam(rel, stack)
                        or _is_allowlisted_tempdir_seam(rel, stack)
                    ):
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
                    if not _is_legacy_rebuild_base_dir_seam(rel, stack):
                        violations.append(
                            RebuildAuditStorageGateViolation(
                                rule="durable_path_env_in_core",
                                path=rel,
                                line=line,
                                symbol=node.value,
                                detail=(
                                    "Core must not read filesystem path env vars "
                                    "for durable rebuild artifacts outside the "
                                    "named legacy seam."
                                ),
                            )
                    )

    return tuple(violations)


__all__ = [
    "RebuildAuditStorageGateViolation",
    "run_rebuild_audit_storage_gate",
]
