"""Boundary gate for R12 telemetry state ownership.

Productive core code may consume the registered full-dict carrier, but it must
not read/write ``metrics_dir/state.json`` directly or import Community storage.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

_FORBIDDEN_FILE_METHODS = {"read_text", "write_text", "open", "mkdir"}
_SENSITIVE_RELATIVE_PATHS = {
    Path("telemetry/settings.py"),
    Path("telemetry/service.py"),
}


@dataclass(frozen=True)
class TelemetryStateBoundaryViolation:
    path: str
    lineno: int
    reason: str


@dataclass(frozen=True)
class TelemetryStateBoundaryReport:
    ok: bool
    violations: tuple[TelemetryStateBoundaryViolation, ...]


def _is_under_core_runtime(path: Path) -> bool:
    parts = path.parts
    return len(parts) >= 3 and parts[-3:] != ("application", "boundary", path.name)


def check_telemetry_state_boundary(core_package_root: Path) -> TelemetryStateBoundaryReport:
    """Scan productive core modules for direct telemetry state persistence."""
    root = Path(core_package_root)
    violations: list[TelemetryStateBoundaryViolation] = []
    for path in sorted(root.rglob("*.py")):
        if not _is_under_core_runtime(path):
            continue
        if path == Path(__file__):
            continue
        rel = path.relative_to(root)
        try:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(path))
        except (OSError, SyntaxError) as exc:
            violations.append(
                TelemetryStateBoundaryViolation(str(rel), 0, f"unreadable_python:{type(exc).__name__}")
            )
            continue

        has_state_literal = any(
            isinstance(node, ast.Constant) and node.value == "state.json"
            for node in ast.walk(tree)
        )
        sensitive_module = rel in _SENSITIVE_RELATIVE_PATHS
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
                "okto_pulse.community"
            ):
                violations.append(
                    TelemetryStateBoundaryViolation(
                        str(rel),
                        node.lineno,
                        "core_imports_community_storage_or_adapter",
                    )
                )
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            attr = node.func.attr
            if attr == "replace":
                direct_state_file_io = has_state_literal
            else:
                direct_state_file_io = attr in _FORBIDDEN_FILE_METHODS and (
                    sensitive_module or has_state_literal
                )
            if direct_state_file_io:
                violations.append(
                    TelemetryStateBoundaryViolation(
                        str(rel),
                        node.lineno,
                        f"direct_state_file_io:{attr}",
                    )
                )
    return TelemetryStateBoundaryReport(ok=not violations, violations=tuple(violations))


__all__ = [
    "TelemetryStateBoundaryReport",
    "TelemetryStateBoundaryViolation",
    "check_telemetry_state_boundary",
]
