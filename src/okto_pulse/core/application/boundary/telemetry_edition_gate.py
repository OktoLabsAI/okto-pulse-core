"""Zero-budget gate for the edition-neutral Core telemetry boundary."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

FORBIDDEN_IMPORT_ROOTS = frozenset({"pathlib", "requests", "chardet", "sqlite3"})
FORBIDDEN_PUBLIC_IDENTIFIERS = frozenset(
    {
        "metrics_dir",
        "install_id_path",
        "beacon_url",
        "http_client",
        "export_local",
        "purge_local",
    }
)
NEUTRAL_RUNTIME_FILES = (
    "ports/telemetry.py",
    "telemetry/effect_config_registry.py",
    "telemetry/event_store_registry.py",
    "telemetry/product_aggregator_registry.py",
    "telemetry/service.py",
    "telemetry/settings.py",
    "telemetry/telemetry_state_registry.py",
)
FORBIDDEN_CORE_EDITION_FILES = ("api/metrics.py",)


@dataclass(frozen=True)
class TelemetryEditionFinding:
    file: str
    reason: str
    line: int


@dataclass(frozen=True)
class TelemetryEditionReport:
    findings: tuple[TelemetryEditionFinding, ...]

    @property
    def ok(self) -> bool:
        return not self.findings

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "budget": 0,
            "findings": [
                {"file": item.file, "reason": item.reason, "line": item.line}
                for item in self.findings
            ],
        }


def _default_core_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _import_roots(node: ast.Import | ast.ImportFrom) -> set[str]:
    if isinstance(node, ast.ImportFrom):
        return {(node.module or "").partition(".")[0]}
    return {alias.name.partition(".")[0] for alias in node.names}


def run_telemetry_edition_gate(
    core_root: str | Path | None = None,
) -> TelemetryEditionReport:
    """Reject concrete edition details in the Core telemetry contract/runtime."""
    root = Path(core_root) if core_root is not None else _default_core_root()
    findings: list[TelemetryEditionFinding] = []
    for relative in FORBIDDEN_CORE_EDITION_FILES:
        if (root / relative).exists():
            findings.append(TelemetryEditionFinding(relative, "edition_owned_file", 0))
    for relative in NEUTRAL_RUNTIME_FILES:
        path = root / relative
        if not path.exists():
            findings.append(TelemetryEditionFinding(relative, "missing_file", 0))
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                for root_name in _import_roots(node) & FORBIDDEN_IMPORT_ROOTS:
                    findings.append(
                        TelemetryEditionFinding(
                            relative,
                            f"forbidden_import:{root_name}",
                            node.lineno,
                        )
                    )
            if isinstance(node, ast.Name):
                identifier = node.id
            elif isinstance(node, ast.arg):
                identifier = node.arg
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                identifier = node.name
            else:
                continue
            if identifier in FORBIDDEN_PUBLIC_IDENTIFIERS:
                findings.append(
                    TelemetryEditionFinding(
                        relative,
                        f"forbidden_identifier:{identifier}",
                        node.lineno,
                    )
                )
    return TelemetryEditionReport(
        tuple(sorted(findings, key=lambda item: (item.file, item.line, item.reason)))
    )


__all__ = [
    "FORBIDDEN_IMPORT_ROOTS",
    "FORBIDDEN_CORE_EDITION_FILES",
    "FORBIDDEN_PUBLIC_IDENTIFIERS",
    "NEUTRAL_RUNTIME_FILES",
    "TelemetryEditionFinding",
    "TelemetryEditionReport",
    "run_telemetry_edition_gate",
]
