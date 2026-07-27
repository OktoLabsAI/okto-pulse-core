"""R10A gate for board source read consumer boundaries."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from .report import GateReport

TARGET_SUFFIXES: tuple[str, ...] = (
    "okto_pulse/core/api/kg_rebuild.py",
    "okto_pulse/core/kg/canonical_debt_replay.py",
    "okto_pulse/core/kg/canonical_stale_reconciler.py",
    "okto_pulse/core/kg/stale_canonical_parity.py",
    "okto_pulse/core/services/kg_health_service.py",
)

ALLOWED_INGESTION_SQLITE_SUFFIXES: tuple[str, ...] = ()

SOURCE_READ_PATH_FUNCTIONS: frozenset[str] = frozenset(
    {
        "_pulse_db_path",
        "_health_pulse_db_path",
        "_build_source_store",
        "_build_source_index",
        "_build_source_classification_map",
        "_source_index",
        "_probe_rebuild_source_diagnostics",
    }
)


@dataclass(frozen=True)
class SourceReadConsumerFinding:
    file: str
    line: int
    kind: str
    symbol: str

    def as_dict(self) -> dict[str, object]:
        return {
            "file": self.file,
            "line": self.line,
            "kind": self.kind,
            "symbol": self.symbol,
        }


def _default_source_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _scan_root(source_root: Path | None) -> Path:
    root = source_root or _default_source_root()
    if (root / "okto_pulse").exists():
        return root
    return root / "src"


def _core_root(source_root: Path | None) -> Path:
    root = _scan_root(source_root)
    direct = root / "okto_pulse" / "core"
    if direct.exists():
        return direct
    return root


def _display_root(core_root: Path) -> Path:
    if core_root.name == "core" and core_root.parent.name == "okto_pulse":
        return core_root.parent.parent
    return core_root


def _is_runtime_file(path: Path) -> bool:
    parts = set(path.parts)
    return not ({"tests", "test", "fakes", "fixtures"} & parts)


def _is_allowed_finding(finding: SourceReadConsumerFinding) -> bool:
    if finding.kind != "forbidden_sqlite_connect":
        return False
    return any(
        finding.file.endswith(suffix)
        for suffix in ALLOWED_INGESTION_SQLITE_SUFFIXES
    )


class SourceReadConsumerGate:
    """Blocks raw BoardSourceStore/SQLite source reads from core consumers."""

    gate_id = "source_read_consumer"

    def run(self, *, source_root: Path | None = None) -> GateReport:
        root = _core_root(source_root)
        display_root = _display_root(root)
        findings: list[SourceReadConsumerFinding] = []
        scanned_files: list[str] = []

        if not root.exists():
            return GateReport(
                gate_id=self.gate_id,
                subject="core board source read consumers",
                status="xfail_advisory",
                severity="low",
                owner="okto-pulse-core/kg",
                evidence={"error": "source_root_not_found", "source_root": str(root)},
                promotion_criteria="Run against a source root containing okto_pulse/core.",
            )

        for path in sorted(root.rglob("*.py")):
            if not _is_runtime_file(path):
                continue
            rel = path.relative_to(display_root).as_posix()
            scanned_files.append(rel)
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except SyntaxError as exc:
                findings.append(
                    SourceReadConsumerFinding(
                        file=rel,
                        line=exc.lineno or 0,
                        kind="parse_error",
                        symbol="<syntax_error>",
                    )
                )
                continue
            findings.extend(
                finding
                for finding in _scan_tree(rel, tree)
                if not _is_allowed_finding(finding)
            )

        evidence = {
            "source_root": str(root),
            "scanned_files": scanned_files,
            "historical_source_read_suffixes": list(TARGET_SUFFIXES),
            "allowed_ingestion_sqlite_suffixes": list(ALLOWED_INGESTION_SQLITE_SUFFIXES),
            "offenders": [finding.as_dict() for finding in findings],
        }
        if findings:
            return GateReport(
                gate_id=self.gate_id,
                subject="core board source read consumers",
                status="blocking",
                severity="high",
                owner="okto-pulse-core/kg",
                evidence={**evidence, "error": "direct_board_source_read"},
                observed_value=[finding.as_dict() for finding in findings],
                expected_value=[],
                remediation_hint=(
                    "Core source-read consumers must use "
                    "get_kg_registry().require_board_source_reader(). Move SQLite "
                    "path resolution and raw reads to the Community adapter."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="core board source read consumers",
            status="passed",
            severity="low",
            owner="okto-pulse-core/kg",
            evidence=evidence,
        )


def _scan_tree(rel: str, tree: ast.AST) -> list[SourceReadConsumerFinding]:
    findings: list[SourceReadConsumerFinding] = []
    parents = _parent_map(tree)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "okto_pulse.core.kg.board_source_store":
            for alias in node.names:
                if alias.name == "BoardSourceStore":
                    findings.append(
                        SourceReadConsumerFinding(
                            file=rel,
                            line=node.lineno,
                            kind="forbidden_import",
                            symbol=alias.name,
                        )
                    )
        elif isinstance(node, ast.Name) and node.id == "BoardSourceStore":
            if isinstance(parents.get(node), (ast.alias, ast.ImportFrom)):
                continue
            findings.append(
                SourceReadConsumerFinding(
                    file=rel,
                    line=node.lineno,
                    kind="forbidden_symbol_reference",
                    symbol=node.id,
                )
            )
        elif isinstance(node, ast.FunctionDef) and node.name in {
            "_pulse_db_path",
            "_health_pulse_db_path",
        }:
            findings.append(
                SourceReadConsumerFinding(
                    file=rel,
                    line=node.lineno,
                    kind="forbidden_path_resolver",
                    symbol=node.name,
                )
            )
        elif isinstance(node, ast.Call) and _is_sqlite_connect(node):
            findings.append(
                SourceReadConsumerFinding(
                    file=rel,
                    line=node.lineno,
                    kind="forbidden_sqlite_connect",
                    symbol="sqlite3.connect",
                )
            )
        elif isinstance(node, ast.Attribute) and node.attr == "url":
            fn = _enclosing_function(node, parents)
            if fn in SOURCE_READ_PATH_FUNCTIONS:
                findings.append(
                    SourceReadConsumerFinding(
                        file=rel,
                        line=node.lineno,
                        kind="forbidden_engine_url_path_read",
                        symbol=f"{fn}:get_engine().url",
                    )
                )
    return findings


def _is_sqlite_connect(node: ast.Call) -> bool:
    func = node.func
    return isinstance(func, ast.Attribute) and func.attr == "connect" and (
        isinstance(func.value, ast.Name) and func.value.id == "sqlite3"
    )


def _parent_map(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    parents: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent
    return parents


def _enclosing_function(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> str | None:
    cur = parents.get(node)
    while cur is not None:
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return cur.name
        cur = parents.get(cur)
    return None


__all__ = [
    "ALLOWED_INGESTION_SQLITE_SUFFIXES",
    "SOURCE_READ_PATH_FUNCTIONS",
    "TARGET_SUFFIXES",
    "SourceReadConsumerFinding",
    "SourceReadConsumerGate",
]
