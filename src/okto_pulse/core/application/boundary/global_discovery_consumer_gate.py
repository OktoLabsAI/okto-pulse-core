"""R09 gate for Global Discovery runtime consumer boundaries.

Core consumers may call the Global Discovery port/wrappers, but must not rebuild
the old concrete runtime ownership by referencing private globals or resolving
``discovery.lbug`` locally.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from .report import GateReport

FORBIDDEN_GLOBAL_DISCOVERY_SYMBOLS: tuple[str, ...] = (
    "_global" "_db",
    "_global" "_kuzu_path",
    "GLOBAL_DISCOVERY" "_FILENAME",
)


@dataclass(frozen=True)
class GlobalDiscoveryConsumerFinding:
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


def _core_root(source_root: Path | None) -> Path:
    root = source_root or _default_source_root()
    direct = root / "okto_pulse" / "core"
    if direct.exists():
        return direct
    return root / "src" / "okto_pulse" / "core"


def _is_runtime_file(path: Path) -> bool:
    parts = set(path.parts)
    return not ({"tests", "test", "fakes", "fixtures"} & parts)


class GlobalDiscoveryConsumerGate:
    """Blocks concrete Global Discovery runtime access from Core consumers."""

    gate_id = "global_discovery_consumer"

    def run(self, *, source_root: Path | None = None) -> GateReport:
        root = _core_root(source_root)
        findings: list[GlobalDiscoveryConsumerFinding] = []
        scanned_files: list[str] = []

        if not root.exists():
            return GateReport(
                gate_id=self.gate_id,
                subject="core global discovery runtime consumers",
                status="xfail_advisory",
                severity="low",
                owner="okto-pulse-core/kg",
                evidence={"error": "core_source_not_found", "source_root": str(root)},
                promotion_criteria="Run against a source root containing okto_pulse/core.",
            )

        for path in sorted(root.rglob("*.py")):
            if not _is_runtime_file(path):
                continue
            rel = path.relative_to(root.parent.parent).as_posix()
            scanned_files.append(rel)
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except SyntaxError as exc:
                findings.append(
                    GlobalDiscoveryConsumerFinding(
                        file=rel,
                        line=exc.lineno or 0,
                        kind="parse_error",
                        symbol="<syntax_error>",
                    )
                )
                continue
            findings.extend(_scan_tree(rel, tree))

        evidence = {
            "forbidden_symbols": list(FORBIDDEN_GLOBAL_DISCOVERY_SYMBOLS),
            "forbidden_path_literal": "discovery.lbug",
            "source_root": str(root),
            "scanned_files": scanned_files,
            "offenders": [finding.as_dict() for finding in findings],
        }
        if findings:
            return GateReport(
                gate_id=self.gate_id,
                subject="core global discovery runtime consumers",
                status="blocking",
                severity="high",
                owner="okto-pulse-core/kg",
                evidence={**evidence, "error": "direct_global_discovery_runtime"},
                observed_value=[finding.as_dict() for finding in findings],
                expected_value=[],
                remediation_hint=(
                    "Core code may call GlobalDiscoveryRuntime through the registry "
                    "or schema wrappers only. Move concrete path/handle ownership "
                    "to the Community runtime adapter."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="core global discovery runtime consumers",
            status="passed",
            severity="low",
            owner="okto-pulse-core/kg",
            evidence=evidence,
        )


def _scan_tree(rel: str, tree: ast.AST) -> list[GlobalDiscoveryConsumerFinding]:
    findings: list[GlobalDiscoveryConsumerFinding] = []
    forbidden = set(FORBIDDEN_GLOBAL_DISCOVERY_SYMBOLS)
    parents = _parent_map(tree)

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name in forbidden or (alias.asname or "") in forbidden:
                    findings.append(
                        GlobalDiscoveryConsumerFinding(
                            file=rel,
                            line=node.lineno,
                            kind="forbidden_import",
                            symbol=alias.asname or alias.name,
                        )
                    )
        elif isinstance(node, ast.Name) and node.id in forbidden:
            if isinstance(parents.get(node), (ast.alias, ast.ImportFrom)):
                continue
            findings.append(
                GlobalDiscoveryConsumerFinding(
                    file=rel,
                    line=node.lineno,
                    kind="forbidden_symbol_reference",
                    symbol=node.id,
                )
            )
        elif (
            isinstance(node, ast.Constant)
            and node.value == "discovery.lbug"
            and _is_path_resolution_literal(node, parents)
        ):
            findings.append(
                GlobalDiscoveryConsumerFinding(
                    file=rel,
                    line=node.lineno,
                    kind="forbidden_path_resolution",
                    symbol="discovery.lbug",
                )
            )
    return findings


def _parent_map(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    parents: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent
    return parents


def _is_path_resolution_literal(
    node: ast.Constant,
    parents: dict[ast.AST, ast.AST],
) -> bool:
    parent = parents.get(node)
    if isinstance(parent, ast.Call):
        func = parent.func
        if isinstance(func, ast.Name) and func.id == "Path":
            return True
        if isinstance(func, ast.Attribute) and func.attr == "joinpath":
            return True
    if isinstance(parent, ast.BinOp) and isinstance(parent.op, ast.Div):
        return True
    return False


__all__ = [
    "FORBIDDEN_GLOBAL_DISCOVERY_SYMBOLS",
    "GlobalDiscoveryConsumerFinding",
    "GlobalDiscoveryConsumerGate",
]
