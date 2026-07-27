"""ApplicationPurityGate (spec #09, tr_391ea356 / fr_a016174a).

AST-based conformance check that fails when any module under
``application/use_cases`` imports or uses a transport framework or
transport-coupled symbol. It is a pure static analysis (``ast`` + ``pathlib``),
performs no runtime import of the scanned code and produces a deterministic,
structured report (file, symbol, layer, remediation_hint) per violation.

Scope note (spec #09): ``AsyncSession`` and direct relational access are NOT
forbidden here — removing them is the job of spec #04. This gate guards the
transport boundary only.

This module lives OUTSIDE the guarded ``use_cases`` package on purpose: it is
conformance tooling, not application logic.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

#: Top-level import packages that are transport frameworks and must never be
#: imported by a use case.
FORBIDDEN_IMPORT_ROOTS: frozenset[str] = frozenset(
    {"fastapi", "starlette", "mcp", "fastmcp"}
)

#: Transport-coupled symbol names that must never be imported or used.
FORBIDDEN_SYMBOLS: frozenset[str] = frozenset(
    {"Request", "Depends", "ContextVar", "_active_api_key"}
)

LAYER = "application/use_cases"


@dataclass(frozen=True)
class PurityViolation:
    """A single transport-leak finding."""

    file: str
    line: int
    symbol: str
    layer: str
    remediation_hint: str


@dataclass
class PurityGateReport:
    """Result of an :func:`run_application_purity_gate` scan."""

    ok: bool
    scanned_files: int
    guarded_path: str
    violations: list[PurityViolation] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "scanned_files": self.scanned_files,
            "guarded_path": self.guarded_path,
            "violations": [
                {
                    "file": v.file,
                    "line": v.line,
                    "symbol": v.symbol,
                    "layer": v.layer,
                    "remediation_hint": v.remediation_hint,
                }
                for v in self.violations
            ],
        }


def default_guarded_path() -> Path:
    """The package this gate guards: ``application/use_cases``."""
    return Path(__file__).resolve().parent / "use_cases"


def _hint(symbol: str) -> str:
    return (
        f"Transport leak '{symbol}' in {LAYER}: move transport/auth handling to "
        f"the inbound adapter and pass plain data via the command/ActorContext."
    )


def _scan_tree(tree: ast.AST, file_label: str) -> list[PurityViolation]:
    violations: list[PurityViolation] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                if root in FORBIDDEN_IMPORT_ROOTS:
                    violations.append(
                        PurityViolation(
                            file_label, node.lineno, f"import {alias.name}", LAYER, _hint(root)
                        )
                    )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            root = module.split(".")[0]
            if root in FORBIDDEN_IMPORT_ROOTS:
                violations.append(
                    PurityViolation(
                        file_label,
                        node.lineno,
                        f"from {module} import ...",
                        LAYER,
                        _hint(root),
                    )
                )
            for alias in node.names:
                if alias.name in FORBIDDEN_SYMBOLS:
                    violations.append(
                        PurityViolation(
                            file_label,
                            node.lineno,
                            f"from {module} import {alias.name}",
                            LAYER,
                            _hint(alias.name),
                        )
                    )
        elif isinstance(node, ast.Name) and node.id in FORBIDDEN_SYMBOLS:
            violations.append(
                PurityViolation(file_label, node.lineno, node.id, LAYER, _hint(node.id))
            )
        elif isinstance(node, ast.Attribute) and node.attr in FORBIDDEN_SYMBOLS:
            violations.append(
                PurityViolation(file_label, node.lineno, node.attr, LAYER, _hint(node.attr))
            )
    return violations


def run_application_purity_gate(root: str | Path | None = None) -> PurityGateReport:
    """Scan every ``*.py`` under ``root`` (default: ``application/use_cases``).

    Returns a deterministic report; ``ok`` is True only when no module imports
    or uses a forbidden transport framework / symbol.
    """
    base = Path(root) if root is not None else default_guarded_path()
    violations: list[PurityViolation] = []
    files = sorted(base.rglob("*.py")) if base.exists() else []
    for path in files:
        try:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(path))
        except (OSError, SyntaxError):
            continue
        try:
            label = str(path.relative_to(base.parent))
        except ValueError:
            label = str(path)
        violations.extend(_scan_tree(tree, label))
    return PurityGateReport(
        ok=not violations,
        scanned_files=len(files),
        guarded_path=str(base),
        violations=violations,
    )
