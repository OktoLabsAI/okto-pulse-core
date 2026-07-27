"""Fail-closed gate for the opaque application UnitOfWork boundary."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from .report import GateReport

FORBIDDEN_UOW_BRIDGES = frozenset(
    {"relational_context_from_uow", "session_of", "unwrap_session"}
)
FORBIDDEN_UOW_ANNOTATIONS = frozenset({"Any", "AsyncSession", "object"})


@dataclass(frozen=True)
class UowSessionFinding:
    file: str
    line: int
    symbol: str
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "file": self.file,
            "line": self.line,
            "symbol": self.symbol,
            "reason": self.reason,
        }


def _annotation_name(annotation: ast.expr | None) -> str | None:
    if annotation is None:
        return None
    if isinstance(annotation, ast.Name):
        return annotation.id
    if isinstance(annotation, ast.Attribute):
        return annotation.attr
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        return annotation.value.rsplit(".", 1)[-1]
    return None


class _Visitor(ast.NodeVisitor):
    def __init__(self, *, file: str) -> None:
        self.file = file
        self.findings: list[UowSessionFinding] = []

    def _add(self, node: ast.AST, symbol: str, reason: str) -> None:
        self.findings.append(
            UowSessionFinding(self.file, getattr(node, "lineno", 1), symbol, reason)
        )

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802
        for alias in node.names:
            if alias.name == "sqlalchemy" or alias.name.startswith("sqlalchemy."):
                self._add(node, alias.name, "SQLAlchemy import in application use case")
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802
        module = node.module or ""
        if module == "sqlalchemy" or module.startswith("sqlalchemy."):
            self._add(node, module, "SQLAlchemy import in application use case")
        for alias in node.names:
            if alias.name == "AsyncSession":
                self._add(node, alias.name, "native session type in application use case")
            if alias.name in FORBIDDEN_UOW_BRIDGES:
                self._add(node, alias.name, "UnitOfWork escape bridge import")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        if isinstance(node.func, ast.Name) and node.func.id in FORBIDDEN_UOW_BRIDGES:
            self._add(node, node.func.id, "UnitOfWork escape bridge call")
        if (
            isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id == "uow"
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value == "session"
        ):
            self._add(node, "getattr(uow, session)", "dynamic session extraction")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:  # noqa: N802
        if (
            node.attr == "session"
            and isinstance(node.value, ast.Name)
            and node.value.id == "uow"
        ):
            self._add(node, "uow.session", "native session exposed by UnitOfWork")
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self._check_uow_arguments(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        self._check_uow_arguments(node)
        self.generic_visit(node)

    def _check_uow_arguments(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef
    ) -> None:
        for argument in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs):
            if argument.arg != "uow":
                continue
            annotation = _annotation_name(argument.annotation)
            if annotation is None or annotation in FORBIDDEN_UOW_ANNOTATIONS:
                self._add(
                    argument,
                    f"uow: {annotation or '<missing>'}",
                    "UnitOfWork parameter must use the PulseUnitOfWork port",
                )


class UowSessionBoundaryGate:
    gate_id = "application_uow_session_boundary"

    def run(self, *, source_root: Path | None = None) -> GateReport:
        root = source_root or Path(__file__).resolve().parents[5]
        target = root / "src" / "okto_pulse" / "core" / "application" / "use_cases"
        if not target.exists():
            target = root / "okto_pulse" / "core" / "application" / "use_cases"

        findings: list[UowSessionFinding] = []
        scanned_files: list[str] = []
        for path in sorted(target.rglob("*.py")):
            rel = path.relative_to(root).as_posix()
            scanned_files.append(rel)
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel)
            except SyntaxError as exc:
                findings.append(
                    UowSessionFinding(rel, exc.lineno or 1, "SyntaxError", str(exc))
                )
                continue
            visitor = _Visitor(file=rel)
            visitor.visit(tree)
            findings.extend(visitor.findings)

        evidence = {
            "scanned_files": scanned_files,
            "offenders": [finding.as_dict() for finding in findings],
            "budget": 0,
        }
        if findings:
            return GateReport(
                gate_id=self.gate_id,
                subject="Core application UnitOfWork boundary",
                status="blocking",
                severity="critical",
                owner="okto-pulse-core/application",
                evidence={**evidence, "error": "uow_session_escape"},
                observed_value=len(findings),
                expected_value=0,
                remediation_hint=(
                    "Use typed repositories or named UnitOfWork capabilities; native "
                    "sessions and generic unwrapping are forbidden."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="Core application UnitOfWork boundary",
            status="passed",
            severity="low",
            owner="okto-pulse-core/application",
            evidence=evidence,
            observed_value=0,
            expected_value=0,
        )


__all__ = [
    "FORBIDDEN_UOW_ANNOTATIONS",
    "FORBIDDEN_UOW_BRIDGES",
    "UowSessionBoundaryGate",
    "UowSessionFinding",
]
