"""Fail-closed F12 gate for REST and MCP persistence access."""

from __future__ import annotations

import ast
from dataclasses import asdict, dataclass
from pathlib import Path


FORBIDDEN_INBOUND_MODULES = (
    "sqlalchemy",
    "okto_pulse.core.infra.database",
    "okto_pulse.core.models.db",
    "okto_pulse.core.repositories.sqlalchemy",
)
FORBIDDEN_INBOUND_SYMBOLS = frozenset(
    {
        "AsyncSession",
        "Session",
        "get_db",
        "get_db_for_mcp",
        "get_uow_session_for_mcp",
        "select",
    }
)


@dataclass(frozen=True, slots=True)
class InboundBoundaryFinding:
    file: str
    line: int
    symbol: str
    reason: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class InboundBoundaryReport:
    scanned_files: int
    findings: tuple[InboundBoundaryFinding, ...]

    @property
    def ok(self) -> bool:
        return not self.findings

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "scanned_files": self.scanned_files,
            "finding_count": len(self.findings),
            "findings": [finding.as_dict() for finding in self.findings],
            "allowances": [],
        }


def _forbidden_module(module: str) -> bool:
    return any(
        module == root or module.startswith(f"{root}.")
        for root in FORBIDDEN_INBOUND_MODULES
    )


def _call_name(node: ast.Call) -> str:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def _scan_file(path: Path, root: Path) -> tuple[InboundBoundaryFinding, ...]:
    relative = path.relative_to(root).as_posix()
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    findings: list[InboundBoundaryFinding] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _forbidden_module(alias.name):
                    findings.append(
                        InboundBoundaryFinding(
                            relative,
                            node.lineno,
                            alias.name,
                            "forbidden_concrete_import",
                        )
                    )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if _forbidden_module(module):
                findings.append(
                    InboundBoundaryFinding(
                        relative,
                        node.lineno,
                        module,
                        "forbidden_concrete_import",
                    )
                )
            for alias in node.names:
                if alias.name in FORBIDDEN_INBOUND_SYMBOLS:
                    findings.append(
                        InboundBoundaryFinding(
                            relative,
                            node.lineno,
                            alias.name,
                            "forbidden_session_symbol",
                        )
                    )
        elif isinstance(node, ast.Call):
            name = _call_name(node)
            if name in FORBIDDEN_INBOUND_SYMBOLS:
                findings.append(
                    InboundBoundaryFinding(
                        relative,
                        node.lineno,
                        name,
                        "forbidden_session_call",
                    )
                )
            if name.startswith("SQLAlchemy"):
                findings.append(
                    InboundBoundaryFinding(
                        relative,
                        node.lineno,
                        name,
                        "forbidden_concrete_constructor",
                    )
                )
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "execute"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id in {"db", "session"}
            ):
                findings.append(
                    InboundBoundaryFinding(
                        relative,
                        node.lineno,
                        f"{node.func.value.id}.execute",
                        "forbidden_native_query",
                    )
                )
    return tuple(findings)


def scan_inbound_boundaries(
    core_root: str | Path,
) -> InboundBoundaryReport:
    root = Path(core_root)
    targets = sorted((root / "api").glob("*.py"))
    server = root / "mcp" / "server.py"
    if server.exists():
        targets.append(server)
    findings = tuple(
        finding
        for path in targets
        for finding in _scan_file(path, root)
    )
    return InboundBoundaryReport(len(targets), findings)


__all__ = [
    "FORBIDDEN_INBOUND_MODULES",
    "FORBIDDEN_INBOUND_SYMBOLS",
    "InboundBoundaryFinding",
    "InboundBoundaryReport",
    "scan_inbound_boundaries",
]
