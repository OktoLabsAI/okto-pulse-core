"""AST gate for concrete session-factory lookups in KG transports.

The R01 relational boundary gate intentionally covers use cases only. This gate
is scoped to the REST/MCP transport tree and blocks direct
``core.infra.database.get_session_factory`` lookups in KG handlers. Composition
or bootstrap shims may keep a small governed allowlist; handler code must receive
an injected session scope instead.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

DATABASE_MODULE = "okto_pulse.core.infra.database"
DATABASE_PARENT_MODULE = "okto_pulse.core.infra"
SENSITIVE_SYMBOL = "get_session_factory"


@dataclass(frozen=True)
class TransportSessionFactoryAllowlistEntry:
    file: str
    function: str
    owner: str
    reason: str
    removal_criterion: str

    def matches(self, file: str, function: str | None) -> bool:
        return file == self.file and function == self.function


ALLOWLIST: tuple[TransportSessionFactoryAllowlistEntry, ...] = (
    TransportSessionFactoryAllowlistEntry(
        file="mcp/server.py",
        function="run_mcp_server",
        owner="okto-pulse-core/bootstrap",
        reason=(
            "standalone debug/legacy MCP bootstrap registers the process session "
            "factory before serving requests; production composition registers it "
            "outside request handlers"
        ),
        removal_criterion=(
            "retire when the standalone MCP shim is replaced by an injected "
            "RuntimeComposition entry point"
        ),
    ),
)


@dataclass(frozen=True)
class TransportSessionFactoryFinding:
    file: str
    line: int
    symbol: str
    kind: str
    function: str | None
    allowlisted: bool

    def as_dict(self) -> dict:
        return {
            "file": self.file,
            "line": self.line,
            "symbol": self.symbol,
            "kind": self.kind,
            "function": self.function,
            "allowlisted": self.allowlisted,
        }


@dataclass(frozen=True)
class TransportSessionFactoryReport:
    ok: bool
    findings: tuple[TransportSessionFactoryFinding, ...]
    violations: tuple[TransportSessionFactoryFinding, ...]
    allowlist: tuple[TransportSessionFactoryAllowlistEntry, ...] = ALLOWLIST

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "findings": [finding.as_dict() for finding in self.findings],
            "violations": [finding.as_dict() for finding in self.violations],
            "allowlist": [
                {
                    "file": entry.file,
                    "function": entry.function,
                    "owner": entry.owner,
                    "reason": entry.reason,
                    "removal_criterion": entry.removal_criterion,
                }
                for entry in self.allowlist
            ],
        }


def _default_core_root() -> Path:
    # .../core/application/boundary/transport_session_factory_gate.py -> core/
    return Path(__file__).resolve().parents[2]


def _dotted_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _dotted_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return None


def _parent_map(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    parents: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent
    return parents


def _enclosing_function(
    node: ast.AST,
    parents: dict[ast.AST, ast.AST],
) -> str | None:
    current = node
    while current in parents:
        current = parents[current]
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return current.name
    return None


def _module_aliases(tree: ast.AST) -> set[str]:
    aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == DATABASE_MODULE:
                    aliases.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module == DATABASE_PARENT_MODULE:
                for alias in node.names:
                    if alias.name == "database":
                        aliases.add(alias.asname or alias.name)
    return aliases


def _direct_aliases(tree: ast.AST) -> set[str]:
    aliases = {SENSITIVE_SYMBOL}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == DATABASE_MODULE:
            for alias in node.names:
                if alias.name == SENSITIVE_SYMBOL:
                    aliases.add(alias.asname or alias.name)
    return aliases


def _is_sensitive_call(
    func: ast.AST,
    *,
    direct_aliases: set[str],
    module_aliases: set[str],
) -> bool:
    if isinstance(func, ast.Name):
        return func.id in direct_aliases
    dotted = _dotted_name(func)
    if dotted is None:
        return False
    if dotted == f"{DATABASE_MODULE}.{SENSITIVE_SYMBOL}":
        return True
    return any(dotted == f"{alias}.{SENSITIVE_SYMBOL}" for alias in module_aliases)


def _detections(
    tree: ast.AST,
) -> tuple[tuple[int, str, str], ...]:
    module_aliases = _module_aliases(tree)
    direct_aliases = _direct_aliases(tree)
    factory_vars: set[str] = set()
    out: list[tuple[int, str, str]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == DATABASE_MODULE:
            for alias in node.names:
                if alias.name == SENSITIVE_SYMBOL:
                    out.append((node.lineno, SENSITIVE_SYMBOL, "import"))
        elif isinstance(node, ast.Call):
            if _is_sensitive_call(
                node.func,
                direct_aliases=direct_aliases,
                module_aliases=module_aliases,
            ):
                out.append((node.lineno, SENSITIVE_SYMBOL, "call"))

    changed = True
    while changed:
        changed = False
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            if (
                isinstance(node.value, ast.Call)
                and _is_sensitive_call(
                    node.value.func,
                    direct_aliases=direct_aliases,
                    module_aliases=module_aliases,
                )
            ) or (isinstance(node.value, ast.Name) and node.value.id in factory_vars):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id not in factory_vars:
                        factory_vars.add(target.id)
                        changed = True

    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in factory_vars
        ):
            out.append((node.lineno, node.func.id, "derived_session_factory_call"))

    return tuple(sorted(set(out), key=lambda item: (item[0], item[1], item[2])))


def _is_allowlisted(file: str, function: str | None) -> bool:
    return any(entry.matches(file, function) for entry in ALLOWLIST)


def run_transport_session_factory_gate(
    core_root: str | Path | None = None,
) -> TransportSessionFactoryReport:
    root = Path(core_root) if core_root is not None else _default_core_root()
    findings: list[TransportSessionFactoryFinding] = []
    for base_name in ("api", "mcp"):
        base = root / base_name
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except (OSError, SyntaxError, UnicodeDecodeError):
                continue
            parents = _parent_map(tree)
            rel = path.relative_to(root).as_posix()
            for line, symbol, kind in _detections(tree):
                node = next(
                    (
                        candidate
                        for candidate in ast.walk(tree)
                        if getattr(candidate, "lineno", None) == line
                    ),
                    tree,
                )
                function = _enclosing_function(node, parents)
                findings.append(
                    TransportSessionFactoryFinding(
                        file=rel,
                        line=line,
                        symbol=symbol,
                        kind=kind,
                        function=function,
                        allowlisted=_is_allowlisted(rel, function),
                    )
                )
    findings.sort(key=lambda item: (item.file, item.line, item.symbol, item.kind))
    violations = tuple(finding for finding in findings if not finding.allowlisted)
    return TransportSessionFactoryReport(
        ok=not violations,
        findings=tuple(findings),
        violations=violations,
    )


__all__ = [
    "ALLOWLIST",
    "DATABASE_MODULE",
    "SENSITIVE_SYMBOL",
    "TransportSessionFactoryAllowlistEntry",
    "TransportSessionFactoryFinding",
    "TransportSessionFactoryReport",
    "run_transport_session_factory_gate",
]
