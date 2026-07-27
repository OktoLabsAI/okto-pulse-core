"""AF30-3cR final relational residue gate.

Blocks the final bits of SQLAlchemy runtime ownership that must not live in
core runtime modules after the Community relational adapters take over.
"""

from __future__ import annotations

import ast
from dataclasses import asdict, dataclass
from pathlib import Path

from .report import GateReport

CORE_TARGET_FILES: tuple[str, ...] = (
    "events/handlers/consolidation_enqueuer.py",
    "events/handlers/kg_decay_tick.py",
    "infra/daily_tick.py",
    "infra/startup_schema_sweep.py",
    "infra/config.py",
    "application/boundary/relational_adapter_import_gate.py",
)

COMMUNITY_TARGET_FILES: tuple[str, ...] = (
    "adapters/data_bootstrapper.py",
    "adapters/relational_schema_migrator.py",
)

_PRODUCTIVE_EXCLUDED_PREFIXES: tuple[str, ...] = (
    "application/boundary/",
    "testing/",
    "kg/providers/testing/",
)
_RELATIONAL_NATIVE_ALLOWED: frozenset[str] = frozenset(
    {
        "ports/relational_runtime.py",
    }
)


@dataclass(frozen=True, slots=True)
class RelationalResidueFinding:
    file: str
    category: str
    symbol: str
    line: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RelationalResidueReport:
    findings: tuple[RelationalResidueFinding, ...]

    @property
    def ok(self) -> bool:
        return not self.findings

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "target_files": {
                "core": list(CORE_TARGET_FILES),
                "community": list(COMMUNITY_TARGET_FILES),
            },
            "findings": [finding.as_dict() for finding in self.findings],
        }

    def as_gate_report(self) -> GateReport:
        return GateReport(
            gate_id="af30_3c_relational_residue_gate",
            subject="Final relational SQLAlchemy residues",
            status="passed" if self.ok else "blocking",
            severity="high" if self.findings else "medium",
            owner="okto-pulse-core/application-boundary",
            evidence=self.as_dict(),
            observed_value=len(self.findings),
            expected_value=0,
            promotion_criteria=(
                "Core target modules contain no dialect SQL/upsert branches, "
                "ad hoc Table/MetaData, SQLite DSN default, or engine/session "
                "factory allowlist; Community lifecycle adapters contain no "
                "dynamic private database getattr."
            ),
            remediation_hint=(
                "Move concrete SQLAlchemy mechanics to Community adapters and "
                "route core runtime modules through registered ports."
            )
            if self.findings
            else None,
        )


class _CoreResidueVisitor(ast.NodeVisitor):
    def __init__(self, file: str) -> None:
        self.file = file
        self.findings: list[RelationalResidueFinding] = []

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        if module.startswith("sqlalchemy.dialects"):
            self._add("dialect_sql", module, node.lineno)
        for alias in node.names:
            if alias.name in {"Table", "MetaData"}:
                self._add("ad_hoc_metadata", alias.name, node.lineno)
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name.startswith("sqlalchemy.dialects"):
                self._add("dialect_sql", alias.name, node.lineno)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if (
            self.file == "infra/config.py"
            and isinstance(node.target, ast.Name)
            and node.target.id == "database_url"
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
            and "sqlite+aiosqlite" in node.value.value
        ):
            self._add("sqlite_dsn_default", node.target.id, node.lineno)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute) and node.func.attr == "on_conflict_do_update":
            self._add("dialect_sql", "on_conflict_do_update", node.lineno)
        elif isinstance(node.func, ast.Name) and node.func.id in {"Table", "MetaData"}:
            self._add("ad_hoc_metadata", node.func.id, node.lineno)
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr == "dialect":
            self._add("dialect_sql", "dialect", node.lineno)
        elif node.attr in {"Table", "MetaData"}:
            self._add("ad_hoc_metadata", node.attr, node.lineno)
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if node.id in {"dialect_name", "upsert_insert", "_upsert_insert"}:
            self._add("dialect_sql", node.id, node.lineno)
        self.generic_visit(node)

    def _add(self, category: str, symbol: str, line: int) -> None:
        self.findings.append(
            RelationalResidueFinding(
                file=self.file,
                category=category,
                symbol=symbol,
                line=line,
            )
        )


class _CommunityResidueVisitor(ast.NodeVisitor):
    def __init__(self, file: str) -> None:
        self.file = file
        self.database_aliases: set[str] = set()
        self.findings: list[RelationalResidueFinding] = []

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name == "okto_pulse.core.infra.database":
                self.database_aliases.add(alias.asname or alias.name.rsplit(".", 1)[-1])
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        if module == "okto_pulse.core.infra":
            for alias in node.names:
                if alias.name == "database":
                    self.database_aliases.add(alias.asname or alias.name)
        elif module == "okto_pulse.core.infra.database":
            for alias in node.names:
                self.database_aliases.add(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        if _is_import_database_call(node.value):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self.database_aliases.add(target.id)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if _is_getattr_call(node) and len(node.args) >= 2:
            target = node.args[0]
            if isinstance(target, ast.Name) and self._is_database_alias(target.id):
                self.findings.append(
                    RelationalResidueFinding(
                        file=self.file,
                        category="dynamic_private_database_access",
                        symbol=_safe_unparse(node.args[1]),
                        line=node.lineno,
                    )
                )
        self.generic_visit(node)

    def _is_database_alias(self, name: str) -> bool:
        return name in self.database_aliases or name in {
            "_database",
            "core_database",
            "database",
        }


class _RelationalRuntimeLeakVisitor(ast.NodeVisitor):
    """Detect adapter-native relational concepts across productive Core."""

    _NATIVE_NAMES = {"session_factory", "relational_engine", "sync_engine"}
    _RAW_SCOPE_NAMES = {"session", "db", "sqlite_session"}

    def __init__(self, file: str) -> None:
        self.file = file
        self.findings: list[RelationalResidueFinding] = []

    def visit_Name(self, node: ast.Name) -> None:
        if node.id in self._NATIVE_NAMES:
            self._add("relational_native_symbol", node.id, node.lineno)
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr in self._NATIVE_NAMES:
            self._add("relational_native_symbol", node.attr, node.lineno)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute) and node.func.attr in {
            "commit",
            "rollback",
        }:
            receiver = node.func.value
            if isinstance(receiver, ast.Name) and receiver.id in self._RAW_SCOPE_NAMES:
                self._add(
                    "raw_transaction_call",
                    f"{receiver.id}.{node.func.attr}",
                    node.lineno,
                )
        self.generic_visit(node)

    def _add(self, category: str, symbol: str, line: int) -> None:
        self.findings.append(
            RelationalResidueFinding(
                file=self.file,
                category=category,
                symbol=symbol,
                line=line,
            )
        )


def _safe_unparse(node: ast.AST) -> str:
    try:
        return ast.unparse(node)
    except Exception:  # pragma: no cover - defensive fallback
        return node.__class__.__name__


def _is_getattr_call(node: ast.Call) -> bool:
    return isinstance(node.func, ast.Name) and node.func.id == "getattr"


def _is_import_database_call(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and (
            (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "import_module"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "importlib"
            )
            or (isinstance(node.func, ast.Name) and node.func.id == "import_module")
        )
        and bool(node.args)
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == "okto_pulse.core.infra.database"
    )


def _default_core_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _scan_file(path: Path, rel: str, *, community: bool = False) -> tuple[RelationalResidueFinding, ...]:
    if not path.exists():
        return ()
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
    except (OSError, SyntaxError, UnicodeDecodeError):
        return ()

    if community:
        visitor = _CommunityResidueVisitor(rel)
    else:
        visitor = _CoreResidueVisitor(rel)
    visitor.visit(tree)
    findings = list(visitor.findings)
    if rel == "application/boundary/relational_adapter_import_gate.py":
        findings.extend(_scan_engine_session_allowlist(source, rel))
    return tuple(findings)


def _scan_engine_session_allowlist(
    source: str,
    rel: str,
) -> tuple[RelationalResidueFinding, ...]:
    if "infra/database.py" not in source:
        return ()
    findings: list[RelationalResidueFinding] = []
    for line_no, line in enumerate(source.splitlines(), start=1):
        if "infra/database.py" in line:
            findings.append(
                RelationalResidueFinding(
                    file=rel,
                    category="engine_session_factory_allowlist",
                    symbol="infra/database.py",
                    line=line_no,
                )
            )
    return tuple(findings)


def run_relational_residue_gate(
    *,
    core_root: str | Path | None = None,
    community_root: str | Path | None = None,
) -> RelationalResidueReport:
    """Run the AF30-3cR residue gate against core and, optionally, Community."""

    core = Path(core_root) if core_root is not None else _default_core_root()
    findings: list[RelationalResidueFinding] = []
    for rel in CORE_TARGET_FILES:
        findings.extend(_scan_file(core / rel, rel))

    for path in sorted(core.rglob("*.py")):
        rel = path.relative_to(core).as_posix()
        if rel in _RELATIONAL_NATIVE_ALLOWED or rel.startswith(
            _PRODUCTIVE_EXCLUDED_PREFIXES
        ):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError, UnicodeDecodeError):
            findings.append(
                RelationalResidueFinding(
                    file=rel,
                    category="relational_scan_failed",
                    symbol="unparseable_productive_source",
                    line=1,
                )
            )
            continue
        visitor = _RelationalRuntimeLeakVisitor(rel)
        visitor.visit(tree)
        findings.extend(visitor.findings)

    if community_root is not None:
        community = Path(community_root)
        for rel in COMMUNITY_TARGET_FILES:
            findings.extend(_scan_file(community / rel, rel, community=True))

    deduped = {
        (finding.file, finding.category, finding.symbol, finding.line): finding
        for finding in findings
    }
    return RelationalResidueReport(
        findings=tuple(
            sorted(deduped.values(), key=lambda item: (item.file, item.line, item.category))
        )
    )


__all__ = [
    "COMMUNITY_TARGET_FILES",
    "CORE_TARGET_FILES",
    "RelationalResidueFinding",
    "RelationalResidueReport",
    "run_relational_residue_gate",
]
