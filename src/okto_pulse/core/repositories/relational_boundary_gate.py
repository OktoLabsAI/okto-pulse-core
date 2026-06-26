"""RelationalBoundaryGate (SaaS Refactor spec #04, tr_4c0d19ed / or_9940a072).

AST/import/call-site scan that fails when a MIGRATED use case
(``application/use_cases``) couples directly to the relational layer —
``AsyncSession``, ``select``, ``Depends(get_db)``, ``get_db_for_mcp`` or a
concrete ORM model from ``core.models.db``. Critically it covers BOTH transport
surfaces: ``Depends(get_db)`` (REST) AND ``get_db_for_mcp`` (MCP) — the latter
was the validator's key finding, since the MCP surface would otherwise escape
the strangler.

Violations are bucketed by surface (rest|mcp|service) to feed the
``okto_pulse_relational_boundary_violations_total`` metric. The blocking
threshold is 0 in migrated use cases; the core-wide baseline (the
Depends(get_db)/get_db_for_mcp/AsyncSession/Base/_migrate_ counts) is reported
as transitional debt / out-of-scope, NOT a blind total-removal target
(ac_cddd871d). ORM returns are excepted only when registered in the debt ledger
(``is_orm_return_excepted``, ac_9649051d).

Pure static analysis (``ast`` + ``pathlib``); no runtime import of scanned code.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path

from okto_pulse.core.repositories.debt import (
    ORM_BASE_CLASS_BASELINE,
    is_orm_return_excepted,
)

METRIC_RELATIONAL_BOUNDARY_VIOLATIONS = "okto_pulse_relational_boundary_violations_total"

SURFACE_REST = "rest"
SURFACE_MCP = "mcp"
SURFACE_SERVICE = "service"

LAYER = "application/use_cases"

_ORM_MODULE = "okto_pulse.core.models.db"
_DB_PROVIDER_SUFFIX = "infra.database"

#: Symbol -> surface for name/attribute usages of relational coupling.
_RELATIONAL_NAMES = {
    "AsyncSession": SURFACE_SERVICE,
    "select": SURFACE_SERVICE,
    "get_db": SURFACE_REST,
    "get_db_for_mcp": SURFACE_MCP,
}

#: Modules where only specific attributes are forbidden — used to catch aliased
#: module imports (``import sqlalchemy as sa`` -> ``sa.select``) so the alias
#: cannot bypass the bare-symbol checks.
_MODULE_FORBIDDEN_ATTRS = {
    "sqlalchemy.ext.asyncio": {"AsyncSession": SURFACE_SERVICE},
    "sqlalchemy": {"select": SURFACE_SERVICE},
    "okto_pulse.core.infra.database": {"get_db": SURFACE_REST, "get_db_for_mcp": SURFACE_MCP},
}


def _dotted_name(node: ast.AST) -> str | None:
    """Flatten an attribute/name chain (``a.b.c``) into a dotted string, else None."""
    parts: list[str] = []
    cur: ast.AST = node
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        parts.append(cur.id)
        return ".".join(reversed(parts))
    return None

#: Documented baseline from spec #04 (ac_cddd871d) — debt/out-of-scope, NOT a
#: blind removal target. ``relational_baseline_report`` also recounts live.
RELATIONAL_BASELINE = {
    "depends_get_db": 268,
    "get_db_for_mcp": 225,
    "async_session": 554,
    "orm_base_classes": ORM_BASE_CLASS_BASELINE,
    "migrate_refs": 96,
}


@dataclass(frozen=True)
class RelationalViolation:
    file: str
    line: int
    symbol: str
    surface: str
    severity: str
    remediation_hint: str


@dataclass
class RelationalBoundaryReport:
    ok: bool
    scanned_files: int
    guarded_path: str
    violations: list[RelationalViolation] = field(default_factory=list)
    violations_by_surface: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "scanned_files": self.scanned_files,
            "guarded_path": self.guarded_path,
            "violations_by_surface": self.violations_by_surface,
            "violations": [
                {
                    "file": v.file,
                    "line": v.line,
                    "symbol": v.symbol,
                    "surface": v.surface,
                    "severity": v.severity,
                    "remediation_hint": v.remediation_hint,
                }
                for v in self.violations
            ],
        }


def default_use_cases_path() -> Path:
    """The migrated-use-case package this gate guards."""
    return Path(__file__).resolve().parent.parent / "application" / "use_cases"


def default_core_path() -> Path:
    return Path(__file__).resolve().parent.parent


def _hint(symbol: str, surface: str) -> str:
    return (
        f"Relational coupling '{symbol}' ({surface}) in {LAYER}: depend on the "
        f"PulseUnitOfWork / repository ports instead of the session/ORM directly."
    )


def _scan_tree(tree: ast.AST, file_label: str) -> list[RelationalViolation]:
    found: dict[tuple[int, str], RelationalViolation] = {}

    def record(line: int, symbol: str, surface: str) -> None:
        found.setdefault(
            (line, symbol),
            RelationalViolation(
                file=file_label,
                line=line,
                symbol=symbol,
                surface=surface,
                severity="blocking",
                remediation_hint=_hint(symbol, surface),
            ),
        )

    def record_orm(line: int, attr: str) -> None:
        # An ORM model touched in a use case is a violation unless registered debt
        # for this exact (repository, type) — use cases are never registered
        # repositories, so it is always blocking here.
        orm_fqn = f"{_ORM_MODULE}.{attr}"
        if not is_orm_return_excepted(orm_fqn, repository=file_label):
            record(line, f"orm:{attr}", SURFACE_SERVICE)

    # Pass 1 — collect names bound to the ORM module or a forbidden-attr module so
    # aliased module imports cannot bypass the checks. Covers `import X.Y.Z as a`,
    # `from pkg import sub [as a]`.
    orm_aliases: set[str] = set()
    attr_aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == _ORM_MODULE and alias.asname:
                    orm_aliases.add(alias.asname)
                elif alias.name in _MODULE_FORBIDDEN_ATTRS and alias.asname:
                    attr_aliases[alias.asname] = alias.name
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            for alias in node.names:
                full = f"{base}.{alias.name}" if base else alias.name
                bound = alias.asname or alias.name
                if full == _ORM_MODULE:
                    orm_aliases.add(bound)
                elif full in _MODULE_FORBIDDEN_ATTRS:
                    attr_aliases[bound] = full

    # Pass 2 — detect couplings (direct symbols + chained/aliased module access).
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "sqlalchemy.ext.asyncio":
                for alias in node.names:
                    if alias.name == "AsyncSession":
                        record(node.lineno, "AsyncSession", SURFACE_SERVICE)
            elif module == "sqlalchemy":
                for alias in node.names:
                    if alias.name == "select":
                        record(node.lineno, "select", SURFACE_SERVICE)
            elif module.endswith(_DB_PROVIDER_SUFFIX):
                for alias in node.names:
                    if alias.name == "get_db":
                        record(node.lineno, "get_db", SURFACE_REST)
                    elif alias.name == "get_db_for_mcp":
                        record(node.lineno, "get_db_for_mcp", SURFACE_MCP)
            elif module == _ORM_MODULE:
                for alias in node.names:
                    record_orm(node.lineno, alias.name)
        elif isinstance(node, ast.Name) and node.id in _RELATIONAL_NAMES:
            record(node.lineno, node.id, _RELATIONAL_NAMES[node.id])
        elif isinstance(node, ast.Attribute):
            value = node.value
            dotted = _dotted_name(node)
            if dotted is not None and dotted.startswith(_ORM_MODULE + "."):
                # No-alias chain: okto_pulse.core.models.db.X
                record_orm(node.lineno, dotted.rsplit(".", 1)[-1])
            elif isinstance(value, ast.Name) and value.id in orm_aliases:
                # Aliased ORM module: orm.X / db.X
                record_orm(node.lineno, node.attr)
            elif isinstance(value, ast.Name) and value.id in attr_aliases:
                # Aliased forbidden-attr module: sa.select / aio.AsyncSession
                forbidden = _MODULE_FORBIDDEN_ATTRS[attr_aliases[value.id]]
                if node.attr in forbidden:
                    record(node.lineno, node.attr, forbidden[node.attr])
            elif node.attr in _RELATIONAL_NAMES:
                # Bare attribute on an unaliased module: sqlalchemy.select
                record(node.lineno, node.attr, _RELATIONAL_NAMES[node.attr])
        elif isinstance(node, ast.Call):
            # Depends(get_db) / Depends(get_db_for_mcp) call-site (REST/MCP wiring).
            func = node.func
            if isinstance(func, ast.Name) and func.id == "Depends":
                for arg in node.args:
                    if isinstance(arg, ast.Name) and arg.id in ("get_db", "get_db_for_mcp"):
                        surface = SURFACE_MCP if arg.id == "get_db_for_mcp" else SURFACE_REST
                        record(node.lineno, f"Depends({arg.id})", surface)

    return list(found.values())


def run_relational_boundary_gate(root: str | Path | None = None) -> RelationalBoundaryReport:
    """Scan every ``*.py`` under ``root`` (default: ``application/use_cases``) for
    direct relational coupling. ``ok`` is True only when there are no violations.
    """
    base = Path(root) if root is not None else default_use_cases_path()
    violations: list[RelationalViolation] = []
    files = sorted(base.rglob("*.py")) if base.exists() else []
    for path in files:
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        try:
            label = str(path.relative_to(base.parent))
        except ValueError:
            label = str(path)
        violations.extend(_scan_tree(tree, label))

    by_surface: dict[str, int] = {}
    for v in violations:
        by_surface[v.surface] = by_surface.get(v.surface, 0) + 1

    return RelationalBoundaryReport(
        ok=not violations,
        scanned_files=len(files),
        guarded_path=str(base),
        violations=violations,
        violations_by_surface=by_surface,
    )


def observe_relational_boundary_violations(report: RelationalBoundaryReport) -> dict:
    """Format the by-surface violation counts as the observability metric
    (``okto_pulse_relational_boundary_violations_total`` labelled by surface)."""
    return {
        "metric": METRIC_RELATIONAL_BOUNDARY_VIOLATIONS,
        "by_surface": {
            SURFACE_REST: report.violations_by_surface.get(SURFACE_REST, 0),
            SURFACE_MCP: report.violations_by_surface.get(SURFACE_MCP, 0),
            SURFACE_SERVICE: report.violations_by_surface.get(SURFACE_SERVICE, 0),
        },
        "blocking_total": len(report.violations),
    }


_DEPENDS_GET_DB_RE = re.compile(r"Depends\(\s*get_db\s*\)")
_BASE_CLASS_RE = re.compile(r"^class\s+[A-Za-z0-9_]+\([^)]*\bBase\b[^)]*\):", re.MULTILINE)


def relational_baseline_report(core_root: str | Path | None = None) -> dict:
    """Recount the core-wide relational coupling baseline (ac_cddd871d).

    Returned as transitional debt / out-of-scope — these are NOT blocking and NOT
    blind total-removal targets; drawing them down requires an owner and
    promotion_criteria per the migrated path.
    """
    base = Path(core_root) if core_root is not None else default_core_path()
    depends_get_db = get_db_for_mcp = async_session = migrate_refs = 0
    orm_base_classes = 0
    for path in base.rglob("*.py"):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        depends_get_db += len(_DEPENDS_GET_DB_RE.findall(text))
        get_db_for_mcp += text.count("get_db_for_mcp")
        async_session += text.count("AsyncSession")
        migrate_refs += text.count("_migrate_")
        if path.name == "db.py" and path.parent.name == "models":
            orm_base_classes += len(_BASE_CLASS_RE.findall(text))

    return {
        "classification": "transitional_debt_out_of_scope",
        "blocking": False,
        "owner": "core-refactor",
        "promotion_criteria": "drawn down per migrated endpoint/tool->use case->UoW/repository path; not a blind total-removal target",
        "documented_baseline": dict(RELATIONAL_BASELINE),
        "live_counts": {
            "depends_get_db": depends_get_db,
            "get_db_for_mcp": get_db_for_mcp,
            "async_session": async_session,
            "orm_base_classes": orm_base_classes,
            "migrate_refs": migrate_refs,
        },
    }
