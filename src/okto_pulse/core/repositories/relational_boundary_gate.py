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

    # AC5 (ac_28f50f9d, R01B IMP2) — frozen relational coverage aggregates +
    # undeclared-drift verdict against the R01B live snapshot.
    coverage = relational_coverage_counts(base)
    drift = relational_coverage_drift(coverage)

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
        # R01B drawn-down counter baseline (separate from the spec #04
        # RELATIONAL_BASELINE): the R01A REST+MCP CRUD strangler + the R01B
        # ownership inversion shrank these core-wide counts; new measurement
        # taken POST-TR5/R01B over the complete IMP2 tree.
        "r01b_baseline": dict(RELATIONAL_BASELINE_R01B),
        # AC5 frozen relational coverage: floors (minimums) + the R01B live
        # snapshot + live recount + undeclared-drift verdict (drift fails even
        # above the floor unless declared as an R01A/R01B/R01C removal).
        "coverage_floor": dict(RELATIONAL_COVERAGE_BASELINE),
        "coverage_snapshot_r01b": dict(RELATIONAL_COVERAGE_SNAPSHOT_R01B),
        "coverage_counts": coverage,
        "coverage_floor_ok": all(
            coverage.get(k, 0) >= floor
            for k, floor in RELATIONAL_COVERAGE_BASELINE.items()
        ),
        "coverage_drift": drift,
    }


# ---------------------------------------------------------------------------
# AC5 (ac_28f50f9d, R01B IMP2) — frozen relational coverage baseline.
#
# ``relational_baseline_report`` freezes the relational footprint of the core
# tree BEFORE/AFTER the R01B ownership inversion. Three aggregate categories are
# frozen, with TWO layers of protection (Codex AC5 ruling):
#   1. STABLE coverage FLOORS (``RELATIONAL_COVERAGE_BASELINE``): the report
#      requires ``live >= floor`` (the AC "ao menos 245/605/276" minimums).
#   2. The R01B LIVE SNAPSHOT (``RELATIONAL_COVERAGE_SNAPSHOT_R01B``): the exact
#      measured values of THIS implementation, frozen. The report fails on ANY
#      undeclared drift from the snapshot — even while still above the floor — a
#      RISE being new coupling (never a removal) and a DROP being allowed only
#      when structurally DECLARED as an R01A/R01B/R01C removal.
#
# Definitions (reproducible AST scan over the whole core tree):
#   - ``relational_imports``: import statements (``Import``/``ImportFrom``)
#     referencing the relational layer (``sqlalchemy*``, ``core.models.db``,
#     ``core.infra.database`` ``get_db``/``get_db_for_mcp``,
#     ``repositories.sqlalchemy``), OR importing a canonical relational symbol.
#   - ``relational_symbols``: ``Name``/``Attribute`` references to the canonical
#     relational surface symbols (``_RELATIONAL_COVERAGE_SYMBOLS``). String
#     literals do NOT count — only real AST name/attribute usages — so this
#     aggregate is immune to symbol-name mentions in docstrings/constants.
#   - ``classified_call_sites``: relational occurrences classified by surface
#     (rest|mcp|service) via the gate's own ``_scan_tree``, summed over core.
# ---------------------------------------------------------------------------

#: Task-id shape that may DECLARE a coverage drawdown (R01A/R01B/R01C).
_R01_TASK_RE = re.compile(r"\bR01[ABC]\b")

#: Canonical relational surface symbols counted for ``relational_symbols``
#: (AC5). Stable, documented set so the count is reproducible.
_RELATIONAL_COVERAGE_SYMBOLS = frozenset(
    {
        "AsyncSession",
        "async_sessionmaker",
        "create_async_engine",
        "select",
        "get_db",
        "get_db_for_mcp",
        "sessionmaker",
        "declarative_base",
        "AsyncEngine",
        "AsyncConnection",
    }
)


def _is_relational_import_module(module: str | None) -> bool:
    """True when an import's module belongs to the relational layer."""
    if not module:
        return False
    return (
        module == "sqlalchemy"
        or module.startswith("sqlalchemy.")
        or module == _ORM_MODULE
        or module.endswith(_DB_PROVIDER_SUFFIX)
        or "repositories.sqlalchemy" in module
    )


#: AC5 stable coverage FLOORS (ac_28f50f9d) — the "ao menos" minimums. A live
#: aggregate below its floor is a coverage regression the report rejects unless
#: declared as an R01A/R01B/R01C removal.
RELATIONAL_COVERAGE_BASELINE = {
    "relational_imports": 245,
    "relational_symbols": 605,
    "classified_call_sites": 276,
}

#: AC5 frozen LIVE snapshot — the exact relational coverage measured for the
#: core tree. The report fails on any undeclared drift from this snapshot even
#: above the floor. ``by_surface`` is the per-surface breakdown of
#: ``classified_call_sites``.
#:
#: R01C IMP1 declared drawdown (enum extraction + lazy package init), re-frozen
#: ONCE after lazy ``core.models.__init__`` + the FR3 seam landed:
#:   * ``relational_imports`` 395 -> 384 (-11): enum-only ``from core.models.db``
#:     imports repointed to the agnostic ``core.domain.enums`` leaf (FR1/FR2), so
#:     those statements no longer reference the relational layer.
#:   * ``classified_call_sites`` 1490 -> 1422 (-68, all ``service``: 1211 -> 1143):
#:     the same enum imports/usages reclassified out of the relational surface,
#:     plus the PEP 562 lazy ``core.models.__init__`` no longer eager-importing
#:     ``core.models.db`` at module level.
#:   * ``relational_symbols`` unchanged (841): the extracted enums were never in
#:     ``_RELATIONAL_COVERAGE_SYMBOLS``.
#:   * ``file_count`` 429 -> 431 (+2): the two new agnostic leaf modules
#:     ``core/domain/enums.py`` and ``core/infra/schema_lifecycle.py``.
#: This is a structural re-baseline (enums leaving the relational layer), NOT a
#: gate relaxation: the stable FLOORS below are untouched and live >> floor.
#:
#: AF-02 IMP3 re-baseline (2026-07-02) keeps the exact-live gate semantics and
#: only updates this frozen snapshot after attributing HEAD 50e3193 -> worktree.
#: Residual drift already committed before this window: MCP factory counter +2,
#: async session type counter +1, import aggregate -5, symbol aggregate +6,
#: call-site aggregate +3 (rest -3/service +4/mcp +2), file_count +22.
#: Current sanctioned window: AF-02 transport factory strangler in
#: ``mcp/server.py`` and ``api/kg_tick.py``, injected ``kg_events_hub`` factory
#: wiring, AF-11 cleanup, and 8 new AF-02/03/04/09/11 gate/facade modules.
#: Stable floors stay untouched; this remains an exact drift detector.
RELATIONAL_COVERAGE_SNAPSHOT_R01B = {
    "relational_imports": 371,
    "relational_symbols": 851,
    "classified_call_sites": 1429,
    "by_surface": {"rest": 163, "service": 1149, "mcp": 117},
    "source_root": "okto_pulse/core",
    "file_count": 461,
}

#: R01B drawn-down counter baseline (ac_28f50f9d), SEPARATE from the spec #04
#: ``RELATIONAL_BASELINE`` (268/225/554, kept as historical). The R01A REST+MCP
#: CRUD strangler plus the R01B ownership inversion shrank the core-wide
#: Depends(get_db)/get_db_for_mcp/AsyncSession counts to this snapshot. New
#: measurement taken POST-TR5/R01B over the complete IMP2 tree — it supersedes
#: the mid-flight provisional (76/168/419), which predated the FR3 REST/MCP
#: repointing, the TR5 PRAGMA-seam edits and this AC5 module; the small deltas
#: are that re-measurement, not a guess.
#:
#: AF-02 IMP3 re-baseline (2026-07-02) updates the exact counters to the
#: attributed live tree after the same 50e3193 split above. The MCP factory
#: delta includes the sanctioned MCP twin health/session-scope wiring in
#: ``mcp/server.py``; the async session type counter reflects the typed injected scope in
#: ``api/kg_tick.py`` plus pre-existing committed drift. These counters are not
#: floors or relaxations: tests still require exact live equality and mutation
#: teeth prove mismatches fail.
RELATIONAL_BASELINE_R01B = {
    "depends_get_db": 79,
    "get_db_for_mcp": 175,
    "async_session": 425,
}

_COVERAGE_AGGREGATES = ("relational_imports", "relational_symbols", "classified_call_sites")
_COVERAGE_SURFACES = (SURFACE_REST, SURFACE_SERVICE, SURFACE_MCP)


def relational_coverage_counts(core_root: str | Path | None = None) -> dict:
    """Compute the three AC5 relational-coverage aggregates over the core tree.

    Reproducible AST scan (see the AC5 section above for the definitions).
    Returns the three aggregates plus the per-surface breakdown of the
    classified call-sites, the file count and the source root.
    """
    base = Path(core_root) if core_root is not None else default_core_path()
    relational_imports = 0
    relational_symbols = 0
    classified_call_sites = 0
    by_surface = {SURFACE_REST: 0, SURFACE_SERVICE: 0, SURFACE_MCP: 0}
    file_count = 0
    for path in base.rglob("*.py"):
        try:
            text = path.read_text(encoding="utf-8")
            tree = ast.parse(text, filename=str(path))
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        file_count += 1
        try:
            label = str(path.relative_to(base.parent))
        except ValueError:
            label = str(path)
        for violation in _scan_tree(tree, label):
            classified_call_sites += 1
            by_surface[violation.surface] = by_surface.get(violation.surface, 0) + 1
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if _is_relational_import_module(node.module) or any(
                    alias.name in _RELATIONAL_COVERAGE_SYMBOLS for alias in node.names
                ):
                    relational_imports += 1
            elif isinstance(node, ast.Import):
                if any(_is_relational_import_module(alias.name) for alias in node.names):
                    relational_imports += 1
            elif isinstance(node, ast.Name) and node.id in _RELATIONAL_COVERAGE_SYMBOLS:
                relational_symbols += 1
            elif isinstance(node, ast.Attribute) and node.attr in _RELATIONAL_COVERAGE_SYMBOLS:
                relational_symbols += 1
    return {
        "relational_imports": relational_imports,
        "relational_symbols": relational_symbols,
        "classified_call_sites": classified_call_sites,
        "by_surface": by_surface,
        "file_count": file_count,
        "source_root": "okto_pulse/core",
    }


def relational_coverage_drift(
    live: dict | None = None,
    *,
    core_root: str | Path | None = None,
    floors: dict | None = None,
    snapshot: dict | None = None,
    declared_removals: dict | None = None,
) -> dict:
    """AC5 coverage-drift verdict (ac_28f50f9d).

    Two checks against the frozen relational coverage:
      1. FLOOR — each aggregate must stay ``>=`` its ``RELATIONAL_COVERAGE_BASELINE``
         minimum.
      2. SNAPSHOT DRIFT — each frozen counter (the 3 aggregates + the 3 surface
         buckets) is compared to ``RELATIONAL_COVERAGE_SNAPSHOT_R01B``. A RISE is
         new coupling and is ALWAYS undeclared drift (a removal can never raise a
         count). A DROP is allowed ONLY when ``declared_removals`` carries a
         structured entry ``{"count": N, "task": "R01x-..."}`` whose task matches
         R01A/R01B/R01C and whose count covers the shortfall.

    ``ok`` is True only when there are no floor violations and no undeclared
    drift.
    """
    floors = dict(RELATIONAL_COVERAGE_BASELINE if floors is None else floors)
    snapshot = dict(RELATIONAL_COVERAGE_SNAPSHOT_R01B if snapshot is None else snapshot)
    if live is None:
        live = relational_coverage_counts(core_root)
    declared_removals = declared_removals or {}

    # Flatten the frozen counters: 3 aggregates + 3 surface buckets.
    live_counters = {k: int(live.get(k, 0)) for k in _COVERAGE_AGGREGATES}
    snap_counters = {k: int(snapshot.get(k, 0)) for k in _COVERAGE_AGGREGATES}
    live_surface = live.get("by_surface", {}) or {}
    snap_surface = snapshot.get("by_surface", {}) or {}
    for surface in _COVERAGE_SURFACES:
        live_counters[f"callsites_{surface}"] = int(live_surface.get(surface, 0))
        snap_counters[f"callsites_{surface}"] = int(snap_surface.get(surface, 0))

    floor_violations = [
        {"aggregate": k, "floor": floors[k], "live": live_counters.get(k, 0)}
        for k in floors
        if live_counters.get(k, 0) < floors[k]
    ]

    undeclared: list[dict] = []
    declared: list[dict] = []
    for counter, snap_v in snap_counters.items():
        delta = live_counters[counter] - snap_v
        if delta == 0:
            continue
        decl = declared_removals.get(counter)
        if isinstance(decl, dict):
            decl_count = int(decl.get("count", 0))
            decl_task = decl.get("task")
        else:
            decl_count = int(decl or 0)
            decl_task = None
        entry = {
            "counter": counter,
            "snapshot": snap_v,
            "live": live_counters[counter],
            "delta": delta,
            "declared_count": decl_count,
            "task": decl_task,
        }
        if (
            delta < 0
            and decl_task
            and _R01_TASK_RE.search(str(decl_task))
            and decl_count >= -delta
        ):
            declared.append(entry)
        else:
            undeclared.append(entry)

    return {
        "ok": not floor_violations and not undeclared,
        "floor_violations": floor_violations,
        "undeclared_drift": undeclared,
        "declared_removals": declared,
        "live": live_counters,
        "snapshot": snap_counters,
        "floors": floors,
    }


# ---------------------------------------------------------------------------
# Baseline-aware ratchet (SaaS Refactor spec R01A IMP7, FR4 / AC4 ac_877f378b).
#
# The plain gate (``run_relational_boundary_gate``) fails on ANY relational
# coupling under ``root``. The ratchet layer makes that *baseline-aware*: it
# fails only for couplings BEYOND a DECLARED baseline (i.e. a NEW direct
# ``Depends(get_db)`` / ``get_db_for_mcp`` / ``AsyncSession`` outside the
# baseline), reporting file/line/symbol and per-surface counters.
#
# The baseline is COUNT-aware, keyed by ``(file, symbol) -> tolerated count``. A
# bare ``(file, symbol)`` set membership would collapse every call-site of the
# same symbol in a file into one allowlist entry, letting a NEW second call-site
# of an already-baselined symbol slip through. Instead a file/symbol may have at
# most its baselined number of direct couplings; the ``(N+1)``-th is a new
# violation. Counts (not line numbers) carry identity, so the ratchet is robust to
# line shifts yet still catches a duplicate. The invariant is that the baseline may
# only SHRINK as symbols are strangled (see :func:`ratchet_baseline_only_shrinks`)
# — it must never grow to hide a new coupling.
# ---------------------------------------------------------------------------

#: Declared baseline for the strangled use-case package: CLEAN (0 tolerated for
#: every ``(file, symbol)``) — any relational coupling under
#: ``application/use_cases`` is a NEW direct use and must fail.
RELATIONAL_USE_CASES_BASELINE: dict = {}


def relational_ratchet_key(violation: RelationalViolation) -> tuple:
    """Group key for a detected coupling: ``(normalised file, symbol)``."""
    return (violation.file.replace("\\", "/"), violation.symbol)


@dataclass
class RelationalRatchetReport:
    ok: bool
    guarded_path: str
    scanned_files: int
    declared_baseline_total: int
    new_violations: list[RelationalViolation] = field(default_factory=list)
    new_by_surface: dict[str, int] = field(default_factory=dict)
    exceeded: list[dict] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "guarded_path": self.guarded_path,
            "scanned_files": self.scanned_files,
            "declared_baseline_total": self.declared_baseline_total,
            "new_by_surface": self.new_by_surface,
            "new_violations": [
                {
                    "file": v.file,
                    "line": v.line,
                    "symbol": v.symbol,
                    "surface": v.surface,
                }
                for v in self.new_violations
            ],
            "exceeded": self.exceeded,
        }


def run_relational_ratchet_gate(
    root: str | Path | None = None,
    *,
    baseline: dict | None = None,
) -> RelationalRatchetReport:
    """Baseline-aware ratchet over ``root`` (default: ``application/use_cases``).

    ``baseline`` maps ``(file, symbol)`` to the number of direct couplings tolerated
    for that call-site group (the documented baseline). Fails (``ok=False``) for
    every coupling BEYOND that count — a NEW direct ``Depends(get_db)`` /
    ``get_db_for_mcp`` / ``AsyncSession`` / ``select`` / ORM use — reporting it with
    file, line, symbol and per-surface counters, plus an ``exceeded`` block naming
    each ``(file, symbol)`` whose live count outran its baseline. Counts (not line
    numbers) carry identity, so the gate is robust to line shifts yet still catches
    a second call-site of an already-baselined symbol in the same file.
    """
    if baseline is None:
        baseline = RELATIONAL_USE_CASES_BASELINE
    report = run_relational_boundary_gate(root=root)

    grouped: dict[tuple, list[RelationalViolation]] = {}
    for v in report.violations:
        grouped.setdefault(relational_ratchet_key(v), []).append(v)

    new: list[RelationalViolation] = []
    exceeded: list[dict] = []
    for key, group in grouped.items():
        allowed = baseline.get(key, 0)
        if len(group) > allowed:
            ordered = sorted(group, key=lambda v: v.line)
            extra = ordered[allowed:]
            new.extend(extra)
            exceeded.append(
                {
                    "file": key[0],
                    "symbol": key[1],
                    "surface": ordered[0].surface,
                    "baseline": allowed,
                    "live": len(group),
                    "new_lines": [v.line for v in extra],
                }
            )

    by_surface: dict[str, int] = {}
    for v in new:
        by_surface[v.surface] = by_surface.get(v.surface, 0) + 1

    return RelationalRatchetReport(
        ok=not new,
        guarded_path=report.guarded_path,
        scanned_files=report.scanned_files,
        declared_baseline_total=sum(baseline.values()),
        new_violations=new,
        new_by_surface=by_surface,
        exceeded=exceeded,
    )


def ratchet_baseline_only_shrinks(previous: dict, current: dict) -> bool:
    """The ratchet invariant: the declared baseline may only SHRINK.

    Returns True iff no ``(file, symbol)`` tolerates MORE couplings than before —
    every ``current`` count is ``<=`` its ``previous`` count (missing == 0), so the
    baseline only drew down strangled symbols and never widened to hide a new
    coupling. A raised count or a brand-new tolerated key returns False.
    """
    for key, count in current.items():
        if count > previous.get(key, 0):
            return False
    return True
