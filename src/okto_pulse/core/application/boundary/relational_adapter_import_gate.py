"""RelationalAdapterImportGate (spec R01B, REPLAN-IMP2 / FR4 / AC4) — block NEW
direct imports/uses of the relational RUNTIME adapter surface outside a temporary,
GOVERNED allowlist, so the inbound runtime seams (REST ``get_unit_of_work`` / MCP
``get_unit_of_work_factory_for_mcp``) resolve the edition-registered provider via
:mod:`okto_pulse.core.runtime_registry` and the remaining UnitOfWork/concrete
repository surface can finish moving to the Community adapter editions.

The gate fails if core runtime, OUTSIDE the temporary allowlist, references any of:
  * the concrete relational package ``okto_pulse.core.repositories.sqlalchemy``
    (or any submodule / re-exported symbol);
  * ``sqlalchemy.ext.asyncio.create_async_engine`` — the engine factory (the HARD
    textual requirement) — imported directly, aliased, module-aliased, or used in
    an assignment / call chain;
  * ``async_sessionmaker`` — the concrete async session factory (same
    runtime-adapter-ownership pattern);
  * the UnitOfWork concretes ``SQLAlchemyUnitOfWork`` / ``SQLAlchemyUnitOfWorkFactory``.

ALIAS-AWARE (the R05-D lesson — mirrors ``telemetry_import_gate`` /
``data_provider_ownership_gate``): the AST resolves import-as + from-import +
qualified attribute + ASSIGNMENT-CHAIN aliases to the canonical symbol BEFORE the
allowlist check, so ``from sqlalchemy.ext.asyncio import create_async_engine as
cae; engine = cae(url)`` or ``F = SQLAlchemyUnitOfWorkFactory; G = F`` cannot
bypass it. IMPORT-AWARE too: a bare ``from x import create_async_engine`` (the
exact re-introduction a register-before-remove gate guards) is flagged even with
no later use. Every finding carries the offending ``line``.

GOVERNED allowlist (TR3 register-before-remove, NOT a generic escape): every
allowlisted reference is an explicit :class:`AllowlistEntry` carrying an ``owner``,
a ``reason`` and a ``removal_criterion`` bound to R01C — surfaced in the report so
a reviewer can audit each waiver. A NEW importer/user outside the allowlist BLOCKS.
The strangled runtime seams (``api/deps.py`` / ``mcp/server.py`` / engine
startup) are deliberately OFF the allowlist and no longer reference the adapter
surface (proof of strangle). String literals (e.g. names quoted in ``__all__`` /
docstrings / a debt-ledger ``location=``) are ``ast.Constant`` and are never
flagged.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

#: Relational runtime-adapter symbols whose direct import/use is restricted.
#: ``create_async_engine`` is the HARD textual requirement; ``async_sessionmaker``
#: is the concrete session factory (same ownership pattern); the two
#: ``SQLAlchemyUnitOfWork*`` concretes complete the UnitOfWork surface.
SENSITIVE_SYMBOLS: tuple[str, ...] = (
    "SQLAlchemyUnitOfWork",
    "SQLAlchemyUnitOfWorkFactory",
    "create_async_engine",
    "async_sessionmaker",
)
_SENSITIVE = frozenset(SENSITIVE_SYMBOLS)

#: Concrete relational ADAPTER package; importing it (or a submodule / a symbol
#: from it) outside the allowlist is the import-side re-introduction the gate blocks.
SENSITIVE_MODULES: tuple[str, ...] = ("okto_pulse.core.repositories.sqlalchemy",)


@dataclass(frozen=True)
class AllowlistEntry:
    """A single GOVERNED waiver — a temporary reference that R01C retires.

    ``pattern`` is matched as an exact rel-path when ``kind == 'file'`` or as a
    path prefix when ``kind == 'prefix'`` (norm path under ``okto_pulse/core``).
    """

    pattern: str
    kind: str  # 'file' | 'prefix'
    owner: str
    reason: str
    removal_criterion: str

    def matches(self, rel: str) -> bool:
        if self.kind == "file":
            return rel == self.pattern
        return rel.startswith(self.pattern)


#: The TEMPORARY, governed allowlist. Each entry retires at R01C — none is a
#: generic escape: every one names an owner + a concrete removal criterion.
ALLOWLIST: tuple[AllowlistEntry, ...] = (
    AllowlistEntry(
        pattern="repositories/sqlalchemy/",
        kind="prefix",
        owner="okto-pulse-core/repositories",
        reason="concrete relational adapter package — UoW + repository definitions "
        "and the per-package re-export (the home of the concretes).",
        removal_criterion="R01C physically removes the SQLAlchemy concretes from core.",
    ),
    AllowlistEntry(
        pattern="repositories/__init__.py",
        kind="file",
        owner="okto-pulse-core/repositories",
        reason="core aggregator re-exporting the UoW concretes for the remaining "
        "test / R01C-compat importers (no runtime construction).",
        removal_criterion="R01C: drop the re-export once no core caller imports the concretes.",
    ),
)

#: Back-compat flat views derived from the governed allowlist.
ALLOWLISTED_PREFIXES: tuple[str, ...] = tuple(
    e.pattern for e in ALLOWLIST if e.kind == "prefix"
)
ALLOWLISTED_FILES: frozenset[str] = frozenset(
    e.pattern for e in ALLOWLIST if e.kind == "file"
)

#: Gate-level removal summary (per-entry criteria live on each AllowlistEntry).
REMOVAL_CRITERION = (
    "R01B IMP2: runtime authority over the relational adapter (UnitOfWork concretes + "
    "engine/session factory) removed from startup and the inbound REST/MCP seams "
    "(now resolve registered providers via runtime_registry / edition injection). "
    "Each remaining reference is a GOVERNED allowlist waiver (owner + "
    "removal_criterion) retired by R01C when the concretes physically leave core."
)


@dataclass(frozen=True)
class RelationalAdapterImportFinding:
    file: str
    symbol: str
    line: int
    allowlisted: bool


@dataclass
class RelationalAdapterImportReport:
    ok: bool
    findings: tuple[RelationalAdapterImportFinding, ...] = ()
    violations: tuple[RelationalAdapterImportFinding, ...] = ()

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "violations": [
                {"file": v.file, "symbol": v.symbol, "line": v.line} for v in self.violations
            ],
            "findings": [
                {"file": f.file, "symbol": f.symbol, "line": f.line, "allowlisted": f.allowlisted}
                for f in self.findings
            ],
            "allowlist": [
                {
                    "pattern": e.pattern,
                    "kind": e.kind,
                    "owner": e.owner,
                    "reason": e.reason,
                    "removal_criterion": e.removal_criterion,
                }
                for e in ALLOWLIST
            ],
            "removal_criterion": REMOVAL_CRITERION,
        }


def _default_core_root() -> Path:
    # src/okto_pulse/core/application/boundary/relational_adapter_import_gate.py -> core/
    return Path(__file__).resolve().parents[2]


def _build_alias_map(tree: ast.AST) -> dict[str, str]:
    """Map every local name referring to a sensitive symbol to its canonical
    name (import renames + assignment chains, R05-D pattern)."""
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for a in node.names:
                if a.name in _SENSITIVE:
                    aliases[a.asname or a.name] = a.name
        elif isinstance(node, ast.Import):
            for a in node.names:
                last = a.name.rsplit(".", 1)[-1]
                if last in _SENSITIVE:
                    aliases[a.asname or last] = last

    def _resolve(name: str) -> str | None:
        return name if name in _SENSITIVE else aliases.get(name)

    changed = True
    while changed:
        changed = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Name):
                canonical = _resolve(node.value.id)
                if canonical is None:
                    continue
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name) and aliases.get(tgt.id) != canonical:
                        aliases[tgt.id] = canonical
                        changed = True
    return aliases


def _is_sensitive_module(dotted: str) -> bool:
    return any(dotted == m or dotted.startswith(m + ".") for m in SENSITIVE_MODULES)


def _detections(tree: ast.AST) -> list[tuple[str, int]]:
    """``(symbol_label, lineno)`` for every sensitive import or use.

    Covers: sensitive-symbol imports (import-aware), sensitive-symbol uses (Name /
    Attribute, alias + assignment-chain aware), and sensitive-MODULE imports
    (``from okto_pulse.core.repositories.sqlalchemy[...] import X`` /
    ``from okto_pulse.core.repositories import sqlalchemy`` / ``import ...sqlalchemy``).
    """
    alias_map = _build_alias_map(tree)
    out: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if node.level == 0 and mod and _is_sensitive_module(mod):
                out.append(("repositories.sqlalchemy", node.lineno))
            for a in node.names:
                if node.level == 0 and mod and _is_sensitive_module(f"{mod}.{a.name}"):
                    out.append(("repositories.sqlalchemy", node.lineno))
                if a.name in _SENSITIVE:
                    out.append((a.name, node.lineno))
        elif isinstance(node, ast.Import):
            for a in node.names:
                if _is_sensitive_module(a.name):
                    out.append(("repositories.sqlalchemy", node.lineno))
                last = a.name.rsplit(".", 1)[-1]
                if last in _SENSITIVE:
                    out.append((last, node.lineno))
        elif isinstance(node, ast.Name):
            canonical = node.id if node.id in _SENSITIVE else alias_map.get(node.id)
            if canonical is not None:
                out.append((canonical, node.lineno))
        elif isinstance(node, ast.Attribute) and node.attr in _SENSITIVE:
            out.append((node.attr, node.lineno))
    return out


def _is_allowlisted(rel: str) -> bool:
    return any(e.matches(rel) for e in ALLOWLIST)


def run_relational_adapter_import_gate(
    core_root: str | Path | None = None,
) -> RelationalAdapterImportReport:
    """Scan ``core_root`` (the ``okto_pulse/core`` dir) for direct references to
    the relational adapter surface; any outside the governed allowlist is a violation."""
    root = Path(core_root) if core_root is not None else _default_core_root()
    self_file = Path(__file__).resolve()
    findings: list[RelationalAdapterImportFinding] = []
    seen: set[tuple[str, str, int]] = set()
    for py in sorted(root.rglob("*.py")):
        if "__pycache__" in py.parts or py.resolve() == self_file:
            continue
        try:
            tree = ast.parse(py.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        rel = py.relative_to(root).as_posix()
        allow = _is_allowlisted(rel)
        for symbol, line in _detections(tree):
            key = (rel, symbol, line)
            if key in seen:
                continue
            seen.add(key)
            findings.append(
                RelationalAdapterImportFinding(
                    file=rel, symbol=symbol, line=line, allowlisted=allow
                )
            )
    findings.sort(key=lambda f: (f.file, f.line, f.symbol))
    violations = tuple(f for f in findings if not f.allowlisted)
    return RelationalAdapterImportReport(
        ok=not violations,
        findings=tuple(findings),
        violations=violations,
    )


__all__ = [
    "SENSITIVE_SYMBOLS",
    "SENSITIVE_MODULES",
    "AllowlistEntry",
    "ALLOWLIST",
    "ALLOWLISTED_FILES",
    "ALLOWLISTED_PREFIXES",
    "REMOVAL_CRITERION",
    "RelationalAdapterImportFinding",
    "RelationalAdapterImportReport",
    "run_relational_adapter_import_gate",
]
