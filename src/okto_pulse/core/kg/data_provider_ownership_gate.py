"""DataProviderOwnershipGate (SaaS Refactor spec R05-D, Onda B / IMP2).

Fail-closed AST/import gate guarding the move of the three KG DATA providers to
the Community composition root:
  - EventBus        -> SqliteOutboxEventBus
  - AuditRepository -> SqlAlchemyAuditRepository
  - KGConfig        -> SettingsKGConfig

It BLOCKS (``ok=False``) when:
  1. any core module imports ``okto_pulse.community`` (the edition must never be
     a core dependency — recontamination); OR
  2. a core module OTHER than the single ledgered fallback
     (``kg/interfaces/registry.py``) directly INSTANTIATES one of the three data
     adapters — i.e. a NEW local consumer / a NEW auto-wire outside the ledger.

The one legitimate remaining core auto-wire — the ``configure_kg_registry``
session_factory auto-wire of audit_repo/event_bus — is the LEDGERED FALLBACK
(``LEDGERED_DATA_FALLBACK``): a register-before-fallback temporary exception that
fires ONLY when a ``base_registry`` / ``defaults_factory`` composition left those
slots ``None``; the Community edition supplies them explicitly, so it never runs
for that edition. R-P2-03D RETIRED the ``SettingsKGConfig`` config fallback —
``_build_graph_defaults`` no longer fills the config slot and config is now a
composition-required slot (``configure_kg_registry`` fails closed without it); the
only remaining ``SettingsKGConfig`` instantiation (``registry._build_defaults``) is
the TEST-ONLY fake route. SQLAlchemy / the ORM is NOT moved by R05-D (that is the
gated spec #04 strangling). Read-only static analysis (``ast`` + ``pathlib``).
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

#: The three data-adapter class names whose core instantiation is owned wiring.
DATA_ADAPTER_SYMBOLS: frozenset[str] = frozenset(
    {
        "SqliteOutboxEventBus",
        "SqlAlchemyAuditRepository",
        "SettingsKGConfig",
    }
)

#: The SINGLE ledgered fallback location (norm path) where the core may still
#: instantiate a data adapter — register-before-fallback temporary exception.
LEDGERED_DATA_FALLBACK: dict[str, dict] = {
    "kg/interfaces/registry.py": {
        "owner": "core-kg/registry",
        "reason": (
            "configure_kg_registry session_factory auto-wire of audit_repo "
            "(SqlAlchemyAuditRepository) + event_bus (SqliteOutboxEventBus) — fires "
            "ONLY when the composition left the slot None (prefer-provided); the "
            "Community edition supplies both explicitly "
            "(community.adapters.composition._apply_data_providers), so this "
            "auto-wire never runs for it; it remains a ledgered fallback for a "
            "base_registry/defaults_factory that leaves audit_repo/event_bus None "
            "(R-P2-03 retired the non-composed path — it now fails closed). "
            "R-P2-03D RETIRED the SettingsKGConfig config "
            "fallback: _build_graph_defaults no longer fills the config slot and "
            "configure_kg_registry now REQUIRES config (fail-closed). The remaining "
            "SettingsKGConfig instantiation in registry._build_defaults is the "
            "TEST-ONLY fake route (defaults_factory), NOT a runtime fallback."
        ),
        "target_ports": ["EventBus", "AuditRepository"],
        "retired_ports": {
            "KGConfig": "R-P2-03D — config is now composition-required (fail-closed)"
        },
        "removal_criterion": (
            "R-P2-03D already retired the SettingsKGConfig config fallback. spec "
            "#04: when the Repository-UoW strangling lands and every edition "
            "composes its data providers, drop the session_factory auto-wire of "
            "audit_repo/event_bus — this entry then becomes empty and the gate "
            "enforces zero core instantiation."
        ),
    },
}

#: SQLAlchemy / the relational ORM is a gated spec #04 temporary exception — it
#: STAYS in core (R05-D does NOT strangle the Repository-UoW). Recorded for the
#: dependency audit, not a violation here.
SQLALCHEMY_OWNERSHIP_STATUS = "core-gated-04-temporary-exception"

COMMUNITY_ROOT = "okto_pulse.community"


@dataclass
class DataOwnershipReport:
    ok: bool
    community_import_offenders: list = field(default_factory=list)
    new_data_consumers: list = field(default_factory=list)
    ledger: dict = field(default_factory=dict)
    scanned_files: int = 0

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "community_import_offenders": list(self.community_import_offenders),
            "new_data_consumers": list(self.new_data_consumers),
            "ledger": self.ledger,
            "sqlalchemy_ownership": SQLALCHEMY_OWNERSHIP_STATUS,
            "scanned_files": self.scanned_files,
        }


def default_core_package_path() -> Path:
    """The ``okto_pulse/core`` package dir (this file lives in core/kg/)."""
    return Path(__file__).resolve().parents[1]


def _norm(path: Path, base: Path) -> str:
    return str(path.relative_to(base)).replace("\\", "/")


def _imports_community(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod == COMMUNITY_ROOT or mod.startswith(COMMUNITY_ROOT + "."):
                return True
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.name == COMMUNITY_ROOT or a.name.startswith(COMMUNITY_ROOT + "."):
                    return True
    return False


def _build_alias_map(tree: ast.AST) -> dict[str, str]:
    """Map every LOCAL name that ultimately refers to a data-adapter symbol to its
    canonical ``DATA_ADAPTER_SYMBOLS`` name, so an aliased instantiation cannot
    slip past the Call check. Resolves:

      - ``from x import SqliteOutboxEventBus``        -> {SqliteOutboxEventBus: ...}
      - ``from x import SqliteOutboxEventBus as Bus`` -> {Bus: SqliteOutboxEventBus}
      - ``import pkg.mod.SettingsKGConfig as Y``      -> {Y: SettingsKGConfig}
      - ``Alias = SqliteOutboxEventBus`` / ``B = Alias`` -> transitive (fixpoint)
    """
    aliases: dict[str, str] = {}

    # Pass 1 — import renames.
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for a in node.names:
                if a.name in DATA_ADAPTER_SYMBOLS:
                    aliases[a.asname or a.name] = a.name
        elif isinstance(node, ast.Import):
            for a in node.names:
                last = a.name.rsplit(".", 1)[-1]
                if last in DATA_ADAPTER_SYMBOLS:
                    aliases[a.asname or last] = last

    # Pass 2 — simple-name assignment chains (``B = A = Symbol``) to a fixpoint.
    def _resolve(name: str) -> str | None:
        if name in DATA_ADAPTER_SYMBOLS:
            return name
        return aliases.get(name)

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


def _data_instantiations(
    tree: ast.AST, alias_map: dict[str, str]
) -> list[tuple[str, int]]:
    """Return (canonical_symbol, lineno) for each Call that instantiates a data
    adapter — resolving import/assignment aliases via ``alias_map`` so a renamed
    instantiation (``from x import SqliteOutboxEventBus as Bus; Bus(sf)``) counts."""
    hits: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            fn = node.func
            local = (
                fn.id
                if isinstance(fn, ast.Name)
                else (fn.attr if isinstance(fn, ast.Attribute) else None)
            )
            if local is None:
                continue
            canonical = (
                local if local in DATA_ADAPTER_SYMBOLS else alias_map.get(local)
            )
            if canonical is not None:
                hits.append((canonical, node.lineno))
    return hits


def run_data_provider_ownership_gate(
    root: str | Path | None = None,
) -> DataOwnershipReport:
    """Scan the core package and enforce data-provider ownership (fail-closed)."""
    base = Path(root) if root is not None else default_core_package_path()
    self_file = Path(__file__).resolve()

    community_offenders: list[dict] = []
    new_consumers: list[dict] = []
    scanned = 0

    for path in sorted(base.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        if path.resolve() == self_file:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        scanned += 1
        rel = _norm(path, base)

        if _imports_community(tree):
            community_offenders.append({"file": rel})

        alias_map = _build_alias_map(tree)
        for symbol, lineno in _data_instantiations(tree, alias_map):
            if rel not in LEDGERED_DATA_FALLBACK:
                new_consumers.append(
                    {"file": rel, "symbol": symbol, "line": lineno}
                )

    ok = not community_offenders and not new_consumers
    return DataOwnershipReport(
        ok=ok,
        community_import_offenders=sorted(
            community_offenders, key=lambda d: d["file"]
        ),
        new_data_consumers=sorted(
            new_consumers, key=lambda d: (d["file"], d["line"])
        ),
        ledger=dict(LEDGERED_DATA_FALLBACK),
        scanned_files=scanned,
    )


__all__ = [
    "DATA_ADAPTER_SYMBOLS",
    "LEDGERED_DATA_FALLBACK",
    "SQLALCHEMY_OWNERSHIP_STATUS",
    "DataOwnershipReport",
    "run_data_provider_ownership_gate",
    "default_core_package_path",
]
