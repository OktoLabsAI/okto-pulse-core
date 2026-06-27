"""DataProviderOwnershipGate (SaaS Refactor spec R05-D, Onda B / IMP2).

Fail-closed AST/import gate guarding the move of the KG DATA providers to the
Community composition root:
  - EventBus        -> CommunityOutboxEventBus
  - AuditRepository -> CommunityAuditRepository
  - KGConfig        -> SettingsKGConfig

It BLOCKS (``ok=False``) when:
  1. any core module imports ``okto_pulse.community`` (the edition must never be
     a core dependency — recontamination); OR
  2. any core module directly INSTANTIATES one of the retired data adapter
     classes. R-P2-02 retired the registry session_factory auto-wire, so the
     ledger is now empty and there is no legitimate core fallback path.

R-P2-03D already retired the ``SettingsKGConfig`` config fallback from real
composition. The remaining ``SettingsKGConfig`` instantiation in
``registry._build_defaults`` is a narrowly allowlisted TEST-ONLY fake route; any
new config instantiation elsewhere is still blocked. SQLAlchemy / the ORM model
layer remains a gated #04 temporary exception; the EventBus and AuditRepository
concrete adapters are Community-owned. Read-only static analysis (``ast`` +
``pathlib``).
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

#: R-P2-02 retires the last relational data-provider fallback in core. This stays
#: as an empty public constant so previous conformance tests can assert the
#: zero-fallback contract directly.
LEDGERED_DATA_FALLBACK: dict[str, dict] = {}

#: R-P2-03D leaves only the sanctioned test helper path that builds
#: SettingsKGConfig through ``defaults_factory``. This is not a runtime fallback
#: and is kept separate from the relational fallback ledger, which must remain
#: empty after R-P2-02.
TEST_ONLY_CONFIG_FAKE_ROUTES: frozenset[str] = frozenset(
    {"kg/interfaces/registry.py"}
)

#: SQLAlchemy / the relational ORM model layer is a gated spec #04 temporary
#: exception. Recorded for dependency audit, not a direct data-adapter fallback.
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
            if symbol == "SettingsKGConfig" and rel in TEST_ONLY_CONFIG_FAKE_ROUTES:
                continue
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
    "TEST_ONLY_CONFIG_FAKE_ROUTES",
    "SQLALCHEMY_OWNERSHIP_STATUS",
    "DataOwnershipReport",
    "run_data_provider_ownership_gate",
    "default_core_package_path",
]
