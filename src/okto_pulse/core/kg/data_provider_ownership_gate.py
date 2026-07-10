"""DataProviderOwnershipGate (SaaS Refactor spec R05-D, Onda B / IMP2).

Fail-closed AST/import gate guarding the move of the KG DATA providers to the
Community composition root:
  - EventBus        -> CommunityOutboxEventBus
  - AuditRepository -> CommunityAuditRepository
  - KGConfig        -> SettingsKGConfig (retired core concrete)

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
concrete adapters are Community-owned.

R07 also makes ``CommunityKGConfig`` standalone. The gate therefore scans the
Community runtime package, when available, and blocks any import, reference, or
subclassing of the concrete core ``SettingsKGConfig`` outside tests/allowlist.
Legitimate protocol/DTO imports from core remain allowed. Read-only static
analysis (``ast`` + ``pathlib``).
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from importlib import util as importlib_util
from pathlib import Path

#: The three data-adapter class names whose core instantiation is owned wiring.
DATA_ADAPTER_SYMBOLS: frozenset[str] = frozenset(
    {
        "SqliteOutboxEventBus",
        "SqlAlchemyAuditRepository",
        "SettingsKGConfig",
    }
)

SETTINGS_CONFIG_MODULE = "okto_pulse.core.kg.providers.testing.settings_config"

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

# Runtime Community modules must not depend on the concrete core config helper.
# Tests are excluded by path; this allowlist stays explicit and empty until there
# is a documented non-runtime compatibility shim.
COMMUNITY_SETTINGS_CONFIG_ALLOWLIST: frozenset[str] = frozenset()

#: SQLAlchemy / the relational ORM model layer is a gated spec #04 temporary
#: exception. Recorded for dependency audit, not a direct data-adapter fallback.
SQLALCHEMY_OWNERSHIP_STATUS = "core-gated-04-temporary-exception"

COMMUNITY_ROOT = "okto_pulse.community"


@dataclass
class DataOwnershipReport:
    ok: bool
    community_import_offenders: list = field(default_factory=list)
    new_data_consumers: list = field(default_factory=list)
    community_settings_config_offenders: list = field(default_factory=list)
    ledger: dict = field(default_factory=dict)
    scanned_files: int = 0
    scanned_community_files: int = 0

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "community_import_offenders": list(self.community_import_offenders),
            "new_data_consumers": list(self.new_data_consumers),
            "community_settings_config_offenders": list(
                self.community_settings_config_offenders
            ),
            "ledger": self.ledger,
            "sqlalchemy_ownership": SQLALCHEMY_OWNERSHIP_STATUS,
            "scanned_files": self.scanned_files,
            "scanned_community_files": self.scanned_community_files,
        }


def default_core_package_path() -> Path:
    """The ``okto_pulse/core`` package dir (this file lives in core/kg/)."""
    return Path(__file__).resolve().parents[1]


def default_community_package_path() -> Path | None:
    """Best-effort importable ``okto_pulse/community`` package dir."""
    spec = importlib_util.find_spec("okto_pulse.community")
    if spec is None or spec.submodule_search_locations is None:
        return None
    for location in spec.submodule_search_locations:
        path = Path(location)
        if path.exists():
            return path.resolve()
    return None


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


def _attr_chain(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, ast.Name):
        return (node.id,)
    if isinstance(node, ast.Attribute):
        return (*_attr_chain(node.value), node.attr)
    return ()


def _settings_config_import_aliases(tree: ast.AST) -> tuple[dict[str, str], set[str]]:
    """Return local class aliases and module aliases for SettingsKGConfig."""
    class_aliases: dict[str, str] = {}
    module_aliases: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module != SETTINGS_CONFIG_MODULE:
                continue
            for alias in node.names:
                if alias.name == "SettingsKGConfig":
                    class_aliases[alias.asname or alias.name] = "SettingsKGConfig"
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == SETTINGS_CONFIG_MODULE:
                    module_aliases.add(alias.asname or alias.name.split(".")[0])
    return class_aliases, module_aliases


def _is_settings_config_ref(
    node: ast.AST,
    class_aliases: dict[str, str],
    module_aliases: set[str],
) -> bool:
    if isinstance(node, ast.Name):
        return node.id == "SettingsKGConfig" or node.id in class_aliases
    if isinstance(node, ast.Attribute):
        chain = _attr_chain(node)
        if not chain or chain[-1] != "SettingsKGConfig":
            return False
        if chain[0] in module_aliases:
            return True
        return tuple(chain[:-1]) == tuple(SETTINGS_CONFIG_MODULE.split("."))
    return False


def _community_settings_config_offenders(
    tree: ast.AST,
) -> list[tuple[str, int, str]]:
    """Return (kind, lineno, symbol) for concrete SettingsKGConfig runtime use."""
    class_aliases, module_aliases = _settings_config_import_aliases(tree)
    hits: list[tuple[str, int, str]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "") == SETTINGS_CONFIG_MODULE:
            for alias in node.names:
                if alias.name == "SettingsKGConfig":
                    hits.append(("import", node.lineno, "SettingsKGConfig"))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == SETTINGS_CONFIG_MODULE:
                    hits.append(("import", node.lineno, "SettingsKGConfig"))
        elif isinstance(node, ast.ClassDef):
            for base in node.bases:
                if _is_settings_config_ref(base, class_aliases, module_aliases):
                    hits.append(("subclass", node.lineno, "SettingsKGConfig"))
        elif isinstance(node, ast.Name):
            if node.id in class_aliases or node.id == "SettingsKGConfig":
                hits.append(("reference", node.lineno, "SettingsKGConfig"))
        elif isinstance(node, ast.Attribute):
            if _is_settings_config_ref(node, class_aliases, module_aliases):
                hits.append(("reference", node.lineno, "SettingsKGConfig"))

    deduped: list[tuple[str, int, str]] = []
    seen: set[tuple[str, int, str]] = set()
    for hit in hits:
        if hit not in seen:
            deduped.append(hit)
            seen.add(hit)
    return deduped


def _is_test_path(path: Path, base: Path) -> bool:
    rel_parts = path.relative_to(base).parts
    return "tests" in rel_parts or any(part.startswith("test_") for part in rel_parts)


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
    *,
    community_root: str | Path | None = None,
) -> DataOwnershipReport:
    """Scan core and Community runtime packages for data-provider ownership."""
    base = Path(root) if root is not None else default_core_package_path()
    self_file = Path(__file__).resolve()
    community_base = (
        Path(community_root)
        if community_root is not None
        else default_community_package_path()
    )

    community_offenders: list[dict] = []
    new_consumers: list[dict] = []
    community_config_offenders: list[dict] = []
    scanned = 0
    scanned_community = 0

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

    if community_base is not None and community_base.exists():
        for path in sorted(community_base.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            if _is_test_path(path, community_base):
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except (OSError, SyntaxError):
                continue
            scanned_community += 1
            rel = _norm(path, community_base)
            if rel in COMMUNITY_SETTINGS_CONFIG_ALLOWLIST:
                continue
            for kind, lineno, symbol in _community_settings_config_offenders(tree):
                community_config_offenders.append(
                    {
                        "file": rel,
                        "symbol": symbol,
                        "line": lineno,
                        "kind": kind,
                    }
                )

    ok = not community_offenders and not new_consumers and not community_config_offenders
    return DataOwnershipReport(
        ok=ok,
        community_import_offenders=sorted(
            community_offenders, key=lambda d: d["file"]
        ),
        new_data_consumers=sorted(
            new_consumers, key=lambda d: (d["file"], d["line"])
        ),
        community_settings_config_offenders=sorted(
            community_config_offenders,
            key=lambda d: (d["file"], d["line"], d["kind"]),
        ),
        ledger=dict(LEDGERED_DATA_FALLBACK),
        scanned_files=scanned,
        scanned_community_files=scanned_community,
    )


__all__ = [
    "DATA_ADAPTER_SYMBOLS",
    "LEDGERED_DATA_FALLBACK",
    "TEST_ONLY_CONFIG_FAKE_ROUTES",
    "COMMUNITY_SETTINGS_CONFIG_ALLOWLIST",
    "SQLALCHEMY_OWNERSHIP_STATUS",
    "DataOwnershipReport",
    "run_data_provider_ownership_gate",
    "default_core_package_path",
    "default_community_package_path",
]
