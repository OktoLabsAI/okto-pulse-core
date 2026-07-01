"""create_database decomposition manifest + startup-parity oracle + R01C removal
ordering invariant (SaaS Refactor spec R01C, FR5 fr_4b186577, TR4 tr_418fc1a3,
AC ac_af454ee3; integration scenario ``ts_cdbe5d65``).

R01C must DECOMPOSE ``core/infra/database.py`` along the R01B boundary WITHOUT
changing startup (decision ``dec_0f85b536``) and WITHOUT removing anything yet
(ac_af454ee3 — startup parity preserved until R01B exposes the definitive
relational provider). This module is the *removal evidence*: it does not move or
delete code, it makes the decomposition explicit, testable and ordered.

Three artifacts:

1. **Decomposition manifest** — every module-level function (and the key state
   globals) in ``database.py`` is assigned to either the R01B *relational
   provider* concern (engine / session / pool / PRAGMA / connection cleanup) or
   the R01C *schema-lifecycle* concern (``init_db`` / migrations / bootstrap /
   seed / the declarative ``Base``). :func:`decomposition_drift` parses the live
   module and FAILS if any function is unclassified or any manifest entry is
   stale — so the decomposition can never silently drift from the source.

2. **Startup-parity oracle** (:func:`startup_parity_errors`) — a no-functional-
   change oracle pinning the exact SQLite hardening PRAGMAs (WAL /
   busy_timeout=30000 / synchronous=NORMAL), the SQLite engine pool tuning
   (pool_size/max_overflow/pool_timeout/pool_recycle/pool_pre_ping) and the
   preserved startup API (``create_database`` / ``get_engine`` /
   ``get_session_factory`` / ``close_db``). Any drift in those values or a missing
   API symbol is an error.

3. **R01C removal ordering invariant** (:func:`r01c_lifecycle_removal_readiness`)
   — TR4: the core may only retire its schema lifecycle AFTER R01B registers the
   Community relational provider (the UnitOfWorkFactory + SQLite PRAGMA installer
   seams in :mod:`runtime_registry`). FAIL-CLOSED: with no composition root the
   provider is unregistered, so removal is NOT allowed.

Pure static analysis for the manifest/oracle (``ast`` + source read); the
ordering invariant reads the runtime registry seams. No engine is constructed.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from okto_pulse.core.runtime_registry import (
    is_sqlite_pragma_installer_registered,
    is_unit_of_work_factory_registered,
)

# --- decomposition manifest ----------------------------------------------------

#: R01B relational-provider concern: engine / session / pool / PRAGMA /
#: connection cleanup. Preserved by R01B; NOT removed by R01C.
R01B_PROVIDER_FUNCTIONS: frozenset[str] = frozenset({
    "create_database",
    "get_engine",
    "get_session_factory",
    "close_db",
    "cancel_safe_session",
    "get_pool_status",
    "_install_pool_observability",
    "_consume_cleanup_exception",
    "_await_cleanup",
    "_quiet_cleanup",
    "get_db_session",
    "get_db",
})

#: R01C schema-lifecycle concern: init_db + migrations + data bootstrap/seed.
#: These move to the Community lifecycle (RelationalSchemaMigrator /
#: DataBootstrapper) once R01B provides the registered relational provider.
R01C_LIFECYCLE_FUNCTIONS: frozenset[str] = frozenset({
    "init_db",
    "_bootstrap_default_discovery_intents",
    "_seed_builtin_presets",
    "_merge_missing_flags",
    "_set_all_leaves",
    "_count_leaves",
    "_reconcile_builtin_presets",
    "_reconcile_agent_permission_flags",
})

#: Migrations are R01C lifecycle too, matched by the ``_migrate_`` prefix so a
#: newly-added migration is auto-classified (never silently unclassified).
R01C_MIGRATION_PREFIX = "_migrate_"

#: Key module state globals split across the same boundary (``Base`` is the ORM
#: declarative base — schema concern, R01C; the engine/session singletons are the
#: provider, R01B). Other module constants (logger, pool-warn thresholds) are
#: provider-internal and out of the classified surface.
R01B_PROVIDER_STATE: frozenset[str] = frozenset({"_engine", "_session_factory"})
R01C_LIFECYCLE_STATE: frozenset[str] = frozenset({"Base"})

#: Preserved startup API (ac_af454ee3). ``close_db`` / ``get_session_factory`` are
#: the concrete symbols the AC references as ``close_database`` / ``session_factory``.
PRESERVED_STARTUP_API: tuple[str, ...] = (
    "create_database", "get_engine", "get_session_factory", "close_db",
)

#: Exact SQLite hardening PRAGMAs that MUST stay byte-for-byte (ts_cdbe5d65).
EXPECTED_CORE_SQLITE_PRAGMAS: tuple[str, ...] = (
    "PRAGMA journal_mode=WAL",
    "PRAGMA busy_timeout=30000",
    "PRAGMA synchronous=NORMAL",
)

#: Exact SQLite engine pool tuning that MUST stay (deadlock fix 2026-04-29).
EXPECTED_SQLITE_POOL_KWARGS: dict[str, object] = {
    "pool_size": 20,
    "max_overflow": 30,
    "pool_timeout": 10,
    "pool_recycle": 1800,
    "pool_pre_ping": True,
}


def _database_path() -> Path:
    return Path(__file__).resolve().parent / "database.py"


def _runtime_registry_path() -> Path:
    return Path(__file__).resolve().parent.parent / "runtime_registry.py"


def _module_functions(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return [
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]


def classify_function(name: str) -> str | None:
    """Return ``"r01b"`` / ``"r01c"`` for a database.py function name, or None if
    unclassified."""
    if name in R01B_PROVIDER_FUNCTIONS:
        return "r01b"
    if name in R01C_LIFECYCLE_FUNCTIONS or name.startswith(R01C_MIGRATION_PREFIX):
        return "r01c"
    return None


@dataclass
class DecompositionDriftReport:
    ok: bool
    unclassified: list[str]
    stale: list[str]

    def as_dict(self) -> dict:
        return {"ok": self.ok, "unclassified": self.unclassified, "stale": self.stale}


def decomposition_drift(database_path: str | Path | None = None) -> DecompositionDriftReport:
    """Verify the manifest covers ``database.py`` with no drift.

    ``unclassified`` = live functions assigned to neither concern (a new function
    silently escaping the decomposition). ``stale`` = manifest function names no
    longer present in the source. Both empty == the decomposition is real and current.
    """
    path = Path(database_path) if database_path is not None else _database_path()
    live = _module_functions(path)
    unclassified = sorted(n for n in live if classify_function(n) is None)
    live_set = set(live)
    manifest = set(R01B_PROVIDER_FUNCTIONS) | set(R01C_LIFECYCLE_FUNCTIONS)
    stale = sorted(n for n in manifest if n not in live_set)
    return DecompositionDriftReport(ok=not unclassified and not stale, unclassified=unclassified, stale=stale)


def _create_database_pool_pairs(database_path: Path) -> set[tuple[str, object]]:
    """Collect ``(key, literal_value)`` pairs from every dict literal inside
    ``create_database`` — covers the engine pool kwargs without constructing an engine."""
    tree = ast.parse(database_path.read_text(encoding="utf-8"), filename=str(database_path))
    pairs: set[tuple[str, object]] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "create_database":
            for dict_node in ast.walk(node):
                if isinstance(dict_node, ast.Dict):
                    for key, value in zip(dict_node.keys, dict_node.values):
                        if isinstance(key, ast.Constant) and isinstance(value, ast.Constant):
                            pairs.add((key.value, value.value))
    return pairs


def startup_parity_errors(
    database_path: str | Path | None = None,
    runtime_registry_path: str | Path | None = None,
) -> list[str]:
    """Return startup-parity violations (empty == parity preserved).

    Pins the SQLite PRAGMAs, the SQLite pool tuning and the preserved startup API
    so any change to those is surfaced as a no-functional-change regression.
    """
    db_path = Path(database_path) if database_path is not None else _database_path()
    rr_path = Path(runtime_registry_path) if runtime_registry_path is not None else _runtime_registry_path()
    errors: list[str] = []

    # PRAGMAs (the core-default installer lives in runtime_registry).
    rr_source = rr_path.read_text(encoding="utf-8")
    for pragma in EXPECTED_CORE_SQLITE_PRAGMAS:
        if pragma not in rr_source:
            errors.append(f"missing core SQLite PRAGMA: {pragma!r}")

    # SQLite pool tuning (inside create_database).
    pairs = _create_database_pool_pairs(db_path)
    for key, value in EXPECTED_SQLITE_POOL_KWARGS.items():
        if (key, value) not in pairs:
            errors.append(f"SQLite pool tuning drift: expected {key}={value!r}")

    # Preserved startup API.
    live = set(_module_functions(db_path))
    for symbol in PRESERVED_STARTUP_API:
        if symbol not in live:
            errors.append(f"preserved startup API missing: {symbol}")

    return errors


@dataclass
class R01CRemovalReadiness:
    allowed: bool
    uow_factory_registered: bool
    sqlite_pragma_installer_registered: bool
    reason: str

    def as_dict(self) -> dict:
        return {
            "allowed": self.allowed,
            "uow_factory_registered": self.uow_factory_registered,
            "sqlite_pragma_installer_registered": self.sqlite_pragma_installer_registered,
            "reason": self.reason,
        }


def r01c_lifecycle_removal_readiness() -> R01CRemovalReadiness:
    """TR4 ordering invariant: the core may retire its schema lifecycle ONLY after
    R01B registers the Community relational provider (UnitOfWorkFactory seam) and
    SQLite PRAGMA installer. FAIL-CLOSED — unregistered means removal is blocked.
    """
    uow = is_unit_of_work_factory_registered()
    pragma = is_sqlite_pragma_installer_registered()
    allowed = uow and pragma
    if allowed:
        reason = (
            "R01B relational provider registered (UnitOfWorkFactory + SQLite PRAGMA "
            "installer): R01C may move schema lifecycle to Community."
        )
    else:
        missing = []
        if not uow:
            missing.append("UnitOfWorkFactory")
        if not pragma:
            missing.append("SQLite PRAGMA installer")
        reason = (
            "R01B relational provider NOT fully registered (missing: "
            f"{', '.join(missing)}); core retains schema lifecycle (TR4 — "
            "register-before-remove)."
        )
    return R01CRemovalReadiness(
        allowed=allowed,
        uow_factory_registered=uow,
        sqlite_pragma_installer_registered=pragma,
        reason=reason,
    )
