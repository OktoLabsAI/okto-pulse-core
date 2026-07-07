"""Relational lifecycle decomposition manifest + startup-boundary oracle.

R01C decomposes ``core/infra/database.py`` along the R01B boundary. Core keeps
runtime injection, sessions, cleanup and the ORM declarative ``Base``. Concrete
schema lifecycle execution (migrations, ``create_all`` and data bootstrap) is
Community-owned and reached through the mandatory schema-lifecycle seam.

Three artifacts:

1. **Decomposition manifest** — every module-level function (and the key state
   globals) in ``database.py`` is assigned to either the R01B *relational
   provider* concern (engine / session / pool / PRAGMA / connection cleanup) or
   the R01C *schema-lifecycle* concern (``init_db`` delegation). :func:`decomposition_drift` parses the live
   module and FAILS if any function is unclassified or any manifest entry is
   stale — so the decomposition can never silently drift from the source.

2. **Startup-boundary oracle** (:func:`startup_parity_errors`) — verifies the
   core no longer owns concrete engine/session construction or SQLite listener
   installation while preserving the injected runtime facade
   (``configure_database_runtime`` / ``get_engine`` / ``get_session_factory`` /
   ``close_db``).

3. **R01C removal ordering invariant** (:func:`r01c_lifecycle_removal_readiness`)
   — TR4: the core may only retire its schema lifecycle AFTER R01B registers the
   Community relational provider (the UnitOfWorkFactory + relational runtime
   factory seams in :mod:`runtime_registry`). FAIL-CLOSED: with no composition root the
   provider is unregistered, so removal is NOT allowed.

Pure static analysis for the manifest/oracle (``ast`` + source read); the
ordering invariant reads the runtime registry seams. No engine is constructed.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from okto_pulse.core.runtime_registry import (
    is_relational_runtime_factory_registered,
    is_unit_of_work_factory_registered,
)

# --- decomposition manifest ----------------------------------------------------

#: R01B relational-provider concern: engine / session / pool / PRAGMA /
#: connection cleanup. Preserved by R01B; NOT removed by R01C.
R01B_PROVIDER_FUNCTIONS: frozenset[str] = frozenset({
    "configure_database_runtime",
    "create_database",
    "get_engine",
    "get_session_factory",
    "is_database_runtime_configured",
    "reset_database_runtime_for_tests",
    "close_db",
    "cancel_safe_session",
    "get_pool_status",
    "_consume_cleanup_exception",
    "_await_cleanup",
    "_quiet_cleanup",
    "get_db_session",
    "get_db",
})

#: R01C schema-lifecycle concern left in core: mandatory delegation only.
R01C_LIFECYCLE_FUNCTIONS: frozenset[str] = frozenset({"init_db"})

#: Migration names remain R01C lifecycle by classification, but concrete
#: ``_migrate_*`` implementations must live in edition adapters.
R01C_MIGRATION_PREFIX = "_migrate_"

#: Key module state globals split across the same boundary (``Base`` is the ORM
#: declarative base retained by core; the engine/session singletons are the
#: provider, R01B). Other module constants are provider-internal and out of the
#: classified surface.
R01B_PROVIDER_STATE: frozenset[str] = frozenset({"_engine", "_session_factory"})
R01C_LIFECYCLE_STATE: frozenset[str] = frozenset({"Base"})

#: Preserved injected-runtime API (ac_af454ee3). ``close_db`` /
#: ``get_session_factory`` are the concrete symbols the AC references as
#: ``close_database`` / ``session_factory``.
PRESERVED_STARTUP_API: tuple[str, ...] = (
    "configure_database_runtime", "get_engine", "get_session_factory", "close_db",
)

FORBIDDEN_CORE_RUNTIME_TOKENS: tuple[str, ...] = (
    "create_async_engine",
    "async_sessionmaker",
    "event.listens_for",
)


def _database_path() -> Path:
    return Path(__file__).resolve().parent / "database.py"


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


def startup_parity_errors(
    database_path: str | Path | None = None,
    runtime_registry_path: str | Path | None = None,
) -> list[str]:
    """Return startup-boundary violations (empty == boundary preserved).

    The ``runtime_registry_path`` argument is accepted for backwards-compatible
    tests but no longer participates in the oracle: concrete pool/PRAGMA policy
    lives in edition adapters.
    """
    db_path = Path(database_path) if database_path is not None else _database_path()
    errors: list[str] = []
    source = db_path.read_text(encoding="utf-8")
    for token in FORBIDDEN_CORE_RUNTIME_TOKENS:
        if token in source:
            errors.append(f"core relational runtime token present: {token}")

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
    relational_runtime_factory_registered: bool
    reason: str

    def as_dict(self) -> dict:
        return {
            "allowed": self.allowed,
            "uow_factory_registered": self.uow_factory_registered,
            "relational_runtime_factory_registered": self.relational_runtime_factory_registered,
            "reason": self.reason,
        }


def r01c_lifecycle_removal_readiness() -> R01CRemovalReadiness:
    """TR4 ordering invariant: the core may retire its schema lifecycle ONLY after
    R01B registers the Community relational provider (UnitOfWorkFactory seam) and
    relational runtime factory. FAIL-CLOSED — unregistered means removal is blocked.
    """
    uow = is_unit_of_work_factory_registered()
    runtime = is_relational_runtime_factory_registered()
    allowed = uow and runtime
    if allowed:
        reason = (
            "R01B relational provider registered (UnitOfWorkFactory + runtime "
            "installer): R01C may move schema lifecycle to Community."
        )
    else:
        missing = []
        if not uow:
            missing.append("UnitOfWorkFactory")
        if not runtime:
            missing.append("relational runtime factory")
        reason = (
            "R01B relational provider NOT fully registered (missing: "
            f"{', '.join(missing)}); core retains schema lifecycle (TR4 — "
            "register-before-remove)."
        )
    return R01CRemovalReadiness(
        allowed=allowed,
        uow_factory_registered=uow,
        relational_runtime_factory_registered=runtime,
        reason=reason,
    )
