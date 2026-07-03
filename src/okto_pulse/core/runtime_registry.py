"""Process-level registry for the edition-owned relational UnitOfWorkFactory
(SaaS Refactor R01B REPLAN-IMP2, FR3 / TR4).

The inbound consumers — REST ``api/deps.get_unit_of_work`` and MCP
``mcp/server.get_unit_of_work_factory_for_mcp`` — must resolve the relational
UnitOfWork provider WITHOUT the core importing or constructing the concrete
SQLAlchemy adapter (TR4: the dependency direction is core-contracts ->
Community-adapters, never the reverse). The edition composition root registers
ITS concrete factory here at boot — the SAME object it also exposes on
``app.state.runtime_composition.uow_factory`` — so the core resolves the edition
provider through this seam.

Register-before-remove, FAIL-CLOSED: this is an EXPLICIT, RESETTABLE registration
(``register_unit_of_work_factory`` / ``reset_unit_of_work_factory``). There is NO
default that constructs a core ``SQLAlchemyUnitOfWorkFactory`` — a consumer that
resolves before any edition registered gets a structured ``RuntimeError`` (never
an implicit core concrete in production). The test harness (``tests/conftest``)
may register a CORE-ONLY provider as EXPLICIT test/transitional-compat wiring;
that is test wiring, NOT a runtime fallback.

This module global is ledgered in
``application/boundary/singleton_gate`` (``SINGLETON_LEDGER`` +
``BASELINE_SINGLETONS``) so the ``AntiSingletonGate`` keeps it register-before-
remove (owner ``okto-pulse-core/runtime``, target provider ``uow_factory``,
retirement = R01C when the core relational concretes are physically removed).

SQLite PRAGMA single-owner seam (R01B REPLAN-IMP2, TR5)
-------------------------------------------------------
Production runs SQLite under two connect listeners today — the core
``create_database`` (WAL + busy_timeout=30000 + synchronous=NORMAL) and
``community/main.py:_configure_sqlite_pragmas`` (WAL + foreign_keys=ON) — so the
hardening is split across two partial points. ``register_sqlite_pragma_installer``
makes the edition the SINGLE owner of the effective PRAGMA listener: the Community
composition root registers its UNION installer (``install_community_sqlite_pragmas``
= WAL + busy_timeout + synchronous + foreign_keys) BEFORE ``create_database`` runs,
so the core attaches exactly ONE listener — the edition's.

Unlike the UoW seam this is NOT fail-closed: PRAGMAs are a data-integrity concern,
so when nothing is registered (core-standalone / the test suite that calls
``create_database`` directly) ``resolve_sqlite_pragma_installer`` returns the
EXPLICIT core-default fallback ``install_core_default_sqlite_pragmas`` — the three
HISTORICAL core PRAGMAs (WAL + busy_timeout + synchronous, NO foreign_keys), byte
-for-byte the prior inline behavior. This fallback is core-only/transitional, NOT
the Community runtime path (the edition always registers its UNION installer). The
``_sqlite_pragma_installer`` global is ledgered in ``singleton_gate`` the same
register-before-remove way (owner ``okto-pulse-core/runtime``, target provider
``sqlite_pragma_installer``, retirement = R01C when the engine + PRAGMA ownership
physically moves to Community).
"""

from __future__ import annotations

from typing import Any

#: The edition-registered relational UnitOfWorkFactory (None until a composition
#: root — or the test harness — registers one). NO implicit default.
_unit_of_work_factory: Any = None
_content_ingestion_resolver: Any = None


def register_unit_of_work_factory(factory: Any) -> None:
    """Register the edition-owned relational ``UnitOfWorkFactory`` (R01B FR3).

    Called once by the edition composition root (Community ``main.py``) with the
    same object it puts on ``app.state.runtime_composition.uow_factory``. Last
    writer wins so a re-composed app / test can re-point the seam. ``factory`` is
    duck-typed: it must expose ``wrap(session)`` (request-scoped REST path) and be
    callable for the MCP / worker fresh-session path."""
    global _unit_of_work_factory
    _unit_of_work_factory = factory


def reset_unit_of_work_factory() -> None:
    """Drop the registered factory (test isolation / teardown). After a reset a
    resolve fails closed until something registers again — there is no fallback."""
    global _unit_of_work_factory
    _unit_of_work_factory = None


def is_unit_of_work_factory_registered() -> bool:
    """Whether an edition provider is currently registered on the seam."""
    return _unit_of_work_factory is not None


def resolve_unit_of_work_factory(*, preferred: Any = None) -> Any:
    """Return the relational ``UnitOfWorkFactory``, FAIL-CLOSED.

    ``preferred`` lets the REST request path pass the per-app composition provider
    (``request.app.state.runtime_composition.uow_factory``) so a composed app wins
    over the process-global seam. When neither ``preferred`` nor the registered
    seam is available, raise — there is NO implicit fallback to a core concrete
    (R01B FR3 / AC4 fail-closed)."""
    factory = preferred if preferred is not None else _unit_of_work_factory
    if factory is None:
        raise RuntimeError(
            "No relational UnitOfWorkFactory provider is registered (R01B FR3 "
            "fail-closed). The edition composition root must register one via "
            "okto_pulse.core.runtime_registry.register_unit_of_work_factory(); the "
            "core no longer constructs a SQLAlchemyUnitOfWorkFactory fallback."
        )
    return factory


# ---------------------------------------------------------------------------
# MCP content ingestion resolver seam (AF12)
# ---------------------------------------------------------------------------


def register_content_ingestion_resolver(resolver: Any) -> None:
    """Register the edition-owned resolver for MCP content references.

    The core MCP tools may accept direct inline payloads or an abstract
    ``content_reference``. Concrete local file/URL behavior is edition-owned, so
    Community registers a resolver here instead of the core importing pathlib or
    http clients in tool handlers.
    """
    global _content_ingestion_resolver
    _content_ingestion_resolver = resolver


def reset_content_ingestion_resolver() -> None:
    """Drop the registered content-ingestion resolver (test isolation)."""
    global _content_ingestion_resolver
    _content_ingestion_resolver = None


def resolve_content_ingestion_resolver(*, preferred: Any = None) -> Any | None:
    """Return the registered content resolver, if any.

    This seam is optional because inline MCP payloads are a complete core path.
    A missing resolver only fails calls that provide ``content_reference``.
    """
    return preferred if preferred is not None else _content_ingestion_resolver


# ---------------------------------------------------------------------------
# SQLite PRAGMA single-owner seam (R01B REPLAN-IMP2, TR5)
# ---------------------------------------------------------------------------

#: The edition-registered SQLite PRAGMA installer (None until a composition root
#: registers one). NOT fail-closed: when unregistered the core falls back to its
#: OWN historical installer (see ``install_core_default_sqlite_pragmas``) so test
#: / standalone boots keep identical SQLite semantics. The Community edition
#: registers the UNION installer (adds ``foreign_keys=ON``) so production runs
#: exactly ONE connect listener — the edition's. Ledgered in ``singleton_gate``.
_sqlite_pragma_installer: Any = None


def register_sqlite_pragma_installer(installer: Any) -> None:
    """Register the edition-owned SQLite PRAGMA installer (R01B TR5).

    Called once by the edition composition root (Community ``main.py`` / ``cli.py``)
    BEFORE ``create_database`` builds the engine, so the core attaches the
    edition's single connect listener — the UNION
    (``install_community_sqlite_pragmas``: WAL + busy_timeout=30000 +
    synchronous=NORMAL + foreign_keys=ON) — and nothing adds a second partial
    listener. ``installer`` is a ``Callable[[AsyncEngine], None]`` that attaches a
    ``connect`` listener (and no-ops for non-SQLite engines). Last writer wins."""
    global _sqlite_pragma_installer
    _sqlite_pragma_installer = installer


def reset_sqlite_pragma_installer() -> None:
    """Drop the registered installer (test isolation / teardown). After a reset,
    ``resolve_sqlite_pragma_installer`` returns the core-default fallback again."""
    global _sqlite_pragma_installer
    _sqlite_pragma_installer = None


def is_sqlite_pragma_installer_registered() -> bool:
    """Whether an edition PRAGMA installer is currently registered on the seam."""
    return _sqlite_pragma_installer is not None


def install_core_default_sqlite_pragmas(engine: Any) -> None:
    """Core-default FALLBACK PRAGMA installer (R01B TR5) — explicit, core-only.

    The three HISTORICAL core PRAGMAs (``journal_mode=WAL`` + ``busy_timeout=30000``
    + ``synchronous=NORMAL``, NO ``foreign_keys``), byte-for-byte the prior inline
    ``create_database`` listener. Used ONLY when no edition installer is registered
    (core-standalone / the test suite that calls ``create_database`` directly) — it
    is NOT the Community runtime path. Fires per pooled connection; no-op for
    non-SQLite engines."""
    from sqlalchemy import event

    if engine.url.get_backend_name() != "sqlite":
        return

    @event.listens_for(engine.sync_engine, "connect")
    def _install_core_default_pragmas(dbapi_conn, _conn_record):  # noqa: ANN001
        cursor = dbapi_conn.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.execute("PRAGMA synchronous=NORMAL")
        finally:
            cursor.close()


def resolve_sqlite_pragma_installer() -> Any:
    """Return the SQLite PRAGMA installer to run against a freshly-built engine.

    Returns the edition-registered installer when present (Community UNION: WAL +
    busy_timeout + synchronous + foreign_keys), otherwise the EXPLICIT core-default
    fallback ``install_core_default_sqlite_pragmas`` (WAL + busy_timeout +
    synchronous). Always returns a callable — PRAGMAs are a data-integrity concern,
    so unlike the UoW seam this resolves to a core-only fallback rather than failing
    closed. The result is a ``Callable[[AsyncEngine], None]`` that installs exactly
    one connect listener."""
    if _sqlite_pragma_installer is not None:
        return _sqlite_pragma_installer
    return install_core_default_sqlite_pragmas


__all__ = [
    "register_unit_of_work_factory",
    "reset_unit_of_work_factory",
    "is_unit_of_work_factory_registered",
    "resolve_unit_of_work_factory",
    "register_content_ingestion_resolver",
    "reset_content_ingestion_resolver",
    "resolve_content_ingestion_resolver",
    "register_sqlite_pragma_installer",
    "reset_sqlite_pragma_installer",
    "is_sqlite_pragma_installer_registered",
    "install_core_default_sqlite_pragmas",
    "resolve_sqlite_pragma_installer",
]
