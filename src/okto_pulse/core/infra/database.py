"""Compatibility facade for the public relational runtime port.

New code should import :mod:`okto_pulse.core.ports.relational_runtime`.  This
module intentionally contains no engine, session, pool or transaction logic.
"""

from __future__ import annotations

from okto_pulse.core.ports.relational_runtime import (
    RelationalDatabasePathUnavailable,
    RelationalRuntime,
    cancel_safe_session,
    cancel_safe_session_scope,
    close_db,
    configure_database_runtime,
    get_db,
    get_db_session,
    get_engine,
    get_pool_status,
    get_session_factory,
    init_db,
    is_database_runtime_configured,
    reset_database_runtime_for_tests,
    resolve_database_runtime,
    resolve_sqlite_database_path,
)


def create_database(url: str, *, echo: bool = False) -> None:
    """Build a runtime through an explicitly registered edition/test factory."""

    from okto_pulse.core.runtime_registry import resolve_relational_runtime_factory

    runtime = resolve_relational_runtime_factory()(url, echo=echo)
    configure_database_runtime(runtime=runtime)


__all__ = [
    "RelationalDatabasePathUnavailable",
    "RelationalRuntime",
    "cancel_safe_session",
    "cancel_safe_session_scope",
    "close_db",
    "configure_database_runtime",
    "create_database",
    "get_db",
    "get_db_session",
    "get_engine",
    "get_pool_status",
    "get_session_factory",
    "init_db",
    "is_database_runtime_configured",
    "reset_database_runtime_for_tests",
    "resolve_database_runtime",
    "resolve_sqlite_database_path",
]
