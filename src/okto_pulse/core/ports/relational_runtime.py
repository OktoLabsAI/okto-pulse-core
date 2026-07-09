"""Public relational runtime facade for edition-owned SQLAlchemy adapters.

The concrete engine/session construction remains outside core. This module only
delegates to the transitional runtime registry already owned by
``core.infra.database`` and exposes a narrow public surface for edition adapters.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

_DATABASE_MODULE = "okto_pulse.core.infra.database"
_BASE_EXPORT = "Base"


class RelationalDatabasePathUnavailable(RuntimeError):
    """Raised when the active relational runtime has no local SQLite file path."""


def _database_module() -> Any:
    return importlib.import_module(_DATABASE_MODULE)


def __getattr__(name: str) -> Any:
    if name != _BASE_EXPORT:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(_database_module(), name)
    globals()[name] = value
    return value


def configure_database_runtime(*, engine: Any, session_factory: Any) -> None:
    """Register the edition-owned relational engine and session factory."""

    _database_module().configure_database_runtime(
        engine=engine,
        session_factory=session_factory,
    )


def reset_database_runtime_for_tests() -> None:
    """Reset registered relational runtime handles for isolated tests."""

    _database_module().reset_database_runtime_for_tests()


def is_database_runtime_configured() -> bool:
    """Return whether both engine and session factory are registered."""

    return bool(_database_module().is_database_runtime_configured())


def get_engine() -> Any:
    """Return the registered relational engine."""

    return _database_module().get_engine()


def get_session_factory() -> Any:
    """Return the registered relational session factory."""

    return _database_module().get_session_factory()


async def init_db() -> None:
    """Initialize the registered relational schema lifecycle."""

    await _database_module().init_db()


async def close_db() -> None:
    """Close the registered relational runtime."""

    await _database_module().close_db()


def resolve_sqlite_database_path() -> Path:
    """Return the local SQLite file path for the registered runtime.

    The resolver is intentionally fail-closed: absent runtime, non-SQLite URLs,
    in-memory SQLite URLs and URLs without a file database all raise
    :class:`RelationalDatabasePathUnavailable`.
    """

    try:
        url = get_engine().url
    except Exception as exc:
        raise RelationalDatabasePathUnavailable(
            "database runtime is not configured"
        ) from exc

    backend = _backend_name(url)
    if backend != "sqlite":
        raise RelationalDatabasePathUnavailable(
            f"database runtime backend {backend!r} is not local SQLite"
        )

    database = _database_name(url)
    if not database or database in {":memory:", "file::memory:"}:
        raise RelationalDatabasePathUnavailable(
            "database runtime SQLite URL does not point to a file"
        )

    return Path(database)


def _backend_name(url: Any) -> str:
    get_backend_name = getattr(url, "get_backend_name", None)
    if callable(get_backend_name):
        return str(get_backend_name())
    raw = str(url)
    return raw.split(":", 1)[0].split("+", 1)[0]


def _database_name(url: Any) -> str:
    database = getattr(url, "database", None)
    if database is not None:
        return str(database)

    raw = str(url)
    marker = ":///"
    idx = raw.rfind(marker)
    if idx < 0:
        return ""
    return raw[idx + len(marker) :].split("?", 1)[0]


__all__ = [
    "Base",
    "RelationalDatabasePathUnavailable",
    "close_db",
    "configure_database_runtime",
    "get_engine",
    "get_session_factory",
    "init_db",
    "is_database_runtime_configured",
    "reset_database_runtime_for_tests",
    "resolve_sqlite_database_path",
]
