"""Relational runtime port owned by the application core.

Concrete engine, session factory, pool and lifecycle behavior belongs to an
edition adapter. Core requires only an opaque transactional work scope. Legacy
facades remain callable for installed integrations, but they are not members of
the port and a new edition does not have to implement them.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncContextManager, Protocol, runtime_checkable

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)

_RELATIONAL_RUNTIME_KEY = "ports.relational_runtime"


class RelationalDatabasePathUnavailable(RuntimeError):
    """Raised when the active runtime has no local database file path."""


@runtime_checkable
class RelationalRuntime(Protocol):
    """Edition-owned transactional work-scope behavior."""

    def transactional_session(self) -> AsyncContextManager[Any]:
        """Return a commit/rollback/close managed session scope."""



def configure_database_runtime(*, runtime: RelationalRuntime) -> None:
    """Register the edition-owned relational runtime."""

    if runtime is None:
        raise ValueError("runtime is required")
    register_runtime_value(_RELATIONAL_RUNTIME_KEY, runtime)


def reset_database_runtime_for_tests() -> None:
    """Drop the registered runtime for isolated tests."""

    reset_runtime_values(_RELATIONAL_RUNTIME_KEY)


def is_database_runtime_configured() -> bool:
    """Return whether an edition relational runtime is registered."""

    return resolve_runtime_value(_RELATIONAL_RUNTIME_KEY) is not None


def resolve_database_runtime() -> RelationalRuntime:
    """Resolve the active runtime, failing closed before edition composition."""

    runtime = resolve_runtime_value(_RELATIONAL_RUNTIME_KEY)
    if runtime is None:
        raise RuntimeError(
            "Database runtime not initialised. The edition composition root must "
            "call configure_database_runtime(runtime=...) first."
        )
    return runtime


def get_engine() -> Any:
    """Return the opaque engine handle for compatibility consumers."""

    engine = getattr(resolve_database_runtime(), "engine", None)
    if engine is None:
        raise RuntimeError("active edition does not expose a native engine")
    return engine


def get_session_factory() -> Callable[[], Any]:
    """Return the opaque session factory for compatibility consumers."""

    factory = getattr(resolve_database_runtime(), "session_factory", None)
    if factory is None:
        raise RuntimeError("active edition does not expose a session factory")
    return factory


async def init_db() -> None:
    """Initialize tables through the registered schema lifecycle port."""

    from okto_pulse.core.ports.schema_lifecycle import (
        resolve_relational_schema_lifecycle_orchestrator,
    )

    orchestrator = resolve_relational_schema_lifecycle_orchestrator()
    if orchestrator is None:
        raise RuntimeError(
            "Relational schema lifecycle orchestrator not registered. The edition "
            "composition root must register one before calling init_db()."
        )
    await orchestrator.initialize_schema()


async def close_db() -> None:
    """Close the registered edition runtime."""

    close = getattr(resolve_database_runtime(), "close", None)
    if not callable(close):
        raise RuntimeError("active edition does not expose database lifecycle")
    await close()


def get_pool_status() -> str:
    """Return pool diagnostics without exposing backend operations in Core."""

    status = getattr(resolve_database_runtime(), "pool_status", None)
    if not callable(status):
        raise RuntimeError("active edition does not expose pool diagnostics")
    return str(status())


@asynccontextmanager
async def cancel_safe_session_scope(
    session_factory: Callable[[], Any],
) -> AsyncIterator[Any]:
    """Delegate cancellation-safe cleanup to the edition adapter."""

    scope = getattr(resolve_database_runtime(), "cancel_safe_session_scope", None)
    if not callable(scope):
        raise RuntimeError("active edition does not expose cancellation-safe sessions")
    async with scope(session_factory) as session:
        yield session


@asynccontextmanager
async def cancel_safe_session() -> AsyncIterator[Any]:
    """Open an edition session whose cleanup survives cancellation."""

    scope = getattr(resolve_database_runtime(), "cancel_safe_session_scope", None)
    if not callable(scope):
        raise RuntimeError("active edition does not expose cancellation-safe sessions")
    async with scope() as session:
        yield session


@asynccontextmanager
async def get_db_session() -> AsyncIterator[Any]:
    """Open a transactional session using edition-owned lifecycle semantics."""

    async with resolve_database_runtime().transactional_session() as session:
        yield session


async def get_db() -> AsyncGenerator[Any, None]:
    """FastAPI-compatible dependency backed by the runtime port."""

    async with resolve_database_runtime().transactional_session() as session:
        yield session


def resolve_sqlite_database_path() -> Path:
    """Return the active local database file path, failing closed otherwise."""

    try:
        resolver = getattr(resolve_database_runtime(), "local_database_path", None)
        if not callable(resolver):
            raise RelationalDatabasePathUnavailable(
                "database runtime has no local database file path"
            )
        path = resolver()
    except Exception as exc:
        raise RelationalDatabasePathUnavailable(
            "database runtime has no local database file path"
        ) from exc
    if path is None:
        raise RelationalDatabasePathUnavailable(
            "database runtime has no local database file path"
        )
    return Path(path)


__all__ = [
    "RelationalDatabasePathUnavailable",
    "RelationalRuntime",
    "cancel_safe_session",
    "cancel_safe_session_scope",
    "close_db",
    "configure_database_runtime",
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
