"""Database configuration and session management."""

import asyncio
from collections.abc import Callable
import contextlib
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Awaitable

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import declarative_base

logger = logging.getLogger(__name__)

# Base class for models — always available at import time
Base = declarative_base()

# Module-level runtime handles injected by the edition composition root.
_engine = None
_session_factory = None


def configure_database_runtime(*, engine: Any, session_factory: Any) -> None:
    """Register the edition-owned relational runtime.

    Core owns the ORM schema and business lifecycle still present in this module,
    but the concrete engine/session construction belongs to an edition adapter.
    """
    global _engine, _session_factory
    if engine is None:
        raise ValueError("engine is required")
    if session_factory is None:
        raise ValueError("session_factory is required")
    _engine = engine
    _session_factory = session_factory


def reset_database_runtime_for_tests() -> None:
    """Drop registered runtime handles for isolated tests."""
    global _engine, _session_factory
    _engine = None
    _session_factory = None


def is_database_runtime_configured() -> bool:
    return _engine is not None and _session_factory is not None


def create_database(url: str, *, echo: bool = False) -> None:
    """Compatibility shim that delegates runtime creation to a registered adapter.

    Production edition composition should call ``configure_database_runtime``
    after building its concrete engine/session. Legacy tests and transitional
    tooling may still call this symbol, but the core no longer constructs the
    SQLAlchemy engine or session factory itself.
    """
    from okto_pulse.core.runtime_registry import resolve_relational_runtime_factory

    runtime = resolve_relational_runtime_factory()(url, echo=echo)
    if isinstance(runtime, tuple):
        engine, session_factory = runtime
    else:
        engine = getattr(runtime, "engine")
        session_factory = getattr(runtime, "session_factory")
    configure_database_runtime(engine=engine, session_factory=session_factory)


# ---------------------------------------------------------------------------
# Pool observability — leak detection (corruption/freeze root-cause hardening)
# ---------------------------------------------------------------------------
#
# Conexões "vazadas" (checked-out e nunca devolvidas) eram invisíveis até o
# GC emitir SAWarnings espalhados — quando o pool já estava exausto e o
# servidor parecia travado. Estes listeners mantêm o timestamp de checkout
# por _ConnectionRecord e logam um warning agregado quando algum checkout
# ultrapassa o threshold, ANTES da exaustão.

def get_pool_status() -> str:
    """Snapshot legível do pool (size/checked-out/overflow) para diagnóstico."""
    return get_engine().sync_engine.pool.status()


# ---------------------------------------------------------------------------
# Cancel-safe session — para endpoints de streaming (SSE/exports)
# ---------------------------------------------------------------------------
#
# Quando o cliente de um StreamingResponse desconecta, o servidor cancela a
# task da request com hard-cancel; o CancelledError aterrissa em QUALQUER
# await — inclusive no ``session.close()`` do ``async with``. O close
# interrompido nunca devolve a conexão ao pool (vazamento → exaustão →
# "travamento"). Este context manager roda o close numa task própria,
# referenciada em módulo (não morre com a request), e faz shield para que o
# fechamento SEMPRE complete mesmo com a request já cancelada.

_pending_session_closes: set[asyncio.Task] = set()


async def _cancel_safe_close(awaitable: Awaitable[None]) -> None:
    loop = asyncio.get_running_loop()
    close_task = loop.create_task(awaitable)
    _pending_session_closes.add(close_task)
    close_task.add_done_callback(_pending_session_closes.discard)
    try:
        await asyncio.shield(close_task)
    except asyncio.CancelledError:
        # O close continua em background na task referenciada acima.
        raise
    except Exception:
        logger.exception("db.session.cancel_safe_close_failed")


@asynccontextmanager
async def cancel_safe_session_scope(
    session_factory: Callable[[], Any],
) -> AsyncGenerator[Any, None]:
    """Session scope cujo fechamento sobrevive a hard-cancel.

    Preserva factories injetadas: aceita tanto uma factory que devolve um
    ``AsyncSession`` quanto uma que devolve um async context manager.
    """
    scope = session_factory()
    enter = getattr(scope, "__aenter__", None)
    exit_ = getattr(scope, "__aexit__", None)
    if callable(enter) and callable(exit_):
        session = await enter()
    else:
        session = scope
        exit_ = None
    exc_info: tuple[type[BaseException] | None, BaseException | None, Any] = (
        None,
        None,
        None,
    )
    try:
        yield session
    except BaseException as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        raise
    finally:
        if exit_ is not None:
            await _cancel_safe_close(exit_(*exc_info))
        else:
            await _cancel_safe_close(session.close())


@asynccontextmanager
async def cancel_safe_session() -> AsyncGenerator[AsyncSession, None]:
    """AsyncSession cujo fechamento sobrevive a um hard-cancel da request.

    Use em generators de streaming (SSE, exports) onde a desconexão do
    cliente cancela a task no meio de awaits. Fora de streaming, o
    ``async with session_factory() as s`` normal continua sendo o padrão.
    """
    async with cancel_safe_session_scope(get_session_factory()) as session:
        yield session


def get_engine():
    """Return the async engine (asserts it has been initialised)."""
    assert _engine is not None, (
        "Database runtime not initialised. The edition composition root must call "
        "configure_database_runtime() first."
    )
    return _engine


def get_session_factory():
    """Return the async session factory (asserts it has been initialised)."""
    assert _session_factory is not None, (
        "Database runtime not initialised. The edition composition root must call "
        "configure_database_runtime() first."
    )
    return _session_factory


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def init_db() -> None:
    """Initialize database tables through the registered edition lifecycle."""
    from okto_pulse.core.infra.schema_lifecycle import (
        resolve_relational_schema_lifecycle_orchestrator,
    )

    orchestrator = resolve_relational_schema_lifecycle_orchestrator()
    if orchestrator is None:
        raise RuntimeError(
            "Relational schema lifecycle orchestrator not registered. "
            "The edition composition root must register a schema lifecycle "
            "adapter before calling init_db()."
        )
    await orchestrator.initialize_schema()


async def close_db() -> None:
    """Close database connections."""
    await _await_cleanup(get_engine().dispose())


def _consume_cleanup_exception(task: asyncio.Future[None]) -> None:
    if task.cancelled():
        return
    with contextlib.suppress(BaseException):
        task.exception()


async def _await_cleanup(awaitable: Awaitable[None]) -> None:
    """Let connection cleanup continue even when request shutdown cancels the caller."""
    task = asyncio.ensure_future(awaitable)
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        task.add_done_callback(_consume_cleanup_exception)
        raise


async def _quiet_cleanup(awaitable: Awaitable[None]) -> None:
    with contextlib.suppress(BaseException):
        await _await_cleanup(awaitable)


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Get database session as async context manager."""
    session = get_session_factory()()
    try:
        yield session
        await session.commit()
    except BaseException:
        await _quiet_cleanup(session.rollback())
        raise
    finally:
        await _quiet_cleanup(session.close())


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for database session."""
    session = get_session_factory()()
    try:
        yield session
        await session.commit()
    except BaseException:
        await _quiet_cleanup(session.rollback())
        raise
    finally:
        await _quiet_cleanup(session.close())
