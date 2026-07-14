from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from okto_pulse.core.ports import (
    RelationalDatabasePathUnavailable,
    close_db,
    configure_database_runtime,
    get_engine,
    get_session_factory,
    init_db,
    is_database_runtime_configured,
    reset_database_runtime_for_tests,
    resolve_sqlite_database_path,
)
from okto_pulse.core.ports.relational_runtime import resolve_database_runtime


class _Url:
    def __init__(self, backend: str, database: str | None) -> None:
        self._backend = backend
        self.database = database

    def get_backend_name(self) -> str:
        return self._backend


class _Engine:
    def __init__(self, url: Any) -> None:
        self.url = url


class _Runtime:
    def __init__(self, engine: Any, session_factory: Any) -> None:
        self.engine = engine
        self.session_factory = session_factory
        self.calls: list[str] = []

    @asynccontextmanager
    async def transactional_session(self):
        yield self.session_factory()

    @asynccontextmanager
    async def cancel_safe_session_scope(self, session_factory=None):
        yield (session_factory or self.session_factory)()

    async def close(self) -> None:
        self.calls.append("close")

    def pool_status(self) -> str:
        return "test-pool"

    def local_database_path(self) -> Path | None:
        url = self.engine.url
        if url.get_backend_name() != "sqlite":
            return None
        if not url.database or url.database == ":memory:":
            return None
        return Path(url.database)


@pytest.fixture(autouse=True)
def _reset_runtime():
    previous_runtime = resolve_database_runtime()
    reset_database_runtime_for_tests()
    try:
        yield
    finally:
        reset_database_runtime_for_tests()
        configure_database_runtime(runtime=previous_runtime)


def test_public_facade_preserves_injected_runtime() -> None:
    engine = _Engine(_Url("sqlite", "pulse.db"))

    def session_factory() -> object:
        return object()

    assert is_database_runtime_configured() is False

    configure_database_runtime(runtime=_Runtime(engine, session_factory))

    assert is_database_runtime_configured() is True
    assert get_engine() is engine
    assert get_session_factory() is session_factory

    reset_database_runtime_for_tests()

    assert is_database_runtime_configured() is False


def test_public_close_facade_delegates_to_registered_runtime() -> None:
    calls: list[str] = []
    runtime = _Runtime(_Engine(_Url("sqlite", "pulse.db")), lambda: object())
    runtime.calls = calls
    configure_database_runtime(runtime=runtime)
    asyncio.run(close_db())

    assert calls == ["close"]


def test_resolve_sqlite_database_path_returns_file_path(tmp_path: Path) -> None:
    db_path = tmp_path / "pulse.db"
    configure_database_runtime(
        runtime=_Runtime(
            _Engine(_Url("sqlite", str(db_path))),
            lambda: object(),
        )
    )

    assert resolve_sqlite_database_path() == db_path


def test_resolve_sqlite_database_path_fails_without_runtime() -> None:
    with pytest.raises(RelationalDatabasePathUnavailable):
        resolve_sqlite_database_path()


@pytest.mark.parametrize(
    ("backend", "database"),
    [
        ("postgresql", "pulse"),
        ("sqlite", ":memory:"),
        ("sqlite", ""),
        ("sqlite", None),
    ],
    ids=["postgresql", "sqlite-memory", "sqlite-empty", "sqlite-none"],
)
def test_resolve_sqlite_database_path_fails_closed_for_non_file_urls(
    backend: str,
    database: str | None,
) -> None:
    configure_database_runtime(
        runtime=_Runtime(
            _Engine(_Url(backend, database)),
            lambda: object(),
        )
    )

    with pytest.raises(RelationalDatabasePathUnavailable):
        resolve_sqlite_database_path()
