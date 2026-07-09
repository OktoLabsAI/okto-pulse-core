from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from okto_pulse.core.infra import database as private_database
from okto_pulse.core.ports import (
    Base,
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


class _Url:
    def __init__(self, backend: str, database: str | None) -> None:
        self._backend = backend
        self.database = database

    def get_backend_name(self) -> str:
        return self._backend


class _Engine:
    def __init__(self, url: Any) -> None:
        self.url = url


@pytest.fixture(autouse=True)
def _reset_runtime():
    previous_engine = get_engine()
    previous_session_factory = get_session_factory()
    reset_database_runtime_for_tests()
    try:
        yield
    finally:
        reset_database_runtime_for_tests()
        configure_database_runtime(
            engine=previous_engine,
            session_factory=previous_session_factory,
        )


def test_public_facade_preserves_injected_runtime() -> None:
    engine = _Engine(_Url("sqlite", "pulse.db"))

    def session_factory() -> object:
        return object()

    assert Base is private_database.Base
    assert is_database_runtime_configured() is False

    configure_database_runtime(engine=engine, session_factory=session_factory)

    assert is_database_runtime_configured() is True
    assert get_engine() is engine
    assert get_session_factory() is session_factory

    reset_database_runtime_for_tests()

    assert is_database_runtime_configured() is False


def test_public_lifecycle_facade_delegates_to_private_runtime(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_init_db() -> None:
        calls.append("init")

    async def fake_close_db() -> None:
        calls.append("close")

    monkeypatch.setattr(private_database, "init_db", fake_init_db)
    monkeypatch.setattr(private_database, "close_db", fake_close_db)

    asyncio.run(init_db())
    asyncio.run(close_db())

    assert calls == ["init", "close"]


def test_resolve_sqlite_database_path_returns_file_path(tmp_path: Path) -> None:
    db_path = tmp_path / "pulse.db"
    configure_database_runtime(
        engine=_Engine(_Url("sqlite", str(db_path))),
        session_factory=lambda: object(),
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
        engine=_Engine(_Url(backend, database)),
        session_factory=lambda: object(),
    )

    with pytest.raises(RelationalDatabasePathUnavailable):
        resolve_sqlite_database_path()
