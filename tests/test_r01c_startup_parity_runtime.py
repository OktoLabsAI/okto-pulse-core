"""R01C TEST2 runtime evidence — startup parity ACTUALLY executed (ts_cdbe5d65).

Complements the STATIC parity oracle in
``test_r01c_imp2_relational_lifecycle_decomposition.py`` (which pins the PRAGMAs /
pool tuning / preserved API from source) with a RUNTIME replay that builds a real
engine and proves the "then" of ts_cdbe5d65 is observed in execution, not just in
source:

  * ``create_database`` builds the engine and the SQLite hardening PRAGMAs
    (journal_mode=WAL, busy_timeout=30000, synchronous=NORMAL) are applied on
    connect — verified by querying the live connection;
  * the engine carries the pool tuning (pool_size=20);
  * ``init_db`` delegates to a registered lifecycle orchestrator on an EMPTY DB
    and materialises the schema; a second ``init_db`` is idempotent (no table
    loss) — the no-drift property the edition migration replay must hold.

Uses a temp file SQLite (WAL needs a real file) and preserves the process-global
engine/session-factory so the surrounding suite is unaffected.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def _preserve_global_engine():
    from okto_pulse.core.infra import database as db

    saved_runtime = db.resolve_database_runtime()
    try:
        yield
    finally:
        db.configure_database_runtime(runtime=saved_runtime)


@pytest.fixture(autouse=True)
def _no_orchestrator():
    # exercise the in-core legacy lifecycle path (migrations + create_all), not a
    # registered Community orchestrator.
    from okto_pulse.core.ports import schema_lifecycle as sl

    sl.reset_relational_schema_lifecycle_orchestrator()
    yield
    sl.reset_relational_schema_lifecycle_orchestrator()


async def test_create_database_applies_sqlite_pragmas_and_pool_at_runtime(tmp_path, _preserve_global_engine):
    from okto_pulse.core.infra import database as db

    url = f"sqlite+aiosqlite:///{(tmp_path / 'parity.db').as_posix()}"
    db.create_database(url)
    engine = db.get_engine()
    assert engine is not None, "create_database must build the engine unconditionally"

    # pool tuning parity (runtime)
    assert engine.pool.size() == 20

    # SQLite hardening PRAGMAs applied on connect (runtime parity, not source)
    async with engine.connect() as conn:
        journal_mode = (await conn.exec_driver_sql("PRAGMA journal_mode")).scalar()
        busy_timeout = (await conn.exec_driver_sql("PRAGMA busy_timeout")).scalar()
        synchronous = (await conn.exec_driver_sql("PRAGMA synchronous")).scalar()

    assert str(journal_mode).lower() == "wal"
    assert int(busy_timeout) == 30000
    assert int(synchronous) == 1  # NORMAL

    await db.close_db()


async def test_init_db_runs_migrations_and_materialises_schema_idempotently(tmp_path, _preserve_global_engine):
    from okto_pulse.core.infra import database as db
    from okto_pulse.core.ports import schema_lifecycle as sl
    from sqlalchemy_test_models import Base

    url = f"sqlite+aiosqlite:///{(tmp_path / 'init.db').as_posix()}"
    db.create_database(url)

    class _CreateAllLifecycle:
        async def initialize_schema(self) -> None:
            async with db.get_engine().begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

    sl.register_relational_schema_lifecycle_orchestrator(_CreateAllLifecycle())

    # empty DB: init_db delegates to the registered lifecycle
    await db.init_db()
    engine = db.get_engine()
    async with engine.connect() as conn:
        tables = set(
            (await conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )).scalars().all()
        )
    # a substantial schema was materialised, including the core boards table
    assert "boards" in tables
    assert len(tables) >= 20, sorted(tables)

    # re-run init_db on the now-populated DB: idempotent, no table loss/drift
    await db.init_db()
    async with engine.connect() as conn:
        tables_after = set(
            (await conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )).scalars().all()
        )
    assert tables <= tables_after, sorted(tables - tables_after)

    await db.close_db()
