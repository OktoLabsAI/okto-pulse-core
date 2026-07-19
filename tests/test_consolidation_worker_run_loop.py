from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.ports.coordination import (
    register_coordination_providers,
    reset_coordination_providers_for_tests,
)

from coordination_fakes import FakeWriteLockPort


@pytest.mark.asyncio
async def test_queue_entry_processing_is_serialized_per_board(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active = 0
    max_active = 0

    async def fake_process(_db, _entry, **_kwargs) -> bool:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.02)
        active -= 1
        return True

    monkeypatch.setattr(consolidation, "_process_queue_entry", fake_process)
    register_coordination_providers(write_lock_port=FakeWriteLockPort())
    entry_a = SimpleNamespace(board_id="board-1")
    entry_b = SimpleNamespace(board_id="board-1")

    try:
        await asyncio.gather(
            consolidation._process_queue_entry_serialized(None, entry_a),
            consolidation._process_queue_entry_serialized(None, entry_b),
        )
    finally:
        reset_coordination_providers_for_tests()

    assert max_active == 1


# Since R08C core.app no longer constructs workers itself: edition composition
# roots inject a RuntimeWorkerRegistry and the default lifespan drives
# start_all()/stop_all(). The old source-inspection assert on
# get_consolidation_worker went stale; this behavioral twin proves the same
# contract (lifespan starts and stops the consolidation worker) through the
# injected registry. Database/scheduler side effects of the default lifespan
# are neutralized the same way the #03/R08B shell tests do, without touching
# the worker-registry wire under test.
def test_app_lifespan_starts_and_stops_consolidation_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient

    from okto_pulse.community import app as app_mod
    from okto_pulse.core.infra import auth as auth_mod
    from okto_pulse.core.infra import database as database_mod
    from okto_pulse.core.infra import storage as storage_mod
    from okto_pulse.core.infra.config import configure_settings, get_settings
    from okto_pulse.core.ports.runtime_workers import (
        RuntimeWorkerRegistry,
        RuntimeWorkerSpec,
    )

    events: list[str] = []

    class _Runtime:
        engine = object()
        session_factory = staticmethod(lambda: None)

        @asynccontextmanager
        async def transactional_session(self):
            yield None

        @asynccontextmanager
        async def cancel_safe_session_scope(self, session_factory=None):
            yield (session_factory or self.session_factory)()

        async def close(self) -> None:
            return None

    class _Handle:
        async def stop(self) -> None:
            events.append("stop:consolidation_worker")

    async def _start() -> _Handle:
        events.append("start:consolidation_worker")
        return _Handle()

    registry = RuntimeWorkerRegistry(
        (
            RuntimeWorkerSpec(
                family="consolidation_worker",
                start=_start,
                stop=lambda handle: handle.stop(),
            ),
        )
    )

    original_settings = get_settings()
    try:
        original_auth = auth_mod.get_auth_provider()
    except RuntimeError:
        original_auth = None
    try:
        original_storage = storage_mod.get_storage_provider()
    except RuntimeError:
        original_storage = None
    original_runtime = database_mod.resolve_database_runtime()

    monkeypatch.setenv("KG_DAILY_TICK_DISABLED", "1")
    database_mod.configure_database_runtime(runtime=_Runtime())

    async def _noop_init_db() -> None:
        return None

    async def _noop_shutdown(close_db, logger=None) -> None:
        return None

    monkeypatch.setattr(app_mod, "init_db", _noop_init_db)
    monkeypatch.setattr(app_mod, "shutdown_kg_then_db", _noop_shutdown)

    settings = SimpleNamespace(
        app_name="Worker Lifespan Smoke",
        app_version="test",
        database_url="sqlite+aiosqlite:///:memory:",
        debug=False,
    )
    try:
        app = app_mod.create_app(
            settings,
            object(),
            object(),
            runtime_worker_registry=registry,
        )
        with TestClient(app) as client:
            assert client.get("/health").status_code == 200
            assert registry.active_families == ("consolidation_worker",)
            assert registry.start_count("consolidation_worker") == 1
            assert events == ["start:consolidation_worker"]
        assert registry.active_families == ()
        assert events == [
            "start:consolidation_worker",
            "stop:consolidation_worker",
        ]
    finally:
        configure_settings(original_settings)
        if original_auth is None:
            auth_mod.reset_auth_for_tests()
        else:
            auth_mod.configure_auth(original_auth)
        if original_storage is None:
            storage_mod.reset_storage_provider_for_tests()
        else:
            storage_mod.configure_storage(original_storage)
        database_mod.configure_database_runtime(runtime=original_runtime)
