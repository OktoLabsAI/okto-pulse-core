from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.workers import consolidation
from okto_pulse.core.kg.workers.consolidation import ConsolidationWorker
from okto_pulse.core.ports.coordination import (
    register_coordination_providers,
    reset_coordination_providers_for_tests,
)

from tests.coordination_fakes import FakeWriteLockPort


class _RecordingWorker(ConsolidationWorker):
    def __init__(self) -> None:
        super().__init__(
            session_factory=lambda: None,
            heartbeat_seconds=30,
            batch_size=1,
        )
        self.calls: list[float] = []

    async def process_batch(self) -> int:
        self.calls.append(time.monotonic())
        if len(self.calls) <= 2:
            return 1
        raise asyncio.CancelledError


@pytest.mark.asyncio
async def test_worker_drains_next_batch_immediately_after_progress() -> None:
    worker = _RecordingWorker()
    worker._running = True
    worker._wake_event = asyncio.Event()

    await asyncio.wait_for(worker._run_loop(), timeout=1.0)

    assert len(worker.calls) == 3
    assert worker.calls[1] - worker.calls[0] < 0.1


@pytest.mark.asyncio
async def test_queue_entry_processing_is_serialized_per_board(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active = 0
    max_active = 0

    async def fake_process(_db, _entry) -> bool:
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

    from okto_pulse.core import app as app_mod
    from okto_pulse.core.infra import auth as auth_mod
    from okto_pulse.core.infra import database as database_mod
    from okto_pulse.core.infra import storage as storage_mod
    from okto_pulse.core.infra.config import configure_settings, get_settings
    from okto_pulse.core.ports.runtime_workers import (
        RuntimeWorkerRegistry,
        RuntimeWorkerSpec,
    )

    events: list[str] = []

    class _Engine:
        async def dispose(self) -> None:
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
    original_auth = auth_mod._auth_provider
    original_storage = storage_mod._storage_provider
    original_engine = database_mod._engine
    original_session_factory = database_mod._session_factory

    monkeypatch.setenv("KG_DAILY_TICK_DISABLED", "1")
    database_mod.configure_database_runtime(
        engine=_Engine(),
        session_factory=lambda: None,
    )

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
        auth_mod._auth_provider = original_auth
        storage_mod._storage_provider = original_storage
        if original_engine is None or original_session_factory is None:
            database_mod.reset_database_runtime_for_tests()
        else:
            database_mod.configure_database_runtime(
                engine=original_engine,
                session_factory=original_session_factory,
            )
