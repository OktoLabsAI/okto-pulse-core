from __future__ import annotations

import asyncio
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.workers import consolidation
from okto_pulse.core.kg.workers.consolidation import ConsolidationWorker


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
    entry_a = SimpleNamespace(board_id="board-1")
    entry_b = SimpleNamespace(board_id="board-1")

    await asyncio.gather(
        consolidation._process_queue_entry_serialized(None, entry_a),
        consolidation._process_queue_entry_serialized(None, entry_b),
    )

    assert max_active == 1


def test_app_lifespan_starts_and_stops_consolidation_worker() -> None:
    app_source = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "app.py"
    ).read_text(encoding="utf-8")

    assert "get_consolidation_worker" in app_source
    assert "await consolidation_worker.start()" in app_source
    assert "await consolidation_worker.stop()" in app_source
