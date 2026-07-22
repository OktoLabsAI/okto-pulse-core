from __future__ import annotations

import asyncio
import threading

import pytest

from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.global_discovery import layer_parity
from okto_pulse.core.kg import stale_canonical_parity


class _ContextDroppingExecutor:
    """Witness adapter: run_in_executor does not copy contextvars itself."""

    async def run(self, operation):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, operation)

    async def join(self, timeout: float) -> int:
        del timeout
        return 0


@pytest.mark.asyncio
async def test_blocking_graph_io_runs_direct_reader_off_event_loop() -> None:
    event_loop_thread = threading.get_ident()

    worker_thread = await run_blocking_graph_io(
        threading.get_ident,
        task_name="test.kg.blocking_io.thread",
    )

    assert worker_thread != event_loop_thread


@pytest.mark.asyncio
async def test_core_bridge_preserves_write_context_with_unaware_executor() -> None:
    from okto_pulse.core.kg.write_barrier import (
        has_active_global_guard,
        under_global_safe_write,
    )

    with under_global_safe_write("context-test", "core_bridge"):
        active = await run_blocking_graph_io(
            has_active_global_guard,
            task_name="test.kg.blocking_io.context",
            blocking_execution=_ContextDroppingExecutor(),
        )

    assert active is True


@pytest.mark.asyncio
async def test_blocking_graph_io_drains_native_reader_after_repeated_cancel() -> None:
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def native_read() -> None:
        started.set()
        release.wait(timeout=5.0)
        finished.set()

    parent = asyncio.create_task(
        run_blocking_graph_io(
            native_read,
            task_name="test.kg.blocking_io.cancel",
        )
    )
    assert await asyncio.to_thread(started.wait, 1.0)

    parent.cancel()
    await asyncio.sleep(0)
    parent.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await parent
    assert finished.is_set()


@pytest.mark.asyncio
async def test_stale_parity_offloads_board_graph_reader(monkeypatch) -> None:
    event_loop_thread = threading.get_ident()
    observed_threads: list[int] = []

    def board_read(_board_id: str) -> list[dict]:
        observed_threads.append(threading.get_ident())
        return []

    async def digest_read(
        _db: object,
        *,
        board_id: str,
        blocking_execution=None,
    ) -> dict:
        del board_id, blocking_execution
        return {
            "status": "available",
            "evaluation": "evaluated",
            "reason": "ok",
            "items": [],
        }

    monkeypatch.setattr(
        stale_canonical_parity,
        "detect_board_graph_stale",
        board_read,
    )
    monkeypatch.setattr(
        layer_parity,
        "detect_digest_layer_mismatches",
        digest_read,
    )

    result = await stale_canonical_parity.list_stale_canonical_parity(
        object(),
        board_id="board-offload",
    )

    assert result["count"] == 0
    assert observed_threads and observed_threads[0] != event_loop_thread


@pytest.mark.asyncio
async def test_digest_parity_offloads_graph_input_collection(monkeypatch) -> None:
    event_loop_thread = threading.get_ident()
    observed_threads: list[int] = []

    def collect(_board_id: str) -> dict:
        observed_threads.append(threading.get_ident())
        return {
            "status": "unavailable",
            "reason": "test",
            "digests": [],
            "board_meta": {},
            "needs_overlay": False,
        }

    monkeypatch.setattr(
        layer_parity,
        "collect_digest_layer_mismatch_inputs",
        collect,
    )

    result = await layer_parity.detect_digest_layer_mismatches(
        object(),
        board_id="board-offload",
    )

    assert result == {
        "status": "unavailable",
        "evaluation": "not_evaluated",
        "reason": "test",
        "items": [],
    }
    assert observed_threads and observed_threads[0] != event_loop_thread
