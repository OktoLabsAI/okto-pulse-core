from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace

import pytest

from okto_pulse.core.mcp.server import _run_rebuild_service_cooperatively


@pytest.mark.asyncio
async def test_cancelled_mcp_rebuild_finishes_background_cleanup() -> None:
    started = threading.Event()
    cleaned = threading.Event()

    class _Service:
        lock_held = False
        promoted = False

        def run(self, *, cancel_requested, **_kwargs):  # noqa: ANN001, ANN201
            self.lock_held = True
            started.set()
            while not cancel_requested():
                time.sleep(0.005)
            # Models the service/processor contract: cancellation completes
            # compensation and releases the writer lock before returning.
            self.lock_held = False
            cleaned.set()
            return SimpleNamespace(outcome="failed", run_id="run-cancelled")

    service = _Service()
    task = asyncio.create_task(
        _run_rebuild_service_cooperatively(
            service,
            board_id="board-1",
            confirmation_id="confirmation-1",
        )
    )
    assert await asyncio.to_thread(started.wait, 1.0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert await asyncio.to_thread(cleaned.wait, 1.0)
    assert service.lock_held is False
    assert service.promoted is False
