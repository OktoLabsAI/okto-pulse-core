from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager

import pytest

from okto_pulse.core.application.processors.global_outbox import (
    GlobalOutboxProcessor,
)


@pytest.mark.asyncio
async def test_direct_outbox_graph_io_never_blocks_the_event_loop_thread() -> None:
    processor = GlobalOutboxProcessor(lambda: None)
    event_loop_thread = threading.get_ident()

    graph_thread = await processor._run_graph_io(threading.get_ident)

    assert graph_thread != event_loop_thread


@pytest.mark.asyncio
async def test_process_once_propagates_durable_writer_fence_to_graph_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg.global_discovery_writer import (
        assert_global_discovery_writer_fence,
    )
    from okto_pulse.core.kg.write_barrier import require_global_write_token

    processor = GlobalOutboxProcessor(lambda: None)
    event_loop_thread = threading.get_ident()
    observed: dict[str, object] = {}

    def graph_operation() -> int:
        assert_global_discovery_writer_fence()
        guard = require_global_write_token()
        observed["thread"] = threading.get_ident()
        observed["operation"] = guard.operation
        return 7

    async def process_under_writer() -> int:
        return await processor._run_graph_io(graph_operation)

    monkeypatch.setattr(
        processor,
        "_process_once_under_writer",
        process_under_writer,
    )

    assert await processor.process_once() == 7
    assert observed["operation"] == "global_outbox_apply"
    assert observed["thread"] != event_loop_thread


@pytest.mark.asyncio
async def test_cancelled_outbox_drains_native_graph_io_before_returning() -> None:
    processor = GlobalOutboxProcessor(lambda: None)
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    relational_scope_exited = asyncio.Event()

    def graph_operation() -> str:
        started.set()
        assert release.wait(timeout=5)
        finished.set()
        return "durable"

    @asynccontextmanager
    async def relational_scope():
        try:
            yield
        finally:
            relational_scope_exited.set()

    async def process_inside_relational_scope() -> None:
        async with relational_scope():
            await processor._run_graph_io(graph_operation)

    task = asyncio.create_task(process_inside_relational_scope())
    assert await asyncio.to_thread(started.wait, 1)

    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0.01)
    assert not task.done()
    assert not relational_scope_exited.is_set()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()
    assert relational_scope_exited.is_set()
