from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.processors import global_outbox as worker_module
from okto_pulse.core.application.processors.global_outbox import (
    GLOBAL_OUTBOX_WRITER_MAX_TTL_SECONDS,
    GLOBAL_OUTBOX_WRITER_TTL_SECONDS,
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


class _ObservedWriterLease:
    def __init__(self, *, renewal_error: Exception | None = None) -> None:
        self.renewal_error = renewal_error
        self.renew_calls = 0
        self.release_calls = 0
        self.renewed = threading.Event()

    @contextmanager
    def guard(self):
        yield

    def renew(self) -> None:
        self.renew_calls += 1
        self.renewed.set()
        if self.renewal_error is not None:
            raise self.renewal_error

    def assert_fenced(self) -> None:
        return None

    def release(self) -> bool:
        self.release_calls += 1
        return True


def _install_observed_lease(
    monkeypatch: pytest.MonkeyPatch,
    lease: _ObservedWriterLease,
    acquired: dict[str, object],
) -> None:
    from okto_pulse.core.kg.global_discovery_writer import (
        GlobalDiscoveryWriterLease,
    )

    def acquire(_cls, **kwargs):
        acquired.update(kwargs)
        return lease

    monkeypatch.setattr(
        GlobalDiscoveryWriterLease,
        "acquire",
        classmethod(acquire),
    )


@pytest.mark.asyncio
async def test_outbox_writer_crash_ttl_is_bounded_to_one_minute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = _ObservedWriterLease()
    acquired: dict[str, object] = {}
    _install_observed_lease(monkeypatch, lease, acquired)
    processor = GlobalOutboxProcessor(lambda: None)

    async def no_work() -> int:
        return 0

    monkeypatch.setattr(processor, "_process_once_under_writer", no_work)

    assert await processor.process_once() == 0
    assert acquired["ttl_seconds"] == GLOBAL_OUTBOX_WRITER_TTL_SECONDS
    assert GLOBAL_OUTBOX_WRITER_TTL_SECONDS <= 60
    assert GLOBAL_OUTBOX_WRITER_MAX_TTL_SECONDS <= 60
    assert lease.release_calls == 1

    with pytest.raises(ValueError, match="writer_lease_ttl_seconds"):
        GlobalOutboxProcessor(
            lambda: None,
            writer_lease_ttl_seconds=3600,
        )


@pytest.mark.asyncio
async def test_outbox_writer_lease_renews_while_batch_is_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = _ObservedWriterLease()
    acquired: dict[str, object] = {}
    _install_observed_lease(monkeypatch, lease, acquired)
    processor = GlobalOutboxProcessor(
        lambda: None,
        writer_lease_ttl_seconds=2,
        writer_lease_renew_interval_seconds=0.01,
    )

    async def wait_for_renewal() -> int:
        renewed = await asyncio.to_thread(lease.renewed.wait, 1)
        assert renewed is True
        return 9

    monkeypatch.setattr(
        processor,
        "_process_once_under_writer",
        wait_for_renewal,
    )

    assert await processor.process_once() == 9
    assert acquired["ttl_seconds"] == 2
    assert lease.renew_calls >= 1
    assert lease.release_calls == 1


@pytest.mark.asyncio
async def test_outbox_writer_renewal_failure_cancels_batch_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg.global_discovery_writer import (
        GlobalDiscoveryWriterFenceLost,
    )

    lease = _ObservedWriterLease(renewal_error=RuntimeError("renew failed"))
    acquired: dict[str, object] = {}
    _install_observed_lease(monkeypatch, lease, acquired)
    processor = GlobalOutboxProcessor(
        lambda: None,
        writer_lease_ttl_seconds=2,
        writer_lease_renew_interval_seconds=0.01,
    )
    batch_started = asyncio.Event()
    batch_cancelled = asyncio.Event()

    async def blocked_batch() -> int:
        batch_started.set()
        try:
            await asyncio.Future()
        finally:
            batch_cancelled.set()
        return 1

    monkeypatch.setattr(processor, "_process_once_under_writer", blocked_batch)

    with pytest.raises(GlobalDiscoveryWriterFenceLost):
        await processor.process_once()

    assert batch_started.is_set()
    assert batch_cancelled.is_set()
    assert lease.renew_calls == 1
    assert lease.release_calls == 1


@pytest.mark.asyncio
async def test_repeated_cancel_keeps_renewing_until_native_batch_drains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = _ObservedWriterLease()
    acquired: dict[str, object] = {}
    _install_observed_lease(monkeypatch, lease, acquired)
    processor = GlobalOutboxProcessor(
        lambda: None,
        writer_lease_ttl_seconds=2,
        writer_lease_renew_interval_seconds=0.01,
    )
    native_started = threading.Event()
    native_release = threading.Event()
    native_finished = threading.Event()

    def native_write() -> None:
        native_started.set()
        assert native_release.wait(timeout=5)
        native_finished.set()

    async def blocked_batch() -> int:
        await processor._run_graph_io(native_write)
        return 1

    monkeypatch.setattr(processor, "_process_once_under_writer", blocked_batch)
    parent = asyncio.create_task(processor.process_once())
    assert await asyncio.to_thread(native_started.wait, 1)

    parent.cancel()
    await asyncio.sleep(0)
    parent.cancel()
    assert await asyncio.to_thread(lease.renewed.wait, 1)
    assert not parent.done()
    assert not native_finished.is_set()
    assert lease.renew_calls >= 1
    assert lease.release_calls == 0

    native_release.set()
    with pytest.raises(asyncio.CancelledError):
        await parent
    assert native_finished.is_set()
    assert lease.release_calls == 1


def test_post_write_verification_uses_one_reopen_window_for_whole_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Runtime:
        def __init__(self) -> None:
            self.close_calls = 0
            self.reopen_calls = 0
            self.handle_open = True
            self.scope_calls = 0

        @contextmanager
        def post_write_verification_scope(self):
            self.scope_calls += 1
            yield

        def close(self) -> None:
            self.close_calls += 1
            self.handle_open = False

    runtime = _Runtime()
    processor = GlobalOutboxProcessor(lambda: None)
    flush_calls = 0
    verified: list[str] = []
    source_rechecks: list[str] = []

    def flush_once() -> None:
        nonlocal flush_calls
        flush_calls += 1

    def verify(_runtime, board_id, _expected) -> None:
        if not runtime.handle_open:
            runtime.handle_open = True
            runtime.reopen_calls += 1
        verified.append(board_id)

    monkeypatch.setattr(worker_module, "_global_discovery_runtime", lambda: runtime)
    monkeypatch.setattr(
        processor,
        "_flush_global_discovery_storage_after_batch",
        flush_once,
    )
    monkeypatch.setattr(
        processor,
        "_assert_source_inventory_unchanged",
        lambda board_id, _expected: source_rechecks.append(board_id),
    )
    monkeypatch.setattr(processor, "_verify_reconciled_digest_layers", verify)

    errors, flush_error = processor._verify_processed_batch(
        [
            (
                SimpleNamespace(board_id="board-a"),
                {"source-a": ("canonical", "Decision")},
            ),
            (
                SimpleNamespace(board_id="board-b"),
                {"source-b": ("working", "Learning")},
            ),
        ]
    )

    assert errors == {}
    assert flush_error is None
    assert flush_calls == 1
    assert runtime.scope_calls == 1
    assert runtime.close_calls == 1
    assert runtime.reopen_calls == 1
    assert source_rechecks == ["board-a", "board-b"]
    assert verified == ["board-a", "board-b"]
