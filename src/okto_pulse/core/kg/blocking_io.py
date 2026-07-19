"""Cancellation-safe bridge for synchronous embedded-graph operations.

The Core graph ports are synchronous because some editions embed a native
database.  Async request paths must therefore dispatch those calls away from
the event-loop thread.  Keeping the native operation alive after its caller is
cancelled is equally important: it may still own an edition-level writer lease.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextvars import copy_context
from typing import TypeVar

from okto_pulse.core.ports.runtime_workers import BlockingExecutionPort


_T = TypeVar("_T")


async def run_blocking_graph_io(
    operation: Callable[[], _T],
    *,
    task_name: str,
    blocking_execution: BlockingExecutionPort | None = None,
) -> _T:
    """Run one synchronous graph operation off-loop and drain it on cancel.

    Edition workers can supply their tracked :class:`BlockingExecutionPort`.
    Direct request/read-model callers use ``asyncio.to_thread`` while retaining
    the same cancellation guarantee: cancellation is propagated only after the
    native operation has released any graph resources it owns.
    """

    # ``BlockingExecutionPort`` implementations may use executors whose native
    # submission API does not propagate contextvars.  Capture at the Core
    # boundary so write guards, runtime composition and request ownership do
    # not depend on edition-specific executor behaviour.
    context = copy_context()

    def _run_in_context() -> _T:
        return context.run(operation)

    async def _run() -> _T:
        if blocking_execution is not None:
            return await blocking_execution.run(_run_in_context)
        return await asyncio.to_thread(_run_in_context)

    task = asyncio.create_task(_run(), name=task_name)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # A second cancellation request must not split cleanup from the native
        # operation. Keep draining until the owned task reaches a terminal state.
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        if task.done() and not task.cancelled():
            # Retrieve a possible exception so asyncio never reports an
            # unobserved task. The caller's cancellation remains authoritative.
            task.exception()
        raise


__all__ = ["run_blocking_graph_io"]
