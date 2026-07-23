"""Structured ownership for short-lived application child tasks.

Long-lived polling loops belong to edition-owned runtime workers. Core
processors may still need bounded concurrency inside one already-owned
iteration (for example, work racing a lease-renewal heartbeat). This scope
keeps creation and cancellation-draining behind one public application
primitive so processors do not become task-runtime composition roots.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any


class AsyncTaskScope:
    """Own and terminally drain bounded child tasks for one operation."""

    def __init__(self) -> None:
        self._tasks: set[asyncio.Task[Any]] = set()

    def start(self, operation: Awaitable[Any], *, name: str) -> asyncio.Task[Any]:
        task = asyncio.get_running_loop().create_task(operation, name=name)
        self._tasks.add(task)
        return task

    async def drain(
        self,
        task: asyncio.Task[Any],
        *,
        cancel: bool,
    ) -> None:
        """Reach a terminal child state despite repeated parent cancellation."""

        if cancel and not task.done():
            task.cancel()
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        if task.done() and not task.cancelled():
            # Consume a possible failure so a cleanup path never emits an
            # unobserved-task warning or replaces the authoritative race error.
            task.exception()
        self._tasks.discard(task)

    async def drain_all(self, *, cancel: bool) -> None:
        for task in tuple(self._tasks):
            await self.drain(task, cancel=cancel)


__all__ = ["AsyncTaskScope"]
