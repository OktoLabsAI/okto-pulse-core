"""Small sync bridge for async KG ports used from worker threads."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine
from contextvars import copy_context
from typing import Any, TypeVar

T = TypeVar("T")


def run_async_blocking(coro: Coroutine[Any, Any, T]) -> T:
    """Run an async port call from synchronous KG code.

    Most migrated graph call sites are sync functions already dispatched through
    ``asyncio.to_thread``. When a caller accidentally invokes them from a running
    event loop, execute the coroutine in a short-lived helper thread instead of
    trying to nest event loops.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    box: dict[str, Any] = {}

    def _runner() -> None:
        try:
            box["result"] = asyncio.run(coro)
        except BaseException as exc:  # pragma: no cover - pass-through bridge
            box["error"] = exc

    context = copy_context()
    thread = threading.Thread(target=lambda: context.run(_runner), daemon=True)
    thread.start()
    thread.join()
    if "error" in box:
        raise box["error"]
    return box["result"]
