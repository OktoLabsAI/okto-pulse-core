"""Startup per-board KG schema sweep over the spec #06 ports (R16-D, tr_7ea5e6d8).

Extracted from ``core/app.py`` so the loop / skip-missing / off-loop /
soft-fail-per-board / count / log behaviour is unit-testable WITHOUT booting
FastAPI.

This module consumes the #06 ports (``GraphPathResolver`` +
``GraphSchemaManager``) and NEVER imports ``kg.schema`` directly:

  * ``graph_path_resolver.exists(board_id)`` decides skip-missing (replaces
    ``kg.schema.board_kuzu_path(bid).exists()``).
  * ``graph_schema_manager.ensure_bootstrapped(board_id)`` performs the
    idempotent per-board migration (equivalent to the old
    ``open_board_connection(bid) + bc.close()`` open/close sweep).

The #06 embedded adapter runs the synchronous Kùzu work under the async port, so
each ensure is driven OFF the event loop (``asyncio.to_thread`` + ``asyncio.run``)
to preserve the original non-blocking boot (opening a graph can stall on lock
contention; running it on the loop froze the whole server at startup).
``run_blocking`` is injectable for deterministic tests.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Iterable

#: A zero-arg factory returning the port coroutine to run (so the coroutine is
#: created inside the worker thread, never across threads).
CoroFactory = Callable[[], Awaitable[Any]]
RunBlocking = Callable[[CoroFactory], Awaitable[Any]]


async def _run_off_loop(coro_factory: CoroFactory) -> Any:
    """Run an async #06 port call off the event loop.

    The embedded adapter is synchronous under the async port, so we offload to a
    worker thread (which owns a throwaway event loop) — preserving the original
    non-blocking sweep/shutdown behaviour. The coroutine is created inside the
    worker thread to avoid cross-thread coroutine binding.
    """
    return await asyncio.to_thread(lambda: asyncio.run(coro_factory()))


async def sweep_board_schemas(
    board_ids: Iterable[str],
    *,
    graph_path_resolver: Any,
    graph_schema_manager: Any,
    logger: logging.Logger,
    run_blocking: RunBlocking | None = None,
) -> int:
    """Idempotently ensure each existing board graph is migrated; return count.

    Semantics preserved bit-for-bit from the old ``core/app.py`` sweep:

      * skip a board whose graph file does not exist
        (``graph_path_resolver.exists`` — a resolver error is NOT caught here so
        it propagates to the caller's outer guard -> ``kg.schema.migration_skipped``,
        exactly like the old ``board_kuzu_path(bid).exists()`` did);
      * ``graph_schema_manager.ensure_bootstrapped`` per existing board, run off
        the event loop;
      * soft-fail per board: log ``kg.schema.migration_failed`` and continue;
      * log ``kg.schema.migration_swept`` with the migrated count and return it.
    """
    run = run_blocking or _run_off_loop
    migrated = 0
    for board_id in board_ids:
        # NB: a resolver exception is intentionally NOT caught — it propagates to
        # the caller's outer try (-> kg.schema.migration_skipped), matching the
        # old behaviour where board_kuzu_path(bid).exists() raising aborted the
        # whole sweep rather than soft-failing one board.
        if not graph_path_resolver.exists(board_id):
            continue
        try:
            await run(
                lambda b=board_id: graph_schema_manager.ensure_bootstrapped(b)
            )
            migrated += 1
        except Exception as exc:  # noqa: BLE001 — soft-fail per board, never abort
            logger.warning(
                "kg.schema.migration_failed board=%s err=%s",
                board_id, exc,
                extra={
                    "event": "kg.schema.migration_failed",
                    "board_id": board_id,
                    "error": str(exc),
                },
            )
    logger.info(
        "kg.schema.migration_swept boards=%d", migrated,
        extra={
            "event": "kg.schema.migration_swept",
            "boards_swept": migrated,
        },
    )
    return migrated


__all__ = ["sweep_board_schemas"]
