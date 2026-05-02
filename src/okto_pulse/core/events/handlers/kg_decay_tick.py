"""KGDailyTickHandler — daily decay tick for the KG (Ideação #4, IMPL-D).

Reacts to ``kg.tick.daily`` (emitted by the APScheduler cron at 03:00 UTC)
by walking every active board and recomputing the relevance_score of nodes
that haven't been recomputed in ``KG_DECAY_TICK_STALENESS_DAYS`` days.

Cursor scan keeps memory bounded: results stream in batches of
``KG_DECAY_TICK_BATCH_SIZE`` ordered by ``id ASC`` so a tick never revisits
a node in the same run. Failure of one node never aborts the loop (BR14).

The tick run is logged in the ``kg_tick_runs`` table (Ideação #4 IMPL-F)
so kg_health can surface ``last_decay_tick_at`` and
``nodes_recomputed_in_last_tick`` to operators / agents.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import KGDailyTick
from okto_pulse.core.kg.schema import (
    NODE_TYPES,
    board_kuzu_path,
    open_board_connection,
)
from okto_pulse.core.kg.scoring import _recompute_relevance_batch
from okto_pulse.core.models.db import Board, KGTickRun

logger = logging.getLogger(__name__)


KG_DECAY_TICK_BATCH_SIZE = int(os.getenv("KG_DECAY_TICK_BATCH_SIZE", "200"))
KG_DECAY_TICK_STALENESS_DAYS = int(os.getenv("KG_DECAY_TICK_STALENESS_DAYS", "7"))


def _fetch_stale_nodes(
    conn,
    node_type: str,
    cutoff_iso: str,
    cursor_id: str | None,
    *,
    limit: int,
) -> list[tuple[str, str]]:
    """Return up to ``limit`` (node_type, node_id) pairs needing recompute.

    A node is "stale" when ``last_recomputed_at IS NULL`` or strictly less
    than ``cutoff_iso`` (ISO datetime). Pagination uses a strictly
    increasing ``id > cursor_id`` keyset so a tick never revisits the same
    node within one scan, even when later writes shift the candidate set.
    """
    res = None
    rows: list[tuple[str, str]] = []
    try:
        if cursor_id is None:
            res = conn.execute(
                f"MATCH (n:{node_type}) "
                f"WHERE (n.last_recomputed_at IS NULL "
                f"       OR n.last_recomputed_at < $cutoff) "
                f"RETURN n.id "
                f"ORDER BY n.id ASC "
                f"LIMIT $limit",
                {"cutoff": cutoff_iso, "limit": limit},
            )
        else:
            res = conn.execute(
                f"MATCH (n:{node_type}) "
                f"WHERE (n.last_recomputed_at IS NULL "
                f"       OR n.last_recomputed_at < $cutoff) "
                f"  AND n.id > $cursor "
                f"RETURN n.id "
                f"ORDER BY n.id ASC "
                f"LIMIT $limit",
                {"cutoff": cutoff_iso, "cursor": cursor_id, "limit": limit},
            )
        while res.has_next():
            row = res.get_next()
            rows.append((node_type, str(row[0])))
    except Exception as exc:
        logger.warning(
            "kg.tick.fetch_stale_failed node_type=%s err=%s", node_type, exc,
        )
    finally:
        if res is not None:
            try:
                res.close()
            except Exception:
                pass
    return rows


def _count_stale_nodes_pre_tick(conn, cutoff_iso: str) -> int:
    """Count nodes meeting the stale criterion BEFORE the tick processes them.

    Spec 28583299 (Ideação #4, AC41/TS42): the structured log
    ``kg.relevance.tick.completed`` exposes ``nodes_with_stale_score_pre_tick``
    so operators / agents can spot drift in the recompute mechanism — a
    sustained high value means hit-flush / boost-change recompute aren't
    keeping pace and the tick is doing more catch-up work than expected.
    """
    total = 0
    for node_type in NODE_TYPES:
        res = None
        try:
            res = conn.execute(
                f"MATCH (n:{node_type}) "
                f"WHERE (n.last_recomputed_at IS NULL "
                f"       OR n.last_recomputed_at < $cutoff) "
                f"RETURN count(n) AS c",
                {"cutoff": cutoff_iso},
            )
            if res.has_next():
                row = res.get_next()
                total += int(row[0] or 0)
        except Exception as exc:
            logger.debug(
                "kg.tick.stale_count_failed node_type=%s err=%s",
                node_type, exc,
            )
        finally:
            if res is not None:
                try:
                    res.close()
                except Exception:
                    pass
    return total


def _process_board_sync(
    board_id: str, cutoff_iso: str, *, batch_size: int,
) -> tuple[int, int]:
    """Drain stale nodes for one board. Returns (recomputed, stale_pre_count)."""
    if not board_kuzu_path(board_id).exists():
        return (0, 0)
    total = 0
    stale_pre_count = 0
    bc = open_board_connection(board_id)
    try:
        stale_pre_count = _count_stale_nodes_pre_tick(bc.conn, cutoff_iso)
        for node_type in NODE_TYPES:
            cursor: str | None = None
            while True:
                stale = _fetch_stale_nodes(
                    bc.conn, node_type, cutoff_iso, cursor,
                    limit=batch_size,
                )
                if not stale:
                    break
                try:
                    persisted = _recompute_relevance_batch(
                        bc.conn, board_id, stale, trigger="daily_tick",
                    )
                    total += persisted
                except Exception as exc:
                    # BR14 — a single batch failure does not abort the tick.
                    logger.warning(
                        "kg.tick.batch_failed board=%s node_type=%s err=%s",
                        board_id, node_type, exc,
                    )
                cursor = stale[-1][1]
                if len(stale) < batch_size:
                    break
    finally:
        bc.close()
        del bc
        gc.collect()
    return (total, stale_pre_count)


async def _persist_tick_run(
    session: AsyncSession,
    *,
    tick_id: str,
    started_at: datetime,
    completed_at: datetime,
    nodes_recomputed: int,
    duration_ms: float,
    boards_processed: int,
    error: str | None = None,
) -> None:
    """Insert the row that kg_health will surface as the latest tick state."""
    session.add(
        KGTickRun(
            tick_id=tick_id,
            started_at=started_at,
            completed_at=completed_at,
            nodes_recomputed=nodes_recomputed,
            duration_ms=duration_ms,
            boards_processed=boards_processed,
            error=error,
        )
    )
    await session.commit()


async def _run_daily_tick(
    *,
    tick_id: str,
    session: AsyncSession,
    board_id: str | None = None,
    batch_size: int = KG_DECAY_TICK_BATCH_SIZE,
    staleness_days: int = KG_DECAY_TICK_STALENESS_DAYS,
) -> dict:
    """Execute the full tick cycle and persist the run row.

    Returns a summary dict suitable for structured logging / tests:
    ``{tick_id, nodes_recomputed, boards_processed, duration_ms}``.
    """
    started_at = datetime.now(timezone.utc)
    cutoff_iso = (started_at - timedelta(days=staleness_days)).isoformat()
    boards_processed = 0
    total_recomputed = 0

    nodes_with_stale_score_pre_tick = 0
    if board_id and board_id != "*":
        boards = [board_id]
    else:
        boards = (await session.execute(select(Board.id))).scalars().all()
    for board_id in boards:
        boards_processed += 1
        recomputed, stale_pre = await asyncio.to_thread(
            _process_board_sync, board_id, cutoff_iso, batch_size=batch_size,
        )
        total_recomputed += recomputed
        nodes_with_stale_score_pre_tick += stale_pre

    completed_at = datetime.now(timezone.utc)
    duration_ms = (completed_at - started_at).total_seconds() * 1000.0
    await _persist_tick_run(
        session,
        tick_id=tick_id,
        started_at=started_at,
        completed_at=completed_at,
        nodes_recomputed=total_recomputed,
        duration_ms=duration_ms,
        boards_processed=boards_processed,
    )

    summary = {
        "tick_id": tick_id,
        "nodes_recomputed": total_recomputed,
        "boards_processed": boards_processed,
        "duration_ms": duration_ms,
        "nodes_with_stale_score_pre_tick": nodes_with_stale_score_pre_tick,
    }
    logger.info(
        "kg.relevance.tick.completed",
        extra={"event": "kg.relevance.tick.completed", **summary},
    )
    return summary


@register_handler("kg.tick.daily")
class KGDailyTickHandler:
    async def handle(self, event: KGDailyTick, session: AsyncSession) -> None:
        await _run_daily_tick(
            tick_id=event.tick_id,
            session=session,
            board_id=event.board_id,
        )


__all__ = [
    "KGDailyTickHandler",
    "KG_DECAY_TICK_BATCH_SIZE",
    "KG_DECAY_TICK_STALENESS_DAYS",
    "_fetch_stale_nodes",
    "_process_board_sync",
    "_run_daily_tick",
    "_persist_tick_run",
]
