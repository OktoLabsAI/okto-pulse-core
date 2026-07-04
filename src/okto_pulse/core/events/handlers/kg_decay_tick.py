"""KGDailyTickHandler — daily decay tick for the KG (Ideação #4, IMPL-D).

Reacts to ``kg.tick.daily`` (emitted by the active scheduler adapter using the
``kg_decay_tick_interval_minutes`` policy configured in ``config.py``) by
walking every active board and recomputing the relevance_score of nodes that
haven't been recomputed in
``KG_DECAY_TICK_STALENESS_DAYS`` days.

Cursor scan keeps memory bounded: results stream in batches of
``KG_DECAY_TICK_BATCH_SIZE`` ordered by ``id ASC`` so a tick never revisits
a node in the same run. Failure of one node never aborts the loop (BR14).
Board-level failures (corrupt/locked graph) are caught and counted
(``boards_failed``) so the rest of the fleet continues — FR1.

The tick run is logged in the ``kg_tick_runs`` table (Ideação #4 IMPL-F)
so kg_health can surface ``last_decay_tick_at`` and
``nodes_recomputed_in_last_tick`` to operators / agents.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import KGDailyTick
from okto_pulse.core.kg.async_bridge import run_async_blocking
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.kg.schema_contract import NODE_TYPES
from okto_pulse.core.kg.scoring import _recompute_relevance_batch
from okto_pulse.core.models.db import KGTickRun

logger = logging.getLogger(__name__)


KG_DECAY_TICK_BATCH_SIZE = int(os.getenv("KG_DECAY_TICK_BATCH_SIZE", "200"))
KG_DECAY_TICK_STALENESS_DAYS = int(os.getenv("KG_DECAY_TICK_STALENESS_DAYS", "7"))


async def publish_tick_events(
    session: AsyncSession,
    *,
    board_id: str | None = None,
    actor_id: str | None = None,
    actor_type: str | None = None,
    scheduled_at: str | None = None,
) -> list[str]:
    """Publica ``KGDailyTick`` com FAN-OUT por board real. Retorna os tick_ids.

    Campo 2026-06-10: ``domain_events.board_id`` é NOT NULL com FK para
    ``boards.id``, e o runtime de produção (community ``serve``) liga
    ``PRAGMA foreign_keys=ON`` desde sempre — o sentinel global ``'*'``
    violava a constraint no INSERT do evento, então NENHUM tick (cron OU
    manual) chegava a ser agendado em produção (IntegrityError →
    ``tick_schedule_failed``). Os testes do core não ligam o PRAGMA, por
    isso o bug nunca apareceu na suíte.

    O fan-out publica um evento POR board existente: a FK passa, o
    isolamento por board vira total (falha de um board não toca os outros
    nem na enumeração) e eventos de boards deletados são limpos pelo
    ON DELETE CASCADE. O handler já suportava ``board_id`` concreto.
    """
    from sqlalchemy import select

    from okto_pulse.core.events import publish as event_publish
    from okto_pulse.core.models.db import Board

    if board_id and board_id != "*":
        board_ids = [board_id]
    else:
        board_ids = list(
            (await session.execute(select(Board.id))).scalars().all()
        )

    when = scheduled_at or datetime.now(timezone.utc).isoformat()
    extra_kwargs: dict[str, str] = {}
    if actor_id is not None:
        extra_kwargs["actor_id"] = actor_id
    if actor_type is not None:
        extra_kwargs["actor_type"] = actor_type

    tick_ids: list[str] = []
    for bid in board_ids:
        tid = str(uuid.uuid4())
        await event_publish(
            KGDailyTick(
                board_id=bid,
                tick_id=tid,
                scheduled_at=when,
                **extra_kwargs,
            ),
            session=session,
        )
        tick_ids.append(tid)
    return tick_ids


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
    """Drain stale nodes for one board. Returns (recomputed, stale_pre_count).

    FR7: opening the graph transaction port happens before the per-node loop.
    If the concrete backend raises (e.g. corrupt or locked graph), the exception
    propagates to ``_run_daily_tick`` so only that board increments
    ``boards_failed``.
    """
    registry = get_kg_registry()
    if not registry.graph_runtime_store.exists(board_id):
        return (0, 0)

    async def _run() -> tuple[int, int]:
        total = 0
        async with await registry.graph_transaction.begin(board_id) as scope:
            stale_pre_count = _count_stale_nodes_pre_tick(scope, cutoff_iso)
            for node_type in NODE_TYPES:
                cursor: str | None = None
                while True:
                    stale = _fetch_stale_nodes(
                        scope, node_type, cutoff_iso, cursor,
                        limit=batch_size,
                    )
                    if not stale:
                        break
                    try:
                        persisted = _recompute_relevance_batch(
                            scope, board_id, stale, trigger="daily_tick",
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
        return (total, stale_pre_count)

    return run_async_blocking(_run())


async def _persist_tick_run(
    session: AsyncSession,
    *,
    tick_id: str,
    started_at: datetime,
    completed_at: datetime,
    nodes_recomputed: int,
    duration_ms: float,
    boards_processed: int,
    boards_failed: int = 0,
    error: str | None = None,
) -> None:
    """Insert or update the row that kg_health will surface as the latest tick state.

    Uses an idempotent upsert (ON CONFLICT DO UPDATE on ``tick_id`` PK) so
    that dispatcher retries with the same ``tick_id`` produce exactly one
    row — FR2/TR1.  ``session.merge`` is NOT used (it issues a SELECT +
    conditional INSERT/UPDATE; the explicit upsert is a single statement).
    """
    # Dialect dispatch: same approach as consolidation_enqueuer.py so both
    # SQLite (dev/test) and PostgreSQL (prod) are covered by a single code path.
    dialect_name = session.bind.dialect.name if session.bind else None
    if dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as _upsert_insert
    else:
        from sqlalchemy.dialects.sqlite import insert as _upsert_insert

    stmt = _upsert_insert(KGTickRun).values(
        tick_id=tick_id,
        started_at=started_at,
        completed_at=completed_at,
        nodes_recomputed=nodes_recomputed,
        duration_ms=duration_ms,
        boards_processed=boards_processed,
        boards_failed=boards_failed,
        error=error,
    ).on_conflict_do_update(
        index_elements=["tick_id"],
        set_={
            "completed_at": completed_at,
            "nodes_recomputed": nodes_recomputed,
            "duration_ms": duration_ms,
            "boards_processed": boards_processed,
            "boards_failed": boards_failed,
            "error": error,
        },
    )
    await session.execute(stmt)
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
    ``{tick_id, nodes_recomputed, boards_processed, boards_failed,
    duration_ms, nodes_with_stale_score_pre_tick}``.

    FR1/TR2: each board iteration is wrapped in an independent try/except.
    A board whose graph transaction open raises (corrupt/locked graph)
    increments ``boards_failed`` and emits a ``kg.tick.board_failed`` warning
    without aborting the remaining boards.
    """
    from sqlalchemy import select
    from okto_pulse.core.models.db import Board

    started_at = datetime.now(timezone.utc)
    cutoff_iso = (started_at - timedelta(days=staleness_days)).isoformat()
    boards_processed = 0
    boards_failed = 0
    total_recomputed = 0

    nodes_with_stale_score_pre_tick = 0
    if board_id and board_id != "*":
        boards = [board_id]
    else:
        boards = (await session.execute(select(Board.id))).scalars().all()

    for bid in boards:
        try:
            recomputed, stale_pre = await asyncio.to_thread(
                _process_board_sync, bid, cutoff_iso, batch_size=batch_size,
            )
            boards_processed += 1
            total_recomputed += recomputed
            nodes_with_stale_score_pre_tick += stale_pre
        except Exception as exc:
            # FR1/TR2: any exception (RuntimeError from graph transaction open,
            # asyncio wrapper, etc.) is caught here so the fleet continues.
            boards_failed += 1
            logger.warning(
                "kg.tick.board_failed board_id=%s error=%s",
                bid, str(exc),
                extra={
                    "event": "kg.tick.board_failed",
                    "board_id": bid,
                    "error": str(exc),
                },
            )

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
        boards_failed=boards_failed,
    )

    summary = {
        "tick_id": tick_id,
        "nodes_recomputed": total_recomputed,
        "boards_processed": boards_processed,
        "boards_failed": boards_failed,
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
        started_at = datetime.now(timezone.utc)
        try:
            await _run_daily_tick(
                tick_id=event.tick_id,
                session=session,
                board_id=event.board_id,
            )
        except Exception as exc:
            # FR3: on error, persist the tick_run with the error string and
            # return normally. The persisted row IS the idempotent signal; the
            # dispatcher marks the handler execution done. Do NOT re-raise.
            completed_at = datetime.now(timezone.utc)
            try:
                await _persist_tick_run(
                    session,
                    tick_id=event.tick_id,
                    started_at=started_at,
                    completed_at=completed_at,
                    nodes_recomputed=0,
                    duration_ms=(
                        completed_at - started_at
                    ).total_seconds() * 1000.0,
                    boards_processed=0,
                    boards_failed=0,
                    error=str(exc),
                )
            except Exception:
                logger.exception(
                    "kg.relevance.tick.failure_persist_failed tick_id=%s",
                    event.tick_id,
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
