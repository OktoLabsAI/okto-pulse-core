"""KG health snapshot service — feeds /api/v1/kg/health.

Spec 20f67c2a (Ideação #5, FR1, FR2, BR1). Composes 10 fields into a
JSON payload describing the live state of a board's knowledge graph:

    * SQL aggregations against ConsolidationQueue + ConsolidationDeadLetter
      for queue depth, oldest pending age, and dead letter count.
    * Kùzu aggregations across all node tables for total_nodes, the count
      of nodes whose relevance_score is in the [0.45, 0.55] "default"
      band (sintoma de inflation), avg_relevance, and the top-N
      most-disconnected nodes (lowest degree).
    * In-process counter from scoring.get_contradict_warn_count for
      contradict_warn_count.
    * schema_version is a fixed string ("1.0") versioning the response
      payload independently of the Kùzu schema.

When Kùzu hasn't been bootstrapped for the board (or any aggregation
fails), Kùzu-derived fields gracefully degrade to zero and the response
still ships. The endpoint must never 500 on a healthy app DB.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.kg.scoring import get_contradict_warn_count
from okto_pulse.core.models.db import (
    Board,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    KGTickRun,
)

logger = logging.getLogger("okto_pulse.services.kg_health")


HEALTH_SCHEMA_VERSION = "1.0"

# Spec 20f67c2a (Ideação #5): "default score" band used to flag inflation
# sintoma. Nodes whose relevance_score falls in [0.45, 0.55] are likely
# stuck near the neutral default and don't reflect any real signal yet.
DEFAULT_SCORE_BAND_LOW = 0.45
DEFAULT_SCORE_BAND_HIGH = 0.55

# When the ratio of default-band nodes crosses this threshold the service
# emits a structured WARN log so observability tooling can flag the board.
DEFAULT_SCORE_RATIO_ALARM_THRESHOLD = 0.7

# How many "most disconnected" nodes the response surfaces.
TOP_DISCONNECTED_NODES_LIMIT = 10


class BoardNotFoundError(Exception):
    """Raised when the requested board does not exist."""


async def get_kg_health(board_id: str, db: AsyncSession) -> dict[str, Any]:
    """Compose the /api/v1/kg/health payload for ``board_id``.

    Raises ``BoardNotFoundError`` when the board is not found in the
    SQLite app DB. All Kùzu-derived metrics degrade to zero on lookup
    errors — the endpoint never 500s for a transient Kùzu issue.
    """
    board = await db.get(Board, board_id)
    if board is None:
        raise BoardNotFoundError(f"board not found: {board_id}")

    now = datetime.now(timezone.utc)

    queue_depth = await db.scalar(
        select(func.count()).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.status.in_(["pending", "claimed"]),
        )
    ) or 0

    oldest_triggered = await db.scalar(
        select(func.min(ConsolidationQueue.triggered_at)).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.status.in_(["pending", "claimed"]),
        )
    )
    if oldest_triggered is not None:
        if oldest_triggered.tzinfo is None:
            oldest_triggered = oldest_triggered.replace(tzinfo=timezone.utc)
        oldest_pending_age_s = max(0.0, (now - oldest_triggered).total_seconds())
    else:
        oldest_pending_age_s = 0.0

    dead_letter_count = await db.scalar(
        select(func.count()).where(
            ConsolidationDeadLetter.board_id == board_id,
        )
    ) or 0

    last_tick_run = await db.scalar(
        select(KGTickRun)
        .where(KGTickRun.completed_at.is_not(None))
        .order_by(KGTickRun.completed_at.desc())
        .limit(1)
    )
    if last_tick_run is not None:
        last_completed = last_tick_run.completed_at
        if last_completed is not None and last_completed.tzinfo is None:
            last_completed = last_completed.replace(tzinfo=timezone.utc)
        last_decay_tick_at = (
            last_completed.isoformat() if last_completed is not None else None
        )
        nodes_recomputed_in_last_tick = int(last_tick_run.nodes_recomputed or 0)
    else:
        last_decay_tick_at = None
        nodes_recomputed_in_last_tick = 0
    if last_tick_run is not None:
        last_tick_status = "failed" if last_tick_run.error else "completed"
        last_tick_error = last_tick_run.error
    else:
        last_tick_status = None
        last_tick_error = None

    kuzu_metrics = _aggregate_kuzu_metrics(board_id)
    graph_schema_version = _get_graph_schema_version(board_id)

    default_score_count = kuzu_metrics["default_score_count"]
    total_nodes = kuzu_metrics["total_nodes"]
    default_score_ratio = (
        default_score_count / total_nodes if total_nodes > 0 else 0.0
    )

    if default_score_ratio > DEFAULT_SCORE_RATIO_ALARM_THRESHOLD:
        logger.warning(
            "kg.health.default_score_skew_high board=%s ratio=%.3f "
            "count=%d total=%d threshold=%.2f",
            board_id, default_score_ratio, default_score_count, total_nodes,
            DEFAULT_SCORE_RATIO_ALARM_THRESHOLD,
            extra={
                "event": "kg.health.default_score_skew_high",
                "board_id": board_id,
                "default_score_ratio": default_score_ratio,
                "default_score_count": default_score_count,
                "total_nodes": total_nodes,
                "threshold": DEFAULT_SCORE_RATIO_ALARM_THRESHOLD,
            },
        )

    # Bug fix (Playwright E2E reproduzido): se o usuário fechar o modal
    # enquanto o tick roda e voltar, o frontend perde o state local de
    # "running" e re-habilita o botão. Para o frontend conseguir desabilitar
    # através de remount, expomos o estado real do advisory lock global
    # ``("kg_daily_tick", "global")``. Reuso do lock que kg_tick.py e o
    # cron já consultam — single source of truth.
    from okto_pulse.core.kg.workers.advisory_lock import get_async_lock
    tick_lock = get_async_lock("kg_daily_tick", "global")
    tick_in_progress = tick_lock.locked()
    if tick_in_progress:
        last_tick_status = "running"

    return {
        "queue_depth": int(queue_depth),
        "oldest_pending_age_s": round(oldest_pending_age_s, 3),
        "dead_letter_count": int(dead_letter_count),
        "total_nodes": total_nodes,
        "default_score_count": default_score_count,
        "default_score_ratio": round(default_score_ratio, 4),
        "avg_relevance": kuzu_metrics["avg_relevance"],
        "top_disconnected_nodes": kuzu_metrics["top_disconnected_nodes"],
        "schema_version": HEALTH_SCHEMA_VERSION,
        "health_schema_version": HEALTH_SCHEMA_VERSION,
        "graph_schema_version": graph_schema_version,
        "contradict_warn_count": get_contradict_warn_count(board_id),
        "last_decay_tick_at": last_decay_tick_at,
        "last_tick_status": last_tick_status,
        "last_tick_error": last_tick_error,
        "nodes_recomputed_in_last_tick": nodes_recomputed_in_last_tick,
        "tick_in_progress": tick_in_progress,
    }


def _get_graph_schema_version(board_id: str) -> str | None:
    try:
        from okto_pulse.core.kg.kg_service import get_kg_service

        return get_kg_service().get_schema_version(board_id)
    except Exception as exc:
        logger.debug(
            "kg.health.graph_schema_lookup_failed board=%s err=%s",
            board_id, exc,
        )
        return None


def _aggregate_kuzu_metrics(board_id: str) -> dict[str, Any]:
    """Pull node-level aggregates from Kùzu for ``board_id``.

    Returns a dict with total_nodes, default_score_count, avg_relevance and
    top_disconnected_nodes. On any Kùzu error (board not bootstrapped,
    schema drift, lock contention) returns zeroed defaults plus an empty
    list so the health endpoint stays available.
    """
    try:
        from okto_pulse.core.kg.schema import NODE_TYPES, open_board_connection
    except Exception as exc:
        logger.warning(
            "kg.health.kuzu_import_failed board=%s err=%s",
            board_id, exc,
        )
        return _zero_kuzu_metrics()

    total_nodes = 0
    default_score_count = 0
    relevance_sum = 0.0
    relevance_n = 0
    disconnected: list[dict[str, Any]] = []

    try:
        with open_board_connection(board_id) as (_db, conn):
            for node_type in NODE_TYPES:
                try:
                    res = conn.execute(
                        f"MATCH (n:{node_type}) "
                        f"OPTIONAL MATCH (n)-[r_out]->() "
                        f"WITH n, COUNT(r_out) AS od "
                        f"OPTIONAL MATCH (n)<-[r_in]-() "
                        f"WITH n, od, COUNT(r_in) AS id_ "
                        f"RETURN n.id, n.relevance_score, od + id_ AS deg",
                        {},
                    )
                except Exception as exc:
                    logger.debug(
                        "kg.health.kuzu_query_failed board=%s type=%s err=%s",
                        board_id, node_type, exc,
                    )
                    continue
                while res.has_next():
                    row = res.get_next()
                    node_id = row[0]
                    rel = row[1]
                    deg = int(row[2] or 0)
                    total_nodes += 1
                    if rel is not None:
                        rel_f = float(rel)
                        relevance_sum += rel_f
                        relevance_n += 1
                        if DEFAULT_SCORE_BAND_LOW <= rel_f <= DEFAULT_SCORE_BAND_HIGH:
                            default_score_count += 1
                    disconnected.append(
                        {"id": node_id, "type": node_type, "degree": deg}
                    )
    except Exception as exc:
        logger.warning(
            "kg.health.kuzu_open_failed board=%s err=%s",
            board_id, exc,
        )
        return _zero_kuzu_metrics()

    disconnected.sort(key=lambda r: r["degree"])
    top_disconnected = disconnected[:TOP_DISCONNECTED_NODES_LIMIT]

    avg_relevance = (
        round(relevance_sum / relevance_n, 4) if relevance_n > 0 else 0.0
    )

    return {
        "total_nodes": total_nodes,
        "default_score_count": default_score_count,
        "avg_relevance": avg_relevance,
        "top_disconnected_nodes": top_disconnected,
    }


def _zero_kuzu_metrics() -> dict[str, Any]:
    return {
        "total_nodes": 0,
        "default_score_count": 0,
        "avg_relevance": 0.0,
        "top_disconnected_nodes": [],
    }
