"""Live consolidation queue health metrics for /api/v1/kg/queue/health.

Spec bdcda842 (FR9, TR13). Combines:
    * SQL aggregations against ConsolidationQueue + ConsolidationDeadLetter
      (depth, oldest pending age, claimed count, claimed_boards set, DLQ size).
    * In-process sliding-window counters for ``claims_per_min_1m`` /
      ``claims_per_min_5m`` populated by the consolidation worker.
    * Worker pool snapshot (active/idle/draining counts) populated by the
      worker singleton.
    * Cross-process Kùzu file-lock retry counter exposed by
      ``commit_coordinator.kuzu_lock_retries_5m``.

The endpoint is read-only: it touches SQLite for queue stats but does not
hit Kùzu (alert_active is computed on-read from queue_depth + alert_threshold).
"""

from __future__ import annotations

import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.kg.commit_coordinator import kuzu_lock_retries_5m
from okto_pulse.core.models.db import (
    ConsolidationDeadLetter,
    ConsolidationQueue,
)


_CLAIMS_WINDOW_S = 300  # keep enough history for both 1m and 5m views
_CLAIM_TIMESTAMPS: deque[datetime] = deque()
_CLAIM_LOCK = threading.Lock()

_ALERT_FIRED_TOTAL = 0
_ALERT_FIRED_LOCK = threading.Lock()


def record_claim(now: datetime | None = None) -> None:
    """Append a claim event to the sliding window. Called by the worker on
    every successful claim transition (pending→claimed). Pruned on read."""
    ts = now or datetime.now(timezone.utc)
    cutoff = ts - timedelta(seconds=_CLAIMS_WINDOW_S)
    with _CLAIM_LOCK:
        while _CLAIM_TIMESTAMPS and _CLAIM_TIMESTAMPS[0] < cutoff:
            _CLAIM_TIMESTAMPS.popleft()
        _CLAIM_TIMESTAMPS.append(ts)


def claims_per_min(window_s: int, now: datetime | None = None) -> int:
    """Count claims observed in the last ``window_s`` seconds.

    The denominator collapses to "per minute" via a simple linear projection
    (count * 60 / window_s) so 1m and 5m can be compared directly.
    """
    ts = now or datetime.now(timezone.utc)
    cutoff = ts - timedelta(seconds=window_s)
    with _CLAIM_LOCK:
        while _CLAIM_TIMESTAMPS and _CLAIM_TIMESTAMPS[0] < cutoff - timedelta(seconds=_CLAIMS_WINDOW_S):
            _CLAIM_TIMESTAMPS.popleft()
        relevant = [t for t in _CLAIM_TIMESTAMPS if t >= cutoff]
    if not relevant or window_s <= 0:
        return 0
    return int(round(len(relevant) * 60.0 / window_s))


def reset_claim_counters_for_tests() -> None:
    """Drop the claims sliding window — only for tests."""
    global _ALERT_FIRED_TOTAL
    with _CLAIM_LOCK:
        _CLAIM_TIMESTAMPS.clear()
    with _ALERT_FIRED_LOCK:
        _ALERT_FIRED_TOTAL = 0


def record_alert_fired() -> None:
    """Increment the lifetime counter of low→high crossings.

    The enqueuer calls this on every threshold-crossing INSERT. The endpoint
    surfaces this as a monotonic counter (no time window) — operators
    typically chart the delta between scrapes, similar to a Prometheus counter.
    """
    global _ALERT_FIRED_TOTAL
    with _ALERT_FIRED_LOCK:
        _ALERT_FIRED_TOTAL += 1


def alert_fired_total() -> int:
    """Return the lifetime crossing counter (monotonic since process start)."""
    with _ALERT_FIRED_LOCK:
        return _ALERT_FIRED_TOTAL


async def get_queue_health(db: AsyncSession) -> dict[str, Any]:
    """Compose the full /api/v1/kg/queue/health payload.

    Returns the 13-key shape declared in the API contract (FR9 + TR16
    workers_draining_count). When the worker singleton hasn't been started
    yet (e.g. unit tests), worker counts default to 0.
    """
    from okto_pulse.core.infra.config import get_settings

    settings = get_settings()
    alert_threshold = settings.kg_queue_alert_threshold
    now = datetime.now(timezone.utc)

    queue_depth = await db.scalar(
        select(func.count()).where(ConsolidationQueue.status == "pending")
    ) or 0

    oldest_triggered = await db.scalar(
        select(func.min(ConsolidationQueue.triggered_at)).where(
            ConsolidationQueue.status == "pending",
        )
    )
    if oldest_triggered is not None:
        if oldest_triggered.tzinfo is None:
            oldest_triggered = oldest_triggered.replace(tzinfo=timezone.utc)
        oldest_pending_age_s = max(0.0, (now - oldest_triggered).total_seconds())
    else:
        oldest_pending_age_s = 0.0

    claimed_count = await db.scalar(
        select(func.count()).where(ConsolidationQueue.status == "claimed")
    ) or 0

    claimed_boards_result = await db.execute(
        select(distinct(ConsolidationQueue.board_id)).where(
            ConsolidationQueue.status == "claimed",
        )
    )
    claimed_boards = sorted(b for b in claimed_boards_result.scalars().all() if b)

    dead_letter_count = await db.scalar(
        select(func.count()).select_from(ConsolidationDeadLetter)
    ) or 0

    # Worker pool snapshot — gracefully degrades when the singleton is
    # absent or hasn't been started yet (e.g. unit tests with no lifespan).
    workers_active = 0
    workers_idle = 0
    workers_draining_count = 0
    try:
        from okto_pulse.core.kg.workers.consolidation import (
            get_consolidation_worker,
        )
        worker = get_consolidation_worker()
        snapshot = getattr(worker, "snapshot_pool", lambda: None)()
        if snapshot is not None:
            workers_active = int(snapshot.get("active", 0))
            workers_idle = int(snapshot.get("idle", 0))
            workers_draining_count = int(snapshot.get("draining", 0))
    except Exception:
        pass

    return {
        "queue_depth": int(queue_depth),
        "oldest_pending_age_s": round(oldest_pending_age_s, 3),
        "claimed_count": int(claimed_count),
        "claimed_boards": claimed_boards,
        "dead_letter_count": int(dead_letter_count),
        "claims_per_min_1m": claims_per_min(60, now=now),
        "claims_per_min_5m": claims_per_min(300, now=now),
        "alert_threshold": int(alert_threshold),
        "alert_active": int(queue_depth) >= int(alert_threshold),
        "alert_fired_total": alert_fired_total(),
        "workers_active": workers_active,
        "workers_idle": workers_idle,
        "workers_draining_count": workers_draining_count,
        "kuzu_lock_retries_5m": kuzu_lock_retries_5m(now=now),
    }
