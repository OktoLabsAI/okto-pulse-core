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


# ---------------------------------------------------------------------------
# R6-IMP2 — active-queue drill-down (ConsolidationQueue + global_update_outbox)
#
# "Active" = work the operational workers will still drain: ConsolidationQueue
# rows in pending/claimed + GlobalUpdateOutbox rows still in the retry window.
# DLQ / dead_letter / canonical debt are TERMINAL and deliberately EXCLUDED
# (their dedup/separation is R6-IMP5). Read-only: SQL aggregates only.
# ---------------------------------------------------------------------------

_ACTIVE_CQ_STATUSES = ("pending", "claimed")
# worst-wins ordering for the overall classification.
_ACTIVE_QUEUE_RANK = {"idle": 0, "transient": 1, "stuck": 2, "backpressure": 3}


def _age_seconds(oldest: datetime | None, now: datetime) -> float:
    if oldest is None:
        return 0.0
    if oldest.tzinfo is None:
        oldest = oldest.replace(tzinfo=timezone.utc)
    return max(0.0, (now - oldest).total_seconds())


def classify_active_queue(
    *, depth: int, oldest_age_s: float, alert_threshold: int, stuck_age_s: int,
) -> str:
    """transient | stuck | backpressure | idle (R6-IMP2).

    backpressure wins on volume (depth at/over the queue alert threshold); stuck
    on age (oldest active item older than the advisory stuck age); transient when
    active but under both thresholds; idle when empty."""
    if depth <= 0:
        return "idle"
    if depth >= alert_threshold:
        return "backpressure"
    if oldest_age_s >= stuck_age_s:
        return "stuck"
    return "transient"


def _active_queue_next_action(classification: str, worker_mode: str) -> str:
    """Suggested next action for the active-queue drill-down (SPEC4 card
    2e913ac3, AC ac_26acf1db) — actionable without local-file forensics."""
    if classification == "backpressure":
        return "investigate_backpressure_pause_writes_or_scale"
    if classification == "stuck":
        return (
            "start_consolidation_worker"
            if worker_mode == "stopped"
            else "inspect_stuck_queue_check_worker"
        )
    if classification == "transient":
        return "monitor_transient_inflight_work"
    return "none"


async def get_active_queue_drilldown(
    db: AsyncSession, board_id: str | None = None,
) -> dict[str, Any]:
    """Drill-down of the ACTIVE operational queue depth, split by source.

    Board-scoped when ``board_id`` is given (KG Health), else global (ops view).
    Reuses the existing primitives + the ``global_update_outbox`` retry-window
    definition from ``kg.health``; never creates a new queue/store. DLQ /
    canonical debt are NOT counted here (R6-IMP5 owns that separation)."""
    from okto_pulse.core.infra.config import get_settings
    from okto_pulse.core.kg.health import (
        DEAD_LETTER_RETRY_SENTINEL,
        MAX_OUTBOX_RETRIES,
    )
    from okto_pulse.core.models.db import GlobalUpdateOutbox

    settings = get_settings()
    alert_threshold = int(settings.kg_queue_alert_threshold)
    stuck_age_s = int(settings.kg_queue_stuck_age_seconds)
    now = datetime.now(timezone.utc)

    # worker_mode — derived from the consolidation worker singleton (no new state).
    worker_mode = "unknown"
    try:
        from okto_pulse.core.kg.workers.consolidation import (
            get_consolidation_worker,
        )
        worker = get_consolidation_worker()
        worker_mode = "running" if getattr(worker, "is_running", False) else "stopped"
    except Exception:
        worker_mode = "unknown"

    def _cq_where(*extra):
        clauses = list(extra)
        if board_id is not None:
            clauses.append(ConsolidationQueue.board_id == board_id)
        return clauses

    # --- Source 1: ConsolidationQueue (pending/claimed) ---
    cq_by_status: dict[str, int] = {}
    for status in _ACTIVE_CQ_STATUSES:
        cq_by_status[status] = int(await db.scalar(
            select(func.count()).where(*_cq_where(ConsolidationQueue.status == status))
        ) or 0)
    cq_depth = sum(cq_by_status.values())
    cat_rows = (await db.execute(
        select(ConsolidationQueue.artifact_type, func.count())
        .where(*_cq_where(ConsolidationQueue.status.in_(_ACTIVE_CQ_STATUSES)))
        .group_by(ConsolidationQueue.artifact_type)
    )).all()
    cq_by_category = {str(a or "unknown"): int(n) for a, n in cat_rows}
    cq_oldest = await db.scalar(
        select(func.min(ConsolidationQueue.triggered_at))
        .where(*_cq_where(ConsolidationQueue.status.in_(_ACTIVE_CQ_STATUSES)))
    )
    cq_age = _age_seconds(cq_oldest, now)
    cq_class = classify_active_queue(
        depth=cq_depth, oldest_age_s=cq_age,
        alert_threshold=alert_threshold, stuck_age_s=stuck_age_s,
    )

    # --- Source 2: GlobalUpdateOutbox (still in the retry window; dead_letter excluded) ---
    ob_active = [
        GlobalUpdateOutbox.processed_at.is_(None),
        GlobalUpdateOutbox.retry_count >= 0,
        GlobalUpdateOutbox.retry_count < MAX_OUTBOX_RETRIES,
        GlobalUpdateOutbox.retry_count != DEAD_LETTER_RETRY_SENTINEL,
    ]
    if board_id is not None:
        ob_active.append(GlobalUpdateOutbox.board_id == board_id)
    ob_depth = int(await db.scalar(select(func.count()).where(*ob_active)) or 0)
    ob_oldest = await db.scalar(
        select(func.min(GlobalUpdateOutbox.created_at)).where(*ob_active)
    )
    ob_age = _age_seconds(ob_oldest, now)
    ob_class = classify_active_queue(
        depth=ob_depth, oldest_age_s=ob_age,
        alert_threshold=alert_threshold, stuck_age_s=stuck_age_s,
    )

    total_active_depth = cq_depth + ob_depth
    overall = max((cq_class, ob_class), key=lambda c: _ACTIVE_QUEUE_RANK[c])

    return {
        "board_id": board_id,
        "worker_mode": worker_mode,
        "total_active_depth": total_active_depth,
        "classification": overall,
        # SPEC4 (card 2e913ac3, AC ac_26acf1db): bounded suggested next action so
        # the active-queue drill-down is actionable from the payload alone.
        "next_action": _active_queue_next_action(overall, worker_mode),
        "alert_threshold": alert_threshold,
        "stuck_age_seconds": stuck_age_s,
        "drill_down_tool": "okto_pulse_kg_queue_drilldown",
        "sources": [
            {
                "source": "consolidation_queue",
                "queue_depth": cq_depth,
                "by_status": cq_by_status,
                "by_category": cq_by_category,
                "oldest_age_seconds": round(cq_age, 3),
                "classification": cq_class,
            },
            {
                "source": "global_update_outbox",
                "queue_depth": ob_depth,
                "by_status": {"pending": ob_depth},
                "by_category": {},
                "oldest_age_seconds": round(ob_age, 3),
                "classification": ob_class,
            },
        ],
    }
