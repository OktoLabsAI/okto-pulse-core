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

from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value

from okto_pulse.core.domain.queue_health import (
    active_queue_next_action as _active_queue_next_action,
    age_seconds as _age_seconds,
    classify_active_queue,
    worst_active_queue_classification,
)
from okto_pulse.core.kg.commit_coordinator import kuzu_lock_retries_5m
from okto_pulse.core.ports.queue_health import (
    get_queue_health_read_port,
)


_CLAIMS_WINDOW_S = 300  # keep enough history for both 1m and 5m views
_CLAIM_TIMESTAMPS: deque[datetime] = deque()
_CLAIM_LOCK = threading.Lock()

_ALERT_FIRED_LOCK = threading.Lock()
_ALERT_FIRED_KEY = "services.queue_health.alert_fired_total"


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
    with _CLAIM_LOCK:
        _CLAIM_TIMESTAMPS.clear()
    with _ALERT_FIRED_LOCK:
        reset_runtime_values(_ALERT_FIRED_KEY)


def record_alert_fired() -> None:
    """Increment the lifetime counter of low→high crossings.

    The enqueuer calls this on every threshold-crossing INSERT. The endpoint
    surfaces this as a monotonic counter (no time window) — operators
    typically chart the delta between scrapes, similar to a Prometheus counter.
    """
    with _ALERT_FIRED_LOCK:
        total = int(resolve_runtime_value(_ALERT_FIRED_KEY) or 0)
        register_runtime_value(_ALERT_FIRED_KEY, total + 1)


def alert_fired_total() -> int:
    """Return the lifetime crossing counter (monotonic since process start)."""
    with _ALERT_FIRED_LOCK:
        return int(resolve_runtime_value(_ALERT_FIRED_KEY) or 0)


async def get_queue_health(db: object) -> dict[str, Any]:
    """Compose the full /api/v1/kg/queue/health payload.

    Returns the 13-key shape declared in the API contract (FR9 + TR16
    workers_draining_count). When the worker singleton hasn't been started
    yet (e.g. unit tests), worker counts default to 0.
    """
    from okto_pulse.core.infra.config import get_settings

    settings = get_settings()
    alert_threshold = settings.kg_queue_alert_threshold
    now = datetime.now(timezone.utc)

    storage = await get_queue_health_read_port().health_snapshot(db)
    queue_depth = storage.queue_depth
    oldest_triggered = storage.oldest_pending_at
    if oldest_triggered is not None:
        if oldest_triggered.tzinfo is None:
            oldest_triggered = oldest_triggered.replace(tzinfo=timezone.utc)
        oldest_pending_age_s = max(0.0, (now - oldest_triggered).total_seconds())
    else:
        oldest_pending_age_s = 0.0

    claimed_count = storage.claimed_count
    claimed_boards = list(storage.claimed_boards)
    dead_letter_count = storage.dead_letter_count

    # Worker pool snapshot — gracefully degrades when the singleton is
    # absent or hasn't been started yet (e.g. unit tests with no lifespan).
    workers_active = 0
    workers_idle = 0
    workers_draining_count = 0
    try:
        from okto_pulse.core.application.runtime_workers import (
            runtime_worker_snapshot,
        )
        snapshot = runtime_worker_snapshot("consolidation_worker")
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


async def get_active_queue_drilldown(
    db: object, board_id: str | None = None,
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
    settings = get_settings()
    alert_threshold = int(settings.kg_queue_alert_threshold)
    stuck_age_s = int(settings.kg_queue_stuck_age_seconds)
    now = datetime.now(timezone.utc)

    # worker_mode comes from the active app-scoped runner registry.
    worker_mode = "unknown"
    try:
        from okto_pulse.core.application.runtime_workers import (
            runtime_worker_is_running,
        )
        worker_mode = (
            "running"
            if runtime_worker_is_running("consolidation_worker")
            else "stopped"
        )
    except Exception:
        worker_mode = "unknown"

    storage = await get_queue_health_read_port().active_snapshot(
        db,
        board_id=board_id,
        active_statuses=_ACTIVE_CQ_STATUSES,
        max_outbox_retries=MAX_OUTBOX_RETRIES,
        dead_letter_retry_sentinel=DEAD_LETTER_RETRY_SENTINEL,
    )

    # --- Source 1: ConsolidationQueue (pending/claimed) ---
    cq_by_status = storage.consolidation_by_status
    cq_depth = sum(cq_by_status.values())
    cq_by_category = storage.consolidation_by_category
    cq_oldest = storage.consolidation_oldest_at
    cq_age = _age_seconds(cq_oldest, now)
    cq_class = classify_active_queue(
        depth=cq_depth, oldest_age_s=cq_age,
        alert_threshold=alert_threshold, stuck_age_s=stuck_age_s,
    )

    # --- Source 2: GlobalUpdateOutbox (still in the retry window; dead_letter excluded) ---
    ob_depth = storage.outbox_depth
    ob_oldest = storage.outbox_oldest_at
    ob_age = _age_seconds(ob_oldest, now)
    ob_class = classify_active_queue(
        depth=ob_depth, oldest_age_s=ob_age,
        alert_threshold=alert_threshold, stuck_age_s=stuck_age_s,
    )

    total_active_depth = cq_depth + ob_depth
    overall = worst_active_queue_classification(cq_class, ob_class)

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
