"""Live consolidation queue health metrics for /api/v1/kg/queue/health.

Spec bdcda842 (FR9, TR13). Combines:
    * SQL aggregations against ConsolidationQueue + ConsolidationDeadLetter
      (depth, oldest pending age, claimed count, claimed_boards set, DLQ size).
    * In-process sliding-window counters for ``claims_per_min_1m`` /
      ``claims_per_min_5m`` populated by the consolidation worker.
    * Worker pool snapshot (active/idle/draining counts) populated by the
      worker singleton.
    * Cross-process graph backend file-lock retry counter exposed by
      ``commit_coordinator.graph_lock_retries_5m``.

The endpoint is read-only: it touches SQLite for queue stats but does not
hit graph backend (alert_active is computed on-read from queue_depth + alert_threshold).
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
    runtime_lock,
    runtime_state,
)

from okto_pulse.core.domain.queue_health import (
    ACTIVE_QUEUE_RANK,
    active_queue_next_action as _active_queue_next_action,
    age_seconds as _age_seconds,
    classify_active_queue,
    worst_active_queue_classification,
)
from okto_pulse.core.kg.commit_coordinator import graph_lock_retries_5m
from okto_pulse.core.ports.queue_health import (
    get_queue_health_read_port,
)


_CLAIMS_WINDOW_S = 300  # keep enough history for both 1m and 5m views
_CLAIM_TIMESTAMPS = runtime_state("services.queue_health.claim_timestamps", deque)
_CLAIM_LOCK = runtime_lock("services.queue_health.claim_timestamps")

_ALERT_FIRED_LOCK = runtime_lock("services.queue_health.alert_fired")
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
    global_outbox_dead_letter = await get_global_outbox_dead_letter_drilldown(
        db,
        limit=0,
    )

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
        "global_outbox_dead_letter_count": int(
            global_outbox_dead_letter["total_count"]
        ),
        "claims_per_min_1m": claims_per_min(60, now=now),
        "claims_per_min_5m": claims_per_min(300, now=now),
        "alert_threshold": int(alert_threshold),
        "alert_active": int(queue_depth) >= int(alert_threshold),
        "alert_fired_total": alert_fired_total(),
        "workers_active": workers_active,
        "workers_idle": workers_idle,
        "workers_draining_count": workers_draining_count,
        "graph_lock_retries_5m": graph_lock_retries_5m(now=now),
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
_ACTIVE_SOURCE_WORKERS = {
    "consolidation_queue": "consolidation_worker",
    "global_update_outbox": "outbox_worker",
}
_WORKER_MODE_RANK = {"running": 0, "unknown": 1, "stopped": 2}
_GLOBAL_OUTBOX_DLQ_LIMIT_MAX = 200
_GLOBAL_OPEN_ERROR_MARKERS = (
    "graph_corruption",
    "graph_unavailable",
    "graph_lock_contention",
    # The Community embedded backend can surface allocation failure without a
    # GraphError wrapper while opening a corrupt discovery artifact.
    "memoryerror",
    "bad allocation",
)


def _runtime_worker_mode(worker_family: str) -> str:
    """Return a bounded app-runner state for one queue source."""

    try:
        from okto_pulse.core.application.runtime_workers import (
            runtime_worker_is_running,
        )

        return "running" if runtime_worker_is_running(worker_family) else "stopped"
    except Exception:
        return "unknown"


def _source_next_action(
    source: str,
    classification: str,
    worker_mode: str,
) -> str:
    """Derive an action from the worker that actually drains ``source``."""

    if classification == "stuck":
        if worker_mode == "unknown":
            return f"inspect_{_ACTIVE_SOURCE_WORKERS[source]}_state"
        if source == "global_update_outbox":
            return (
                "start_outbox_worker"
                if worker_mode == "stopped"
                else "inspect_stuck_outbox_check_worker"
            )
    return _active_queue_next_action(classification, worker_mode)


def _bounded_error(value: str | None, *, max_chars: int = 240) -> str | None:
    from okto_pulse.core.services.kg_health_service import safe_health_error

    return safe_health_error(
        value,
        sensitive_reason="global_outbox_error_redacted",
        max_chars=max_chars,
    )


def _classify_global_outbox_dead_letter(last_error: str | None) -> str:
    from okto_pulse.core.application.global_outbox_dead_letter import (
        classify_global_outbox_dead_letter,
    )

    return classify_global_outbox_dead_letter(last_error)


def _global_outbox_dead_letter_next_action(classification: str) -> str:
    if classification == "global_open_failure":
        return "inspect_global_discovery_health_before_requeue"
    if classification == "board_source_failure":
        return "inspect_board_source_graph"
    return "inspect_global_outbox_event_failure"


async def get_global_outbox_dead_letter_drilldown(
    db: object,
    board_id: str | None = None,
    *,
    limit: int = 50,
) -> dict[str, Any]:
    """Return a bounded, read-only view of terminal global-outbox rows.

    This is deliberately separate from :func:`get_active_queue_drilldown`:
    terminal rows cannot be drained by the active retry-window worker and must
    never inflate ``total_active_depth``.
    """

    from okto_pulse.core.kg.health import (
        DEAD_LETTER_RETRY_SENTINEL,
        MAX_OUTBOX_RETRIES,
    )

    bounded_limit = max(0, min(int(limit), _GLOBAL_OUTBOX_DLQ_LIMIT_MAX))
    storage = await get_queue_health_read_port().global_outbox_dead_letter_snapshot(
        db,
        board_id=board_id,
        limit=bounded_limit,
        max_outbox_retries=MAX_OUTBOX_RETRIES,
        dead_letter_retry_sentinel=DEAD_LETTER_RETRY_SENTINEL,
    )
    now = datetime.now(timezone.utc)
    items = []
    for row in storage.rows:
        classification = _classify_global_outbox_dead_letter(row.last_error)
        items.append(
            {
                "event_id": row.event_id,
                "board_id": row.board_id,
                "event_type": row.event_type,
                "retry_count": row.retry_count,
                "created_at": row.created_at.isoformat(),
                "age_seconds": round(_age_seconds(row.created_at, now), 3),
                "last_error": _bounded_error(row.last_error),
                "classification": classification,
                "next_action": _global_outbox_dead_letter_next_action(
                    classification
                ),
            }
        )
    return {
        "domain": "global_outbox_dead_letter",
        "semantics": "terminal_global_discovery_delivery_failure",
        "board_id": board_id,
        "read_only": True,
        "total_count": int(storage.total_count),
        "returned_count": len(items),
        "limit": bounded_limit,
        "truncated": int(storage.total_count) > len(items),
        "oldest_age_seconds": round(
            _age_seconds(storage.oldest_created_at, now), 3
        ),
        "items": items,
    }


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

    # Each source has a different app-scoped runner. Reporting one shared mode
    # hid an outbox worker outage behind a healthy consolidation worker.
    worker_modes = {
        source: _runtime_worker_mode(worker)
        for source, worker in _ACTIVE_SOURCE_WORKERS.items()
    }

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

    sources = [
        {
            "source": "consolidation_queue",
            "worker_family": _ACTIVE_SOURCE_WORKERS["consolidation_queue"],
            "worker_mode": worker_modes["consolidation_queue"],
            "queue_depth": cq_depth,
            "by_status": cq_by_status,
            "by_category": cq_by_category,
            "oldest_age_seconds": round(cq_age, 3),
            "classification": cq_class,
            "next_action": _source_next_action(
                "consolidation_queue",
                cq_class,
                worker_modes["consolidation_queue"],
            ),
        },
        {
            "source": "global_update_outbox",
            "worker_family": _ACTIVE_SOURCE_WORKERS["global_update_outbox"],
            "worker_mode": worker_modes["global_update_outbox"],
            "queue_depth": ob_depth,
            "by_status": {"pending": ob_depth},
            "by_category": {},
            "oldest_age_seconds": round(ob_age, 3),
            "classification": ob_class,
            "next_action": _source_next_action(
                "global_update_outbox",
                ob_class,
                worker_modes["global_update_outbox"],
            ),
        },
    ]
    total_active_depth = cq_depth + ob_depth
    overall = worst_active_queue_classification(cq_class, ob_class)
    worst_source = max(
        sources,
        key=lambda item: (
            ACTIVE_QUEUE_RANK[item["classification"]],
            _WORKER_MODE_RANK[item["worker_mode"]],
            item["oldest_age_seconds"],
            item["queue_depth"],
            item["source"],
        ),
    )
    diagnostic_reason = {
        "idle": "no_active_work",
        "transient": "active_work_within_operational_thresholds",
        "stuck": "oldest_active_item_exceeds_stuck_threshold",
        "backpressure": "source_depth_reaches_alert_threshold",
    }[worst_source["classification"]]

    return {
        "board_id": board_id,
        # Backward-compatible scalar now follows the worst source rather than
        # incorrectly mirroring the consolidation worker for both sources.
        "worker_mode": worst_source["worker_mode"],
        "worker_modes": worker_modes,
        "total_active_depth": total_active_depth,
        "classification": overall,
        # SPEC4 (card 2e913ac3, AC ac_26acf1db): bounded suggested next action so
        # the active-queue drill-down is actionable from the payload alone.
        "next_action": worst_source["next_action"],
        "diagnostic": {
            "bounded": True,
            "worst_source": worst_source["source"],
            "classification": worst_source["classification"],
            "worker_mode": worst_source["worker_mode"],
            "reason": diagnostic_reason,
        },
        "alert_threshold": alert_threshold,
        "stuck_age_seconds": stuck_age_s,
        "drill_down_tool": "okto_pulse_kg_queue_drilldown",
        "sources": sources,
    }
