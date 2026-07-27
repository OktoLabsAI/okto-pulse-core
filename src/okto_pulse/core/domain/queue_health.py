"""Pure queue-health classification and operator-action policy."""

from __future__ import annotations

from datetime import datetime, timezone


ACTIVE_QUEUE_RANK = {"idle": 0, "transient": 1, "stuck": 2, "backpressure": 3}


def age_seconds(oldest: datetime | None, now: datetime) -> float:
    if oldest is None:
        return 0.0
    if oldest.tzinfo is None:
        oldest = oldest.replace(tzinfo=timezone.utc)
    return max(0.0, (now - oldest).total_seconds())


def classify_active_queue(
    *,
    depth: int,
    oldest_age_s: float,
    alert_threshold: int,
    stuck_age_s: int,
) -> str:
    if depth <= 0:
        return "idle"
    if depth >= alert_threshold:
        return "backpressure"
    if oldest_age_s >= stuck_age_s:
        return "stuck"
    return "transient"


def active_queue_next_action(classification: str, worker_mode: str) -> str:
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


def worst_active_queue_classification(*classifications: str) -> str:
    if not classifications:
        return "idle"
    return max(classifications, key=lambda value: ACTIVE_QUEUE_RANK[value])


__all__ = [
    "ACTIVE_QUEUE_RANK",
    "active_queue_next_action",
    "age_seconds",
    "classify_active_queue",
    "worst_active_queue_classification",
]
