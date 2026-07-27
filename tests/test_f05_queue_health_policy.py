from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.domain.queue_health import (
    active_queue_next_action,
    age_seconds,
    classify_active_queue,
    worst_active_queue_classification,
)


@pytest.mark.parametrize(
    ("depth", "age", "expected"),
    [(0, 999, "idle"), (3, 10, "transient"), (3, 60, "stuck"), (10, 1, "backpressure")],
)
def test_f05_queue_classification_is_pure(
    depth: int, age: float, expected: str
) -> None:
    assert classify_active_queue(
        depth=depth,
        oldest_age_s=age,
        alert_threshold=10,
        stuck_age_s=60,
    ) == expected


def test_f05_queue_worst_wins_and_action_is_worker_aware() -> None:
    assert worst_active_queue_classification("transient", "backpressure", "stuck") == (
        "backpressure"
    )
    assert active_queue_next_action("stuck", "stopped") == (
        "start_consolidation_worker"
    )
    assert active_queue_next_action("stuck", "running") == (
        "inspect_stuck_queue_check_worker"
    )


def test_f05_queue_age_normalizes_naive_timestamps() -> None:
    now = datetime(2026, 7, 11, tzinfo=timezone.utc)
    assert age_seconds((now - timedelta(seconds=5)).replace(tzinfo=None), now) == 5
