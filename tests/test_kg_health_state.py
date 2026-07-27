"""KG-01 — Health state classifier + memory-pressure correlator (FR1, FR2, FR3, TR3, TR4).

Pure unit tests against the deterministic logic. No DB, no Kùzu, no
LadybugDB — these classes are intentionally side-effect free so the rules
encoded by the spec can be audited line-by-line.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from okto_pulse.core.kg.health_state import (
    DEFAULT_BUFFER_AT_RISK_PCT,
    GraphTelemetry,
    HealthState,
    KGHealthStateClassifier,
    LockState,
    MetricStatus,
)
from okto_pulse.core.kg.memory_pressure import (
    CORRELATION_WINDOW_MINUTES,
    FailureEvent,
    HIGH_WATER_MARK_THRESHOLD_PCT,
    HighWaterMarkSample,
    MemoryPressureCorrelator,
    MemoryPressureStatus,
)


def _ok_telemetry(graph_type: str = "board", high_water: float = 10.0) -> GraphTelemetry:
    return GraphTelemetry(
        graph_type=graph_type,
        buffer_utilization_pct=high_water,
        high_water_mark_pct=high_water,
        recent_buffer_errors=0,
        recent_wal_errors=0,
        recent_commit_errors=0,
    )


def _null_lock() -> LockState:
    return LockState(is_held=False, is_admin_lane=False, owner_token=None)


# --- FR1: state machine precedence ------------------------------------------------


def test_quarantine_wins_over_everything():
    classifier = KGHealthStateClassifier()
    result = classifier.evaluate(
        telemetries=[_ok_telemetry()],
        lock_state=LockState(is_held=True, is_admin_lane=True, owner_token="x"),
        quarantine_present=True,
        backpressure_rejecting=True,
    )
    assert result.state is HealthState.QUARANTINED
    assert "quarantine.present" in result.reasons


def test_wal_errors_yield_recovery_needed():
    classifier = KGHealthStateClassifier()
    t = GraphTelemetry(
        graph_type="board",
        buffer_utilization_pct=10.0,
        high_water_mark_pct=10.0,
        recent_buffer_errors=0,
        recent_wal_errors=1,
        recent_commit_errors=0,
    )
    result = classifier.evaluate(
        telemetries=[t],
        lock_state=_null_lock(),
        quarantine_present=False,
        backpressure_rejecting=False,
    )
    assert result.state is HealthState.RECOVERY_NEEDED
    assert "wal_or_commit_errors.present" in result.reasons


def test_admin_lane_lock_yields_backpressure():
    classifier = KGHealthStateClassifier()
    result = classifier.evaluate(
        telemetries=[_ok_telemetry()],
        lock_state=LockState(is_held=True, is_admin_lane=True, owner_token="x"),
        quarantine_present=False,
        backpressure_rejecting=False,
    )
    assert result.state is HealthState.BACKPRESSURE
    assert "lock.admin_lane.held" in result.reasons


def test_backpressure_rejecting_yields_backpressure_even_without_lock():
    classifier = KGHealthStateClassifier()
    result = classifier.evaluate(
        telemetries=[_ok_telemetry()],
        lock_state=_null_lock(),
        quarantine_present=False,
        backpressure_rejecting=True,
    )
    assert result.state is HealthState.BACKPRESSURE
    assert "backpressure.rejecting" in result.reasons


def test_high_water_mark_at_threshold_is_at_risk():
    classifier = KGHealthStateClassifier()
    t = _ok_telemetry(high_water=DEFAULT_BUFFER_AT_RISK_PCT)
    result = classifier.evaluate(
        telemetries=[t],
        lock_state=_null_lock(),
        quarantine_present=False,
        backpressure_rejecting=False,
    )
    assert result.state is HealthState.AT_RISK
    assert any("buffer.high_water_mark" in r for r in result.reasons)


def test_recent_buffer_errors_yield_at_risk():
    classifier = KGHealthStateClassifier()
    t = GraphTelemetry(
        graph_type="board",
        buffer_utilization_pct=10.0,
        high_water_mark_pct=10.0,
        recent_buffer_errors=1,
        recent_wal_errors=0,
        recent_commit_errors=0,
    )
    result = classifier.evaluate(
        telemetries=[t],
        lock_state=_null_lock(),
        quarantine_present=False,
        backpressure_rejecting=False,
    )
    assert result.state is HealthState.AT_RISK
    assert "buffer.recent_errors>0" in result.reasons


def test_clean_state_yields_healthy():
    classifier = KGHealthStateClassifier()
    result = classifier.evaluate(
        telemetries=[_ok_telemetry("board"), _ok_telemetry("discovery")],
        lock_state=_null_lock(),
        quarantine_present=False,
        backpressure_rejecting=False,
    )
    assert result.state is HealthState.HEALTHY
    assert result.metric_status is MetricStatus.AVAILABLE
    assert result.reasons == tuple()


# --- FR2 / BR "Health unavailable is not zero" -----------------------------------


def test_all_metrics_none_yields_unavailable_and_at_risk():
    """BR: cannot emit HEALTHY when telemetry is unreadable."""
    classifier = KGHealthStateClassifier()
    blind = GraphTelemetry(
        graph_type="board",
        buffer_utilization_pct=None,
        high_water_mark_pct=None,
        recent_buffer_errors=None,
        recent_wal_errors=None,
        recent_commit_errors=None,
    )
    result = classifier.evaluate(
        telemetries=[blind],
        lock_state=_null_lock(),
        quarantine_present=False,
        backpressure_rejecting=False,
    )
    assert result.metric_status is MetricStatus.UNAVAILABLE
    assert result.state is HealthState.AT_RISK
    assert "metric.unavailable" in result.reasons


def test_some_metrics_none_yields_partial():
    classifier = KGHealthStateClassifier()
    partial = GraphTelemetry(
        graph_type="board",
        buffer_utilization_pct=10.0,
        high_water_mark_pct=None,
        recent_buffer_errors=0,
        recent_wal_errors=0,
        recent_commit_errors=0,
    )
    result = classifier.evaluate(
        telemetries=[partial],
        lock_state=_null_lock(),
        quarantine_present=False,
        backpressure_rejecting=False,
    )
    assert result.metric_status is MetricStatus.PARTIAL


# --- FR3 / TR3: memory-pressure correlator ---------------------------------------


def _now() -> datetime:
    return datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)


def test_no_failures_returns_unconfirmed_no_failures():
    result = MemoryPressureCorrelator().evaluate(samples=[], failures=[])
    assert result.status is MemoryPressureStatus.UNCONFIRMED
    assert result.reason == "no_failures"
    assert result.correlation_id is None


def test_three_samples_over_threshold_in_window_yields_confirmed():
    failure_at = _now()
    failure = FailureEvent(
        timestamp=failure_at,
        event_kind="kg.wal.flush.failed",
        graph_type="board",
        correlation_id="corr-1",
    )
    samples = [
        HighWaterMarkSample(
            timestamp=failure_at - timedelta(minutes=i),
            high_water_mark_pct=95.0,
            graph_type="board",
        )
        for i in (1, 2, 3)
    ]
    result = MemoryPressureCorrelator().evaluate(samples=samples, failures=[failure])
    assert result.status is MemoryPressureStatus.CONFIRMED_PRIMARY_CAUSE
    assert result.matched_samples_count == 3
    assert result.correlation_id == "corr-1"
    assert result.failure_event is failure


def test_two_samples_over_threshold_yields_unconfirmed():
    failure_at = _now()
    failure = FailureEvent(
        timestamp=failure_at,
        event_kind="kg.commit.failed",
        graph_type="board",
        correlation_id="corr-2",
    )
    samples = [
        HighWaterMarkSample(
            timestamp=failure_at - timedelta(minutes=i),
            high_water_mark_pct=95.0,
            graph_type="board",
        )
        for i in (1, 2)
    ]
    result = MemoryPressureCorrelator().evaluate(samples=samples, failures=[failure])
    assert result.status is MemoryPressureStatus.UNCONFIRMED
    assert result.matched_samples_count == 2
    assert "only_2_samples" in result.reason
    assert result.correlation_id == "corr-2"


def test_samples_outside_window_do_not_count():
    failure_at = _now()
    failure = FailureEvent(
        timestamp=failure_at,
        event_kind="kg.wal.flush.failed",
        graph_type="board",
        correlation_id="corr-3",
    )
    samples = [
        HighWaterMarkSample(
            timestamp=failure_at - timedelta(minutes=CORRELATION_WINDOW_MINUTES + 1),
            high_water_mark_pct=99.0,
            graph_type="board",
        ),
        HighWaterMarkSample(
            timestamp=failure_at - timedelta(minutes=CORRELATION_WINDOW_MINUTES + 2),
            high_water_mark_pct=99.0,
            graph_type="board",
        ),
        HighWaterMarkSample(
            timestamp=failure_at - timedelta(minutes=CORRELATION_WINDOW_MINUTES + 3),
            high_water_mark_pct=99.0,
            graph_type="board",
        ),
    ]
    result = MemoryPressureCorrelator().evaluate(samples=samples, failures=[failure])
    assert result.status is MemoryPressureStatus.UNCONFIRMED
    assert result.matched_samples_count == 0


def test_samples_for_different_graph_do_not_count():
    failure_at = _now()
    failure = FailureEvent(
        timestamp=failure_at,
        event_kind="kg.commit.failed",
        graph_type="board",
        correlation_id="corr-4",
    )
    samples = [
        HighWaterMarkSample(
            timestamp=failure_at - timedelta(minutes=i),
            high_water_mark_pct=95.0,
            graph_type="discovery",
        )
        for i in (1, 2, 3)
    ]
    result = MemoryPressureCorrelator().evaluate(samples=samples, failures=[failure])
    assert result.status is MemoryPressureStatus.UNCONFIRMED


def test_threshold_strict_inequality():
    """Spec: high_water_mark_pct > 90% (strict). 90.0 must NOT count."""
    failure_at = _now()
    failure = FailureEvent(
        timestamp=failure_at,
        event_kind="kg.wal.flush.failed",
        graph_type="board",
        correlation_id="corr-5",
    )
    samples = [
        HighWaterMarkSample(
            timestamp=failure_at - timedelta(minutes=i),
            high_water_mark_pct=HIGH_WATER_MARK_THRESHOLD_PCT,
            graph_type="board",
        )
        for i in (1, 2, 3)
    ]
    result = MemoryPressureCorrelator().evaluate(samples=samples, failures=[failure])
    assert result.status is MemoryPressureStatus.UNCONFIRMED
    assert result.matched_samples_count == 0


def test_most_recent_failure_is_picked():
    earlier = FailureEvent(
        timestamp=_now() - timedelta(hours=1),
        event_kind="kg.commit.failed",
        graph_type="board",
        correlation_id="corr-old",
    )
    latest = FailureEvent(
        timestamp=_now(),
        event_kind="kg.wal.flush.failed",
        graph_type="board",
        correlation_id="corr-new",
    )
    result = MemoryPressureCorrelator().evaluate(
        samples=[], failures=[earlier, latest]
    )
    assert result.failure_event is latest
    assert result.correlation_id == "corr-new"
