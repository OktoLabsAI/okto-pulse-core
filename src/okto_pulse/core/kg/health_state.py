"""KG Health state machine + telemetry classifier (Spec KG-01, FR1 + FR2).

Maps the live telemetry, errors and lock state of `graph.lbug` and
`discovery.lbug` to the canonical 5-state machine defined in FR1:

    healthy | at_risk | backpressure | recovery_needed | quarantined

`metric_status` (FR2 / BR "Health unavailable is not zero") distinguishes
telemetry that is genuinely zero from telemetry that could not be read.
This is what allows the KG Health UI to surface `at_risk` BEFORE the
storage corrupts, instead of waiting for a recovery-needed event.

The classifier is intentionally pure: it accepts the inputs it needs and
returns a deterministic decision plus the reasons that drove it. The
`KGHealthService` wraps this classifier and supplies the inputs from
SQL, LadybugDB telemetry and the cross-process advisory lock.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable

# Threshold used by FR3 / TR3 for "buffer pressure" signal. Kept here so
# both the classifier (at_risk transition) and the MemoryPressureCorrelator
# (confirmation criterion) share one number. Override per board via the
# `at_risk_buffer_pct` argument of evaluate().
DEFAULT_BUFFER_AT_RISK_PCT = 80.0


class HealthState(str, Enum):
    """Canonical KG Health state machine (FR1)."""

    HEALTHY = "healthy"
    AT_RISK = "at_risk"
    BACKPRESSURE = "backpressure"
    RECOVERY_NEEDED = "recovery_needed"
    QUARANTINED = "quarantined"


class MetricStatus(str, Enum):
    """Whether the telemetry could be read (FR2, BR `Health unavailable is not zero`)."""

    AVAILABLE = "available"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class GraphTelemetry:
    """Minimum signal set classifier needs about graph.lbug / discovery.lbug.

    Any field set to None means the metric could not be read — the
    classifier turns this into `metric_status=unavailable` rather than
    silently treating it as zero (BR "Health unavailable is not zero").
    """

    graph_type: str  # "board" or "discovery"
    buffer_utilization_pct: float | None
    high_water_mark_pct: float | None
    recent_buffer_errors: int | None
    recent_wal_errors: int | None
    recent_commit_errors: int | None


@dataclass(frozen=True, slots=True)
class LockState:
    """Snapshot of the cross-process single-writer lock for the board.

    `is_admin_lane` is True when a rebuild/reset operation owns the lock
    via the administrative lane defined in TR7. The classifier treats this
    as `backpressure` for callers (writes drained) but never `at_risk`.
    """

    is_held: bool
    is_admin_lane: bool
    owner_token: str | None


@dataclass(frozen=True, slots=True)
class HealthClassification:
    """Frozen result of `KGHealthStateClassifier.evaluate` (FR1 + FR2)."""

    state: HealthState
    metric_status: MetricStatus
    reasons: tuple[str, ...] = field(default_factory=tuple)
    correlation_id: str | None = None


def _classify_metric_status(telemetries: Iterable[GraphTelemetry]) -> MetricStatus:
    """Compute the metric_status across graph.lbug + discovery.lbug.

    Rule (FR2): if every numeric field on every telemetry is None →
    UNAVAILABLE; if SOME but not all → PARTIAL; otherwise AVAILABLE. This
    is what stops the service from degrading silently to zero on a
    transient LadybugDB open failure.
    """
    has_any = False
    has_missing = False
    for telemetry in telemetries:
        for value in (
            telemetry.buffer_utilization_pct,
            telemetry.high_water_mark_pct,
            telemetry.recent_buffer_errors,
            telemetry.recent_wal_errors,
            telemetry.recent_commit_errors,
        ):
            if value is None:
                has_missing = True
            else:
                has_any = True
    if not has_any:
        return MetricStatus.UNAVAILABLE
    if has_missing:
        return MetricStatus.PARTIAL
    return MetricStatus.AVAILABLE


@dataclass(frozen=True, slots=True)
class KGHealthStateClassifier:
    """Stateless classifier mapping telemetry/errors/lock → HealthState.

    Order of precedence (highest first):

    1. QUARANTINED — any graph file already in quarantine (corruption
       already detected; recovery service must clear it).
    2. RECOVERY_NEEDED — WAL/commit errors seen recently AND no
       quarantine; the storage is degraded and needs operator action.
    3. BACKPRESSURE — single-writer lock held by admin lane OR caller
       hit the bounded queue's reject threshold.
    4. AT_RISK — telemetry is available but `high_water_mark_pct` is
       at/above `at_risk_buffer_pct` (FR3 hypothesis surface) OR
       `recent_buffer_errors > 0`. This is the new state added by
       KG-01 — it MUST appear before recovery_needed when memory
       pressure rises, never after.
    5. HEALTHY — nothing above triggered.

    Notes:
    - If `metric_status=UNAVAILABLE`, the classifier downgrades to
      AT_RISK (never HEALTHY) because the absence of signal is itself a
      risk indicator. BR "Health unavailable is not zero" forbids
      emitting HEALTHY when telemetry can't be read.
    """

    at_risk_buffer_pct: float = DEFAULT_BUFFER_AT_RISK_PCT

    def evaluate(
        self,
        *,
        telemetries: Iterable[GraphTelemetry],
        lock_state: LockState,
        quarantine_present: bool,
        backpressure_rejecting: bool,
        correlation_id: str | None = None,
    ) -> HealthClassification:
        telemetries = tuple(telemetries)
        metric_status = _classify_metric_status(telemetries)
        reasons: list[str] = []

        if quarantine_present:
            reasons.append("quarantine.present")
            return HealthClassification(
                state=HealthState.QUARANTINED,
                metric_status=metric_status,
                reasons=tuple(reasons),
                correlation_id=correlation_id,
            )

        wal_or_commit_errors = any(
            (t.recent_wal_errors or 0) > 0 or (t.recent_commit_errors or 0) > 0
            for t in telemetries
        )
        if wal_or_commit_errors:
            reasons.append("wal_or_commit_errors.present")
            return HealthClassification(
                state=HealthState.RECOVERY_NEEDED,
                metric_status=metric_status,
                reasons=tuple(reasons),
                correlation_id=correlation_id,
            )

        if backpressure_rejecting or (lock_state.is_held and lock_state.is_admin_lane):
            if backpressure_rejecting:
                reasons.append("backpressure.rejecting")
            if lock_state.is_held and lock_state.is_admin_lane:
                reasons.append("lock.admin_lane.held")
            return HealthClassification(
                state=HealthState.BACKPRESSURE,
                metric_status=metric_status,
                reasons=tuple(reasons),
                correlation_id=correlation_id,
            )

        buffer_pressure = any(
            (t.high_water_mark_pct or 0) >= self.at_risk_buffer_pct
            for t in telemetries
        )
        buffer_errors = any((t.recent_buffer_errors or 0) > 0 for t in telemetries)
        if buffer_pressure or buffer_errors or metric_status == MetricStatus.UNAVAILABLE:
            if buffer_pressure:
                reasons.append(f"buffer.high_water_mark>={self.at_risk_buffer_pct}")
            if buffer_errors:
                reasons.append("buffer.recent_errors>0")
            if metric_status == MetricStatus.UNAVAILABLE:
                reasons.append("metric.unavailable")
            return HealthClassification(
                state=HealthState.AT_RISK,
                metric_status=metric_status,
                reasons=tuple(reasons),
                correlation_id=correlation_id,
            )

        return HealthClassification(
            state=HealthState.HEALTHY,
            metric_status=metric_status,
            reasons=tuple(),
            correlation_id=correlation_id,
        )


__all__ = [
    "DEFAULT_BUFFER_AT_RISK_PCT",
    "GraphTelemetry",
    "HealthClassification",
    "HealthState",
    "KGHealthStateClassifier",
    "LockState",
    "MetricStatus",
]
