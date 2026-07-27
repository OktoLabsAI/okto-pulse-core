"""Memory pressure correlator (Spec KG-01, FR3 + TR3).

Deterministic evaluator that answers a single question:

    Is memory pressure the primary cause of the most recent WAL/commit
    failure on this graph?

The criterion is fixed by TR3 of KG-01 — it MUST NOT be subjective:

    high_water_mark_pct > 90% must have been observed in >=3 distinct
    samples within the 10 minutes immediately BEFORE a
    kg.wal.flush.failed or kg.commit.failed event for the same graph.

If the criterion holds we return CONFIRMED_PRIMARY_CAUSE. Otherwise we
return UNCONFIRMED (TR4: absence of correlation in instrumented runs must
prevent overfitting mitigations). The correlator never returns "possible"
or "likely" — the spec rejects fuzzy levels in favour of an auditable
binary plus a `correlation_id` you can join back to the underlying
samples and failure event in observability storage.

This module is pure: it operates on pre-collected samples/failures. The
KGHealthService wraps it with a memory-bounded ring buffer of recent
samples and the latest failure event from telemetry. Tests in
``tests/test_kg_health_state.py`` cover the confirmed/unconfirmed cases
end-to-end.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Iterable

# TR3: thresholds are NOT configurable in KG-01 because the criterion
# itself is part of the contract. Changing them is a spec change.
HIGH_WATER_MARK_THRESHOLD_PCT = 90.0
MIN_SAMPLES_BEFORE_FAILURE = 3
CORRELATION_WINDOW_MINUTES = 10


class MemoryPressureStatus(str, Enum):
    """Outcome of the correlator (FR3, TR3, TR4)."""

    UNCONFIRMED = "unconfirmed"
    CONFIRMED_PRIMARY_CAUSE = "confirmed_primary_cause"


@dataclass(frozen=True, slots=True)
class HighWaterMarkSample:
    """A single embedded graph backend high-water-mark observation."""

    timestamp: datetime
    high_water_mark_pct: float
    graph_type: str  # "board" or "discovery"


@dataclass(frozen=True, slots=True)
class FailureEvent:
    """A WAL/commit failure event surfaced by embedded graph backend instrumentation.

    `event_kind` MUST match one of the names the spec already documents
    in the telemetry boundary so observability storage can join correlator
    output to the underlying signal:

        - "kg.wal.flush.failed"
        - "kg.commit.failed"
    """

    timestamp: datetime
    event_kind: str
    graph_type: str
    correlation_id: str


@dataclass(frozen=True, slots=True)
class CorrelationResult:
    """Result of `MemoryPressureCorrelator.evaluate`."""

    status: MemoryPressureStatus
    failure_event: FailureEvent | None
    matched_samples_count: int
    window_started_at: datetime | None
    correlation_id: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class MemoryPressureCorrelator:
    """Deterministic correlator. No internal state, no thresholds to tune."""

    threshold_pct: float = HIGH_WATER_MARK_THRESHOLD_PCT
    min_samples: int = MIN_SAMPLES_BEFORE_FAILURE
    window_minutes: int = CORRELATION_WINDOW_MINUTES

    def evaluate(
        self,
        *,
        samples: Iterable[HighWaterMarkSample],
        failures: Iterable[FailureEvent],
    ) -> CorrelationResult:
        """Return a frozen `CorrelationResult` for the most recent failure.

        Algorithm:

        1. Pick the most recent failure event (by timestamp). If there
           is no failure, return UNCONFIRMED with reason="no_failures".
        2. Build the correlation window as
           [failure.timestamp - 10min, failure.timestamp).
        3. Count samples in that window for the same graph_type with
           high_water_mark_pct > 90%.
        4. If count >= 3 → CONFIRMED_PRIMARY_CAUSE with the failure's
           correlation_id and the actual matched count.
        5. Else → UNCONFIRMED. Reason carries the gap so the operator
           can see whether it was "not enough samples" or "no samples".
        """
        failures_list = sorted(failures, key=lambda f: f.timestamp)
        if not failures_list:
            return CorrelationResult(
                status=MemoryPressureStatus.UNCONFIRMED,
                failure_event=None,
                matched_samples_count=0,
                window_started_at=None,
                correlation_id=None,
                reason="no_failures",
            )

        failure = failures_list[-1]
        window_start = failure.timestamp - timedelta(minutes=self.window_minutes)
        matched = [
            s
            for s in samples
            if s.graph_type == failure.graph_type
            and window_start <= s.timestamp < failure.timestamp
            and s.high_water_mark_pct > self.threshold_pct
        ]

        if len(matched) >= self.min_samples:
            return CorrelationResult(
                status=MemoryPressureStatus.CONFIRMED_PRIMARY_CAUSE,
                failure_event=failure,
                matched_samples_count=len(matched),
                window_started_at=window_start,
                correlation_id=failure.correlation_id,
                reason=(
                    f"{len(matched)}_samples_over_"
                    f"{int(self.threshold_pct)}pct_in_"
                    f"{self.window_minutes}min_before_failure"
                ),
            )

        return CorrelationResult(
            status=MemoryPressureStatus.UNCONFIRMED,
            failure_event=failure,
            matched_samples_count=len(matched),
            window_started_at=window_start,
            correlation_id=failure.correlation_id,
            reason=(
                f"only_{len(matched)}_samples_over_{int(self.threshold_pct)}pct_"
                f"need_{self.min_samples}"
            ),
        )


__all__ = [
    "CORRELATION_WINDOW_MINUTES",
    "CorrelationResult",
    "FailureEvent",
    "HIGH_WATER_MARK_THRESHOLD_PCT",
    "HighWaterMarkSample",
    "MIN_SAMPLES_BEFORE_FAILURE",
    "MemoryPressureCorrelator",
    "MemoryPressureStatus",
]
