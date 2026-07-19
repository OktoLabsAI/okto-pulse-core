"""Bounded observability for truthful KG materialization health.

The health response remains the source of truth for callers. This module adds
dependency-free operational counters and structured audit events for the four
signals owned by spec c04639a2: classification, bounded probe failures,
first-write convergence, and the read-side filesystem mutation guard.

High-cardinality or local values never become metric labels. Board IDs and
generation/path fingerprints are retained only in the capped diagnostic
samples and structured logs.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

from okto_pulse.core.observability.sample_buffer import (
    runtime_counter_sample_buffer,
)
from okto_pulse.core.runtime_context import runtime_lock, runtime_state


FIRST_WRITE_CONVERGENCE_DEADLINE_SECONDS = 10.0
_MAX_PENDING_FIRST_WRITES = 1024

_CLASSIFICATION_METRIC = "kg_health_materialization_classification_total"
_PROBE_AUDIT_METRIC = "kg_health_materialization_probe_audit_total"
_CONVERGENCE_METRIC = "kg_health_first_write_convergence_total"
_MUTATION_GUARD_METRIC = "kg_health_read_side_mutation_guard_total"

_logger = logging.getLogger("okto_pulse.kg.materialization_health.observability")

_classification_samples = runtime_counter_sample_buffer(
    "kg.materialization_health.classification",
    ("classification", "materialization_state", "metric_status"),
)
_probe_audit_samples = runtime_counter_sample_buffer(
    "kg.materialization_health.probe_audit",
    ("audit_kind", "outcome"),
)
_convergence_samples = runtime_counter_sample_buffer(
    "kg.materialization_health.first_write_convergence",
    ("outcome",),
    sum_fields=("elapsed_ms",),
)
_mutation_guard_samples = runtime_counter_sample_buffer(
    "kg.materialization_health.mutation_guard",
    ("outcome",),
    sum_fields=("changed_path_count",),
)

_pending_first_writes = runtime_state(
    "kg.materialization_health.pending_first_writes",
    dict,
)
_observability_lock = runtime_lock("kg.materialization_health.observability")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _classification_bucket(
    *,
    materialization_state: str,
    metric_status: str,
    classification_reason: str,
    probe_reason_codes: Mapping[str, str],
) -> str:
    reasons = " ".join(
        (
            str(classification_reason),
            *(str(value) for value in probe_reason_codes.values()),
        )
    ).lower()
    if "materialization_generation_changed" in reasons:
        return "generation_race"
    if "timeout" in reasons or "deadline" in reasons:
        return "deadline_unavailable"
    if (
        materialization_state == "not_materialized"
        and classification_reason == "empty_board_not_materialized"
        and metric_status == "available"
    ):
        return "confirmed_empty"
    if materialization_state == "materialized":
        return "materialized"
    if metric_status == "unavailable":
        return "unavailable"
    return "unknown"


def _probe_audit_kinds(
    *,
    classification_reason: str,
    probe_reason_codes: Mapping[str, str],
) -> tuple[tuple[str, str], ...]:
    values = (classification_reason, *probe_reason_codes.values())
    normalized = tuple(str(value).lower() for value in values)
    audits: list[tuple[str, str]] = []
    if any("timeout" in value or "deadline" in value for value in normalized):
        audits.append(("deadline", "fail_closed"))
    if any("materialization_generation_changed" in value for value in normalized):
        audits.append(("generation_race", "fail_closed"))
    return tuple(audits)


def _record_probe_audit(
    *,
    board_id: str,
    audit_kind: str,
    outcome: str,
    classification_reason: str,
    probe_reason_codes: Mapping[str, str],
) -> None:
    sample = {
        "board_id": str(board_id),
        "audit_kind": audit_kind,
        "outcome": outcome,
        "classification_reason": str(classification_reason),
        "reason_codes": tuple(sorted(set(map(str, probe_reason_codes.values())))),
    }
    _probe_audit_samples.append(sample)
    _logger.warning(
        "kg.health.materialization_probe_audit board=%s kind=%s outcome=%s",
        board_id,
        audit_kind,
        outcome,
        extra={
            "event": "kg.health.materialization_probe_audit",
            "metric_name": _PROBE_AUDIT_METRIC,
            **sample,
        },
    )


def record_materialization_classification(
    *,
    board_id: str,
    materialization_state: str,
    metric_status: str,
    classification_reason: str,
    materialization_generation: str | None,
    probe_reason_codes: Mapping[str, str],
    observed_monotonic: float | None = None,
) -> None:
    """Record one completed health classification and its causal audits."""

    observed = (
        time.monotonic() if observed_monotonic is None else float(observed_monotonic)
    )
    bucket = _classification_bucket(
        materialization_state=str(materialization_state),
        metric_status=str(metric_status),
        classification_reason=str(classification_reason),
        probe_reason_codes=probe_reason_codes,
    )
    generation_sha256 = (
        _sha256(str(materialization_generation))[:16]
        if materialization_generation is not None
        else None
    )
    sample = {
        "board_id": str(board_id),
        "classification": bucket,
        "materialization_state": str(materialization_state),
        "metric_status": str(metric_status),
        "classification_reason": str(classification_reason),
        "generation_sha256": generation_sha256,
        "reason_codes": tuple(sorted(set(map(str, probe_reason_codes.values())))),
    }
    with _observability_lock:
        _classification_samples.append(sample)
        for audit_kind, outcome in _probe_audit_kinds(
            classification_reason=str(classification_reason),
            probe_reason_codes=probe_reason_codes,
        ):
            _record_probe_audit(
                board_id=str(board_id),
                audit_kind=audit_kind,
                outcome=outcome,
                classification_reason=str(classification_reason),
                probe_reason_codes=probe_reason_codes,
            )
        _observe_first_write_convergence(
            board_id=str(board_id),
            materialization_state=str(materialization_state),
            materialization_generation=materialization_generation,
            observed_monotonic=observed,
        )

    _logger.info(
        "kg.health.materialization_classified board=%s classification=%s "
        "state=%s metric_status=%s",
        board_id,
        bucket,
        materialization_state,
        metric_status,
        extra={
            "event": "kg.health.materialization_classified",
            "metric_name": _CLASSIFICATION_METRIC,
            **sample,
        },
    )


def record_first_write_acknowledged(
    *,
    board_id: str,
    previous_generation: str,
    generation: str,
    correlation_id: str | None,
    is_first_write: bool,
    acknowledged_monotonic: float | None = None,
) -> None:
    """Start the client-observed convergence clock after durable write ACK.

    The Community transaction adapter calls this only after its commit succeeds.
    Later health reads complete (or expire) the metric; no retry/background task
    is started here.
    """

    if not is_first_write:
        return
    acknowledged = (
        time.monotonic()
        if acknowledged_monotonic is None
        else float(acknowledged_monotonic)
    )
    key = (str(board_id), str(generation))
    with _observability_lock:
        pending = _pending_first_writes.resolve()
        if key not in pending and len(pending) >= _MAX_PENDING_FIRST_WRITES:
            pending.pop(next(iter(pending)))
        pending[key] = {
            "acknowledged_monotonic": acknowledged,
            "previous_generation_sha256": _sha256(str(previous_generation))[:16],
            "correlation_id": (
                str(correlation_id) if correlation_id is not None else None
            ),
        }
    _logger.info(
        "kg.health.first_write_acknowledged board=%s generation=%s",
        board_id,
        _sha256(str(generation))[:16],
        extra={
            "event": "kg.health.first_write_acknowledged",
            "board_id": str(board_id),
            "generation_sha256": _sha256(str(generation))[:16],
            "correlation_id": correlation_id,
            "deadline_ms": int(FIRST_WRITE_CONVERGENCE_DEADLINE_SECONDS * 1000),
        },
    )


def _observe_first_write_convergence(
    *,
    board_id: str,
    materialization_state: str,
    materialization_generation: str | None,
    observed_monotonic: float,
) -> None:
    pending = _pending_first_writes.resolve()
    exact_key = (
        (board_id, str(materialization_generation))
        if materialization_generation is not None
        else None
    )
    key = exact_key if exact_key in pending else None
    if key is None:
        candidates = [candidate for candidate in pending if candidate[0] == board_id]
        if len(candidates) == 1:
            key = candidates[0]
    if key is None:
        return

    tracked = pending[key]
    elapsed_seconds = max(
        0.0,
        observed_monotonic - float(tracked["acknowledged_monotonic"]),
    )
    if (
        materialization_state != "materialized"
        and elapsed_seconds <= FIRST_WRITE_CONVERGENCE_DEADLINE_SECONDS
    ):
        return
    outcome = (
        "converged"
        if materialization_state == "materialized"
        and elapsed_seconds <= FIRST_WRITE_CONVERGENCE_DEADLINE_SECONDS
        else "deadline_exceeded"
    )
    elapsed_ms = int(round(elapsed_seconds * 1000))
    sample = {
        "board_id": board_id,
        "generation_sha256": _sha256(key[1])[:16],
        "correlation_id": tracked["correlation_id"],
        "outcome": outcome,
        "elapsed_ms": elapsed_ms,
        "deadline_ms": int(FIRST_WRITE_CONVERGENCE_DEADLINE_SECONDS * 1000),
    }
    _convergence_samples.append(sample)
    del pending[key]
    log = _logger.info if outcome == "converged" else _logger.error
    log(
        "kg.health.first_write_convergence board=%s outcome=%s elapsed_ms=%d",
        board_id,
        outcome,
        elapsed_ms,
        extra={
            "event": "kg.health.first_write_convergence",
            "metric_name": _CONVERGENCE_METRIC,
            **sample,
        },
    )


def record_read_side_mutation_guard(
    *,
    board_id: str,
    outcome: str,
    snapshot_before_sha256: str | None,
    snapshot_after_sha256: str | None,
    changed_paths: Sequence[str],
) -> None:
    """Record the bounded filesystem guard result without retaining paths."""

    normalized_outcome = str(outcome)
    if normalized_outcome not in {"clean", "violation", "unavailable"}:
        raise ValueError("materialization_mutation_guard_outcome_invalid")
    changed_hashes = tuple(
        _sha256(path)[:16] for path in sorted(set(map(str, changed_paths)))
    )
    sample = {
        "board_id": str(board_id),
        "outcome": normalized_outcome,
        "snapshot_before_sha256": snapshot_before_sha256,
        "snapshot_after_sha256": snapshot_after_sha256,
        "changed_path_count": len(changed_hashes),
        "changed_path_sha256": changed_hashes,
    }
    with _observability_lock:
        _mutation_guard_samples.append(sample)
    log = _logger.error if normalized_outcome == "violation" else _logger.info
    log(
        "kg.health.read_side_mutation_guard board=%s outcome=%s changed=%d",
        board_id,
        normalized_outcome,
        len(changed_hashes),
        extra={
            "event": "kg.health.read_side_mutation_guard",
            "metric_name": _MUTATION_GUARD_METRIC,
            **sample,
        },
    )


def _counter_projection(
    *,
    metric_name: str,
    samples: list[dict[str, Any]],
    counters: list[dict[str, Any]],
    fields: tuple[str, ...],
) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    for counter in counters:
        labels = counter["labels"]
        counts[":".join(str(labels[field]) for field in fields)] += int(
            counter["count"]
        )
    return {
        "metric_name": metric_name,
        "counts": dict(sorted(counts.items())),
        "samples": samples,
    }


def materialization_observability_snapshot() -> dict[str, Any]:
    """Return capped diagnostic samples plus monotonic counter projections."""

    with _observability_lock:
        classification = _classification_samples.snapshot()
        classification_counters = _classification_samples.counter_snapshot()
        probe_audit = _probe_audit_samples.snapshot()
        probe_audit_counters = _probe_audit_samples.counter_snapshot()
        convergence = _convergence_samples.snapshot()
        convergence_counters = _convergence_samples.counter_snapshot()
        mutation_guard = _mutation_guard_samples.snapshot()
        mutation_guard_counters = _mutation_guard_samples.counter_snapshot()
        pending_count = len(_pending_first_writes.resolve())
    return {
        "classification": _counter_projection(
            metric_name=_CLASSIFICATION_METRIC,
            samples=classification,
            counters=classification_counters,
            fields=(
                "classification",
                "materialization_state",
                "metric_status",
            ),
        ),
        "probe_audit": _counter_projection(
            metric_name=_PROBE_AUDIT_METRIC,
            samples=probe_audit,
            counters=probe_audit_counters,
            fields=("audit_kind", "outcome"),
        ),
        "convergence": _counter_projection(
            metric_name=_CONVERGENCE_METRIC,
            samples=convergence,
            counters=convergence_counters,
            fields=("outcome",),
        ),
        "mutation_guard": _counter_projection(
            metric_name=_MUTATION_GUARD_METRIC,
            samples=mutation_guard,
            counters=mutation_guard_counters,
            fields=("outcome",),
        ),
        "pending_first_writes": pending_count,
    }


def reset_materialization_observability_for_tests() -> None:
    """Reset all runtime-owned buffers and convergence state."""

    with _observability_lock:
        _classification_samples.clear()
        _probe_audit_samples.clear()
        _convergence_samples.clear()
        _mutation_guard_samples.clear()
        _pending_first_writes.resolve().clear()


__all__ = [
    "FIRST_WRITE_CONVERGENCE_DEADLINE_SECONDS",
    "materialization_observability_snapshot",
    "record_first_write_acknowledged",
    "record_materialization_classification",
    "record_read_side_mutation_guard",
    "reset_materialization_observability_for_tests",
]
