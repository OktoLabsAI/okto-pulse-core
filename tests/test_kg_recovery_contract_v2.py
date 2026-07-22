from __future__ import annotations

import uuid

from okto_pulse.core.application.processors.dead_letter import build_attempt_entry
from okto_pulse.core.kg.interfaces.audit_repository import AuditWriteContention
from okto_pulse.core.ports.kg_operational import classify_kg_recovery_failure


def test_recovery_classifier_has_three_fail_closed_classes():
    connectivity = classify_kg_recovery_failure(
        "GraphUnavailable", "connection refused while opening board graph"
    )
    invalid = classify_kg_recovery_failure(
        "ValidationError", "payload field edge_type is invalid"
    )
    drift = classify_kg_recovery_failure(
        "ProvenanceError", "content_hash_mismatch true_drift"
    )

    assert (connectivity.recovery_class, connectivity.replay_safe) == (
        "connectivity",
        True,
    )
    assert (invalid.recovery_class, invalid.replay_safe) == (
        "invalid_payload",
        False,
    )
    assert (drift.recovery_class, drift.replay_safe) == ("true_drift", True)


def test_audit_write_contention_is_transient_and_replay_safe():
    error = AuditWriteContention("audit.stage_consolidation_records")
    assert error.code == "audit_write_contention"
    assert error.retryable is True

    classified = classify_kg_recovery_failure(
        type(error).__name__,
        str(error),
    )

    assert classified.recovery_class == "connectivity"
    assert classified.reason_code == "kg_recovery.audit_write_contention"
    assert classified.replay_safe is True


def test_backend_errors_without_stable_contention_type_remain_fail_closed():
    classified = classify_kg_recovery_failure(
        "IntegrityError",
        "duplicate consolidation session",
    )

    assert classified.recovery_class == "invalid_payload"
    assert classified.reason_code == "kg_recovery.invalid_payload"
    assert classified.replay_safe is False


def test_dlq_attempt_carries_reason_and_correlation_without_payload_leak():
    entry = build_attempt_entry(
        attempt=3,
        error_type="GraphUnavailable",
        message="graph_unavailable",
    )

    assert entry["recovery_class"] == "connectivity"
    assert entry["reason_code"] == "kg_recovery.connectivity"
    assert entry["replay_safe"] is True
    uuid.UUID(entry["correlation_id"])
