from __future__ import annotations

import pytest

from okto_pulse.core.services.architecture_observability import (
    METRIC_PAYLOAD_PARITY_VIOLATION_TOTAL,
    ArchitectureMetricEvent,
    assert_architecture_metric_payload_is_safe,
    get_architecture_metric_labels,
    get_architecture_metric_samples,
    observe_architecture_payload_parity_violation,
    reset_architecture_observability_for_tests,
    sanitize_architecture_metric_event,
)


def setup_function() -> None:
    reset_architecture_observability_for_tests()


def teardown_function() -> None:
    reset_architecture_observability_for_tests()


def test_architecture_observability_rejects_unsafe_metric_names_and_labels():
    with pytest.raises(ValueError, match="Unsupported architecture metric"):
        sanitize_architecture_metric_event(
            ArchitectureMetricEvent("diagram_payload_bytes", 1, {"board_id": "board-1"})
        )

    with pytest.raises(ValueError, match="Unsupported architecture metric label"):
        sanitize_architecture_metric_event(
            ArchitectureMetricEvent(
                METRIC_PAYLOAD_PARITY_VIOLATION_TOTAL,
                1,
                {"diagram_payload": "{}"},
            )
        )

    with pytest.raises(ValueError, match="Unsafe architecture metric payload"):
        assert_architecture_metric_payload_is_safe({"board_id": "board-1", "message": "raw text"})


def test_architecture_payload_parity_violation_metric_uses_safe_labels_only():
    observe_architecture_payload_parity_violation(
        board_id="board-1",
        surface="rest_mcp_contract",
        reason_code="shape_drift",
    )

    samples = get_architecture_metric_samples()
    assert len(samples) == 1
    sample = samples[0]
    assert sample["metric_name"] == METRIC_PAYLOAD_PARITY_VIOLATION_TOTAL
    assert sample["labels"] == {
        "board_id": "board-1",
        "surface": "rest_mcp_contract",
        "outcome": "violation",
        "reason_code": "shape_drift",
    }
    assert set(sample["labels"]) <= set(get_architecture_metric_labels())
    assert_architecture_metric_payload_is_safe(sample["labels"])
