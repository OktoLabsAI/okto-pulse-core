from __future__ import annotations

import logging

from okto_pulse.core.observability.materialization_health import (
    materialization_observability_snapshot,
    record_first_write_acknowledged,
    record_materialization_classification,
    record_read_side_mutation_guard,
    reset_materialization_observability_for_tests,
)


def setup_function() -> None:
    reset_materialization_observability_for_tests()


def teardown_function() -> None:
    reset_materialization_observability_for_tests()


def test_classification_metric_and_probe_audits_use_bounded_labels(
    caplog,
) -> None:
    caplog.set_level(logging.INFO)

    record_materialization_classification(
        board_id="board-observability",
        materialization_state="not_materialized",
        metric_status="available",
        classification_reason="empty_board_not_materialized",
        materialization_generation="unmaterialized-v1",
        probe_reason_codes={
            "board_graph": "board_graph_confirmed_absent",
            "board_census": "board_census_available",
            "global_discovery": "global_discovery_confirmed_absent",
        },
        observed_monotonic=100.0,
    )
    record_materialization_classification(
        board_id="board-observability",
        materialization_state="unknown",
        metric_status="unavailable",
        classification_reason="materialization_evidence_timeout",
        materialization_generation=None,
        probe_reason_codes={
            "board_graph": "board_graph_probe_timeout",
            "board_census": "board_census_timeout",
            "global_discovery": "global_discovery_probe_timeout",
        },
        observed_monotonic=101.0,
    )
    record_materialization_classification(
        board_id="board-observability",
        materialization_state="unknown",
        metric_status="unavailable",
        classification_reason="materialization_generation_changed",
        materialization_generation=None,
        probe_reason_codes={
            "board_graph": "board_graph_confirmed_absent",
            "board_census": "materialization_generation_changed",
            "global_discovery": "global_discovery_confirmed_absent",
        },
        observed_monotonic=102.0,
    )

    snapshot = materialization_observability_snapshot()
    assert snapshot["classification"]["metric_name"] == (
        "kg_health_materialization_classification_total"
    )
    assert snapshot["classification"]["counts"] == {
        "confirmed_empty:not_materialized:available": 1,
        "deadline_unavailable:unknown:unavailable": 1,
        "generation_race:unknown:unavailable": 1,
    }
    assert snapshot["probe_audit"]["counts"] == {
        "deadline:fail_closed": 1,
        "generation_race:fail_closed": 1,
    }
    assert {getattr(record, "event", None) for record in caplog.records} >= {
        "kg.health.materialization_classified",
        "kg.health.materialization_probe_audit",
    }


def test_first_write_convergence_is_monotonic_bounded_and_deduplicated() -> None:
    record_first_write_acknowledged(
        board_id="board-first-write",
        previous_generation="unmaterialized-v1",
        generation="mg-first",
        correlation_id="correlation-first",
        acknowledged_monotonic=500.0,
        is_first_write=True,
    )

    record_materialization_classification(
        board_id="board-first-write",
        materialization_state="materialized",
        metric_status="available",
        classification_reason="materialized",
        materialization_generation="mg-first",
        probe_reason_codes={},
        observed_monotonic=500.125,
    )
    # A repeated health read must not publish the same convergence twice.
    record_materialization_classification(
        board_id="board-first-write",
        materialization_state="materialized",
        metric_status="available",
        classification_reason="materialized",
        materialization_generation="mg-first",
        probe_reason_codes={},
        observed_monotonic=500.250,
    )

    snapshot = materialization_observability_snapshot()
    assert snapshot["convergence"]["metric_name"] == (
        "kg_health_first_write_convergence_total"
    )
    assert snapshot["convergence"]["counts"] == {"converged": 1}
    assert snapshot["convergence"]["samples"] == [
        {
            "board_id": "board-first-write",
            "generation_sha256": snapshot["convergence"]["samples"][0][
                "generation_sha256"
            ],
            "correlation_id": "correlation-first",
            "outcome": "converged",
            "elapsed_ms": 125,
            "deadline_ms": 10_000,
        }
    ]
    assert snapshot["pending_first_writes"] == 0


def test_first_write_poll_observed_after_deadline_is_audited_once() -> None:
    record_first_write_acknowledged(
        board_id="board-late",
        previous_generation="unmaterialized-v1",
        generation="mg-late",
        correlation_id=None,
        acknowledged_monotonic=0.0,
        is_first_write=True,
    )
    record_materialization_classification(
        board_id="board-late",
        materialization_state="unknown",
        metric_status="unavailable",
        classification_reason="still_pending",
        materialization_generation="mg-late",
        probe_reason_codes={},
        observed_monotonic=10.001,
    )
    record_materialization_classification(
        board_id="board-late",
        materialization_state="unknown",
        metric_status="unavailable",
        classification_reason="still_pending",
        materialization_generation="mg-late",
        probe_reason_codes={},
        observed_monotonic=11.0,
    )

    snapshot = materialization_observability_snapshot()
    assert snapshot["convergence"]["counts"] == {"deadline_exceeded": 1}
    assert snapshot["convergence"]["samples"][0]["elapsed_ms"] == 10_001
    assert snapshot["pending_first_writes"] == 0


def test_mutation_guard_keeps_paths_out_of_metric_labels_and_samples() -> None:
    record_read_side_mutation_guard(
        board_id="board-guard",
        outcome="clean",
        snapshot_before_sha256="a" * 64,
        snapshot_after_sha256="a" * 64,
        changed_paths=(),
    )
    record_read_side_mutation_guard(
        board_id="board-guard",
        outcome="violation",
        snapshot_before_sha256="a" * 64,
        snapshot_after_sha256="b" * 64,
        changed_paths=("C:/secret/graph.lbug",),
    )

    snapshot = materialization_observability_snapshot()
    guard = snapshot["mutation_guard"]
    assert guard["metric_name"] == "kg_health_read_side_mutation_guard_total"
    assert guard["counts"] == {"clean": 1, "violation": 1}
    assert guard["samples"][1]["changed_path_count"] == 1
    assert guard["samples"][1]["changed_path_sha256"]
    assert "C:/secret/graph.lbug" not in repr(guard)


def test_metric_counts_remain_monotonic_after_sample_retention_rolls_over() -> None:
    for index in range(1_030):
        record_materialization_classification(
            board_id=f"board-{index}",
            materialization_state="materialized",
            metric_status="available",
            classification_reason="materialized",
            materialization_generation=f"mg-{index}",
            probe_reason_codes={},
            observed_monotonic=float(index),
        )

    classification = materialization_observability_snapshot()["classification"]
    assert len(classification["samples"]) == 1_024
    assert classification["counts"] == {"materialized:materialized:available": 1_030}
