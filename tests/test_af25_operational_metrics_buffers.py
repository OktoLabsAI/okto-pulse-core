from __future__ import annotations

import re
from pathlib import Path

from okto_pulse.core.observability.sample_buffer import METRIC_SAMPLE_RETENTION_LIMIT


def _overflow_size() -> int:
    return METRIC_SAMPLE_RETENTION_LIMIT + 7


def test_no_unbounded_dict_sample_lists_remain_in_core_operational_metrics() -> None:
    source_root = Path(__file__).parents[1] / "src" / "okto_pulse" / "core"
    pattern = re.compile(
        r"^_[A-Za-z0-9_]*(samples|SAMPLES).*: list\[dict\[str, Any\]\] = \[\]",
        re.MULTILINE,
    )

    matches: list[str] = []
    for path in source_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for match in pattern.finditer(text):
            line_no = text.count("\n", 0, match.start()) + 1
            matches.append(f"{path.relative_to(source_root)}:{line_no}")

    assert matches == []


def test_global_discovery_metric_counts_are_monotonic_after_sample_eviction() -> None:
    from okto_pulse.core.kg.global_discovery import metrics as gdm

    total = _overflow_size()
    gdm.reset_global_discovery_metrics()

    for _ in range(total):
        gdm.emit_missing_embedding_skipped(board_id="board-1", node_type="Bug")
        gdm.emit_digest_upsert(board_id="board-1", node_type="Decision", outcome="created")
        gdm.emit_canonical_incomplete_excluded(
            board_id="board-1",
            reason_code="active_cognitive_pending",
        )
        gdm.emit_digest_layer_mismatch(
            board_id="board-1",
            expected_layer="canonical",
            actual_layer="working",
        )

    assert len(gdm.get_missing_embedding_skipped_samples()) == METRIC_SAMPLE_RETENTION_LIMIT
    assert len(gdm.get_digest_upsert_samples()) == METRIC_SAMPLE_RETENTION_LIMIT
    assert len(gdm.get_canonical_incomplete_excluded_samples()) == METRIC_SAMPLE_RETENTION_LIMIT
    assert len(gdm.get_digest_layer_mismatch_samples()) == METRIC_SAMPLE_RETENTION_LIMIT

    assert gdm.get_missing_embedding_skipped_count(board_id="board-1", node_type="Bug") == total
    assert gdm.get_digest_upsert_count(
        board_id="board-1",
        node_type="Decision",
        outcome="created",
    ) == total
    assert gdm.get_canonical_incomplete_excluded_count(
        board_id="board-1",
        reason_code="active_cognitive_pending",
    ) == total
    assert gdm.get_digest_layer_mismatch_count(
        board_id="board-1",
        expected_layer="canonical",
        actual_layer="working",
    ) == total


def test_service_metric_sample_buffers_are_bounded() -> None:
    from okto_pulse.core.services.architecture_observability import (
        get_architecture_metric_samples,
        observe_architecture_payload_parity_violation,
        reset_architecture_observability_for_tests,
    )
    from okto_pulse.core.services.bug_regression_observability import (
        BugRegressionMetricEvent,
        BugRegressionMetricsSink,
        METRIC_PATH_B_TOTAL,
        get_bug_regression_metric_samples,
        reset_bug_regression_observability_for_tests,
    )
    from okto_pulse.core.services.governance_observability import (
        emit_governance_metric,
        get_governance_metric_samples,
        reset_governance_metric_samples,
    )
    from okto_pulse.core.services.resource_lineage import (
        get_resource_lineage_metric_samples,
        observe_resource_lineage_coverage_uncovered,
        reset_resource_lineage_observability_for_tests,
    )

    total = _overflow_size()
    reset_architecture_observability_for_tests()
    reset_bug_regression_observability_for_tests()
    reset_governance_metric_samples()
    reset_resource_lineage_observability_for_tests()

    bug_sink = BugRegressionMetricsSink()
    for index in range(total):
        emit_governance_metric(
            {
                "metric_name": "board_missing_context_warning_total",
                "board_id": "board-1",
                "warning_code": "board_rules_missing",
                "surface": "menu_board",
                "outcome": "warning",
            }
        )
        observe_architecture_payload_parity_violation(
            board_id="board-1",
            surface="rest_mcp_contract",
            reason_code="shape_drift",
        )
        bug_sink.emit(
            BugRegressionMetricEvent(
                METRIC_PATH_B_TOTAL,
                1,
                {
                    "reason_code": "coverage_pending",
                    "coverage_state": "coverage_pending",
                    "outcome": "blocked",
                    "surface": "mcp",
                },
            )
        )
        observe_resource_lineage_coverage_uncovered(
            resource_type="knowledge_base",
            reason=f"missing_{index}",
        )

    assert len(get_governance_metric_samples()) == METRIC_SAMPLE_RETENTION_LIMIT
    assert len(get_architecture_metric_samples()) == METRIC_SAMPLE_RETENTION_LIMIT
    assert len(get_bug_regression_metric_samples()) == METRIC_SAMPLE_RETENTION_LIMIT
    assert len(get_resource_lineage_metric_samples()) == METRIC_SAMPLE_RETENTION_LIMIT


def test_remaining_core_metric_counts_survive_sample_eviction() -> None:
    from okto_pulse.core.kg import rebuild_audit
    from okto_pulse.core.services import human_control_metrics

    total = _overflow_size()
    human_control_metrics.reset_human_control_required_counter()
    rebuild_audit.reset_materialized_counter()
    rebuild_audit.reset_cognitive_technical_signal_counter()

    for _ in range(total):
        human_control_metrics.emit_human_control_required(
            board_id="board-1",
            blocked_tool="okto_pulse_update_cognitive_item",
            blocked_action="skip",
        )
        rebuild_audit._emit_materialized_sample(
            board_id="board-1",
            outcome=rebuild_audit.CognitiveMaterializeOutcome.MATERIALIZED.value,
            item_count=3,
        )
        rebuild_audit.emit_cognitive_technical_signal_sample(
            signal="technical_dlq",
            surface="mcp",
            blocking=True,
            would_block_done=False,
            board_id="board-1",
        )

    assert (
        len(human_control_metrics.get_human_control_required_samples())
        == METRIC_SAMPLE_RETENTION_LIMIT
    )
    assert len(rebuild_audit.get_materialized_samples()) == METRIC_SAMPLE_RETENTION_LIMIT
    assert (
        len(rebuild_audit.get_cognitive_technical_signal_samples())
        == METRIC_SAMPLE_RETENTION_LIMIT
    )

    assert (
        human_control_metrics.get_human_control_required_count(
            board_id="board-1",
            blocked_tool="okto_pulse_update_cognitive_item",
            blocked_action="skip",
        )
        == total
    )
    assert (
        rebuild_audit.get_materialized_event_count(
            "board-1",
            outcome=rebuild_audit.CognitiveMaterializeOutcome.MATERIALIZED.value,
        )
        == total
    )
    assert (
        rebuild_audit.get_materialized_count(
            "board-1",
            outcome=rebuild_audit.CognitiveMaterializeOutcome.MATERIALIZED.value,
        )
        == total * 3
    )
    assert (
        rebuild_audit.get_cognitive_technical_signal_event_count(
            signal="technical_dlq",
            surface="mcp",
            blocking=True,
            would_block_done=False,
            board_id="board-1",
        )
        == total
    )
