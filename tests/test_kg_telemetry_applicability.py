"""Absent limits are inapplicable; missing readings still fail closed."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.health_state import (
    GraphTelemetry,
    HealthState,
    KGHealthStateClassifier,
    LockState,
    MetricStatus,
)
from okto_pulse.core.kg.interfaces.graph_runtime_store import GraphStorageFootprint
from okto_pulse.core.kg.interfaces.storage_ref import StorageRef
from okto_pulse.core.services import kg_health_service as service


def _classify(telemetry):
    return KGHealthStateClassifier().evaluate(
        telemetries=[telemetry],
        lock_state=LockState(False, False, None),
        quarantine_present=False,
        backpressure_rejecting=False,
    )


@pytest.mark.parametrize(
    "applicable,expected",
    [(False, MetricStatus.AVAILABLE), (True, MetricStatus.PARTIAL)],
)
def test_only_explicit_inapplicability_skips_missing_capacity(applicable, expected):
    telemetry = GraphTelemetry("board", 0.0, None, 0, 0, 0, applicable)
    assert _classify(telemetry).metric_status is expected
    assert (
        _classify(replace(telemetry, recent_commit_errors=None)).metric_status
        is MetricStatus.PARTIAL
    )
    assert (
        _classify(replace(telemetry, recent_wal_errors=1)).state
        is HealthState.RECOVERY_NEEDED
    )


def test_inapplicability_does_not_hide_all_missing_metrics():
    assert (
        _classify(
            GraphTelemetry("board", None, None, None, None, None, False)
        ).metric_status
        is MetricStatus.UNAVAILABLE
    )


def test_inapplicability_rejects_contradictory_contracts():
    with pytest.raises(ValueError):
        GraphTelemetry("board", 0.0, 0.0, 0, 0, 0, False)
    with pytest.raises(ValueError):
        GraphStorageFootprint(
            "b",
            StorageRef("board:b", "test"),
            "available",
            "runtime_capability",
            percentage=0.0,
            percentage_applicable=False,
        )


@pytest.mark.parametrize(
    "status,applicable,percentage,expected",
    [
        ("available", False, None, "not_applicable"),
        ("available", True, None, "unavailable"),
        ("available", True, 25.0, "available"),
        ("unavailable", False, None, "unavailable"),
    ],
)
def test_footprint_public_projection_and_probe(
    monkeypatch, status, applicable, percentage, expected
):
    footprint = GraphStorageFootprint(
        "b",
        StorageRef("board:b", "test"),
        status,
        "runtime_capability",
        total_bytes=100 if status == "available" else None,
        percentage=percentage,
        percentage_applicable=applicable,
    )
    calls = []

    def read(board):
        calls.append(board)
        return footprint

    monkeypatch.setattr(
        service,
        "get_kg_registry",
        lambda: SimpleNamespace(graph_runtime_store=SimpleNamespace(footprint=read)),
    )
    payload = service._build_storage_footprint_proxy("b")
    assert payload["percentage_status"] == expected
    assert payload["percentage"] == percentage
    if expected == "not_applicable":
        assert payload["percentage_reason"] == "no_capacity_limit_configured"
    samples = []
    monkeypatch.setattr(
        service, "_record_board_hwm_sample", lambda board, value: samples.append(value)
    )
    calls.clear()
    telemetry = service._probe_board_graph_telemetry(
        board_id="b",
        total_nodes=1,
        graph_schema_version="1",
        empty_after_materialized_history=False,
    )
    assert calls == ["b"]  # No extra filesystem scan to decide applicability.
    assert telemetry.high_water_mark_applicable is (expected != "not_applicable")
    assert telemetry.high_water_mark_pct == percentage
    assert samples == [percentage]


def test_failed_footprint_and_budget_stay_unavailable_and_redacted(monkeypatch):
    def fail(*args):
        raise OSError("private/path")

    monkeypatch.setattr(
        service,
        "get_kg_registry",
        lambda: SimpleNamespace(
            graph_runtime_store=SimpleNamespace(footprint=fail, budget_snapshot=fail)
        ),
    )
    assert (
        service._build_storage_footprint_proxy("b")["percentage_status"]
        == "unavailable"
    )
    budget = service._build_native_runtime_budget()
    assert budget["status"] == "unavailable"
    assert budget["unavailable_reason"] == "budget_snapshot_unavailable"
    assert "private" not in str(budget)
