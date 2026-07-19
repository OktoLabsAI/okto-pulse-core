"""Atomic public-contract tests for KG Health schema 1.1."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

import okto_pulse.core.services.kg_health_service as kg_health_service
from okto_pulse.core.kg.interfaces.graph_runtime_store import (
    GraphRuntimeObservationState,
    GraphRuntimeState,
)
from okto_pulse.core.kg.interfaces.storage_ref import StorageRef
from okto_pulse.core.kg.materialization_health import (
    BoardHealthCensus,
    CensusStatus,
    MaterializationEvidence,
)
from okto_pulse.core.mcp.kg_query_safety import KGHealthMCPProjection
from okto_pulse.core.ports.materialization_health import (
    register_materialization_evidence_port,
    reset_materialization_evidence_port_for_tests,
)
from okto_pulse.core.services.kg_health_service import HEALTH_SCHEMA_VERSION
from sqlalchemy_test_models import Board


def test_health_schema_version_advances_to_1_1() -> None:
    assert HEALTH_SCHEMA_VERSION == "1.1"


def test_mcp_summary_preserves_versioned_materialization_contract() -> None:
    payload = {
        "board_id": "board-schema-1-1",
        "graph_state": "healthy",
        "discovery_state": "healthy",
        "overall_state": "healthy",
        "metric_status": "available",
        "classification_reason": "empty_board_not_materialized",
        "correlation_id": "corr-schema-1-1",
        "memory_pressure_status": "unconfirmed",
        "recent_events": [],
        "checked_at": "2026-07-16T00:00:00+00:00",
        "health_schema_version": "1.1",
        "materialization_state": "not_materialized",
        "materialization_generation": "generation-1",
        "probe_reason_codes": {
            "board_graph": "board_graph_confirmed_absent",
            "board_census": "board_census_available",
            "global_discovery": "global_discovery_confirmed_absent",
        },
        "global_outbox_dead_letter_count": 0,
        "queue_depth": 0,
        "dead_letter_count": 0,
        "total_nodes": 0,
        "default_score_ratio": 0.0,
        "avg_relevance": 0.0,
    }

    projected = KGHealthMCPProjection().project(payload, profile="summary")

    assert projected["health_schema_version"] == "1.1"
    assert projected["materialization_state"] == "not_materialized"
    assert projected["materialization_generation"] == "generation-1"
    assert projected["probe_reason_codes"] == payload["probe_reason_codes"]
    assert projected["global_outbox_dead_letter_count"] == 0


class _ConfirmedEmptyEvidencePort:
    async def current_generation(self, board_id: str) -> str:
        return "generation-empty-1"

    async def probe(self, request) -> MaterializationEvidence:  # noqa: ANN001
        observed_at = datetime.now(timezone.utc)
        return MaterializationEvidence(
            board_store=GraphRuntimeState.from_observation(
                board_id=request.board_id,
                storage_ref=StorageRef(
                    f"board:{request.board_id}",
                    "contract-test",
                ),
                state=GraphRuntimeObservationState.CONFIRMED_ABSENT,
                generation=request.generation,
                reason_code="board_graph_confirmed_absent",
                observed_at=observed_at,
                backend="contract-test",
            ),
            census=BoardHealthCensus(
                generation=request.generation,
                status=CensusStatus.AVAILABLE,
                source_count=0,
                queue_depth=0,
                active_queue_count=0,
                dead_letter_count=0,
                global_outbox_dead_letter_count=0,
                reason_code="board_census_available",
                observed_at=observed_at,
            ),
            discovery_store=GraphRuntimeState.from_observation(
                board_id="_global",
                storage_ref=StorageRef("global-discovery", "contract-test"),
                state=GraphRuntimeObservationState.CONFIRMED_ABSENT,
                generation=request.generation,
                reason_code="global_discovery_confirmed_absent",
                observed_at=observed_at,
                backend="contract-test",
            ),
        )


class _UnreadableEvidencePort:
    def __init__(self, *, quarantined: bool) -> None:
        self._quarantined = quarantined

    async def current_generation(self, board_id: str) -> str:
        return "generation-unreadable-1"

    async def probe(self, request) -> MaterializationEvidence:  # noqa: ANN001
        observed_at = datetime.now(timezone.utc)
        return MaterializationEvidence(
            board_store=GraphRuntimeState.from_observation(
                board_id=request.board_id,
                storage_ref=StorageRef(
                    f"board:{request.board_id}",
                    "contract-test",
                ),
                state=(GraphRuntimeObservationState.PRESENT_UNREADABLE_OR_ERROR),
                generation=request.generation,
                reason_code=(
                    "board_graph_quarantine_present"
                    if self._quarantined
                    else "board_graph_stat_io_error"
                ),
                observed_at=observed_at,
                backend="contract-test",
                quarantined=self._quarantined,
            ),
            census=BoardHealthCensus(
                generation=request.generation,
                status=CensusStatus.AVAILABLE,
                source_count=0,
                queue_depth=0,
                active_queue_count=0,
                dead_letter_count=0,
                global_outbox_dead_letter_count=0,
                reason_code="board_census_available",
                observed_at=observed_at,
            ),
            discovery_store=GraphRuntimeState.from_observation(
                board_id="_global",
                storage_ref=StorageRef("global-discovery", "contract-test"),
                state=GraphRuntimeObservationState.CONFIRMED_ABSENT,
                generation=request.generation,
                reason_code="global_discovery_confirmed_absent",
                observed_at=observed_at,
                backend="contract-test",
            ),
        )


class _TimedOutEvidencePort:
    async def current_generation(self, board_id: str) -> str:
        raise TimeoutError

    async def probe(self, request) -> MaterializationEvidence:  # noqa: ANN001
        raise AssertionError("probe must not run after generation timeout")


async def _ensure_board(db_factory, board_id: str) -> None:  # noqa: ANN001
    async with db_factory() as session:
        if await session.get(Board, board_id) is None:
            session.add(Board(id=board_id, name="health", owner_id="owner"))
            await session.commit()


@pytest.mark.asyncio
async def test_confirmed_empty_composes_known_zero_contract_without_graph_reads(
    db_factory,
    monkeypatch,
) -> None:
    board_id = "board-health-schema-1-1-empty"
    await _ensure_board(db_factory, board_id)
    forbidden_calls: list[str] = []

    def forbidden_graph_read(*_args, **_kwargs):  # noqa: ANN002, ANN003
        forbidden_calls.append("graph_read")
        raise AssertionError("confirmed empty health must not open graph storage")

    monkeypatch.setattr(
        kg_health_service,
        "_build_graph_health_snapshot",
        forbidden_graph_read,
    )
    monkeypatch.setattr(
        kg_health_service,
        "_build_parity_health_snapshot",
        forbidden_graph_read,
    )
    monkeypatch.setattr(
        kg_health_service,
        "_build_artifact_health_snapshot",
        forbidden_graph_read,
    )
    monkeypatch.setattr(
        kg_health_service,
        "_has_materialized_kg_history",
        forbidden_graph_read,
    )
    register_materialization_evidence_port(_ConfirmedEmptyEvidencePort())
    try:
        async with db_factory() as session:
            result = await kg_health_service.get_kg_health(board_id, session)
    finally:
        reset_materialization_evidence_port_for_tests()

    assert result["schema_version"] == "1.0"
    assert result["health_schema_version"] == "1.1"
    assert result["materialization_state"] == "not_materialized"
    assert result["materialization_generation"] == "generation-empty-1"
    assert result["classification_reason"] == "empty_board_not_materialized"
    assert result["graph_state"] == "healthy"
    assert result["discovery_state"] == "healthy"
    assert result["overall_state"] == "healthy"
    assert result["metric_status"] == "available"
    assert result["oldest_pending_age_s"] is None
    assert result["source_count"] == 0
    assert result["storage_footprint_proxy"]["total_bytes"] == 0
    assert result["storage_footprint_proxy"]["high_water_mark_pct"] is None
    assert result["kg_layer_counts"]["by_layer"]["canonical"] == 0
    assert result["kg_layer_counts"]["by_layer"]["working"] == 0
    assert result["probe_diagnostics"]["graph_metrics"] == {
        "status": "available",
        "reason": "confirmed_empty_known_zero",
    }
    assert result["probe_diagnostics"]["schema_version"] == {
        "status": "not_applicable",
        "reason": "board_not_materialized",
    }
    assert result["probe_diagnostics"]["storage_footprint"] == {
        "status": "available",
        "reason": "confirmed_empty_known_zero",
    }
    assert result["probe_diagnostics"]["layer_counts"] == {
        "status": "available",
        "reason": "confirmed_empty_known_zero",
    }
    assert result["probe_reason_codes"] == {
        "board_graph": "board_graph_confirmed_absent",
        "board_census": "board_census_available",
        "global_discovery": "global_discovery_confirmed_absent",
    }
    assert json.dumps(result).count('"global_outbox_dead_letter_count"') == 1
    assert forbidden_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("quarantined", "expected_state"),
    [(False, "recovery_needed"), (True, "quarantined")],
)
async def test_unreadable_store_preserves_concrete_recovery_state_without_open(
    db_factory,
    monkeypatch,
    quarantined: bool,
    expected_state: str,
) -> None:
    board_id = f"board-health-unreadable-{quarantined}"
    await _ensure_board(db_factory, board_id)
    forbidden_calls: list[str] = []

    def forbidden_graph_read(*_args, **_kwargs):  # noqa: ANN002, ANN003
        forbidden_calls.append("graph_read")
        raise AssertionError("unreadable evidence must not open graph storage")

    monkeypatch.setattr(
        kg_health_service,
        "_build_graph_health_snapshot",
        forbidden_graph_read,
    )
    monkeypatch.setattr(
        kg_health_service,
        "_build_parity_health_snapshot",
        forbidden_graph_read,
    )
    monkeypatch.setattr(
        kg_health_service,
        "_build_artifact_health_snapshot",
        kg_health_service._not_materialized_artifact_snapshot,
    )
    register_materialization_evidence_port(
        _UnreadableEvidencePort(quarantined=quarantined)
    )
    try:
        async with db_factory() as session:
            result = await kg_health_service.get_kg_health(board_id, session)
    finally:
        reset_materialization_evidence_port_for_tests()

    assert result["materialization_state"] == "unknown"
    assert result["graph_state"] == expected_state
    assert result["overall_state"] == expected_state
    assert result["metric_status"] == "unavailable"
    assert result["probe_reason_codes"]["board_graph"].startswith("board_graph_")
    assert result["probe_diagnostics"]["graph_metrics"]["status"] == ("unavailable")
    assert forbidden_calls == []


@pytest.mark.asyncio
async def test_evidence_timeout_returns_typed_fail_closed_payload_without_open(
    db_factory,
    monkeypatch,
) -> None:
    board_id = "board-health-evidence-timeout"
    await _ensure_board(db_factory, board_id)
    forbidden_calls: list[str] = []

    def forbidden_graph_read(*_args, **_kwargs):  # noqa: ANN002, ANN003
        forbidden_calls.append("graph_read")
        raise AssertionError("evidence timeout must not open graph storage")

    monkeypatch.setattr(
        kg_health_service,
        "_build_graph_health_snapshot",
        forbidden_graph_read,
    )
    monkeypatch.setattr(
        kg_health_service,
        "_build_parity_health_snapshot",
        forbidden_graph_read,
    )
    monkeypatch.setattr(
        kg_health_service,
        "_probe_global_discovery_telemetry",
        forbidden_graph_read,
    )
    monkeypatch.setattr(
        kg_health_service,
        "_build_artifact_health_snapshot",
        kg_health_service._not_materialized_artifact_snapshot,
    )
    register_materialization_evidence_port(_TimedOutEvidencePort())
    try:
        async with db_factory() as session:
            result = await kg_health_service.get_kg_health(board_id, session)
    finally:
        reset_materialization_evidence_port_for_tests()

    assert result["health_schema_version"] == "1.1"
    assert result["materialization_state"] == "unknown"
    assert result["materialization_generation"] is None
    assert result["metric_status"] == "unavailable"
    assert result["overall_state"] != "healthy"
    assert set(result["probe_reason_codes"].values()) == {
        "materialization_evidence_timeout"
    }
    assert forbidden_calls == []
