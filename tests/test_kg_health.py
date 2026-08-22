"""Tests for the KG health snapshot service and endpoint.

Spec 20f67c2a (Ideação #5). Covers FR1, FR2, FR3, FR4, FR5, FR8 and
ACs 1-10. Aligns with the Ideação #3 lesson: every scenario marked
``automated`` in the Pulse has a real pytest assertion in this file.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from contextvars import copy_context
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio

from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult
from okto_pulse.core.kg.scoring import (
    CONTRADICT_PENALTY_CAP,
    DECAY_REORDER_POOL_MULTIPLIER,
    _apply_decay_reorder,
    _fetch_node_inputs,
    get_contradict_warn_count,
    reset_contradict_warn_counters,
)
from sqlalchemy_test_models import (
    Board,
    CanonicalDebt,
    ConsolidationAudit,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    KGTickRun,
    KuzuNodeRef,
)
from okto_pulse.core.ports.scheduler import SchedulerJobSnapshot
import okto_pulse.core.services.kg_health_service as kg_health_service
from repository_checkout_testing import community_repo_for
from okto_pulse.core.services.kg_health_service import (
    BoardNotFoundError,
    DEFAULT_SCORE_BAND_HIGH,
    DEFAULT_SCORE_BAND_LOW,
    DEFAULT_SCORE_RATIO_ALARM_THRESHOLD,
    HEALTH_SCHEMA_VERSION,
    get_kg_health,
)


KG_HEALTH_BOARD_ID = "board-kg-health-test"
KG_HEALTH_USER_ID = "user-kg-health-test"


class _HealthSchedulerControl:
    def __init__(
        self,
        *,
        available: bool = True,
        exists: bool = True,
        next_run_time: datetime | None = None,
        message: str | None = None,
    ) -> None:
        self._available = available
        self._snapshot = SchedulerJobSnapshot(
            job_id="kg_daily_tick",
            exists=exists,
            next_run_time=next_run_time,
            message=message,
        )

    def is_available(self) -> bool:
        return self._available

    async def get_job_snapshot(self, job_id: str) -> SchedulerJobSnapshot:
        return SchedulerJobSnapshot(
            job_id=job_id,
            exists=self._snapshot.exists,
            next_run_time=self._snapshot.next_run_time,
            message=self._snapshot.message,
        )


@pytest_asyncio.fixture
async def kg_health_board(db_factory):
    """Idempotent Board row for the health tests; reset queue + counter state."""
    async with db_factory() as session:
        existing = await session.get(Board, KG_HEALTH_BOARD_ID)
        if existing is None:
            session.add(
                Board(
                    id=KG_HEALTH_BOARD_ID,
                    name="kg-health-test",
                    owner_id=KG_HEALTH_USER_ID,
                )
            )
            await session.commit()

        await session.execute(
            ConsolidationQueue.__table__.delete().where(
                ConsolidationQueue.board_id == KG_HEALTH_BOARD_ID,
            )
        )
        await session.execute(
            ConsolidationDeadLetter.__table__.delete().where(
                ConsolidationDeadLetter.board_id == KG_HEALTH_BOARD_ID,
            )
        )
        await session.execute(
            ConsolidationAudit.__table__.delete().where(
                ConsolidationAudit.board_id == KG_HEALTH_BOARD_ID,
            )
        )
        await session.execute(
            KuzuNodeRef.__table__.delete().where(
                KuzuNodeRef.board_id == KG_HEALTH_BOARD_ID,
            )
        )
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == KG_HEALTH_BOARD_ID,
            )
        )
        await session.execute(KGTickRun.__table__.delete())
        await session.commit()

    reset_contradict_warn_counters()
    yield KG_HEALTH_BOARD_ID
    reset_contradict_warn_counters()


# --- TS1 / AC1: 10 fields in the health response ---


@pytest.mark.asyncio
async def test_health_response_carries_10_fields(db_factory, kg_health_board):
    """get_kg_health returns the contracted shape (FR1, BR1).

    Spec 28583299 (Ideação #4) extended this additively with
    last_decay_tick_at + nodes_recomputed_in_last_tick — HEALTH_SCHEMA_VERSION
    stays at "1.0" because no existing field changed semantics.
    """
    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    expected_fields = {
        "queue_depth",
        "oldest_pending_age_s",
        "dead_letter_count",
        "global_outbox_dead_letter_count",
        "total_nodes",
        "default_score_count",
        "default_score_ratio",
        "avg_relevance",
        "schema_version",
        "health_schema_version",
        "graph_schema_version",
        "contradict_warn_count",
        "last_decay_tick_at",
        "last_tick_status",
        "last_tick_error",
        "nodes_recomputed_in_last_tick",
        # Bug fix (Run tick now cross-mount race): expõe o estado do
        # advisory lock global ``kg_daily_tick`` para que o frontend
        # consiga desabilitar o botão mesmo se o usuário fechar o modal
        # e voltar enquanto o tick (cron OU manual) está rodando.
        "tick_in_progress",
        # FR5/FR6 (spec R2b, IMPL-3): board counters from last tick run.
        "boards_processed_in_last_tick",
        "boards_failed_in_last_tick",
        # KG-01 REST contract api_3ed9037f (required fields).
        "board_id",
        "graph_state",
        "discovery_state",
        "overall_state",
        "current_kg_generation_id",
        "metric_status",
        "classification_reason",
        "materialization_state",
        "materialization_generation",
        "probe_reason_codes",
        "correlation_id",
        "recent_events",
        "checked_at",
        # KG-01 internal / debug surface kept in addition to the contract
        # so dashboards built against the old 0.2.2 endpoint don't regress.
        "state",
        "memory_pressure_status",
        "classification_reasons",
        # Additive UI diagnosis: canonical state stays conservative, but the
        # dashboard can explain whether the board graph itself is queryable.
        "graph_read_status",
        "board_graph_queryable",
        "board_graph_recovery_required",
        "discovery_recovery_required",
        "discovery_health_cause",
        "primary_health_cause",
        "operator_action",
        "health_issues",
        # FR6 (spec R2c, IMPL-3): DLQ auto-drain telemetry (additive).
        "dlq_auto_drain_last_run_at",
        "dlq_auto_drain_requeued_count",
        # KG-HS.1 clarity payloads.
        "decay_scheduler_diagnostics",
        "storage_footprint_proxy",
        "native_runtime_budget",
        "probe_diagnostics",
        # KG-ZO-02 integrity debt projection.
        "orphan_integrity",
        # KG partitioning/canonical debt diagnostics.
        "kg_layer_counts",
        "canonical_debt",
        "rebuild_diagnostics",
        # R6-IMP2: active operational-queue drill-down (additive).
        "active_queue",
        # R6-IMP5: deduplicated 3-domain operational separation (additive).
        "operational_domains",
        # SPEC4 (card 2e913ac3): structured bounded recovery root-cause (additive).
        "root_cause",
        "source_count",
    }
    assert set(result.keys()) == expected_fields
    assert result["schema_version"] == "1.0"
    assert result["health_schema_version"] == HEALTH_SCHEMA_VERSION
    assert result["health_schema_version"] == "1.1"
    assert isinstance(result["queue_depth"], int)
    assert result["oldest_pending_age_s"] is None or isinstance(
        result["oldest_pending_age_s"], float
    )
    assert "top_disconnected_nodes" not in result
    assert result["last_decay_tick_at"] is None or isinstance(
        result["last_decay_tick_at"], str
    )
    assert result["last_tick_status"] is None or isinstance(
        result["last_tick_status"], str
    )
    assert result["last_tick_error"] is None or isinstance(
        result["last_tick_error"], str
    )
    assert isinstance(result["nodes_recomputed_in_last_tick"], int)
    assert isinstance(result["tick_in_progress"], bool)
    assert isinstance(result["boards_processed_in_last_tick"], int)
    assert isinstance(result["boards_failed_in_last_tick"], int)
    # KG-01 contract enforcement: states must use the 5 canonical values;
    # metric_status REST surface is strictly available|unavailable (partial
    # is internal and gets collapsed to unavailable);
    # memory_pressure_status is binary per TR3/TR4. With no real
    # instrumentation wired yet (KG-01.5 lands the sensors), every sensor
    # is None so metric_status=unavailable and overall_state degrades to
    # at_risk per BR br_2a8cdfdc "Health unavailable is not zero".
    canonical_states = {
        "healthy",
        "at_risk",
        "backpressure",
        "recovery_needed",
        "quarantined",
    }
    assert result["graph_state"] in canonical_states
    assert result["discovery_state"] in canonical_states
    assert result["overall_state"] in canonical_states
    assert result["state"] == result["overall_state"]
    assert result["metric_status"] in {"available", "unavailable"}
    assert result["memory_pressure_status"] in {
        "unconfirmed",
        "confirmed_primary_cause",
    }
    assert isinstance(result["classification_reasons"], list)
    assert isinstance(result["correlation_id"], str)
    assert len(result["correlation_id"]) > 0
    assert isinstance(result["classification_reason"], str)
    assert result["board_id"] == kg_health_board
    assert isinstance(result["graph_read_status"], str)
    assert isinstance(result["board_graph_queryable"], bool)
    assert isinstance(result["board_graph_recovery_required"], bool)
    assert isinstance(result["discovery_recovery_required"], bool)
    assert isinstance(result["discovery_health_cause"], str)
    assert isinstance(result["primary_health_cause"], str)
    assert isinstance(result["operator_action"], str)
    assert isinstance(result["health_issues"], list)
    assert result["current_kg_generation_id"] is None or isinstance(
        result["current_kg_generation_id"], str
    )
    assert isinstance(result["recent_events"], list)
    assert isinstance(result["checked_at"], str)
    assert isinstance(result["decay_scheduler_diagnostics"], dict)
    assert isinstance(result["storage_footprint_proxy"], dict)
    assert isinstance(result["native_runtime_budget"], dict)
    assert isinstance(result["orphan_integrity"], dict)
    assert isinstance(result["kg_layer_counts"], dict)
    assert isinstance(result["canonical_debt"], dict)
    assert isinstance(result["rebuild_diagnostics"], dict)
    assert result["decay_scheduler_diagnostics"]["graph_recovery_required"] is False
    assert result["storage_footprint_proxy"]["source"] == "runtime_capability"
    assert result["storage_footprint_proxy"]["is_direct_memory_telemetry"] is False
    assert result["native_runtime_budget"]["source"] == "runtime_capability"
    assert result["native_runtime_budget"]["is_direct_memory_telemetry"] is False


@pytest.mark.asyncio
async def test_native_budget_failure_stays_available_as_health_response(
    monkeypatch, db_factory, kg_health_board
):
    from okto_pulse.community.api.kg_health import KGHealthResponse
    from okto_pulse.core.kg.interfaces import get_kg_registry

    async with db_factory() as session:
        baseline = await get_kg_health(kg_health_board, session)

    runtime = get_kg_registry().graph_runtime_store

    def fail_budget_snapshot():
        raise RuntimeError(r"C:\private\board\graph.lbug secret")

    monkeypatch.setattr(runtime, "budget_snapshot", fail_budget_snapshot)
    async with db_factory() as session:
        degraded = await get_kg_health(kg_health_board, session)

    response = KGHealthResponse(**degraded)
    assert response.native_runtime_budget.status == "unavailable"
    assert (
        response.native_runtime_budget.unavailable_reason
        == "budget_snapshot_unavailable"
    )
    assert response.native_runtime_budget.is_direct_memory_telemetry is False
    assert response.memory_pressure_status == baseline["memory_pressure_status"]
    assert response.native_runtime_budget.requested.model_dump() == {
        "board_buffer_pool_mb": None,
        "global_buffer_pool_mb": None,
        "max_db_size_gb": None,
        "connection_pool_size": None,
    }
    assert "private" not in str(response.native_runtime_budget.model_dump()).lower()


@pytest.mark.asyncio
async def test_orphan_integrity_warning_is_at_risk_not_recovery_needed(
    monkeypatch, db_factory, kg_health_board
):
    """KG-ZO-02.3: orphan debt is actionable integrity debt, not corruption."""

    monkeypatch.setattr(
        kg_health_service,
        "_get_or_schedule_orphan_integrity_for_health",
        lambda **_: {
            "classification_delta": "at_risk",
            "integrity_warning": True,
            "orphan_count": 2,
            "orphan_count_by_type": {"Learning": 2},
            "samples": [
                {
                    "node_id": "learning_orphan_1",
                    "node_type": "Learning",
                    "writer_path": "cognitive_consolidation",
                    "source_artifact_ref": "bug:bug-1",
                    "source_resolution_status": "unresolved_source_ref",
                    "generation_id": "gen-test",
                    "reason": "zero_graph_degree",
                    "correlation_id": "corr-orphan-health",
                }
            ],
            "unresolved_reasons": {"unresolved_source_ref": 2},
            "allowlisted_root_count": 0,
            "generation_id": "gen-test",
            "correlation_id": "corr-orphan-health",
            "zero_orphan_validation": "pending_backfill",
            "reason": "orphan_count_gt_zero",
        },
    )
    monkeypatch.setattr(
        kg_health_service,
        "_aggregate_graph_metrics",
        lambda _board_id: {
            "total_nodes": 10,
            "default_score_count": 0,
            "avg_relevance": 0.75,
        },
    )
    monkeypatch.setattr(
        kg_health_service,
        "_get_graph_schema_version",
        lambda _board_id: "0.3.5",
    )
    monkeypatch.setattr(
        kg_health_service,
        "_probe_board_graph_telemetry",
        lambda **_kwargs: kg_health_service._telemetry_ok("board"),
    )
    monkeypatch.setattr(
        kg_health_service,
        "_probe_global_discovery_telemetry",
        lambda: kg_health_service._telemetry_ok("discovery"),
    )

    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    assert result["orphan_integrity"]["integrity_warning"] is True
    assert result["orphan_integrity"]["orphan_count"] == 2
    assert result["graph_state"] == "at_risk"
    assert result["overall_state"] == "at_risk"
    assert "graph:orphan_integrity_warning" in result["classification_reasons"]
    assert result["classification_reason"].count("orphan_integrity_warning") == 1
    orphan_issue = next(
        issue
        for issue in result["health_issues"]
        if issue["code"] == "orphan_integrity_warning"
    )
    assert orphan_issue["operator_action"] == "inspect_orphan_integrity_report"
    assert orphan_issue["reason"] == "orphan_count_gt_zero"


# --- KG-01 BR br_2a8cdfdc: "Health unavailable is not zero" ----------------------


@pytest.mark.asyncio
async def test_default_response_is_conservative_never_healthy(
    monkeypatch, db_factory, kg_health_board
):
    """When no real sensor data flows (KG-01.5 not landed yet) the
    endpoint MUST emit conservative state and metric_status. A composition
    bug that accidentally produced state=healthy would be masked silently
    without this guard. BR br_2a8cdfdc is the canonical rule.
    """
    from okto_pulse.core.services import kg_health_service as svc

    monkeypatch.setattr(
        svc,
        "_probe_board_graph_telemetry",
        lambda **_kwargs: svc._telemetry_unavailable("board"),
    )
    monkeypatch.setattr(
        svc,
        "_probe_global_discovery_telemetry",
        lambda: svc._telemetry_unavailable("discovery"),
    )

    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    assert result["graph_state"] != "healthy"
    assert result["discovery_state"] != "healthy"
    assert result["overall_state"] != "healthy"
    assert result["metric_status"] == "unavailable"
    assert "metric.unavailable" in result["classification_reason"]


@pytest.mark.asyncio
async def test_empty_graph_after_materialized_history_requires_recovery(
    monkeypatch, db_factory, kg_health_board
):
    """If SQLite audit proves prior KG materialization but Ladybug reports
    zero nodes, Health must surface recovery_needed instead of a generic
    at_risk/empty state.
    """
    now = datetime.now(timezone.utc)
    async with db_factory() as session:
        session.add(
            ConsolidationAudit(
                session_id=f"kgses_{uuid.uuid4().hex}",
                board_id=kg_health_board,
                artifact_id="artifact-with-prior-kg-materialization",
                artifact_type="spec",
                agent_id="test",
                started_at=now,
                committed_at=now,
                nodes_added=3,
                edges_added=1,
                summary_text="prior materialization evidence",
            )
        )
        await session.commit()

    from okto_pulse.core.services import kg_health_service as svc

    monkeypatch.setattr(
        svc,
        "_probe_board_graph_telemetry",
        lambda **_kwargs: svc._telemetry_unavailable("board"),
    )
    monkeypatch.setattr(
        svc,
        "_probe_global_discovery_telemetry",
        lambda: svc._telemetry_unavailable("discovery"),
    )

    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    assert result["total_nodes"] == 0
    assert result["graph_state"] == "recovery_needed"
    assert result["overall_state"] == "recovery_needed"
    assert "graph:empty_after_materialized_history" in result["classification_reason"]
    assert result["graph_read_status"] == "empty_after_materialized_history"
    assert result["board_graph_queryable"] is False
    assert result["board_graph_recovery_required"] is True
    assert result["primary_health_cause"] == "board_graph_recovery_required"
    assert result["operator_action"] == "run_local_offline_kg_recovery_executor"
    assert any(
        issue["code"] == "board_graph_empty_after_materialized_history"
        and issue["execution_mode"] == "recovery_only_offline"
        and issue["recovery_executor"] == "okto-pulse-kg-recovery-only"
        and "within 2 hours" in issue["remediation"]
        for issue in result["health_issues"]
    )


@pytest.mark.asyncio
async def test_health_stays_recovery_needed_with_actionable_drilldown(
    monkeypatch, db_factory, kg_health_board
):
    """SPEC4 card 89f61f6f / ts_f7c8bcdc (C6 — no false healthy): while the root
    cause (empty graph after prior materialization) persists, KG Health stays
    recovery_needed — never a false healthy — and exposes an actionable
    structured root-cause + per-domain drill-down so an agent can act without
    local-file forensics.
    """
    now = datetime.now(timezone.utc)
    async with db_factory() as session:
        session.add(
            ConsolidationAudit(
                session_id=f"kgses_{uuid.uuid4().hex}",
                board_id=kg_health_board,
                artifact_id="prior-materialization",
                artifact_type="spec",
                agent_id="test",
                started_at=now,
                committed_at=now,
                nodes_added=3,
                edges_added=1,
                summary_text="prior materialization evidence",
            )
        )
        await session.commit()

    from okto_pulse.core.services import kg_health_service as svc

    # Supply a completed zero-node sensor explicitly. The bounded production
    # probe must not infer "empty" from a cold-start timeout alone, because
    # that would turn latency into a false recovery signal.
    monkeypatch.setattr(
        svc,
        "_aggregate_graph_metrics",
        lambda _board_id: {
            "total_nodes": 0,
            "default_score_count": 0,
            "avg_relevance": 0.0,
        },
    )
    monkeypatch.setattr(
        svc,
        "_probe_board_graph_telemetry",
        lambda **_kwargs: svc._telemetry_unavailable("board"),
    )
    monkeypatch.setattr(
        svc,
        "_probe_global_discovery_telemetry",
        lambda: svc._telemetry_unavailable("discovery"),
    )

    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    # NO FALSE HEALTHY: an empty-after-materialized graph stays recovery_needed.
    assert result["overall_state"] == "recovery_needed"
    assert result["overall_state"] != "healthy"
    assert "graph:empty_after_materialized_history" in result["classification_reason"]
    # actionable structured root-cause (SPEC4 card 2e913ac3 block).
    root_cause = result["root_cause"]
    assert (
        root_cause["categories"]["empty_after_materialized_history"]["present"] is True
    )
    assert "empty_after_materialized_history" in root_cause["present_categories"]
    # actionable per-domain drill-down — every operational domain names a tool.
    assert result["operational_domains"]
    for domain in result["operational_domains"].values():
        assert domain.get("drill_down_tool")
    # an actionable health issue + operator action while the root cause persists.
    assert any(
        issue["code"] == "board_graph_empty_after_materialized_history"
        for issue in result["health_issues"]
    )
    assert result["operator_action"] == "run_local_offline_kg_recovery_executor"


@pytest.mark.asyncio
async def test_queryable_graph_with_unavailable_telemetry_is_not_recovery_required(
    monkeypatch, db_factory, kg_health_board
):
    """A readable board graph may still be `at_risk` because sensors are
    unavailable. The UI diagnosis must distinguish this from corruption so the
    operator is not pushed into an unnecessary rebuild.
    """
    from okto_pulse.core.services import kg_health_service as svc

    original_metrics = svc._aggregate_graph_metrics
    original_schema_version = svc._get_graph_schema_version

    def _metrics(_board_id):
        return {
            "total_nodes": 7,
            "default_score_count": 1,
            "avg_relevance": 0.61,
        }

    svc._aggregate_graph_metrics = _metrics
    svc._get_graph_schema_version = lambda _board_id: "0.3.5"
    monkeypatch.setattr(
        svc,
        "_probe_board_graph_telemetry",
        lambda **_kwargs: svc._telemetry_ok("board"),
    )
    monkeypatch.setattr(
        svc,
        "_probe_global_discovery_telemetry",
        lambda: svc._telemetry_unavailable("discovery"),
    )
    try:
        async with db_factory() as session:
            result = await get_kg_health(kg_health_board, session)
    finally:
        svc._aggregate_graph_metrics = original_metrics
        svc._get_graph_schema_version = original_schema_version

    assert result["total_nodes"] == 7
    assert result["graph_read_status"] == "queryable"
    assert result["board_graph_queryable"] is True
    assert result["board_graph_recovery_required"] is False
    assert result["metric_status"] == "unavailable"
    assert result["overall_state"] == "at_risk"
    assert result["primary_health_cause"] == "telemetry_unavailable"
    assert result["operator_action"] == "inspect_telemetry"
    issue_codes = {issue["code"] for issue in result["health_issues"]}
    assert "board_graph_queryable" in issue_codes
    assert "telemetry_unavailable" in issue_codes
    assert "board_graph_empty_after_materialized_history" not in issue_codes


@pytest.mark.asyncio
async def test_dead_letters_are_operational_debt_not_graph_rebuild_signal(
    monkeypatch, db_factory, kg_health_board
):
    """Dead-letter backlog should be explicit in Health, but it must not mark
    a queryable board graph as needing recovery by itself.
    """
    from okto_pulse.core.services import kg_health_service as svc

    async with db_factory() as session:
        session.add(
            ConsolidationDeadLetter(
                board_id=kg_health_board,
                artifact_type="spec",
                artifact_id="spec-dlq-diagnostic",
                attempts=3,
                errors=[
                    {
                        "attempt": 1,
                        "occurred_at": "2026-05-27T00:00:00Z",
                        "error_type": "TestError",
                        "message": "seeded for diagnostic test",
                    }
                ],
            )
        )
        await session.commit()

    original_metrics = svc._aggregate_graph_metrics
    original_schema_version = svc._get_graph_schema_version

    def _metrics(_board_id):
        return {
            "total_nodes": 3,
            "default_score_count": 0,
            "avg_relevance": 0.8,
        }

    svc._aggregate_graph_metrics = _metrics
    svc._get_graph_schema_version = lambda _board_id: "0.3.5"
    monkeypatch.setattr(
        svc,
        "_probe_board_graph_telemetry",
        lambda **_kwargs: svc._telemetry_ok("board"),
    )
    monkeypatch.setattr(
        svc,
        "_probe_global_discovery_telemetry",
        lambda: svc._telemetry_unavailable("discovery"),
    )
    try:
        async with db_factory() as session:
            result = await get_kg_health(kg_health_board, session)
    finally:
        svc._aggregate_graph_metrics = original_metrics
        svc._get_graph_schema_version = original_schema_version

    assert result["dead_letter_count"] >= 1
    assert result["board_graph_queryable"] is True
    assert result["board_graph_recovery_required"] is False
    assert any(
        issue["code"] == "dead_letter_backlog"
        and issue["operator_action"] == "inspect_dead_letters"
        for issue in result["health_issues"]
    )


@pytest.mark.asyncio
async def test_discovery_open_error_is_concrete_recovery_signal(
    monkeypatch, db_factory, kg_health_board
):
    """An existing unreadable discovery graph should not appear as vague
    telemetry-unavailable. It is a recovery signal independent of whether the
    current board graph is queryable.
    """
    from okto_pulse.core.services import kg_health_service as svc

    monkeypatch.setattr(
        svc,
        "_aggregate_graph_metrics",
        lambda _board_id: {
            "total_nodes": 5,
            "default_score_count": 0,
            "avg_relevance": 0.7,
        },
    )
    monkeypatch.setattr(svc, "_get_graph_schema_version", lambda _board_id: "0.3.5")
    monkeypatch.setattr(
        svc,
        "_probe_board_graph_telemetry",
        lambda **_kwargs: svc._telemetry_ok("board"),
    )
    monkeypatch.setattr(
        svc,
        "_probe_global_discovery_telemetry",
        lambda: svc._telemetry_wal_or_open_error("discovery"),
    )

    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    assert result["board_graph_queryable"] is True
    assert result["board_graph_recovery_required"] is False
    assert result["discovery_state"] == "recovery_needed"
    assert result["overall_state"] == "recovery_needed"
    # The legacy discovery probe is concrete, but this Core-only fixture has
    # no edition materialization-evidence provider. Schema 1.1 therefore keeps
    # the aggregate metric contract fail-closed while preserving the stronger
    # discovery recovery state.
    assert result["metric_status"] == "unavailable"
    assert result["discovery_recovery_required"] is True
    assert result["primary_health_cause"] == "discovery_recovery_required"
    assert result["operator_action"] == "run_explicit_global_discovery_recovery"
    assert any(
        issue["code"] == "discovery_recovery_required"
        for issue in result["health_issues"]
    )


# --- TS2 / AC2: 404 (BoardNotFoundError) for unknown board ---


@pytest.mark.asyncio
async def test_health_raises_for_nonexistent_board(db_factory):
    """Unknown board_id raises BoardNotFoundError so the route maps to 404."""
    async with db_factory() as session:
        with pytest.raises(BoardNotFoundError):
            await get_kg_health("ghost-board-id-does-not-exist", session)


# --- TS3 / AC3: REST + MCP shape parity (proxy via service layer) ---


@pytest.mark.asyncio
async def test_service_layer_response_matches_pydantic_model(
    db_factory, kg_health_board
):
    """The KGHealthResponse Pydantic model accepts the service dict as-is.

    AC3 expects MCP and REST to share a shape; with the MCP wire deferred,
    we prove parity by feeding the service output into the Pydantic model
    that the REST endpoint serializes — any drift would fail validation.
    """
    from okto_pulse.community.api.kg_health import KGHealthResponse

    async with db_factory() as session:
        data = await get_kg_health(kg_health_board, session)

    # Should construct without raising — proves shape parity.
    response = KGHealthResponse(**data)
    # And the dump round-trips into the same set of keys.
    assert set(response.model_dump().keys()) == set(data.keys())


def test_rest_health_issue_preserves_unavailable_probe_names():
    from okto_pulse.community.api.kg_health import HealthIssue

    issue = HealthIssue(
        code="health_probe_unavailable",
        component="kg_health",
        severity="warning",
        reason="health_probe_unavailable:graph_metrics",
        description="A bounded health probe did not finish in time.",
        operator_action="inspect_probe_diagnostics",
        probes=["graph_metrics"],
    )

    assert issue.model_dump()["probes"] == ["graph_metrics"]


@pytest.mark.asyncio
async def test_recent_completed_tick_produces_ok_scheduler_diagnostics(
    db_factory, kg_health_board
):
    """KG-HS.1 AC1/AC7 — recent success is explicit and legacy fields stay."""
    now = datetime.now(timezone.utc)
    next_run = now + timedelta(minutes=30)
    async with db_factory() as session:
        session.add(
            KGTickRun(
                tick_id=f"kg-hs-ok-{uuid.uuid4().hex}",
                started_at=now - timedelta(minutes=5),
                completed_at=now,
                nodes_recomputed=17,
                boards_processed=2,
                boards_failed=0,
            )
        )
        await session.commit()

    async with db_factory() as session:
        result = await get_kg_health(
            kg_health_board,
            session,
            scheduler_control=_HealthSchedulerControl(next_run_time=next_run),
        )

    diag = result["decay_scheduler_diagnostics"]
    assert diag["status"] == "ok"
    assert diag["severity"] == "info"
    assert diag["last_success_at"] is not None
    assert diag["next_scheduled_at"] == next_run.isoformat()
    assert diag["reason"] == "ok"
    assert diag["operational_debt"] is False
    assert diag["graph_recovery_required"] is False
    assert result["last_decay_tick_at"] == diag["last_success_at"]
    assert result["last_tick_status"] == "completed"
    assert result["nodes_recomputed_in_last_tick"] == 17


@pytest.mark.asyncio
async def test_next_scheduled_at_unavailable_preserves_tick_evidence(
    db_factory, kg_health_board
):
    """KG-HS.1 AC6 — scheduler metadata loss is not graph recovery."""
    now = datetime.now(timezone.utc)
    async with db_factory() as session:
        session.add(
            KGTickRun(
                tick_id=f"kg-hs-next-run-unavailable-{uuid.uuid4().hex}",
                started_at=now - timedelta(minutes=5),
                completed_at=now,
                nodes_recomputed=13,
                boards_processed=1,
                boards_failed=0,
            )
        )
        await session.commit()

    async with db_factory() as session:
        result = await get_kg_health(
            kg_health_board,
            session,
            scheduler_control=_HealthSchedulerControl(
                exists=False,
                message="scheduler_next_run_unavailable",
            ),
        )

    diag = result["decay_scheduler_diagnostics"]
    assert diag["status"] == "ok"
    assert diag["severity"] == "info"
    assert diag["next_scheduled_at"] is None
    assert diag["reason"] == "scheduler_next_run_unavailable"
    assert diag["last_success_at"] is not None
    assert diag["operational_debt"] is False
    assert diag["graph_recovery_required"] is False
    assert result["last_tick_status"] == "completed"
    assert result["nodes_recomputed_in_last_tick"] == 13


@pytest.mark.asyncio
async def test_no_tick_runs_produces_never_run_without_recovery(
    db_factory, kg_health_board
):
    """KG-HS.1 AC2 — absence of tick history is operational debt only."""
    async with db_factory() as session:
        await session.execute(KGTickRun.__table__.delete())
        await session.commit()

    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    diag = result["decay_scheduler_diagnostics"]
    assert diag["status"] == "never_run"
    assert diag["severity"] == "warning"
    assert diag["recommended_action"] == "run_tick_now"
    assert diag["operational_debt"] is True
    assert diag["graph_recovery_required"] is False
    assert any(
        issue["code"] == "decay_scheduler_never_run"
        and issue["operator_action"] == "run_tick_now"
        for issue in result["health_issues"]
    )


@pytest.mark.asyncio
async def test_stale_success_is_scheduler_debt_not_graph_recovery(
    db_factory, kg_health_board
):
    """KG-HS.1 AC3 — stale decay ticks are operational debt only."""
    now = datetime.now(timezone.utc)
    stale_success_at = now - timedelta(days=8)
    async with db_factory() as session:
        session.add(
            KGTickRun(
                tick_id=f"kg-hs-stale-{uuid.uuid4().hex}",
                started_at=stale_success_at - timedelta(minutes=4),
                completed_at=stale_success_at,
                nodes_recomputed=21,
                boards_processed=2,
                boards_failed=0,
            )
        )
        await session.commit()

    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    diag = result["decay_scheduler_diagnostics"]
    assert diag["status"] == "stale"
    assert diag["severity"] == "warning"
    assert diag["last_success_at"] is not None
    assert diag["stale_tolerance_seconds"] and diag["stale_tolerance_seconds"] > 0
    assert diag["recommended_action"] == "run_tick_now"
    assert diag["operational_debt"] is True
    assert diag["graph_recovery_required"] is False
    assert any(
        issue["code"] == "decay_scheduler_stale"
        and issue["operator_action"] == "run_tick_now"
        and "does not imply" in issue["description"].lower()
        for issue in result["health_issues"]
    )


@pytest.mark.asyncio
async def test_failed_tick_after_success_preserves_success_and_safe_error(
    db_factory, kg_health_board
):
    """KG-HS.1 AC4/TR11 — failure does not erase last success and errors are safe."""
    now = datetime.now(timezone.utc)
    success_at = now - timedelta(hours=2)
    failure_at = now - timedelta(minutes=5)
    async with db_factory() as session:
        session.add_all(
            [
                KGTickRun(
                    tick_id=f"kg-hs-success-{uuid.uuid4().hex}",
                    started_at=success_at - timedelta(minutes=3),
                    completed_at=success_at,
                    nodes_recomputed=11,
                    boards_processed=3,
                    boards_failed=0,
                ),
                KGTickRun(
                    tick_id=f"kg-hs-failed-{uuid.uuid4().hex}",
                    started_at=failure_at - timedelta(minutes=2),
                    completed_at=failure_at,
                    nodes_recomputed=0,
                    boards_processed=1,
                    boards_failed=1,
                    error='Traceback File "C:\\secret\\graph.lbug\\worker.py": boom',
                ),
            ]
        )
        await session.commit()

    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    diag = result["decay_scheduler_diagnostics"]
    assert diag["status"] == "failed"
    assert diag["last_success_at"] is not None
    assert diag["last_failure_at"] is not None
    assert diag["last_error"] == "scheduler_tick_failed"
    assert "C:\\" not in diag["last_error"]
    assert "Traceback" not in diag["last_error"]
    assert result["last_tick_status"] == "failed"
    assert result["last_tick_error"] == "scheduler_tick_failed"
    assert result["last_decay_tick_at"] == diag["last_failure_at"]


@pytest.mark.asyncio
async def test_running_tick_preserves_previous_checkpoint_without_recovery(
    db_factory, kg_health_board
):
    """KG-HS.1 running matrix — in-progress tick is not failure or recovery."""
    now = datetime.now(timezone.utc)
    success_at = now - timedelta(hours=1)
    running_started_at = now - timedelta(minutes=3)
    async with db_factory() as session:
        session.add_all(
            [
                KGTickRun(
                    tick_id=f"kg-hs-prev-{uuid.uuid4().hex}",
                    started_at=success_at - timedelta(minutes=3),
                    completed_at=success_at,
                    nodes_recomputed=9,
                    boards_processed=2,
                    boards_failed=0,
                ),
                KGTickRun(
                    tick_id=f"kg-hs-running-{uuid.uuid4().hex}",
                    started_at=running_started_at,
                    completed_at=None,
                    nodes_recomputed=0,
                    boards_processed=0,
                    boards_failed=0,
                ),
            ]
        )
        await session.commit()

    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    diag = result["decay_scheduler_diagnostics"]
    assert diag["status"] == "running"
    assert diag["severity"] == "info"
    assert diag["last_success_at"] is not None
    assert diag["running_started_at"] is not None
    assert diag["operational_debt"] is False
    assert diag["graph_recovery_required"] is False
    assert result["last_tick_status"] == "running"
    assert result["last_decay_tick_at"] == diag["last_success_at"]


@pytest.mark.asyncio
async def test_tick_evidence_query_failure_degrades_to_unknown_without_recovery(
    db_factory, kg_health_board, monkeypatch
):
    """KG-HS.1 AC5 — scheduler storage errors do not turn into graph recovery."""

    async def _failed_tick_evidence(_db):
        return {
            "query_failed": True,
            "query_error": "scheduler_tick_failed",
            "latest_success": None,
            "latest_failure": None,
            "latest_terminal": None,
            "running_row": None,
        }

    monkeypatch.setattr(
        kg_health_service,
        "_load_tick_evidence",
        _failed_tick_evidence,
    )

    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    diag = result["decay_scheduler_diagnostics"]
    assert diag["status"] == "unknown"
    assert diag["severity"] == "warning"
    assert diag["reason"] == "tick_run_query_failed"
    assert diag["recommended_action"] == "inspect_scheduler_storage"
    assert diag["last_error"] == "scheduler_tick_failed"
    assert diag["operational_debt"] is True
    assert diag["graph_recovery_required"] is False
    assert any(
        issue["code"] == "decay_scheduler_unknown"
        and issue["operator_action"] == "inspect_scheduler_storage"
        for issue in result["health_issues"]
    )


@pytest.mark.asyncio
async def test_storage_footprint_proxy_payload_is_not_direct_memory_telemetry(
    db_factory, kg_health_board
):
    """KG-HS.1 AC10 — footprint is adapter-provided, not direct memory telemetry."""
    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    proxy = result["storage_footprint_proxy"]
    assert proxy["source"] == "runtime_capability"
    assert proxy["is_direct_memory_telemetry"] is False
    copy = f"{proxy['description']} {proxy['tooltip']}".lower()
    assert "storage footprint" in copy
    assert "not live graph memory telemetry" in copy
    assert "buffer-pool telemetry" not in copy
    assert "direct memory pressure" not in copy
    assert "raw buffer pressure" not in copy


# --- TS4 / AC4: contradict cap preserves floor + structured log ---


def test_contradict_cap_preserves_floor_and_emits_log(caplog):
    """raw_sum=2.5 (5 contradicts × 0.5) is capped at 0.5 with a WARN log.

    Uses a stub graph transaction so we don't need a real graph; exercises
    the same code path as production (cap + counter + log + dict shape).
    """
    caplog.set_level(logging.WARNING, logger="okto_pulse.kg.scoring")
    reset_contradict_warn_counters()

    class _StubConn:
        def execute(self, _cypher, _params):
            return GraphStatementResult.from_rows(
                [
                    [
                        1.0,  # source_confidence
                        3,  # out_deg
                        2,  # in_deg
                        10,  # query_hits
                        None,  # last_queried_at
                        0.5,  # relevance_score
                        2.5,  # SUM(contradict_confidence) — way above cap
                        0.0,  # priority_boost
                    ]
                ]
            )

    inputs = _fetch_node_inputs(
        _StubConn(),
        "Decision",
        "decision_x",
        board_id=KG_HEALTH_BOARD_ID,
    )

    assert inputs is not None
    assert inputs["raw_contradict_penalty"] == 2.5
    assert inputs["contradict_penalty"] == CONTRADICT_PENALTY_CAP == 0.5
    assert any("contradict_penalty_capped" in rec.message for rec in caplog.records)
    reset_contradict_warn_counters()


# --- TS5 / AC5: contradict_warn_count increments per cap event ---


def test_contradict_warn_count_increments_on_cap_event():
    """Counter is per-board and increments only when raw_sum > cap."""
    reset_contradict_warn_counters()

    class _Stub:
        def __init__(self, penalty):
            self._penalty = penalty

        def execute(self, _c, _p):
            return GraphStatementResult.from_rows(
                [[1.0, 2, 1, 5, None, 0.5, self._penalty, 0.0]]
            )

    # Three nodes that trigger the cap.
    for _ in range(3):
        _fetch_node_inputs(
            _Stub(2.5),
            "Decision",
            "x",
            board_id=KG_HEALTH_BOARD_ID,
        )
    # One node BELOW the cap should not increment.
    _fetch_node_inputs(_Stub(0.2), "Decision", "y", board_id=KG_HEALTH_BOARD_ID)

    assert get_contradict_warn_count(KG_HEALTH_BOARD_ID) == 3
    reset_contradict_warn_counters()


# --- TS6 / AC6: decay reorder reverts stale ranking ---


def test_apply_decay_reorder_reverts_stale_ranking():
    """B (lower raw, fresh) ranks above A (higher raw, 60d stale)."""
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    old = (now - timedelta(days=60)).isoformat()
    fresh = (now - timedelta(days=1)).isoformat()
    rows = [
        {
            "node_id": "A",
            "relevance_score": 0.8,
            "query_hits": 10,
            "last_queried_at": old,
        },
        {
            "node_id": "B",
            "relevance_score": 0.7,
            "query_hits": 10,
            "last_queried_at": fresh,
        },
    ]
    result = _apply_decay_reorder(rows, top_k=2, now=now)

    assert [r["node_id"] for r in result] == ["B", "A"]
    assert result[0]["decayed_relevance"] > result[1]["decayed_relevance"]


def test_apply_decay_reorder_handles_edge_cases():
    """Empty input and top_k <= 0 return [] without raising."""
    assert _apply_decay_reorder([], top_k=5) == []
    assert _apply_decay_reorder([{"node_id": "z"}], top_k=0) == []


def test_pool_multiplier_constant_is_three():
    """Refinement decision: limit_pre = top_k * 3."""
    assert DECAY_REORDER_POOL_MULTIPLIER == 3


# --- TS7 / AC7: Cypher ORDER BY clauses unchanged (BR4) ---


def test_cypher_templates_order_by_relevance_score_unchanged():
    """The literal ORDER BY clauses in cypher_templates.py and
    kuzu_graph_store.py are preserved (BR4)."""
    repo_root = Path(__file__).parent.parent

    cypher_templates = (
        repo_root / "src" / "okto_pulse" / "core" / "kg" / "cypher_templates.py"
    ).read_text(encoding="utf-8")
    community_repo = community_repo_for(repo_root)
    kuzu_store = (
        community_repo
        / "src"
        / "okto_pulse"
        / "community"
        / "adapters"
        / "kuzu_graph_store.py"
    ).read_text(encoding="utf-8")

    assert "ORDER BY d.relevance_score DESC" in cypher_templates
    assert "ORDER BY l.relevance_score DESC" in cypher_templates
    assert "ORDER BY n.relevance_score DESC" in kuzu_store


# --- TS8 / AC8: default-score distribution alarm ---


@pytest.mark.asyncio
async def test_default_score_ratio_skew_emits_alarm_log(
    caplog, db_factory, kg_health_board
):
    """When ratio > threshold, a structured WARN is emitted.

    Without a populated graph, total_nodes is 0 so the ratio path
    cannot trigger via aggregations alone. We assert the threshold + log
    machinery by stubbing _aggregate_graph_metrics in the service module.
    """
    from okto_pulse.core.services import kg_health_service as svc

    caplog.set_level(logging.WARNING, logger="okto_pulse.services.kg_health")

    original = svc._aggregate_graph_metrics

    def _stub(_board_id):
        return {
            "total_nodes": 10,
            "default_score_count": 8,
            "avg_relevance": 0.5,
        }

    svc._aggregate_graph_metrics = _stub
    try:
        async with db_factory() as session:
            data = await get_kg_health(kg_health_board, session)
    finally:
        svc._aggregate_graph_metrics = original

    assert data["default_score_count"] == 8
    assert data["default_score_ratio"] == 0.8
    assert 0.8 > DEFAULT_SCORE_RATIO_ALARM_THRESHOLD
    assert any("default_score_skew_high" in rec.message for rec in caplog.records)


# --- TS9 / AC9: agent_instructions.md doc subseção complete ---


def test_agent_instructions_documents_kg_health_subsection():
    """The new subsection covers the 4 required topics (FR7, BR7) and
    primarily directs agents to the MCP tool (not the REST endpoint)."""
    repo_root = Path(__file__).parent.parent
    mcp_dir = repo_root / "src" / "okto_pulse" / "core" / "mcp"
    instr = (mcp_dir / "agent_instructions.md").read_text(encoding="utf-8")
    # R1.1: the always-loaded index keeps the KG-health subsection + stop-rule +
    # the MCP tool, and points to the mandatory lazy resource for the deep
    # payload contract (REST endpoint, contradict_penalty/decay fields,
    # when-to-consult). Assert against the COMBINED agent-facing surface.
    kg_health_res = (mcp_dir / "resources" / "reference" / "kg-health.md").read_text(
        encoding="utf-8"
    )
    doc = instr + "\n" + kg_health_res

    # The subsection + MCP tool stay in the always-loaded index.
    assert "KG health and operational signals" in instr
    assert "okto_pulse_kg_health" in instr
    assert "reference/kg-health" in instr  # mandatory resource pointer
    # The deep payload contract is discoverable in the combined surface.
    assert "/api/v1/kg/health" in doc
    assert "contradict_penalty" in doc.lower() or "CONTRADICT_PENALTY" in doc
    assert "decay" in doc.lower()
    assert "When to consult" in doc or "when to consult" in doc.lower()


# --- TS10 / AC10: queue_depth + oldest_pending_age_s populated correctly ---


@pytest.mark.asyncio
async def test_queue_metrics_reflect_pending_rows(db_factory, kg_health_board):
    """A pending row bumps queue_depth and exposes oldest_pending_age_s."""
    async with db_factory() as session:
        session.add(
            ConsolidationQueue(
                board_id=kg_health_board,
                artifact_type="spec",
                artifact_id="spec-health-test",
                priority="normal",
                source="test:health",
                status="pending",
            )
        )
        await session.commit()

    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    assert result["queue_depth"] >= 1
    assert result["oldest_pending_age_s"] >= 0.0


@pytest.mark.asyncio
async def test_dead_letter_metric_reflects_dlq_rows(db_factory, kg_health_board):
    """A dead-letter row bumps dead_letter_count."""
    async with db_factory() as session:
        session.add(
            ConsolidationDeadLetter(
                board_id=kg_health_board,
                artifact_type="spec",
                artifact_id="spec-dlq-test",
                attempts=3,
                errors=[
                    {
                        "attempt": 1,
                        "occurred_at": "2026-04-27T00:00:00Z",
                        "error_type": "TestError",
                        "message": "seeded for kg-health test",
                    }
                ],
            )
        )
        await session.commit()

    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    assert result["dead_letter_count"] >= 1


# --- Bonus: contradict band constants are coherent ---


def test_default_score_band_bounds():
    """The [low, high] band brackets the neutral 0.5."""
    assert DEFAULT_SCORE_BAND_LOW == 0.45
    assert DEFAULT_SCORE_BAND_HIGH == 0.55
    assert DEFAULT_SCORE_BAND_LOW <= 0.5 <= DEFAULT_SCORE_BAND_HIGH
    assert 0.0 < DEFAULT_SCORE_RATIO_ALARM_THRESHOLD < 1.0


# --- Single-flight do orphan scan (campo 2026-06-10, 4º crash) ---


def test_orphan_scan_single_flight_serves_stale_while_revalidating(monkeypatch):
    """Health requests concorrentes não podem empilhar scans de minutos em
    paralelo (cada um segura um leitor do board → bloqueia a higiene do
    buffer). Com um scan em andamento: quem chega recebe o cache stale (ou
    a projeção indisponível) SEM disparar um segundo scan."""
    import threading as _threading

    from okto_pulse.core.kg import orphan_integrity as oi
    from okto_pulse.core.services.kg_health_service import (
        _build_orphan_integrity_for_health,
        reset_orphan_projection_cache_for_tests,
    )

    board_id = "board-orphan-singleflight"
    reset_orphan_projection_cache_for_tests(board_id)

    scan_started = _threading.Event()
    release_scan = _threading.Event()
    scan_calls: list[str] = []

    class SlowScanner:
        def scan(self, *, board_id, generation_id=None):
            scan_calls.append(board_id)
            scan_started.set()
            assert release_scan.wait(10), "teste destravou o scan tarde demais"
            return None  # projection de report None = unavailable, ok p/ teste

    monkeypatch.setattr(oi, "OrphanNodeScanner", SlowScanner)

    results: dict[str, dict] = {}

    def first_caller():
        results["first"] = _build_orphan_integrity_for_health(
            board_id=board_id, generation_id=None
        )

    context = copy_context()
    t = _threading.Thread(target=context.run, args=(first_caller,))
    t.start()
    try:
        assert scan_started.wait(10), "primeiro scan nao comecou"
        # Segundo caller chega DURANTE o scan: não pode rodar outro scan.
        second = _build_orphan_integrity_for_health(
            board_id=board_id, generation_id=None
        )
        # Sem cache prévio, o segundo caller recebe a projeção indisponível
        # (scan_error vira reason=orphan_scan_unavailable no safe dict).
        assert second["reason"] == "orphan_scan_unavailable"
        assert scan_calls == [board_id], (
            f"single-flight violado: {len(scan_calls)} scans concorrentes"
        )
    finally:
        release_scan.set()
        t.join(timeout=10)

    reset_orphan_projection_cache_for_tests(board_id)


def test_health_first_read_schedules_orphan_scan_without_waiting(monkeypatch):
    """A cold read returns immediately and completed data becomes visible."""
    import threading as _threading
    import time as _time

    from okto_pulse.core.kg import orphan_integrity as oi
    from okto_pulse.core.kg.orphan_integrity import OrphanScanReport

    board_id = "board-orphan-background-refresh"
    kg_health_service.reset_orphan_projection_cache_for_tests(board_id)

    scan_started = _threading.Event()
    release_scan = _threading.Event()
    scan_calls: list[str] = []

    class SlowScanner:
        def scan(self, *, board_id: str, generation_id: str | None = None):
            scan_calls.append(board_id)
            scan_started.set()
            assert release_scan.wait(10), "test did not release background scan"
            return OrphanScanReport(
                board_id=board_id,
                generation_id=generation_id,
                orphan_count=0,
                orphan_count_by_type={},
                orphan_count_by_writer_path={},
                samples=(),
                unresolved_reasons={},
                allowlisted_root_count=0,
                correlation_id="background-refresh-complete",
            )

    monkeypatch.setattr(oi, "OrphanNodeScanner", SlowScanner)

    started_at = _time.perf_counter()
    first = kg_health_service._get_or_schedule_orphan_integrity_for_health(
        board_id=board_id,
        generation_id=None,
    )
    elapsed = _time.perf_counter() - started_at

    try:
        assert elapsed < 0.25
        assert first["reason"] == "orphan_scan_unavailable"
        assert scan_started.wait(2), "background scan was not scheduled"

        second = kg_health_service._get_or_schedule_orphan_integrity_for_health(
            board_id=board_id,
            generation_id=None,
        )
        assert second["reason"] == "orphan_scan_unavailable"
        assert scan_calls == [board_id]
    finally:
        release_scan.set()
        deadline = _time.monotonic() + 2
        while _time.monotonic() < deadline:
            with kg_health_service._ORPHAN_SCAN_INFLIGHT_LOCK:
                if board_id not in kg_health_service._ORPHAN_REFRESH_SCHEDULED:
                    break
            _time.sleep(0.01)

    completed = kg_health_service._get_or_schedule_orphan_integrity_for_health(
        board_id=board_id,
        generation_id=None,
    )
    assert completed["reason"] == "zero_non_allowlisted_orphans"
    assert completed["correlation_id"] == "background-refresh-complete"
    assert scan_calls == [board_id]
    kg_health_service.reset_orphan_projection_cache_for_tests(board_id)


def test_health_serves_stale_projection_during_single_background_refresh(
    monkeypatch,
):
    """Expired data is served immediately while exactly one refresh runs."""
    import threading
    import time

    from okto_pulse.core.kg import orphan_integrity as oi
    from okto_pulse.core.kg.orphan_integrity import OrphanScanReport

    board_id = "board-orphan-stale-while-revalidate"
    generation_id = "generation-stale"
    kg_health_service.reset_orphan_projection_cache_for_tests(board_id)
    stale = {"reason": "cached-stale", "generation_id": generation_id}
    with kg_health_service._ORPHAN_SCAN_INFLIGHT_LOCK:
        kg_health_service._ORPHAN_PROJECTION_CACHE[board_id] = (
            time.monotonic() - kg_health_service._ORPHAN_PROJECTION_TTL_S - 1,
            generation_id,
            stale,
        )

    scan_started = threading.Event()
    release_scan = threading.Event()
    calls: list[str] = []

    class SlowScanner:
        def scan(self, *, board_id: str, generation_id: str | None = None):
            calls.append(board_id)
            scan_started.set()
            assert release_scan.wait(10), "test did not release stale refresh"
            return OrphanScanReport(
                board_id=board_id,
                generation_id=generation_id,
                orphan_count=0,
                orphan_count_by_type={},
                orphan_count_by_writer_path={},
                samples=(),
                unresolved_reasons={},
                allowlisted_root_count=0,
                correlation_id="stale-refresh-complete",
            )

    monkeypatch.setattr(oi, "OrphanNodeScanner", SlowScanner)

    started_at = time.perf_counter()
    first = kg_health_service._get_or_schedule_orphan_integrity_for_health(
        board_id=board_id,
        generation_id=generation_id,
    )
    assert time.perf_counter() - started_at < 0.25
    assert first is stale
    assert scan_started.wait(2)

    second = kg_health_service._get_or_schedule_orphan_integrity_for_health(
        board_id=board_id,
        generation_id=generation_id,
    )
    assert second is stale
    assert calls == [board_id]

    release_scan.set()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        with kg_health_service._ORPHAN_SCAN_INFLIGHT_LOCK:
            if board_id not in kg_health_service._ORPHAN_REFRESH_SCHEDULED:
                break
        time.sleep(0.01)

    refreshed = kg_health_service._get_or_schedule_orphan_integrity_for_health(
        board_id=board_id,
        generation_id=generation_id,
    )
    assert refreshed["correlation_id"] == "stale-refresh-complete"
    assert calls == [board_id]
    kg_health_service.reset_orphan_projection_cache_for_tests(board_id)


def test_orphan_refresh_reset_invalidates_late_publish_and_cleanup(monkeypatch):
    """An old worker cannot publish or clear a newer refresh after reset."""
    import threading
    import time

    from okto_pulse.core.kg import orphan_integrity as oi
    from okto_pulse.core.kg.orphan_integrity import OrphanScanReport

    board_id = "board-orphan-reset-token"
    generation_id = "generation-reset"
    kg_health_service.reset_orphan_projection_cache_for_tests(board_id)

    scan_started = [threading.Event(), threading.Event()]
    release_scan = [threading.Event(), threading.Event()]
    build_done = [threading.Event(), threading.Event()]
    call_lock = threading.Lock()
    scan_index = 0
    build_index = 0

    class ResetScanner:
        def scan(self, *, board_id: str, generation_id: str | None = None):
            nonlocal scan_index
            with call_lock:
                index = scan_index
                scan_index += 1
            scan_started[index].set()
            assert release_scan[index].wait(10), "test did not release reset scan"
            return OrphanScanReport(
                board_id=board_id,
                generation_id=generation_id,
                orphan_count=0,
                orphan_count_by_type={},
                orphan_count_by_writer_path={},
                samples=(),
                unresolved_reasons={},
                allowlisted_root_count=0,
                correlation_id=f"reset-refresh-{index + 1}",
            )

    original_build = kg_health_service._build_orphan_integrity_for_health

    def tracked_build(**kwargs):
        nonlocal build_index
        with call_lock:
            index = build_index
            build_index += 1
        try:
            return original_build(**kwargs)
        finally:
            build_done[index].set()

    monkeypatch.setattr(oi, "OrphanNodeScanner", ResetScanner)
    monkeypatch.setattr(
        kg_health_service,
        "_build_orphan_integrity_for_health",
        tracked_build,
    )

    first = kg_health_service._get_or_schedule_orphan_integrity_for_health(
        board_id=board_id,
        generation_id=generation_id,
    )
    assert first["reason"] == "orphan_scan_unavailable"
    assert scan_started[0].wait(2)
    with kg_health_service._ORPHAN_SCAN_INFLIGHT_LOCK:
        old_refresh_token = kg_health_service._ORPHAN_REFRESH_SCHEDULED[board_id]

    kg_health_service.reset_orphan_projection_cache_for_tests(board_id)
    second = kg_health_service._get_or_schedule_orphan_integrity_for_health(
        board_id=board_id,
        generation_id=generation_id,
    )
    assert second["reason"] == "orphan_scan_unavailable"
    assert scan_started[1].wait(2)
    with kg_health_service._ORPHAN_SCAN_INFLIGHT_LOCK:
        new_refresh_token = kg_health_service._ORPHAN_REFRESH_SCHEDULED[board_id]
    assert new_refresh_token != old_refresh_token

    release_scan[0].set()
    assert build_done[0].wait(2)
    with kg_health_service._ORPHAN_SCAN_INFLIGHT_LOCK:
        assert kg_health_service._ORPHAN_REFRESH_SCHEDULED[board_id] == (
            new_refresh_token
        )
        assert kg_health_service._ORPHAN_PROJECTION_CACHE.get(board_id) is None

    release_scan[1].set()
    assert build_done[1].wait(2)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        with kg_health_service._ORPHAN_SCAN_INFLIGHT_LOCK:
            if board_id not in kg_health_service._ORPHAN_REFRESH_SCHEDULED:
                break
        time.sleep(0.01)

    completed = kg_health_service._get_or_schedule_orphan_integrity_for_health(
        board_id=board_id,
        generation_id=generation_id,
    )
    assert completed["correlation_id"] == "reset-refresh-2"
    kg_health_service.reset_orphan_projection_cache_for_tests(board_id)


def test_orphan_refresh_is_runtime_scoped_and_generation_aware(monkeypatch):
    """Caches do not leak between runtimes or across KG generations."""
    import threading
    import time

    from okto_pulse.core.kg import orphan_integrity as oi
    from okto_pulse.core.kg.orphan_integrity import OrphanScanReport
    from okto_pulse.core.runtime_context import (
        RuntimeValueRegistry,
        runtime_value_scope,
    )

    board_id = "board-orphan-runtime-scope"
    calls: list[tuple[str | None, str]] = []
    calls_lock = threading.Lock()

    class RuntimeScanner:
        def scan(self, *, board_id: str, generation_id: str | None = None):
            with calls_lock:
                correlation_id = f"runtime-refresh-{len(calls) + 1}"
                calls.append((generation_id, correlation_id))
            return OrphanScanReport(
                board_id=board_id,
                generation_id=generation_id,
                orphan_count=0,
                orphan_count_by_type={},
                orphan_count_by_writer_path={},
                samples=(),
                unresolved_reasons={},
                allowlisted_root_count=0,
                correlation_id=correlation_id,
            )

    monkeypatch.setattr(oi, "OrphanNodeScanner", RuntimeScanner)
    runtime_a = RuntimeValueRegistry()
    runtime_b = RuntimeValueRegistry()

    def wait_for_refresh(runtime: RuntimeValueRegistry) -> None:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with runtime_value_scope(runtime):
                with kg_health_service._ORPHAN_SCAN_INFLIGHT_LOCK:
                    if board_id not in kg_health_service._ORPHAN_REFRESH_SCHEDULED:
                        return
            time.sleep(0.01)
        raise AssertionError("background refresh did not complete")

    with runtime_value_scope(runtime_a):
        cold_a = kg_health_service._get_or_schedule_orphan_integrity_for_health(
            board_id=board_id,
            generation_id="generation-1",
        )
    assert cold_a["reason"] == "orphan_scan_unavailable"
    wait_for_refresh(runtime_a)
    with runtime_value_scope(runtime_a):
        warm_a = kg_health_service._get_or_schedule_orphan_integrity_for_health(
            board_id=board_id,
            generation_id="generation-1",
        )
    assert warm_a["correlation_id"] == "runtime-refresh-1"

    with runtime_value_scope(runtime_b):
        cold_b = kg_health_service._get_or_schedule_orphan_integrity_for_health(
            board_id=board_id,
            generation_id="generation-1",
        )
    assert cold_b["reason"] == "orphan_scan_unavailable"
    wait_for_refresh(runtime_b)
    with runtime_value_scope(runtime_b):
        warm_b = kg_health_service._get_or_schedule_orphan_integrity_for_health(
            board_id=board_id,
            generation_id="generation-1",
        )
    assert warm_b["correlation_id"] == "runtime-refresh-2"

    with runtime_value_scope(runtime_a):
        next_generation = (
            kg_health_service._get_or_schedule_orphan_integrity_for_health(
                board_id=board_id,
                generation_id="generation-2",
            )
        )
    assert next_generation["reason"] == "orphan_scan_unavailable"
    wait_for_refresh(runtime_a)
    with runtime_value_scope(runtime_a):
        warm_next_generation = (
            kg_health_service._get_or_schedule_orphan_integrity_for_health(
                board_id=board_id,
                generation_id="generation-2",
            )
        )
    assert warm_next_generation["correlation_id"] == "runtime-refresh-3"
    assert [generation for generation, _ in calls] == [
        "generation-1",
        "generation-1",
        "generation-2",
    ]


@pytest.mark.asyncio
async def test_get_kg_health_cold_read_does_not_wait_for_orphan_scan(
    monkeypatch,
    db_factory,
    kg_health_board,
):
    """The public service first read is bounded even if orphan IO is stuck."""
    import asyncio
    import threading
    import time

    from okto_pulse.core.kg import orphan_integrity as oi

    scan_started = threading.Event()
    release_scan = threading.Event()

    class StuckScanner:
        def scan(self, *, board_id: str, generation_id: str | None = None):
            del board_id, generation_id
            scan_started.set()
            assert release_scan.wait(10), "test did not release stuck orphan scan"
            return None

    monkeypatch.setattr(oi, "OrphanNodeScanner", StuckScanner)
    kg_health_service.reset_orphan_projection_cache_for_tests(kg_health_board)

    started_at = time.perf_counter()
    try:
        async with db_factory() as session:
            result = await asyncio.wait_for(
                get_kg_health(kg_health_board, session),
                timeout=2,
            )
        assert time.perf_counter() - started_at < 1.5
        assert result["orphan_integrity"]["reason"] == "orphan_scan_unavailable"
        assert scan_started.wait(2)
    finally:
        release_scan.set()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with kg_health_service._ORPHAN_SCAN_INFLIGHT_LOCK:
                if kg_health_board not in kg_health_service._ORPHAN_REFRESH_SCHEDULED:
                    break
            time.sleep(0.01)
        kg_health_service.reset_orphan_projection_cache_for_tests(kg_health_board)


def test_runtime_health_probe_workers_retire_and_restart_with_owner_context():
    """Runtime-local workers retire when idle and restart without losing context."""

    pool = kg_health_service._DaemonHealthProbePool(
        max_workers=2,
        max_queue_size=4,
        idle_timeout_s=0.02,
    )
    owner = kg_health_service._HealthProbeOwnerContext()

    first_token = owner.set(("board-a", (1, 2)))
    try:
        first = pool.submit(context=copy_context(), build=owner.get)
    finally:
        owner.reset(first_token)
    assert first is not None
    assert first.result(timeout=1) == ("board-a", (1, 2))

    deadline = time.monotonic() + 1
    while pool.active_worker_count() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert pool.active_worker_count() == 0

    second_token = owner.set(("board-b", (3, 4)))
    try:
        second = pool.submit(context=copy_context(), build=owner.get)
    finally:
        owner.reset(second_token)
    assert second is not None
    assert second.result(timeout=1) == ("board-b", (3, 4))
    assert pool.active_worker_count() > 0


def test_runtime_health_probe_pool_exposes_bounded_lifecycle_drain():
    """Teardown sees every queued/running daemon job before graph close."""

    started = threading.Event()
    release = threading.Event()
    pool = kg_health_service._DaemonHealthProbePool(
        max_workers=1,
        max_queue_size=2,
        idle_timeout_s=0.02,
    )

    def blocking_probe():
        started.set()
        assert release.wait(2), "test did not release health probe"
        return "done"

    future = pool.submit(context=copy_context(), build=blocking_probe)
    assert future is not None
    assert started.wait(1)
    assert pool.wait_until_idle(timeout_s=0.0) == 1

    release.set()
    assert future.result(timeout=1) == "done"
    assert pool.wait_until_idle(timeout_s=1.0) == 0


@pytest.mark.parametrize(
    ("blocked_probe", "expected_group"),
    [
        ("graph_metrics", "graph_snapshot"),
        ("schema_version", "graph_snapshot"),
        ("board_telemetry", "graph_snapshot"),
        ("storage_footprint", "graph_snapshot"),
        ("layer_counts", "graph_snapshot"),
        ("discovery_telemetry", "discovery_snapshot"),
        ("current_generation", "artifact_snapshot"),
        ("cognitive_items", "artifact_snapshot"),
        ("rebuild_source_diagnostics", "artifact_snapshot"),
        ("stale_canonical_parity", "parity_snapshot"),
        ("digest_layer_parity", "parity_snapshot"),
    ],
)
@pytest.mark.asyncio
async def test_each_blocking_health_probe_respects_endpoint_budget(
    monkeypatch,
    db_factory,
    kg_health_board,
    blocked_probe,
    expected_group,
):
    """Every synchronous/file-backed probe degrades without blocking health."""
    import asyncio
    import threading
    import time

    from okto_pulse.core.kg import stale_canonical_parity as stale_module
    from okto_pulse.core.kg.global_discovery import layer_parity as parity_module

    svc = kg_health_service
    started = threading.Event()
    release = threading.Event()

    safe_values = {
        "graph_metrics": {
            "total_nodes": 0,
            "default_score_count": 0,
            "avg_relevance": 0.0,
        },
        "schema_version": None,
        "board_telemetry": svc._telemetry_unavailable("board"),
        "storage_footprint": svc._unavailable_storage_footprint_proxy("test"),
        "layer_counts": svc._unavailable_kg_layer_counts("test"),
        "discovery_telemetry": svc._telemetry_unavailable("discovery"),
        "current_generation": ("generation-probe-test", "available", "ok"),
        "cognitive_items": (0, 0, "available"),
        "rebuild_source_diagnostics": {
            "source_count": 0,
            "canonical_source_count": 0,
            "working_source_count": 0,
            "enumeration_failure": False,
            "error": None,
        },
        "stale_canonical_parity": [],
        "digest_layer_parity": {
            "status": "available",
            "reason": "no_digests",
            "digests": [],
            "board_meta": {},
            "needs_overlay": False,
        },
    }
    targets = {
        "graph_metrics": (svc, "_aggregate_graph_metrics"),
        "schema_version": (svc, "_get_graph_schema_version"),
        "board_telemetry": (svc, "_probe_board_graph_telemetry"),
        "storage_footprint": (svc, "_build_storage_footprint_proxy"),
        "layer_counts": (svc, "_aggregate_kg_layer_counts"),
        "discovery_telemetry": (svc, "_probe_global_discovery_telemetry"),
        "current_generation": (svc, "_read_current_kg_generation"),
        "cognitive_items": (svc, "_read_cognitive_health_counts"),
        "rebuild_source_diagnostics": (svc, "_probe_rebuild_source_diagnostics"),
        "stale_canonical_parity": (stale_module, "detect_board_graph_stale"),
        "digest_layer_parity": (
            parity_module,
            "collect_digest_layer_mismatch_inputs",
        ),
    }

    monkeypatch.setattr(
        svc, "_aggregate_graph_metrics", lambda _board: safe_values["graph_metrics"]
    )
    monkeypatch.setattr(
        svc, "_get_graph_schema_version", lambda _board: safe_values["schema_version"]
    )
    monkeypatch.setattr(
        svc,
        "_probe_board_graph_telemetry",
        lambda **_kw: safe_values["board_telemetry"],
    )
    monkeypatch.setattr(
        svc,
        "_build_storage_footprint_proxy",
        lambda _board: safe_values["storage_footprint"],
    )
    monkeypatch.setattr(
        svc, "_aggregate_kg_layer_counts", lambda _board: safe_values["layer_counts"]
    )
    monkeypatch.setattr(
        svc,
        "_probe_global_discovery_telemetry",
        lambda: safe_values["discovery_telemetry"],
    )
    monkeypatch.setattr(
        svc,
        "_read_current_kg_generation",
        lambda _board: safe_values["current_generation"],
    )
    monkeypatch.setattr(
        svc,
        "_read_cognitive_health_counts",
        lambda _board: safe_values["cognitive_items"],
    )
    monkeypatch.setattr(
        svc,
        "_probe_rebuild_source_diagnostics",
        lambda _board: safe_values["rebuild_source_diagnostics"],
    )
    monkeypatch.setattr(
        stale_module,
        "detect_board_graph_stale",
        lambda _board: safe_values["stale_canonical_parity"],
    )
    monkeypatch.setattr(
        parity_module,
        "collect_digest_layer_mismatch_inputs",
        lambda _board: safe_values["digest_layer_parity"],
    )
    monkeypatch.setattr(
        svc,
        "_get_or_schedule_orphan_integrity_for_health",
        lambda **_kw: svc._orphan_projection_unavailable(scan_error="test"),
    )

    target, attribute = targets[blocked_probe]

    def blocked(*_args, **_kwargs):
        started.set()
        assert release.wait(10), "test did not release blocked health probe"
        return safe_values[blocked_probe]

    monkeypatch.setattr(target, attribute, blocked)
    svc.reset_orphan_projection_cache_for_tests(kg_health_board)

    started_at = time.perf_counter()
    try:
        async with db_factory() as session:
            result = await asyncio.wait_for(
                get_kg_health(kg_health_board, session),
                timeout=1.5,
            )
        assert time.perf_counter() - started_at < 1.25
        assert started.wait(1)
        diagnostic = result["probe_diagnostics"][expected_group]
        assert diagnostic["status"] == "unavailable"
        assert "probe_budget_exceeded" in diagnostic["reason"]
        assert result["metric_status"] == "unavailable"
        if blocked_probe in {"cognitive_items", "rebuild_source_diagnostics"}:
            assert result["current_kg_generation_id"] == "generation-probe-test"
            current_generation = result["probe_diagnostics"]["current_generation"]
            assert current_generation["status"] == "available"
            assert current_generation["reason"] == "ok"
            assert current_generation["source_status"] == "available"
            assert current_generation["snapshot_freshness"]["status"] == "unavailable"
            assert current_generation["snapshot_freshness"]["is_stale"] is False
        if blocked_probe == "digest_layer_parity":
            assert (
                result["probe_diagnostics"]["stale_canonical_parity"]["status"]
                == "available"
            )
    finally:
        release.set()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            with svc._HEALTH_PROBE_LOCK:
                if (expected_group, kg_health_board) not in svc._HEALTH_PROBE_INFLIGHT:
                    break
            time.sleep(0.01)
        svc.reset_orphan_projection_cache_for_tests(kg_health_board)


def test_snapshot_derived_diagnostic_marks_cached_cognitive_count_stale():
    """A source-success flag must not present a stale ledger count as live."""
    probe = kg_health_service._HealthProbeResult(
        value={"cognitive_pending_active": 1},
        status="stale",
        reason="refresh_scheduled",
        age_seconds=31.25,
        refresh_in_progress=True,
    )

    diagnostic = kg_health_service._snapshot_derived_diagnostic(
        source_status="available",
        source_reason="ok",
        snapshot=probe,
    )

    assert diagnostic["status"] == "stale"
    assert diagnostic["source_status"] == "available"
    assert diagnostic["reason"] == "refresh_scheduled"
    assert diagnostic["snapshot_freshness"] == {
        "status": "stale",
        "reason": "refresh_scheduled",
        "age_seconds": 31.25,
        "refresh_in_progress": True,
        "is_stale": True,
        "max_age_seconds": 30.0,
    }


@pytest.mark.asyncio
async def test_managed_health_probe_singleflight_and_stale_while_revalidate():
    import asyncio
    import threading
    import time

    svc = kg_health_service
    board_id = "managed-probe-singleflight"
    probe_name = "test_managed_probe"
    started = threading.Event()
    release = threading.Event()
    calls: list[str] = []

    def build():
        calls.append(board_id)
        started.set()
        assert release.wait(10)
        return {"value": "fresh"}

    request = svc._HealthProbeRequest(
        name=probe_name,
        board_id=board_id,
        generation_id="generation-1",
        build=build,
        fallback={"value": "fallback"},
        ttl_s=0.2,
    )
    svc._reset_health_probe_cache_for_tests(board_id)
    first, second = await asyncio.gather(
        svc._resolve_health_probe_batch((request,), budget_s=0.02),
        svc._resolve_health_probe_batch((request,), budget_s=0.02),
    )
    assert started.wait(1)
    assert first[probe_name].status == "unavailable"
    assert second[probe_name].status == "unavailable"
    assert calls == [board_id]

    release.set()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        with svc._HEALTH_PROBE_LOCK:
            if (probe_name, board_id) not in svc._HEALTH_PROBE_INFLIGHT:
                break
        await asyncio.sleep(0.01)
    warm = await svc._resolve_health_probe_batch((request,), budget_s=0)
    assert warm[probe_name].value == {"value": "fresh"}

    await asyncio.sleep(0.25)
    stale = await svc._resolve_health_probe_batch((request,), budget_s=0)
    assert stale[probe_name].status == "stale"
    assert stale[probe_name].value == {"value": "fresh"}
    svc._reset_health_probe_cache_for_tests(board_id)


@pytest.mark.asyncio
async def test_managed_health_probe_reset_rejects_late_worker_publication():
    import asyncio
    import threading

    svc = kg_health_service
    board_id = "managed-probe-reset"
    probe_name = "test_managed_probe_reset"
    started = [threading.Event(), threading.Event()]
    release = [threading.Event(), threading.Event()]
    call_lock = threading.Lock()
    call_index = 0

    def build():
        nonlocal call_index
        with call_lock:
            index = call_index
            call_index += 1
        started[index].set()
        assert release[index].wait(10)
        return {"value": f"refresh-{index + 1}"}

    request = svc._HealthProbeRequest(
        name=probe_name,
        board_id=board_id,
        generation_id="generation-reset",
        build=build,
        fallback={"value": "fallback"},
    )
    svc._reset_health_probe_cache_for_tests(board_id)
    first, future_one = svc._ensure_health_probe(request)
    assert first.status == "unavailable"
    assert future_one is not None
    assert started[0].wait(1)

    svc._reset_health_probe_cache_for_tests(board_id)
    second, future_two = svc._ensure_health_probe(request)
    assert second.status == "unavailable"
    assert future_two is not None
    assert started[1].wait(1)

    release[0].set()
    await asyncio.wrap_future(future_one)
    with svc._HEALTH_PROBE_LOCK:
        assert svc._HEALTH_PROBE_CACHE.get((probe_name, board_id)) is None
        assert svc._HEALTH_PROBE_INFLIGHT[(probe_name, board_id)][2] is future_two

    release[1].set()
    await asyncio.wrap_future(future_two)
    completed = svc._read_health_probe_result(
        request,
        fallback_reason="unexpected",
    )
    assert completed.status == "available"
    assert completed.value == {"value": "refresh-2"}
    svc._reset_health_probe_cache_for_tests(board_id)
