"""Tests for the KG health snapshot service and endpoint.

Spec 20f67c2a (Ideação #5). Covers FR1, FR2, FR3, FR4, FR5, FR8 and
ACs 1-10. Aligns with the Ideação #3 lesson: every scenario marked
``automated`` in the Pulse has a real pytest assertion in this file.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio

from okto_pulse.core.kg.scoring import (
    CONTRADICT_PENALTY_CAP,
    DECAY_REORDER_POOL_MULTIPLIER,
    _apply_decay_reorder,
    _fetch_node_inputs,
    get_contradict_warn_count,
    reset_contradict_warn_counters,
)
from okto_pulse.core.models.db import (
    Board,
    CanonicalDebt,
    ConsolidationAudit,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    KGTickRun,
    KuzuNodeRef,
)
import okto_pulse.core.services.kg_health_service as kg_health_service
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
    }
    assert set(result.keys()) == expected_fields
    assert result["schema_version"] == HEALTH_SCHEMA_VERSION
    assert result["health_schema_version"] == HEALTH_SCHEMA_VERSION
    assert result["schema_version"] == "1.0"
    assert isinstance(result["queue_depth"], int)
    assert isinstance(result["oldest_pending_age_s"], float)
    assert "top_disconnected_nodes" not in result
    assert result["last_decay_tick_at"] is None or isinstance(result["last_decay_tick_at"], str)
    assert result["last_tick_status"] is None or isinstance(result["last_tick_status"], str)
    assert result["last_tick_error"] is None or isinstance(result["last_tick_error"], str)
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
        "healthy", "at_risk", "backpressure", "recovery_needed", "quarantined",
    }
    assert result["graph_state"] in canonical_states
    assert result["discovery_state"] in canonical_states
    assert result["overall_state"] in canonical_states
    assert result["state"] == result["overall_state"]
    assert result["metric_status"] in {"available", "unavailable"}
    assert result["memory_pressure_status"] in {
        "unconfirmed", "confirmed_primary_cause",
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
    assert isinstance(result["orphan_integrity"], dict)
    assert isinstance(result["kg_layer_counts"], dict)
    assert isinstance(result["canonical_debt"], dict)
    assert isinstance(result["rebuild_diagnostics"], dict)
    assert result["decay_scheduler_diagnostics"]["graph_recovery_required"] is False
    assert result["storage_footprint_proxy"]["source"] == "file_size_proxy"
    assert result["storage_footprint_proxy"]["is_direct_memory_telemetry"] is False


@pytest.mark.asyncio
async def test_orphan_integrity_warning_is_at_risk_not_recovery_needed(
    monkeypatch, db_factory, kg_health_board
):
    """KG-ZO-02.3: orphan debt is actionable integrity debt, not corruption."""

    monkeypatch.setattr(
        kg_health_service,
        "_build_orphan_integrity_for_health",
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
        "_aggregate_kuzu_metrics",
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
    assert result["operator_action"] == "run_explicit_rebuild"
    assert any(
        issue["code"] == "board_graph_empty_after_materialized_history"
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

    monkeypatch.setattr(
        svc, "_probe_board_graph_telemetry",
        lambda **_kwargs: svc._telemetry_unavailable("board"),
    )
    monkeypatch.setattr(
        svc, "_probe_global_discovery_telemetry",
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
    assert root_cause["categories"]["empty_after_materialized_history"]["present"] is True
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
    assert result["operator_action"] == "run_explicit_rebuild"


@pytest.mark.asyncio
async def test_queryable_graph_with_unavailable_telemetry_is_not_recovery_required(
    monkeypatch, db_factory, kg_health_board
):
    """A readable board graph may still be `at_risk` because sensors are
    unavailable. The UI diagnosis must distinguish this from corruption so the
    operator is not pushed into an unnecessary rebuild.
    """
    from okto_pulse.core.services import kg_health_service as svc

    original_metrics = svc._aggregate_kuzu_metrics
    original_schema_version = svc._get_graph_schema_version

    def _metrics(_board_id):
        return {
            "total_nodes": 7,
            "default_score_count": 1,
            "avg_relevance": 0.61,
        }

    svc._aggregate_kuzu_metrics = _metrics
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
        svc._aggregate_kuzu_metrics = original_metrics
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
                errors=[{
                    "attempt": 1,
                    "occurred_at": "2026-05-27T00:00:00Z",
                    "error_type": "TestError",
                    "message": "seeded for diagnostic test",
                }],
            )
        )
        await session.commit()

    original_metrics = svc._aggregate_kuzu_metrics
    original_schema_version = svc._get_graph_schema_version

    def _metrics(_board_id):
        return {
            "total_nodes": 3,
            "default_score_count": 0,
            "avg_relevance": 0.8,
        }

    svc._aggregate_kuzu_metrics = _metrics
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
        svc._aggregate_kuzu_metrics = original_metrics
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
        "_aggregate_kuzu_metrics",
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
    assert result["metric_status"] == "available"
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
    from okto_pulse.core.api.kg_health import KGHealthResponse

    async with db_factory() as session:
        data = await get_kg_health(kg_health_board, session)

    # Should construct without raising — proves shape parity.
    response = KGHealthResponse(**data)
    # And the dump round-trips into the same set of keys.
    assert set(response.model_dump().keys()) == set(data.keys())


@pytest.mark.asyncio
async def test_recent_completed_tick_produces_ok_scheduler_diagnostics(
    db_factory, kg_health_board
):
    """KG-HS.1 AC1/AC7 — recent success is explicit and legacy fields stay."""
    now = datetime.now(timezone.utc)
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
        result = await get_kg_health(kg_health_board, session)

    diag = result["decay_scheduler_diagnostics"]
    assert diag["status"] == "ok"
    assert diag["severity"] == "info"
    assert diag["last_success_at"] is not None
    assert diag["operational_debt"] is False
    assert diag["graph_recovery_required"] is False
    assert result["last_decay_tick_at"] == diag["last_success_at"]
    assert result["last_tick_status"] == "completed"
    assert result["nodes_recomputed_in_last_tick"] == 17


@pytest.mark.asyncio
async def test_next_scheduled_at_unavailable_preserves_tick_evidence(
    db_factory, kg_health_board, monkeypatch
):
    """KG-HS.1 AC6 — scheduler metadata loss is not graph recovery."""
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(
        kg_health_service,
        "_read_next_scheduled_at",
        lambda: (None, "scheduler_next_run_unavailable"),
    )
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
        result = await get_kg_health(kg_health_board, session)

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
        session.add_all([
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
        ])
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
        session.add_all([
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
        ])
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
    """KG-HS.1 AC10 — proxy payload explicitly labels file-size semantics."""
    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    proxy = result["storage_footprint_proxy"]
    assert proxy["source"] == "file_size_proxy"
    assert proxy["is_direct_memory_telemetry"] is False
    copy = f"{proxy['description']} {proxy['tooltip']}".lower()
    assert "storage footprint" in copy or "file-size proxy" in copy
    assert "not live ladybug memory telemetry" in copy
    assert "buffer-pool telemetry" not in copy
    assert "direct memory pressure" not in copy
    assert "raw buffer pressure" not in copy


# --- TS4 / AC4: contradict cap preserves floor + structured log ---


def test_contradict_cap_preserves_floor_and_emits_log(caplog):
    """raw_sum=2.5 (5 contradicts × 0.5) is capped at 0.5 with a WARN log.

    Uses a stub Kùzu connection so we don't need a real graph; exercises
    the same code path as production (cap + counter + log + dict shape).
    """
    caplog.set_level(logging.WARNING, logger="okto_pulse.kg.scoring")
    reset_contradict_warn_counters()

    class _StubResult:
        def __init__(self, row):
            self._row = row
            self._consumed = False

        def has_next(self):
            return not self._consumed

        def get_next(self):
            self._consumed = True
            return self._row

        def close(self):
            pass

    class _StubConn:
        def execute(self, _cypher, _params):
            return _StubResult([
                1.0,    # source_confidence
                3,      # out_deg
                2,      # in_deg
                10,     # query_hits
                None,   # last_queried_at
                0.5,    # relevance_score
                2.5,    # SUM(contradict_confidence) — way above cap
                0.0,    # priority_boost
            ])

    inputs = _fetch_node_inputs(
        _StubConn(), "Decision", "decision_x", board_id=KG_HEALTH_BOARD_ID,
    )

    assert inputs is not None
    assert inputs["raw_contradict_penalty"] == 2.5
    assert inputs["contradict_penalty"] == CONTRADICT_PENALTY_CAP == 0.5
    assert any(
        "contradict_penalty_capped" in rec.message
        for rec in caplog.records
    )
    reset_contradict_warn_counters()


# --- TS5 / AC5: contradict_warn_count increments per cap event ---


def test_contradict_warn_count_increments_on_cap_event():
    """Counter is per-board and increments only when raw_sum > cap."""
    reset_contradict_warn_counters()

    class _Stub:
        def __init__(self, penalty):
            self._penalty = penalty
            self._consumed = False

        def execute(self, _c, _p):
            return self

        def has_next(self):
            return not self._consumed

        def get_next(self):
            self._consumed = True
            return [1.0, 2, 1, 5, None, 0.5, self._penalty, 0.0]

        def close(self):
            pass

    # Three nodes that trigger the cap.
    for _ in range(3):
        _fetch_node_inputs(
            _Stub(2.5), "Decision", "x", board_id=KG_HEALTH_BOARD_ID,
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
    kuzu_store = (
        repo_root / "src" / "okto_pulse" / "core" / "kg" / "providers"
        / "embedded" / "kuzu_graph_store.py"
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

    Without a populated Kùzu graph, total_nodes is 0 so the ratio path
    cannot trigger via aggregations alone. We assert the threshold + log
    machinery by stubbing _aggregate_kuzu_metrics in the service module.
    """
    from okto_pulse.core.services import kg_health_service as svc

    caplog.set_level(logging.WARNING, logger="okto_pulse.services.kg_health")

    original = svc._aggregate_kuzu_metrics

    def _stub(_board_id):
        return {
            "total_nodes": 10,
            "default_score_count": 8,
            "avg_relevance": 0.5,
        }

    svc._aggregate_kuzu_metrics = _stub
    try:
        async with db_factory() as session:
            data = await get_kg_health(kg_health_board, session)
    finally:
        svc._aggregate_kuzu_metrics = original

    assert data["default_score_count"] == 8
    assert data["default_score_ratio"] == 0.8
    assert 0.8 > DEFAULT_SCORE_RATIO_ALARM_THRESHOLD
    assert any(
        "default_score_skew_high" in rec.message for rec in caplog.records
    )


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
    kg_health_res = (
        mcp_dir / "resources" / "reference" / "kg-health.md"
    ).read_text(encoding="utf-8")
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

    t = _threading.Thread(target=first_caller)
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
