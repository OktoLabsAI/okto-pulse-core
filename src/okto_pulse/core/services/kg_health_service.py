"""KG health snapshot service — feeds /api/v1/kg/health.

Spec 20f67c2a (Ideação #5, FR1, FR2, BR1). Composes 10 fields into a
JSON payload describing the live state of a board's knowledge graph:

    * SQL aggregations against ConsolidationQueue + ConsolidationDeadLetter
      for queue depth, oldest pending age, and dead letter count.
    * Kùzu aggregations across all node tables for total_nodes, the count
      of nodes whose relevance_score is in the [0.45, 0.55] "default"
      band (sintoma de inflation), avg_relevance, and the top-N
      most-disconnected nodes (lowest degree).
    * In-process counter from scoring.get_contradict_warn_count for
      contradict_warn_count.
    * schema_version is a fixed string ("1.0") versioning the response
      payload independently of the Kùzu schema.

When Kùzu hasn't been bootstrapped for the board (or any aggregation
fails), Kùzu-derived fields gracefully degrade to zero and the response
still ships. The endpoint must never 500 on a healthy app DB.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.kg.health_state import (
    GraphTelemetry,
    HealthState,
    KGHealthStateClassifier,
    LockState,
    MetricStatus,
)
from okto_pulse.core.kg.memory_pressure import (
    MemoryPressureCorrelator,
    MemoryPressureStatus,
)

# KG-01 contract api_3ed9037f: REST surface restricts metric_status to
# `available|unavailable`. The internal classifier may produce `partial`
# (some sensors present, others missing). We collapse it to `unavailable`
# so callers can't observe an ambiguous intermediate value.
_REST_METRIC_STATUS_MAP = {
    MetricStatus.AVAILABLE: "available",
    MetricStatus.PARTIAL: "unavailable",
    MetricStatus.UNAVAILABLE: "unavailable",
}

# Precedence used to fold per-graph states into `overall_state`. Matches
# FR1 worst-case-wins: quarantined > recovery_needed > backpressure >
# at_risk > healthy.
_STATE_SEVERITY = {
    HealthState.HEALTHY: 0,
    HealthState.AT_RISK: 1,
    HealthState.BACKPRESSURE: 2,
    HealthState.RECOVERY_NEEDED: 3,
    HealthState.QUARANTINED: 4,
}
from okto_pulse.core.kg.scoring import get_contradict_warn_count
from okto_pulse.core.models.db import (
    Board,
    ConsolidationAudit,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    KGTickRun,
    KuzuNodeRef,
)

logger = logging.getLogger("okto_pulse.services.kg_health")


HEALTH_SCHEMA_VERSION = "1.0"

# Spec 20f67c2a (Ideação #5): "default score" band used to flag inflation
# sintoma. Nodes whose relevance_score falls in [0.45, 0.55] are likely
# stuck near the neutral default and don't reflect any real signal yet.
DEFAULT_SCORE_BAND_LOW = 0.45
DEFAULT_SCORE_BAND_HIGH = 0.55

# When the ratio of default-band nodes crosses this threshold the service
# emits a structured WARN log so observability tooling can flag the board.
DEFAULT_SCORE_RATIO_ALARM_THRESHOLD = 0.7

# How many "most disconnected" nodes the response surfaces.
TOP_DISCONNECTED_NODES_LIMIT = 10


class BoardNotFoundError(Exception):
    """Raised when the requested board does not exist."""


def _build_health_diagnostics(
    *,
    total_nodes: int,
    graph_schema_version: str | None,
    empty_after_materialized_history: bool,
    discovery_state: HealthState,
    discovery_reasons: list[str],
    rest_metric_status: str,
    dead_letter_count: int,
) -> dict[str, Any]:
    """Build UI-facing diagnosis without weakening the health state machine.

    ``metric_status=unavailable`` must still keep the canonical state at
    ``at_risk``. That is a conservative sensor classification, not proof that
    the board graph is empty or needs rebuild. These additive fields let the UI
    explain the distinction and avoid nudging operators into recovery when the
    graph is queryable and the only problems are telemetry/dead-letter debt.
    """

    if empty_after_materialized_history:
        graph_read_status = "empty_after_materialized_history"
        board_graph_queryable = False
        board_graph_recovery_required = True
    elif total_nodes > 0 and graph_schema_version:
        graph_read_status = "queryable"
        board_graph_queryable = True
        board_graph_recovery_required = False
    elif total_nodes > 0:
        graph_read_status = "queryable_schema_unknown"
        board_graph_queryable = True
        board_graph_recovery_required = False
    elif graph_schema_version:
        graph_read_status = "empty_bootstrapped"
        board_graph_queryable = True
        board_graph_recovery_required = False
    else:
        graph_read_status = "not_materialized_or_unreadable"
        board_graph_queryable = False
        board_graph_recovery_required = False

    issues: list[dict[str, Any]] = []
    if empty_after_materialized_history:
        issues.append({
            "code": "board_graph_empty_after_materialized_history",
            "component": "board_graph",
            "severity": "critical",
            "reason": "graph:empty_after_materialized_history",
            "description": (
                "SQLite audit/ref history shows prior KG materialization but "
                "LadybugDB currently returns zero nodes."
            ),
            "operator_action": "run_explicit_rebuild",
        })
    elif board_graph_queryable:
        issues.append({
            "code": "board_graph_queryable",
            "component": "board_graph",
            "severity": "info",
            "reason": graph_read_status,
            "description": (
                "The board graph opened and returned materialized content; "
                "this is not a board-graph recovery signal."
            ),
            "operator_action": "none",
        })

    if rest_metric_status == "unavailable":
        issues.append({
            "code": "telemetry_unavailable",
            "component": "health_telemetry",
            "severity": "warning",
            "reason": "metric_status:unavailable",
            "description": (
                "Health sensors are unavailable, so canonical state remains "
                "at_risk by policy even when the graph is queryable."
            ),
            "operator_action": "inspect_telemetry",
        })

    if dead_letter_count > 0:
        issues.append({
            "code": "dead_letter_backlog",
            "component": "consolidation_queue",
            "severity": "warning",
            "reason": "dead_letter_count_gt_zero",
            "description": (
                f"{dead_letter_count} consolidation dead-letter row(s) remain; "
                "this is operational debt, not by itself proof that the current "
                "board graph is corrupt."
            ),
            "operator_action": "inspect_dead_letters",
        })

    discovery_recovery_required = discovery_state in {
        HealthState.RECOVERY_NEEDED,
        HealthState.QUARANTINED,
    }
    discovery_health_cause = (
        ";".join(discovery_reasons) if discovery_reasons else discovery_state.value
    )
    if discovery_recovery_required:
        issues.append({
            "code": "discovery_recovery_required",
            "component": "global_discovery",
            "severity": "critical",
            "reason": discovery_health_cause,
            "description": (
                "The global discovery graph has a concrete recovery signal. "
                "This can make overall KG Health recovery_needed even when the "
                "current board graph is queryable."
            ),
            "operator_action": "run_explicit_global_discovery_recovery",
        })

    if board_graph_recovery_required:
        primary = "board_graph_recovery_required"
        operator_action = "run_explicit_rebuild"
    elif discovery_recovery_required:
        primary = "discovery_recovery_required"
        operator_action = "run_explicit_global_discovery_recovery"
    elif rest_metric_status == "unavailable":
        primary = "telemetry_unavailable"
        operator_action = "inspect_telemetry"
    elif dead_letter_count > 0:
        primary = "dead_letter_backlog"
        operator_action = "inspect_dead_letters"
    else:
        primary = "none"
        operator_action = "none"

    return {
        "graph_read_status": graph_read_status,
        "board_graph_queryable": board_graph_queryable,
        "board_graph_recovery_required": board_graph_recovery_required,
        "discovery_recovery_required": discovery_recovery_required,
        "discovery_health_cause": discovery_health_cause,
        "primary_health_cause": primary,
        "operator_action": operator_action,
        "health_issues": issues,
    }


def _telemetry_ok(graph_type: str) -> GraphTelemetry:
    return GraphTelemetry(
        graph_type=graph_type,
        buffer_utilization_pct=0.0,
        high_water_mark_pct=0.0,
        recent_buffer_errors=0,
        recent_wal_errors=0,
        recent_commit_errors=0,
    )


def _telemetry_unavailable(graph_type: str) -> GraphTelemetry:
    return GraphTelemetry(
        graph_type=graph_type,
        buffer_utilization_pct=None,
        high_water_mark_pct=None,
        recent_buffer_errors=None,
        recent_wal_errors=None,
        recent_commit_errors=None,
    )


def _telemetry_wal_or_open_error(graph_type: str) -> GraphTelemetry:
    return GraphTelemetry(
        graph_type=graph_type,
        buffer_utilization_pct=0.0,
        high_water_mark_pct=0.0,
        recent_buffer_errors=0,
        recent_wal_errors=1,
        recent_commit_errors=0,
    )


def _probe_board_graph_telemetry(
    *,
    board_id: str,
    total_nodes: int,
    graph_schema_version: str | None,
    empty_after_materialized_history: bool,
) -> GraphTelemetry:
    """Return current board graph liveness telemetry for KG Health.

    Ladybug does not expose buffer high-water metrics through the current
    Python API, but Health can still distinguish a successful open/read probe
    from an existing graph that cannot expose schema/content. Missing graphs
    with no materialized history remain `metric.unavailable`; existing graphs
    that fail schema/readability are surfaced as a concrete WAL/open error.
    """

    if empty_after_materialized_history:
        return _telemetry_wal_or_open_error("board")
    if graph_schema_version or total_nodes > 0:
        return _telemetry_ok("board")
    try:
        from okto_pulse.core.kg.schema import board_kuzu_path

        if board_kuzu_path(board_id).exists():
            return _telemetry_wal_or_open_error("board")
    except Exception:
        return _telemetry_unavailable("board")
    return _telemetry_unavailable("board")


def _probe_global_discovery_telemetry() -> GraphTelemetry:
    """Probe global discovery without bootstrapping or purging it.

    If the file exists and cannot be opened, this is a current recovery signal,
    not merely missing telemetry. If it is absent, the discovery metric is
    unavailable because there is no graph to inspect yet.
    """

    try:
        from okto_pulse.core.kg.global_discovery.schema import (
            _global_kuzu_path,
            open_global_connection,
        )
    except Exception:
        return _telemetry_unavailable("discovery")

    try:
        path = _global_kuzu_path()
    except Exception:
        return _telemetry_unavailable("discovery")
    if not path.exists():
        return _telemetry_unavailable("discovery")

    try:
        _db, conn = open_global_connection()
        try:
            res = conn.execute("CALL SHOW_TABLES() RETURN name")
            try:
                # Consuming one row is enough to prove the graph opens and the
                # catalog is readable. Empty catalog still means the storage
                # itself was readable.
                if res.has_next():
                    res.get_next()
            finally:
                res.close()
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return _telemetry_ok("discovery")
    except Exception as exc:
        logger.warning(
            "kg.health.discovery_probe_failed err=%s",
            exc,
            extra={
                "event": "kg.health.discovery_probe_failed",
                "error": str(exc),
            },
        )
        return _telemetry_wal_or_open_error("discovery")


async def get_kg_health(board_id: str, db: AsyncSession) -> dict[str, Any]:
    """Compose the /api/v1/kg/health payload for ``board_id``.

    Raises ``BoardNotFoundError`` when the board is not found in the
    SQLite app DB. All Kùzu-derived metrics degrade to zero on lookup
    errors — the endpoint never 500s for a transient Kùzu issue.
    """
    board = await db.get(Board, board_id)
    if board is None:
        raise BoardNotFoundError(f"board not found: {board_id}")

    now = datetime.now(timezone.utc)

    queue_depth = await db.scalar(
        select(func.count()).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.status.in_(["pending", "claimed"]),
        )
    ) or 0

    oldest_triggered = await db.scalar(
        select(func.min(ConsolidationQueue.triggered_at)).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.status.in_(["pending", "claimed"]),
        )
    )
    if oldest_triggered is not None:
        if oldest_triggered.tzinfo is None:
            oldest_triggered = oldest_triggered.replace(tzinfo=timezone.utc)
        oldest_pending_age_s = max(0.0, (now - oldest_triggered).total_seconds())
    else:
        oldest_pending_age_s = 0.0

    dead_letter_count = await db.scalar(
        select(func.count()).where(
            ConsolidationDeadLetter.board_id == board_id,
        )
    ) or 0

    last_tick_run = await db.scalar(
        select(KGTickRun)
        .where(KGTickRun.completed_at.is_not(None))
        .order_by(KGTickRun.completed_at.desc())
        .limit(1)
    )
    if last_tick_run is not None:
        last_completed = last_tick_run.completed_at
        if last_completed is not None and last_completed.tzinfo is None:
            last_completed = last_completed.replace(tzinfo=timezone.utc)
        last_decay_tick_at = (
            last_completed.isoformat() if last_completed is not None else None
        )
        nodes_recomputed_in_last_tick = int(last_tick_run.nodes_recomputed or 0)
    else:
        last_decay_tick_at = None
        nodes_recomputed_in_last_tick = 0
    if last_tick_run is not None:
        last_tick_status = "failed" if last_tick_run.error else "completed"
        last_tick_error = last_tick_run.error
    else:
        last_tick_status = None
        last_tick_error = None

    kuzu_metrics = _aggregate_kuzu_metrics(board_id)
    graph_schema_version = _get_graph_schema_version(board_id)

    default_score_count = kuzu_metrics["default_score_count"]
    total_nodes = kuzu_metrics["total_nodes"]
    default_score_ratio = (
        default_score_count / total_nodes if total_nodes > 0 else 0.0
    )

    if default_score_ratio > DEFAULT_SCORE_RATIO_ALARM_THRESHOLD:
        logger.warning(
            "kg.health.default_score_skew_high board=%s ratio=%.3f "
            "count=%d total=%d threshold=%.2f",
            board_id, default_score_ratio, default_score_count, total_nodes,
            DEFAULT_SCORE_RATIO_ALARM_THRESHOLD,
            extra={
                "event": "kg.health.default_score_skew_high",
                "board_id": board_id,
                "default_score_ratio": default_score_ratio,
                "default_score_count": default_score_count,
                "total_nodes": total_nodes,
                "threshold": DEFAULT_SCORE_RATIO_ALARM_THRESHOLD,
            },
        )

    # Bug fix (Playwright E2E reproduzido): se o usuário fechar o modal
    # enquanto o tick roda e voltar, o frontend perde o state local de
    # "running" e re-habilita o botão. Para o frontend conseguir desabilitar
    # através de remount, expomos o estado real do advisory lock global
    # ``("kg_daily_tick", "global")``. Reuso do lock que kg_tick.py e o
    # cron já consultam — single source of truth.
    from okto_pulse.core.kg.workers.advisory_lock import get_async_lock
    tick_lock = get_async_lock("kg_daily_tick", "global")
    tick_in_progress = tick_lock.locked()
    if tick_in_progress:
        last_tick_status = "running"

    # KG-01 FR1+FR2+FR3 (contract api_3ed9037f): per-graph classification
    # for board_graph and global_discovery, deterministic memory-pressure
    # correlator, and contract-shaped REST payload. Low-level Ladybug buffer
    # high-water metrics are not exposed by the current Python API, but the
    # Health surface must still distinguish "telemetry missing" from "current
    # graph opens and is readable" and from "existing graph failed to open".
    # The probes below therefore publish liveness telemetry: readable graphs
    # are AVAILABLE with zero recent errors; existing unreadable graphs emit a
    # concrete WAL/open error and become recovery_needed; missing graphs remain
    # unavailable. The advisory tick lock is NOT the single-writer storage lock
    # — it stays off the LockState surface to avoid false positives.
    classifier = KGHealthStateClassifier()
    null_lock = LockState(is_held=False, is_admin_lane=False, owner_token=None)
    correlation_id = uuid.uuid4().hex

    def _evaluate_graph(telemetry: GraphTelemetry):
        return classifier.evaluate(
            telemetries=[telemetry],
            lock_state=null_lock,
            quarantine_present=False,
            backpressure_rejecting=False,
            correlation_id=correlation_id,
        )

    empty_after_materialized_history = (
        total_nodes == 0
        and await _has_materialized_kg_history(db, board_id)
    )
    graph_telemetry = _probe_board_graph_telemetry(
        board_id=board_id,
        total_nodes=total_nodes,
        graph_schema_version=graph_schema_version,
        empty_after_materialized_history=empty_after_materialized_history,
    )
    discovery_telemetry = _probe_global_discovery_telemetry()

    graph_classification = _evaluate_graph(graph_telemetry)
    discovery_classification = _evaluate_graph(discovery_telemetry)

    graph_state = graph_classification.state
    if empty_after_materialized_history:
        graph_state = HealthState.RECOVERY_NEEDED

    overall_state = max(
        graph_state,
        discovery_classification.state,
        key=lambda s: _STATE_SEVERITY[s],
    )

    # Fold reasons from both graphs into a deterministic single string for
    # the contract field `classification_reason`. The reason tuple is kept
    # separately in `classification_reasons` (plural) for dashboards that
    # already consume it.
    combined_reasons: list[str] = []
    for prefix, classification in (
        ("graph", graph_classification),
        ("discovery", discovery_classification),
    ):
        for r in classification.reasons:
            combined_reasons.append(f"{prefix}:{r}")
    if empty_after_materialized_history:
        combined_reasons.append("graph:empty_after_materialized_history")
    classification_reason = (
        ";".join(combined_reasons) if combined_reasons else "no_signal"
    )

    # KG-01 REST contract restricts metric_status to {available, unavailable}.
    # If either graph's telemetry is unavailable we degrade the surface
    # value to "unavailable" (the worst-case wins). `partial` collapses to
    # `unavailable` per the mapping table above.
    rest_metric_status = "available"
    for c in (graph_classification, discovery_classification):
        mapped = _REST_METRIC_STATUS_MAP[c.metric_status]
        if mapped == "unavailable":
            rest_metric_status = "unavailable"
            break

    memory_pressure = MemoryPressureCorrelator().evaluate(
        samples=[],
        failures=[],
    )

    diagnostics = _build_health_diagnostics(
        total_nodes=total_nodes,
        graph_schema_version=graph_schema_version,
        empty_after_materialized_history=empty_after_materialized_history,
        discovery_state=discovery_classification.state,
        discovery_reasons=[
            f"discovery:{reason}" for reason in discovery_classification.reasons
        ],
        rest_metric_status=rest_metric_status,
        dead_letter_count=int(dead_letter_count),
    )

    # bug b4c6920c follow-up: wire `current_kg_generation_id` to the
    # KG-02.4 `KGGenerationRepository` file-backed pointer. Previously
    # this was hard-None, so the UI Recovery panel showed "no generation
    # yet" even after a successful rebuild promoted a UUID v4 generation
    # (the run audit had the id but the health endpoint didn't read it
    # back). Defensive: any IO/import failure degrades silently to None
    # so the health endpoint stays available (br_2a8cdfdc forbids 500s
    # on telemetry failure).
    current_kg_generation_id: str | None = None
    try:
        from pathlib import Path
        import tempfile
        from okto_pulse.core.kg.rebuild_generation import (
            KGGenerationRepository,
        )

        # Same base_dir as the REST endpoint wires (kg_rebuild.py).
        rebuild_base = Path(tempfile.gettempdir()) / "okto_pulse_kg_rebuild"
        current_kg_generation_id = KGGenerationRepository(
            base_dir=rebuild_base
        ).get_current(board_id)
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning(
            "kg.health.current_generation_lookup_failed board=%s err=%s",
            board_id, exc,
        )

    recent_events: list[dict[str, Any]] = []
    checked_at = now.isoformat()

    return {
        # --- KG-01 REST contract api_3ed9037f ---
        "board_id": board_id,
        "graph_state": graph_state.value,
        "discovery_state": discovery_classification.state.value,
        "overall_state": overall_state.value,
        "current_kg_generation_id": current_kg_generation_id,
        "metric_status": rest_metric_status,
        "classification_reason": classification_reason,
        "correlation_id": correlation_id,
        "recent_events": recent_events,
        "checked_at": checked_at,
        # --- Legacy / dashboard surface (backward compat) ---
        "queue_depth": int(queue_depth),
        "oldest_pending_age_s": round(oldest_pending_age_s, 3),
        "dead_letter_count": int(dead_letter_count),
        "total_nodes": total_nodes,
        "default_score_count": default_score_count,
        "default_score_ratio": round(default_score_ratio, 4),
        "avg_relevance": kuzu_metrics["avg_relevance"],
        "top_disconnected_nodes": kuzu_metrics["top_disconnected_nodes"],
        "schema_version": HEALTH_SCHEMA_VERSION,
        "health_schema_version": HEALTH_SCHEMA_VERSION,
        "graph_schema_version": graph_schema_version,
        "contradict_warn_count": get_contradict_warn_count(board_id),
        "last_decay_tick_at": last_decay_tick_at,
        "last_tick_status": last_tick_status,
        "last_tick_error": last_tick_error,
        "nodes_recomputed_in_last_tick": nodes_recomputed_in_last_tick,
        "tick_in_progress": tick_in_progress,
        # --- KG-01 internal/debug surface (alias to contract overall_state) ---
        "state": overall_state.value,
        "memory_pressure_status": memory_pressure.status.value,
        "classification_reasons": combined_reasons,
        # --- UI diagnosis surface (additive, does not weaken canonical state) ---
        **diagnostics,
    }


def _get_graph_schema_version(board_id: str) -> str | None:
    try:
        from okto_pulse.core.kg.kg_service import get_kg_service

        return get_kg_service().get_schema_version(board_id)
    except Exception as exc:
        logger.debug(
            "kg.health.graph_schema_lookup_failed board=%s err=%s",
            board_id, exc,
        )
        return None


async def _has_materialized_kg_history(db: AsyncSession, board_id: str) -> bool:
    """Return True when SQLite says the board had materialized KG content.

    If LadybugDB reports zero nodes while KuzuNodeRef/audit rows still show
    previous commits, the graph is not merely "empty"; it has lost visibility
    into previously materialized content and should be classified as recovery
    needed.
    """
    try:
        ref_count = await db.scalar(
            select(func.count()).where(KuzuNodeRef.board_id == board_id)
        ) or 0
        if int(ref_count) > 0:
            return True
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug(
            "kg.health.materialized_ref_probe_failed board=%s err=%s",
            board_id, exc,
        )

    try:
        nodes_added = await db.scalar(
            select(func.coalesce(func.sum(ConsolidationAudit.nodes_added), 0))
            .where(ConsolidationAudit.board_id == board_id)
        ) or 0
        return int(nodes_added) > 0
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug(
            "kg.health.materialized_audit_probe_failed board=%s err=%s",
            board_id, exc,
        )
        return False


def _aggregate_kuzu_metrics(board_id: str) -> dict[str, Any]:
    """Pull node-level aggregates from Kùzu for ``board_id``.

    Returns a dict with total_nodes, default_score_count, avg_relevance and
    top_disconnected_nodes. On any Kùzu error (board not bootstrapped,
    schema drift, lock contention) returns zeroed defaults plus an empty
    list so the health endpoint stays available.
    """
    try:
        from okto_pulse.core.kg.schema import NODE_TYPES, open_board_connection
    except Exception as exc:
        logger.warning(
            "kg.health.kuzu_import_failed board=%s err=%s",
            board_id, exc,
        )
        return _zero_kuzu_metrics()

    total_nodes = 0
    default_score_count = 0
    relevance_sum = 0.0
    relevance_n = 0
    disconnected: list[dict[str, Any]] = []

    try:
        with open_board_connection(board_id) as (_db, conn):
            for node_type in NODE_TYPES:
                try:
                    res = conn.execute(
                        f"MATCH (n:{node_type}) "
                        f"OPTIONAL MATCH (n)-[r_out]->() "
                        f"WITH n, COUNT(r_out) AS od "
                        f"OPTIONAL MATCH (n)<-[r_in]-() "
                        f"WITH n, od, COUNT(r_in) AS id_ "
                        f"RETURN n.id, n.relevance_score, od + id_ AS deg",
                        {},
                    )
                except Exception as exc:
                    logger.debug(
                        "kg.health.kuzu_query_failed board=%s type=%s err=%s",
                        board_id, node_type, exc,
                    )
                    continue
                while res.has_next():
                    row = res.get_next()
                    node_id = row[0]
                    rel = row[1]
                    deg = int(row[2] or 0)
                    total_nodes += 1
                    if rel is not None:
                        rel_f = float(rel)
                        relevance_sum += rel_f
                        relevance_n += 1
                        if DEFAULT_SCORE_BAND_LOW <= rel_f <= DEFAULT_SCORE_BAND_HIGH:
                            default_score_count += 1
                    disconnected.append(
                        {"id": node_id, "type": node_type, "degree": deg}
                    )
    except Exception as exc:
        logger.warning(
            "kg.health.kuzu_open_failed board=%s err=%s",
            board_id, exc,
        )
        return _zero_kuzu_metrics()

    disconnected.sort(key=lambda r: r["degree"])
    top_disconnected = disconnected[:TOP_DISCONNECTED_NODES_LIMIT]

    avg_relevance = (
        round(relevance_sum / relevance_n, 4) if relevance_n > 0 else 0.0
    )

    return {
        "total_nodes": total_nodes,
        "default_score_count": default_score_count,
        "avg_relevance": avg_relevance,
        "top_disconnected_nodes": top_disconnected,
    }


def _zero_kuzu_metrics() -> dict[str, Any]:
    return {
        "total_nodes": 0,
        "default_score_count": 0,
        "avg_relevance": 0.0,
        "top_disconnected_nodes": [],
    }
