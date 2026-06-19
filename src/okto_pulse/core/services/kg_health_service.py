"""KG health snapshot service — feeds /api/v1/kg/health.

Spec 20f67c2a (Ideação #5, FR1, FR2, BR1). Composes 10 fields into a
JSON payload describing the live state of a board's knowledge graph:

    * SQL aggregations against ConsolidationQueue + ConsolidationDeadLetter
      for queue depth, oldest pending age, and dead letter count.
    * Kùzu aggregations across all node tables for total_nodes, the count
      of nodes whose relevance_score is in the [0.45, 0.55] "default"
      band (sintoma de inflation), and avg_relevance.
    * In-process counter from scoring.get_contradict_warn_count for
      contradict_warn_count.
    * schema_version is a fixed string ("1.0") versioning the response
      payload independently of the Kùzu schema.

When Kùzu hasn't been bootstrapped for the board (or any aggregation
fails), Kùzu-derived fields gracefully degrade to zero and the response
still ships. The endpoint must never 500 on a healthy app DB.
"""

from __future__ import annotations

import asyncio
import logging
import re
import threading
import time
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
    HighWaterMarkSample,
    MemoryPressureCorrelator,
    MemoryPressureStatus,
)
from okto_pulse.core.kg.memory_pressure_collector import (
    get_failures,
    get_samples,
    record_sample,
)
from okto_pulse.core.kg.schema import board_kuzu_path
from okto_pulse.core.kg.scoring import get_contradict_warn_count
from okto_pulse.core.infra.config import get_settings
from okto_pulse.core.models.db import (
    Board,
    ConsolidationAudit,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    KGTickRun,
    KuzuNodeRef,
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

_SENSITIVE_ERROR_RE = re.compile(
    r"([A-Za-z]:\\|/[^ \t\r\n]+/|Traceback|File \"|\.lbug|\.py\b)",
    re.IGNORECASE,
)
_KG_DAILY_TICK_JOB_ID = "kg_daily_tick"


class BoardNotFoundError(Exception):
    """Raised when the requested board does not exist."""


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    normalized = _as_utc(value)
    return normalized.isoformat() if normalized is not None else None


def _safe_scheduler_error(value: Any) -> str | None:
    """Return a bounded, UI-safe scheduler error summary.

    The KG Health surface is user/agent facing. It must not expose local graph
    paths, Python file paths, stack frames, or payload bodies. When those appear,
    keep only a bounded reason code.
    """

    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    first_line = text.splitlines()[0].strip()
    if _SENSITIVE_ERROR_RE.search(text):
        return "scheduler_tick_failed"
    return first_line[:180]


def _read_decay_settings() -> tuple[int | None, str | None]:
    try:
        settings = get_settings()
        return int(settings.kg_decay_tick_interval_minutes) * 60, None
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning(
            "kg.health.decay_scheduler_settings_unavailable err=%s",
            exc,
            extra={"event": "kg.health.decay_scheduler_settings_unavailable"},
        )
        return None, "settings_unavailable"


def _read_next_scheduled_at() -> tuple[str | None, str | None]:
    try:
        from okto_pulse.core.kg.scheduler_singleton import get_scheduler

        scheduler = get_scheduler()
        if scheduler is None:
            return None, "scheduler_unavailable"
        job = scheduler.get_job(_KG_DAILY_TICK_JOB_ID)
        if job is None:
            return None, "scheduler_job_unavailable"
        return _iso(getattr(job, "next_run_time", None)), None
    except Exception as exc:
        logger.info(
            "kg.health.scheduler_next_run_unavailable reason=%s",
            exc.__class__.__name__,
            extra={
                "event": "kg.health.scheduler_next_run_unavailable",
                "reason": "scheduler_next_run_unavailable",
            },
        )
        return None, "scheduler_next_run_unavailable"


def _is_after(left: datetime | None, right: datetime | None) -> bool:
    left = _as_utc(left)
    right = _as_utc(right)
    if left is None:
        return False
    if right is None:
        return True
    return left > right


async def _load_tick_evidence(db: AsyncSession) -> dict[str, Any]:
    """Load independent tick facts from KGTickRun.

    This keeps success, failure, terminal legacy state, and in-progress rows as
    separate facts. A failed tick after a success must not erase the last
    successful scoring checkpoint.
    """

    try:
        latest_success = await db.scalar(
            select(KGTickRun)
            .where(KGTickRun.completed_at.is_not(None), KGTickRun.error.is_(None))
            .order_by(KGTickRun.completed_at.desc())
            .limit(1)
        )
        latest_failure = await db.scalar(
            select(KGTickRun)
            .where(KGTickRun.error.is_not(None))
            .order_by(KGTickRun.completed_at.desc(), KGTickRun.started_at.desc())
            .limit(1)
        )
        latest_terminal = await db.scalar(
            select(KGTickRun)
            .where(KGTickRun.completed_at.is_not(None))
            .order_by(KGTickRun.completed_at.desc())
            .limit(1)
        )
        running_row = await db.scalar(
            select(KGTickRun)
            .where(KGTickRun.completed_at.is_(None))
            .order_by(KGTickRun.started_at.desc())
            .limit(1)
        )
    except Exception as exc:
        return {
            "query_failed": True,
            "query_error": _safe_scheduler_error(exc) or "tick_run_query_failed",
            "latest_success": None,
            "latest_failure": None,
            "latest_terminal": None,
            "running_row": None,
        }

    return {
        "query_failed": False,
        "query_error": None,
        "latest_success": latest_success,
        "latest_failure": latest_failure,
        "latest_terminal": latest_terminal,
        "running_row": running_row,
    }


def _build_decay_scheduler_diagnostics(
    *,
    tick_evidence: dict[str, Any],
    tick_in_progress: bool,
    now: datetime,
) -> dict[str, Any]:
    tolerance_seconds, settings_reason = _read_decay_settings()
    next_scheduled_at, next_reason = _read_next_scheduled_at()
    latest_success = tick_evidence.get("latest_success")
    latest_failure = tick_evidence.get("latest_failure")
    running_row = tick_evidence.get("running_row")

    last_success_at = _as_utc(getattr(latest_success, "completed_at", None))
    failure_completed_at = _as_utc(getattr(latest_failure, "completed_at", None))
    failure_started_at = _as_utc(getattr(latest_failure, "started_at", None))
    last_failure_at = failure_completed_at or failure_started_at
    running_started_at = _as_utc(getattr(running_row, "started_at", None))

    reason = next_reason or "ok"
    status = "ok"
    severity = "info"
    recommended_action = "none"
    operational_debt = False
    last_error = _safe_scheduler_error(getattr(latest_failure, "error", None))

    if tick_evidence.get("query_failed"):
        status = "unknown"
        severity = "warning"
        reason = "tick_run_query_failed"
        recommended_action = "inspect_scheduler_storage"
        operational_debt = True
        last_error = tick_evidence.get("query_error") or "tick_run_query_failed"
    elif settings_reason is not None:
        status = "unknown"
        severity = "warning"
        reason = settings_reason
        recommended_action = "inspect_runtime_settings"
        operational_debt = True
    elif tick_in_progress or running_row is not None:
        status = "running"
        severity = "info"
        reason = "tick_in_progress"
        recommended_action = "wait_for_tick_completion"
        operational_debt = False
    elif last_success_at is None and last_failure_at is None:
        status = "never_run"
        severity = "warning"
        reason = "no_tick_run"
        recommended_action = "run_tick_now"
        operational_debt = True
    elif _is_after(last_failure_at, last_success_at):
        status = "failed"
        severity = "warning"
        reason = "latest_tick_failed"
        recommended_action = "inspect_last_failure"
        operational_debt = True
    elif (
        last_success_at is not None
        and tolerance_seconds is not None
        and (now - last_success_at).total_seconds() > tolerance_seconds
    ):
        status = "stale"
        severity = "warning"
        reason = "last_success_stale"
        recommended_action = "run_tick_now"
        operational_debt = True

    diagnostics = {
        "status": status,
        "severity": severity,
        "last_success_at": _iso(last_success_at),
        "last_failure_at": _iso(last_failure_at),
        "last_error": last_error if status in {"failed", "unknown"} else None,
        "next_scheduled_at": next_scheduled_at,
        "stale_tolerance_seconds": tolerance_seconds,
        "recommended_action": recommended_action,
        "operational_debt": operational_debt,
        "graph_recovery_required": False,
        "reason": reason,
        "running_started_at": _iso(running_started_at),
        "source": "kg_tick_runs",
    }
    logger.info(
        "kg.health.decay_scheduler_diagnostic status=%s severity=%s reason=%s "
        "operational_debt=%s graph_recovery_required=%s",
        diagnostics["status"],
        diagnostics["severity"],
        diagnostics["reason"],
        diagnostics["operational_debt"],
        diagnostics["graph_recovery_required"],
        extra={
            "event": "kg.health.decay_scheduler_diagnostic",
            "status": diagnostics["status"],
            "severity": diagnostics["severity"],
            "reason": diagnostics["reason"],
            "operational_debt": diagnostics["operational_debt"],
            "graph_recovery_required": diagnostics["graph_recovery_required"],
        },
    )
    return diagnostics


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

    # R6-IMP5: dead_letter_backlog is emitted ONCE below (the FR7 issue with its
    # own drill_down_tool). The earlier duplicate append (same code, no drill-down)
    # was removed so retries/re-evaluations never surface two dead_letter_backlog
    # health issues for the same backlog.

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

    # FR7 (spec 007d1308 / dec_68fd26a2): the dead-letter queue is its own
    # operational signal with its own drill-down tool, kept distinct from
    # cognitive pending and canonical debt.
    if dead_letter_count > 0:
        issues.append({
            "code": "dead_letter_backlog",
            "component": "consolidation_queue",
            "severity": "warning",
            "reason": "dead_letter_count_gt_zero",
            "description": (
                f"{dead_letter_count} consolidation row(s) are dead-lettered "
                "and need inspection/reprocess. Distinct from cognitive pending "
                "and canonical debt."
            ),
            "operator_action": "inspect_dead_letters",
            "drill_down_tool": "okto_pulse_kg_dead_letter_list",
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


# Cache TTL do orphan scan: o scan conta o grau de CADA node por tipo de
# relação (O(nodes × rel_types) queries Kùzu) — em campo (3927 nodes) uma
# única execução leva minutos e rodava NO EVENT LOOP a cada GET /kg/health
# (py-spy 2026-06-10: 28/30 dumps presos em _node_degree). A projeção é
# aditiva/observacional: minutos de defasagem são inofensivos.
#
# Single-flight (4º crash em campo, 2026-06-10): sem ele, health requests
# concorrentes (monitores + frontend) empilhavam scans de minutos em
# paralelo, mantendo SEMPRE um leitor ativo no board — o que (a) bloqueava
# a higiene periódica do buffer e (b) colidia com o caminho de close
# fail-open. Agora: 1 scan por board por vez; quem chega durante um scan
# recebe o último resultado cacheado mesmo expirado (stale-while-
# revalidate) ou a projeção indisponível quando nunca houve scan.
_ORPHAN_PROJECTION_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_ORPHAN_PROJECTION_TTL_S = 300.0
_ORPHAN_SCAN_INFLIGHT: set[str] = set()
_ORPHAN_SCAN_INFLIGHT_LOCK = threading.Lock()


def reset_orphan_projection_cache_for_tests(board_id: str | None = None) -> None:
    if board_id is None:
        _ORPHAN_PROJECTION_CACHE.clear()
        with _ORPHAN_SCAN_INFLIGHT_LOCK:
            _ORPHAN_SCAN_INFLIGHT.clear()
    else:
        _ORPHAN_PROJECTION_CACHE.pop(board_id, None)
        with _ORPHAN_SCAN_INFLIGHT_LOCK:
            _ORPHAN_SCAN_INFLIGHT.discard(board_id)


def _build_orphan_integrity_for_health(
    *, board_id: str, generation_id: str | None
) -> dict[str, Any]:
    """Return the KG-ZO-02 orphan integrity projection for Health.

    This is read-only. A scan failure is surfaced as an unavailable additive
    projection and must not mask hard graph recovery signals computed by the
    KG-01 health state machine.
    """

    from okto_pulse.core.kg.orphan_integrity import (
        OrphanNodeScanner,
        build_orphan_integrity_projection,
    )

    now = time.monotonic()
    cached = _ORPHAN_PROJECTION_CACHE.get(board_id)
    if cached is not None and now - cached[0] < _ORPHAN_PROJECTION_TTL_S:
        return cached[1]

    with _ORPHAN_SCAN_INFLIGHT_LOCK:
        if board_id in _ORPHAN_SCAN_INFLIGHT:
            # Outro scan deste board está em andamento: serve o stale (ou a
            # projeção indisponível) em vez de empilhar mais um leitor.
            if cached is not None:
                return cached[1]
            return build_orphan_integrity_projection(
                None,
                scan_error="ScanAlreadyInProgress",
            ).to_safe_dict()
        _ORPHAN_SCAN_INFLIGHT.add(board_id)

    try:
        report = OrphanNodeScanner().scan(
            board_id=board_id,
            generation_id=generation_id,
        )
        projection = build_orphan_integrity_projection(report).to_safe_dict()
        # Timestamp de CONCLUSÃO, não de início (review dcea02d): o scan
        # leva minutos; carimbar o início encurtava o TTL efetivo para
        # 300s − duração e, com scan ≥ TTL, gerava scans costas-com-costas
        # — um leitor quase perpétuo no board que starvava a higiene de
        # buffer. Com o carimbo no fim, há sempre 300s de janela sem scan.
        _ORPHAN_PROJECTION_CACHE[board_id] = (time.monotonic(), projection)
        return projection
    except Exception as exc:
        logger.debug(
            "kg.health.orphan_integrity_scan_unavailable board=%s err=%s",
            board_id, exc,
        )
        # Falhas NÃO são cacheadas — a próxima chamada re-tenta o scan.
        return build_orphan_integrity_projection(
            None,
            scan_error=type(exc).__name__,
        ).to_safe_dict()
    finally:
        with _ORPHAN_SCAN_INFLIGHT_LOCK:
            _ORPHAN_SCAN_INFLIGHT.discard(board_id)


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


def _compute_board_graph_high_water_mark_pct(board_id: str) -> float | None:
    """Compute the real high-water-mark percentage for the board's LadybugDB files.

    Sums the on-disk sizes of graph.lbug and all its siblings (e.g.
    graph.lbug.wal, graph.lbug.shm) using the same glob pattern as
    ``_fsync_board_graph_files`` in schema.py (lines 567-571), then
    divides by ``kg_kuzu_max_db_size_gb * 1024**3`` and clamps to [0, 100].

    Returns ``None`` (not 0.0) when:
    - graph.lbug does not exist (AC4: absent graph, not even 0.0).
    - any ``OSError`` is raised during ``Path.stat()`` (AC5: IO errors must
      not propagate to the health endpoint — TR2).

    This is the proxy for FR2 (spec R2c): real on-disk usage drives the
    HighWaterMarkSample fed to the correlator, so the correlator sees real
    pressure rather than the synthetic 0.0 placeholder that was previously
    hardcoded in ``_telemetry_ok``.
    """
    try:
        path = board_kuzu_path(board_id)
    except Exception as exc:
        logger.debug(
            "kg.health.hwm.path_resolution_failed board=%s err=%s",
            board_id, exc,
        )
        return None

    # AC4: absent graph.lbug → None, not 0.0
    if not path.exists():
        return None

    try:
        total_bytes = path.stat().st_size
    except OSError as exc:
        logger.debug(
            "kg.health.hwm.stat_failed board=%s path=%s err=%s",
            board_id, path, exc,
        )
        return None

    # Include siblings (e.g. .wal, .shm) — same glob as _fsync_board_graph_files
    for sibling in sorted(path.parent.glob(path.name + ".*")):
        try:
            total_bytes += sibling.stat().st_size
        except OSError as exc:
            logger.debug(
                "kg.health.hwm.sibling_stat_failed board=%s sibling=%s err=%s",
                board_id, sibling, exc,
            )
            # Partial read: still return None to avoid misleading partial sums
            return None

    try:
        settings = get_settings()
        max_bytes = settings.kg_kuzu_max_db_size_gb * 1024 ** 3
    except Exception as exc:
        logger.debug(
            "kg.health.hwm.config_failed board=%s err=%s",
            board_id, exc,
        )
        return None

    if max_bytes <= 0:
        return None

    pct = (total_bytes / max_bytes) * 100.0
    # Clamp to [0, 100]
    return max(0.0, min(100.0, pct))


def _build_storage_footprint_proxy(board_id: str) -> dict[str, Any]:
    """Build an explanatory file-size proxy payload for REST/MCP/UI.

    The underlying high-water calculation remains the existing on-disk size
    proxy. This object makes that explicit and intentionally does not expose
    filesystem paths.
    """

    base: dict[str, Any] = {
        "source": "file_size_proxy",
        "status": "unavailable",
        "percentage": None,
        "high_water_mark_pct": None,
        "graph_lbug_bytes": None,
        "sidecar_bytes": None,
        "total_bytes": None,
        "configured_max_db_size_bytes": None,
        "configured_max_db_size_gb": None,
        "is_direct_memory_telemetry": False,
        "description": (
            "On-disk storage footprint proxy derived from graph.lbug file sizes."
        ),
        "tooltip": (
            "This is not live Ladybug memory telemetry. It is a file-size proxy "
            "used as an early warning signal."
        ),
        "unavailable_reason": None,
    }

    try:
        settings = get_settings()
        max_bytes = int(settings.kg_kuzu_max_db_size_gb * 1024 ** 3)
        base["configured_max_db_size_gb"] = int(settings.kg_kuzu_max_db_size_gb)
        base["configured_max_db_size_bytes"] = max_bytes
    except Exception:
        base["unavailable_reason"] = "settings_unavailable"
        return base

    if max_bytes <= 0:
        base["unavailable_reason"] = "invalid_max_db_size"
        return base

    try:
        path = board_kuzu_path(board_id)
    except Exception:
        base["unavailable_reason"] = "path_resolution_failed"
        return base

    if not path.exists():
        base["unavailable_reason"] = "graph_lbug_absent"
        return base

    try:
        graph_bytes = int(path.stat().st_size)
        sidecar_bytes = 0
        for sibling in sorted(path.parent.glob(path.name + ".*")):
            sidecar_bytes += int(sibling.stat().st_size)
    except OSError:
        base["unavailable_reason"] = "stat_failed"
        return base

    total_bytes = graph_bytes + sidecar_bytes
    pct = max(0.0, min(100.0, (total_bytes / max_bytes) * 100.0))
    base.update({
        "status": "available",
        "percentage": pct,
        "high_water_mark_pct": pct,
        "graph_lbug_bytes": graph_bytes,
        "sidecar_bytes": sidecar_bytes,
        "total_bytes": total_bytes,
        "unavailable_reason": None,
    })
    return base


def _probe_board_graph_telemetry(
    *,
    board_id: str,
    total_nodes: int,
    graph_schema_version: str | None,
    empty_after_materialized_history: bool,
) -> GraphTelemetry:
    """Return current board graph liveness telemetry for KG Health.

    FR2 (spec R2c): when the graph exists (schema version known or nodes
    present), this probe now computes the REAL high-water-mark percentage
    from on-disk file sizes (graph.lbug + siblings) and records a
    HighWaterMarkSample via the collector so the MemoryPressureCorrelator
    receives real observations.  IO errors are swallowed with a DEBUG log
    and degrade to ``high_water_mark_pct=None`` (TR2: health never 500s).

    Missing graphs with no materialized history remain
    ``metric_status.unavailable``; existing unreadable graphs are surfaced
    as a concrete WAL/open error.
    """

    if empty_after_materialized_history:
        return _telemetry_wal_or_open_error("board")
    if graph_schema_version or total_nodes > 0:
        # FR2: compute real high_water_mark_pct from on-disk sizes and feed
        # the collector ring-buffer for the correlator.
        hwm_pct = _compute_board_graph_high_water_mark_pct(board_id)
        _record_board_hwm_sample(board_id, hwm_pct)
        return GraphTelemetry(
            graph_type="board",
            buffer_utilization_pct=0.0,
            high_water_mark_pct=hwm_pct,
            recent_buffer_errors=0,
            recent_wal_errors=0,
            recent_commit_errors=0,
        )
    try:
        if board_kuzu_path(board_id).exists():
            return _telemetry_wal_or_open_error("board")
    except Exception:
        return _telemetry_unavailable("board")
    return _telemetry_unavailable("board")


def _record_board_hwm_sample(board_id: str, hwm_pct: float | None) -> None:
    """Record a HighWaterMarkSample in the collector ring-buffer.

    Only records when ``hwm_pct`` is not None.  Non-blocking and non-raising
    (TR2: swallow any unexpected error so the health endpoint never 500s).
    """
    if hwm_pct is None:
        return
    try:
        record_sample(
            board_id,
            HighWaterMarkSample(
                timestamp=datetime.now(timezone.utc),
                high_water_mark_pct=hwm_pct,
                graph_type="board",
            ),
        )
    except Exception as exc:  # pragma: no cover — defensive
        logger.debug(
            "kg.health.hwm.record_sample_failed board=%s err=%s",
            board_id, exc,
        )


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

    # R6-IMP2: active operational-queue drill-down (ConsolidationQueue pending/
    # claimed + global_update_outbox retry-window rows). DLQ / canonical debt are
    # deliberately NOT counted here — that separation is R6-IMP5.
    from okto_pulse.core.services.queue_health_service import (
        get_active_queue_drilldown,
    )
    active_queue = await get_active_queue_drilldown(db, board_id)

    tick_evidence = await _load_tick_evidence(db)
    last_terminal_tick = tick_evidence.get("latest_terminal")
    if last_terminal_tick is not None:
        last_decay_tick_at = _iso(last_terminal_tick.completed_at)
        nodes_recomputed_in_last_tick = int(last_terminal_tick.nodes_recomputed or 0)
        # FR5 (spec R2b, IMPL-3): expose boards_processed / boards_failed from
        # the last tick run so callers can distinguish "tick failed before any
        # board ran" (boards_processed==0) from "processed N but M failed"
        # (boards_failed>0). KGTickRun.boards_failed exists since IMPL-2.
        boards_processed_in_last_tick: int = int(
            last_terminal_tick.boards_processed or 0
        )
        boards_failed_in_last_tick: int = int(last_terminal_tick.boards_failed or 0)
    else:
        last_decay_tick_at = None
        nodes_recomputed_in_last_tick = 0
        boards_processed_in_last_tick = 0  # BR5: default 0 when no tick_run
        boards_failed_in_last_tick = 0     # BR5: default 0 when no tick_run
    if last_terminal_tick is not None:
        last_tick_status = "failed" if last_terminal_tick.error else "completed"
        last_tick_error = _safe_scheduler_error(last_terminal_tick.error)
    else:
        last_tick_status = None
        last_tick_error = None

    # to_thread: ambos abrem conexões Kùzu e executam queries SÍNCRONAS —
    # rodando no event loop, qualquer board grande/lento congela o servidor
    # inteiro (py-spy 2026-06-10).
    kuzu_metrics = await asyncio.to_thread(_aggregate_kuzu_metrics, board_id)
    graph_schema_version = await asyncio.to_thread(
        _get_graph_schema_version, board_id
    )

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
    if tick_in_progress or tick_evidence.get("running_row") is not None:
        last_tick_status = "running"
    decay_scheduler_diagnostics = _build_decay_scheduler_diagnostics(
        tick_evidence=tick_evidence,
        tick_in_progress=tick_in_progress,
        now=now,
    )

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

    # FR1/TR1 (spec R2c): feed real ring-buffer observations to the
    # correlator instead of empty-list stubs.  The collector module keeps
    # per-board deques (maxlen 200 / 50) that telemetry writers populate
    # via record_sample/record_failure; get_samples/get_failures return
    # thread-safe snapshot lists so iteration here is race-free.
    memory_pressure = MemoryPressureCorrelator().evaluate(
        samples=get_samples(board_id),
        failures=get_failures(board_id),
    )

    health_diagnostics = _build_health_diagnostics(
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
    if decay_scheduler_diagnostics["operational_debt"]:
        health_diagnostics["health_issues"].append({
            "code": f"decay_scheduler_{decay_scheduler_diagnostics['status']}",
            "component": "decay_scheduler",
            "severity": decay_scheduler_diagnostics["severity"],
            "reason": f"decay_scheduler:{decay_scheduler_diagnostics['reason']}",
            "description": (
                "Decay scheduler has operational debt. This does not imply "
                "board graph corruption or require KG rebuild by itself."
            ),
            "operator_action": decay_scheduler_diagnostics["recommended_action"],
        })
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = "decay_scheduler_debt"
            health_diagnostics["operator_action"] = decay_scheduler_diagnostics[
                "recommended_action"
            ]

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

    # FR4 (spec R2c): populate recent_events with the FailureEvent that the
    # correlator used when memory_pressure is confirmed_primary_cause.  The
    # correlation_id in the payload is the join-key between the health
    # snapshot and the underlying failure event in observability storage
    # (contract api_3ed9037f / memory_pressure.py:17-19).
    recent_events: list[dict[str, Any]] = []
    if (
        memory_pressure.status is MemoryPressureStatus.CONFIRMED_PRIMARY_CAUSE
        and memory_pressure.failure_event is not None
    ):
        fe = memory_pressure.failure_event
        recent_events.append({
            "occurred_at": fe.timestamp.isoformat(),
            "event_type": fe.event_kind,
            "reason": memory_pressure.reason,
            "correlation_id": fe.correlation_id,
        })
        # When memory pressure is the confirmed primary cause we surface
        # the correlator's correlation_id (== FailureEvent.correlation_id)
        # as the canonical correlation_id for the entire health snapshot so
        # callers can join health rows directly to failure events.
        correlation_id = fe.correlation_id

    # FR6 (spec R2c): pull DLQ auto-drain stats from the in-process worker
    # singleton. Defensive: if the worker is not running (e.g. tests that
    # don't start the worker) the stats default to null/0 per BR5.
    dlq_auto_drain_last_run_at: str | None = None
    dlq_auto_drain_requeued_count: int = 0
    try:
        from okto_pulse.core.kg.workers.consolidation import get_consolidation_worker
        worker = get_consolidation_worker()
        drain_stats = worker.get_dlq_drain_stats(board_id)
        dlq_auto_drain_last_run_at = drain_stats["last_run_at"]
        dlq_auto_drain_requeued_count = drain_stats["requeued_count"]
    except Exception:
        pass  # defensive: health endpoint must never 500 on telemetry failure

    checked_at = now.isoformat()
    storage_footprint_proxy = _build_storage_footprint_proxy(board_id)
    # to_thread + cache TTL: o scan de órfãos é O(nodes × rel_types) em
    # queries Kùzu síncronas — era o bloqueador dominante do event loop.
    orphan_integrity = await asyncio.to_thread(
        _build_orphan_integrity_for_health,
        board_id=board_id,
        generation_id=current_kg_generation_id,
    )
    kg_layer_counts = await asyncio.to_thread(_aggregate_kg_layer_counts, board_id)
    try:
        from okto_pulse.core.services.canonical_debt_service import (
            summarize_canonical_debt,
        )

        canonical_debt = await summarize_canonical_debt(db, board_id)
    except Exception as exc:  # pragma: no cover - defensive health path
        logger.warning(
            "kg.health.canonical_debt_summary_failed board=%s err=%s",
            board_id, exc,
        )
        canonical_debt = {
            "open_count": 0,
            "retryable_count": 0,
            "blocked_count": 0,
            "retry_scheduled_count": 0,
            "terminal_count": 0,
            "by_state": {},
            "status": "unavailable",
        }
    if int(canonical_debt.get("open_count") or 0) > 0:
        health_diagnostics["health_issues"].append({
            "code": "canonical_debt_open",
            "component": "canonical_graph",
            "severity": "warning",
            "reason": "canonical_debt_open_count_gt_zero",
            "description": (
                f"{int(canonical_debt.get('open_count') or 0)} artifact(s) "
                "remain outside canonical consolidation and require retry or "
                "cognitive promotion."
            ),
            "operator_action": "inspect_canonical_debt",
            "drill_down_tool": "okto_pulse_kg_canonical_debt_list",
        })
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = "canonical_debt_open"
            health_diagnostics["operator_action"] = "inspect_canonical_debt"

    # FR7 (spec 007d1308 / dec_68fd26a2): cognitive pending is its OWN
    # operational signal with its own drill-down tool, kept separate from the
    # dead-letter queue and canonical debt. Defensive: the store read is
    # file-backed, so it runs off the event loop and any IO failure degrades
    # to 0 — the health endpoint must never 500 on telemetry failure
    # (br_2a8cdfdc).
    def _count_active_cognitive_pending(_board_id: str) -> int:
        from okto_pulse.core.kg.rebuild_audit import (
            CognitiveConsolidationItemStore,
            compute_status_counts,
            default_rebuild_base_dir,
        )

        store = CognitiveConsolidationItemStore(base_dir=default_rebuild_base_dir())
        gen = store.latest_generation(_board_id)
        if not gen:
            return 0
        counts = compute_status_counts(store.list_items(_board_id, gen))
        return (
            int(counts.get("pending", 0))
            + int(counts.get("in_progress", 0))
            + int(counts.get("failed", 0))
        )

    try:
        cognitive_pending_active = await asyncio.to_thread(
            _count_active_cognitive_pending, board_id,
        )
    except Exception as exc:  # pragma: no cover - defensive health path
        logger.warning(
            "kg.health.cognitive_pending_lookup_failed board=%s err=%s",
            board_id, exc,
        )
        cognitive_pending_active = 0
    if cognitive_pending_active > 0:
        health_diagnostics["health_issues"].append({
            "code": "cognitive_consolidation_pending",
            "component": "cognitive_consolidation",
            "severity": "info",
            "reason": "cognitive_pending_active_count_gt_zero",
            "description": (
                f"{cognitive_pending_active} cognitive consolidation item(s) "
                "are pending/in_progress/failed and await agent action. "
                "Distinct from the dead-letter queue and canonical debt."
            ),
            "operator_action": "inspect_cognitive_pending",
            "drill_down_tool": "okto_pulse_kg_list_cognitive_pending_items",
        })
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = (
                "cognitive_consolidation_pending"
            )
            health_diagnostics["operator_action"] = "inspect_cognitive_pending"

    # FR6 / AC5 (R7): canonical Learning partition integrity is exposed as ONE
    # AGGREGATE health issue. Per-node detail (mixed-evidence deferred,
    # provenance-only observed) lives ONLY in the read-only drilldown — Health
    # uses cheap COUNTs (a SQL count + a file-backed store read off the event
    # loop), never a graph scan, so it stays light per tick. Counts are disjoint
    # (a go-forward HOLD and a historical DEBT are mutually exclusive per
    # artifact), so there is no internal double-count; precedence_explanation
    # documents the relationship to the broader canonical_debt_open /
    # cognitive_consolidation_pending signals.
    def _count_r7_partition_cognitive_pending(_board_id: str) -> int:
        from okto_pulse.core.kg.cognitive_readiness import R7_HOLD_REASON_CODES
        from okto_pulse.core.kg.rebuild_audit import (
            CognitiveConsolidationItemStore,
            default_rebuild_base_dir,
        )

        store = CognitiveConsolidationItemStore(base_dir=default_rebuild_base_dir())
        gen = store.latest_generation(_board_id)
        if not gen:
            return 0
        active = {"pending", "in_progress", "failed"}
        return sum(
            1
            for it in store.list_items(_board_id, gen)
            if it.status in active
            and str(getattr(it, "reason_code", "") or "") in R7_HOLD_REASON_CODES
        )

    partition_debt_open = 0
    try:
        from sqlalchemy import func as _func
        from sqlalchemy import select as _select

        from okto_pulse.core.kg.canonical_learning_partition import (
            PARTITION_TARGET_STATUS,
        )
        from okto_pulse.core.models.db import CanonicalDebt as _CanonicalDebt
        from okto_pulse.core.services.canonical_debt_service import (
            OPEN_STATES as _OPEN_STATES,
        )

        partition_debt_open = int(
            await db.scalar(
                _select(_func.count())
                .select_from(_CanonicalDebt)
                .where(
                    _CanonicalDebt.board_id == board_id,
                    _CanonicalDebt.target_status == PARTITION_TARGET_STATUS,
                    _CanonicalDebt.canonical_state.in_(tuple(_OPEN_STATES)),
                )
            )
            or 0
        )
    except Exception as exc:  # pragma: no cover - defensive health path
        logger.warning(
            "kg.health.partition_integrity_debt_failed board=%s err=%s",
            board_id, exc,
        )
        partition_debt_open = 0
    try:
        partition_cognitive_pending = await asyncio.to_thread(
            _count_r7_partition_cognitive_pending, board_id,
        )
    except Exception as exc:  # pragma: no cover - defensive health path
        logger.warning(
            "kg.health.partition_integrity_cognitive_failed board=%s err=%s",
            board_id, exc,
        )
        partition_cognitive_pending = 0
    partition_blocking = partition_debt_open + partition_cognitive_pending
    if partition_blocking > 0:
        health_diagnostics["health_issues"].append({
            "code": "canonical_partition_integrity",
            "component": "canonical_graph",
            "severity": "warning",
            "reason": "canonical_partition_integrity_open_gt_zero",
            "description": (
                f"{partition_blocking} canonical Learning partition-integrity "
                "signal(s): bug-derived canonical Learning lacking canonical Bug "
                "evidence (go-forward holds + historical remediation debt). "
                "Per-node detail is in the drilldown."
            ),
            "operator_action": "inspect_canonical_partition_integrity",
            "drill_down_tool": "okto_pulse_kg_canonical_partition_integrity_list",
            "counts": {
                "cognitive_pending": partition_cognitive_pending,
                "canonical_debt": partition_debt_open,
            },
            "precedence_explanation": (
                "Aggregate of R7 go-forward cognitive_pending (IMP1) + historical "
                "canonical_debt (IMP2), which are mutually exclusive per artifact "
                "(no internal double-count). These items are also reflected in the "
                "broader cognitive_consolidation_pending / canonical_debt_open "
                "signals; this entry is the partition-integrity (R7) lens, not an "
                "additional blocker. DLQ is counted separately."
            ),
        })
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = (
                "canonical_partition_integrity"
            )
            health_diagnostics["operator_action"] = (
                "inspect_canonical_partition_integrity"
            )

    # R2-IMP4 — stale_canonical_parity: canonical DETERMINISTIC board-graph nodes
    # whose SQL source regressed below canonical eligibility (read-only diagnostic;
    # never demotes/reconciles/syncs). DISTINCT category. Ranked BELOW
    # canonical_debt_open / cognitive_consolidation_pending /
    # canonical_partition_integrity (and any DLQ/operational failure) so it NEVER
    # masks an R7/debt/cognitive blocker, and ABOVE digest_vs_board_layer_mismatch
    # (the source regression is the more specific cause; the digest mismatch is its
    # R1 consequence).
    try:
        from okto_pulse.core.kg.stale_canonical_parity import (
            list_stale_canonical_parity,
        )
        stale_parity = await list_stale_canonical_parity(db, board_id=board_id)
    except Exception as exc:  # pragma: no cover - defensive; never crash the tick
        logger.warning(
            "kg.health.stale_canonical_parity_lookup_failed board=%s err=%s",
            board_id, exc,
        )
        stale_parity = {
            "count": 0, "items": [], "global_discovery_evaluation": "not_evaluated",
        }
    if stale_parity.get("count"):
        _scp_sample = stale_parity["items"][0]
        health_diagnostics["health_issues"].append({
            "code": "stale_canonical_parity",
            "component": "board_graph",
            "severity": "warning",
            "reason": "stale_canonical_parity_count_gt_zero",
            "description": (
                f"{stale_parity['count']} canonical deterministic node(s) are stale "
                "because their SQL source regressed below canonical eligibility. "
                "Read-only diagnostic; the R2 reconciler demotes them on the next "
                "maturity/status event or sweep."
            ),
            "count": stale_parity["count"],
            "operator_action": "inspect_stale_canonical_parity",
            "drill_down_tool": "okto_pulse_kg_stale_canonical_parity_list",
            # AC5/AC13: read-only diagnostic — the literal contract flag makes the
            # no-mutation guarantee explicit (no agent-facing mutation tool clears
            # this; the R2 reconciler is the only internal demotion path).
            "mutation_allowed": False,
            "global_discovery_evaluation": stale_parity.get(
                "global_discovery_evaluation"
            ),
            "sample": {
                "node_id": _scp_sample.get("node_id"),
                "source_artifact_ref": _scp_sample.get("source_artifact_ref"),
                "board_graph_stale": _scp_sample.get("board_graph_stale"),
                "global_discovery_stale_digest": _scp_sample.get(
                    "global_discovery_stale_digest"
                ),
                "expected_graph_layer": _scp_sample.get("expected_graph_layer"),
                "expected_maturity_status": _scp_sample.get("expected_maturity_status"),
                "current_source_status": _scp_sample.get("current_source_status"),
                "recommended_action": _scp_sample.get("recommended_action"),
            },
            "precedence_explanation": (
                "Ranked BELOW canonical_debt_open, cognitive_consolidation_pending, "
                "canonical_partition_integrity and DLQ/operational failures (never "
                "masks them) and ABOVE digest_vs_board_layer_mismatch; a distinct "
                "stale-source category (no double-count)."
            ),
        })
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = "stale_canonical_parity"
            health_diagnostics["operator_action"] = "inspect_stale_canonical_parity"

    # R1-IMP2 — digest_vs_board_layer_mismatch: a published DecisionDigest
    # graph_layer diverging from the expected_digest_layer recomputed from the
    # board graph (read-only; the R1-IMP1 reconciler clears these on drain).
    # Precedence: BELOW canonical_debt_open / cognitive_consolidation_pending /
    # canonical_partition_integrity (only claims primary if still "none"), ABOVE
    # orphan_integrity_warning. Distinct cause -> no double-count with those.
    try:
        from okto_pulse.core.kg.global_discovery.layer_parity import (
            detect_digest_layer_mismatches,
        )
        digest_layer_mismatches = await detect_digest_layer_mismatches(
            db, board_id=board_id
        )
    except Exception as exc:  # pragma: no cover - defensive; never crash the tick
        logger.warning(
            "kg.health.digest_layer_mismatch_lookup_failed board=%s err=%s",
            board_id, exc,
        )
        digest_layer_mismatches = []
    if digest_layer_mismatches:
        _ddm_sample = digest_layer_mismatches[0]
        health_diagnostics["health_issues"].append({
            "code": "digest_vs_board_layer_mismatch",
            "component": "global_discovery",
            "severity": "warning",
            "reason": "digest_vs_board_layer_mismatch_count_gt_zero",
            "description": (
                f"{len(digest_layer_mismatches)} Global Discovery DecisionDigest(s) "
                "publish a graph_layer that diverges from the expected layer "
                "computed from the board graph; the parity reconciler corrects "
                "these on the next drain."
            ),
            "count": len(digest_layer_mismatches),
            "operator_action": "inspect_digest_layer_mismatch",
            "drill_down_tool": "okto_pulse_kg_digest_layer_mismatch_list",
            "sample": {
                "board_id": _ddm_sample["board_id"],
                "digest_id": _ddm_sample["digest_id"],
                "original_node_id": _ddm_sample["original_node_id"],
                "expected_layer": _ddm_sample["expected_layer"],
                "actual_layer": _ddm_sample["actual_layer"],
            },
            "precedence_explanation": (
                "Ranked BELOW canonical_debt_open, cognitive_consolidation_pending "
                "and canonical_partition_integrity (never overrides them) and ABOVE "
                "orphan_integrity_warning; digest publication-layer parity debt, "
                "distinct from those causes (no double-count)."
            ),
        })
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = (
                "digest_vs_board_layer_mismatch"
            )
            health_diagnostics["operator_action"] = "inspect_digest_layer_mismatch"

    if orphan_integrity.get("integrity_warning"):
        if _STATE_SEVERITY[graph_state] < _STATE_SEVERITY[HealthState.AT_RISK]:
            graph_state = HealthState.AT_RISK
        overall_state = max(
            graph_state,
            discovery_classification.state,
            key=lambda s: _STATE_SEVERITY[s],
        )
        if "graph:orphan_integrity_warning" not in combined_reasons:
            combined_reasons.append("graph:orphan_integrity_warning")
        classification_reason = ";".join(combined_reasons)
        health_diagnostics["health_issues"].append({
            "code": "orphan_integrity_warning",
            "component": "board_graph",
            "severity": "warning",
            "reason": "orphan_count_gt_zero",
            "description": (
                f"{int(orphan_integrity.get('orphan_count') or 0)} "
                "non-allowlisted orphan KG node(s) remain. This is graph "
                "integrity debt, not by itself a LadybugDB recovery signal."
            ),
            "operator_action": "inspect_orphan_integrity_report",
        })
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = "orphan_integrity_warning"
            health_diagnostics["operator_action"] = "inspect_orphan_integrity_report"

    # R6-IMP2 (FR2/AC2): the ACTIVE operational-queue backlog is its OWN signal
    # with its own drill-down tool — distinct from dead-letter / canonical debt /
    # cognitive pending. Surfaced only when there is active depth; only promoted to
    # primary_health_cause when stuck/backpressure (transient is normal in-flight).
    if active_queue["total_active_depth"] > 0:
        _aq_class = active_queue["classification"]
        health_diagnostics["health_issues"].append({
            "code": f"active_queue_{_aq_class}",
            "component": "operational_queue",
            "severity": "warning" if _aq_class in ("stuck", "backpressure") else "info",
            "reason": f"active_queue:{_aq_class}",
            "description": (
                f"{active_queue['total_active_depth']} active operational-queue "
                f"item(s) ({_aq_class}) across consolidation_queue + "
                "global_update_outbox. Distinct from dead-letter and canonical debt."
            ),
            "operator_action": "inspect_active_queue",
            "drill_down_tool": "okto_pulse_kg_queue_drilldown",
            "counts": {s["source"]: s["queue_depth"] for s in active_queue["sources"]},
        })
        if (
            _aq_class in ("stuck", "backpressure")
            and health_diagnostics["primary_health_cause"] == "none"
        ):
            health_diagnostics["primary_health_cause"] = f"active_queue_{_aq_class}"
            health_diagnostics["operator_action"] = "inspect_active_queue"

    # R6-IMP5 (FR5/AC5): explicit, deduplicated separation of the THREE operational
    # domains so a caller never conflates them — each is its OWN counter + drill-down:
    #   active_queue   = transient operational work (transient|stuck|backpressure),
    #   dead_letter    = TERMINAL consolidation failures (DLQ),
    #   canonical_debt = semantic canonicality pendency (retry/cognitive promotion).
    # Reuses the existing counts (no new store); DLQ is NEVER summed into the active
    # queue and canonical debt is NEVER reported as DLQ.
    operational_domains = {
        "active_queue": {
            "domain": "active_queue",
            "semantics": "transient_operational",
            "count": int(active_queue["total_active_depth"]),
            "classification": active_queue["classification"],
            "drill_down_tool": "okto_pulse_kg_queue_drilldown",
        },
        "dead_letter": {
            "domain": "dead_letter",
            "semantics": "terminal_failure",
            "count": int(dead_letter_count),
            "drill_down_tool": "okto_pulse_kg_dead_letter_list",
        },
        "canonical_debt": {
            "domain": "canonical_debt",
            "semantics": "semantic_canonicality_pending",
            "count": int(canonical_debt.get("open_count") or 0),
            "drill_down_tool": "okto_pulse_kg_canonical_debt_list",
        },
    }

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
        # R6-IMP2: active operational-queue drill-down (sources/worker_mode/
        # classification). Read-only; DLQ/canonical debt excluded.
        "active_queue": active_queue,
        # R6-IMP5: deduplicated 3-domain separation (active_queue / dead_letter /
        # canonical_debt), each with its own count + drill-down tool.
        "operational_domains": operational_domains,
        "total_nodes": total_nodes,
        "default_score_count": default_score_count,
        "default_score_ratio": round(default_score_ratio, 4),
        "avg_relevance": kuzu_metrics["avg_relevance"],
        "schema_version": HEALTH_SCHEMA_VERSION,
        "health_schema_version": HEALTH_SCHEMA_VERSION,
        "graph_schema_version": graph_schema_version,
        "contradict_warn_count": get_contradict_warn_count(board_id),
        "last_decay_tick_at": last_decay_tick_at,
        "last_tick_status": last_tick_status,
        "last_tick_error": last_tick_error,
        "nodes_recomputed_in_last_tick": nodes_recomputed_in_last_tick,
        "tick_in_progress": tick_in_progress,
        # FR5/FR6 (spec R2b, IMPL-3): tick board counters — distinguishes
        # "tick global falhou" (boards_processed==0) from "processou N mas M
        # falharam" (boards_failed>0). Default 0 per BR5 when no tick_run.
        "boards_processed_in_last_tick": boards_processed_in_last_tick,
        "boards_failed_in_last_tick": boards_failed_in_last_tick,
        # --- KG-01 internal/debug surface (alias to contract overall_state) ---
        "state": overall_state.value,
        "memory_pressure_status": memory_pressure.status.value,
        "classification_reasons": combined_reasons,
        # FR6 (spec R2c): DLQ auto-drain telemetry (additive, null/0 default).
        "dlq_auto_drain_last_run_at": dlq_auto_drain_last_run_at,
        "dlq_auto_drain_requeued_count": dlq_auto_drain_requeued_count,
        # --- KG-HS.1 additive scheduler/footprint clarity surface ---
        "decay_scheduler_diagnostics": decay_scheduler_diagnostics,
        "storage_footprint_proxy": storage_footprint_proxy,
        "orphan_integrity": orphan_integrity,
        "kg_layer_counts": kg_layer_counts,
        "canonical_debt": canonical_debt,
        "rebuild_diagnostics": {
            "last_outcome": (
                "rebuild_complete_with_canonical_debt"
                if int(canonical_debt.get("open_count") or 0) > 0
                else "rebuild_complete"
                if current_kg_generation_id
                else "no_generation"
            ),
            "canonical_open_debt_count": int(canonical_debt.get("open_count") or 0),
            "layer_counts_status": kg_layer_counts.get("status", "unknown"),
            "operator_action": health_diagnostics["operator_action"],
        },
        # --- UI diagnosis surface (additive, does not weaken canonical state) ---
        **health_diagnostics,
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

    Returns a dict with total_nodes, default_score_count and avg_relevance.
    On any Kùzu error (board not bootstrapped, schema drift, lock contention)
    returns zeroed defaults so the health endpoint stays available.
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

    try:
        with open_board_connection(board_id) as (_db, conn):
            for node_type in NODE_TYPES:
                try:
                    res = conn.execute(
                        f"MATCH (n:{node_type}) RETURN n.relevance_score",
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
                    rel = row[0]
                    total_nodes += 1
                    if rel is not None:
                        rel_f = float(rel)
                        relevance_sum += rel_f
                        relevance_n += 1
                        if DEFAULT_SCORE_BAND_LOW <= rel_f <= DEFAULT_SCORE_BAND_HIGH:
                            default_score_count += 1
    except Exception as exc:
        logger.warning(
            "kg.health.kuzu_open_failed board=%s err=%s",
            board_id, exc,
        )
        return _zero_kuzu_metrics()

    avg_relevance = (
        round(relevance_sum / relevance_n, 4) if relevance_n > 0 else 0.0
    )

    return {
        "total_nodes": total_nodes,
        "default_score_count": default_score_count,
        "avg_relevance": avg_relevance,
    }


def _zero_kuzu_metrics() -> dict[str, Any]:
    return {
        "total_nodes": 0,
        "default_score_count": 0,
        "avg_relevance": 0.0,
    }


def _aggregate_kg_layer_counts(board_id: str) -> dict[str, Any]:
    """Count nodes by graph_layer and maturity_status.

    This is additive diagnostic telemetry. Any Kuzu/schema error degrades to an
    unavailable projection and must not affect the health state machine.
    """

    counts = {
        "canonical": 0,
        "working": 0,
        "none": 0,
        "legacy_unknown": 0,
        "unclassified": 0,
    }
    maturity_counts: dict[str, int] = {}
    try:
        from okto_pulse.core.kg.schema import NODE_TYPES, open_board_connection
    except Exception as exc:
        logger.warning(
            "kg.health.layer_counts_import_failed board=%s err=%s",
            board_id, exc,
        )
        return {
            "status": "unavailable",
            "by_layer": counts,
            "by_maturity_status": maturity_counts,
            "reason": "schema_import_failed",
        }

    try:
        with open_board_connection(board_id) as (_db, conn):
            successful_node_types = 0
            failed_node_types = 0
            for node_type in NODE_TYPES:
                result = None
                try:
                    result = conn.execute(
                        f"MATCH (n:{node_type}) "
                        f"RETURN n.graph_layer, n.maturity_status, count(n)"
                    )
                    while result.has_next():
                        row = result.get_next()
                        layer = str(row[0] or "unclassified")
                        maturity = str(row[1] or "unclassified")
                        count = int(row[2] or 0)
                        counts[layer] = counts.get(layer, 0) + count
                        maturity_counts[maturity] = (
                            maturity_counts.get(maturity, 0) + count
                        )
                    successful_node_types += 1
                except Exception:
                    failed_node_types += 1
                    continue
                finally:
                    if result is not None:
                        try:
                            result.close()
                        except Exception:
                            pass
    except Exception as exc:
        logger.warning(
            "kg.health.layer_counts_failed board=%s err=%s",
            board_id, exc,
        )
        return {
            "status": "unavailable",
            "by_layer": counts,
            "by_maturity_status": maturity_counts,
            "reason": "kuzu_open_failed",
        }
    if successful_node_types == 0 and failed_node_types > 0:
        return {
            "status": "unavailable",
            "by_layer": counts,
            "by_maturity_status": maturity_counts,
            "reason": "layer_columns_unavailable",
            "failed_node_types": failed_node_types,
        }
    return {
        "status": "partial" if failed_node_types else "ok",
        "by_layer": counts,
        "by_maturity_status": maturity_counts,
        "failed_node_types": failed_node_types,
    }
