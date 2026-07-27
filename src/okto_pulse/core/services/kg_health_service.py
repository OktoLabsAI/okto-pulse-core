"""KG health snapshot service — feeds /api/v1/kg/health.

Spec 20f67c2a (Ideação #5, FR1, FR2, BR1). Composes 10 fields into a
JSON payload describing the live state of a board's knowledge graph:

    * SQL aggregations against ConsolidationQueue + ConsolidationDeadLetter
      for queue depth, oldest pending age, and dead letter count.
    * graph backend aggregations across all node tables for total_nodes, the count
      of nodes whose relevance_score is in the [0.45, 0.55] "default"
      band (sintoma de inflation), and avg_relevance.
    * In-process counter from scoring.get_contradict_warn_count for
      contradict_warn_count.
    * schema_version is a fixed string ("1.0") versioning the response
      payload independently of the graph backend schema.

When graph backend hasn't been bootstrapped for the board (or any aggregation
fails), graph backend-derived fields gracefully degrade to zero and the response
still ships. The endpoint must never 500 on a healthy app DB.
"""

from __future__ import annotations

import asyncio
import logging
import queue
import re
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import Future
from contextvars import ContextVar, copy_context
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

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
from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.kg.interfaces.graph_runtime_store import (
    GraphRuntimeObservationState,
)
from okto_pulse.core.kg.materialization_health import (
    CensusStatus,
    HealthProbeDeadline,
    MaterializationEvidence,
    MaterializationEvidenceRequest,
    MaterializationHealthBaseline,
    MaterializationHealthPolicy,
)
from okto_pulse.core.kg.scoring import get_contradict_warn_count
from okto_pulse.core.infra.config import get_settings
from okto_pulse.core.ports.kg_health import get_kg_health_read_port
from okto_pulse.core.ports.materialization_health import (
    get_materialization_evidence_port,
)
from okto_pulse.core.ports.relational_runtime import cancel_safe_session
from okto_pulse.core.runtime_context import runtime_lock, runtime_state
from okto_pulse.core.ports.scheduler import KG_DAILY_TICK_JOB_ID, SchedulerControl

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


HEALTH_SCHEMA_VERSION = "1.1"
LEGACY_HEALTH_SCHEMA_VERSION = "1.0"
_MATERIALIZATION_EVIDENCE_BUDGET_S = 2.0
_MATERIALIZATION_EVIDENCE_UNAVAILABLE = "materialization_evidence_unavailable"
_CURRENT_GENERATION_STORE_UNAVAILABLE_REASON = "current_generation_store_unavailable"

# Spec 20f67c2a (Ideação #5): "default score" band used to flag inflation
# sintoma. Nodes whose relevance_score falls in [0.45, 0.55] are likely
# stuck near the neutral default and don't reflect any real signal yet.
DEFAULT_SCORE_BAND_LOW = 0.45
DEFAULT_SCORE_BAND_HIGH = 0.55

# When the ratio of default-band nodes crosses this threshold the service
# emits a structured WARN log so observability tooling can flag the board.
DEFAULT_SCORE_RATIO_ALARM_THRESHOLD = 0.7

# The canonical-partition overlay is optional health enrichment.  Its SQL read
# is bounded so a busy relational store cannot hold the complete health request,
# but cancelling an aiosqlite statement invalidates the connection that owns the
# active transaction.  The read therefore runs in its own cancellation-safe
# scope; the caller's session must never inherit that cancelled transaction.
_DIGEST_OVERLAY_TIMEOUT_S = 0.1

_SENSITIVE_ERROR_RE = re.compile(
    r"([A-Za-z]:\\|/[^ \t\r\n]+/|Traceback|File \"|\.py\b|"
    r"(?:postgres(?:ql)?|mysql|mariadb|sqlite|mongodb(?:\+srv)?|redis|amqps?)://|"
    r"(?:password|passwd|pwd|token|secret)=)",
    re.IGNORECASE,
)


class BoardNotFoundError(Exception):
    """Raised when the requested board does not exist."""


async def _load_digest_partition_overlay(*, board_id: str) -> dict[str, str]:
    """Load the optional digest overlay in an isolated relational scope.

    ``asyncio.wait_for`` cancels its child query on timeout.  SQLAlchemy marks
    an aiosqlite connection invalid in that case and requires a rollback before
    it can reconnect.  ``cancel_safe_session`` delegates rollback + close to the
    edition-owned runtime, keeping both cancellation cleanup and concrete
    session behavior outside Core while preserving the caller's transaction.
    """

    from okto_pulse.core.kg.canonical_partition_integrity import (
        pending_or_debt_exclusions,
    )

    async with cancel_safe_session() as overlay_db:
        return await asyncio.wait_for(
            pending_or_debt_exclusions(overlay_db, board_id=board_id),
            timeout=_DIGEST_OVERLAY_TIMEOUT_S,
        )


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    normalized = _as_utc(value)
    return normalized.isoformat() if normalized is not None else None


def safe_health_error(
    value: Any,
    *,
    sensitive_reason: str = "health_error_redacted",
    max_chars: int = 180,
) -> str | None:
    """Return a bounded, UI-safe error summary for health surfaces.

    The KG Health surface is user/agent facing. It must not expose local graph
    paths, DSNs, Python file paths, stack frames, credentials or payload bodies.
    When those appear, keep only a caller-provided bounded reason code.
    """

    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    first_line = text.splitlines()[0].strip()
    if _SENSITIVE_ERROR_RE.search(text):
        return sensitive_reason
    return first_line[: max(1, min(int(max_chars), 240))]


def _safe_scheduler_error(value: Any) -> str | None:
    return safe_health_error(
        value,
        sensitive_reason="scheduler_tick_failed",
        max_chars=180,
    )


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


async def _read_next_scheduled_at(
    scheduler_control: SchedulerControl | None,
) -> tuple[str | None, str | None]:
    if scheduler_control is None:
        return None, "scheduler_unavailable"
    if not scheduler_control.is_available():
        return None, "scheduler_unavailable"
    try:
        snapshot = await scheduler_control.get_job_snapshot(KG_DAILY_TICK_JOB_ID)
        if not snapshot.exists:
            return None, snapshot.message or "scheduler_job_unavailable"
        return _iso(snapshot.next_run_time), None
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


async def _load_tick_evidence(db: Any) -> dict[str, Any]:
    """Load independent tick facts from KGTickRun.

    This keeps success, failure, terminal legacy state, and in-progress rows as
    separate facts. A failed tick after a success must not erase the last
    successful scoring checkpoint.
    """

    try:
        rows = list(await get_kg_health_read_port().list_tick_runs(db))
        terminal = [row for row in rows if row.completed_at is not None]
        successes = [row for row in terminal if row.error is None]
        failures = [row for row in rows if row.error is not None]
        running = [row for row in rows if row.completed_at is None]
        latest_success = (
            max(successes, key=lambda row: _as_utc(row.completed_at))
            if successes
            else None
        )
        latest_failure = (
            max(
                failures,
                key=lambda row: _as_utc(row.completed_at) or _as_utc(row.started_at),
            )
            if failures
            else None
        )
        latest_terminal = (
            max(
                terminal,
                key=lambda row: _as_utc(row.completed_at),
            )
            if terminal
            else None
        )
        running_row = (
            max(
                running,
                key=lambda row: _as_utc(row.started_at),
            )
            if running
            else None
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


async def _build_decay_scheduler_diagnostics(
    *,
    tick_evidence: dict[str, Any],
    tick_in_progress: bool,
    now: datetime,
    scheduler_control: SchedulerControl | None = None,
) -> dict[str, Any]:
    tolerance_seconds, settings_reason = _read_decay_settings()
    next_scheduled_at, next_reason = await _read_next_scheduled_at(
        scheduler_control,
    )
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
        issues.append(
            {
                "code": "board_graph_empty_after_materialized_history",
                "component": "board_graph",
                "severity": "critical",
                "reason": "graph:empty_after_materialized_history",
                "description": (
                    "Relational audit/ref history shows prior KG materialization "
                    "but the graph adapter currently returns zero nodes."
                ),
                "operator_action": "run_explicit_rebuild",
            }
        )
    elif board_graph_queryable:
        issues.append(
            {
                "code": "board_graph_queryable",
                "component": "board_graph",
                "severity": "info",
                "reason": graph_read_status,
                "description": (
                    "The board graph opened and returned materialized content; "
                    "this is not a board-graph recovery signal."
                ),
                "operator_action": "none",
            }
        )

    if rest_metric_status == "unavailable":
        issues.append(
            {
                "code": "telemetry_unavailable",
                "component": "health_telemetry",
                "severity": "warning",
                "reason": "metric_status:unavailable",
                "description": (
                    "Health sensors are unavailable, so canonical state remains "
                    "at_risk by policy even when the graph is queryable."
                ),
                "operator_action": "inspect_telemetry",
            }
        )

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
        issues.append(
            {
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
            }
        )

    # FR7 (spec 007d1308 / dec_68fd26a2): the dead-letter queue is its own
    # operational signal with its own drill-down tool, kept distinct from
    # cognitive pending and canonical debt.
    if dead_letter_count > 0:
        issues.append(
            {
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
            }
        )

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


# Heavy KG adapters are synchronous and can block for minutes on an embedded
# graph lock. Health reads use a fixed, runtime-owned daemon pool so the event
# loop never executes those calls and a stuck backend cannot create an unbounded
# thread per request/board/probe. Idle workers retire and are restarted lazily;
# copied runtime compositions receive a fresh queue/pool and never share jobs.
_HEALTH_PROBE_BUDGET_S = 0.35
_HEALTH_PARITY_PROBE_BUDGET_S = 0.3
_HEALTH_PROBE_CACHE_TTL_S = 30.0
_HEALTH_PROBE_WORKERS = 4
_HEALTH_PROBE_QUEUE_SIZE = 64
_HEALTH_PROBE_WORKER_IDLE_S = 30.0


class _DaemonHealthProbePool:
    """Small fixed worker pool whose jobs carry their originating ContextVar."""

    def __init__(
        self,
        *,
        max_workers: int,
        max_queue_size: int,
        idle_timeout_s: float = _HEALTH_PROBE_WORKER_IDLE_S,
    ) -> None:
        self._max_workers = max_workers
        self._idle_timeout_s = max(0.001, float(idle_timeout_s))
        self._jobs: queue.Queue[tuple[Future[Any], Any, Callable[[], Any]]] = (
            queue.Queue(maxsize=max_queue_size)
        )
        self._start_lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        self._thread_sequence = 0
        # A health request is intentionally allowed to return before its
        # bounded daemon probes finish.  Runtime teardown still needs an
        # ownership boundary for those jobs: closing an embedded graph while
        # an old-runtime probe can open another connection races the next
        # runtime and can strand a WAL/lease.  Track every submitted future,
        # including additive orphan refreshes that are not represented in the
        # request-level single-flight map.
        self._idle_condition = threading.Condition()
        self._pending_futures: set[Future[Any]] = set()

    def clone_for_runtime(self) -> "_DaemonHealthProbePool":
        """Return an empty pool owned by a copied runtime composition.

        Jobs, queues and worker threads never cross runtime boundaries. The
        cloned pool remains lazy, so copying a composition does not start
        threads until that runtime actually performs a health probe.
        """

        return _DaemonHealthProbePool(
            max_workers=self._max_workers,
            max_queue_size=self._jobs.maxsize,
            idle_timeout_s=self._idle_timeout_s,
        )

    def _prune_threads_locked(self) -> None:
        self._threads[:] = [thread for thread in self._threads if thread.is_alive()]

    def active_worker_count(self) -> int:
        """Return the live worker count after pruning retired threads."""

        with self._start_lock:
            self._prune_threads_locked()
            return len(self._threads)

    def _retire_future(self, future: Future[Any]) -> None:
        with self._idle_condition:
            self._pending_futures.discard(future)
            self._idle_condition.notify_all()

    def wait_until_idle(self, *, timeout_s: float) -> int:
        """Wait for every job owned by this runtime-local pool.

        Returns the number of jobs still running/queued at the deadline.  A
        child job submitted by a running probe cannot create a false idle gap:
        the parent remains tracked until its build callable returns.
        """

        if timeout_s < 0:
            raise ValueError("timeout_s must be non-negative")
        deadline = time.monotonic() + timeout_s
        with self._idle_condition:
            while self._pending_futures:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._idle_condition.wait(timeout=remaining)
            return len(self._pending_futures)

    def _ensure_started(self) -> None:
        with self._start_lock:
            self._prune_threads_locked()
            for _index in range(self._max_workers - len(self._threads)):
                self._thread_sequence += 1
                thread = threading.Thread(
                    target=self._worker,
                    name=(f"okto-pulse-health-probe-{self._thread_sequence}"),
                    daemon=True,
                )
                thread.start()
                self._threads.append(thread)

    def submit(
        self,
        *,
        context: Any,
        build: Callable[[], Any],
    ) -> Future[Any] | None:
        future: Future[Any] = Future()
        try:
            self._jobs.put_nowait((future, context, build))
        except queue.Full:
            future.cancel()
            return None
        with self._idle_condition:
            self._pending_futures.add(future)
        future.add_done_callback(self._retire_future)
        # Enqueue before checking workers. This closes the retirement race: an
        # idle worker either sees the queued job, or removes itself before this
        # call starts a replacement.
        self._ensure_started()
        return future

    def _worker(self) -> None:
        while True:
            try:
                future, context, build = self._jobs.get(timeout=self._idle_timeout_s)
            except queue.Empty:
                with self._start_lock:
                    if not self._jobs.empty():
                        continue
                    current = threading.current_thread()
                    if current in self._threads:
                        self._threads.remove(current)
                    return
            try:
                if not future.set_running_or_notify_cancel():
                    continue
                try:
                    value = context.run(build)
                except BaseException as exc:  # Future preserves worker failure
                    future.set_exception(exc)
                else:
                    future.set_result(value)
            finally:
                self._jobs.task_done()


class _HealthProbeOwnerContext:
    """Runtime-owned worker attribution used by multi-step probe guards."""

    def __init__(self) -> None:
        self._value: ContextVar[tuple[str, tuple[int, int]] | None] = ContextVar(
            "kg_health_probe_owner", default=None
        )

    def get(self) -> tuple[str, tuple[int, int]] | None:
        return self._value.get()

    def set(self, value: tuple[str, tuple[int, int]]) -> Any:
        return self._value.set(value)

    def reset(self, token: Any) -> None:
        self._value.reset(token)

    def clone_for_runtime(self) -> "_HealthProbeOwnerContext":
        return _HealthProbeOwnerContext()


def _new_health_probe_pool() -> _DaemonHealthProbePool:
    return _DaemonHealthProbePool(
        max_workers=_HEALTH_PROBE_WORKERS,
        max_queue_size=_HEALTH_PROBE_QUEUE_SIZE,
        idle_timeout_s=_HEALTH_PROBE_WORKER_IDLE_S,
    )


_HEALTH_PROBE_POOL = runtime_state(
    "services.kg_health.probe_pool",
    _new_health_probe_pool,
)
_HEALTH_PROBE_CACHE = runtime_state("services.kg_health.probe_cache", dict)
_HEALTH_PROBE_INFLIGHT = runtime_state("services.kg_health.probe_inflight", dict)
_HEALTH_PROBE_RESET_EPOCHS = runtime_state(
    "services.kg_health.probe_reset_epochs",
    dict,
)
_HEALTH_PROBE_GENERATION_HINTS = runtime_state(
    "services.kg_health.probe_generation_hints",
    dict,
)
_HEALTH_PROBE_PHASES = runtime_state("services.kg_health.probe_phases", dict)
_HEALTH_PROBE_PARTIALS = runtime_state("services.kg_health.probe_partials", dict)
_HEALTH_PROBE_LOCK = runtime_lock("services.kg_health.probes")
_HEALTH_PROBE_OWNER = runtime_state(
    "services.kg_health.probe_owner",
    _HealthProbeOwnerContext,
)


def drain_health_probe_runtime(*, timeout_s: float = 30.0) -> int:
    """Drain daemon health jobs owned by the current runtime composition.

    Health reads stay latency-bounded during normal operation, while edition
    lifecycle adapters can call this synchronous boundary from their teardown
    worker before checkpointing/closing embedded graph handles.  The return
    value is the number of jobs that did not finish within ``timeout_s``.
    """

    return _HEALTH_PROBE_POOL.wait_until_idle(timeout_s=timeout_s)


@dataclass(frozen=True)
class _HealthProbeRequest:
    name: str
    board_id: str
    generation_id: str | None
    build: Callable[[], Any]
    fallback: Any
    ttl_s: float = _HEALTH_PROBE_CACHE_TTL_S
    prefer_fresh_within_budget: bool = False


@dataclass(frozen=True)
class _HealthProbeResult:
    value: Any
    status: str
    reason: str
    age_seconds: float | None
    refresh_in_progress: bool

    def diagnostic(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "age_seconds": (
                round(self.age_seconds, 3) if self.age_seconds is not None else None
            ),
            "refresh_in_progress": self.refresh_in_progress,
        }


@dataclass(frozen=True)
class BoundedHealthProbeResult:
    """Public projection over the runtime-owned daemon probe machinery."""

    value: Any
    status: str
    reason: str
    age_seconds: float | None
    refresh_in_progress: bool


def _snapshot_derived_diagnostic(
    *,
    source_status: str,
    source_reason: str | None,
    snapshot: _HealthProbeResult,
    max_age_seconds: float = _HEALTH_PROBE_CACHE_TTL_S,
) -> dict[str, Any]:
    """Describe a snapshot-derived value without hiding its cache age.

    ``source_status`` reports whether the underlying probe step succeeded;
    ``snapshot_freshness`` reports whether its cached value is current,
    stale-while-revalidate, or unavailable. Previously a cached cognitive count
    could still say ``available`` after the owning snapshot had become stale.
    """

    normalized_source_status = str(source_status or "unavailable")
    status = normalized_source_status
    reason = source_reason or (
        "ok" if normalized_source_status == "available" else snapshot.reason
    )
    if normalized_source_status == "available" and snapshot.status == "stale":
        status = "stale"
        reason = snapshot.reason
    freshness = snapshot.diagnostic()
    freshness["is_stale"] = snapshot.status == "stale"
    freshness["max_age_seconds"] = float(max_age_seconds)
    return {
        "status": status,
        "reason": reason,
        "source_status": normalized_source_status,
        "snapshot_freshness": freshness,
    }


def _health_probe_epoch(board_id: str) -> tuple[int, int]:
    return (
        int(_HEALTH_PROBE_RESET_EPOCHS.get("*", 0)),
        int(_HEALTH_PROBE_RESET_EPOCHS.get(board_id, 0)),
    )


def _health_probe_key(request: _HealthProbeRequest) -> tuple[str, str]:
    return (request.name, request.board_id)


def _cached_health_probe_result(
    request: _HealthProbeRequest,
    *,
    now: float,
) -> _HealthProbeResult | None:
    entry = _HEALTH_PROBE_CACHE.get(_health_probe_key(request))
    if entry is None:
        return None
    completed_at, generation_id, ok, value, reason = entry
    if generation_id != request.generation_id:
        return None
    age = max(0.0, now - float(completed_at))
    refresh_in_progress = _health_probe_key(request) in _HEALTH_PROBE_INFLIGHT
    if not ok:
        return _HealthProbeResult(
            value=request.fallback,
            status="unavailable",
            reason=str(reason or "probe_failed"),
            age_seconds=age,
            refresh_in_progress=refresh_in_progress,
        )
    return _HealthProbeResult(
        value=value,
        status="available" if age < request.ttl_s else "stale",
        reason="ok" if age < request.ttl_s else "cache_expired_refresh_scheduled",
        age_seconds=age,
        refresh_in_progress=refresh_in_progress,
    )


def _ensure_health_probe(
    request: _HealthProbeRequest,
) -> tuple[_HealthProbeResult, Future[Any] | None]:
    key = _health_probe_key(request)
    with _HEALTH_PROBE_LOCK:
        cached = _cached_health_probe_result(request, now=time.monotonic())
        if cached is not None and (
            cached.status == "available"
            or (
                cached.status == "unavailable"
                and cached.age_seconds is not None
                and cached.age_seconds < request.ttl_s
            )
        ):
            return cached, None

        inflight = _HEALTH_PROBE_INFLIGHT.get(key)
        future: Future[Any] | None = None
        reason = "probe_not_cached"
        schedule = inflight is None
        if inflight is not None:
            _token, inflight_generation, future = inflight
            if inflight_generation != request.generation_id:
                future = None
                reason = "previous_generation_refresh_in_progress"
                # A stale-generation job must not prevent a first-write
                # generation from forcing a fresh bounded probe. The fixed
                # queue/pool remains the concurrency bound; the token guard
                # below prevents the replaced job from publishing stale data.
                schedule = True
            else:
                reason = "refresh_in_progress"
        if schedule:
            token = uuid.uuid4().hex
            epoch = _health_probe_epoch(request.board_id)

            def run_probe() -> None:
                try:
                    with _HEALTH_PROBE_LOCK:
                        # A queued job may start after a test/runtime reset.
                        # Do not spend a managed worker on invalidated I/O or
                        # let that old owner publish phase/cache state.
                        if _health_probe_epoch(request.board_id) != epoch:
                            return
                        _HEALTH_PROBE_PHASES[key] = request.name
                    owner_context_token = _HEALTH_PROBE_OWNER.set(
                        (request.board_id, epoch)
                    )
                    try:
                        value = request.build()
                    except Exception as exc:
                        ok = False
                        value = None
                        failure_reason = type(exc).__name__
                    else:
                        ok = True
                        failure_reason = "ok"
                    finally:
                        _HEALTH_PROBE_OWNER.reset(owner_context_token)
                    with _HEALTH_PROBE_LOCK:
                        current = _HEALTH_PROBE_INFLIGHT.get(key)
                        if (
                            _health_probe_epoch(request.board_id) == epoch
                            and current is not None
                            and current[0] == token
                        ):
                            _HEALTH_PROBE_CACHE[key] = (
                                time.monotonic(),
                                request.generation_id,
                                ok,
                                value,
                                failure_reason,
                            )
                finally:
                    with _HEALTH_PROBE_LOCK:
                        current = _HEALTH_PROBE_INFLIGHT.get(key)
                        if current is not None and current[0] == token:
                            _HEALTH_PROBE_PHASES.pop(key, None)
                            _HEALTH_PROBE_INFLIGHT.pop(key, None)

            future = _HEALTH_PROBE_POOL.submit(
                context=copy_context(),
                build=run_probe,
            )
            if future is None:
                reason = "probe_queue_saturated"
            else:
                _HEALTH_PROBE_INFLIGHT[key] = (
                    token,
                    request.generation_id,
                    future,
                )
                reason = "refresh_scheduled"

        if cached is not None:
            return _HealthProbeResult(
                value=cached.value,
                status="stale",
                reason=reason,
                age_seconds=cached.age_seconds,
                refresh_in_progress=future is not None,
            ), future
        return _HealthProbeResult(
            value=request.fallback,
            status="unavailable",
            reason=reason,
            age_seconds=None,
            refresh_in_progress=future is not None,
        ), future


def _read_health_probe_result(
    request: _HealthProbeRequest,
    *,
    fallback_reason: str,
) -> _HealthProbeResult:
    with _HEALTH_PROBE_LOCK:
        cached = _cached_health_probe_result(request, now=time.monotonic())
        if cached is not None:
            return cached
        phase = _HEALTH_PROBE_PHASES.get(_health_probe_key(request))
        reason = fallback_reason
        if phase and fallback_reason == "probe_budget_exceeded":
            reason = f"probe_budget_exceeded:{phase}"
        return _HealthProbeResult(
            value=request.fallback,
            status="unavailable",
            reason=reason,
            age_seconds=None,
            refresh_in_progress=(_health_probe_key(request) in _HEALTH_PROBE_INFLIGHT),
        )


async def _resolve_health_probe_batch(
    requests: tuple[_HealthProbeRequest, ...],
    *,
    budget_s: float = _HEALTH_PROBE_BUDGET_S,
) -> dict[str, _HealthProbeResult]:
    initial: dict[str, _HealthProbeResult] = {}
    cold_futures: dict[Future[Any], str] = {}
    for request in requests:
        result, future = _ensure_health_probe(request)
        initial[request.name] = result
        if (
            result.status == "unavailable"
            or (result.status == "stale" and request.prefer_fresh_within_budget)
        ) and future is not None:
            cold_futures[future] = request.name

    if cold_futures and budget_s > 0:
        # Shield the concurrent futures from caller cancellation. A cancelled
        # queued future would otherwise skip ``run_probe`` in the worker and
        # strand its single-flight marker forever.
        wrapped = [
            asyncio.shield(asyncio.wrap_future(future)) for future in cold_futures
        ]
        await asyncio.wait(wrapped, timeout=budget_s)

    resolved: dict[str, _HealthProbeResult] = {}
    for request in requests:
        initial_result = initial[request.name]
        if initial_result.status == "stale" and not request.prefer_fresh_within_budget:
            resolved[request.name] = initial_result
            continue
        resolved[request.name] = _read_health_probe_result(
            request,
            fallback_reason=(
                "probe_budget_exceeded"
                if initial_result.refresh_in_progress
                else initial_result.reason
            ),
        )
    return resolved


async def run_bounded_health_probe(
    *,
    name: str,
    board_id: str,
    generation_id: str | None,
    build: Callable[[], Any],
    fallback: Any,
    deadline_at: float,
    ttl_s: float = _HEALTH_PROBE_CACHE_TTL_S,
) -> BoundedHealthProbeResult:
    """Run one synchronous health probe in the fixed daemon/single-flight pool.

    ``deadline_at`` is an absolute monotonic deadline shared with the caller's
    other probes. A generation change bypasses cached and in-flight work from
    the previous generation without spawning an unbounded worker or retry loop.
    """

    remaining = max(0.0, float(deadline_at) - time.monotonic())
    if remaining <= 0.0:
        return BoundedHealthProbeResult(
            value=fallback,
            status="unavailable",
            reason="probe_deadline_exhausted",
            age_seconds=None,
            refresh_in_progress=False,
        )
    request = _HealthProbeRequest(
        name=str(name),
        board_id=str(board_id),
        generation_id=generation_id,
        build=build,
        fallback=fallback,
        ttl_s=max(0.0, float(ttl_s)),
        prefer_fresh_within_budget=True,
    )
    resolved = await _resolve_health_probe_batch(
        (request,),
        budget_s=remaining,
    )
    result = resolved[request.name]
    return BoundedHealthProbeResult(
        value=result.value,
        status=result.status,
        reason=result.reason,
        age_seconds=result.age_seconds,
        refresh_in_progress=result.refresh_in_progress,
    )


def _reset_health_probe_cache_for_tests(board_id: str | None = None) -> None:
    with _HEALTH_PROBE_LOCK:
        if board_id is None:
            _HEALTH_PROBE_RESET_EPOCHS["*"] = (
                int(_HEALTH_PROBE_RESET_EPOCHS.get("*", 0)) + 1
            )
            _HEALTH_PROBE_CACHE.clear()
            _HEALTH_PROBE_INFLIGHT.clear()
            _HEALTH_PROBE_GENERATION_HINTS.clear()
            _HEALTH_PROBE_PHASES.clear()
            _HEALTH_PROBE_PARTIALS.clear()
            return

        _HEALTH_PROBE_RESET_EPOCHS[board_id] = (
            int(_HEALTH_PROBE_RESET_EPOCHS.get(board_id, 0)) + 1
        )
        for state in (
            _HEALTH_PROBE_CACHE,
            _HEALTH_PROBE_INFLIGHT,
            _HEALTH_PROBE_PHASES,
            _HEALTH_PROBE_PARTIALS,
        ):
            for key in [key for key in state if key[1] == board_id]:
                state.pop(key, None)
        _HEALTH_PROBE_GENERATION_HINTS.pop(board_id, None)


def _run_health_probe_step(
    *,
    probe_name: str,
    board_id: str,
    step_name: str,
    build: Callable[[], Any],
) -> Any:
    with _HEALTH_PROBE_LOCK:
        owner = _HEALTH_PROBE_OWNER.get()
        if (
            owner is not None
            and owner[0] == board_id
            and _health_probe_epoch(board_id) != owner[1]
        ):
            raise RuntimeError("health_probe_invalidated")
        _HEALTH_PROBE_PHASES[(probe_name, board_id)] = step_name
    return build()


def _assert_health_probe_epoch(
    *,
    board_id: str,
    epoch: tuple[int, int],
) -> None:
    """Stop an invalidated multi-step probe before it starts more I/O."""

    with _HEALTH_PROBE_LOCK:
        if _health_probe_epoch(board_id) != epoch:
            raise RuntimeError("health_probe_invalidated")


def _begin_health_probe_partial(
    *,
    probe_name: str,
    board_id: str,
    generation_id: str | None,
) -> tuple[int, int]:
    with _HEALTH_PROBE_LOCK:
        owner = _HEALTH_PROBE_OWNER.get()
        epoch = (
            owner[1]
            if owner is not None and owner[0] == board_id
            else _health_probe_epoch(board_id)
        )
        if _health_probe_epoch(board_id) == epoch:
            _HEALTH_PROBE_PARTIALS[(probe_name, board_id)] = (
                epoch,
                generation_id,
                {},
            )
    return epoch


def _publish_health_probe_partial(
    *,
    probe_name: str,
    board_id: str,
    generation_id: str | None,
    epoch: tuple[int, int],
    values: dict[str, Any],
) -> None:
    key = (probe_name, board_id)
    with _HEALTH_PROBE_LOCK:
        current = _HEALTH_PROBE_PARTIALS.get(key)
        if (
            current is None
            or current[0] != epoch
            or current[1] != generation_id
            or _health_probe_epoch(board_id) != epoch
        ):
            return
        partial = dict(current[2])
        partial.update(values)
        _HEALTH_PROBE_PARTIALS[key] = (epoch, generation_id, partial)


def _read_health_probe_partial(
    *,
    probe_name: str,
    board_id: str,
    generation_id: str | None,
) -> dict[str, Any]:
    with _HEALTH_PROBE_LOCK:
        current = _HEALTH_PROBE_PARTIALS.get((probe_name, board_id))
        if (
            current is None
            or current[0] != _health_probe_epoch(board_id)
            or current[1] != generation_id
        ):
            return {}
        return dict(current[2])


# Cache TTL do orphan scan: o scan conta o grau de CADA node por tipo de
# relação (O(nodes × rel_types) queries graph backend) — em campo (3927 nodes) uma
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
_ORPHAN_PROJECTION_CACHE = runtime_state(
    "services.kg_health.orphan_projection_cache",
    dict,
)
_ORPHAN_PROJECTION_TTL_S = 300.0
_ORPHAN_SCAN_INFLIGHT = runtime_state(
    "services.kg_health.orphan_scan_inflight_state",
    dict,
)
_ORPHAN_SCAN_INFLIGHT_LOCK = runtime_lock("services.kg_health.orphan_scan_inflight")
_ORPHAN_REFRESH_SCHEDULED = runtime_state(
    "services.kg_health.orphan_refresh_scheduled",
    dict,
)
_ORPHAN_RESET_EPOCHS = runtime_state(
    "services.kg_health.orphan_reset_epochs",
    dict,
)


def _orphan_reset_epoch(board_id: str) -> tuple[int, int]:
    """Return the runtime-local global + board reset epoch."""

    return (
        int(_ORPHAN_RESET_EPOCHS.get("*", 0)),
        int(_ORPHAN_RESET_EPOCHS.get(board_id, 0)),
    )


def _cached_orphan_projection(
    *,
    board_id: str,
    generation_id: str | None,
    now: float,
) -> tuple[dict[str, Any] | None, bool]:
    """Return a generation-matched projection and whether it is fresh.

    Cache entries created before the generation-aware format are accepted only
    when their projection explicitly identifies the requested generation. This
    keeps a rolling upgrade fail-closed instead of serving an old generation as
    current health evidence.
    """

    entry = _ORPHAN_PROJECTION_CACHE.get(board_id)
    if entry is None:
        return None, False

    if len(entry) == 3:
        completed_at, cached_generation_id, projection = entry
    else:  # pragma: no cover - rolling-upgrade compatibility
        completed_at, projection = entry
        cached_generation_id = projection.get("generation_id")
    if cached_generation_id != generation_id:
        return None, False
    return projection, now - float(completed_at) < _ORPHAN_PROJECTION_TTL_S


def reset_orphan_projection_cache_for_tests(board_id: str | None = None) -> None:
    """Invalidate orphan refresh state without trusting late worker cleanup.

    A graph call already executing in a daemon thread cannot be cancelled. The
    reset epoch makes its eventual publication a no-op, while owner tokens stop
    that old worker from clearing a newer scan's markers.
    """

    with _ORPHAN_SCAN_INFLIGHT_LOCK:
        if board_id is None:
            _ORPHAN_RESET_EPOCHS["*"] = int(_ORPHAN_RESET_EPOCHS.get("*", 0)) + 1
            _ORPHAN_PROJECTION_CACHE.clear()
            _ORPHAN_SCAN_INFLIGHT.clear()
            _ORPHAN_REFRESH_SCHEDULED.clear()
        else:
            _ORPHAN_RESET_EPOCHS[board_id] = (
                int(_ORPHAN_RESET_EPOCHS.get(board_id, 0)) + 1
            )
            _ORPHAN_PROJECTION_CACHE.pop(board_id, None)
            _ORPHAN_SCAN_INFLIGHT.pop(board_id, None)
            _ORPHAN_REFRESH_SCHEDULED.pop(board_id, None)
    _reset_health_probe_cache_for_tests(board_id)


def _orphan_projection_unavailable(*, scan_error: str) -> dict[str, Any]:
    from okto_pulse.core.kg.orphan_integrity import (
        build_orphan_integrity_projection,
    )

    return build_orphan_integrity_projection(
        None,
        scan_error=scan_error,
    ).to_safe_dict()


def _get_or_schedule_orphan_integrity_for_health(
    *, board_id: str, generation_id: str | None
) -> dict[str, Any]:
    """Return cached integrity immediately and refresh it in the background.

    The orphan scan is an additive health enrichment and can take minutes on a
    large graph.  A health/readiness call must never inherit that latency.  The
    first caller schedules one daemon refresh and receives an unavailable
    projection; later callers receive stale-while-revalidate until the refresh
    atomically replaces the cache.
    """

    with _ORPHAN_SCAN_INFLIGHT_LOCK:
        cached, cache_is_fresh = _cached_orphan_projection(
            board_id=board_id,
            generation_id=generation_id,
            now=time.monotonic(),
        )
        if cache_is_fresh:
            assert cached is not None
            return cached

        should_schedule = board_id not in _ORPHAN_REFRESH_SCHEDULED
        if should_schedule:
            refresh_token = uuid.uuid4().hex
            refresh_epoch = _orphan_reset_epoch(board_id)
            _ORPHAN_REFRESH_SCHEDULED[board_id] = refresh_token

    if should_schedule:

        def refresh() -> None:
            try:
                _build_orphan_integrity_for_health(
                    board_id=board_id,
                    generation_id=generation_id,
                    _expected_epoch=refresh_epoch,
                )
            except Exception as exc:  # pragma: no cover - defensive worker shell
                logger.warning(
                    "kg.health.orphan_refresh_failed board=%s reason=%s",
                    board_id,
                    type(exc).__name__,
                )
            finally:
                with _ORPHAN_SCAN_INFLIGHT_LOCK:
                    if _ORPHAN_REFRESH_SCHEDULED.get(board_id) == refresh_token:
                        _ORPHAN_REFRESH_SCHEDULED.pop(board_id, None)

        future = _HEALTH_PROBE_POOL.submit(
            context=copy_context(),
            build=refresh,
        )
        if future is None:
            with _ORPHAN_SCAN_INFLIGHT_LOCK:
                if _ORPHAN_REFRESH_SCHEDULED.get(board_id) == refresh_token:
                    _ORPHAN_REFRESH_SCHEDULED.pop(board_id, None)
            logger.warning(
                "kg.health.orphan_refresh_not_started board=%s reason=%s",
                board_id,
                "probe_queue_saturated",
            )

    if cached is not None:
        return cached
    return _orphan_projection_unavailable(scan_error="ScanScheduled")


def _build_orphan_integrity_for_health(
    *,
    board_id: str,
    generation_id: str | None,
    _expected_epoch: tuple[int, int] | None = None,
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

    with _ORPHAN_SCAN_INFLIGHT_LOCK:
        current_epoch = _orphan_reset_epoch(board_id)
        if _expected_epoch is not None and current_epoch != _expected_epoch:
            return build_orphan_integrity_projection(
                None,
                scan_error="ScanInvalidated",
            ).to_safe_dict()

        cached, cache_is_fresh = _cached_orphan_projection(
            board_id=board_id,
            generation_id=generation_id,
            now=time.monotonic(),
        )
        if cache_is_fresh:
            assert cached is not None
            return cached

        if board_id in _ORPHAN_SCAN_INFLIGHT:
            # Outro scan deste board está em andamento: serve o stale (ou a
            # projeção indisponível) em vez de empilhar mais um leitor.
            if cached is not None:
                return cached
            return build_orphan_integrity_projection(
                None,
                scan_error="ScanAlreadyInProgress",
            ).to_safe_dict()
        scan_token = uuid.uuid4().hex
        scan_epoch = current_epoch
        _ORPHAN_SCAN_INFLIGHT[board_id] = scan_token

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
        with _ORPHAN_SCAN_INFLIGHT_LOCK:
            if _orphan_reset_epoch(board_id) == scan_epoch:
                _ORPHAN_PROJECTION_CACHE[board_id] = (
                    time.monotonic(),
                    generation_id,
                    projection,
                )
        return projection
    except Exception as exc:
        logger.debug(
            "kg.health.orphan_integrity_scan_unavailable board=%s err=%s",
            board_id,
            exc,
        )
        # Falhas NÃO são cacheadas — a próxima chamada re-tenta o scan.
        return build_orphan_integrity_projection(
            None,
            scan_error=type(exc).__name__,
        ).to_safe_dict()
    finally:
        with _ORPHAN_SCAN_INFLIGHT_LOCK:
            if _ORPHAN_SCAN_INFLIGHT.get(board_id) == scan_token:
                _ORPHAN_SCAN_INFLIGHT.pop(board_id, None)


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
    """Return high-water mark from the logical graph runtime footprint."""
    try:
        footprint = get_kg_registry().graph_runtime_store.footprint(board_id)
    except Exception as exc:
        logger.debug(
            "kg.health.hwm.footprint_failed board=%s err=%s",
            board_id,
            exc,
        )
        return None

    return footprint.percentage if footprint.status == "available" else None


def _unavailable_storage_footprint_proxy(
    reason: str | None = None,
) -> dict[str, Any]:
    return {
        "source": "runtime_capability",
        "status": "unavailable",
        "percentage": None,
        "high_water_mark_pct": None,
        "graph_primary_bytes": None,
        "primary_bytes": None,
        "sidecar_bytes": None,
        "total_bytes": None,
        "configured_max_db_size_bytes": None,
        "configured_max_db_size_gb": None,
        "is_direct_memory_telemetry": False,
        "description": (
            "Storage footprint projection derived from the graph runtime adapter."
        ),
        "tooltip": (
            "This is not live graph memory telemetry. It is an adapter-provided "
            "early warning signal."
        ),
        "unavailable_reason": reason,
    }


def _build_storage_footprint_proxy(board_id: str) -> dict[str, Any]:
    """Build an explanatory storage footprint payload for REST/MCP/UI."""

    base = _unavailable_storage_footprint_proxy()

    try:
        footprint = get_kg_registry().graph_runtime_store.footprint(board_id)
    except Exception:
        base["unavailable_reason"] = "footprint_unavailable"
        return base

    configured_max_gb = None
    if footprint.configured_max_bytes is not None:
        configured_max_gb = int(footprint.configured_max_bytes / (1024**3))
    base.update(
        {
            "source": footprint.source,
            "status": footprint.status,
            "percentage": footprint.percentage,
            "high_water_mark_pct": footprint.percentage,
            "graph_primary_bytes": footprint.primary_bytes,
            "primary_bytes": footprint.primary_bytes,
            "sidecar_bytes": footprint.sidecar_bytes,
            "total_bytes": footprint.total_bytes,
            "configured_max_db_size_bytes": footprint.configured_max_bytes,
            "configured_max_db_size_gb": configured_max_gb,
            "unavailable_reason": footprint.unavailable_reason,
        }
    )
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
    from on-disk file sizes (board graph + siblings) and records a
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
        if get_kg_registry().graph_runtime_store.exists(board_id):
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
            board_id,
            exc,
        )


def _probe_global_discovery_telemetry() -> GraphTelemetry:
    """Classify global discovery from non-opening runtime metadata only."""

    try:
        runtime = get_kg_registry().require_global_discovery_runtime()
    except Exception:
        return _telemetry_unavailable("discovery")

    try:
        state = runtime.state()
    except Exception:
        return _telemetry_unavailable("discovery")
    return _telemetry_from_materialization_observation(
        state.normalized_state,
        graph_type="discovery",
    )


_GRAPH_HEALTH_PROBE = "graph_snapshot"
_ARTIFACT_HEALTH_PROBE = "artifact_snapshot"
_PARITY_HEALTH_PROBE = "parity_snapshot"
_DISCOVERY_HEALTH_PROBE = "discovery_snapshot"


def _unavailable_kg_layer_counts(reason: str) -> dict[str, Any]:
    return {
        "status": "unavailable",
        "by_layer": {
            "canonical": 0,
            "working": 0,
            "none": 0,
            "legacy_unknown": 0,
            "unclassified": 0,
        },
        "by_maturity_status": {},
        "reason": reason,
    }


def _graph_health_snapshot_fallback(reason: str) -> dict[str, Any]:
    return {
        "graph_metrics": _zero_graph_metrics(),
        "graph_schema_version": None,
        "graph_telemetry": _telemetry_unavailable("board"),
        "storage_footprint_proxy": _unavailable_storage_footprint_proxy(reason),
        "kg_layer_counts": _unavailable_kg_layer_counts(reason),
    }


def _build_graph_health_snapshot(
    board_id: str,
    generation_id: str | None = None,
) -> dict[str, Any]:
    """Run all synchronous graph reads in one managed, bounded worker job."""

    epoch = _begin_health_probe_partial(
        probe_name=_GRAPH_HEALTH_PROBE,
        board_id=board_id,
        generation_id=generation_id,
    )

    def step(field: str, name: str, build: Callable[[], Any]) -> Any:
        _assert_health_probe_epoch(board_id=board_id, epoch=epoch)
        value = _run_health_probe_step(
            probe_name=_GRAPH_HEALTH_PROBE,
            board_id=board_id,
            step_name=name,
            build=build,
        )
        _publish_health_probe_partial(
            probe_name=_GRAPH_HEALTH_PROBE,
            board_id=board_id,
            generation_id=generation_id,
            epoch=epoch,
            values={field: value},
        )
        return value

    graph_metrics = step(
        "graph_metrics",
        "graph_metrics",
        lambda: _aggregate_graph_metrics(board_id),
    )
    graph_schema_version = step(
        "graph_schema_version",
        "schema_version",
        lambda: _get_graph_schema_version(board_id),
    )
    graph_telemetry = step(
        "graph_telemetry",
        "board_telemetry",
        lambda: _probe_board_graph_telemetry(
            board_id=board_id,
            total_nodes=int(graph_metrics.get("total_nodes") or 0),
            graph_schema_version=graph_schema_version,
            empty_after_materialized_history=False,
        ),
    )
    storage_footprint_proxy = step(
        "storage_footprint_proxy",
        "storage_footprint",
        lambda: _build_storage_footprint_proxy(board_id),
    )
    kg_layer_counts = step(
        "kg_layer_counts",
        "layer_counts",
        lambda: _aggregate_kg_layer_counts(board_id),
    )

    return {
        "graph_metrics": graph_metrics,
        "graph_schema_version": graph_schema_version,
        "graph_telemetry": graph_telemetry,
        "storage_footprint_proxy": storage_footprint_proxy,
        "kg_layer_counts": kg_layer_counts,
    }


def _parity_health_snapshot_fallback(reason: str) -> dict[str, Any]:
    return {
        "stale_board_items": [],
        "stale_board_status": "unavailable",
        "digest_inputs": {
            "status": "unavailable",
            "reason": reason,
            "digests": [],
            "board_meta": {},
            "needs_overlay": False,
        },
    }


def _build_parity_health_snapshot(
    board_id: str,
    generation_id: str | None = None,
) -> dict[str, Any]:
    epoch = _begin_health_probe_partial(
        probe_name=_PARITY_HEALTH_PROBE,
        board_id=board_id,
        generation_id=generation_id,
    )

    def step(name: str, build: Callable[[], Any]) -> Any:
        _assert_health_probe_epoch(board_id=board_id, epoch=epoch)
        return _run_health_probe_step(
            probe_name=_PARITY_HEALTH_PROBE,
            board_id=board_id,
            step_name=name,
            build=build,
        )

    def stale_board_items() -> tuple[list[dict[str, Any]], str]:
        try:
            from okto_pulse.core.kg.stale_canonical_parity import (
                detect_board_graph_stale,
            )

            return detect_board_graph_stale(board_id), "available"
        except Exception:
            return [], "unavailable"

    stale_items, stale_status = step("stale_canonical_parity", stale_board_items)
    _publish_health_probe_partial(
        probe_name=_PARITY_HEALTH_PROBE,
        board_id=board_id,
        generation_id=generation_id,
        epoch=epoch,
        values={
            "stale_board_items": stale_items,
            "stale_board_status": stale_status,
        },
    )

    def digest_inputs() -> dict[str, Any]:
        try:
            from okto_pulse.core.kg.global_discovery.layer_parity import (
                collect_digest_layer_mismatch_inputs,
            )

            return collect_digest_layer_mismatch_inputs(board_id)
        except Exception as exc:
            return {
                "status": "unavailable",
                "reason": type(exc).__name__,
                "digests": [],
                "board_meta": {},
                "needs_overlay": False,
            }

    digest_snapshot = step("digest_layer_parity", digest_inputs)
    _publish_health_probe_partial(
        probe_name=_PARITY_HEALTH_PROBE,
        board_id=board_id,
        generation_id=generation_id,
        epoch=epoch,
        values={"digest_inputs": digest_snapshot},
    )
    return {
        "stale_board_items": stale_items,
        "stale_board_status": stale_status,
        "digest_inputs": digest_snapshot,
    }


def _artifact_health_snapshot_fallback(reason: str) -> dict[str, Any]:
    return {
        "current_kg_generation_id": None,
        "generation_status": "unavailable",
        "generation_reason": reason,
        "cognitive_pending_active": 0,
        "partition_cognitive_pending": 0,
        "cognitive_status": "unavailable",
        "source_diag": {
            "source_count": None,
            "canonical_source_count": None,
            "working_source_count": None,
            "enumeration_failure": True,
            "error": reason,
        },
    }


def _read_cognitive_health_counts(board_id: str) -> tuple[int, int, str]:
    try:
        from okto_pulse.core.kg.cognitive_readiness import R7_HOLD_REASON_CODES
        from okto_pulse.core.kg.rebuild_audit import (
            CognitiveConsolidationItemStore,
            compute_status_counts,
            require_rebuild_audit_artifact_store,
        )

        store = CognitiveConsolidationItemStore(
            artifact_store=require_rebuild_audit_artifact_store()
        )
        generation = store.latest_generation(board_id)
        if not generation:
            return 0, 0, "available"
        items = list(store.list_items(board_id, generation))
        counts = compute_status_counts(items)
        active = (
            int(counts.get("pending", 0))
            + int(counts.get("in_progress", 0))
            + int(counts.get("failed", 0))
        )
        active_statuses = {"pending", "in_progress", "failed"}
        partition = sum(
            1
            for item in items
            if item.status in active_statuses
            and str(getattr(item, "reason_code", "") or "") in R7_HOLD_REASON_CODES
        )
        return active, partition, "available"
    except Exception:
        return 0, 0, "unavailable"


def _read_current_kg_generation(board_id: str) -> tuple[str | None, str, str]:
    """Read the generation pointer from its file-backed repository."""

    try:
        from okto_pulse.core.kg.interfaces import get_kg_registry
        from okto_pulse.core.kg.rebuild_generation import (
            RebuildAuditKGGenerationRepository,
        )

        artifact_store = get_kg_registry().require_rebuild_audit_artifact_store()
        generation_id = RebuildAuditKGGenerationRepository(
            artifact_store=artifact_store
        ).get_current(board_id)
        return generation_id, "available", "ok"
    except Exception:
        return None, "unavailable", _CURRENT_GENERATION_STORE_UNAVAILABLE_REASON


def _build_artifact_health_snapshot(board_id: str) -> dict[str, Any]:
    """Read generation/source/cognitive files once outside the event loop."""

    epoch = _begin_health_probe_partial(
        probe_name=_ARTIFACT_HEALTH_PROBE,
        board_id=board_id,
        generation_id=None,
    )

    def step(name: str, build: Callable[[], Any]) -> Any:
        _assert_health_probe_epoch(board_id=board_id, epoch=epoch)
        return _run_health_probe_step(
            probe_name=_ARTIFACT_HEALTH_PROBE,
            board_id=board_id,
            step_name=name,
            build=build,
        )

    generation_id, generation_status, generation_reason = step(
        "current_generation",
        lambda: _read_current_kg_generation(board_id),
    )
    _publish_health_probe_partial(
        probe_name=_ARTIFACT_HEALTH_PROBE,
        board_id=board_id,
        generation_id=None,
        epoch=epoch,
        values={
            "current_kg_generation_id": generation_id,
            "generation_status": generation_status,
            "generation_reason": generation_reason,
        },
    )

    cognitive_pending, partition_pending, cognitive_status = step(
        "cognitive_items",
        lambda: _read_cognitive_health_counts(board_id),
    )
    _publish_health_probe_partial(
        probe_name=_ARTIFACT_HEALTH_PROBE,
        board_id=board_id,
        generation_id=None,
        epoch=epoch,
        values={
            "cognitive_pending_active": cognitive_pending,
            "partition_cognitive_pending": partition_pending,
            "cognitive_status": cognitive_status,
        },
    )
    source_diag = step(
        "rebuild_source_diagnostics",
        lambda: _probe_rebuild_source_diagnostics(board_id),
    )
    _publish_health_probe_partial(
        probe_name=_ARTIFACT_HEALTH_PROBE,
        board_id=board_id,
        generation_id=None,
        epoch=epoch,
        values={"source_diag": source_diag},
    )
    return {
        "current_kg_generation_id": generation_id,
        "generation_status": generation_status,
        "generation_reason": generation_reason,
        "cognitive_pending_active": cognitive_pending,
        "partition_cognitive_pending": partition_pending,
        "cognitive_status": cognitive_status,
        "source_diag": source_diag,
    }


async def _collect_materialization_evidence(
    board_id: str,
) -> tuple[MaterializationEvidence | None, str, bool]:
    """Collect one generation-fenced snapshot under one absolute deadline."""

    port = get_materialization_evidence_port()
    if port is None:
        return None, "materialization_evidence_provider_unavailable", False

    deadline = HealthProbeDeadline(
        deadline_at=time.monotonic() + _MATERIALIZATION_EVIDENCE_BUDGET_S
    )
    try:
        async with asyncio.timeout(deadline.remaining_seconds(now=time.monotonic())):
            generation = str(await port.current_generation(board_id)).strip()
            if not generation:
                return None, "materialization_generation_unavailable", True
            evidence = await port.probe(
                MaterializationEvidenceRequest(
                    board_id=board_id,
                    generation=generation,
                    deadline=deadline,
                )
            )
    except TimeoutError:
        return None, "materialization_evidence_timeout", True
    except Exception:
        return None, "materialization_evidence_provider_unavailable", True
    return evidence, "ok", True


def _materialization_reason_codes(reason: str) -> dict[str, str]:
    stable = str(reason or _MATERIALIZATION_EVIDENCE_UNAVAILABLE)
    return {
        "board_graph": stable,
        "board_census": stable,
        "global_discovery": stable,
    }


def _is_confirmed_empty_evidence(
    evidence: MaterializationEvidence | None,
) -> bool:
    if evidence is None:
        return False
    return (
        evidence.board_store.normalized_state
        is GraphRuntimeObservationState.CONFIRMED_ABSENT
        and evidence.census.status is CensusStatus.AVAILABLE
        and evidence.board_store.generation is not None
        and evidence.board_store.generation == evidence.census.generation
        and evidence.census.is_confirmed_zero
    )


def _telemetry_from_materialization_observation(
    observation: GraphRuntimeObservationState,
    *,
    graph_type: str,
) -> GraphTelemetry:
    if observation in {
        GraphRuntimeObservationState.PRESENT_READABLE_CANDIDATE,
        GraphRuntimeObservationState.CONFIRMED_ABSENT,
    }:
        return _telemetry_ok(graph_type)
    if observation is GraphRuntimeObservationState.PRESENT_UNREADABLE_OR_ERROR:
        return _telemetry_wal_or_open_error(graph_type)
    return _telemetry_unavailable(graph_type)


def _not_materialized_artifact_snapshot() -> dict[str, Any]:
    return {
        "current_kg_generation_id": None,
        "generation_status": "available",
        "generation_reason": "board_not_materialized",
        "cognitive_pending_active": 0,
        "partition_cognitive_pending": 0,
        "cognitive_status": "available",
        "source_diag": {
            "source_count": 0,
            "canonical_source_count": 0,
            "working_source_count": 0,
            "enumeration_failure": False,
            "error": None,
            "skipped": "board_not_materialized",
        },
    }


def _not_materialized_orphan_projection() -> dict[str, Any]:
    return {
        "classification_delta": "none",
        "integrity_warning": False,
        "orphan_count": 0,
        "orphan_count_by_type": {},
        "samples": [],
        "unresolved_reasons": {},
        "allowlisted_root_count": 0,
        "generation_id": None,
        "correlation_id": None,
        "zero_orphan_validation": "not_applicable",
        "reason": "board_not_materialized",
    }


def _unavailable_orphan_projection(reason: str) -> dict[str, Any]:
    """Return a fail-closed projection without touching board graph storage."""

    return {
        "classification_delta": "unavailable",
        "integrity_warning": False,
        "orphan_count": 0,
        "orphan_count_by_type": {},
        "samples": [],
        "unresolved_reasons": {},
        "allowlisted_root_count": 0,
        "generation_id": None,
        "correlation_id": None,
        "zero_orphan_validation": "unavailable",
        "reason": reason,
    }


async def get_kg_health(
    board_id: str,
    db: Any,
    *,
    scheduler_control: SchedulerControl | None = None,
) -> dict[str, Any]:
    """Compose the /api/v1/kg/health payload for ``board_id``.

    Raises ``BoardNotFoundError`` when the board is not found in the
    SQLite app DB. All graph backend-derived metrics degrade to zero on lookup
    errors — the endpoint never 500s for a transient graph backend issue.
    """
    relational = await get_kg_health_read_port().queue_snapshot(
        db,
        board_id=board_id,
    )
    if not relational.board_exists:
        raise BoardNotFoundError(f"board not found: {board_id}")

    (
        materialization_evidence,
        materialization_probe_reason,
        materialization_port_configured,
    ) = await _collect_materialization_evidence(board_id)
    materialization_observation = (
        materialization_evidence.board_store.normalized_state
        if materialization_evidence is not None
        else None
    )
    confirmed_empty_evidence = _is_confirmed_empty_evidence(materialization_evidence)
    # A non-present observation is already authoritative for the safety
    # decision. Do not fall through to legacy graph reads that may open or
    # bootstrap an absent/unreadable store.
    skip_board_graph_reads = materialization_port_configured and (
        materialization_observation
        is not GraphRuntimeObservationState.PRESENT_READABLE_CANDIDATE
    )

    now = datetime.now(timezone.utc)

    queue_depth = relational.queue_depth
    oldest_triggered = relational.oldest_triggered_at
    if oldest_triggered is not None:
        if oldest_triggered.tzinfo is None:
            oldest_triggered = oldest_triggered.replace(tzinfo=timezone.utc)
        oldest_pending_age_s = max(0.0, (now - oldest_triggered).total_seconds())
    else:
        oldest_pending_age_s = 0.0

    dead_letter_count = relational.dead_letter_count

    # R6-IMP2: active operational-queue drill-down (ConsolidationQueue pending/
    # claimed + global_update_outbox retry-window rows). DLQ / canonical debt are
    # deliberately NOT counted here — that separation is R6-IMP5.
    from okto_pulse.core.services.queue_health_service import (
        get_active_queue_drilldown,
        get_global_outbox_dead_letter_drilldown,
    )

    active_queue = await get_active_queue_drilldown(db, board_id)
    global_outbox_dead_letter = await get_global_outbox_dead_letter_drilldown(
        db,
        board_id,
        limit=0,
    )
    global_outbox_dead_letter_count = int(global_outbox_dead_letter["total_count"])

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
        boards_failed_in_last_tick = 0  # BR5: default 0 when no tick_run
    if last_terminal_tick is not None:
        last_tick_status = "failed" if last_terminal_tick.error else "completed"
        last_tick_error = _safe_scheduler_error(last_terminal_tick.error)
    else:
        last_tick_status = None
        last_tick_error = None

    generation_hint = _HEALTH_PROBE_GENERATION_HINTS.get(board_id)
    probe_requests: list[_HealthProbeRequest] = []
    if not skip_board_graph_reads:
        probe_requests.append(
            _HealthProbeRequest(
                name=_GRAPH_HEALTH_PROBE,
                board_id=board_id,
                generation_id=generation_hint,
                build=lambda: _build_graph_health_snapshot(
                    board_id,
                    generation_hint,
                ),
                fallback=_graph_health_snapshot_fallback("probe_budget_exceeded"),
            )
        )
    if not confirmed_empty_evidence:
        probe_requests.append(
            _HealthProbeRequest(
                name=_ARTIFACT_HEALTH_PROBE,
                board_id=board_id,
                generation_id=None,
                build=lambda: _build_artifact_health_snapshot(board_id),
                fallback=_artifact_health_snapshot_fallback("probe_budget_exceeded"),
            )
        )
    if materialization_evidence is None and not materialization_port_configured:
        # Backward-compatible Core-only mode: without an edition evidence
        # adapter, retain the pre-1.1 diagnostics. The result is still forced
        # fail-closed below and can never become ``not_materialized``.
        probe_requests.append(
            _HealthProbeRequest(
                name=_DISCOVERY_HEALTH_PROBE,
                board_id=board_id,
                generation_id=generation_hint,
                build=_probe_global_discovery_telemetry,
                fallback=_telemetry_unavailable("discovery"),
            )
        )
    probe_results = await _resolve_health_probe_batch(tuple(probe_requests))

    if skip_board_graph_reads:
        graph_reason = (
            materialization_evidence.board_store.reason_code
            if materialization_evidence is not None
            else materialization_probe_reason
        ) or _MATERIALIZATION_EVIDENCE_UNAVAILABLE
        graph_status = (
            "unavailable"
            if materialization_observation
            is GraphRuntimeObservationState.PROVIDER_UNAVAILABLE
            else "available"
        )
        graph_fallback = _graph_health_snapshot_fallback(graph_reason)
        if materialization_observation is not None:
            graph_fallback["graph_telemetry"] = (
                _telemetry_from_materialization_observation(
                    materialization_observation,
                    graph_type="board",
                )
            )
        graph_probe = _HealthProbeResult(
            value=graph_fallback,
            status=graph_status,
            reason=graph_reason,
            age_seconds=0.0,
            refresh_in_progress=False,
        )
    else:
        graph_probe = probe_results[_GRAPH_HEALTH_PROBE]

    if confirmed_empty_evidence:
        artifact_probe = _HealthProbeResult(
            value=_not_materialized_artifact_snapshot(),
            status="available",
            reason="board_not_materialized",
            age_seconds=0.0,
            refresh_in_progress=False,
        )
    else:
        artifact_probe = probe_results[_ARTIFACT_HEALTH_PROBE]

    if materialization_evidence is not None:
        discovery_observation = (
            materialization_evidence.discovery_store.normalized_state
        )
        discovery_reason = (
            materialization_evidence.discovery_store.reason_code
            or materialization_evidence.discovery_store.unavailable_reason
            or "global_discovery_observation_unclassified"
        )
        discovery_probe = _HealthProbeResult(
            value=_telemetry_from_materialization_observation(
                discovery_observation,
                graph_type="discovery",
            ),
            status=(
                "unavailable"
                if discovery_observation
                is GraphRuntimeObservationState.PROVIDER_UNAVAILABLE
                else "available"
            ),
            reason=discovery_reason,
            age_seconds=0.0,
            refresh_in_progress=False,
        )
    elif not materialization_port_configured:
        discovery_probe = probe_results[_DISCOVERY_HEALTH_PROBE]
    else:
        discovery_probe = _HealthProbeResult(
            value=_telemetry_unavailable("discovery"),
            status="unavailable",
            reason=materialization_probe_reason,
            age_seconds=0.0,
            refresh_in_progress=False,
        )
    graph_snapshot = dict(graph_probe.value)
    graph_partial: dict[str, Any] = {}
    if graph_probe.status == "unavailable":
        graph_partial = _read_health_probe_partial(
            probe_name=_GRAPH_HEALTH_PROBE,
            board_id=board_id,
            generation_id=generation_hint,
        )
        graph_snapshot.update(graph_partial)
    graph_metrics_available = confirmed_empty_evidence or (
        not skip_board_graph_reads
        and (graph_probe.status != "unavailable" or "graph_metrics" in graph_partial)
    )
    artifact_snapshot = dict(artifact_probe.value)
    if artifact_probe.status == "unavailable":
        artifact_snapshot.update(
            _read_health_probe_partial(
                probe_name=_ARTIFACT_HEALTH_PROBE,
                board_id=board_id,
                generation_id=None,
            )
        )
    graph_metrics = graph_snapshot["graph_metrics"]
    graph_schema_version = graph_snapshot["graph_schema_version"]
    current_kg_generation_id = artifact_snapshot["current_kg_generation_id"]
    if artifact_snapshot.get("generation_status") == "available":
        _HEALTH_PROBE_GENERATION_HINTS[board_id] = current_kg_generation_id

    if skip_board_graph_reads:
        parity_probe = _HealthProbeResult(
            value=(
                {
                    "stale_board_items": [],
                    "stale_board_status": "available",
                    "digest_inputs": {
                        "status": "available",
                        "reason": "board_not_materialized",
                        "digests": [],
                        "board_meta": {},
                        "needs_overlay": False,
                    },
                }
                if confirmed_empty_evidence
                else _parity_health_snapshot_fallback("board_graph_not_opened")
            ),
            status="available" if confirmed_empty_evidence else "unavailable",
            reason="board_graph_not_opened",
            age_seconds=0.0,
            refresh_in_progress=False,
        )
    else:
        parity_results = await _resolve_health_probe_batch(
            (
                _HealthProbeRequest(
                    name=_PARITY_HEALTH_PROBE,
                    board_id=board_id,
                    generation_id=current_kg_generation_id,
                    build=lambda: _build_parity_health_snapshot(
                        board_id,
                        current_kg_generation_id,
                    ),
                    fallback=_parity_health_snapshot_fallback("probe_budget_exceeded"),
                    ttl_s=0.0,
                    prefer_fresh_within_budget=True,
                ),
            ),
            budget_s=_HEALTH_PARITY_PROBE_BUDGET_S,
        )
        parity_probe = parity_results[_PARITY_HEALTH_PROBE]
    parity_snapshot = dict(parity_probe.value)
    if parity_probe.status == "unavailable":
        parity_snapshot.update(
            _read_health_probe_partial(
                probe_name=_PARITY_HEALTH_PROBE,
                board_id=board_id,
                generation_id=current_kg_generation_id,
            )
        )

    # A present discovery artifact is initially classified from metadata only.
    # The parity probe above may be the first native open after process start;
    # Community feeds a proven corruption back into the same runtime instance.
    # Re-read only that semantic state after the bounded native probe so the
    # first health response cannot overwrite a real open failure with a false
    # healthy. This remains a non-opening state read and never resolves storage
    # paths in Core.
    if (
        materialization_evidence is not None
        and materialization_evidence.discovery_store.normalized_state
        is GraphRuntimeObservationState.PRESENT_READABLE_CANDIDATE
    ):
        try:
            refreshed_discovery = (
                get_kg_registry()
                .require_global_discovery_runtime()
                .state(generation=materialization_evidence.discovery_store.generation)
            )
        except Exception:
            refreshed_discovery = None
        if (
            refreshed_discovery is not None
            and refreshed_discovery.normalized_state
            is GraphRuntimeObservationState.PRESENT_UNREADABLE_OR_ERROR
        ):
            materialization_evidence = replace(
                materialization_evidence,
                discovery_store=refreshed_discovery,
            )
            discovery_probe = _HealthProbeResult(
                value=_telemetry_from_materialization_observation(
                    refreshed_discovery.normalized_state,
                    graph_type="discovery",
                ),
                status="available",
                reason=(
                    refreshed_discovery.reason_code
                    or refreshed_discovery.unavailable_reason
                    or "global_discovery_observation_unclassified"
                ),
                age_seconds=0.0,
                refresh_in_progress=False,
            )

    probe_diagnostics = {
        _GRAPH_HEALTH_PROBE: graph_probe.diagnostic(),
        _ARTIFACT_HEALTH_PROBE: artifact_probe.diagnostic(),
        _PARITY_HEALTH_PROBE: parity_probe.diagnostic(),
        _DISCOVERY_HEALTH_PROBE: discovery_probe.diagnostic(),
        "graph_metrics": {
            "status": ("available" if graph_metrics_available else "unavailable"),
            "reason": "ok" if graph_metrics_available else graph_probe.reason,
        },
        "schema_version": {
            "status": (
                "available" if graph_schema_version is not None else "unavailable"
            ),
            "reason": (
                "ok" if graph_schema_version is not None else graph_probe.reason
            ),
        },
        "discovery_telemetry": {
            "status": discovery_probe.status,
            "reason": discovery_probe.reason,
        },
        "current_generation": _snapshot_derived_diagnostic(
            source_status=artifact_snapshot.get("generation_status", "unavailable"),
            source_reason=artifact_snapshot.get(
                "generation_reason", artifact_probe.reason
            ),
            snapshot=artifact_probe,
        ),
        "storage_footprint": {
            "status": graph_snapshot["storage_footprint_proxy"].get(
                "status", "unavailable"
            ),
            "reason": graph_snapshot["storage_footprint_proxy"].get(
                "unavailable_reason"
            ),
        },
        "layer_counts": {
            "status": graph_snapshot["kg_layer_counts"].get("status", "unavailable"),
            "reason": graph_snapshot["kg_layer_counts"].get("reason"),
        },
        "cognitive_items": _snapshot_derived_diagnostic(
            source_status=artifact_snapshot.get("cognitive_status", "unavailable"),
            source_reason=(
                "ok"
                if artifact_snapshot.get("cognitive_status") == "available"
                else artifact_probe.reason
            ),
            snapshot=artifact_probe,
        ),
        "rebuild_source_diagnostics": _snapshot_derived_diagnostic(
            source_status=(
                "unavailable"
                if artifact_snapshot["source_diag"].get("enumeration_failure")
                else "available"
            ),
            source_reason=artifact_snapshot["source_diag"].get("error") or "ok",
            snapshot=artifact_probe,
        ),
    }

    default_score_count = graph_metrics["default_score_count"]
    total_nodes = graph_metrics["total_nodes"]
    default_score_ratio = default_score_count / total_nodes if total_nodes > 0 else 0.0

    if default_score_ratio > DEFAULT_SCORE_RATIO_ALARM_THRESHOLD:
        logger.warning(
            "kg.health.default_score_skew_high board=%s ratio=%.3f "
            "count=%d total=%d threshold=%.2f",
            board_id,
            default_score_ratio,
            default_score_count,
            total_nodes,
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
    # através de remount, expomos o estado real da lease ``kg_daily_tick``.
    # Reuso da LeaseProvider que kg_tick.py e o cron consultam — single source
    # of truth, sem primitiva concreta no core.
    from okto_pulse.core.ports.coordination import (
        CoordinationProviderMissing,
        get_lease_provider,
    )

    try:
        tick_in_progress = get_lease_provider().is_held("kg_daily_tick")
    except CoordinationProviderMissing:
        tick_in_progress = False
    if tick_in_progress or tick_evidence.get("running_row") is not None:
        last_tick_status = "running"
    decay_scheduler_diagnostics = await _build_decay_scheduler_diagnostics(
        tick_evidence=tick_evidence,
        tick_in_progress=tick_in_progress,
        now=now,
        scheduler_control=scheduler_control,
    )

    # KG-01 FR1+FR2+FR3 (contract api_3ed9037f): per-graph classification
    # for board_graph and global_discovery, deterministic memory-pressure
    # correlator, and contract-shaped REST payload. Low-level embedded graph backend buffer
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

    def _evaluate_graph(
        telemetry: GraphTelemetry,
        *,
        quarantine_present: bool = False,
    ):
        return classifier.evaluate(
            telemetries=[telemetry],
            lock_state=null_lock,
            quarantine_present=quarantine_present,
            backpressure_rejecting=False,
            correlation_id=correlation_id,
        )

    empty_after_materialized_history = (
        not confirmed_empty_evidence
        and graph_metrics_available
        and total_nodes == 0
        and await _has_materialized_kg_history(db, board_id)
    )
    graph_telemetry = (
        _telemetry_wal_or_open_error("board")
        if empty_after_materialized_history
        else graph_snapshot["graph_telemetry"]
    )
    discovery_telemetry = discovery_probe.value

    graph_classification = _evaluate_graph(
        graph_telemetry,
        quarantine_present=bool(
            materialization_evidence
            and materialization_evidence.board_store.quarantined
        ),
    )
    discovery_classification = _evaluate_graph(
        discovery_telemetry,
        quarantine_present=bool(
            materialization_evidence
            and materialization_evidence.discovery_store.quarantined
        ),
    )

    graph_state = graph_classification.state
    effective_discovery_state = discovery_classification.state
    if empty_after_materialized_history:
        graph_state = HealthState.RECOVERY_NEEDED

    overall_state = max(
        graph_state,
        effective_discovery_state,
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

    materialization_state = "unknown"
    materialization_generation: str | None = None
    probe_reason_codes = _materialization_reason_codes(materialization_probe_reason)
    if materialization_evidence is not None:
        materialization_snapshot = MaterializationHealthPolicy().evaluate(
            board_store=materialization_evidence.board_store,
            census=materialization_evidence.census,
            discovery_store=materialization_evidence.discovery_store,
            baseline=MaterializationHealthBaseline(
                graph_state=graph_state,
                discovery_state=effective_discovery_state,
                overall_state=overall_state,
                metric_status=(
                    MetricStatus.AVAILABLE
                    if rest_metric_status == "available"
                    else MetricStatus.UNAVAILABLE
                ),
                classification_reasons=tuple(combined_reasons),
            ),
        )
        materialization_state = materialization_snapshot.materialization_state.value
        materialization_generation = materialization_snapshot.materialization_generation
        probe_reason_codes = dict(materialization_snapshot.probe_reason_codes)
        graph_state = materialization_snapshot.graph_state
        effective_discovery_state = materialization_snapshot.discovery_state
        overall_state = materialization_snapshot.overall_state
        rest_metric_status = _REST_METRIC_STATUS_MAP[
            materialization_snapshot.metric_status
        ]
        classification_reason = materialization_snapshot.classification_reason
        combined_reasons = list(materialization_snapshot.classification_reasons)

        if materialization_snapshot.known_empty_metrics is not None:
            known_empty = materialization_snapshot.known_empty_metrics
            queue_depth = known_empty.queue_depth
            oldest_pending_age_s = known_empty.oldest_pending_age_s
            dead_letter_count = known_empty.dead_letter_count
            global_outbox_dead_letter_count = (
                known_empty.global_outbox_dead_letter_count
            )
            total_nodes = known_empty.total_nodes
            default_score_count = known_empty.default_score_count
            default_score_ratio = known_empty.default_score_ratio
            graph_metrics.update(
                {
                    "total_nodes": known_empty.total_nodes,
                    "default_score_count": known_empty.default_score_count,
                    "avg_relevance": known_empty.avg_relevance,
                }
            )
            graph_schema_version = known_empty.graph_schema_version
            last_decay_tick_at = known_empty.last_decay_tick_at
            active_queue = {
                **active_queue,
                "total_active_depth": known_empty.active_queue_count,
                "classification": "empty",
                "sources": [
                    {**source, "queue_depth": 0}
                    for source in active_queue.get("sources", [])
                ],
            }
            global_outbox_dead_letter = {
                **global_outbox_dead_letter,
                "total_count": known_empty.global_outbox_dead_letter_count,
                "oldest_age_seconds": known_empty.oldest_dead_letter_age_s,
            }
            graph_snapshot["storage_footprint_proxy"] = {
                **graph_snapshot["storage_footprint_proxy"],
                "status": "available",
                "percentage": None,
                "high_water_mark_pct": known_empty.high_water_mark_pct,
                "graph_primary_bytes": 0,
                "primary_bytes": 0,
                "sidecar_bytes": 0,
                "total_bytes": known_empty.board_storage_total_bytes,
                "unavailable_reason": None,
            }
            graph_snapshot["kg_layer_counts"] = {
                "status": "ok",
                "by_layer": {
                    "canonical": known_empty.canonical_layer_count,
                    "working": known_empty.working_layer_count,
                    "none": 0,
                    "legacy_unknown": 0,
                    "unclassified": 0,
                },
                "by_maturity_status": {
                    "canonical_eligible": 0,
                    "working_immature": 0,
                    "cancelled": 0,
                },
            }
            probe_diagnostics.update(
                {
                    "graph_metrics": {
                        "status": "available",
                        "reason": "confirmed_empty_known_zero",
                    },
                    "schema_version": {
                        "status": "not_applicable",
                        "reason": "board_not_materialized",
                    },
                    "storage_footprint": {
                        "status": "available",
                        "reason": "confirmed_empty_known_zero",
                    },
                    "layer_counts": {
                        "status": "available",
                        "reason": "confirmed_empty_known_zero",
                    },
                }
            )
    else:
        # Missing edition evidence is not confirmed absence. Preserve the
        # legacy diagnostics, but make the new diagnosis explicitly fail closed.
        rest_metric_status = "unavailable"
        if _STATE_SEVERITY[graph_state] < _STATE_SEVERITY[HealthState.AT_RISK]:
            graph_state = HealthState.AT_RISK
        overall_state = max(
            graph_state,
            effective_discovery_state,
            key=lambda state: _STATE_SEVERITY[state],
        )
        if materialization_probe_reason not in combined_reasons:
            combined_reasons.append(materialization_probe_reason)
        classification_reason = ";".join(combined_reasons)

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
        discovery_state=effective_discovery_state,
        discovery_reasons=[
            f"discovery:{reason}" for reason in discovery_classification.reasons
        ],
        rest_metric_status=rest_metric_status,
        dead_letter_count=int(dead_letter_count),
    )
    if global_outbox_dead_letter_count > 0:
        health_diagnostics["health_issues"].append(
            {
                "code": "global_outbox_dead_letter_backlog",
                "component": "global_discovery_delivery",
                "severity": "warning",
                "reason": "global_outbox_dead_letter_count_gt_zero",
                "description": (
                    f"{global_outbox_dead_letter_count} terminal global-discovery "
                    "delivery event(s) need read-only diagnosis. This domain is "
                    "separate from consolidation DLQ and the active retry window."
                ),
                "operator_action": "inspect_global_outbox_dead_letters",
                "drill_down_tool": (
                    "okto_pulse_kg_global_outbox_dead_letter_list"
                ),
            }
        )
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = (
                "global_outbox_dead_letter_backlog"
            )
            health_diagnostics["operator_action"] = "inspect_global_outbox_dead_letters"
    if decay_scheduler_diagnostics["operational_debt"]:
        health_diagnostics["health_issues"].append(
            {
                "code": f"decay_scheduler_{decay_scheduler_diagnostics['status']}",
                "component": "decay_scheduler",
                "severity": decay_scheduler_diagnostics["severity"],
                "reason": f"decay_scheduler:{decay_scheduler_diagnostics['reason']}",
                "description": (
                    "Decay scheduler has operational debt. This does not imply "
                    "board graph corruption or require KG rebuild by itself."
                ),
                "operator_action": decay_scheduler_diagnostics["recommended_action"],
            }
        )
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = "decay_scheduler_debt"
            health_diagnostics["operator_action"] = decay_scheduler_diagnostics[
                "recommended_action"
            ]

    # AF16: generation/file-backed evidence is part of the managed artifact
    # snapshot. A cold timeout is explicit and never blocks the event loop.
    if artifact_snapshot.get("generation_status") != "available":
        rest_metric_status = "unavailable"
        health_diagnostics["health_issues"].append(
            {
                "code": "rebuild_audit_artifact_store_unavailable",
                "component": "kg_generation_store",
                "severity": "warning",
                "reason": artifact_snapshot.get(
                    "generation_reason",
                    _CURRENT_GENERATION_STORE_UNAVAILABLE_REASON,
                ),
                "description": (
                    "Current KG generation could not be read from the injected "
                    "RebuildAuditArtifactStore."
                ),
                "operator_action": "inspect_runtime_provider",
            }
        )
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = (
                _CURRENT_GENERATION_STORE_UNAVAILABLE_REASON
            )
            health_diagnostics["operator_action"] = "inspect_runtime_provider"

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
        recent_events.append(
            {
                "occurred_at": fe.timestamp.isoformat(),
                "event_type": fe.event_kind,
                "reason": memory_pressure.reason,
                "correlation_id": fe.correlation_id,
            }
        )
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
        from okto_pulse.core.application.runtime_workers import (
            runtime_worker_snapshot,
        )

        drain_stats = runtime_worker_snapshot(
            "consolidation_worker",
            board_id=board_id,
        )
        dlq_auto_drain_last_run_at = drain_stats["last_run_at"]
        dlq_auto_drain_requeued_count = drain_stats["requeued_count"]
    except Exception:
        pass  # defensive: health endpoint must never 500 on telemetry failure

    checked_at = now.isoformat()
    storage_footprint_proxy = graph_snapshot["storage_footprint_proxy"]
    if confirmed_empty_evidence:
        orphan_integrity = _not_materialized_orphan_projection()
    elif skip_board_graph_reads:
        orphan_integrity = _unavailable_orphan_projection(
            graph_probe.reason or "board_graph_not_opened"
        )
    else:
        orphan_integrity = _get_or_schedule_orphan_integrity_for_health(
            board_id=board_id,
            generation_id=current_kg_generation_id,
        )
    kg_layer_counts = graph_snapshot["kg_layer_counts"]
    try:
        from okto_pulse.core.services.canonical_debt_service import (
            summarize_canonical_debt,
        )

        canonical_debt = await summarize_canonical_debt(db, board_id)
    except Exception as exc:  # pragma: no cover - defensive health path
        logger.warning(
            "kg.health.canonical_debt_summary_failed board=%s err=%s",
            board_id,
            exc,
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
        health_diagnostics["health_issues"].append(
            {
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
            }
        )
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = "canonical_debt_open"
            health_diagnostics["operator_action"] = "inspect_canonical_debt"

    # FR7 (spec 007d1308 / dec_68fd26a2): cognitive pending is its OWN
    # operational signal with its own drill-down tool, kept separate from the
    # dead-letter queue and canonical debt. Defensive: the store read is
    # file-backed, so it runs off the event loop and any IO failure degrades
    # to 0 — the health endpoint must never 500 on telemetry failure
    # (br_2a8cdfdc).
    cognitive_pending_active = int(
        artifact_snapshot.get("cognitive_pending_active") or 0
    )
    if cognitive_pending_active > 0:
        health_diagnostics["health_issues"].append(
            {
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
            }
        )
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
    partition_debt_open = 0
    try:
        from okto_pulse.core.kg.canonical_learning_partition import (
            PARTITION_TARGET_STATUS,
        )
        from okto_pulse.core.services.canonical_debt_service import (
            OPEN_STATES as _OPEN_STATES,
        )

        partition_debt_open = await get_kg_health_read_port().count_partition_debt(
            db,
            board_id=board_id,
            target_status=PARTITION_TARGET_STATUS,
            open_states=tuple(_OPEN_STATES),
        )
    except Exception as exc:  # pragma: no cover - defensive health path
        logger.warning(
            "kg.health.partition_integrity_debt_failed board=%s err=%s",
            board_id,
            exc,
        )
        partition_debt_open = 0
    partition_cognitive_pending = int(
        artifact_snapshot.get("partition_cognitive_pending") or 0
    )
    partition_blocking = partition_debt_open + partition_cognitive_pending
    if partition_blocking > 0:
        health_diagnostics["health_issues"].append(
            {
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
            }
        )
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = "canonical_partition_integrity"
            health_diagnostics["operator_action"] = (
                "inspect_canonical_partition_integrity"
            )

    digest_inputs = parity_snapshot["digest_inputs"]
    digest_layer_mismatches: list[dict[str, Any]] = []
    digest_probe_status = str(digest_inputs.get("status") or "unavailable")
    digest_probe_reason = str(digest_inputs.get("reason") or graph_probe.reason)
    if graph_probe.status != "unavailable" and digest_probe_status == "available":
        overlay: dict[str, str] = {}
        if digest_inputs.get("needs_overlay"):
            try:
                overlay = await _load_digest_partition_overlay(
                    board_id=board_id,
                )
            except Exception as exc:
                digest_probe_status = "unavailable"
                digest_probe_reason = type(exc).__name__
        if digest_probe_status == "available":
            from okto_pulse.core.kg.global_discovery.layer_parity import (
                evaluate_digest_layer_mismatch_inputs,
            )

            digest_evaluation = evaluate_digest_layer_mismatch_inputs(
                digest_inputs,
                overlay=overlay,
            )
            if (
                digest_evaluation.get("status") == "available"
                and digest_evaluation.get("evaluation") == "evaluated"
                and isinstance(digest_evaluation.get("items"), list)
            ):
                digest_layer_mismatches = list(digest_evaluation["items"])
            else:
                digest_probe_status = "unavailable"
                digest_probe_reason = str(
                    digest_evaluation.get("reason")
                    or "digest_layer_parity_not_evaluated"
                )

    stale_items = list(parity_snapshot.get("stale_board_items") or [])
    stale_probe_status = str(parity_snapshot.get("stale_board_status") or "unavailable")
    stale_digest_node_ids = {
        str(item.get("original_node_id") or "") for item in digest_layer_mismatches
    }
    for item in stale_items:
        item["global_discovery_stale_digest"] = (
            str(item.get("node_id") or "") in stale_digest_node_ids
            if digest_probe_status == "available"
            else None
        )
    stale_parity = {
        "count": len(stale_items),
        "items": stale_items,
        "global_discovery_evaluation": (
            "evaluated" if digest_probe_status == "available" else "not_evaluated"
        ),
    }
    probe_diagnostics["stale_canonical_parity"] = {
        "status": stale_probe_status,
        "reason": "ok" if stale_probe_status == "available" else parity_probe.reason,
    }
    probe_diagnostics["digest_layer_parity"] = {
        "status": digest_probe_status,
        "reason": digest_probe_reason,
    }

    # R2-IMP4 — stale_canonical_parity: canonical DETERMINISTIC board-graph nodes
    # whose SQL source regressed below canonical eligibility (read-only diagnostic;
    # never demotes/reconciles/syncs). DISTINCT category. Ranked BELOW
    # canonical_debt_open / cognitive_consolidation_pending /
    # canonical_partition_integrity (and any DLQ/operational failure) so it NEVER
    # masks an R7/debt/cognitive blocker, and ABOVE digest_vs_board_layer_mismatch
    # (the source regression is the more specific cause; the digest mismatch is its
    # R1 consequence).
    if stale_parity.get("count"):
        _scp_sample = stale_parity["items"][0]
        health_diagnostics["health_issues"].append(
            {
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
                    "expected_maturity_status": _scp_sample.get(
                        "expected_maturity_status"
                    ),
                    "current_source_status": _scp_sample.get("current_source_status"),
                    "recommended_action": _scp_sample.get("recommended_action"),
                },
                "precedence_explanation": (
                    "Ranked BELOW canonical_debt_open, cognitive_consolidation_pending, "
                    "canonical_partition_integrity and DLQ/operational failures (never "
                    "masks them) and ABOVE digest_vs_board_layer_mismatch; a distinct "
                    "stale-source category (no double-count)."
                ),
            }
        )
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = "stale_canonical_parity"
            health_diagnostics["operator_action"] = "inspect_stale_canonical_parity"

    # R1-IMP2 — digest_vs_board_layer_mismatch: a published DecisionDigest
    # graph_layer diverging from the expected_digest_layer recomputed from the
    # board graph (read-only; the R1-IMP1 reconciler clears these on drain).
    # Precedence: BELOW canonical_debt_open / cognitive_consolidation_pending /
    # canonical_partition_integrity (only claims primary if still "none"), ABOVE
    # orphan_integrity_warning. Distinct cause -> no double-count with those.
    if digest_layer_mismatches:
        _ddm_sample = digest_layer_mismatches[0]
        health_diagnostics["health_issues"].append(
            {
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
            }
        )
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = (
                "digest_vs_board_layer_mismatch"
            )
            health_diagnostics["operator_action"] = "inspect_digest_layer_mismatch"

    unavailable_hot_probes = sorted(
        name
        for name in (
            _GRAPH_HEALTH_PROBE,
            _ARTIFACT_HEALTH_PROBE,
            _PARITY_HEALTH_PROBE,
            _DISCOVERY_HEALTH_PROBE,
            "cognitive_items",
            "stale_canonical_parity",
            "digest_layer_parity",
        )
        if probe_diagnostics.get(name, {}).get("status") == "unavailable"
    )
    if unavailable_hot_probes:
        rest_metric_status = "unavailable"
        reason = "health_probe_unavailable:" + ",".join(unavailable_hot_probes)
        if reason not in combined_reasons:
            combined_reasons.append(reason)
        classification_reason = ";".join(combined_reasons)
        if _STATE_SEVERITY[overall_state] < _STATE_SEVERITY[HealthState.AT_RISK]:
            overall_state = HealthState.AT_RISK
        health_diagnostics["health_issues"].append(
            {
                "code": "health_probe_unavailable",
                "component": "kg_health",
                "severity": "warning",
                "reason": reason,
                "description": (
                    "One or more bounded KG Health probes did not complete within "
                    "the read budget; the endpoint returned fail-safe projections."
                ),
                "operator_action": "inspect_probe_diagnostics",
                "probes": unavailable_hot_probes,
            }
        )
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = "health_probe_unavailable"
            health_diagnostics["operator_action"] = "inspect_probe_diagnostics"

    if orphan_integrity.get("integrity_warning"):
        if _STATE_SEVERITY[graph_state] < _STATE_SEVERITY[HealthState.AT_RISK]:
            graph_state = HealthState.AT_RISK
        overall_state = max(
            graph_state,
            effective_discovery_state,
            key=lambda s: _STATE_SEVERITY[s],
        )
        if "graph:orphan_integrity_warning" not in combined_reasons:
            combined_reasons.append("graph:orphan_integrity_warning")
        classification_reason = ";".join(combined_reasons)
        health_diagnostics["health_issues"].append(
            {
                "code": "orphan_integrity_warning",
                "component": "board_graph",
                "severity": "warning",
                "reason": "orphan_count_gt_zero",
                "description": (
                    f"{int(orphan_integrity.get('orphan_count') or 0)} "
                    "non-allowlisted orphan KG node(s) remain. This is graph "
                    "integrity debt, not by itself a graph recovery signal."
                ),
                "operator_action": "inspect_orphan_integrity_report",
            }
        )
        if health_diagnostics["primary_health_cause"] == "none":
            health_diagnostics["primary_health_cause"] = "orphan_integrity_warning"
            health_diagnostics["operator_action"] = "inspect_orphan_integrity_report"

    # R6-IMP2 (FR2/AC2): the ACTIVE operational-queue backlog is its OWN signal
    # with its own drill-down tool — distinct from dead-letter / canonical debt /
    # cognitive pending. Surfaced only when there is active depth; only promoted to
    # primary_health_cause when stuck/backpressure (transient is normal in-flight).
    if active_queue["total_active_depth"] > 0:
        _aq_class = active_queue["classification"]
        health_diagnostics["health_issues"].append(
            {
                "code": f"active_queue_{_aq_class}",
                "component": "operational_queue",
                "severity": "warning"
                if _aq_class in ("stuck", "backpressure")
                else "info",
                "reason": f"active_queue:{_aq_class}",
                "description": (
                    f"{active_queue['total_active_depth']} active operational-queue "
                    f"item(s) ({_aq_class}) across consolidation_queue + "
                    "global_update_outbox. Distinct from dead-letter and canonical debt."
                ),
                "operator_action": "inspect_active_queue",
                "drill_down_tool": "okto_pulse_kg_queue_drilldown",
                "counts": {
                    s["source"]: s["queue_depth"] for s in active_queue["sources"]
                },
            }
        )
        if (
            _aq_class in ("stuck", "backpressure")
            and health_diagnostics["primary_health_cause"] == "none"
        ):
            health_diagnostics["primary_health_cause"] = f"active_queue_{_aq_class}"
            health_diagnostics["operator_action"] = "inspect_active_queue"

    # Explicit, deduplicated separation of operational domains. Terminal global
    # discovery delivery failures are not consolidation DLQ and never inflate
    # the active retry-window count.
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
        "global_outbox_dead_letter": {
            "domain": "global_outbox_dead_letter",
            "semantics": "terminal_global_discovery_delivery_failure",
            "count": global_outbox_dead_letter_count,
            "oldest_age_seconds": global_outbox_dead_letter["oldest_age_seconds"],
            "drill_down_tool": (
                "okto_pulse_kg_global_outbox_dead_letter_list"
            ),
            "drill_down_signal": "global_outbox_dead_letter",
        },
        "canonical_debt": {
            "domain": "canonical_debt",
            "semantics": "semantic_canonicality_pending",
            "count": int(canonical_debt.get("open_count") or 0),
            "drill_down_tool": "okto_pulse_kg_canonical_debt_list",
        },
    }

    # SPEC4 (card 2e913ac3): structured, bounded recovery root-cause block.
    # The safe-write probe reads an in-memory counter (cheap). The source
    # enumeration probe reads SQLite, so it runs only when recovery root-cause
    # actually matters (degraded board or zero materialized nodes) — a healthy
    # board's hot health path stays cheap.
    safe_write_diag = _probe_safe_write_diagnostics(board_id)
    _needs_source_probe = (
        empty_after_materialized_history
        or total_nodes == 0
        or overall_state != HealthState.HEALTHY
    )
    source_diag = (
        artifact_snapshot["source_diag"]
        if _needs_source_probe
        else {
            "source_count": None,
            "canonical_source_count": None,
            "working_source_count": None,
            "enumeration_failure": False,
            "error": None,
            "skipped": "board_healthy",
        }
    )
    recovery_states = {
        HealthState.RECOVERY_NEEDED,
        HealthState.QUARANTINED,
    }
    if (
        effective_discovery_state in recovery_states
        and graph_state not in recovery_states
    ):
        root_cause_scope = "discovery"
    elif graph_state in recovery_states:
        root_cause_scope = "graph"
    elif _STATE_SEVERITY[effective_discovery_state] > _STATE_SEVERITY[graph_state]:
        root_cause_scope = "discovery"
    else:
        root_cause_scope = "graph"

    root_cause = _build_kg_root_cause(
        total_nodes=total_nodes,
        queue_depth=queue_depth,
        dead_letter_count=dead_letter_count,
        active_queue=active_queue,
        empty_after_materialized_history=empty_after_materialized_history,
        combined_reasons=combined_reasons,
        source_diag=source_diag,
        safe_write_diag=safe_write_diag,
        scope=root_cause_scope,
    )
    # Card detail #4: an unavailable recovery drill-down must NOT read as healthy.
    if root_cause["drilldown_unavailable"]:
        if overall_state == HealthState.HEALTHY:
            overall_state = HealthState.AT_RISK
        if "drilldown.source_enumeration.unavailable" not in combined_reasons:
            combined_reasons.append("drilldown.source_enumeration.unavailable")
        classification_reason = ";".join(combined_reasons)

    payload = {
        # --- KG-01 REST contract api_3ed9037f ---
        "board_id": board_id,
        "graph_state": graph_state.value,
        "discovery_state": effective_discovery_state.value,
        "overall_state": overall_state.value,
        "current_kg_generation_id": current_kg_generation_id,
        "metric_status": rest_metric_status,
        "classification_reason": classification_reason,
        "materialization_state": materialization_state,
        "materialization_generation": materialization_generation,
        "probe_reason_codes": probe_reason_codes,
        "correlation_id": correlation_id,
        "recent_events": recent_events,
        "checked_at": checked_at,
        "probe_diagnostics": probe_diagnostics,
        # --- Legacy / dashboard surface (backward compat) ---
        "queue_depth": int(queue_depth),
        "oldest_pending_age_s": (
            round(oldest_pending_age_s, 3) if oldest_pending_age_s is not None else None
        ),
        "dead_letter_count": int(dead_letter_count),
        "global_outbox_dead_letter_count": int(global_outbox_dead_letter_count),
        # R6-IMP2: active operational-queue drill-down (sources/worker_mode/
        # classification). Read-only; DLQ/canonical debt excluded.
        "active_queue": active_queue,
        # R6-IMP5: deduplicated 3-domain separation (active_queue / dead_letter /
        # canonical_debt), each with its own count + drill-down tool.
        "operational_domains": operational_domains,
        # SPEC4 (card 2e913ac3): structured bounded recovery root-cause —
        # distinguishes wal_or_commit / empty_after_materialized_history /
        # source_enumeration_failure / safe_write_drain_failure with materialized
        # node count, source count, queue state and last safe-write outcome.
        "root_cause": root_cause,
        "total_nodes": total_nodes,
        "default_score_count": default_score_count,
        "default_score_ratio": round(default_score_ratio, 4),
        "avg_relevance": graph_metrics["avg_relevance"],
        "source_count": (
            int(source_diag["source_count"])
            if source_diag.get("source_count") is not None
            else None
        ),
        "schema_version": LEGACY_HEALTH_SCHEMA_VERSION,
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
    try:
        from okto_pulse.core.observability.materialization_health import (
            record_materialization_classification,
        )

        record_materialization_classification(
            board_id=board_id,
            materialization_state=materialization_state,
            metric_status=rest_metric_status,
            classification_reason=classification_reason,
            materialization_generation=materialization_generation,
            probe_reason_codes=probe_reason_codes,
        )
    except Exception as exc:  # observability must never break health availability
        logger.warning(
            "kg.health.materialization_observability_failed board=%s error=%s",
            board_id,
            type(exc).__name__,
            extra={
                "event": "kg.health.materialization_observability_failed",
                "board_id": board_id,
                "error_type": type(exc).__name__,
            },
        )
    return payload


def _get_graph_schema_version(board_id: str) -> str | None:
    try:
        from okto_pulse.core.kg.kg_service import get_kg_service

        return get_kg_service().get_schema_version(board_id)
    except Exception as exc:
        logger.debug(
            "kg.health.graph_schema_lookup_failed board=%s err=%s",
            board_id,
            exc,
        )
        return None


async def _has_materialized_kg_history(db: Any, board_id: str) -> bool:
    """Return True when SQLite says the board had materialized KG content.

    If embedded graph backend reports zero nodes while graph reference/audit rows still show
    previous commits, the graph is not merely "empty"; it has lost visibility
    into previously materialized content and should be classified as recovery
    needed.
    """
    try:
        return await get_kg_health_read_port().has_materialized_history(
            db,
            board_id=board_id,
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug(
            "kg.health.materialized_audit_probe_failed board=%s err=%s",
            board_id,
            exc,
        )
        return False


# ---------------------------------------------------------------------------
# SPEC4 (card 2e913ac3): structured recovery root-cause diagnostics.
# Read-only probes that distinguish the FOUR recovery_needed root causes with
# bounded fields, reusing the deterministic rebuild source enumerator and the
# safe-write lifecycle counter. Never mutate, never rebuild.
# ---------------------------------------------------------------------------

#: Safe-write lifecycle outcomes that signal a drain/commit failure.
_SAFE_WRITE_FAILURE_OUTCOMES: frozenset[str] = frozenset(
    {"failed", "boundary_violation"}
)
#: Severity rank to pick the worst observed safe-write outcome. The bounded
#: counter has no per-event timestamp, so "last" is reported as "worst observed".
_SAFE_WRITE_OUTCOME_SEVERITY: dict[str, int] = {
    "applied": 0,
    "blocked": 1,
    "owner_token_required": 2,
    "failed": 3,
    "boundary_violation": 3,
}


def _bounded_probe_error(exc: BaseException) -> str:
    """Bounded, body-free description of a probe failure (type + short msg)."""
    msg = str(exc).replace("\n", " ").strip()
    if len(msg) > 200:
        msg = msg[:200] + "…"
    return f"{type(exc).__name__}: {msg}" if msg else type(exc).__name__


def _probe_rebuild_source_diagnostics(board_id: str) -> dict[str, Any]:
    """Read-only probe of the deterministic rebuild source enumeration (D1/D3).

    Returns bounded source counts + an ``enumeration_failure`` flag. Reuses the
    SAME ``RebuildSourceEnumerator`` the formal preflight uses, so the count is
    the reexecutable source set (NOT the materialized graph reference count). Never
    raises, never rebuilds.
    """
    try:
        from okto_pulse.core.kg.rebuild_sources import RebuildSourceEnumerator

        from okto_pulse.core.application.kg_rebuild import build_source_store

        source_set = RebuildSourceEnumerator(
            source_store=build_source_store()
        ).enumerate(
            board_id=board_id
        )
        canonical = int(source_set.canonical_source_count)
        working = int(source_set.working_source_count)
        return {
            "source_count": canonical + working,
            "canonical_source_count": canonical,
            "working_source_count": working,
            "enumeration_failure": False,
            "error": None,
        }
    except Exception as exc:  # bounded — source store unavailable / schema drift
        logger.warning(
            "kg.health.source_enumeration_probe_failed board=%s err=%s",
            board_id,
            exc,
        )
        return {
            "source_count": None,
            "canonical_source_count": None,
            "working_source_count": None,
            "enumeration_failure": True,
            "error": _bounded_probe_error(exc),
        }


def _probe_safe_write_diagnostics(board_id: str) -> dict[str, Any]:
    """Read-only summary of safe-write lifecycle outcomes for the board (D2).

    Derives the outcome from the EXISTING bounded lifecycle counter. The counter
    has no per-event timestamp, so ``last_safe_write_outcome`` reports the WORST
    observed outcome (a conservative recovery signal) or ``"unknown"`` when no
    safe-write was recorded. NEVER touches the write-path.
    """
    try:
        from okto_pulse.core.kg.safe_write_lifecycle import (
            get_lifecycle_counter_samples,
        )

        outcomes: dict[str, int] = {}
        for s in get_lifecycle_counter_samples():
            if s.get("board_id") != board_id:
                continue
            count = int(s.get("count") or 0)
            if count <= 0:
                continue
            out = str(s.get("outcome"))
            outcomes[out] = outcomes.get(out, 0) + count
        if not outcomes:
            return {
                "last_safe_write_outcome": "unknown",
                "drain_failure": False,
                "outcomes": {},
            }
        worst = max(outcomes, key=lambda o: _SAFE_WRITE_OUTCOME_SEVERITY.get(o, 0))
        drain_failure = any(o in _SAFE_WRITE_FAILURE_OUTCOMES for o in outcomes)
        return {
            "last_safe_write_outcome": worst,
            "drain_failure": drain_failure,
            "outcomes": outcomes,
        }
    except Exception as exc:  # bounded — counter unreadable
        logger.warning(
            "kg.health.safe_write_probe_failed board=%s err=%s",
            board_id,
            exc,
        )
        return {
            "last_safe_write_outcome": "unknown",
            "drain_failure": False,
            "outcomes": {},
            "probe_error": _bounded_probe_error(exc),
        }


def _build_kg_root_cause(
    *,
    total_nodes: int,
    queue_depth: int,
    dead_letter_count: int,
    active_queue: dict[str, Any],
    empty_after_materialized_history: bool,
    combined_reasons: list[str],
    source_diag: dict[str, Any],
    safe_write_diag: dict[str, Any],
    scope: str = "graph",
) -> dict[str, Any]:
    """Assemble the structured, bounded recovery root-cause block (FR fr_66eeff50,
    TR tr_be1dc85d).

    Distinguishes the FOUR root causes — wal_or_commit_errors,
    empty_after_materialized_history, source_enumeration_failure and
    safe_write_drain_failure — each with ``present`` + bounded detail, plus the
    bounded fields (materialized node count, source count, queue state, last
    safe-write outcome). Additive: never replaces ``classification_reason``.
    """
    if scope not in {"graph", "discovery"}:
        raise ValueError("scope must be 'graph' or 'discovery'")

    wal_present_scopes = sorted(
        reason_scope
        for reason_scope in ("graph", "discovery")
        if f"{reason_scope}:wal_or_commit_errors.present" in combined_reasons
    )
    wal_or_commit_present = scope in wal_present_scopes
    graph_scope = scope == "graph"
    source_enum_failure = graph_scope and bool(source_diag.get("enumeration_failure"))
    safe_write_drain_failure = graph_scope and bool(
        safe_write_diag.get("drain_failure")
    )
    categories = {
        "wal_or_commit_errors": {
            "present": wal_or_commit_present,
            "scope": scope,
            "present_scopes": wal_present_scopes,
        },
        "empty_after_materialized_history": {
            "present": graph_scope and bool(empty_after_materialized_history),
            "scope": "graph",
            "applicable": graph_scope,
            "materialized_node_count": int(total_nodes),
            "source_count": source_diag.get("source_count"),
        },
        "source_enumeration_failure": {
            "present": source_enum_failure,
            "scope": "graph",
            "applicable": graph_scope,
            "error": source_diag.get("error"),
        },
        "safe_write_drain_failure": {
            "present": safe_write_drain_failure,
            "scope": "graph",
            "applicable": graph_scope,
            "outcomes": safe_write_diag.get("outcomes", {}),
        },
    }
    return {
        "scope": scope,
        "classification_reasons": [
            reason for reason in combined_reasons if reason.startswith(f"{scope}:")
        ],
        "materialized_node_count": int(total_nodes),
        "source_count": source_diag.get("source_count"),
        "queue_state": {
            "queue_depth": int(queue_depth),
            "active_classification": active_queue.get("classification"),
            "active_depth": int(active_queue.get("total_active_depth") or 0),
            "dead_letter_count": int(dead_letter_count),
        },
        "last_safe_write_outcome": safe_write_diag.get(
            "last_safe_write_outcome", "unknown"
        ),
        "categories": categories,
        # When the source-enumeration recovery drill-down can't be read, the
        # recovery surface is incomplete and must NOT read as healthy (#4).
        "drilldown_unavailable": source_enum_failure,
        "present_categories": [
            name for name, c in categories.items() if c.get("present")
        ],
    }


def _aggregate_graph_metrics(board_id: str) -> dict[str, Any]:
    """Pull node-level aggregates from graph backend for ``board_id``.

    Returns a dict with total_nodes, default_score_count and avg_relevance.
    On any graph backend error (board not bootstrapped, schema drift, lock contention)
    returns zeroed defaults so the health endpoint stays available.
    """
    try:
        from okto_pulse.core.kg.interfaces import get_kg_registry
        from okto_pulse.core.kg.schema_contract import NODE_TYPES
    except Exception as exc:
        logger.warning(
            "kg.health.graph_import_failed board=%s err=%s",
            board_id,
            exc,
        )
        return _zero_graph_metrics()

    total_nodes = 0
    default_score_count = 0
    relevance_sum = 0.0
    relevance_n = 0

    try:
        cypher = get_kg_registry().cypher_executor
        for node_type in NODE_TYPES:
            try:
                result = cypher.execute_read_only(
                    board_id,
                    f"MATCH (n:{node_type}) RETURN n.relevance_score",
                    {},
                    max_rows=10000,
                )
            except Exception as exc:
                logger.debug(
                    "kg.health.graph_query_failed board=%s type=%s err=%s",
                    board_id,
                    node_type,
                    exc,
                )
                continue
            for row in result.get("rows", []):
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
            "kg.health.graph_open_failed board=%s err=%s",
            board_id,
            exc,
        )
        return _zero_graph_metrics()

    avg_relevance = round(relevance_sum / relevance_n, 4) if relevance_n > 0 else 0.0

    return {
        "total_nodes": total_nodes,
        "default_score_count": default_score_count,
        "avg_relevance": avg_relevance,
    }


def _zero_graph_metrics() -> dict[str, Any]:
    return {
        "total_nodes": 0,
        "default_score_count": 0,
        "avg_relevance": 0.0,
    }


def _aggregate_kg_layer_counts(board_id: str) -> dict[str, Any]:
    """Count nodes by graph_layer and maturity_status.

    This is additive diagnostic telemetry. Any graph backend/schema error degrades to an
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
        from okto_pulse.core.kg.interfaces import get_kg_registry
        from okto_pulse.core.kg.schema_contract import NODE_TYPES
    except Exception as exc:
        logger.warning(
            "kg.health.layer_counts_import_failed board=%s err=%s",
            board_id,
            exc,
        )
        return {
            "status": "unavailable",
            "by_layer": counts,
            "by_maturity_status": maturity_counts,
            "reason": "schema_import_failed",
        }

    try:
        cypher = get_kg_registry().cypher_executor
        successful_node_types = 0
        failed_node_types = 0
        for node_type in NODE_TYPES:
            try:
                result = cypher.execute_read_only(
                    board_id,
                    f"MATCH (n:{node_type}) "
                    f"RETURN n.graph_layer, n.maturity_status, count(n)",
                    max_rows=10000,
                )
                for row in result.get("rows", []):
                    layer = str(row[0] or "unclassified")
                    maturity = str(row[1] or "unclassified")
                    count = int(row[2] or 0)
                    counts[layer] = counts.get(layer, 0) + count
                    maturity_counts[maturity] = maturity_counts.get(maturity, 0) + count
                successful_node_types += 1
            except Exception:
                failed_node_types += 1
                continue
    except Exception as exc:
        logger.warning(
            "kg.health.layer_counts_failed board=%s err=%s",
            board_id,
            exc,
        )
        return {
            "status": "unavailable",
            "by_layer": counts,
            "by_maturity_status": maturity_counts,
            "reason": "graph_open_failed",
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
