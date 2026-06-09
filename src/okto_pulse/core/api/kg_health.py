"""REST endpoint for the KG health snapshot.

KG-01 spec a7659ba3 / contract api_3ed9037f. GET /api/v1/kg/health returns
the per-graph health classification (graph + discovery + overall), the
deterministic memory-pressure correlation and the legacy aggregation
fields the dashboard still consumes. The endpoint is READ-ONLY — it must
NEVER call quarantine, purge, rebuild or any storage-mutating path.

Defaults are conservative by contract: when telemetry can't be read (the
KG-01.5 instrumentation hasn't landed yet) the endpoint emits
`metric_status=unavailable` and `*_state=at_risk` instead of degrading
silently to "healthy". This is enforced by BR br_2a8cdfdc ("Health
unavailable is not zero").
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.services.kg_health_service import (
    BoardNotFoundError,
    get_kg_health,
)

router = APIRouter()


class TopDisconnectedNode(BaseModel):
    id: str
    type: str
    degree: int


class RecentHealthEvent(BaseModel):
    """One row in `recent_events`. KG-01 contract api_3ed9037f."""

    occurred_at: str
    event_type: str
    reason: str
    correlation_id: str


class HealthIssue(BaseModel):
    """UI-facing explanation for one health signal.

    This is intentionally additive to the canonical KG-01 state machine. The
    dashboard can render tooltips/actions from these rows without weakening the
    conservative `metric_status=unavailable` policy.
    """

    code: str
    component: str
    severity: str
    reason: str
    description: str
    operator_action: str


class DecaySchedulerDiagnostics(BaseModel):
    """Operational decay scheduler state, separate from graph recovery."""

    status: str
    severity: str
    last_success_at: str | None = None
    last_failure_at: str | None = None
    last_error: str | None = None
    next_scheduled_at: str | None = None
    stale_tolerance_seconds: int | None = None
    recommended_action: str
    operational_debt: bool
    graph_recovery_required: bool = False
    reason: str | None = None
    running_started_at: str | None = None
    source: str = "kg_tick_runs"


class StorageFootprintProxy(BaseModel):
    """On-disk file-size proxy; not direct memory/buffer telemetry."""

    source: str = "file_size_proxy"
    status: str = "unavailable"
    percentage: float | None = None
    high_water_mark_pct: float | None = None
    graph_lbug_bytes: int | None = None
    sidecar_bytes: int | None = None
    total_bytes: int | None = None
    configured_max_db_size_bytes: int | None = None
    configured_max_db_size_gb: int | None = None
    is_direct_memory_telemetry: bool = False
    description: str
    tooltip: str
    unavailable_reason: str | None = None


class OrphanIntegritySample(BaseModel):
    node_id: str
    node_type: str
    writer_path: str
    source_artifact_ref: str | None = None
    source_resolution_status: str
    generation_id: str | None = None
    reason: str
    correlation_id: str


class OrphanIntegrityProjection(BaseModel):
    classification_delta: str = "none"
    integrity_warning: bool = False
    orphan_count: int = 0
    orphan_count_by_type: dict[str, int] = Field(default_factory=dict)
    samples: list[OrphanIntegritySample] = Field(default_factory=list)
    unresolved_reasons: dict[str, int] = Field(default_factory=dict)
    allowlisted_root_count: int = 0
    generation_id: str | None = None
    correlation_id: str | None = None
    zero_orphan_validation: str = "not_evaluated"
    reason: str = "orphan_scan_not_evaluated"


class KGHealthResponse(BaseModel):
    """Live KG health snapshot for one board.

    Field set is the union of:
    - KG-01 REST contract (api_3ed9037f, required) — board_id, graph_state,
      discovery_state, overall_state, current_kg_generation_id,
      metric_status, classification_reason, correlation_id, recent_events,
      checked_at.
    - Legacy aggregation fields preserved for backward compat with the
      existing dashboard (queue_depth, dead_letter_count, total_nodes, etc.).

    `metric_status` is intentionally restricted to `available|unavailable`
    at the REST surface (contract). The internal classifier may produce
    `partial`, which the service maps to `unavailable` before serialising
    so callers can't observe ambiguous intermediate states.

    Defaults are conservative (`at_risk`, `unavailable`) so a malformed
    composition path never accidentally publishes `healthy` — BR
    br_2a8cdfdc forbids degrading silently to zero/healthy on telemetry
    failure.
    """

    # --- KG-01 contract api_3ed9037f (required) ---
    board_id: str
    graph_state: str = "at_risk"
    discovery_state: str = "at_risk"
    overall_state: str = "at_risk"
    current_kg_generation_id: str | None = None
    metric_status: str = "unavailable"
    classification_reason: str = "metric.unavailable"
    correlation_id: str
    recent_events: list[RecentHealthEvent] = []
    checked_at: str

    # --- Legacy / dashboard fields (preserved for backward compatibility) ---
    queue_depth: int
    oldest_pending_age_s: float
    dead_letter_count: int
    total_nodes: int
    default_score_count: int
    default_score_ratio: float
    avg_relevance: float
    top_disconnected_nodes: list[TopDisconnectedNode]
    schema_version: str
    health_schema_version: str = "1.0"
    graph_schema_version: str | None = None
    contradict_warn_count: int
    last_decay_tick_at: str | None = None
    last_tick_status: str | None = None
    last_tick_error: str | None = None
    nodes_recomputed_in_last_tick: int = 0
    # True se o advisory lock global ``kg_daily_tick`` está atualmente
    # acquired (cron OU run-now). Frontend usa para desabilitar o botão
    # "Run tick now" através de remount do componente.
    tick_in_progress: bool = False
    # FR5/FR6 (spec R2b TR5, IMPL-3): board counters from last tick run.
    # Allows callers to distinguish "tick failed before processing any board"
    # (boards_processed_in_last_tick==0) from "processed N boards but M
    # failed" (boards_failed_in_last_tick>0). Defaults to 0 per BR5 when no
    # completed tick_run exists. Aditivo: nenhum campo existente é alterado
    # (br_2a8cdfdc).
    boards_processed_in_last_tick: int = 0
    boards_failed_in_last_tick: int = 0

    # KG-01 internal/debug surface — kept in addition to the contract
    # `graph_state`/`overall_state` so dashboards built against the
    # original 0.2.2 endpoint don't regress. `state` is an alias for
    # `overall_state` and `memory_pressure_status` exposes the correlator
    # outcome verbatim. `classification_reasons` (plural) carries the
    # raw reason tuple while `classification_reason` (singular) is the
    # contract-mandated single string (joined).
    state: str = "at_risk"
    memory_pressure_status: str = "unconfirmed"
    classification_reasons: list[str] = []

    # FR6 (spec R2c): DLQ auto-drain telemetry (additive, default null/0 per
    # BR5 — never 500s when the worker is not running or has no stats yet).
    dlq_auto_drain_last_run_at: str | None = None
    dlq_auto_drain_requeued_count: int = 0

    # KG-HS.1 additive clarity fields. These are explicit objects so REST
    # clients no longer infer scheduler debt or memory pressure from legacy
    # scalar fields.
    decay_scheduler_diagnostics: DecaySchedulerDiagnostics
    storage_footprint_proxy: StorageFootprintProxy
    orphan_integrity: OrphanIntegrityProjection = Field(
        default_factory=OrphanIntegrityProjection
    )

    # Additive UI diagnosis: separates "board graph is actually unreadable or
    # empty after prior materialization" from conservative at_risk states caused
    # by telemetry gaps or dead-letter debt.
    graph_read_status: str = "unknown"
    board_graph_queryable: bool = False
    board_graph_recovery_required: bool = False
    discovery_recovery_required: bool = False
    discovery_health_cause: str = "unknown"
    primary_health_cause: str = "unknown"
    operator_action: str = "inspect_health_details"
    health_issues: list[HealthIssue] = []


@router.get("/kg/health", response_model=KGHealthResponse)
async def get_kg_health_endpoint(
    board_id: str = Query(..., description="Board ID (uuid)"),
    _: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> KGHealthResponse:
    """Return the live KG health snapshot for ``board_id``.

    Compute is in-process: SQL aggregations on the app DB and
    per-node-type queries against the board's Kùzu graph. Kùzu errors
    degrade gracefully (zeros), so the endpoint stays available even when
    Kùzu hasn't been bootstrapped or is under a transient lock.

    Per contract api_3ed9037f the endpoint MUST NOT mutate graph or
    discovery storage. It is read-only.
    """
    try:
        data = await get_kg_health(board_id, db)
    except BoardNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return KGHealthResponse(**data)
