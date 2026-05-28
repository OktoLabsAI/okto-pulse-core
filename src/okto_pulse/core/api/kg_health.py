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
from pydantic import BaseModel
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
