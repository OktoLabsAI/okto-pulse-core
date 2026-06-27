"""REST endpoints for runtime settings (0.1.4).

Exposes ``GET`` and ``PUT`` on ``/api/v1/settings/runtime`` so the frontend
Settings menu can read/modify Kùzu memory tuning knobs. Ranges match the
Pydantic validators on :class:`CoreSettings` — invalid values are rejected
with 422.

Kùzu ``Database()`` is constructor-time, so writes only take effect on
the next process restart. The response includes ``restart_required`` to let
the UI display a banner.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.config import validate_graph_db_max_size_gb
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.services.settings_service import (
    ConfigChangeBlocked,
    get_runtime_settings,
    put_runtime_settings,
)

router = APIRouter()


class RuntimeSettingsResponse(BaseModel):
    """GET/PUT response shape — Graph DB keys + Event Queue keys (spec bdcda842).

    ``restart_required`` is true only when a Graph DB key (Kùzu constructor-time)
    diverges from the boot snapshot. Event Queue keys hot-reload via the
    worker pool's 5s settings cache.
    """

    # Graph DB tab — restart-required on change.
    kg_kuzu_buffer_pool_mb: int
    kg_kuzu_max_db_size_gb: int
    kg_connection_pool_size: int
    # Event Queue tab — hot-reload (no restart needed).
    kg_queue_max_concurrent_workers: int
    kg_queue_min_interval_ms: int
    kg_queue_claim_timeout_s: int
    kg_queue_max_attempts: int
    kg_queue_alert_threshold: int
    # Decay Tick tab (spec 54399628) — hot-reload via APScheduler.reschedule_job.
    kg_decay_tick_interval_minutes: int
    kg_decay_tick_staleness_days: int
    kg_decay_tick_max_age_days: int
    restart_required: bool


class RuntimeSettingsPayload(BaseModel):
    """PUT body — every field optional; partial updates are allowed.

    Ranges mirror :class:`CoreSettings` Field validators. Pydantic emits 422
    (FastAPI maps to 400 in the error envelope) with a clear ``greater than
    or equal to`` / ``less than or equal to`` message for violations.
    """

    # Graph DB tab.
    kg_kuzu_buffer_pool_mb: int | None = Field(default=None, ge=128, le=512)
    kg_kuzu_max_db_size_gb: int | None = Field(default=None, ge=2, le=64)
    kg_connection_pool_size: int | None = Field(default=None, ge=1, le=32)
    # Event Queue tab (spec bdcda842).
    kg_queue_max_concurrent_workers: int | None = Field(default=None, ge=1, le=16)
    kg_queue_min_interval_ms: int | None = Field(default=None, ge=0, le=1000)
    kg_queue_claim_timeout_s: int | None = Field(default=None, ge=60, le=3600)
    kg_queue_max_attempts: int | None = Field(default=None, ge=1, le=10)
    kg_queue_alert_threshold: int | None = Field(default=None, ge=100, le=100000)
    # Decay Tick (spec 54399628 — Wave 2 NC f9732afc).
    kg_decay_tick_interval_minutes: int | None = Field(default=None, ge=5, le=10080)
    kg_decay_tick_staleness_days: int | None = Field(default=None, ge=1, le=365)
    kg_decay_tick_max_age_days: int | None = Field(default=None, ge=0, le=365)
    # KG-01.5 guard inputs — optional; for graph-DB changes these may be
    # required by KGConfigChangeGuard (storage/wal/index need a migration
    # plan ref; buffer requires restart_policy in {required, scheduled}).
    # Default restart_policy for graph-DB changes is "required" when
    # omitted — matches the existing "restart_required" semantics.
    migration_plan_ref: str | None = Field(default=None, max_length=256)
    restart_policy: str | None = Field(default=None, pattern="^(none|required|scheduled)$")

    @field_validator("kg_kuzu_max_db_size_gb")
    @classmethod
    def _validate_graph_db_max_size_gb(cls, value: int | None) -> int | None:
        if value is None:
            return value
        return validate_graph_db_max_size_gb(value)


@router.get("/settings/runtime", response_model=RuntimeSettingsResponse)
async def get_runtime(
    _: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> RuntimeSettingsResponse:
    """Return the currently effective runtime settings + restart flag."""
    data = await get_runtime_settings(db)
    return RuntimeSettingsResponse(**data)


@router.put("/settings/runtime", response_model=RuntimeSettingsResponse)
async def put_runtime(
    payload: RuntimeSettingsPayload,
    request: Request,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> RuntimeSettingsResponse:
    """Persist new runtime settings. Values only take effect after restart.

    KG-01.5: LadybugDB runtime changes pass through ``KGConfigChangeGuard``
    inside ``put_runtime_settings``. If the guard blocks the change, the
    service raises ``ConfigChangeBlocked`` which we surface as HTTP 400
    with a bounded reason code (no raw values leak per TR12).
    """
    payload_dict = payload.model_dump()
    migration_plan_ref = payload_dict.pop("migration_plan_ref", None)
    restart_policy = payload_dict.pop("restart_policy", None)
    # Strip unset value fields — pass only what the caller actually sent.
    values = {k: v for k, v in payload_dict.items() if v is not None}
    # R-P2-06B: resolve the SchedulerControl from the composition-owned provider
    # (``RuntimeComposition.scheduler_control``). Absent -> None -> explicit skip
    # in the service (never the process-global singleton). The Community edition
    # wires the provider in its composition root, preserving the reschedule.
    composition = getattr(request.app.state, "runtime_composition", None)
    scheduler_control = (
        getattr(composition, "scheduler_control", None)
        if composition is not None
        else None
    )
    try:
        data = await put_runtime_settings(
            db,
            values,
            actor_id=user_id,
            migration_plan_ref=migration_plan_ref,
            restart_policy=restart_policy,
            scheduler_control=scheduler_control,
        )
    except ConfigChangeBlocked as exc:
        # Safe error envelope: bounded reason + setting_group + audit_event.
        # Raw values are NEVER in the response body (TR12).
        raise HTTPException(
            status_code=400,
            detail={
                "error": "kg_config_change_blocked",
                "reason": exc.reason,
                "setting_group": exc.setting_group,
                "audit_event": exc.audit_event,
            },
        ) from exc
    return RuntimeSettingsResponse(**data)
