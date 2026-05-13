"""Local metrics transparency and consent endpoints."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.config import get_settings
from okto_pulse.core.telemetry.service import TelemetryService

router = APIRouter()


class MetricsSettingsPayload(BaseModel):
    mode: Literal["disabled", "local_only", "anonymous_beacon"]
    source: Literal["settings_ui"] = "settings_ui"
    retention_days: int | None = Field(default=None, ge=1, le=400)
    beacon_url: str | None = None
    policy_version: str | None = None
    schema_version: str | None = None
    acknowledged_items: list[str] = Field(default_factory=list)


@router.get("/metrics/local/summary")
async def get_local_metrics_summary(
    window_days: int = Query(default=30, ge=1, le=400),
    _: str = Depends(require_user),
):
    service = TelemetryService(get_settings())
    return service.summary(window_days=window_days)


@router.post("/metrics/settings")
async def post_metrics_settings(
    payload: MetricsSettingsPayload,
    _: str = Depends(require_user),
):
    required_ack = {
        "schema",
        "privacy_policy",
        "hourly_aggregates",
        "product_aggregates",
        "no_pii",
        "local_control",
    }
    if payload.mode == "anonymous_beacon":
        if not payload.policy_version or not payload.schema_version:
            raise HTTPException(status_code=409, detail="OPT_IN_PREREQUISITES_NOT_APPROVED")
        if not required_ack.issubset(set(payload.acknowledged_items)):
            raise HTTPException(status_code=400, detail="MISSING_POLICY_ACK")
    service = TelemetryService(get_settings())
    try:
        return service.update_settings(
            mode=payload.mode,
            source=payload.source,
            policy_version=payload.policy_version,
            schema_version=payload.schema_version,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/metrics/local/export")
async def export_local_metrics(
    _: str = Depends(require_user),
):
    return TelemetryService(get_settings()).export_local()


@router.delete("/metrics/local")
async def purge_local_metrics(
    _: str = Depends(require_user),
):
    return TelemetryService(get_settings()).purge_local()
