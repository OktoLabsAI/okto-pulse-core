"""Local metrics transparency and consent endpoints."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.config import get_settings
from okto_pulse.core.telemetry.schema import SchemaReject, count_rejected_payload_fields, sanitize_payload
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


class LocalMetricsEventPayload(BaseModel):
    event_type: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)


def _public_event_response(*, written: bool, rejected_fields_count: int, schema_version: str) -> dict[str, Any]:
    return {
        "written": written,
        "rejected_fields_count": max(0, rejected_fields_count),
        "schema_version": schema_version,
    }


def _public_event_error(error: str, *, rejected_fields_count: int, schema_version: str) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content={
            "error": error,
            **_public_event_response(
                written=False,
                rejected_fields_count=max(1, rejected_fields_count),
                schema_version=schema_version,
            ),
        },
    )


@router.get("/metrics/local/summary")
async def get_local_metrics_summary(
    window_days: int = Query(default=30, ge=1, le=400),
    _: str = Depends(require_user),
):
    service = TelemetryService(get_settings())
    return service.summary(window_days=window_days)


@router.post("/metrics/local/events")
async def post_local_metrics_event(
    payload: LocalMetricsEventPayload,
    _: str = Depends(require_user),
):
    service = TelemetryService(get_settings())
    cfg = service.config()
    schema_version = cfg.schema_version
    rejected_fields_count = count_rejected_payload_fields(payload.event_type, payload.payload)
    if payload.event_type != "guided_help":
        return _public_event_error(
            "INVALID_EVENT_TYPE",
            rejected_fields_count=rejected_fields_count,
            schema_version=schema_version,
        )
    try:
        sanitize_payload(payload.event_type, payload.payload)
    except SchemaReject:
        return _public_event_error(
            "INVALID_PAYLOAD",
            rejected_fields_count=rejected_fields_count,
            schema_version=schema_version,
        )
    try:
        result = service.record_event(payload.event_type, payload.payload)
    except Exception:
        return JSONResponse(
            status_code=503,
            content={
                "error": "EVENT_STORE_FAILED",
                **_public_event_response(
                    written=False,
                    rejected_fields_count=rejected_fields_count,
                    schema_version=schema_version,
                ),
            },
        )
    return _public_event_response(
        written=bool(result.get("written")),
        rejected_fields_count=int(result.get("rejected_fields_count") or 0),
        schema_version=str(result.get("schema_version") or schema_version),
    )


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
            acknowledged_items=payload.acknowledged_items,
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
