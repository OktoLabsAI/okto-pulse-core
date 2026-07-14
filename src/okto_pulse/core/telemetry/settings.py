"""Telemetry mode, consent and privacy policy over edition-neutral state refs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from okto_pulse.core.infra.config import CoreSettings
from okto_pulse.core.telemetry.effect_config_registry import (
    delivery_target_from_effect_config,
    state_ref_from_effect_config,
)
from okto_pulse.core.telemetry.schema import CURRENT_SCHEMA_VERSION
from okto_pulse.core.telemetry.telemetry_state_registry import (
    load_telemetry_state,
    save_telemetry_state,
)

TelemetryMode = Literal["disabled", "local_only", "anonymous_beacon"]
EffectiveTelemetryMode = Literal["disabled", "anonymous_beacon"]
VALID_MODES = {"disabled", "local_only", "anonymous_beacon"}
DEFAULT_MODE: EffectiveTelemetryMode = "disabled"
LOCAL_ONLY_MIGRATION_NOTICE = "local_only_to_disabled"

logger = logging.getLogger("okto_pulse.telemetry.settings")


@dataclass(frozen=True)
class ResolvedTelemetryConfig:
    mode: EffectiveTelemetryMode
    ui_mode: Literal["off", "on"]
    normalized_from: TelemetryMode | None
    migration_notice: dict[str, Any] | None
    state_ref: str
    retention_days: int
    delivery_target: str
    policy_version: str
    schema_version: str
    source: str
    resolved_precedence: tuple[str, ...]
    state: dict[str, Any]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def coerce_mode(value: str | None) -> TelemetryMode | None:
    normalized = (value or "").strip().lower().replace("-", "_")
    if not normalized:
        return None
    if normalized == "enable_beacon":
        normalized = "anonymous_beacon"
    if normalized in VALID_MODES:
        return normalized  # type: ignore[return-value]
    raise ValueError(f"invalid telemetry mode: {value}")


def _ui_mode(mode: EffectiveTelemetryMode) -> Literal["off", "on"]:
    return "on" if mode == "anonymous_beacon" else "off"


def _migration_notice(state: dict[str, Any]) -> dict[str, Any]:
    notices = (
        state.get("migration_notices")
        if isinstance(state.get("migration_notices"), dict)
        else {}
    )
    notice_state = (
        notices.get(LOCAL_ONLY_MIGRATION_NOTICE) if isinstance(notices, dict) else {}
    )
    if not isinstance(notice_state, dict):
        notice_state = {}
    seen_at = notice_state.get("seen_at")
    return {
        "type": LOCAL_ONLY_MIGRATION_NOTICE,
        "reason": "legacy_local_only_disabled",
        "from_mode": "local_only",
        "to_mode": "disabled",
        "pending": not bool(notice_state.get("seen")),
        "seen_at": seen_at if isinstance(seen_at, str) and seen_at else None,
        "message": "Previous Local metrics mode was migrated to Off.",
    }


def _normalize_effective_mode(
    mode: TelemetryMode,
    *,
    source: str,
) -> tuple[EffectiveTelemetryMode, TelemetryMode | None]:
    if mode != "local_only":
        return mode, None  # type: ignore[return-value]
    logger.info(
        "metrics.mode.normalized",
        extra={
            "metric_name": "metrics_mode_normalized_total",
            "source": source,
            "from_mode": "local_only",
            "to_mode": "disabled",
            "outcome": "normalized",
        },
    )
    return "disabled", "local_only"


def state_ref_for(settings: CoreSettings) -> str:
    return state_ref_from_effect_config(settings)


def load_state(state_ref: str) -> dict[str, Any]:
    """Compatibility wrapper over the registered full-dict carrier."""
    return load_telemetry_state(state_ref)


def save_state(state_ref: str, state: dict[str, Any]) -> None:
    """Compatibility wrapper over the registered full-dict carrier."""
    save_telemetry_state(state_ref, state)


def record_consent(
    settings: CoreSettings,
    *,
    mode: TelemetryMode,
    source: Literal["settings_ui", "cli"],
    policy_version: str | None = None,
    schema_version: str | None = None,
    acknowledged_items: list[str] | None = None,
) -> dict[str, Any]:
    state_ref = state_ref_for(settings)
    current = load_state(state_ref)
    changed_at = iso_now()
    original_mode = mode
    mode, normalized_from = _normalize_effective_mode(mode, source=source)
    policy = policy_version or getattr(settings, "metrics_policy_version", "2026-05-11")
    schema = schema_version or getattr(
        settings, "metrics_schema_version", CURRENT_SCHEMA_VERSION
    )
    if mode == "anonymous_beacon" and schema != CURRENT_SCHEMA_VERSION:
        raise ValueError("UNSUPPORTED_METRICS_SCHEMA")
    next_prompt = None
    if mode != "anonymous_beacon":
        interval = int(getattr(settings, "metrics_opt_in_prompt_interval_days", 30))
        next_prompt = (
            (utc_now() + timedelta(days=interval)).isoformat().replace("+00:00", "Z")
        )
    acknowledgements = list(
        dict.fromkeys(item for item in acknowledged_items or [] if item)
    )
    history = list(current.get("history") or [])
    history.append(
        {
            "mode": mode,
            "source": source,
            "changed_at": changed_at,
            "policy_version": policy,
            "schema_version": schema,
            "acknowledged_items": acknowledgements,
            **({"normalized_from": normalized_from} if normalized_from else {}),
            **({"requested_mode": original_mode} if normalized_from else {}),
        }
    )
    state = {
        **current,
        "mode": mode,
        "source": source,
        "changed_at": changed_at,
        "policy_version": policy,
        "schema_version": schema,
        "acknowledged_items": acknowledgements,
        "next_opt_in_prompt_after": next_prompt,
        "history": history[-50:],
    }
    if normalized_from:
        state["normalized_from"] = normalized_from
    save_state(state_ref, state)
    return state


def mark_migration_notice_seen(
    settings: CoreSettings,
    *,
    notice_key: str,
) -> dict[str, Any]:
    if notice_key != LOCAL_ONLY_MIGRATION_NOTICE:
        raise ValueError("invalid_notice_key")
    state_ref = state_ref_for(settings)
    current = load_state(state_ref)
    notices = (
        current.get("migration_notices")
        if isinstance(current.get("migration_notices"), dict)
        else {}
    )
    existing = notices.get(notice_key) if isinstance(notices, dict) else None
    if isinstance(existing, dict) and existing.get("seen"):
        seen_at = (
            existing.get("seen_at")
            if isinstance(existing.get("seen_at"), str)
            else None
        )
        return {
            "notice_key": notice_key,
            "pending": False,
            "seen_at": seen_at,
            "idempotent": True,
        }

    seen_at = iso_now()
    next_notices = dict(notices or {})
    next_notices[notice_key] = {"seen": True, "seen_at": seen_at}
    save_state(state_ref, {**current, "migration_notices": next_notices})
    return {
        "notice_key": notice_key,
        "pending": False,
        "seen_at": seen_at,
        "idempotent": False,
    }


def resolve_telemetry_config(
    settings: CoreSettings,
    *,
    cli_mode: str | None = None,
    state_snapshot: dict[str, Any] | None = None,
) -> ResolvedTelemetryConfig:
    state_ref = state_ref_for(settings)
    state = (
        dict(state_snapshot) if state_snapshot is not None else load_state(state_ref)
    )
    precedence = (
        "cli_flag",
        "env",
        "community_settings",
        "persisted_consent",
        "default",
    )

    raw_mode: TelemetryMode | None = coerce_mode(cli_mode)
    mode: EffectiveTelemetryMode | None = None
    normalized_from: TelemetryMode | None = None
    source = "cli_flag" if mode else ""
    if raw_mode is not None:
        mode, normalized_from = _normalize_effective_mode(raw_mode, source="cli_flag")
        source = "cli_flag"
    if mode is None:
        raw_mode = coerce_mode(getattr(settings, "metrics_mode", ""))
        if raw_mode is not None:
            mode, normalized_from = _normalize_effective_mode(
                raw_mode, source="community_settings"
            )
            source = "community_settings"
    stale_persisted_consent = False
    legacy_persisted_local_only = False
    if mode is None:
        persisted_mode = coerce_mode(str(state.get("mode") or ""))
        if (
            persisted_mode == "anonymous_beacon"
            and str(state.get("schema_version") or "") != CURRENT_SCHEMA_VERSION
        ):
            stale_persisted_consent = True
        else:
            if persisted_mode is not None:
                mode, normalized_from = _normalize_effective_mode(
                    persisted_mode,
                    source="persisted_consent",
                )
                legacy_persisted_local_only = persisted_mode == "local_only"
                source = "persisted_consent"
    if mode is None:
        mode = DEFAULT_MODE
        source = "stale_persisted_consent" if stale_persisted_consent else "default"

    migration_notice = _migration_notice(state) if legacy_persisted_local_only else None

    return ResolvedTelemetryConfig(
        mode=mode,
        ui_mode=_ui_mode(mode),
        normalized_from=normalized_from,
        migration_notice=migration_notice,
        state_ref=state_ref,
        retention_days=int(getattr(settings, "metrics_retention_days", 30)),
        delivery_target=delivery_target_from_effect_config(settings),
        policy_version=str(getattr(settings, "metrics_policy_version", "2026-05-11")),
        schema_version=str(
            getattr(settings, "metrics_schema_version", CURRENT_SCHEMA_VERSION)
        ),
        source=source,
        resolved_precedence=precedence,
        state=state,
    )
