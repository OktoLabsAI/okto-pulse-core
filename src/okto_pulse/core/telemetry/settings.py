"""Telemetry mode, consent and privacy policy over edition-neutral state refs."""

from __future__ import annotations

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

from okto_pulse.core.domain.telemetry_modes import TelemetryMode, VALID_TELEMETRY_MODES, validate_telemetry_state
EffectiveTelemetryMode = Literal["disabled", "anonymous_beacon"]
VALID_MODES = VALID_TELEMETRY_MODES
DEFAULT_MODE: EffectiveTelemetryMode = "disabled"



@dataclass(frozen=True)
class ResolvedTelemetryConfig:
    mode: EffectiveTelemetryMode
    ui_mode: Literal["off", "on"]
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
    normalized = value or ""
    if not normalized:
        return None
    if normalized in VALID_MODES:
        return normalized  # type: ignore[return-value]
    raise ValueError(f"invalid telemetry mode: {value}")


def _ui_mode(mode: EffectiveTelemetryMode) -> Literal["off", "on"]:
    return "on" if mode == "anonymous_beacon" else "off"


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
    if mode not in VALID_MODES:
        raise ValueError("invalid telemetry mode")
    state_ref = state_ref_for(settings)
    current = load_state(state_ref)
    validate_telemetry_state(current)
    changed_at = iso_now()
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
    save_state(state_ref, state)
    return state


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
    validate_telemetry_state(state)
    precedence = (
        "cli_flag",
        "env",
        "community_settings",
        "persisted_consent",
        "default",
    )

    mode = coerce_mode(cli_mode)
    source = "cli_flag" if mode is not None else ""
    if mode is None:
        mode = coerce_mode(getattr(settings, "metrics_mode", ""))
        if mode is not None:
            source = "community_settings"
    stale_persisted_consent = False
    if mode is None:
        persisted_mode = coerce_mode(state.get("mode"))
        if persisted_mode == "anonymous_beacon" and state.get("schema_version") != CURRENT_SCHEMA_VERSION:
            stale_persisted_consent = True
        else:
            mode = persisted_mode
            if mode is not None:
                source = "persisted_consent"
    if mode is None:
        mode = DEFAULT_MODE
        source = "stale_persisted_consent" if stale_persisted_consent else "default"

    return ResolvedTelemetryConfig(
        mode=mode,
        ui_mode=_ui_mode(mode),
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
