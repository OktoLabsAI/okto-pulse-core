"""Telemetry mode resolution and persisted local consent state."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from okto_pulse.core.infra.config import CoreSettings, DEFAULT_METRICS_BEACON_URL
from okto_pulse.core.telemetry.schema import CURRENT_SCHEMA_VERSION

TelemetryMode = Literal["disabled", "local_only", "anonymous_beacon"]
VALID_MODES = {"disabled", "local_only", "anonymous_beacon"}
DEFAULT_MODE: TelemetryMode = "local_only"


@dataclass(frozen=True)
class ResolvedTelemetryConfig:
    mode: TelemetryMode
    metrics_dir: Path
    retention_days: int
    beacon_url: str
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


def metrics_dir_for(settings: CoreSettings) -> Path:
    raw = (getattr(settings, "metrics_dir", "") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    data_dir = getattr(settings, "data_dir", "") or ""
    if data_dir:
        return (Path(data_dir).expanduser() / "metrics").resolve()
    return (Path.home() / ".okto-pulse" / "metrics").resolve()


def state_path(metrics_dir: Path) -> Path:
    return metrics_dir / "state.json"


def load_state(metrics_dir: Path) -> dict[str, Any]:
    path = state_path(metrics_dir)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def save_state(metrics_dir: Path, state: dict[str, Any]) -> None:
    metrics_dir.mkdir(parents=True, exist_ok=True)
    tmp = state_path(metrics_dir).with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(state_path(metrics_dir))


def record_consent(
    settings: CoreSettings,
    *,
    mode: TelemetryMode,
    source: Literal["settings_ui", "cli"],
    policy_version: str | None = None,
    schema_version: str | None = None,
) -> dict[str, Any]:
    metrics_dir = metrics_dir_for(settings)
    current = load_state(metrics_dir)
    changed_at = iso_now()
    policy = policy_version or getattr(settings, "metrics_policy_version", "2026-05-11")
    schema = schema_version or getattr(settings, "metrics_schema_version", CURRENT_SCHEMA_VERSION)
    if mode == "anonymous_beacon" and schema != CURRENT_SCHEMA_VERSION:
        raise ValueError("UNSUPPORTED_METRICS_SCHEMA")
    next_prompt = None
    if mode != "anonymous_beacon":
        interval = int(getattr(settings, "metrics_opt_in_prompt_interval_days", 30))
        next_prompt = (utc_now() + timedelta(days=interval)).isoformat().replace("+00:00", "Z")
    history = list(current.get("history") or [])
    history.append(
        {
            "mode": mode,
            "source": source,
            "changed_at": changed_at,
            "policy_version": policy,
            "schema_version": schema,
        }
    )
    state = {
        **current,
        "mode": mode,
        "source": source,
        "changed_at": changed_at,
        "policy_version": policy,
        "schema_version": schema,
        "next_opt_in_prompt_after": next_prompt,
        "history": history[-50:],
    }
    save_state(metrics_dir, state)
    return state


def resolve_telemetry_config(
    settings: CoreSettings,
    *,
    cli_mode: str | None = None,
) -> ResolvedTelemetryConfig:
    metrics_dir = metrics_dir_for(settings)
    state = load_state(metrics_dir)
    precedence = (
        "cli_flag",
        "env",
        "community_settings",
        "persisted_consent",
        "default",
    )

    mode: TelemetryMode | None = coerce_mode(cli_mode)
    source = "cli_flag" if mode else ""
    if mode is None:
        mode = coerce_mode(os.environ.get("OKTO_PULSE_METRICS_MODE"))
        source = "env" if mode else ""
    if mode is None:
        mode = coerce_mode(getattr(settings, "metrics_mode", ""))
        source = "community_settings" if mode else ""
    stale_persisted_consent = False
    if mode is None:
        persisted_mode = coerce_mode(str(state.get("mode") or ""))
        if (
            persisted_mode == "anonymous_beacon"
            and str(state.get("schema_version") or "") != CURRENT_SCHEMA_VERSION
        ):
            stale_persisted_consent = True
        else:
            mode = persisted_mode
            source = "persisted_consent" if mode else ""
    if mode is None:
        mode = DEFAULT_MODE
        source = "stale_persisted_consent" if stale_persisted_consent else "default"

    return ResolvedTelemetryConfig(
        mode=mode,
        metrics_dir=metrics_dir,
        retention_days=int(getattr(settings, "metrics_retention_days", 30)),
        beacon_url=str(getattr(settings, "metrics_beacon_url", DEFAULT_METRICS_BEACON_URL)).rstrip("/"),
        policy_version=str(getattr(settings, "metrics_policy_version", "2026-05-11")),
        schema_version=str(getattr(settings, "metrics_schema_version", CURRENT_SCHEMA_VERSION)),
        source=source,
        resolved_precedence=precedence,
        state=state,
    )
