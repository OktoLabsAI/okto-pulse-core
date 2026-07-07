"""Telemetry effect configuration provider registry.

Core owns telemetry vocabulary, schema and policy. Editions own concrete
runtime defaults for effectful telemetry behavior: local metrics directories and
beacon endpoints. This registry is the composition seam for those defaults.
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any

from okto_pulse.core.ports.telemetry import TelemetryEffectConfigProvider

logger = logging.getLogger("okto_pulse.telemetry.effect_config")

_provider: TelemetryEffectConfigProvider | None = None
_lock = threading.Lock()


def register_telemetry_effect_config_provider(
    provider: TelemetryEffectConfigProvider,
) -> None:
    """Register the edition-owned telemetry effect config provider."""
    global _provider
    with _lock:
        _provider = provider


def reset_telemetry_effect_config_provider_for_tests() -> None:
    """Drop the registered provider (tests only)."""
    global _provider
    with _lock:
        _provider = None


def metrics_dir_from_effect_config(settings: Any) -> Path:
    """Resolve the metrics directory without any core local fallback.

    An explicit ``settings.metrics_dir`` is treated as caller-provided
    configuration. If absent, an edition provider must supply the value.
    """
    raw = (getattr(settings, "metrics_dir", "") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    if _provider is not None:
        value = _provider.metrics_dir(settings)
        if value:
            return Path(value).expanduser().resolve()
    logger.error(
        "telemetry metrics_dir has no explicit value and no effect config provider",
        extra={
            "metric_name": "telemetry_effect_config_no_metrics_dir_total",
            "component": "telemetry_effect_config",
            "outcome": "fail_closed",
            "reason": "no_metrics_dir_provider",
        },
    )
    raise RuntimeError(
        "No telemetry metrics_dir configured. Provide settings.metrics_dir or "
        "register a TelemetryEffectConfigProvider."
    )


def beacon_url_from_effect_config(settings: Any) -> str:
    """Resolve the beacon URL without a concrete core default.

    Empty string is a valid non-sending core value; composed editions may provide
    a concrete URL either on settings or through the provider.
    """
    raw = (getattr(settings, "metrics_beacon_url", "") or "").strip()
    if raw:
        return raw.rstrip("/")
    if _provider is not None:
        value = (_provider.beacon_url(settings) or "").strip()
        if value:
            return value.rstrip("/")
    return ""


__all__ = [
    "register_telemetry_effect_config_provider",
    "reset_telemetry_effect_config_provider_for_tests",
    "metrics_dir_from_effect_config",
    "beacon_url_from_effect_config",
]
