"""Telemetry effect configuration provider registry.

Core owns telemetry vocabulary, schema and policy. Editions own concrete
runtime defaults for effectful telemetry behavior. This registry exposes only
opaque references; interpretation belongs to the edition adapter.
"""

from __future__ import annotations

import logging
from typing import Any

from okto_pulse.core.ports.telemetry import TelemetryEffectConfigProvider
from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value

logger = logging.getLogger("okto_pulse.telemetry.effect_config")

_RUNTIME_KEY = "telemetry.effect_config.provider"


def register_telemetry_effect_config_provider(
    provider: TelemetryEffectConfigProvider,
) -> None:
    """Register the edition-owned telemetry effect config provider."""
    register_runtime_value(_RUNTIME_KEY, provider)


def reset_telemetry_effect_config_provider_for_tests() -> None:
    """Drop the registered provider (tests only)."""
    reset_runtime_values(_RUNTIME_KEY)


def state_ref_from_effect_config(settings: Any) -> str:
    """Resolve an opaque state reference without a Core fallback.

    An explicit edition value is treated as caller-provided configuration. If
    absent, an edition provider must supply the reference.
    """
    raw = (getattr(settings, "telemetry_state_ref", "") or "").strip()
    if raw:
        return raw
    provider = resolve_runtime_value(_RUNTIME_KEY)
    if provider is not None:
        value = provider.state_ref(settings)
        if value:
            return str(value)
    logger.error(
        "telemetry state reference has no explicit value and no effect config provider",
        extra={
            "metric_name": "telemetry_effect_config_no_state_ref_total",
            "component": "telemetry_effect_config",
            "outcome": "fail_closed",
            "reason": "no_state_ref_provider",
        },
    )
    raise RuntimeError(
        "No telemetry state reference configured. Provide settings.telemetry_state_ref or "
        "register a TelemetryEffectConfigProvider."
    )


def delivery_target_from_effect_config(settings: Any) -> str:
    """Resolve an opaque delivery target without a concrete Core default.

    Empty string is a valid non-sending value; composed editions may provide a
    concrete target either on settings or through the provider.
    """
    raw = (getattr(settings, "telemetry_delivery_target", "") or "").strip()
    if raw:
        return raw
    provider = resolve_runtime_value(_RUNTIME_KEY)
    if provider is not None:
        value = (provider.delivery_target(settings) or "").strip()
        if value:
            return value
    return ""


__all__ = [
    "register_telemetry_effect_config_provider",
    "reset_telemetry_effect_config_provider_for_tests",
    "state_ref_from_effect_config",
    "delivery_target_from_effect_config",
]
