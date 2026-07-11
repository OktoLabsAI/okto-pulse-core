"""Publish-health external-source registry (spec R10-D, IMP3).

The composition root (the Community edition) registers a PROVIDER that supplies
the external publish-health source descriptors (``aws_ingest`` / ``report_athena``)
as :class:`~okto_pulse.core.ports.telemetry.PublishHealthSource` signals. The
core telemetry runtime (``TelemetryService.publish_health``) obtains the
descriptors ONLY through :func:`get_external_source_descriptors`; the PURE
``resolve_publish_health`` classifier then maps them to the health vocabulary.

CRITICAL INVARIANT (carried from R5C / restated by R10-D): AWS / report health is
NEVER inferred healthy from a local send. An absent / gap / stale / expired /
unavailable descriptor is classified as a STRUCTURED non-healthy state
(degraded / source_gap / unavailable) by the pure classifier. The
register-before-remove FALLBACK is the core ``discover_external_sources``, whose
default is an explicit observability GAP (``SRC_GAP`` -> degraded) — it can never
mask the missing AWS/report visibility as healthy.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value

logger = logging.getLogger("okto_pulse.telemetry.publish_health_source")

#: A provider: ``(settings) -> (aws_ingest_descriptor, report_athena_descriptor)``.
#: Each descriptor is a bounded dict (``{"availability": ..., "last_success_at": ...}``)
#: or an availability string — exactly what the pure classifier consumes.
ExternalSourceProvider = Callable[[Any], "tuple[Any, Any]"]

_RUNTIME_KEY = "telemetry.publish_health_source.provider"


def register_external_source_provider(provider: ExternalSourceProvider) -> None:
    """Register the edition's external publish-health source provider (composition
    root). Idempotent overwrite; thread-safe."""
    register_runtime_value(_RUNTIME_KEY, provider)


def get_external_source_descriptors(settings: Any) -> tuple[Any, Any]:
    """Resolve the ``(aws_ingest, report_athena)`` descriptors via the registered
    provider, or — register-before-remove — fall back to the core
    ``discover_external_sources`` default (an explicit GAP, never healthy).

    The COMPOSED runtime uses the Community provider's PublishHealthSource signals;
    the fallback (non-composed / retro-compat) is NOT silent — it emits a
    structured signal — and still yields a GAP, so the invariant holds either way.
    """
    provider = resolve_runtime_value(_RUNTIME_KEY)
    if provider is not None:
        return provider(settings)
    logger.error(
        "publish-health external source provider is not configured",
        extra={
            "metric_name": "telemetry_publish_health_source_fallback_total",
            "component": "publish_health_source_registry",
            "outcome": "fail_closed",
            "reason": "no_provider_registered",
        },
    )
    raise RuntimeError(
        "No publish-health ExternalSourceProvider is registered. The edition "
        "composition root must provide the external observability adapter."
    )


def reset_external_source_provider_for_tests() -> None:
    """Drop the registered provider (tests only)."""
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ExternalSourceProvider",
    "register_external_source_provider",
    "get_external_source_descriptors",
    "reset_external_source_provider_for_tests",
]
