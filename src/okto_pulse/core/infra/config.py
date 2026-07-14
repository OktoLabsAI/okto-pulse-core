"""Pure, pre-materialized application configuration for Core policies.

Environment and ``.env`` loading are edition responsibilities.  Core accepts a
validated settings snapshot through composition and never reads process state.
"""

from okto_pulse.core import __version__ as _CORE_PACKAGE_VERSION
from okto_pulse.core.ports.package_version import (
    ImportlibMetadataVersionProvider,
    PackageVersionProvider,
)
from pydantic import BaseModel, ConfigDict, Field

from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value


_VERSION_PROVIDER_KEY = "infra.config.version_provider"
_SETTINGS_KEY = "infra.config.settings"
_DEFAULT_VERSION_PROVIDER = ImportlibMetadataVersionProvider()


def register_package_version_provider(provider: PackageVersionProvider) -> None:
    """Register the runtime package version provider."""
    register_runtime_value(_VERSION_PROVIDER_KEY, provider)


def reset_package_version_provider_for_tests() -> None:
    """Restore the default metadata-backed version provider."""
    reset_runtime_values(_VERSION_PROVIDER_KEY)


def _resolve_version(package_name: str, fallback: str = "0.0.0+local") -> str:
    """Resolve version through provider/package metadata, never source files."""
    try:
        provider = resolve_runtime_value(_VERSION_PROVIDER_KEY) or _DEFAULT_VERSION_PROVIDER
        resolved = provider.version(package_name)
    except Exception:
        resolved = None
    return resolved or fallback


def _default_core_version() -> str:
    return _resolve_version("okto-pulse-core", fallback=_CORE_PACKAGE_VERSION)


class CoreSettings(BaseModel):
    """Validated settings snapshot supplied by an edition composition root."""

    model_config = ConfigDict(
        extra="allow",
    )

    # Application — single source of truth via importlib.metadata
    # so /health, FastAPI title and MCP server-info stay aligned with
    # the installed wheel without manual sync (NC-2 fix).
    app_name: str = "Okto Pulse"
    app_version: str = Field(default_factory=_default_core_version)
    # Telemetry policy. Delivery endpoints and local paths are edition-owned.
    metrics_mode: str = ""
    metrics_retention_days: int = Field(30, ge=1, le=400)
    metrics_policy_version: str = "2026-05-11"
    metrics_schema_version: str = "1.1.0"
    metrics_opt_in_prompt_interval_days: int = Field(30, ge=1, le=365)
    metrics_token_refresh_margin_hours: int = Field(24, ge=0, le=168)

    # Knowledge-graph application policy. Physical storage and providers are
    # supplied through KG ports by the edition.
    kg_session_ttl_seconds: int = 3600
    kg_cleanup_interval_seconds: int = 60
    kg_cleanup_enabled: bool = True
    # DEPRECATED — use kg_queue_alert_threshold. Kept for backward-compat
    # until v0.5.0; the settings_service maps the legacy value into
    # kg_queue_alert_threshold and emits a DeprecationWarning at startup.
    kg_max_queue_depth: int = Field(200, ge=10, le=10000)

    # Consolidation queue runtime tuning (spec bdcda842, v0.2.0) — all
    # hot-reload (worker pool re-reads on every claim with 5s debounce).
    # Mudanças aqui NÃO marcam restart_required.
    kg_queue_max_concurrent_workers: int = Field(4, ge=1, le=16)
    kg_queue_min_interval_ms: int = Field(100, ge=0, le=1000)
    kg_queue_claim_timeout_s: int = Field(300, ge=60, le=3600)
    kg_queue_max_attempts: int = Field(5, ge=1, le=10)
    kg_queue_alert_threshold: int = Field(5000, ge=100, le=100000)
    # R6-IMP2: advisory age (seconds) above which an ACTIVE queue item (oldest
    # pending/claimed) is classified ``stuck`` in the queue drill-down. Advisory
    # only — does not change the queue alert/backpressure threshold above.
    kg_queue_stuck_age_seconds: int = Field(300, ge=1, le=86400)
    # Recovery scan periodicity (TR6); operators can lower for tests but
    # production values below 30s start to compete with normal traffic.
    kg_queue_recovery_scan_interval_s: int = Field(60, ge=10, le=600)
    # S1.3 Cognitive Closure rollout — the FIRST blocking activation of the
    # CognitiveReadinessService done-transition enforcement sits behind this
    # feature flag, default-OFF so existing boards stay advisory until skip
    # ledger-only + no-mask-DLQ are proven green (fr_9d42c5e2 / dec_41db6a36).
    # Even with the per-board policy set to "blocking", enforcement activates
    # ONLY when this global flag is True.
    cognitive_readiness_blocking_enabled: bool = Field(False)

    # Spec R2c (FR5/TR5/TR6/TR7) — DLQ auto-drain opt-in defaults.
    # The feature is disabled by default (board-level flag controls opt-in).
    # kg_queue_dlq_auto_drain_backoff_s: minimum seconds between auto-drain
    #   runs for the same board (in-process per-board cooldown dict).
    # kg_queue_dlq_auto_drain_max_requeue_attempts: DLQ rows that have been
    #   requeued this many times without success are considered poison pills
    #   and are permanently deleted with a WARN log.
    kg_queue_dlq_auto_drain_backoff_s: int = Field(300, ge=30, le=86400)
    kg_queue_dlq_auto_drain_max_requeue_attempts: int = Field(3, ge=1, le=20)

    # Spec 54399628 (NC-Wave2 — KG decay tick controllability) — 3 settings
    # persistidos com hot-reload via SchedulerControl.reschedule_job. Defaults
    # preservam comportamento atual (cron 24h staleness 7d, no max-age cap).
    # Ranges: 5min-7d para interval (impede DoS auto-infligido + impede
    # esquecer); 1-365d para staleness; 0=no-cap, >0 força recompute em
    # nodes "frescos" mais velhos que N dias.
    kg_decay_tick_interval_minutes: int = Field(1440, ge=5, le=10080)
    kg_decay_tick_staleness_days: int = Field(7, ge=1, le=365)
    kg_decay_tick_max_age_days: int = Field(0, ge=0, le=365)

def configure_settings(s: "CoreSettings") -> None:
    """Register a pre-built CoreSettings instance."""
    register_runtime_value(_SETTINGS_KEY, s)


def reset_settings_for_tests() -> None:
    """Remove the composed settings snapshot for isolated tests."""

    reset_runtime_values(_SETTINGS_KEY)


def get_settings() -> "CoreSettings":
    """Get the composed settings snapshot, failing closed when absent."""
    from okto_pulse.core.composition import (
        current_runtime_composition,
    )

    composition = current_runtime_composition()
    if composition is not None and composition.settings_provider is not None:
        return composition.settings_provider
    settings = resolve_runtime_value(_SETTINGS_KEY)
    if settings is None:
        raise RuntimeError(
            "Core settings are not configured. The edition composition root must "
            "call configure_settings() before application work begins."
        )
    return settings
