"""Core application configuration using pydantic-settings."""

from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version as _pkg_version
from pathlib import Path
import tomllib

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _resolve_version(package_name: str, fallback: str = "0.0.0+local") -> str:
    """Read version from installed package metadata; fallback if not installed
    (e.g. running from source tree without ``pip install -e``)."""
    pyproject = Path(__file__).resolve().parents[4] / "pyproject.toml"
    if pyproject.exists():
        try:
            return tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]["version"]
        except Exception:
            pass
    try:
        return _pkg_version(package_name)
    except PackageNotFoundError:
        return fallback


_CORE_VERSION = _resolve_version("okto-pulse-core", fallback="0.2.6+local")
GRAPH_DB_MAX_SIZE_GB_VALUES: tuple[int, ...] = (2, 4, 8, 16, 32, 64)
DEFAULT_METRICS_BEACON_URL = "https://metrics.oktolabs.ai"


def validate_graph_db_max_size_gb(value: int) -> int:
    """Ensure Ladybug receives a max_db_size that is a power of two in bytes."""
    if value not in GRAPH_DB_MAX_SIZE_GB_VALUES:
        allowed = ", ".join(str(v) for v in GRAPH_DB_MAX_SIZE_GB_VALUES)
        raise ValueError(
            "kg_kuzu_max_db_size_gb must be one of "
            f"{allowed} GB. Ladybug requires max_db_size to be a power of 2 "
            "in bytes; even values such as 6 GB are still invalid."
        )
    return value


class CoreSettings(BaseSettings):
    """Core application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application — single source of truth via importlib.metadata
    # so /health, FastAPI title and MCP server-info stay aligned with
    # the installed wheel without manual sync (NC-2 fix).
    app_name: str = "Okto Pulse"
    app_version: str = _CORE_VERSION
    debug: bool = False
    environment: str = "development"

    # Server
    host: str = "127.0.0.1"
    port: int = 8100

    # Database
    database_url: str = "sqlite+aiosqlite:///./dashboard.db"

    # Storage
    upload_dir: str = "./uploads"
    max_upload_size: int = 10 * 1024 * 1024  # 10MB

    # Local-first telemetry (v0.2.1). Empty values mean "not explicitly set"
    # so the resolver can still let persisted consent win over defaults.
    metrics_mode: str = ""
    metrics_dir: str = ""
    metrics_retention_days: int = Field(30, ge=1, le=400)
    metrics_beacon_url: str = DEFAULT_METRICS_BEACON_URL
    metrics_policy_version: str = "2026-05-11"
    metrics_schema_version: str = "1.1.0"
    metrics_opt_in_prompt_interval_days: int = Field(30, ge=1, le=365)
    metrics_token_refresh_margin_hours: int = Field(24, ge=0, le=168)

    # MCP Server
    mcp_server_name: str = "okto-pulse"
    mcp_server_version: str = _CORE_VERSION
    mcp_port: int = 8101

    # CORS
    cors_origins: str = "http://localhost:5173,http://localhost:3000"

    # Knowledge Graph (MVP Fase 0)
    kg_base_dir: str = "~/.okto-pulse"
    kg_embedding_mode: str = "sentence-transformers"  # "stub" | "sentence-transformers"
    kg_embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    kg_embedding_dim: int = 384
    kg_session_ttl_seconds: int = 3600
    kg_cleanup_interval_seconds: int = 60
    kg_cleanup_enabled: bool = True
    # DEPRECATED — use kg_queue_alert_threshold. Kept for backward-compat
    # until v0.5.0; the settings_service maps the legacy value into
    # kg_queue_alert_threshold and emits a DeprecationWarning at startup.
    kg_max_queue_depth: int = Field(200, ge=10, le=10000)

    # Kùzu runtime tuning (0.1.4) — defaults avoid Ladybug 0.16 HNSW buffer
    # exhaustion while keeping max_db_size in the supported power-of-two set.
    # Kùzu's own defaults (buffer_pool_size=0 → ~80% system RAM, max_db_size=1<<43=8TB VA)
    # caused 128GB RSS with 3 instances in field reports.
    kg_kuzu_buffer_pool_mb: int = Field(512, ge=128, le=512)
    kg_kuzu_max_db_size_gb: int = Field(2, ge=2, le=64)
    kg_connection_pool_size: int = Field(8, ge=1, le=32)

    @field_validator("kg_kuzu_max_db_size_gb")
    @classmethod
    def _validate_graph_db_max_size_gb(cls, value: int) -> int:
        return validate_graph_db_max_size_gb(value)

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
    # persistidos com hot-reload via APScheduler.reschedule_job. Defaults
    # preservam comportamento atual (cron 24h staleness 7d, no max-age cap).
    # Ranges: 5min-7d para interval (impede DoS auto-infligido + impede
    # esquecer); 1-365d para staleness; 0=no-cap, >0 força recompute em
    # nodes "frescos" mais velhos que N dias.
    kg_decay_tick_interval_minutes: int = Field(1440, ge=5, le=10080)
    kg_decay_tick_staleness_days: int = Field(7, ge=1, le=365)
    kg_decay_tick_max_age_days: int = Field(0, ge=0, le=365)

    @property
    def cors_origins_list(self) -> list[str]:
        """Parse CORS origins from comma-separated string."""
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


class MCPSettings(BaseSettings):
    """MCP-specific settings for agent authentication."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_prefix="MCP_",
    )

    # MCP authentication
    require_agent_key: bool = True
    agent_keys_env: str = ""  # Comma-separated agent keys for validation


_settings_instance: "CoreSettings | None" = None


def configure_settings(s: "CoreSettings") -> None:
    """Register a pre-built CoreSettings instance."""
    global _settings_instance
    _settings_instance = s


def get_settings() -> "CoreSettings":
    """Get the active CoreSettings (lazy-creates a default if none registered)."""
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = CoreSettings()
    return _settings_instance


@lru_cache
def get_mcp_settings() -> MCPSettings:
    """Get cached MCP settings instance."""
    return MCPSettings()
