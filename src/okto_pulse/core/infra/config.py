"""Core application configuration using pydantic-settings."""

from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version as _pkg_version

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _resolve_version(package_name: str, fallback: str = "0.0.0+local") -> str:
    """Read version from installed package metadata; fallback if not installed
    (e.g. running from source tree without ``pip install -e``)."""
    try:
        return _pkg_version(package_name)
    except PackageNotFoundError:
        return fallback


_CORE_VERSION = _resolve_version("okto-pulse-core", fallback="0.1.14+local")


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

    # Kùzu runtime tuning (0.1.4) — defaults target ≤1.5GB total RAM with 8 pooled boards.
    # Kùzu's own defaults (buffer_pool_size=0 → ~80% system RAM, max_db_size=1<<43=8TB VA)
    # caused 128GB RSS with 3 instances in field reports.
    kg_kuzu_buffer_pool_mb: int = Field(256, ge=16, le=512)
    kg_kuzu_max_db_size_gb: int = Field(1, ge=1, le=64)
    kg_connection_pool_size: int = Field(8, ge=1, le=32)

    # Consolidation queue runtime tuning (spec bdcda842, v0.1.14) — all
    # hot-reload (worker pool re-reads on every claim with 5s debounce).
    # Mudanças aqui NÃO marcam restart_required.
    kg_queue_max_concurrent_workers: int = Field(4, ge=1, le=16)
    kg_queue_min_interval_ms: int = Field(100, ge=0, le=1000)
    kg_queue_claim_timeout_s: int = Field(300, ge=60, le=3600)
    kg_queue_max_attempts: int = Field(5, ge=1, le=10)
    kg_queue_alert_threshold: int = Field(5000, ge=100, le=100000)
    # Recovery scan periodicity (TR6); operators can lower for tests but
    # production values below 30s start to compete with normal traffic.
    kg_queue_recovery_scan_interval_s: int = Field(60, ge=10, le=600)

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
