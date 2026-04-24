"""Core application configuration using pydantic-settings."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CoreSettings(BaseSettings):
    """Core application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_name: str = "Okto Pulse"
    app_version: str = "0.1.4"
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
    mcp_server_version: str = "0.1.4"
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
    kg_max_queue_depth: int = Field(200, ge=10, le=10000)

    # Kùzu runtime tuning (0.1.4) — defaults target ≤1.5GB total RAM with 8 pooled boards.
    # Kùzu's own defaults (buffer_pool_size=0 → ~80% system RAM, max_db_size=1<<43=8TB VA)
    # caused 128GB RSS with 3 instances in field reports.
    kg_kuzu_buffer_pool_mb: int = Field(256, ge=16, le=512)
    kg_kuzu_max_db_size_gb: int = Field(1, ge=1, le=64)
    kg_connection_pool_size: int = Field(8, ge=1, le=32)

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
