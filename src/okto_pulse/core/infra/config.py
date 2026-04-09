"""Core application configuration using pydantic-settings."""

from functools import lru_cache

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
    app_version: str = "0.1.0"
    debug: bool = False
    environment: str = "development"

    # Server
    host: str = "0.0.0.0"
    port: int = 8100

    # Database
    database_url: str = "sqlite+aiosqlite:///./dashboard.db"

    # Storage
    upload_dir: str = "./uploads"
    max_upload_size: int = 10 * 1024 * 1024  # 10MB

    # MCP Server
    mcp_server_name: str = "okto-pulse"
    mcp_server_version: str = "0.1.0"
    mcp_port: int = 8101

    # CORS
    cors_origins: str = "http://localhost:5173,http://localhost:3000"

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
