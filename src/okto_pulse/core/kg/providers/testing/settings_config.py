"""Static ``KGConfig`` fake for explicit test compositions."""

from __future__ import annotations

from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class SettingsKGConfig:
    """Deterministic contract fake with no edition or process configuration."""

    kg_base_dir: str = "memory://okto-pulse-tests"
    kg_embedding_mode: str = "stub"
    kg_embedding_model: str = "stub"
    kg_embedding_dim: int = 384
    kg_session_ttl_seconds: int = 3600
    kg_cleanup_interval_seconds: int = 60
    kg_cleanup_enabled: bool = True


__all__ = ["SettingsKGConfig"]
