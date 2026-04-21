"""SettingsKGConfig — wraps CoreSettings as KGConfig Protocol implementation."""

from __future__ import annotations


class SettingsKGConfig:
    """Default KGConfig that delegates to CoreSettings. Satisfies KGConfig Protocol."""

    def __init__(self):
        from okto_pulse.core.infra.config import get_settings
        self._settings = get_settings()

    @property
    def kg_base_dir(self) -> str:
        return self._settings.kg_base_dir

    @property
    def kg_embedding_mode(self) -> str:
        return self._settings.kg_embedding_mode

    @property
    def kg_embedding_model(self) -> str:
        return self._settings.kg_embedding_model

    @property
    def kg_embedding_dim(self) -> int:
        return self._settings.kg_embedding_dim

    @property
    def kg_session_ttl_seconds(self) -> int:
        return self._settings.kg_session_ttl_seconds

    @property
    def kg_cleanup_interval_seconds(self) -> int:
        return self._settings.kg_cleanup_interval_seconds

    @property
    def kg_cleanup_enabled(self) -> bool:
        return self._settings.kg_cleanup_enabled
