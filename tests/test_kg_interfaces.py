"""Tests for KG Protocol interfaces + ProviderRegistry (Onda 1 — test card 076fff06).

Scenarios:
  ts_00cde792 — Protocol duck typing accepts any conforming object
  ts_cbeb9966 — Registry defaults + override
  ts_d11439f6 — Env var mechanism (reserved, currently no-op)
"""

from __future__ import annotations

import os
import pytest

from okto_pulse.core.kg.interfaces.cache_backend import CacheBackend
from okto_pulse.core.kg.interfaces.embedding import EmbeddingProvider
from okto_pulse.core.kg.interfaces.kg_config import KGConfig
from okto_pulse.core.kg.interfaces.rate_limiter import RateLimiter
from okto_pulse.core.kg.interfaces.registry import (
    KGProviderRegistry,
    configure_kg_registry,
    get_kg_registry,
    reset_registry_for_tests,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_registry_for_tests()
    yield
    reset_registry_for_tests()


# -----------------------------------------------------------------------
# ts_00cde792 — Protocol duck typing
# -----------------------------------------------------------------------


class PlainConfig:
    """Implements KGConfig via duck typing — no Protocol inheritance."""

    kg_base_dir = "~/.test-pulse"
    kg_embedding_mode = "stub"
    kg_embedding_model = "test-model"
    kg_embedding_dim = 128
    kg_session_ttl_seconds = 300
    kg_cleanup_interval_seconds = 60
    kg_cleanup_enabled = False


class PlainCache:
    """Implements CacheBackend via duck typing."""

    def get(self, tool_name, board_id, params):
        return False, None

    def put(self, tool_name, board_id, params, value):
        pass

    def invalidate_board(self, board_id):
        return 0

    def stats(self):
        return {}


class PlainRateLimiter:
    """Implements RateLimiter via duck typing."""

    def allow(self, agent_id):
        return True, 0

    def reset(self, agent_id):
        pass


class PlainEmbedding:
    """Implements EmbeddingProvider via duck typing."""

    dim = 128

    def encode(self, text):
        return [0.0] * self.dim

    def encode_batch(self, texts):
        return [[0.0] * self.dim for _ in texts]


class Incomplete:
    """Missing required methods — should NOT satisfy any Protocol."""

    pass


class TestProtocolDuckTyping:
    """ts_00cde792 — AC-0 through AC-3."""

    def test_kg_config_accepts_plain_object(self):
        assert isinstance(PlainConfig(), KGConfig)

    def test_cache_backend_accepts_plain_object(self):
        assert isinstance(PlainCache(), CacheBackend)

    def test_rate_limiter_accepts_plain_object(self):
        assert isinstance(PlainRateLimiter(), RateLimiter)

    def test_embedding_provider_accepts_plain_object(self):
        assert isinstance(PlainEmbedding(), EmbeddingProvider)

    def test_incomplete_rejected_by_cache_backend(self):
        assert not isinstance(Incomplete(), CacheBackend)

    def test_incomplete_rejected_by_rate_limiter(self):
        assert not isinstance(Incomplete(), RateLimiter)

    def test_incomplete_rejected_by_embedding(self):
        assert not isinstance(Incomplete(), EmbeddingProvider)

    def test_embedded_implementations_satisfy_protocols(self):
        from okto_pulse.core.kg.providers.embedded.memory_cache import InMemoryCacheBackend
        from okto_pulse.core.kg.providers.embedded.memory_rate_limiter import InMemoryTokenBucket

        assert isinstance(InMemoryCacheBackend(), CacheBackend)
        assert isinstance(InMemoryTokenBucket(), RateLimiter)

    def test_stub_embedding_satisfies_protocol(self):
        from okto_pulse.core.kg.embedding import StubEmbeddingProvider

        assert isinstance(StubEmbeddingProvider(), EmbeddingProvider)


# -----------------------------------------------------------------------
# ts_cbeb9966 — Registry defaults + override
# -----------------------------------------------------------------------


class TestRegistryDefaults:
    """ts_cbeb9966 — AC-4, AC-5."""

    def test_lazy_init_returns_embedded_defaults(self):
        reg = get_kg_registry()
        assert reg.config is not None
        assert reg.cache_backend is not None
        assert reg.rate_limiter is not None
        assert reg.embedding_provider is not None

    def test_lazy_init_config_is_settings_based(self):
        from okto_pulse.core.kg.providers.embedded.settings_config import SettingsKGConfig

        reg = get_kg_registry()
        assert isinstance(reg.config, SettingsKGConfig)

    def test_lazy_init_cache_is_in_memory(self):
        from okto_pulse.core.kg.providers.embedded.memory_cache import InMemoryCacheBackend

        reg = get_kg_registry()
        assert isinstance(reg.cache_backend, InMemoryCacheBackend)

    def test_lazy_init_rate_limiter_is_token_bucket(self):
        from okto_pulse.core.kg.providers.embedded.memory_rate_limiter import InMemoryTokenBucket

        reg = get_kg_registry()
        assert isinstance(reg.rate_limiter, InMemoryTokenBucket)

    def test_singleton_identity(self):
        reg1 = get_kg_registry()
        reg2 = get_kg_registry()
        assert reg1 is reg2

    def test_configure_overrides_single_provider(self):
        mock_cache = PlainCache()
        configure_kg_registry(cache_backend=mock_cache)
        reg = get_kg_registry()
        assert reg.cache_backend is mock_cache
        assert reg.rate_limiter is not None
        assert reg.config is not None

    def test_configure_overrides_multiple_providers(self):
        mock_cache = PlainCache()
        mock_limiter = PlainRateLimiter()
        configure_kg_registry(cache_backend=mock_cache, rate_limiter=mock_limiter)
        reg = get_kg_registry()
        assert reg.cache_backend is mock_cache
        assert reg.rate_limiter is mock_limiter

    def test_reset_clears_singleton(self):
        reg1 = get_kg_registry()
        reset_registry_for_tests()
        reg2 = get_kg_registry()
        assert reg1 is not reg2


# -----------------------------------------------------------------------
# ts_d11439f6 — Env var mechanism (reserved for future use)
# -----------------------------------------------------------------------


class TestEnvVarMechanism:
    """ts_d11439f6 — AC-6.

    Currently env vars are reserved but not wired to load classes dynamically.
    Tests verify the mechanism exists and doesn't error.
    """

    def test_env_var_ignored_gracefully(self, monkeypatch):
        monkeypatch.setenv("KG_CACHE_BACKEND", "memory")
        reg = get_kg_registry()
        assert reg.cache_backend is not None

    def test_configure_works_with_env_vars_set(self, monkeypatch):
        monkeypatch.setenv("KG_CACHE_BACKEND", "redis")
        mock_cache = PlainCache()
        configure_kg_registry(cache_backend=mock_cache)
        reg = get_kg_registry()
        assert reg.cache_backend is mock_cache

    def test_unknown_env_var_does_not_crash(self, monkeypatch):
        monkeypatch.setenv("KG_UNKNOWN_PROVIDER", "something")
        reg = get_kg_registry()
        assert reg is not None
