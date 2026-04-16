"""Tests for KG registry integration + backward compat (Onda 1 — test card a89b1a68).

Scenarios:
  ts_d35c07ed — Cache backend integration via registry
  ts_77d9f363 — Rate limiter integration via registry
  ts_7f474a39 — Backward compat + import identity
"""

from __future__ import annotations

import time
import pytest

from okto_pulse.core.kg.interfaces.registry import (
    get_kg_registry,
    reset_registry_for_tests,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_registry_for_tests()
    yield
    reset_registry_for_tests()


# -----------------------------------------------------------------------
# ts_d35c07ed — Cache backend integration
# -----------------------------------------------------------------------


class TestCacheBackendIntegration:
    """ts_d35c07ed — AC-7: kg_service._exec uses registry.cache_backend."""

    def test_cache_miss_then_hit(self):
        cache = get_kg_registry().cache_backend
        hit, val = cache.get("test_tool", "board-1", {"q": "hello"})
        assert hit is False
        assert val is None

        cache.put("test_tool", "board-1", {"q": "hello"}, [["row1"]])
        hit, val = cache.get("test_tool", "board-1", {"q": "hello"})
        assert hit is True
        assert val == [["row1"]]

    def test_different_params_are_separate_keys(self):
        cache = get_kg_registry().cache_backend
        cache.put("tool", "b1", {"q": "a"}, "val_a")
        cache.put("tool", "b1", {"q": "b"}, "val_b")

        _, val_a = cache.get("tool", "b1", {"q": "a"})
        _, val_b = cache.get("tool", "b1", {"q": "b"})
        assert val_a == "val_a"
        assert val_b == "val_b"

    def test_invalidate_board_clears_entries(self):
        cache = get_kg_registry().cache_backend
        cache.put("tool", "board-x", {"a": 1}, "v1")
        cache.put("tool", "board-x", {"a": 2}, "v2")
        cache.put("tool", "board-y", {"a": 1}, "v3")

        evicted = cache.invalidate_board("board-x")
        assert evicted == 2

        hit_x, _ = cache.get("tool", "board-x", {"a": 1})
        assert hit_x is False

        hit_y, val_y = cache.get("tool", "board-y", {"a": 1})
        assert hit_y is True
        assert val_y == "v3"

    def test_stats_returns_dict(self):
        cache = get_kg_registry().cache_backend
        cache.put("tool", "b1", {}, "v")
        s = cache.stats()
        assert s["size"] >= 1
        assert "max_size" in s
        assert "ttl_seconds" in s

    def test_ttl_expiry(self):
        from okto_pulse.core.kg.providers.embedded.memory_cache import InMemoryCacheBackend

        cache = InMemoryCacheBackend(ttl_seconds=0.05)
        cache.put("tool", "b1", {"k": 1}, "val")
        hit, _ = cache.get("tool", "b1", {"k": 1})
        assert hit is True

        time.sleep(0.1)
        hit, _ = cache.get("tool", "b1", {"k": 1})
        assert hit is False

    def test_lru_eviction_at_max_size(self):
        from okto_pulse.core.kg.providers.embedded.memory_cache import InMemoryCacheBackend

        cache = InMemoryCacheBackend(max_size=3)
        for i in range(4):
            cache.put("tool", "b1", {"i": i}, f"val_{i}")

        assert cache.stats()["size"] == 3
        hit, _ = cache.get("tool", "b1", {"i": 0})
        assert hit is False


# -----------------------------------------------------------------------
# ts_77d9f363 — Rate limiter integration
# -----------------------------------------------------------------------


class TestRateLimiterIntegration:
    """ts_77d9f363 — AC-8: tier_power rate limit uses registry.rate_limiter."""

    def test_allows_within_limit(self):
        limiter = get_kg_registry().rate_limiter
        for _ in range(30):
            allowed, retry = limiter.allow("agent-1")
            assert allowed is True
            assert retry == 0

    def test_rejects_at_limit(self):
        limiter = get_kg_registry().rate_limiter
        for _ in range(30):
            limiter.allow("agent-2")

        allowed, retry = limiter.allow("agent-2")
        assert allowed is False
        assert retry > 0

    def test_reset_restores_capacity(self):
        limiter = get_kg_registry().rate_limiter
        for _ in range(30):
            limiter.allow("agent-3")

        allowed, _ = limiter.allow("agent-3")
        assert allowed is False

        limiter.reset("agent-3")
        allowed, retry = limiter.allow("agent-3")
        assert allowed is True
        assert retry == 0

    def test_separate_agents_independent(self):
        limiter = get_kg_registry().rate_limiter
        for _ in range(30):
            limiter.allow("agent-a")

        allowed, _ = limiter.allow("agent-b")
        assert allowed is True

    def test_check_rate_limit_raises_on_exceed(self):
        from okto_pulse.core.kg.tier_power import TierPowerError, check_rate_limit

        limiter = get_kg_registry().rate_limiter
        for _ in range(30):
            limiter.allow("agent-x")

        with pytest.raises(TierPowerError) as exc_info:
            check_rate_limit("agent-x")
        assert exc_info.value.code == "rate_limited"


# -----------------------------------------------------------------------
# ts_7f474a39 — Backward compat + import identity
# -----------------------------------------------------------------------


class TestBackwardCompat:
    """ts_7f474a39 — AC-9, AC-10, AC-11."""

    def test_cache_get_import_works(self):
        from okto_pulse.core.kg.cache import cache_get

        hit, val = cache_get("tool", "board", {})
        assert hit is False

    def test_cache_put_import_works(self):
        from okto_pulse.core.kg.cache import cache_put

        cache_put("tool", "board", {"k": 1}, "value")
        cache = get_kg_registry().cache_backend
        hit, val = cache.get("tool", "board", {"k": 1})
        assert hit is True
        assert val == "value"

    def test_invalidate_board_import_works(self):
        from okto_pulse.core.kg.cache import invalidate_board

        cache = get_kg_registry().cache_backend
        cache.put("tool", "b1", {}, "v")
        count = invalidate_board("b1")
        assert count == 1

    def test_cache_stats_import_works(self):
        from okto_pulse.core.kg.cache import cache_stats

        s = cache_stats()
        assert "size" in s

    def test_clear_cache_import_works(self):
        from okto_pulse.core.kg.cache import clear_cache

        cache = get_kg_registry().cache_backend
        cache.put("tool", "b1", {}, "v")
        clear_cache()
        hit, _ = cache.get("tool", "b1", {})
        assert hit is False

    def test_emit_tool_metrics_import_works(self):
        from okto_pulse.core.kg.cache import emit_tool_metrics

        emit_tool_metrics(
            tool_name="test", board_id="b", cache_hit=False,
            duration_ms=1.0, result_count=0,
        )

    def test_get_embedding_provider_import_works(self):
        from okto_pulse.core.kg.embedding import get_embedding_provider

        provider = get_embedding_provider()
        assert provider is not None
        assert provider.dim > 0

    def test_embedding_provider_identity(self):
        from okto_pulse.core.kg.embedding import get_embedding_provider

        provider = get_embedding_provider()
        registry_provider = get_kg_registry().embedding_provider
        assert provider is registry_provider

    def test_cache_functions_use_same_backend(self):
        from okto_pulse.core.kg.cache import cache_put, cache_get

        cache_put("tool", "board-z", {"x": 1}, "from_compat")
        hit, val = get_kg_registry().cache_backend.get("tool", "board-z", {"x": 1})
        assert hit is True
        assert val == "from_compat"

        get_kg_registry().cache_backend.put("tool", "board-z", {"x": 2}, "from_registry")
        hit, val = cache_get("tool", "board-z", {"x": 2})
        assert hit is True
        assert val == "from_registry"

    def test_reset_embedding_provider_cache_works(self):
        from okto_pulse.core.kg.embedding import reset_embedding_provider_cache

        p1 = get_kg_registry().embedding_provider
        reset_embedding_provider_cache()
        p2 = get_kg_registry().embedding_provider
        assert p1 is not p2

    def test_kg_init_re_exports(self):
        from okto_pulse.core.kg import (
            EmbeddingProvider,
            get_embedding_provider,
            NODE_TYPES,
            REL_TYPES,
            SCHEMA_VERSION,
        )

        assert len(NODE_TYPES) == 11
        assert len(REL_TYPES) == 10
        assert SCHEMA_VERSION == "0.1.0"
        assert get_embedding_provider() is not None
