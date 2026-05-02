"""KGProviderRegistry — central dependency injection container for the KG layer.

Usage:
    # At bootstrap (app.py or main.py):
    from okto_pulse.core.kg.interfaces import configure_kg_registry
    configure_kg_registry(cache_backend=RedisCacheBackend(url))

    # In consumers (kg_service.py, tier_power.py, etc.):
    from okto_pulse.core.kg.interfaces import get_kg_registry
    cache = get_kg_registry().cache_backend
    hit, val = cache.get(tool_name, board_id, params)
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from okto_pulse.core.kg.interfaces.audit_repository import AuditRepository
from okto_pulse.core.kg.interfaces.cache_backend import CacheBackend
from okto_pulse.core.kg.interfaces.cypher_executor import CypherExecutor
from okto_pulse.core.kg.interfaces.embedding import EmbeddingProvider
from okto_pulse.core.kg.interfaces.event_bus import EventBus
from okto_pulse.core.kg.interfaces.graph_store import SemanticGraphStore
from okto_pulse.core.kg.interfaces.kg_config import KGConfig
from okto_pulse.core.kg.interfaces.rate_limiter import RateLimiter
from okto_pulse.core.kg.interfaces.session_store import SessionStore


@dataclass
class KGProviderRegistry:
    """Central registry for all KG layer providers."""

    # Onda 1
    config: KGConfig | None = None
    cache_backend: CacheBackend | None = None
    rate_limiter: RateLimiter | None = None
    embedding_provider: EmbeddingProvider | None = None

    # Onda 2
    session_store: SessionStore | None = None
    audit_repo: AuditRepository | None = None
    auth_context_factory: Any | None = None

    # Onda 3
    graph_store: SemanticGraphStore | None = None
    cypher_executor: CypherExecutor | None = None
    event_bus: EventBus | None = None


_registry: KGProviderRegistry | None = None
_lock = threading.Lock()
_configured = False


def _build_defaults() -> KGProviderRegistry:
    """Build a registry with all embedded defaults.

    Populates Onda 1 (config, cache, rate_limiter, embedding), Onda 2
    (session_store), and Onda 3 (graph_store, cypher_executor, event_bus).
    audit_repo and auth_context_factory require a session_factory and are
    populated via configure_kg_registry() at bootstrap time.
    """
    from okto_pulse.core.kg.providers.embedded.settings_config import SettingsKGConfig
    from okto_pulse.core.kg.providers.embedded.memory_cache import InMemoryCacheBackend
    from okto_pulse.core.kg.providers.embedded.memory_rate_limiter import InMemoryTokenBucket
    from okto_pulse.core.kg.providers.embedded.memory_session_store import InMemorySessionStore
    from okto_pulse.core.kg.providers.embedded.kuzu_graph_store import KuzuGraphStore
    from okto_pulse.core.kg.providers.embedded.kuzu_cypher_executor import KuzuCypherExecutor
    from okto_pulse.core.kg.embedding import _build_provider_from_config

    config = SettingsKGConfig()
    return KGProviderRegistry(
        # Onda 1
        config=config,
        cache_backend=InMemoryCacheBackend(),
        rate_limiter=InMemoryTokenBucket(),
        embedding_provider=_build_provider_from_config(config),
        # Onda 2
        session_store=InMemorySessionStore(
            default_ttl_seconds=config.kg_session_ttl_seconds,
        ),
        # Onda 3
        graph_store=KuzuGraphStore(),
        cypher_executor=KuzuCypherExecutor(),
        # event_bus, audit_repo, auth_context_factory populated by configure_kg_registry()
    )


def configure_kg_registry(
    *,
    session_factory: Any | None = None,
    **overrides: Any,
) -> None:
    """Configure the singleton registry with optional provider overrides.

    Called once at bootstrap. Thread-safe.

    Args:
        session_factory: SQLAlchemy async session factory. When provided,
            auto-wires audit_repo (SqlAlchemyAuditRepository) and event_bus
            (SqliteOutboxEventBus) if not explicitly overridden.
        **overrides: Provider instances keyed by field name.
            Example: configure_kg_registry(cache_backend=RedisCacheBackend(url))
    """
    global _registry, _configured
    with _lock:
        reg = _build_defaults()

        # Auto-wire session_factory-dependent providers
        if session_factory is not None:
            if "audit_repo" not in overrides:
                from okto_pulse.core.kg.providers.embedded.sqlalchemy_audit_repo import (
                    SqlAlchemyAuditRepository,
                )
                reg.audit_repo = SqlAlchemyAuditRepository(session_factory)

            if "event_bus" not in overrides:
                from okto_pulse.core.kg.providers.embedded.sqlite_outbox_event_bus import (
                    SqliteOutboxEventBus,
                )
                reg.event_bus = SqliteOutboxEventBus(session_factory)

        for key, value in overrides.items():
            if hasattr(reg, key):
                setattr(reg, key, value)
        _registry = reg
        _configured = True


def get_kg_registry() -> KGProviderRegistry:
    """Return the singleton registry. Lazy-init with defaults if not configured."""
    global _registry, _configured
    if _registry is None:
        with _lock:
            if _registry is None:
                _registry = _build_defaults()
    return _registry


def reset_registry_for_tests() -> None:
    """Drop the cached registry — tests only."""
    global _registry, _configured
    _registry = None
    _configured = False
