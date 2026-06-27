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
from okto_pulse.core.kg.interfaces.graph_lifecycle import GraphLifecycle
from okto_pulse.core.kg.interfaces.graph_path_resolver import GraphPathResolver
from okto_pulse.core.kg.interfaces.graph_schema_manager import GraphSchemaManager
from okto_pulse.core.kg.interfaces.graph_store import SemanticGraphStore
from okto_pulse.core.kg.interfaces.graph_transaction import GraphTransaction
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

    # Onda 4 — KG storage ports (spec #06): close kg.schema as a port before
    # Kùzu/Ladybug can move out of core.
    graph_transaction: GraphTransaction | None = None
    graph_schema_manager: GraphSchemaManager | None = None
    graph_lifecycle: GraphLifecycle | None = None
    graph_path_resolver: GraphPathResolver | None = None


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
    from okto_pulse.core.kg.providers.embedded.kuzu_graph_lifecycle import KuzuGraphLifecycle
    from okto_pulse.core.kg.providers.embedded.kuzu_graph_path_resolver import (
        KuzuGraphPathResolver,
    )
    from okto_pulse.core.kg.providers.embedded.kuzu_graph_schema_manager import (
        KuzuGraphSchemaManager,
    )
    from okto_pulse.core.kg.providers.embedded.kuzu_graph_transaction import (
        KuzuGraphTransaction,
    )
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
        # Onda 4 — KG storage ports (embedded Kùzu adapters)
        graph_transaction=KuzuGraphTransaction(),
        graph_schema_manager=KuzuGraphSchemaManager(),
        graph_lifecycle=KuzuGraphLifecycle(),
        graph_path_resolver=KuzuGraphPathResolver(),
        # event_bus, audit_repo, auth_context_factory populated by configure_kg_registry()
    )


def _build_graph_defaults() -> dict[str, Any]:
    """Build ONLY the core-owned graph providers: the embedded Kùzu/graph adapters
    (graph_store / cypher_executor / transaction / schema_manager / lifecycle /
    path_resolver).

    These are the providers spec #06 closed but R05 does NOT move (deferred). The
    R05-B base-registry path uses this to mount the core graph slots WITHOUT
    instantiating the Onda A embedded (cache / rate_limiter / session_store /
    embedding / config) — those are supplied by the caller's ``base_registry``.

    R-P2-03D: ``config`` (KGConfig) is NO LONGER filled here. The embedded
    ``SettingsKGConfig`` default was a R05-D ledgered temporary fallback; closing
    the config slot means a composition that does not supply ``config`` leaves it
    ``None`` (fail-closed at ``configure_kg_registry``) rather than silently
    receiving the core's embedded settings — runtime real is ANY composition root,
    not only the current Community. The Kùzu/graph adapters do not need ``config``
    to construct, so nothing here depends on it.
    """
    from okto_pulse.core.kg.providers.embedded.kuzu_graph_store import KuzuGraphStore
    from okto_pulse.core.kg.providers.embedded.kuzu_cypher_executor import KuzuCypherExecutor
    from okto_pulse.core.kg.providers.embedded.kuzu_graph_lifecycle import KuzuGraphLifecycle
    from okto_pulse.core.kg.providers.embedded.kuzu_graph_path_resolver import (
        KuzuGraphPathResolver,
    )
    from okto_pulse.core.kg.providers.embedded.kuzu_graph_schema_manager import (
        KuzuGraphSchemaManager,
    )
    from okto_pulse.core.kg.providers.embedded.kuzu_graph_transaction import (
        KuzuGraphTransaction,
    )

    return {
        "graph_store": KuzuGraphStore(),
        "cypher_executor": KuzuCypherExecutor(),
        "graph_transaction": KuzuGraphTransaction(),
        "graph_schema_manager": KuzuGraphSchemaManager(),
        "graph_lifecycle": KuzuGraphLifecycle(),
        "graph_path_resolver": KuzuGraphPathResolver(),
    }


def configure_kg_registry(
    *,
    session_factory: Any | None = None,
    base_registry: "KGProviderRegistry | None" = None,
    defaults_factory: Any | None = None,
    **overrides: Any,
) -> None:
    """Configure the singleton registry with optional provider overrides.

    Called once at bootstrap. Thread-safe.

    Args:
        session_factory: SQLAlchemy async session factory. When provided,
            auto-wires audit_repo (SqlAlchemyAuditRepository) and event_bus
            (SqliteOutboxEventBus) ONLY when the slot is not explicitly
            overridden AND not already supplied by ``base_registry`` (R05-D
            register-before-fallback / prefer-provided).
        base_registry: (R05-B) a pre-built ``KGProviderRegistry`` whose Onda A
            slots (cache_backend / rate_limiter / session_store /
            embedding_provider) are supplied by the edition (e.g. the Community
            adapters) so the core's embedded Onda A are NOT instantiated. The
            core-owned graph slots it leaves ``None`` (Kùzu/graph) are filled by
            ``_build_graph_defaults`` here. R-P2-03D: ``config`` is NO LONGER a
            graph default — the composition MUST supply it (``configure`` fails
            closed otherwise). audit_repo / event_bus auto-wire from
            ``session_factory`` ONLY if the base left them ``None`` — the Community
            edition supplies them explicitly (R05-D), so the auto-wire is a
            ledgered fallback.
        defaults_factory: (R05-B) a callable returning the base registry, used
            instead of ``base_registry`` when the caller prefers lazy
            construction. Same composition semantics as ``base_registry``.
        **overrides: Provider instances keyed by field name.
            Example: configure_kg_registry(cache_backend=RedisCacheBackend(url))

    R-P2-03: a call WITHOUT ``base_registry``/``defaults_factory`` now FAILS
    CLOSED — the TR3 implicit ``_build_defaults()`` escape is retired. A real
    runtime must supply a ``base_registry`` (the Community adapters); tests use a
    ``defaults_factory`` (the sanctioned fake route). ``config`` is a REQUIRED
    slot the composition must provide.
    """
    global _registry, _configured
    with _lock:
        composed = base_registry is not None or defaults_factory is not None
        if base_registry is not None:
            reg = base_registry
        elif defaults_factory is not None:
            reg = defaults_factory()
        else:
            # R-P2-03: the no-base / no-factory path is NO LONGER an escape to the
            # implicit Onda A defaults. A real runtime must supply a base_registry
            # (Community edition adapters); tests must supply a defaults_factory
            # (the sanctioned fake route). ``_build_defaults`` is never an implicit
            # fallback.
            raise RuntimeError(
                "configure_kg_registry requires an explicit base_registry "
                "(Community composition) or defaults_factory (tests): the core no "
                "longer builds implicit Onda A defaults (cache_backend / "
                "rate_limiter / session_store / config)."
            )

        # R05-B: when a base/factory supplied the Onda A slots, mount the
        # core-owned graph providers (Kùzu/graph) into any slot the base left
        # empty — WITHOUT instantiating the embedded Onda A. R-P2-03D: ``config``
        # is NO LONGER mounted here; it is a required composition-supplied slot.
        if composed:
            for key, value in _build_graph_defaults().items():
                if getattr(reg, key, None) is None:
                    setattr(reg, key, value)

        # R05-D: the session_factory auto-wire of audit_repo/event_bus is now a
        # LEDGERED FALLBACK (register-before-fallback). It fires ONLY when the
        # composition did NOT already supply the slot. The Community edition
        # supplies CommunityAuditRepository / CommunityOutboxEventBus EXPLICITLY
        # (community.adapters.composition._apply_data_providers), so for that
        # edition this auto-wire never runs. R-P2-03 retired the non-composed
        # (no base_registry / no defaults_factory) path — it now fails closed, so
        # this auto-wire only fires for a base_registry/defaults_factory that left
        # audit_repo/event_bus ``None``. This fallback is owned / criteria-tracked
        # in data_provider_ownership_gate.LEDGERED_DATA_FALLBACK and retires when
        # spec #04 strangles the Repository-UoW.
        if session_factory is not None:
            if "audit_repo" not in overrides and reg.audit_repo is None:
                from okto_pulse.core.kg.providers.embedded.sqlalchemy_audit_repo import (
                    SqlAlchemyAuditRepository,
                )
                reg.audit_repo = SqlAlchemyAuditRepository(session_factory)

            if "event_bus" not in overrides and reg.event_bus is None:
                from okto_pulse.core.kg.providers.embedded.sqlite_outbox_event_bus import (
                    SqliteOutboxEventBus,
                )
                reg.event_bus = SqliteOutboxEventBus(session_factory)

        for key, value in overrides.items():
            if hasattr(reg, key):
                setattr(reg, key, value)

        # R-P2-03D: ``config`` (KGConfig) is a REQUIRED slot — the core no longer
        # fills it with an implicit SettingsKGConfig. A composition that does not
        # supply it fails closed HERE with an actionable error (not a late
        # AttributeError when a consumer reads ``registry.config``). The Community
        # edition supplies CommunityKGConfig explicitly via
        # ``community.adapters.composition._apply_data_providers``; tests use the
        # ``defaults_factory`` route, whose embedded fake includes config.
        if reg.config is None:
            raise RuntimeError(
                "KG registry config (KGConfig) is required but the composition did "
                "not supply it: a base_registry / defaults_factory (or an explicit "
                "config= override) must provide `config`. The Community edition "
                "supplies CommunityKGConfig via "
                "community.adapters.composition._apply_data_providers; the core no "
                "longer fills it with an implicit SettingsKGConfig default "
                "(R-P2-03D)."
            )

        _registry = reg
        _configured = True


def get_kg_registry() -> KGProviderRegistry:
    """Return the configured singleton registry.

    R-P2-03: the registry is NEVER lazy-initialised with implicit Onda A
    (cache_backend / rate_limiter / session_store / config) defaults. A real
    runtime MUST configure it explicitly via :func:`configure_kg_registry` — the
    Community edition supplies its adapters through a ``base_registry``; tests
    supply the embedded fakes through a ``defaults_factory`` (the sanctioned test
    route, ``tests.kg_registry_testing.configure_test_kg_registry``). Consuming the
    registry before composition is a fail-closed, actionable error — never a late
    ``AttributeError`` on a ``None`` slot.
    """
    if _registry is None:
        raise RuntimeError(
            "KG registry not configured: the composition must call "
            "configure_kg_registry(base_registry=...) (Community edition) or "
            "configure_kg_registry(defaults_factory=...) (tests) before use. The "
            "core no longer builds implicit Onda A defaults (cache_backend / "
            "rate_limiter / session_store / config)."
        )
    return _registry


def reset_registry_for_tests() -> None:
    """Drop the cached registry — tests only."""
    global _registry, _configured
    _registry = None
    _configured = False
