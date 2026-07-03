"""KGProviderRegistry — central dependency injection container for the KG layer.

Usage:
    # At bootstrap (app.py or main.py):
    from okto_pulse.core.kg.interfaces import configure_kg_registry
    configure_kg_registry(cache_backend=RedisCacheBackend(url))

    # In consumers (kg_service.py, tier_power.py, etc.):
    from okto_pulse.core.kg.interfaces import get_kg_registry
    cache = get_kg_registry().require_cache_backend()
    hit, val = cache.get(tool_name, board_id, params)
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from okto_pulse.core.kg.interfaces.audit_repository import AuditRepository
from okto_pulse.core.kg.interfaces.board_graph_runtime import BoardGraphRuntime
from okto_pulse.core.kg.interfaces.board_source_reader import BoardSourceReader
from okto_pulse.core.kg.interfaces.cache_backend import CacheBackend
from okto_pulse.core.kg.interfaces.cypher_executor import CypherExecutor
from okto_pulse.core.kg.interfaces.embedding import EmbeddingProvider
from okto_pulse.core.kg.interfaces.event_bus import EventBus
from okto_pulse.core.kg.interfaces.graph_lifecycle import GraphLifecycle
from okto_pulse.core.kg.interfaces.graph_path_resolver import GraphPathResolver
from okto_pulse.core.kg.interfaces.graph_schema_manager import GraphSchemaManager
from okto_pulse.core.kg.interfaces.graph_store import SemanticGraphStore
from okto_pulse.core.kg.interfaces.graph_transaction import GraphTransaction
from okto_pulse.core.kg.interfaces.global_discovery_runtime import (
    GlobalDiscoveryRuntime,
)
from okto_pulse.core.kg.interfaces.kg_config import KGConfig
from okto_pulse.core.kg.interfaces.rate_limiter import RateLimiter
from okto_pulse.core.kg.interfaces.rebuild_ingestion import RebuildIngestionPort
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
    safe_write_step_adapter: Any | None = None
    board_graph_runtime: BoardGraphRuntime | None = None
    global_discovery_runtime: GlobalDiscoveryRuntime | None = None
    board_source_reader: BoardSourceReader | None = None
    rebuild_ingestion_port: RebuildIngestionPort | None = None

    # ------------------------------------------------------------------ R03 IMP1
    # AC3 (base fail-closed): cache_backend, rate_limiter and session_store are
    # read-time-fail-closed PORTS, not configure-time required slots — they are
    # deliberately NOT auto-promoted into the configure-time ``_missing`` list
    # (br_01359bdf). A real runtime composes them via the Community adapters; a
    # test composes sanctioned fakes through a ``defaults_factory``. Reading one
    # the composition never supplied raises a structured ``runtime_provider_missing``
    # at the porta — never a late ``AttributeError`` on ``None`` nor a silent
    # concrete fallback.
    def _require_provider(self, slot: str) -> Any:
        value = getattr(self, slot, None)
        if value is None:
            # Lazy import keeps the kg layer free of a module-level dependency on
            # the composition root while reusing the canonical structured error.
            from okto_pulse.core.composition import RuntimeProviderMissing

            raise RuntimeProviderMissing(slot)
        return value

    def require_cache_backend(self) -> CacheBackend:
        return self._require_provider("cache_backend")

    def require_rate_limiter(self) -> RateLimiter:
        return self._require_provider("rate_limiter")

    def require_session_store(self) -> SessionStore:
        return self._require_provider("session_store")

    def require_embedding_provider(self) -> EmbeddingProvider:
        return self._require_provider("embedding_provider")

    def require_global_discovery_runtime(self) -> GlobalDiscoveryRuntime:
        return self._require_provider("global_discovery_runtime")

    def require_board_source_reader(self) -> BoardSourceReader:
        return self._require_provider("board_source_reader")

    def require_rebuild_ingestion_port(self) -> RebuildIngestionPort:
        return self._require_provider("rebuild_ingestion_port")


_registry: KGProviderRegistry | None = None
_lock = threading.Lock()
_configured = False


def _build_defaults() -> KGProviderRegistry:
    """Build a registry with test-only in-memory defaults.

    Populates Onda 1 (config, cache, rate_limiter, embedding), Onda 2
    (session_store), and in-memory graph fakes. event_bus and audit_repo are not
    defaulted here: real runtimes must compose them explicitly and tests must
    provide fakes via defaults_factory/overrides.
    """
    from okto_pulse.core.kg.providers.embedded.settings_config import SettingsKGConfig
    from okto_pulse.core.kg.providers.embedded.memory_cache import InMemoryCacheBackend
    from okto_pulse.core.kg.providers.embedded.memory_rate_limiter import InMemoryTokenBucket
    from okto_pulse.core.kg.providers.embedded.memory_session_store import InMemorySessionStore
    from okto_pulse.core.kg.providers.testing.memory_graph_store import (
        InMemoryCypherExecutor,
        InMemoryBoardGraphRuntime,
        InMemoryGraphLifecycle,
        InMemoryGraphPathResolver,
        InMemoryGraphSchemaManager,
        InMemoryGraphStore,
        InMemoryGraphTransaction,
        in_memory_safe_write_step_adapter,
    )
    from okto_pulse.core.kg.providers.testing.memory_global_discovery_runtime import (
        InMemoryGlobalDiscoveryRuntime,
    )
    from okto_pulse.core.kg.providers.testing.memory_board_source_reader import (
        InMemoryBoardSourceReader,
    )
    from okto_pulse.core.kg.providers.testing.embedding import (
        build_testing_embedding_provider,
    )

    config = SettingsKGConfig()
    graph_store = InMemoryGraphStore()
    graph_path_resolver = InMemoryGraphPathResolver()
    graph_schema_manager = InMemoryGraphSchemaManager(graph_store)
    return KGProviderRegistry(
        # Onda 1
        config=config,
        cache_backend=InMemoryCacheBackend(),
        rate_limiter=InMemoryTokenBucket(),
        embedding_provider=build_testing_embedding_provider(config),
        # Onda 2
        session_store=InMemorySessionStore(
            default_ttl_seconds=config.kg_session_ttl_seconds,
        ),
        # Onda 3
        graph_store=graph_store,
        cypher_executor=InMemoryCypherExecutor(),
        # Onda 4 — test-only graph storage fakes
        graph_transaction=InMemoryGraphTransaction(),
        graph_schema_manager=graph_schema_manager,
        graph_lifecycle=InMemoryGraphLifecycle(
            resolver=graph_path_resolver,
            schema_manager=graph_schema_manager,
        ),
        graph_path_resolver=graph_path_resolver,
        safe_write_step_adapter=in_memory_safe_write_step_adapter,
        board_graph_runtime=InMemoryBoardGraphRuntime(
            store=graph_store,
            resolver=graph_path_resolver,
            schema_manager=graph_schema_manager,
        ),
        global_discovery_runtime=InMemoryGlobalDiscoveryRuntime(),
        board_source_reader=InMemoryBoardSourceReader(),
        # event_bus, audit_repo, auth_context_factory supplied by composition
    )


def _build_graph_defaults() -> dict[str, Any]:
    """No production graph defaults exist in core.

    Runtime graph providers must be supplied by the composition root (Community
    today, future SaaS adapters later). Tests use ``_build_defaults`` fakes.
    """
    return {}


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
        session_factory: Deprecated compatibility argument. R-P2-02 removed
            relational auto-wire from core; passing this no longer creates
            audit_repo/event_bus.
        base_registry: (R05-B) a pre-built ``KGProviderRegistry`` whose Onda A
            slots (cache_backend / rate_limiter / session_store /
            embedding_provider) are supplied by the edition (e.g. the Community
            adapters) so the core's embedded Onda A are NOT instantiated. The
            core-owned graph slots it leaves ``None`` (Kùzu/graph) are filled by
            ``_build_graph_defaults`` here. R-P2-03D: ``config`` is NO LONGER a
            graph default — the composition MUST supply it (``configure`` fails
            closed otherwise). audit_repo / event_bus auto-wire from
            Community edition supplies audit_repo / event_bus explicitly; core
            fails closed if those slots are absent.
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

        # R-P2-05: core no longer mounts Kùzu/Ladybug graph providers. A real
        # composition root must supply graph slots explicitly; tests use
        # _build_defaults fakes.
        if composed:
            for key, value in _build_graph_defaults().items():
                if getattr(reg, key, None) is None:
                    setattr(reg, key, value)

        # R-P2-02: the session_factory auto-wire of audit_repo/event_bus is RETIRED.
        # The core no longer instantiates SqliteOutboxEventBus / SqlAlchemyAuditRepository
        # as a silent relational fallback — the composition root MUST supply both
        # ``event_bus`` and ``audit_repo`` explicitly (the Community edition does, via
        # community.adapters.composition._apply_data_providers; tests supply fakes).
        # A real composed runtime that omits either one fails closed below.
        for key, value in overrides.items():
            if hasattr(reg, key):
                setattr(reg, key, value)

        # R-P2-02 / R-P2-03D / R-P2-05: ``config`` (KGConfig), ``event_bus``
        # (EventBus), ``audit_repo`` (AuditRepository), and graph providers are
        # REQUIRED composition-supplied slots — the core no longer fills any of
        # them with an implicit/relational/Kùzu fallback.
        # A real composed runtime that omits one fails closed HERE with an actionable
        # error (never a late AttributeError when a consumer reads the slot). The
        # Community edition supplies all three explicitly via
        # ``community.adapters.composition._apply_data_providers``; tests use the
        # ``defaults_factory`` route + explicit fakes.
        _missing = [
            name
            for name in (
                "config",
                "event_bus",
                "audit_repo",
                "graph_store",
                "cypher_executor",
                "graph_transaction",
                "graph_schema_manager",
                "graph_lifecycle",
                "graph_path_resolver",
                "safe_write_step_adapter",
                "global_discovery_runtime",
                "board_source_reader",
            )
            if getattr(reg, name) is None
        ]
        if _missing:
            raise RuntimeError(
                "KG registry is missing required composition-supplied slot(s): "
                f"{', '.join(_missing)}. The composition root must provide config "
                "(KGConfig), event_bus (EventBus), audit_repo (AuditRepository), "
                "and graph providers explicitly — the core no longer auto-wires "
                "relational fallbacks (R-P2-02), implicit SettingsKGConfig "
                "(R-P2-03D), or Kuzu/Ladybug graph defaults/step adapters "
                "(R-P2-05), or BoardSourceReader source access (R10A). The "
                "Community edition supplies them via community.adapters.composition; "
                "tests use a defaults_factory / explicit fakes."
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
