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

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
    runtime_lock,
)

from okto_pulse.core.kg.interfaces.audit_repository import AuditRepository
from okto_pulse.core.kg.interfaces.board_source_reader import BoardSourceReader
from okto_pulse.core.kg.interfaces.cache_backend import CacheBackend
from okto_pulse.core.kg.interfaces.cypher_executor import CypherExecutor
from okto_pulse.core.kg.interfaces.cognitive_pending_work import (
    CognitivePendingWorkProvider,
)
from okto_pulse.core.kg.interfaces.embedding import EmbeddingProvider
from okto_pulse.core.kg.interfaces.event_bus import EventBus
from okto_pulse.core.kg.interfaces.graph_lifecycle import GraphLifecycle
from okto_pulse.core.kg.interfaces.graph_recovery import GraphRecovery
from okto_pulse.core.kg.interfaces.graph_runtime_store import GraphRuntimeStore
from okto_pulse.core.kg.interfaces.graph_schema_manager import GraphSchemaManager
from okto_pulse.core.kg.interfaces.graph_store import SemanticGraphStore
from okto_pulse.core.kg.interfaces.graph_transaction import GraphTransaction
from okto_pulse.core.kg.interfaces.global_discovery_runtime import (
    GlobalDiscoveryRuntime,
)
from okto_pulse.core.kg.interfaces.kg_config import KGConfig
from okto_pulse.core.kg.interfaces.quarantine_restore import QuarantineRestore
from okto_pulse.core.kg.interfaces.rate_limiter import RateLimiter
from okto_pulse.core.kg.interfaces.rebuild_ingestion import RebuildIngestionPort
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    RebuildAuditArtifactStore,
    RebuildAuditArtifactStoreResolver,
)
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
    # concrete graph runtimes can move out of core.
    graph_transaction: GraphTransaction | None = None
    graph_schema_manager: GraphSchemaManager | None = None
    graph_lifecycle: GraphLifecycle | None = None
    graph_runtime_store: GraphRuntimeStore | None = None
    global_discovery_runtime: GlobalDiscoveryRuntime | None = None
    board_source_reader: BoardSourceReader | None = None
    rebuild_ingestion_port: RebuildIngestionPort | None = None
    rebuild_audit_artifact_store: RebuildAuditArtifactStore | None = None
    rebuild_audit_artifact_store_resolver: RebuildAuditArtifactStoreResolver | None = None
    cognitive_pending_work_provider: CognitivePendingWorkProvider | None = None
    # KGD-01 FR4 — quarantine restore port (dry-run/apply with backup-swap).
    # Optional slot: supplied by the Community composition; read-time
    # fail-closed via require_quarantine_restore (never a configure-time
    # required slot, so existing compositions/tests stay valid).
    quarantine_restore: QuarantineRestore | None = None
    # KGD-01 FR3/BR2 — wal-only recovery port (degrau 2 da escada de
    # recovery). Same optional-slot contract as quarantine_restore: supplied
    # by the Community composition; read-time fail-closed via
    # require_graph_recovery.
    graph_recovery: GraphRecovery | None = None

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

    def require_graph_schema_manager(self) -> GraphSchemaManager:
        return self._require_provider("graph_schema_manager")

    def require_global_discovery_runtime(self) -> GlobalDiscoveryRuntime:
        return self._require_provider("global_discovery_runtime")

    def require_board_source_reader(self) -> BoardSourceReader:
        return self._require_provider("board_source_reader")

    def require_rebuild_ingestion_port(self) -> RebuildIngestionPort:
        return self._require_provider("rebuild_ingestion_port")

    def require_rebuild_audit_artifact_store(self) -> RebuildAuditArtifactStore:
        return self._require_provider("rebuild_audit_artifact_store")

    def require_rebuild_audit_artifact_store_resolver(
        self,
    ) -> RebuildAuditArtifactStoreResolver:
        return self._require_provider("rebuild_audit_artifact_store_resolver")

    def require_cognitive_pending_work_provider(self) -> CognitivePendingWorkProvider:
        return self._require_provider("cognitive_pending_work_provider")

    def require_quarantine_restore(self) -> QuarantineRestore:
        return self._require_provider("quarantine_restore")

    def require_graph_recovery(self) -> GraphRecovery:
        return self._require_provider("graph_recovery")


_lock = runtime_lock("kg.interfaces.registry")
_RUNTIME_KEY = "kg.provider_registry"


def _build_graph_defaults() -> dict[str, Any]:
    """No production graph defaults exist in core.

    Runtime graph providers must be supplied by the composition root (Community
    today, future SaaS adapters later). Tests inject explicit fakes.
    """
    return {}


def configure_kg_registry(
    *,
    base_registry: "KGProviderRegistry | None" = None,
    defaults_factory: Any | None = None,
    **overrides: Any,
) -> None:
    """Configure the singleton registry with optional provider overrides.

    Called once at bootstrap. Thread-safe.

    Args:
        base_registry: (R05-B) a pre-built ``KGProviderRegistry`` whose Onda A
            slots (cache_backend / rate_limiter / session_store /
            embedding_provider) are supplied by the edition (e.g. the Community
            adapters) so the core's embedded Onda A are NOT instantiated. The
            core-owned graph slots it leaves ``None`` (graph backend/graph) are filled by
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

        # R-P2-05: core no longer mounts concrete graph providers. A real
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
        # them with an implicit/relational/graph backend fallback.
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
                "graph_runtime_store",
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
                "(R-P2-03D), or concrete graph defaults/step adapters "
                "(R-P2-05), or BoardSourceReader source access (R10A). The "
                "Community edition supplies them via community.adapters.composition; "
                "tests use a defaults_factory / explicit fakes."
            )

        register_runtime_value(_RUNTIME_KEY, reg)


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
    registry = resolve_runtime_value(_RUNTIME_KEY)
    if registry is None:
        raise RuntimeError(
            "KG registry not configured: the composition must call "
            "configure_kg_registry(base_registry=...) (Community edition) or "
            "configure_kg_registry(defaults_factory=...) (tests) before use. The "
            "core no longer builds implicit Onda A defaults (cache_backend / "
            "rate_limiter / session_store / config)."
        )
    return registry


def reset_registry_for_tests() -> None:
    """Drop the cached registry — tests only."""
    reset_runtime_values(_RUNTIME_KEY)


def capture_registry_state_for_tests() -> KGProviderRegistry | None:
    """Capture the context-local registry for a test that temporarily replaces it."""

    return resolve_runtime_value(_RUNTIME_KEY)


def restore_registry_state_for_tests(registry: KGProviderRegistry | None) -> None:
    """Restore a registry captured by :func:`capture_registry_state_for_tests`."""

    if registry is None:
        reset_registry_for_tests()
    else:
        register_runtime_value(_RUNTIME_KEY, registry)
