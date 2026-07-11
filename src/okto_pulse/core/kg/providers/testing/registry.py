"""Explicit test-only composition for the KG provider registry."""

from __future__ import annotations

from okto_pulse.core.kg.interfaces.registry import KGProviderRegistry


def build_testing_kg_registry() -> KGProviderRegistry:
    """Build in-memory contract fakes without exposing them from production wiring."""

    from .embedding import build_testing_embedding_provider
    from .memory import InMemoryCacheBackend, InMemorySessionStore, InMemoryTokenBucket
    from .memory_board_source_reader import InMemoryBoardSourceReader
    from .memory_global_discovery_runtime import InMemoryGlobalDiscoveryRuntime
    from .memory_graph_store import (
        InMemoryCypherExecutor,
        InMemoryGraphLifecycle,
        InMemoryGraphRuntimeStore,
        InMemoryGraphSchemaManager,
        InMemoryGraphStore,
        InMemoryGraphTransaction,
    )
    from .memory_rebuild_audit_storage import (
        InMemoryCognitivePendingWorkProvider,
        InMemoryRebuildAuditArtifactStore,
        InMemoryRebuildAuditArtifactStoreResolver,
    )
    from .settings_config import SettingsKGConfig

    config = SettingsKGConfig()
    graph_store = InMemoryGraphStore()
    graph_schema_manager = InMemoryGraphSchemaManager(graph_store)
    return KGProviderRegistry(
        config=config,
        cache_backend=InMemoryCacheBackend(),
        rate_limiter=InMemoryTokenBucket(),
        embedding_provider=build_testing_embedding_provider(config),
        session_store=InMemorySessionStore(
            default_ttl_seconds=config.kg_session_ttl_seconds,
        ),
        graph_store=graph_store,
        cypher_executor=InMemoryCypherExecutor(),
        graph_transaction=InMemoryGraphTransaction(),
        graph_schema_manager=graph_schema_manager,
        graph_lifecycle=InMemoryGraphLifecycle(schema_manager=graph_schema_manager),
        graph_runtime_store=InMemoryGraphRuntimeStore(
            store=graph_store,
            schema_manager=graph_schema_manager,
        ),
        global_discovery_runtime=InMemoryGlobalDiscoveryRuntime(),
        board_source_reader=InMemoryBoardSourceReader(),
        rebuild_audit_artifact_store=InMemoryRebuildAuditArtifactStore(),
        rebuild_audit_artifact_store_resolver=(
            InMemoryRebuildAuditArtifactStoreResolver()
        ),
        cognitive_pending_work_provider=InMemoryCognitivePendingWorkProvider(),
    )


__all__ = ["build_testing_kg_registry"]
