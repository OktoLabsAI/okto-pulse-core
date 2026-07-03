"""Protocol interfaces for the KG layer — dependency injection contracts.

All interfaces use PEP 544 Protocol (structural typing). Implementations
don't need to inherit — they just need to implement the methods.
"""

from okto_pulse.core.kg.interfaces.auth_context import AuthContext
from okto_pulse.core.kg.interfaces.audit_repository import AuditRepository
from okto_pulse.core.kg.interfaces.board_graph_runtime import BoardGraphRuntime
from okto_pulse.core.kg.interfaces.board_source_reader import (
    BoardSourceReader,
    BoardSourceRow,
    InvalidArtifactTypeError,
    SourceReadError,
    SourceReadFailure,
    SourceUnavailableError,
)
from okto_pulse.core.kg.interfaces.cache_backend import CacheBackend
from okto_pulse.core.kg.interfaces.cypher_executor import CypherExecutor
from okto_pulse.core.kg.interfaces.embedding import EmbeddingProvider
from okto_pulse.core.kg.interfaces.event_bus import EventBus, KGEvent
from okto_pulse.core.kg.interfaces.graph_lifecycle import (
    GraphHandle,
    GraphLifecycle,
    PurgeReport,
    RebuildReport,
)
from okto_pulse.core.kg.interfaces.graph_path_resolver import (
    GraphPathResolver,
    GraphStorageState,
)
from okto_pulse.core.kg.interfaces.graph_schema_manager import (
    GraphSchemaManager,
    SchemaValidationResult,
)
from okto_pulse.core.kg.interfaces.graph_store import QueryFilters, SemanticGraphStore
from okto_pulse.core.kg.interfaces.graph_transaction import (
    GraphTransaction,
    GraphTransactionScope,
)
from okto_pulse.core.kg.interfaces.global_discovery_runtime import (
    GlobalDiscoveryRuntime,
)
from okto_pulse.core.kg.interfaces.kg_config import KGConfig
from okto_pulse.core.kg.interfaces.rate_limiter import RateLimiter
from okto_pulse.core.kg.interfaces.rebuild_ingestion import (
    RebuildIngestionPort,
    RebuildSourceResolver,
    RebuildStepAdapterFactory,
)
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    RebuildAuditArtifactStore,
    RebuildAuditKey,
    RebuildAuditNamespace,
)
from okto_pulse.core.kg.interfaces.session_store import SessionStore
from okto_pulse.core.kg.interfaces.registry import (
    KGProviderRegistry,
    configure_kg_registry,
    get_kg_registry,
    reset_registry_for_tests,
)

__all__ = [
    "AuthContext",
    "AuditRepository",
    "BoardGraphRuntime",
    "BoardSourceReader",
    "BoardSourceRow",
    "CacheBackend",
    "CypherExecutor",
    "EmbeddingProvider",
    "EventBus",
    "GraphHandle",
    "GraphLifecycle",
    "GraphPathResolver",
    "GraphSchemaManager",
    "GraphStorageState",
    "GraphTransaction",
    "GraphTransactionScope",
    "GlobalDiscoveryRuntime",
    "InvalidArtifactTypeError",
    "PurgeReport",
    "RebuildReport",
    "KGConfig",
    "KGEvent",
    "KGProviderRegistry",
    "QueryFilters",
    "RateLimiter",
    "RebuildIngestionPort",
    "RebuildAuditArtifactStore",
    "RebuildAuditKey",
    "RebuildAuditNamespace",
    "RebuildSourceResolver",
    "RebuildStepAdapterFactory",
    "SchemaValidationResult",
    "SemanticGraphStore",
    "SessionStore",
    "SourceReadError",
    "SourceReadFailure",
    "SourceUnavailableError",
    "configure_kg_registry",
    "get_kg_registry",
    "reset_registry_for_tests",
]
