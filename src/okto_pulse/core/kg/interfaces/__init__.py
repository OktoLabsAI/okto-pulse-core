"""Protocol interfaces for the KG layer — dependency injection contracts.

All interfaces use PEP 544 Protocol (structural typing). Implementations
don't need to inherit — they just need to implement the methods.
"""

from okto_pulse.core.kg.interfaces.auth_context import AuthContext
from okto_pulse.core.kg.interfaces.audit_repository import AuditRepository
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
from okto_pulse.core.kg.interfaces.cognitive_pending_work import (
    CognitivePendingRecordRef,
    CognitivePendingWorkProvider,
)
from okto_pulse.core.kg.interfaces.embedding import EmbeddingProvider
from okto_pulse.core.kg.interfaces.event_bus import EventBus, KGEvent
from okto_pulse.core.kg.interfaces.graph_lifecycle import (
    GraphHandle,
    GraphLifecycle,
    GraphLifecycleStepResult,
    PurgeReport,
    RebuildReport,
)
from okto_pulse.core.kg.interfaces.graph_errors import (
    GraphCapabilityUnavailable,
    GraphCorruption,
    GraphError,
    GraphIndexUnavailable,
    GraphLockContention,
    GraphUnavailable,
)
from okto_pulse.core.kg.interfaces.graph_recovery import (
    GraphRecovery,
    WalRecoveryReport,
)
from okto_pulse.core.kg.interfaces.graph_runtime_store import (
    GraphPurgeResult,
    GraphRuntimeObservationState,
    GraphRuntimeState,
    GraphRuntimeStore,
    GraphStorageFootprint,
)
from okto_pulse.core.kg.interfaces.graph_schema_manager import (
    GraphSchemaManager,
    SchemaValidationResult,
)
from okto_pulse.core.kg.interfaces.graph_store import (
    GraphCapabilities,
    QueryFilters,
    SemanticGraphStore,
)
from okto_pulse.core.kg.interfaces.graph_transaction import (
    GraphStatementResult,
    GraphTransaction,
    GraphTransactionScope,
)
from okto_pulse.core.kg.interfaces.global_discovery_runtime import (
    GLOBAL_DISCOVERY_WRITER_ARTIFACT_ID,
    GLOBAL_DISCOVERY_WRITER_SCOPE,
    GlobalDiscoveryRuntime,
)
from okto_pulse.core.kg.interfaces.global_discovery_recovery import (
    GlobalDiscoveryArtifactSnapshot,
    GlobalDiscoveryBoardSeed,
    GlobalDiscoveryCutoverResult,
    GlobalDiscoveryDigestSeed,
    GlobalDiscoveryRecovery,
)
from okto_pulse.core.kg.interfaces.kg_config import KGConfig
from okto_pulse.core.kg.interfaces.quarantine_restore import (
    QuarantineRestore,
    QuarantineRestoreError,
    QuarantineRestoreErrorCode,
    RestoreFileEntry,
    RestorePlan,
    RestoreReport,
)
from okto_pulse.core.kg.interfaces.rate_limiter import RateLimiter
from okto_pulse.core.kg.interfaces.reflective_query import (
    Adequacy,
    CriticAction,
    CriticDecision,
    REFLECTIVE_DEFAULT_EDGES,
    ReflectiveCriticPort,
    ReflectiveCriticRequest,
    ReflectiveRetrievalBatch,
    ReflectiveRetrievalPort,
    ReflectiveRetrievalRequest,
    ReflectiveTelemetryPort,
)
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
from okto_pulse.core.kg.interfaces.storage_ref import StorageRef
from okto_pulse.core.kg.interfaces.registry import (
    KGProviderRegistry,
    configure_kg_registry,
    get_kg_registry,
    reset_registry_for_tests,
)

__all__ = [
    "Adequacy",
    "AuthContext",
    "AuditRepository",
    "BoardSourceReader",
    "BoardSourceRow",
    "CacheBackend",
    "CognitivePendingRecordRef",
    "CognitivePendingWorkProvider",
    "CriticAction",
    "CriticDecision",
    "CypherExecutor",
    "EmbeddingProvider",
    "EventBus",
    "GraphHandle",
    "GraphCapabilities",
    "GraphCapabilityUnavailable",
    "GraphCorruption",
    "GraphError",
    "GraphIndexUnavailable",
    "GraphLifecycle",
    "GraphLifecycleStepResult",
    "GraphLockContention",
    "GraphRecovery",
    "GraphPurgeResult",
    "GraphRuntimeObservationState",
    "GraphRuntimeState",
    "GraphRuntimeStore",
    "GraphSchemaManager",
    "GraphStorageFootprint",
    "GraphStatementResult",
    "GraphTransaction",
    "GraphTransactionScope",
    "GraphUnavailable",
    "GlobalDiscoveryRuntime",
    "GLOBAL_DISCOVERY_WRITER_ARTIFACT_ID",
    "GLOBAL_DISCOVERY_WRITER_SCOPE",
    "GlobalDiscoveryArtifactSnapshot",
    "GlobalDiscoveryBoardSeed",
    "GlobalDiscoveryCutoverResult",
    "GlobalDiscoveryDigestSeed",
    "GlobalDiscoveryRecovery",
    "InvalidArtifactTypeError",
    "PurgeReport",
    "RebuildReport",
    "KGConfig",
    "KGEvent",
    "KGProviderRegistry",
    "QuarantineRestore",
    "QuarantineRestoreError",
    "QuarantineRestoreErrorCode",
    "QueryFilters",
    "RateLimiter",
    "REFLECTIVE_DEFAULT_EDGES",
    "ReflectiveCriticPort",
    "ReflectiveCriticRequest",
    "ReflectiveRetrievalBatch",
    "ReflectiveRetrievalPort",
    "ReflectiveRetrievalRequest",
    "ReflectiveTelemetryPort",
    "RestoreFileEntry",
    "RestorePlan",
    "RestoreReport",
    "RebuildIngestionPort",
    "RebuildAuditArtifactStore",
    "RebuildAuditKey",
    "RebuildAuditNamespace",
    "RebuildSourceResolver",
    "RebuildStepAdapterFactory",
    "SchemaValidationResult",
    "SemanticGraphStore",
    "SessionStore",
    "StorageRef",
    "SourceReadError",
    "SourceReadFailure",
    "SourceUnavailableError",
    "WalRecoveryReport",
    "configure_kg_registry",
    "get_kg_registry",
    "reset_registry_for_tests",
]
