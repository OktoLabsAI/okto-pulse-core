"""Protocol interfaces for the KG layer — dependency injection contracts.

All interfaces use PEP 544 Protocol (structural typing). Implementations
don't need to inherit — they just need to implement the methods.
"""

from okto_pulse.core.kg.interfaces.auth_context import AuthContext
from okto_pulse.core.kg.interfaces.audit_repository import AuditRepository
from okto_pulse.core.kg.interfaces.cache_backend import CacheBackend
from okto_pulse.core.kg.interfaces.cypher_executor import CypherExecutor
from okto_pulse.core.kg.interfaces.embedding import EmbeddingProvider
from okto_pulse.core.kg.interfaces.event_bus import EventBus, KGEvent
from okto_pulse.core.kg.interfaces.graph_store import QueryFilters, SemanticGraphStore
from okto_pulse.core.kg.interfaces.kg_config import KGConfig
from okto_pulse.core.kg.interfaces.rate_limiter import RateLimiter
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
    "CacheBackend",
    "CypherExecutor",
    "EmbeddingProvider",
    "EventBus",
    "KGConfig",
    "KGEvent",
    "KGProviderRegistry",
    "QueryFilters",
    "RateLimiter",
    "SemanticGraphStore",
    "SessionStore",
    "configure_kg_registry",
    "get_kg_registry",
    "reset_registry_for_tests",
]
