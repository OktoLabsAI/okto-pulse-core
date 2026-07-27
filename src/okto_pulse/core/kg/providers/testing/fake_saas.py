"""SaaS-shaped fake adapters for Core conformance tests.

They deliberately live under the testing namespace: they prove that a future
edition can satisfy the same ports without importing Community, FastAPI or a
local persistence implementation.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .memory import InMemoryCacheBackend, InMemorySessionStore, InMemoryTokenBucket


class FakeSaaSCacheBackend(InMemoryCacheBackend):
    """Tenant-scoped cache fake used only by SaaS contract tests."""


class FakeSaaSRateLimiter(InMemoryTokenBucket):
    """Service-account rate limiter fake used only by SaaS contract tests."""


class FakeSaaSSessionStore(InMemorySessionStore):
    """Session lifecycle fake used only by SaaS contract tests."""


@dataclass(frozen=True)
class FakeSaaSKGConfig:
    """Minimal tenant-aware implementation of the pure ``KGConfig`` contract."""

    tenant_id: str = "tenant-01"
    kg_base_dir: str = "saas://tenant-01/kg"
    kg_embedding_mode: str = "remote"
    kg_embedding_model: str = "tenant-model"
    kg_embedding_dim: int = 1536
    kg_session_ttl_seconds: int = 900
    kg_cleanup_interval_seconds: int = 60
    kg_cleanup_enabled: bool = True


@dataclass
class FakeSaaSRuntime:
    """Independent fake provider bundle for one SaaS tenant/runtime."""

    config: FakeSaaSKGConfig = field(default_factory=FakeSaaSKGConfig)
    cache_backend: FakeSaaSCacheBackend = field(default_factory=FakeSaaSCacheBackend)
    rate_limiter: FakeSaaSRateLimiter = field(default_factory=FakeSaaSRateLimiter)
    session_store: FakeSaaSSessionStore = field(init=False)

    def __post_init__(self) -> None:
        self.session_store = FakeSaaSSessionStore(
            default_ttl_seconds=self.config.kg_session_ttl_seconds
        )


__all__ = [
    "FakeSaaSCacheBackend",
    "FakeSaaSKGConfig",
    "FakeSaaSRateLimiter",
    "FakeSaaSRuntime",
    "FakeSaaSSessionStore",
]
