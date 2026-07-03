"""Central bounded cache for LLMProvider bridge adapters.

The provider bridge modules adapt an injected ``LLMProvider`` to legacy callable
shapes used by KG flows. They need stable callable identity for downstream
caches keyed by ``id(llm_fn)``, but that state must be owned, bounded, and
resettable from one core-owned place.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Callable, Hashable, TypeVar, cast

T = TypeVar("T")

DEFAULT_BRIDGE_CACHE_MAX_ENTRIES = 1024
DEFAULT_BRIDGE_CACHE_REALM = "runtime"

INVENTORIED_PROVIDER_BRIDGE_NAMESPACES: tuple[str, ...] = (
    "events.handlers.learning_summariser",
    "kg.adaptive_hops",
    "kg.agent.heuristics",
    "kg.context_compress",
    "kg.grounding",
    "kg.query_rewrite",
    "kg.rerank",
    "kg.retrieve_critic",
)


@dataclass(frozen=True)
class BridgeCacheStats:
    namespace: str
    realm: str
    size: int
    max_entries: int


class BridgeCacheRegistry:
    """Thread-safe LRU registry keyed by ``(realm, namespace, key)``."""

    def __init__(self, *, max_entries: int = DEFAULT_BRIDGE_CACHE_MAX_ENTRIES) -> None:
        self._max_entries = _validate_max_entries(max_entries)
        self._lock = threading.RLock()
        self._entries: dict[
            tuple[str, str], OrderedDict[tuple[Hashable, ...], object]
        ] = {}

    @property
    def max_entries(self) -> int:
        return self._max_entries

    def get_or_create(
        self,
        namespace: str,
        key: Hashable | tuple[Hashable, ...],
        factory: Callable[[], T],
        *,
        realm: str = DEFAULT_BRIDGE_CACHE_REALM,
        max_entries: int | None = None,
    ) -> T:
        if not namespace:
            raise ValueError("Bridge cache namespace is required.")
        if not realm:
            raise ValueError("Bridge cache realm is required.")
        limit = _validate_max_entries(
            self._max_entries if max_entries is None else max_entries
        )
        scoped_key = (realm, namespace)
        cache_key = _normalise_key(key)
        with self._lock:
            entries = self._entries.setdefault(scoped_key, OrderedDict())
            if cache_key in entries:
                value = entries.pop(cache_key)
                entries[cache_key] = value
                return cast(T, value)
            value = factory()
            entries[cache_key] = value
            while len(entries) > limit:
                entries.popitem(last=False)
            return value

    def reset_namespace(
        self,
        namespace: str,
        *,
        realm: str | None = None,
    ) -> None:
        if not namespace:
            raise ValueError("Bridge cache namespace is required.")
        with self._lock:
            if realm is not None:
                self._entries.pop((realm, namespace), None)
                return
            for scoped_key in [
                scoped for scoped in self._entries if scoped[1] == namespace
            ]:
                self._entries.pop(scoped_key, None)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def stats(self) -> tuple[BridgeCacheStats, ...]:
        with self._lock:
            return tuple(
                BridgeCacheStats(
                    namespace=namespace,
                    realm=realm,
                    size=len(entries),
                    max_entries=self._max_entries,
                )
                for (realm, namespace), entries in sorted(self._entries.items())
            )


def bridge_cache_get_or_create(
    namespace: str,
    key: Hashable | tuple[Hashable, ...],
    factory: Callable[[], T],
    *,
    realm: str = DEFAULT_BRIDGE_CACHE_REALM,
    max_entries: int | None = None,
) -> T:
    return _bridge_cache_registry.get_or_create(
        namespace,
        key,
        factory,
        realm=realm,
        max_entries=max_entries,
    )


def reset_bridge_cache_namespace(
    namespace: str,
    *,
    realm: str | None = None,
) -> None:
    _bridge_cache_registry.reset_namespace(namespace, realm=realm)


def reset_all_bridge_caches_for_tests(
    *,
    max_entries: int = DEFAULT_BRIDGE_CACHE_MAX_ENTRIES,
) -> None:
    global _bridge_cache_registry
    _bridge_cache_registry = BridgeCacheRegistry(max_entries=max_entries)


def bridge_cache_stats() -> tuple[BridgeCacheStats, ...]:
    return _bridge_cache_registry.stats()


def _normalise_key(key: Hashable | tuple[Hashable, ...]) -> tuple[Hashable, ...]:
    if isinstance(key, tuple):
        return key
    return (key,)


def _validate_max_entries(value: int) -> int:
    if value < 1:
        raise ValueError("Bridge cache max_entries must be >= 1.")
    return value


_bridge_cache_registry = BridgeCacheRegistry()


__all__ = [
    "DEFAULT_BRIDGE_CACHE_MAX_ENTRIES",
    "DEFAULT_BRIDGE_CACHE_REALM",
    "INVENTORIED_PROVIDER_BRIDGE_NAMESPACES",
    "BridgeCacheRegistry",
    "BridgeCacheStats",
    "bridge_cache_get_or_create",
    "bridge_cache_stats",
    "reset_all_bridge_caches_for_tests",
    "reset_bridge_cache_namespace",
]
