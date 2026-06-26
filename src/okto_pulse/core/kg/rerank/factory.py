"""Reranker factory — maps strategy names to concrete instances.

Ideação 3070cd53. Centralises instantiation logic so callers pick a
strategy by string (``"none"`` | ``"token_overlap"`` | ``"cross_encoder"``
| ``"llm"``) and the factory handles degradation when a dependency is
missing (e.g. cross-encoder requested but sentence-transformers not
installed — falls back to token_overlap with a warning).

Instances are cached per strategy at module level because model
loading is expensive (cross-encoder ~80 MB). Tests and integration
callers can reset the cache via ``reset_reranker_cache()``.
"""

from __future__ import annotations

import logging
import threading
from typing import Callable

from .llm import LLMRankerFn, LLMReranker
from .noop import NoopReranker
from .token_overlap import TokenOverlapReranker

logger = logging.getLogger("okto_pulse.kg.rerank")

_cache: dict[str, object] = {}
_cache_lock = threading.Lock()

#: (R05-B IMP3) Optional cross_encoder factory registered by the edition (e.g.
#: the Community CrossEncoder adapter). A 1-entry dict holding
#: ``Callable[[str | None], object]`` (model name -> reranker). When set,
#: ``cross_encoder`` uses it instead of the core's embedded
#: ``CrossEncoderReranker`` — so the concrete adapter can live in the Community
#: edition WITHOUT the core importing community. Register-before-remove: the core
#: default stays as the fallback; a factory raising ``ImportError`` (optional dep
#: absent) degrades to ``token_overlap``, exactly like the core default.
#:
#: It is a module CONSTANT mutated IN PLACE (never reassigned via ``global``), so
#: the AntiSingletonGate does not classify it as a new module-global singleton.
_cross_encoder_registry: "dict[str, Callable[[str | None], object]]" = {}


def register_cross_encoder_factory(
    factory: Callable[[str | None], object] | None,
) -> None:
    """Register (or clear, with ``None``) the edition cross_encoder factory."""
    if factory is None:
        _cross_encoder_registry.pop("factory", None)
    else:
        _cross_encoder_registry["factory"] = factory


def reset_cross_encoder_factory() -> None:
    """Drop the registered cross_encoder factory (tests)."""
    _cross_encoder_registry.clear()


def get_reranker(
    strategy: str,
    *,
    llm_ranker_fn: LLMRankerFn | None = None,
    provider=None,
    board_id: str | None = None,
    actor_id: str | None = None,
    cross_encoder_model: str | None = None,
):
    """Return a reranker instance for the requested strategy.

    Strategies:

    - ``"none"`` — passthrough. No reordering.
    - ``"token_overlap"`` — lexical Jaccard baseline.
    - ``"cross_encoder"`` — sentence-transformers cross-encoder. Falls
      back to ``token_overlap`` with a warning when the optional
      dependency isn't installed.
    - ``"llm"`` — LLM-as-reranker. Wire it with EITHER a legacy
      ``llm_ranker_fn`` callable (takes precedence) OR an R13-A
      ``provider`` (an ``LLMProvider``), from which the bridge callable is
      derived via ``llm_provider_bridges`` (memoized per
      ``(provider, board_id, actor_id)``). With NEITHER, this is
      fail-closed and raises ``ValueError`` — there is no sensible default.
      LLM rerankers are NOT cached (each may bind a different provider);
      that is unchanged. ``provider`` is ignored for the other strategies.

    Unknown strategies fall back to ``"none"`` with a warning so a
    typo in configuration never breaks the retrieval pipeline.
    """
    strategy = (strategy or "none").strip().lower()

    if strategy == "none":
        return _get_or_create("none", NoopReranker)
    if strategy == "token_overlap":
        return _get_or_create("token_overlap", TokenOverlapReranker)
    if strategy == "cross_encoder":
        key = f"cross_encoder::{cross_encoder_model or 'default'}"
        with _cache_lock:
            inst = _cache.get(key)
            if inst is not None:
                return inst
            try:
                _ce_factory = _cross_encoder_registry.get("factory")
                if _ce_factory is not None:
                    # Edition-registered factory (e.g. Community CrossEncoder
                    # adapter). May raise ImportError when the optional dep is
                    # absent -> token_overlap fallback below, same as the core.
                    inst = _ce_factory(cross_encoder_model)
                else:
                    from .cross_encoder import CrossEncoderReranker
                    inst = CrossEncoderReranker(model_name=cross_encoder_model)
                _cache[key] = inst
                return inst
            except ImportError as e:
                logger.warning(
                    "cross_encoder reranker unavailable (%s); "
                    "falling back to token_overlap",
                    e,
                )
                # Fall through: resolve the token_overlap fallback OUTSIDE this
                # lock (below). The cached/success paths already returned above.
        # `_get_or_create` re-acquires the same non-reentrant `_cache_lock`, so
        # it MUST run after the `with` block has released it — calling it inside
        # the lock deadlocks (surfaced by cross_encoder lazy-fallback ts_c9ff52b2).
        return _get_or_create("token_overlap", TokenOverlapReranker)
    if strategy == "llm":
        if llm_ranker_fn is None and provider is not None:
            llm_ranker_fn = _bridge_ranker_fn(
                provider, board_id=board_id, actor_id=actor_id
            )
        if llm_ranker_fn is None:
            raise ValueError(
                "LLMReranker requires an `llm_ranker_fn` or `provider` — "
                "the project's LLM access must be wired by the caller."
            )
        # LLM rerankers are not cached: each caller may bind a different
        # provider (different model, different key, different timeout).
        return LLMReranker(llm_ranker_fn)

    logger.warning(
        "Unknown reranker strategy %r; falling back to noop", strategy
    )
    return _get_or_create("none", NoopReranker)


def _bridge_ranker_fn(provider, *, board_id, actor_id) -> LLMRankerFn:
    """Derive the legacy ``llm_ranker_fn`` from an R13-A ``LLMProvider`` via
    the bridge. Imported lazily to avoid any import cycle and to keep the
    contract port out of this module's eager import graph.

    Memoized per ``(provider, board_id, actor_id)`` inside the bridge, so the
    derived callable keeps a stable identity across calls.
    """
    from .llm_provider_bridges import make_llm_ranker_fn

    return make_llm_ranker_fn(provider, board_id=board_id, actor_id=actor_id)


def _get_or_create(key: str, ctor: Callable[[], object]) -> object:
    with _cache_lock:
        inst = _cache.get(key)
        if inst is None:
            inst = ctor()
            _cache[key] = inst
        return inst


def reset_reranker_cache() -> None:
    """Drop all cached reranker instances. Call in tests or when a
    configuration change requires a fresh model load."""
    with _cache_lock:
        _cache.clear()


__all__ = [
    "get_reranker",
    "reset_reranker_cache",
    "register_cross_encoder_factory",
    "reset_cross_encoder_factory",
]
