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


def get_reranker(
    strategy: str,
    *,
    llm_ranker_fn: LLMRankerFn | None = None,
    cross_encoder_model: str | None = None,
):
    """Return a reranker instance for the requested strategy.

    Strategies:

    - ``"none"`` — passthrough. No reordering.
    - ``"token_overlap"`` — lexical Jaccard baseline.
    - ``"cross_encoder"`` — sentence-transformers cross-encoder. Falls
      back to ``token_overlap`` with a warning when the optional
      dependency isn't installed.
    - ``"llm"`` — LLM-as-reranker. REQUIRES ``llm_ranker_fn`` — raises
      if not provided, since there's no sensible default.

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
                return _get_or_create("token_overlap", TokenOverlapReranker)
    if strategy == "llm":
        if llm_ranker_fn is None:
            raise ValueError(
                "LLMReranker requires an `llm_ranker_fn` — the project's "
                "LLM provider must be wired by the caller."
            )
        # LLM rerankers are not cached: each caller may bind a different
        # provider (different model, different key, different timeout).
        return LLMReranker(llm_ranker_fn)

    logger.warning(
        "Unknown reranker strategy %r; falling back to noop", strategy
    )
    return _get_or_create("none", NoopReranker)


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


__all__ = ["get_reranker", "reset_reranker_cache"]
