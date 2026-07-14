"""QueryRewriter factory — maps strategy names to instances.

Caches instances per strategy+paraphrase-count so loading the LLM
callable once is reused across queries. The LLM itself is injected
by the caller, so no model is downloaded or initialised inside this
module.
"""

from __future__ import annotations

import logging
import threading
from typing import Callable

from okto_pulse.core.runtime_context import runtime_lock, runtime_state

from .decompose import DecomposeRewriter
from .fusion import FusionRewriter
from .hyde import HyDERewriter
from .noop import NoopRewriter

logger = logging.getLogger("okto_pulse.kg.query_rewrite")

_cache = runtime_state("kg.query_rewrite.factory_cache", dict)
_cache_lock = runtime_lock("kg.query_rewrite.factory_cache")


def get_rewriter(
    strategy: str,
    *,
    llm_fn: Callable | None = None,
    provider=None,
    board_id: str | None = None,
    actor_id: str | None = None,
    fusion_paraphrases: int = 3,
):
    """Return a rewriter instance for the requested strategy.

    Strategies:
      - ``"none"`` — NoopRewriter, passthrough.
      - ``"hyde"`` — HyDERewriter, needs `llm_fn: Callable[[str], str]`.
      - ``"decompose"`` — DecomposeRewriter,
          needs `llm_fn: Callable[[str], list[str]]`.
      - ``"fusion"`` — FusionRewriter,
          needs `llm_fn: Callable[[str, int], list[str]]`.

    LLM wiring (hyde/decompose/fusion): pass EITHER a legacy ``llm_fn``
    callable (takes precedence) OR an R13-A ``provider`` (an
    ``LLMProvider``) from which the matching bridge callable is derived
    via ``llm_provider_bridges``. The bridge is memoized per
    ``(provider, board_id, actor_id)`` so the derived ``llm_fn`` keeps a
    stable identity and the per-``id(llm_fn)`` cache below still works.
    With NEITHER, the factory is fail-closed and raises ``ValueError`` —
    it never silently degrades to noop. The ``provider`` is ignored for
    the ``none``/unknown strategies (behaviour unchanged).

    Unknown strategies fall back to NoopRewriter with a warning so a
    typo in configuration never breaks the retrieval pipeline.
    """
    strategy = (strategy or "none").strip().lower()

    if strategy == "none":
        return _get_or_create("none", NoopRewriter)

    if strategy in ("hyde", "decompose", "fusion"):
        if llm_fn is None and provider is not None:
            llm_fn = _bridge_llm_fn(
                strategy, provider, board_id=board_id, actor_id=actor_id
            )
        if llm_fn is None:
            raise ValueError(
                f"Rewriter strategy {strategy!r} requires an `llm_fn` or "
                f"`provider` — the project's LLM access must be wired by "
                f"the caller."
            )

    if strategy == "hyde":
        # Cache per (strategy, id(llm_fn)) — two different callables
        # produce distinct instances so tests can swap providers.
        return _get_or_create(
            f"hyde::{id(llm_fn)}", lambda: HyDERewriter(llm_fn)
        )

    if strategy == "decompose":
        return _get_or_create(
            f"decompose::{id(llm_fn)}", lambda: DecomposeRewriter(llm_fn)
        )

    if strategy == "fusion":
        return _get_or_create(
            f"fusion::{id(llm_fn)}::{fusion_paraphrases}",
            lambda: FusionRewriter(
                llm_fn, fusion_paraphrases=fusion_paraphrases,
            ),
        )

    logger.warning(
        "Unknown query_rewrite strategy %r; falling back to noop",
        strategy,
    )
    return _get_or_create("none", NoopRewriter)


def _bridge_llm_fn(strategy: str, provider, *, board_id, actor_id):
    """Derive the legacy ``llm_fn`` from an R13-A ``LLMProvider`` via the
    matching bridge. Imported lazily to avoid any import cycle and to keep
    the contract port out of this module's import graph until needed.

    The bridge is memoized per ``(provider, board_id, actor_id)``, so
    repeated calls with the same wiring return the SAME callable object —
    preserving the ``id(llm_fn)`` cache identity used by ``_get_or_create``.
    """
    from .llm_provider_bridges import (
        make_decompose_llm_fn,
        make_fusion_llm_fn,
        make_hyde_llm_fn,
    )

    makers = {
        "hyde": make_hyde_llm_fn,
        "decompose": make_decompose_llm_fn,
        "fusion": make_fusion_llm_fn,
    }
    return makers[strategy](provider, board_id=board_id, actor_id=actor_id)


def _get_or_create(key: str, ctor):
    with _cache_lock:
        inst = _cache.get(key)
        if inst is None:
            inst = ctor()
            _cache[key] = inst
        return inst


def reset_rewriter_cache() -> None:
    """Drop all cached rewriter instances. Call in tests or when a
    configuration change requires a fresh instance."""
    with _cache_lock:
        _cache.clear()


__all__ = ["get_rewriter", "reset_rewriter_cache"]
