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

from .decompose import DecomposeRewriter
from .fusion import FusionRewriter
from .hyde import HyDERewriter
from .noop import NoopRewriter

logger = logging.getLogger("okto_pulse.kg.query_rewrite")

_cache: dict[str, object] = {}
_cache_lock = threading.Lock()


def get_rewriter(
    strategy: str,
    *,
    llm_fn: Callable | None = None,
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

    Unknown strategies fall back to NoopRewriter with a warning so a
    typo in configuration never breaks the retrieval pipeline.
    """
    strategy = (strategy or "none").strip().lower()

    if strategy == "none":
        return _get_or_create("none", NoopRewriter)

    if strategy in ("hyde", "decompose", "fusion") and llm_fn is None:
        raise ValueError(
            f"Rewriter strategy {strategy!r} requires an `llm_fn` — "
            f"the project's LLM provider must be wired by the caller."
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
