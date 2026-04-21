"""Factory for HopPlanner strategies."""

from __future__ import annotations

import logging
import threading
from typing import Callable

from .fixed import FixedHopPlanner
from .interfaces import HopDecision
from .iterative import IterativeHopPlanner
from .llm import LLMHopPlanner
from .signal import SignalHopPlanner
from .utils import DEFAULT_HOPS

logger = logging.getLogger("okto_pulse.kg.adaptive_hops")

_cache: dict[str, object] = {}
_cache_lock = threading.Lock()


def get_hop_planner(
    strategy: str,
    *,
    llm_fn: Callable | None = None,
    fixed_max_hops: int = DEFAULT_HOPS,
    fallback_hops: int = DEFAULT_HOPS,
):
    """Return a HopPlanner for the requested strategy.

    - ``"fixed"`` — FixedHopPlanner with ``fixed_max_hops`` (default
      pattern: caller passes intent.max_hops).
    - ``"llm"`` — LLMHopPlanner backed by ``llm_fn`` (required).
    - ``"signal"`` — SignalHopPlanner (V1 placeholder).
    - ``"iterative"`` — IterativeHopPlanner (V1 placeholder).

    Unknown strategies fall back to FixedHopPlanner with a warning so
    a typo never breaks the retrieval pipeline.
    """
    strategy = (strategy or "fixed").strip().lower()

    if strategy == "fixed":
        return _get_or_create(
            f"fixed::{fixed_max_hops}",
            lambda: FixedHopPlanner(fixed_max_hops=fixed_max_hops),
        )

    if strategy == "llm":
        if llm_fn is None:
            raise ValueError(
                "Hop planner strategy 'llm' requires an `llm_fn` — "
                "the caller must wire a planner callable."
            )
        key = f"llm::{id(llm_fn)}::{fallback_hops}"
        return _get_or_create(
            key,
            lambda: LLMHopPlanner(llm_fn, fallback_hops=fallback_hops),
        )

    if strategy == "signal":
        return _get_or_create(
            f"signal::{fallback_hops}",
            lambda: SignalHopPlanner(fallback_hops=fallback_hops),
        )

    if strategy == "iterative":
        return _get_or_create(
            f"iterative::{fallback_hops}",
            lambda: IterativeHopPlanner(fallback_hops=fallback_hops),
        )

    logger.warning(
        "Unknown hop_strategy %r; falling back to FixedHopPlanner", strategy
    )
    return _get_or_create(
        f"fixed::{fixed_max_hops}",
        lambda: FixedHopPlanner(fixed_max_hops=fixed_max_hops),
    )


def _get_or_create(key: str, ctor):
    with _cache_lock:
        inst = _cache.get(key)
        if inst is None:
            inst = ctor()
            _cache[key] = inst
        return inst


def reset_planner_cache() -> None:
    """Drop all cached planner instances. Call in tests."""
    with _cache_lock:
        _cache.clear()


__all__ = ["get_hop_planner", "reset_planner_cache"]
