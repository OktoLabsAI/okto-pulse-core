"""Factory for HopPlanner strategies."""

from __future__ import annotations

import logging
import threading
from typing import Callable

from okto_pulse.core.runtime_context import runtime_lock, runtime_state

from .fixed import FixedHopPlanner
from .iterative import IterativeHopPlanner
from .llm import LLMHopPlanner
from .signal import SignalHopPlanner
from .utils import DEFAULT_HOPS

logger = logging.getLogger("okto_pulse.kg.adaptive_hops")

_cache = runtime_state("kg.adaptive_hops.factory_cache", dict)
_cache_lock = runtime_lock("kg.adaptive_hops.factory_cache")


def get_hop_planner(
    strategy: str,
    *,
    llm_fn: Callable | None = None,
    provider=None,
    board_id: str | None = None,
    actor_id: str | None = None,
    fixed_max_hops: int = DEFAULT_HOPS,
    fallback_hops: int = DEFAULT_HOPS,
):
    """Return a HopPlanner for the requested strategy.

    - ``"fixed"`` — FixedHopPlanner with ``fixed_max_hops`` (default
      pattern: caller passes intent.max_hops).
    - ``"llm"`` — LLMHopPlanner backed by ``llm_fn`` (required).
    - ``"signal"`` — SignalHopPlanner (V1 placeholder).
    - ``"iterative"`` — IterativeHopPlanner (V1 placeholder).

    LLM wiring (``"llm"``): pass EITHER a legacy ``llm_fn`` callable
    (takes precedence) OR an R13-A ``provider`` (an ``LLMProvider``) from
    which the bridge callable is derived via ``llm_provider_bridges``. The
    bridge is memoized per ``(provider, board_id, actor_id)`` so the
    derived callable keeps a stable identity and the per-``id(llm_fn)``
    cache below still works. With NEITHER, the factory is fail-closed and
    raises ``ValueError`` — it never silently degrades to ``fixed``. The
    ``provider`` is ignored for fixed/signal/iterative (unchanged).

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
        if llm_fn is None and provider is not None:
            llm_fn = _bridge_hop_llm_fn(
                provider, board_id=board_id, actor_id=actor_id
            )
        if llm_fn is None:
            raise ValueError(
                "Hop planner strategy 'llm' requires an `llm_fn` or "
                "`provider` — the caller must wire a planner callable."
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


def _bridge_hop_llm_fn(provider, *, board_id, actor_id):
    """Derive the legacy hop ``llm_fn`` from an R13-A ``LLMProvider`` via the
    bridge. Imported lazily to avoid any import cycle.

    Memoized per ``(provider, board_id, actor_id)`` inside the bridge, so the
    derived callable keeps a stable ``id`` -> preserves the cache identity
    used by the ``llm::{id(llm_fn)}`` key above.
    """
    from .llm_provider_bridges import make_hop_llm_fn

    return make_hop_llm_fn(provider, board_id=board_id, actor_id=actor_id)


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
