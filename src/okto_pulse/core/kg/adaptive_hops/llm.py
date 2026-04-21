"""LLMHopPlanner — LLM decides max_hops at runtime.

Delegates the decision to an injected callable with the shape
``Callable[[str, str, list[str]], int]`` (query, intent_name,
seed_titles → hops 1-3). Exceptions fall back to a fixed hop count
with reason="llm_error_fallback".
"""

from __future__ import annotations

import hashlib
import logging
from functools import lru_cache
from typing import Callable

from .interfaces import HopDecision
from .utils import DEFAULT_HOPS, clamp_hops

logger = logging.getLogger("okto_pulse.kg.adaptive_hops.llm")

LLMHopFn = Callable[[str, str, list[str]], int]


class LLMHopPlanner:
    """Runtime hop planner backed by an injected LLM callable."""

    name = "llm"

    def __init__(
        self, llm_fn: LLMHopFn, *, fallback_hops: int = DEFAULT_HOPS
    ) -> None:
        self._llm = llm_fn
        self._fallback = clamp_hops(fallback_hops)

    def plan(
        self, *, query: str, intent_name: str, seed_titles: list[str]
    ) -> HopDecision:
        # Ignore seed_titles in the cache key — titles change less
        # than the query text and caching by query+intent is enough
        # for the "same question asked twice" case.
        return self._compute(query, intent_name)

    @lru_cache(maxsize=128)
    def _compute(self, query: str, intent_name: str) -> HopDecision:
        try:
            raw = self._llm(query, intent_name, [])
        except Exception as e:  # noqa: BLE001 — never propagate LLM failures
            logger.warning(
                "llm_hop_planner.llm_error error=%s query_hash=%s",
                type(e).__name__,
                hashlib.sha256(query.encode("utf-8")).hexdigest()[:16],
            )
            return HopDecision(
                hops=self._fallback, reason="llm_error_fallback"
            )
        return HopDecision(hops=clamp_hops(raw), reason="llm")
