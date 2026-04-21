"""IterativeHopPlanner — V1 placeholder.

Iterative expansion (hop=1 → evaluate stability → hop=2 → ...) needs a
GraphExpander that can be invoked level-by-level. Current expander
is single-shot, so V1 returns the fallback with reason
"iterative_not_yet_implemented". Full implementation is a documented
follow-up.
"""

from __future__ import annotations

from .interfaces import HopDecision
from .utils import DEFAULT_HOPS, clamp_hops


class IterativeHopPlanner:
    name = "iterative"

    def __init__(self, *, fallback_hops: int = DEFAULT_HOPS) -> None:
        self._fallback = clamp_hops(fallback_hops)

    def plan(
        self, *, query: str, intent_name: str, seed_titles: list[str]
    ) -> HopDecision:  # noqa: ARG002
        return HopDecision(
            hops=self._fallback, reason="iterative_not_yet_implemented"
        )
