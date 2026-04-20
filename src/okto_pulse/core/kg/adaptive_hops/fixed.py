"""FixedHopPlanner — returns a pre-configured hop count.

Default strategy. Preserves the current behaviour where the intent
carries its own ``max_hops``.
"""

from __future__ import annotations

from .interfaces import HopDecision
from .utils import DEFAULT_HOPS, clamp_hops


class FixedHopPlanner:
    name = "fixed"

    def __init__(self, *, fixed_max_hops: int = DEFAULT_HOPS) -> None:
        self._hops = clamp_hops(fixed_max_hops)

    def plan(
        self, *, query: str, intent_name: str, seed_titles: list[str]
    ) -> HopDecision:  # noqa: ARG002 — fixed planner ignores inputs
        return HopDecision(hops=self._hops, reason="fixed")
