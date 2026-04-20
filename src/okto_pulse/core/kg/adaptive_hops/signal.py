"""SignalHopPlanner — V1 placeholder.

The full density-gain signal requires a GraphExpander that can report
incrementally per level; current expander executes a single expand
call with fixed max_hops. V1 returns a conservative default with a
clear reason so observability tells the caller the placeholder was
hit. Full implementation is a follow-up (requires expander refactor).
"""

from __future__ import annotations

from .interfaces import HopDecision
from .utils import DEFAULT_HOPS, clamp_hops


class SignalHopPlanner:
    name = "signal"

    def __init__(self, *, fallback_hops: int = DEFAULT_HOPS) -> None:
        self._fallback = clamp_hops(fallback_hops)

    def plan(
        self, *, query: str, intent_name: str, seed_titles: list[str]
    ) -> HopDecision:  # noqa: ARG002
        return HopDecision(hops=self._fallback, reason="signal_placeholder")
