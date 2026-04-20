"""Dataclass for hop decisions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HopDecision:
    """Output of a HopPlanner.

    Attributes:
        hops: Depth decided for graph_expander.expand. Always clamped
            to [1, 3] at the factory + caller layers.
        reason: Human-readable label for observability. Examples:
            "fixed", "llm", "llm_error_fallback", "iterative_not_yet_implemented",
            "signal_placeholder".
    """

    hops: int
    reason: str
