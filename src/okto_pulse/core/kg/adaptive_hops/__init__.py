"""Adaptive hop count for the hybrid retrieve (ideação 1fb13b51).

Usage::

    from okto_pulse.core.kg.adaptive_hops import get_hop_planner

    planner = get_hop_planner("llm", llm_fn=my_planner_fn)
    decision = planner.plan(
        query=query,
        intent_name="contradiction",
        seed_titles=[s.title for s in seeds],
    )
    effective_hops = min(decision.hops, 3)   # ceiling hard
"""

from .factory import get_hop_planner, reset_planner_cache
from .fixed import FixedHopPlanner
from .interfaces import HopDecision
from .iterative import IterativeHopPlanner
from .llm import LLMHopPlanner
from .signal import SignalHopPlanner
from .utils import clamp_hops

__all__ = [
    "FixedHopPlanner",
    "HopDecision",
    "IterativeHopPlanner",
    "LLMHopPlanner",
    "SignalHopPlanner",
    "clamp_hops",
    "get_hop_planner",
    "reset_planner_cache",
]
