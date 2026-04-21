"""Helpers shared across hop planners."""

from __future__ import annotations

#: Global ceiling — no planner (including future strategies) may
#: return more than 3 hops. Anti-explosion guard: the graph grows
#: combinatorially with hop count on dense boards.
MAX_HOPS_CEILING = 3

#: Floor: below 1, the expand stage would be meaningless.
MIN_HOPS_FLOOR = 1

#: Default used when the LLM returns an invalid value we can't coerce.
DEFAULT_HOPS = 2


def clamp_hops(value: int | None) -> int:
    """Clamp ``value`` to the valid hop range.

    - None / non-int / non-numeric → returns ``DEFAULT_HOPS``.
    - Below floor → floor.
    - Above ceiling → ceiling.
    """
    try:
        v = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return DEFAULT_HOPS
    if v < MIN_HOPS_FLOOR:
        return MIN_HOPS_FLOOR
    if v > MAX_HOPS_CEILING:
        return MAX_HOPS_CEILING
    return v
