"""InMemoryTokenBucket — satisfies RateLimiter Protocol.

Refactored from okto_pulse.core.kg.tier_power._TokenBucket.
30 tokens per 60s window per agent.
"""

from __future__ import annotations

import time


class InMemoryTokenBucket:
    def __init__(self, rate: int = 30, window: float = 60.0):
        self._rate = rate
        self._window = window
        self._tokens: dict[str, list[float]] = {}

    def allow(self, agent_id: str) -> tuple[bool, int]:
        now = time.monotonic()
        times = self._tokens.setdefault(agent_id, [])
        cutoff = now - self._window
        self._tokens[agent_id] = [t for t in times if t > cutoff]
        times = self._tokens[agent_id]
        if len(times) >= self._rate:
            oldest = times[0]
            retry_after = int(self._window - (now - oldest)) + 1
            return False, max(1, retry_after)
        times.append(now)
        return True, 0

    def reset(self, agent_id: str) -> None:
        self._tokens.pop(agent_id, None)
