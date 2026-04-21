"""RateLimiter Protocol — token bucket contract."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class RateLimiter(Protocol):
    def allow(self, agent_id: str) -> tuple[bool, int]:
        """Return (allowed, retry_after_seconds). retry_after=0 when allowed."""
        ...

    def reset(self, agent_id: str) -> None:
        """Reset the rate limit for an agent — tests only."""
        ...
