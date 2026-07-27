"""Application processor for expired consolidation sessions.

The Community edition owns scheduling, sleep, cancellation and final-sweep
behavior. Core owns only the operation and its observable result.

Structured logger fields emitted per sweep:
  event: "kg.cleanup.sweep"
  expired_count: int
  active_count: int
  interval_seconds: int

Per abort/eviction:
  event: "kg.cleanup.session_expired"
  session_id, board_id, agent_id, age_seconds
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from okto_pulse.core.kg.session_manager import get_session_manager
from okto_pulse.core.ports.runtime_workers import WorkerClockPort

logger = logging.getLogger("okto_pulse.kg.cleanup")


class SessionCleanupProcessor:
    """Perform one deterministic cleanup iteration."""

    def __init__(
        self,
        interval_seconds: int = 60,
        *,
        clock: WorkerClockPort | None = None,
    ):
        self.interval_seconds = interval_seconds
        self._clock = clock

    async def sweep_once(self) -> int:
        """Single sweep — useful in tests. Returns count of expired sessions."""
        mgr = get_session_manager()
        expired = await mgr.sweep_expired()
        active = await mgr.active_count()
        logger.info(
            "kg.cleanup.sweep expired=%d active=%d", expired, active,
            extra={
                "event": "kg.cleanup.sweep",
                "expired_count": expired,
                "active_count": active,
                "interval_seconds": self.interval_seconds,
                "swept_at": (
                    self._clock.now() if self._clock is not None
                    else datetime.now(timezone.utc)
                ).isoformat(),
            },
        )
        return expired

    async def process_once(self) -> int:
        return await self.sweep_once()


__all__ = ["SessionCleanupProcessor"]
