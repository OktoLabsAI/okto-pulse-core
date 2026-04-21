"""Background cleanup worker for expired consolidation sessions.

Runs inside the FastAPI lifespan — a single asyncio.Task that wakes every
`kg_cleanup_interval_seconds` and calls `SessionManager.sweep_expired()`.
Graceful shutdown is handled via `asyncio.CancelledError`: the task catches
it, finishes its current iteration, and exits.

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

import asyncio
import logging
from datetime import datetime, timezone

from okto_pulse.core.kg.session_manager import get_session_manager

logger = logging.getLogger("okto_pulse.kg.cleanup")


class SessionCleanupWorker:
    """Async task that periodically sweeps expired sessions from the in-memory
    SessionManager. Safe to start/stop multiple times; idempotent."""

    def __init__(self, interval_seconds: int = 60):
        self.interval_seconds = interval_seconds
        self._task: asyncio.Task | None = None
        self._running: bool = False

    @property
    def is_running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    async def start(self) -> None:
        """Start the background sweep loop. No-op if already running."""
        if self.is_running:
            return
        self._running = True
        self._task = asyncio.create_task(
            self._run_loop(), name="kg.cleanup_worker"
        )
        logger.info(
            "kg.cleanup.started interval=%ds", self.interval_seconds,
            extra={"event": "kg.cleanup.started",
                   "interval_seconds": self.interval_seconds},
        )

    async def stop(self, timeout: float = 5.0) -> None:
        """Cancel the sweep loop and wait for it to finish cleanly."""
        if not self.is_running:
            self._running = False
            return
        self._running = False
        assert self._task is not None
        self._task.cancel()
        try:
            await asyncio.wait_for(self._task, timeout=timeout)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        self._task = None
        logger.info(
            "kg.cleanup.stopped",
            extra={"event": "kg.cleanup.stopped"},
        )

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
                "swept_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        return expired

    async def _run_loop(self) -> None:
        try:
            while self._running:
                try:
                    await self.sweep_once()
                except Exception as exc:
                    logger.error(
                        "kg.cleanup.sweep_failed err=%s", exc,
                        extra={"event": "kg.cleanup.sweep_failed",
                               "error": str(exc)},
                    )
                await asyncio.sleep(self.interval_seconds)
        except asyncio.CancelledError:
            # Graceful shutdown path — run one final sweep so no stragglers
            # are left behind when the app stops.
            try:
                await self.sweep_once()
            except Exception:
                pass
            raise


_singleton: SessionCleanupWorker | None = None


def get_cleanup_worker() -> SessionCleanupWorker:
    """Return the process-wide cleanup worker, lazy-init from settings."""
    global _singleton
    if _singleton is None:
        from okto_pulse.core.infra.config import get_settings

        _singleton = SessionCleanupWorker(
            interval_seconds=get_settings().kg_cleanup_interval_seconds
        )
    return _singleton


def reset_cleanup_worker_for_tests() -> None:
    """Drop the cached worker — tests only."""
    global _singleton
    _singleton = None
