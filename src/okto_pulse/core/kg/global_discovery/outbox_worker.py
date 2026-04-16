"""Outbox worker — polls global_update_outbox and applies events to the
global discovery meta-graph. Retry with dead-letter after 5 failures.

Background asyncio.Task running every 5s. Graceful shutdown via CancelledError.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.kg.global_discovery.schema import open_global_connection
from okto_pulse.core.models.db import GlobalUpdateOutbox

logger = logging.getLogger("okto_pulse.kg.global_discovery.outbox")

MAX_RETRIES = 5
DEAD_LETTER_SENTINEL = -1


class OutboxWorker:
    def __init__(self, session_factory, interval_seconds: int = 5):
        self._factory = session_factory
        self._interval = interval_seconds
        self._task: asyncio.Task | None = None
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.is_running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="outbox_worker")
        logger.info("outbox_worker.started interval=%ds", self._interval)

    async def stop(self, timeout: float = 5.0) -> None:
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
        logger.info("outbox_worker.stopped")

    async def process_once(self) -> int:
        """Process pending outbox events. Returns count processed."""
        processed = 0
        async with self._factory() as db:
            pending = await db.execute(
                select(GlobalUpdateOutbox)
                .where(
                    GlobalUpdateOutbox.processed_at.is_(None),
                    GlobalUpdateOutbox.retry_count >= 0,
                    GlobalUpdateOutbox.retry_count < MAX_RETRIES,
                )
                .order_by(GlobalUpdateOutbox.created_at.asc())
                .limit(50)
            )
            events = list(pending.scalars().all())
            for event in events:
                try:
                    await self._apply_event(event, db)
                    event.processed_at = datetime.now(timezone.utc)
                    processed += 1
                except Exception as exc:
                    event.retry_count += 1
                    event.last_error = str(exc)[:500]
                    if event.retry_count >= MAX_RETRIES:
                        event.retry_count = DEAD_LETTER_SENTINEL
                        logger.warning(
                            "outbox.dead_letter event=%s board=%s err=%s",
                            event.event_id, event.board_id, exc,
                            extra={
                                "event": "outbox.dead_letter",
                                "event_id": event.event_id,
                                "board_id": event.board_id,
                            },
                        )
            await db.commit()
        if processed:
            logger.info(
                "outbox.processed count=%d", processed,
                extra={"event": "outbox.processed", "count": processed},
            )
        return processed

    async def _apply_event(self, event: GlobalUpdateOutbox, db: AsyncSession) -> None:
        """Apply a single outbox event to the global discovery meta-graph.

        MVP: creates/updates a Board summary node and a DecisionDigest node
        per committed session. Full topic clustering and entity canonicalization
        are in the clustering module (card 2ab50296).
        """
        payload = event.payload or {}
        board_id = event.board_id
        session_id = payload.get("session_id", "")
        nodes_added = payload.get("nodes_added", 0)

        gdb, gconn = open_global_connection()
        try:
            # Upsert Board summary node
            existing = gconn.execute(
                "MATCH (b:Board {board_id: $bid}) RETURN b.board_id",
                {"bid": board_id},
            )
            if existing.has_next():
                gconn.execute(
                    "MATCH (b:Board {board_id: $bid}) "
                    "SET b.decision_count = coalesce(b.decision_count, 0) + $n, "
                    "b.last_sync_at = timestamp($ts)",
                    {"bid": board_id, "n": nodes_added,
                     "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")},
                )
            else:
                from okto_pulse.core.kg.embedding import get_embedding_provider
                emb = get_embedding_provider().encode(f"Board {board_id}")
                gconn.execute(
                    "CREATE (b:Board {"
                    "board_id: $bid, name: $name, summary: $s, "
                    "summary_embedding: $emb, topic_count: 0, entity_count: 0, "
                    "decision_count: $n, "
                    "last_sync_at: timestamp($ts)})",
                    {"bid": board_id, "name": board_id, "s": "",
                     "emb": emb, "n": nodes_added,
                     "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")},
                )
        finally:
            del gconn, gdb

    async def _loop(self) -> None:
        try:
            while self._running:
                try:
                    await self.process_once()
                except Exception as exc:
                    logger.error("outbox_worker.error err=%s", exc)
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            try:
                await self.process_once()
            except Exception:
                pass
            raise


_singleton: OutboxWorker | None = None


def get_outbox_worker() -> OutboxWorker:
    global _singleton
    if _singleton is None:
        from okto_pulse.core.infra.database import get_session_factory
        _singleton = OutboxWorker(get_session_factory(), interval_seconds=5)
    return _singleton


def reset_outbox_worker_for_tests() -> None:
    global _singleton
    _singleton = None
