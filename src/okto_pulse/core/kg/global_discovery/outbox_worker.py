"""Outbox worker — polls global_update_outbox and applies events to the
global discovery meta-graph. Retry with dead-letter after 5 failures.

Background asyncio.Task running every 5s. Graceful shutdown via CancelledError.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.kg.global_discovery.schema import open_global_connection
from okto_pulse.core.models.db import GlobalUpdateOutbox, KuzuNodeRef

logger = logging.getLogger("okto_pulse.kg.global_discovery.outbox")

MAX_RETRIES = 5
DEAD_LETTER_SENTINEL = -1

# Node types mirrored into the global discovery layer as DecisionDigest.
# Matches the per-board VECTOR_INDEX_TYPES — only nodes with an HNSW-backed
# embedding are worth digesting for cross-board semantic search.
DIGESTED_NODE_TYPES = ("Decision", "Entity", "Criterion", "Constraint", "Learning")


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

        Upserts the Board summary node, then mirrors every searchable node
        added during the session into DecisionDigest (id, title, summary,
        node_type, embedding) linked via (Board)-[:CONTAINS_DECISION]->.
        Without this mirror, `query_global` has nothing to search over.
        """
        payload = event.payload or {}
        board_id = event.board_id
        session_id = payload.get("session_id", "") or event.session_id
        nodes_added = payload.get("nodes_added", 0)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        # 1) Fetch the per-session kuzu node refs — the authoritative list of
        # what was actually written to the per-board graph. We digest only
        # `add` ops; updates/supersedes don't produce new digest rows (a
        # future pass can refresh digest embeddings on update).
        refs_res = await db.execute(
            select(KuzuNodeRef).where(
                KuzuNodeRef.session_id == session_id,
                KuzuNodeRef.board_id == board_id,
                KuzuNodeRef.operation == "add",
                KuzuNodeRef.kuzu_node_type.in_(DIGESTED_NODE_TYPES),
            )
        )
        refs = list(refs_res.scalars().all())

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
                    {"bid": board_id, "n": nodes_added, "ts": ts},
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
                     "emb": emb, "n": nodes_added, "ts": ts},
                )

            if not refs:
                return

            # 2) Read the just-added nodes back from the per-board Kùzu to
            # pick up the title + embedding computed at consolidation time.
            per_board = self._read_board_nodes_for_refs(board_id, refs)
            if not per_board:
                return

            # 3) Create one DecisionDigest per node + CONTAINS_DECISION edge.
            # Kuzu's HNSW-indexed columns (DecisionDigest.embedding) cannot be
            # mutated via SET after node creation — attempting it raises
            # "Cannot set property vec in table embeddings because it is used
            # in one or more indexes". So we MATCH first; on miss we CREATE
            # with the embedding baked in, on hit we SET only non-indexed
            # columns (title/summary/node_type may legitimately evolve; the
            # embedding is frozen for the life of the digest row).
            for node in per_board:
                digest_id = f"dd_{board_id[:8]}_{node['id']}"
                title = node["title"] or ""
                existing_d = gconn.execute(
                    "MATCH (d:DecisionDigest {id: $did}) RETURN d.id",
                    {"did": digest_id},
                )
                if existing_d.has_next():
                    gconn.execute(
                        "MATCH (d:DecisionDigest {id: $did}) "
                        "SET d.board_id = $bid, d.original_node_id = $oid, "
                        "d.title = $title, d.one_line_summary = $summary, "
                        "d.node_type = $ntype",
                        {
                            "did": digest_id,
                            "bid": board_id,
                            "oid": node["id"],
                            "title": title,
                            "summary": title[:280],
                            "ntype": node["node_type"],
                        },
                    )
                else:
                    gconn.execute(
                        "CREATE (d:DecisionDigest {"
                        "id: $did, board_id: $bid, original_node_id: $oid, "
                        "title: $title, one_line_summary: $summary, "
                        "node_type: $ntype, embedding: $emb, "
                        "created_at: timestamp($ts)})",
                        {
                            "did": digest_id,
                            "bid": board_id,
                            "oid": node["id"],
                            "title": title,
                            "summary": title[:280],
                            "ntype": node["node_type"],
                            "emb": node["embedding"],
                            "ts": ts,
                        },
                    )
                # Idempotent edge: MATCH both endpoints, then MERGE the rel.
                # MERGE on a relationship does not touch indexed node properties.
                gconn.execute(
                    "MATCH (b:Board {board_id: $bid}), "
                    "(d:DecisionDigest {id: $did}) "
                    "MERGE (b)-[:CONTAINS_DECISION]->(d)",
                    {"bid": board_id, "did": digest_id},
                )
        finally:
            del gconn, gdb

    @staticmethod
    def _read_board_nodes_for_refs(board_id: str, refs: list[KuzuNodeRef]) -> list[dict]:
        """Read (id, title, embedding) from the per-board Kùzu for the given
        node refs, bucketed by type so we issue one MATCH per type."""
        from okto_pulse.core.kg.schema import open_board_connection

        by_type: dict[str, list[str]] = {}
        for r in refs:
            by_type.setdefault(r.kuzu_node_type, []).append(r.kuzu_node_id)

        out: list[dict] = []
        try:
            with open_board_connection(board_id) as (_db, conn):
                for ntype, ids in by_type.items():
                    cypher = (
                        f"MATCH (n:{ntype}) WHERE n.id IN $ids "
                        f"AND n.embedding IS NOT NULL "
                        f"RETURN n.id, n.title, n.embedding"
                    )
                    res = conn.execute(cypher, {"ids": ids})
                    while res.has_next():
                        row = res.get_next()
                        out.append({
                            "id": row[0],
                            "title": row[1],
                            "embedding": row[2],
                            "node_type": ntype,
                        })
        except Exception as exc:
            logger.warning(
                "outbox.read_board_failed board=%s err=%s", board_id, exc,
            )
            return []
        return out

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
