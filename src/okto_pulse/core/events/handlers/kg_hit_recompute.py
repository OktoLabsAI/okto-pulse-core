"""KGHitRecomputeHandler — recompute relevance after a hit-flush (Ideação #4).

Reacts to ``kg.hit_flushed`` by invoking ``_recompute_relevance`` on the
flushed node so the refreshed query_hits immediately participate in the
ranking. Decoupling the recompute from the search hot path keeps Cypher
MATCH/COUNT pressure off the read latency budget — see decision
``dec_3a6eb8ad``.

Idempotency: replaying the same event simply recomputes the score with the
same inputs, producing the same result and a no-op SET clause when the
score is unchanged. The dispatcher's natural retry/dead-letter from the
Ideação #1 infra applies — failures don't roll back the underlying flush.

Async-safety: Kùzu v0.6 is synchronous, so we wrap the connection work in
``asyncio.to_thread`` to keep the dispatcher's event loop responsive.
"""

from __future__ import annotations

import asyncio
import gc
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import KGHitFlushed
from okto_pulse.core.kg.schema import board_kuzu_path, open_board_connection
from okto_pulse.core.kg.scoring import _recompute_relevance

logger = logging.getLogger(__name__)


def _recompute_sync(
    board_id: str, node_type: str, node_id: str,
) -> float | None:
    """Open a Kùzu connection and recompute one node's relevance score.

    Short-circuits to None when the board has no Kùzu graph yet (event
    races with bootstrap). Always closes the connection — Kùzu v0.6 holds
    a Windows exclusive lock for the lifetime of the Python handle.
    """
    if not board_kuzu_path(board_id).exists():
        return None
    bc = open_board_connection(board_id)
    try:
        return _recompute_relevance(
            bc.conn, board_id, node_type, node_id, trigger="hit_flush",
        )
    finally:
        bc.close()
        del bc
        gc.collect()


@register_handler("kg.hit_flushed")
class KGHitRecomputeHandler:
    """Trigger a single-node relevance recompute after each hit flush."""

    async def handle(self, event: KGHitFlushed, session: AsyncSession) -> None:
        new_score = await asyncio.to_thread(
            _recompute_sync,
            event.board_id,
            event.node_type,
            event.node_id,
        )
        logger.info(
            "kg.scoring.hit_flush_recompute",
            extra={
                "event": "kg.scoring.hit_flush_recompute",
                "board_id": event.board_id,
                "node_id": event.node_id,
                "node_type": event.node_type,
                "hits_delta": event.hits_delta,
                "new_score": new_score,
            },
        )


__all__ = ["KGHitRecomputeHandler"]
