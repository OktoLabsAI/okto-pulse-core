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
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import KGHitFlushed
from okto_pulse.core.kg.async_bridge import run_async_blocking
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.kg.scoring import _recompute_relevance

logger = logging.getLogger(__name__)


def _recompute_sync(
    board_id: str, node_type: str, node_id: str,
) -> float | None:
    """Open a graph transaction port and recompute one node's relevance score.

    Short-circuits to None when the board has no graph yet (event races with
    bootstrap). The concrete adapter owns the underlying connection lifecycle.
    """
    registry = get_kg_registry()
    if not registry.graph_runtime_store.exists(board_id):
        return None

    async def _run() -> float | None:
        async with await registry.graph_transaction.begin(board_id) as scope:
            return _recompute_relevance(
                scope, board_id, node_type, node_id, trigger="hit_flush",
            )

    return run_async_blocking(_run())


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
