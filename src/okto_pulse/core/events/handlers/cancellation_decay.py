"""CancellationDecayHandler / CancellationRestoreHandler — KG integrity pack Fase 1.

Reage a card.cancelled aplicando decay no relevance_score dos nodes do KG
derivados do card cancelado; reage a card.restored revertendo o decay
(apenas em nodes marcados com revocation_reason='source_cancelled' para não
colidir com supersedence futura de outras causas).

Idempotência: a condição WHERE do decay exclui nodes que já têm
revocation_reason='source_cancelled'. Assim, card.cancelled duplicado
(retry do dispatcher, emissão em dupla) não re-aplica penalty.

Isolation: handlers rodam em transação SQL própria pelo dispatcher — falha
aqui não afeta o ConsolidationEnqueuer que também observa os mesmos eventos.

Async-safety: o driver Kùzu v0.6 é síncrono; envelopamos conn.execute em
asyncio.to_thread para não bloquear o event loop do dispatcher.
"""

from __future__ import annotations

import asyncio
import gc
import logging
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import CardCancelled, CardRestored
from okto_pulse.core.kg.schema import (
    NODE_TYPES,
    board_kuzu_path,
    open_board_connection,
)

logger = logging.getLogger(__name__)


DECAY_PENALTY = 0.5
REVOCATION_REASON = "source_cancelled"


def _apply_decay_sync(board_id: str, card_id: str) -> int:
    """Apply decay synchronously inside a Kùzu connection. Returns nodes affected.

    Runs one UPDATE per node type. Kùzu v0.6 has no polymorphic MATCH, so
    iterating NODE_TYPES is the portable way. Skip rows that already carry
    the decay marker (idempotency). Explicit close + gc on exit — Kùzu v0.6
    holds a Windows exclusive lock for the lifetime of the Python handle.

    Short-circuits to 0 if the board has no Kùzu graph yet (card cancelled
    before it was ever consolidated). Avoids the ~1-2s bootstrap cost and
    keeps handler latency negligible for that common case.
    """
    if not board_kuzu_path(board_id).exists():
        return 0

    ref = f"card:{card_id}"
    now = datetime.now(timezone.utc)
    total = 0
    bc = open_board_connection(board_id)
    try:
        for node_type in NODE_TYPES:
            cypher = (
                f"MATCH (n:{node_type}) "
                "WHERE n.source_artifact_ref = $ref "
                "  AND (n.revocation_reason IS NULL "
                "       OR n.revocation_reason <> $reason) "
                "SET n.relevance_score = "
                "      CASE WHEN n.relevance_score - $penalty < 0.0 "
                "           THEN 0.0 "
                "           ELSE n.relevance_score - $penalty END, "
                "    n.revocation_reason = $reason, "
                "    n.superseded_at = $now "
                "RETURN n.id"
            )
            result = bc.conn.execute(
                cypher,
                {
                    "ref": ref,
                    "reason": REVOCATION_REASON,
                    "penalty": DECAY_PENALTY,
                    "now": now,
                },
            )
            while result.has_next():
                result.get_next()
                total += 1
    finally:
        bc.close()
        del bc
        gc.collect()
    return total


def _revert_decay_sync(board_id: str, card_id: str) -> int:
    """Reverse the decay for nodes previously marked by this handler.

    Restores only nodes whose revocation_reason matches this module's marker
    so that future supersedence causes ('auto_superseded', 'source_deleted')
    stay untouched when a card is restored. Short-circuits when the board has
    no Kùzu graph yet.
    """
    if not board_kuzu_path(board_id).exists():
        return 0

    ref = f"card:{card_id}"
    total = 0
    bc = open_board_connection(board_id)
    try:
        for node_type in NODE_TYPES:
            cypher = (
                f"MATCH (n:{node_type}) "
                "WHERE n.source_artifact_ref = $ref "
                "  AND n.revocation_reason = $reason "
                "SET n.relevance_score = n.relevance_score + $penalty, "
                "    n.revocation_reason = NULL, "
                "    n.superseded_at = NULL "
                "RETURN n.id"
            )
            result = bc.conn.execute(
                cypher,
                {
                    "ref": ref,
                    "reason": REVOCATION_REASON,
                    "penalty": DECAY_PENALTY,
                },
            )
            while result.has_next():
                result.get_next()
                total += 1
    finally:
        bc.close()
        del bc
        gc.collect()
    return total


@register_handler("card.cancelled")
class CancellationDecayHandler:
    """Apply decay penalty to KG nodes derived from a cancelled card."""

    async def handle(self, event: CardCancelled, session: AsyncSession) -> None:
        # `session` is the SQL async session provided by the dispatcher; we
        # do NOT write SQL state here — all mutation is on the Kùzu graph.
        nodes_affected = await asyncio.to_thread(
            _apply_decay_sync, event.board_id, event.card_id
        )
        logger.info(
            "kg.cancellation_decay.applied",
            extra={
                "event": "kg.cancellation_decay.applied",
                "card_id": event.card_id,
                "board_id": event.board_id,
                "nodes_affected": nodes_affected,
                "decay_penalty": DECAY_PENALTY,
            },
        )


@register_handler("card.restored")
class CancellationRestoreHandler:
    """Revert the decay penalty when a cancelled card is restored."""

    async def handle(self, event: CardRestored, session: AsyncSession) -> None:
        nodes_affected = await asyncio.to_thread(
            _revert_decay_sync, event.board_id, event.card_id
        )
        logger.info(
            "kg.cancellation_decay.reverted",
            extra={
                "event": "kg.cancellation_decay.reverted",
                "card_id": event.card_id,
                "board_id": event.board_id,
                "nodes_affected": nodes_affected,
            },
        )


__all__ = [
    "CancellationDecayHandler",
    "CancellationRestoreHandler",
    "DECAY_PENALTY",
    "REVOCATION_REASON",
]
