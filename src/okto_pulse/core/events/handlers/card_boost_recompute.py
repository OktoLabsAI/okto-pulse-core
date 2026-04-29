"""CardBoostRecomputeHandler — recompute priority_boost on a card's KG entity.

Reacts to ``card.priority_changed`` and ``card.severity_changed`` (Ideação
#4, IMPL-C). For each event:
1. Fetch the card to read the canonical priority/severity (event payload
   may be stale if multiple updates arrived in flight).
2. Resolve the new boost via MAX(priority_boost, severity_boost) for bugs
   or priority-only for other types (mirrors the consolidation worker).
3. Persist the new ``priority_boost`` column on the root entity node.
4. Trigger ``_recompute_relevance(trigger="boost_change")`` so the score
   reflects the new bound.
5. Emit structured log ``kg.scoring.boost_changed``.
6. When ``|delta_boost| > 0.05`` create a Decision audit node in the KG —
   replaces a SQL audit table by reusing the KG's native semantic.

Idempotency: re-applying the same event computes the same boost and the
delta-vs-cap check is naturally idempotent. Decision nodes carry a stable
content fingerprint so re-emission only adds a new node when the actual
transition changes.
"""

from __future__ import annotations

import asyncio
import gc
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import (
    CardPriorityChanged,
    CardSeverityChanged,
)
from okto_pulse.core.kg.schema import board_kuzu_path, open_board_connection
from okto_pulse.core.kg.scoring import (
    _recompute_relevance,
    _resolve_priority_boost,
    _resolve_severity_boost,
)
from okto_pulse.core.models.db import Card

logger = logging.getLogger(__name__)


# Smallest delta between adjacent priority levels in PRIORITY_BOOST_BY_LEVEL
# (medium=0.05 vs none/low=0.0). Decision audit only fires for changes that
# cross this threshold so numeric noise from refresh-only no-ops is ignored.
DECISION_AUDIT_DELTA = 0.05


def _root_entity_id(card_id: str) -> str:
    """Mirror ``deterministic_worker.process_card``'s root entity ID format.

    Worker builds ``f"card_{cid[:8]}_entity"`` — keeping that format here as
    a constant (not an import) avoids creating a runtime dep on the worker
    module from the event-handler layer.
    """
    return f"card_{card_id[:8]}_entity"


def _resolve_node_type(card_type_value: Optional[str]) -> str:
    return "Bug" if card_type_value == "bug" else "Entity"


def _fetch_priority_boost(conn, node_type: str, node_id: str) -> float:
    """Read the persisted priority_boost. Returns 0.0 when missing/null."""
    res = None
    try:
        res = conn.execute(
            f"MATCH (n:{node_type} {{id: $nid}}) RETURN n.priority_boost",
            {"nid": node_id},
        )
        if not res.has_next():
            return 0.0
        row = res.get_next()
        value = row[0]
        return float(value) if value is not None else 0.0
    except Exception as exc:
        logger.warning(
            "kg.scoring.boost_fetch_failed node=%s err=%s",
            node_id, exc,
        )
        return 0.0
    finally:
        if res is not None:
            try:
                res.close()
            except Exception:
                pass


def _persist_priority_boost(conn, node_type: str, node_id: str, boost: float) -> None:
    """Update the priority_boost column. Best-effort with structured log."""
    try:
        conn.execute(
            f"MATCH (n:{node_type} {{id: $nid}}) SET n.priority_boost = $boost",
            {"nid": node_id, "boost": boost},
        )
    except Exception as exc:
        logger.error(
            "kg.scoring.boost_persist_failed node=%s err=%s",
            node_id, exc,
        )


def _emit_boost_decision_node(
    conn,
    *,
    board_id: str,
    card_id: str,
    spec_id: Optional[str],
    node_type: str,
    root_node_id: str,
    old_boost: float,
    new_boost: float,
    trigger_event_type: str,
    changed_by: Optional[str],
) -> None:
    """Insert a Decision audit node + relates_to edge to the root entity.

    Decision is the KG's native vocabulary for "context-anchored choice"; we
    leverage it to record significant priority_boost recalibrations rather
    than introduce a SQL audit table — see dec_cb956457.
    """
    decision_id = (
        f"dec_boost_{card_id[:8]}_{int(datetime.now(timezone.utc).timestamp())}"
    )
    title = "priority_boost recalibrated"
    delta = new_boost - old_boost
    content = (
        f"Card {card_id} priority_boost transitioned from {old_boost:.2f} "
        f"to {new_boost:.2f} (delta={delta:+.2f}) following "
        f"{trigger_event_type}. Source: spec={spec_id or '-'} "
        f"changed_by={changed_by or '-'}."
    )
    now_iso = datetime.now(timezone.utc).isoformat()
    artifact_ref = f"card:{card_id}"
    try:
        conn.execute(
            "CREATE (d:Decision {"
            "id: $id, title: $title, content: $content, "
            "context: $context, justification: $justification, "
            "source_artifact_ref: $artifact_ref, "
            "source_session_id: $session_id, "
            "created_at: timestamp($now), created_by_agent: $agent, "
            "source_confidence: 1.0, relevance_score: 0.5, "
            "query_hits: 0, last_queried_at: NULL, "
            "last_recomputed_at: $now, priority_boost: 0.0, "
            "human_curated: FALSE"
            "})",
            {
                "id": decision_id,
                "title": title,
                "content": content,
                "context": f"Trigger: {trigger_event_type}",
                "justification": "delta exceeds DECISION_AUDIT_DELTA threshold",
                "artifact_ref": artifact_ref,
                "session_id": f"boost-recompute-{card_id}",
                "now": now_iso,
                "agent": "card_boost_recompute_handler",
            },
        )
    except Exception as exc:
        logger.warning(
            "kg.scoring.decision_node_failed card=%s err=%s",
            card_id, exc,
        )
        return

    # Edge Decision -[:relates_to]-> root entity. Multi-pair belongs_to is
    # for parent hierarchy; relates_to is the right rel for context anchor.
    try:
        conn.execute(
            "MATCH (d:Decision {id: $did}), "
            f"(n:{node_type} {{id: $nid}}) "
            "CREATE (d)-[:relates_to {confidence: 1.0, "
            "created_by_session_id: $session_id, "
            "created_at: timestamp($now), layer: 'deterministic', "
            "rule_id: 'boost_audit', created_by: 'card_boost_handler'}]->(n)",
            {
                "did": decision_id,
                "nid": root_node_id,
                "session_id": f"boost-recompute-{card_id}",
                "now": now_iso,
            },
        )
    except Exception as exc:
        logger.warning(
            "kg.scoring.decision_edge_failed card=%s err=%s",
            card_id, exc,
        )


def _recompute_boost_sync(
    *,
    board_id: str,
    card_id: str,
    spec_id: Optional[str],
    card_type_value: Optional[str],
    new_priority_value: Optional[str],
    new_severity_value: Optional[str],
    trigger_event_type: str,
    changed_by: Optional[str],
) -> tuple[float, float]:
    """Open a Kùzu connection, recompute boost + relevance, audit if needed.

    Returns ``(old_boost, new_boost)``. Short-circuits to (0.0, 0.0) when
    the board has no Kùzu graph yet (event arrived before bootstrap).
    """
    if not board_kuzu_path(board_id).exists():
        return (0.0, 0.0)

    new_boost = _resolve_priority_boost(new_priority_value)
    if card_type_value == "bug":
        new_boost = max(
            new_boost, _resolve_severity_boost(new_severity_value),
        )

    node_type = _resolve_node_type(card_type_value)
    root_node_id = _root_entity_id(card_id)

    bc = open_board_connection(board_id)
    try:
        old_boost = _fetch_priority_boost(bc.conn, node_type, root_node_id)
        _persist_priority_boost(bc.conn, node_type, root_node_id, new_boost)
        _recompute_relevance(
            bc.conn, board_id, node_type, root_node_id,
            trigger="boost_change",
        )
        delta = new_boost - old_boost
        logger.info(
            "kg.scoring.boost_changed",
            extra={
                "event": "kg.scoring.boost_changed",
                "board_id": board_id,
                "card_id": card_id,
                "node_type": node_type,
                "node_id": root_node_id,
                "old_boost": old_boost,
                "new_boost": new_boost,
                "delta": delta,
                "trigger_event": trigger_event_type,
            },
        )
        if abs(delta) > DECISION_AUDIT_DELTA:
            _emit_boost_decision_node(
                bc.conn,
                board_id=board_id,
                card_id=card_id,
                spec_id=spec_id,
                node_type=node_type,
                root_node_id=root_node_id,
                old_boost=old_boost,
                new_boost=new_boost,
                trigger_event_type=trigger_event_type,
                changed_by=changed_by,
            )
        return (old_boost, new_boost)
    finally:
        bc.close()
        del bc
        gc.collect()


async def _handle_boost_event(
    event: CardPriorityChanged | CardSeverityChanged,
    session: AsyncSession,
    *,
    trigger_event_type: str,
) -> None:
    """Shared handler body for both priority and severity events."""
    card = await session.get(Card, event.card_id)
    if card is None:
        logger.warning(
            "kg.scoring.boost_handler_card_missing card=%s board=%s",
            event.card_id, event.board_id,
        )
        return

    new_priority_value = (
        card.priority.value if card.priority is not None else None
    )
    new_severity_value = (
        card.severity.value if card.severity is not None else None
    )
    card_type_value = (
        card.card_type.value if card.card_type is not None else None
    )

    await asyncio.to_thread(
        _recompute_boost_sync,
        board_id=event.board_id,
        card_id=event.card_id,
        spec_id=event.spec_id,
        card_type_value=card_type_value,
        new_priority_value=new_priority_value,
        new_severity_value=new_severity_value,
        trigger_event_type=trigger_event_type,
        changed_by=event.changed_by,
    )


@register_handler("card.priority_changed")
class CardPriorityChangedHandler:
    async def handle(
        self, event: CardPriorityChanged, session: AsyncSession,
    ) -> None:
        await _handle_boost_event(
            event, session, trigger_event_type="card.priority_changed",
        )


@register_handler("card.severity_changed")
class CardSeverityChangedHandler:
    async def handle(
        self, event: CardSeverityChanged, session: AsyncSession,
    ) -> None:
        await _handle_boost_event(
            event, session, trigger_event_type="card.severity_changed",
        )


__all__ = [
    "CardPriorityChangedHandler",
    "CardSeverityChangedHandler",
    "DECISION_AUDIT_DELTA",
]
