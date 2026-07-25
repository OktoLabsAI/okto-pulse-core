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

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import (
    CardPriorityChanged,
    CardSeverityChanged,
)
from okto_pulse.core.kg.async_bridge import run_async_blocking
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.guarded_write import guarded_board_write
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.kg.node_identity import derive_natural_key, mint_node_id
from okto_pulse.core.kg.scoring import (
    _recompute_relevance,
    _resolve_priority_boost,
    _resolve_severity_boost,
)
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_WORKING,
    MATURITY_WORKING_IMMATURE,
)
from okto_pulse.core.ports.domain_event_delivery import (
    get_domain_event_fact_reader,
)

logger = logging.getLogger(__name__)


# Smallest delta between adjacent priority levels in PRIORITY_BOOST_BY_LEVEL
# (medium=0.05 vs none/low=0.0). Decision audit only fires for changes that
# cross this threshold so numeric noise from refresh-only no-ops is ignored.
DECISION_AUDIT_DELTA = 0.05


def _root_entity_id(board_id: str, card_id: str, node_type: str) -> str:
    """Return the persisted deterministic identity for a card root node.

    ``process_card`` emits a session-local candidate id such as
    ``card_<short>_entity``.  The graph commit never persists that candidate
    id: it mints a content-addressed node id from board, type and source ref.
    Reusing the canonical identity policy here prevents silent MATCH misses.
    """
    source_ref = f"card:{card_id}"
    return mint_node_id(
        board_id,
        node_type,
        derive_natural_key(source_ref, node_type, None),
        0,
    )


def _resolve_root_node(
    conn,
    *,
    board_id: str,
    card_id: str,
    node_type: str,
) -> tuple[str, str, str] | None:
    """Resolve the active root by provenance, including legacy node ids."""
    source_ref = f"card:{card_id}"
    try:
        result = conn.execute(
            f"MATCH (n:{node_type}) "
            "WHERE n.source_artifact_ref = $source_ref "
            "AND n.superseded_by IS NULL "
            "AND (n.revocation_reason IS NULL "
            "OR n.revocation_reason <> 'source_deleted') "
            "AND (n.maturity_status IS NULL "
            "OR n.maturity_status <> 'working_stale') "
            "RETURN n.id, n.graph_layer, n.maturity_status, n.generation",
            {"source_ref": source_ref},
        )
    except Exception as exc:
        logger.warning(
            "kg.scoring.root_resolve_failed card=%s type=%s err=%s",
            card_id,
            node_type,
            exc,
        )
        return None

    if not result.rows:
        logger.warning(
            "kg.scoring.root_missing card=%s type=%s expected_id=%s",
            card_id,
            node_type,
            _root_entity_id(board_id, card_id, node_type),
        )
        return None

    expected_id = _root_entity_id(board_id, card_id, node_type)

    def _row_key(row) -> tuple[int, bool, str]:
        try:
            generation = int(row[3] or 0)
        except (TypeError, ValueError, IndexError):
            generation = 0
        node_id = str(row[0])
        return (generation, node_id == expected_id, node_id)

    row = max(result.rows, key=_row_key)
    graph_layer = str(row[1] or GRAPH_LAYER_WORKING)
    maturity_status = str(row[2] or MATURITY_WORKING_IMMATURE)
    return str(row[0]), graph_layer, maturity_status


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
        if not res.rows:
            return 0.0
        row = res.rows[0]
        value = row[0]
        return float(value) if value is not None else 0.0
    except Exception as exc:
        logger.warning(
            "kg.scoring.boost_fetch_failed node=%s err=%s",
            node_id,
            exc,
        )
        return 0.0


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
            node_id,
            exc,
        )


def _emit_boost_decision_node(
    conn,
    *,
    board_id: str,
    card_id: str,
    spec_id: Optional[str],
    node_type: str,
    root_node_id: str,
    root_graph_layer: str,
    root_maturity_status: str,
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
    title = "priority_boost recalibrated"
    delta = new_boost - old_boost
    content = (
        f"Card {card_id} priority_boost transitioned from {old_boost:.2f} "
        f"to {new_boost:.2f} (delta={delta:+.2f}) following "
        f"{trigger_event_type}. Source: spec={spec_id or '-'} "
        f"changed_by={changed_by or '-'}."
    )
    audit_identity = json.dumps(
        {
            "card_id": card_id,
            "spec_id": spec_id,
            "old_boost": f"{old_boost:.8f}",
            "new_boost": f"{new_boost:.8f}",
            "trigger_event_type": trigger_event_type,
            "changed_by": changed_by,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    fingerprint = hashlib.sha256(audit_identity.encode("utf-8")).hexdigest()[:24]
    artifact_ref = f"card:{card_id}:boost_audit:{fingerprint}"
    decision_id = mint_node_id(
        board_id,
        "Decision",
        derive_natural_key(artifact_ref, "Decision", title),
        0,
    )
    now_iso = datetime.now(timezone.utc).isoformat()
    session_id = f"boost-recompute-{fingerprint}"

    def _decision_exists() -> bool:
        result = conn.execute(
            "MATCH (n:Decision {id: $decision_id}) RETURN n.id LIMIT 1",
            {"decision_id": decision_id},
        )
        return bool(result.rows)

    def _edge_exists() -> bool:
        result = conn.execute(
            f"MATCH (d:Decision {{id: $decision_id}})"
            f"-[r:relates_to]->(n:{node_type} {{id: $root_node_id}}) "
            "RETURN r LIMIT 1",
            {
                "decision_id": decision_id,
                "root_node_id": root_node_id,
            },
        )
        return bool(result.rows)

    node_preexisted = _decision_exists()
    node_created = False
    if not node_preexisted:
        try:
            conn.create_node(
                "Decision",
                decision_id,
                {
                    "title": title,
                    "content": content,
                    "context": f"Trigger: {trigger_event_type}",
                    "justification": "delta exceeds DECISION_AUDIT_DELTA threshold",
                    "source_artifact_ref": artifact_ref,
                    "graph_layer": root_graph_layer,
                    "maturity_status": root_maturity_status,
                    "created_at": now_iso,
                    "created_by_agent": "system:card_boost_recompute_handler",
                    "source_confidence": 1.0,
                    "relevance_score": 0.5,
                    "pre_cancellation_relevance_score": None,
                    "query_hits": 0,
                    "last_queried_at": None,
                    "last_recomputed_at": now_iso,
                    "priority_boost": 0.0,
                    "superseded_by": None,
                    "superseded_at": None,
                    "revocation_reason": None,
                    "human_curated": False,
                    "generation": 0,
                    "attestation_count": 1,
                    "last_attested_at": now_iso,
                },
                source_session_id=session_id,
            )
            node_created = True
        except Exception as exc:
            # Concurrent delivery may have created the same deterministic
            # audit between the existence check and CREATE.
            if not _decision_exists():
                logger.warning(
                    "kg.scoring.decision_node_failed card=%s err=%s",
                    card_id,
                    exc,
                )
                return
            node_preexisted = True

    # Edge Decision -[:relates_to]-> root entity. Multi-pair belongs_to is
    # for parent hierarchy; relates_to is the right rel for context anchor.
    try:
        # The board fence serializes this check with CREATE.  This explicit
        # replay guard is required because the Kuzu transaction adapter's
        # ``create_edge`` uses CREATE and therefore cannot report a duplicate.
        if _edge_exists():
            return
        created = conn.create_edge(
            "relates_to",
            "Decision",
            node_type,
            decision_id,
            root_node_id,
            {
                "confidence": 1.0,
                "created_by_session_id": session_id,
                "created_at": now_iso,
                "layer": "deterministic",
                "rule_id": "boost_audit",
                "created_by": "card_boost_handler",
            },
        )
        if not created:
            if node_preexisted:
                # Deterministic replay: the node and its edge already exist.
                return
            raise RuntimeError("root endpoint was not matched")
    except Exception as exc:
        # The embedded graph auto-commits statements, so an edge failure after
        # CREATE would otherwise strand an active zero-degree Decision.
        if node_created:
            try:
                conn.execute(
                    "MATCH (n:Decision {id: $decision_id}) DETACH DELETE n",
                    {"decision_id": decision_id},
                )
            except Exception as cleanup_exc:
                logger.error(
                    "kg.scoring.decision_cleanup_failed card=%s node=%s err=%s",
                    card_id,
                    decision_id,
                    cleanup_exc,
                )
        logger.warning(
            "kg.scoring.decision_edge_failed card=%s err=%s",
            card_id,
            exc,
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
    """Recompute boost + relevance through the GraphTransaction port.

    Returns ``(old_boost, new_boost)``. Short-circuits to (0.0, 0.0) when
    the board has no graph backend graph yet (event arrived before bootstrap).
    """
    registry = get_kg_registry()
    if not registry.graph_runtime_store.exists(board_id):
        return (0.0, 0.0)

    new_boost = _resolve_priority_boost(new_priority_value)
    if card_type_value == "bug":
        new_boost = max(
            new_boost,
            _resolve_severity_boost(new_severity_value),
        )

    node_type = _resolve_node_type(card_type_value)

    with guarded_board_write(
        board_id,
        operation="kg.card_boost_recompute",
        owner_id="system:card_boost_recompute_handler",
        mutation_ref=f"{trigger_event_type}:{card_id}",
    ) as lease:
        try:
            async def _run() -> tuple[float, float]:
                async with await registry.graph_transaction.begin(
                    board_id
                ) as scope:
                    root = _resolve_root_node(
                        scope,
                        board_id=board_id,
                        card_id=card_id,
                        node_type=node_type,
                    )
                    if root is None:
                        return (0.0, 0.0)
                    (
                        root_node_id,
                        root_graph_layer,
                        root_maturity_status,
                    ) = root
                    old_boost = _fetch_priority_boost(
                        scope,
                        node_type,
                        root_node_id,
                    )
                    _persist_priority_boost(
                        scope,
                        node_type,
                        root_node_id,
                        new_boost,
                    )
                    _recompute_relevance(
                        scope,
                        board_id,
                        node_type,
                        root_node_id,
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
                            scope,
                            board_id=board_id,
                            card_id=card_id,
                            spec_id=spec_id,
                            node_type=node_type,
                            root_node_id=root_node_id,
                            root_graph_layer=root_graph_layer,
                            root_maturity_status=root_maturity_status,
                            old_boost=old_boost,
                            new_boost=new_boost,
                            trigger_event_type=trigger_event_type,
                            changed_by=changed_by,
                        )
                    return (old_boost, new_boost)

            return run_async_blocking(_run())
        finally:
            lease.ensure_durable()


async def _recompute_boost(
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
    """Dispatch the complete fenced graph recompute away from the event loop."""

    return await run_blocking_graph_io(
        lambda: _recompute_boost_sync(
            board_id=board_id,
            card_id=card_id,
            spec_id=spec_id,
            card_type_value=card_type_value,
            new_priority_value=new_priority_value,
            new_severity_value=new_severity_value,
            trigger_event_type=trigger_event_type,
            changed_by=changed_by,
        ),
        task_name="kg.card_boost_recompute",
    )


async def _handle_boost_event(
    event: CardPriorityChanged | CardSeverityChanged,
    session: object,
    *,
    trigger_event_type: str,
) -> None:
    """Shared handler body for both priority and severity events."""
    card = await get_domain_event_fact_reader().load_card_boost_facts(
        session,
        card_id=event.card_id,
    )
    if card is None:
        logger.warning(
            "kg.scoring.boost_handler_card_missing card=%s board=%s",
            event.card_id,
            event.board_id,
        )
        return

    new_priority_value = card.priority
    new_severity_value = card.severity
    card_type_value = card.card_type

    await _recompute_boost(
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
        self,
        event: CardPriorityChanged,
        session: object,
    ) -> None:
        await _handle_boost_event(
            event,
            session,
            trigger_event_type="card.priority_changed",
        )


@register_handler("card.severity_changed")
class CardSeverityChangedHandler:
    async def handle(
        self,
        event: CardSeverityChanged,
        session: object,
    ) -> None:
        await _handle_boost_event(
            event,
            session,
            trigger_event_type="card.severity_changed",
        )


__all__ = [
    "CardPriorityChangedHandler",
    "CardSeverityChangedHandler",
    "DECISION_AUDIT_DELTA",
]
