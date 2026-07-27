"""CancellationDecayHandler / CancellationRestoreHandler — KG integrity pack Fase 1.

Reage a card.cancelled aplicando decay no relevance_score dos nodes do KG
derivados do card cancelado; reage a card.restored revertendo o decay
(apenas em nodes marcados com revocation_reason='source_cancelled' para não
colidir com supersedence futura de outras causas).

Idempotência: a condição WHERE do decay aceita apenas nodes sem revogação e
persiste o score original antes do clamp. Assim, card.cancelled duplicado
(retry do dispatcher, emissão em dupla) não re-aplica penalty nem sobrescreve
outras causas de revogação.

Isolation: handlers rodam em transação SQL própria pelo dispatcher — falha
aqui não afeta o ConsolidationEnqueuer que também observa os mesmos eventos.

Storage boundary: writes go through the GraphTransaction port. The embedded
adapter still uses the synchronous graph backend driver internally, but this handler no
longer opens board graph connections directly.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import (
    ArtifactArchiveChanged,
    CardCancelled,
    CardRestored,
    IdeationMoved,
    RefinementMoved,
    SpecMoved,
    SprintMoved,
)
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.kg.canonical_demotion_global_sync import (
    enqueue_digest_layer_reconciliation,
)
from okto_pulse.core.kg.canonical_stale_reconciler import (
    _source_identity_from_ref,
)
from okto_pulse.core.kg.async_bridge import run_async_blocking
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.guarded_write import guarded_board_write
from okto_pulse.core.kg.schema_contract import NODE_TYPES
from okto_pulse.core.kg.source_maturity import (
    CANCELLATION_REVOCATION_REASON,
    CANCELLATION_SCORE_PENALTY,
    TERMINAL_CANCELLED_STATUSES,
)
from okto_pulse.core.ports.application_persistence import (
    get_application_persistence_port,
)

logger = logging.getLogger(__name__)


DECAY_PENALTY = CANCELLATION_SCORE_PENALTY
REVOCATION_REASON = CANCELLATION_REVOCATION_REASON


def _source_owner_match_clause(source_ref: str) -> tuple[str, dict[str, str]]:
    """Build the type-qualified owner-family predicate used by stale sweep.

    Deterministic projections use child refs (``spec:{id}:fr:*``) and card
    aliases (``task:``, ``test:``, ``bug:``, ...).  Matching only the anchor
    ref leaves those children live after their authoritative source is
    cancelled or archived.  Keep the graph predicate aligned with
    ``_source_identity_from_ref`` instead of using an unsafe textual prefix.
    """

    identity = _source_identity_from_ref(source_ref)
    if identity is None:
        raise ValueError(f"ungoverned source_artifact_ref: {source_ref!r}")
    owner_type, owner_id = identity
    clause = (
        "WITH n, string_split(n.source_artifact_ref, ':') AS parts "
        "WHERE size(parts) >= 2 "
        "WITH n, CASE parts[1] "
        "  WHEN 'card' THEN 'card' "
        "  WHEN 'card_relationship_target' THEN 'card' "
        "  WHEN 'task' THEN 'card' "
        "  WHEN 'test' THEN 'card' "
        "  WHEN 'bug' THEN 'card' "
        "  ELSE parts[1] "
        "END AS owner_type, parts[2] AS owner_id "
        "WHERE owner_type = $owner_type AND owner_id = $owner_id "
    )
    return clause, {"owner_type": owner_type, "owner_id": owner_id}


def _apply_source_decay_sync(board_id: str, source_ref: str) -> int:
    """Apply reversible cancellation decay to one source artifact.

    Runs one UPDATE per node type. graph backend v0.6 has no polymorphic MATCH, so
    iterating NODE_TYPES is the portable way. Skip rows that already carry
    the decay marker (idempotency). The embedded GraphTransaction adapter owns
    the connection lifecycle and releases Windows file handles on scope exit.

    Short-circuits to 0 if the board has no graph backend graph yet (card cancelled
    before it was ever consolidated). Avoids the ~1-2s bootstrap cost and
    keeps handler latency negligible for that common case.
    """
    registry = get_kg_registry()
    if not registry.graph_runtime_store.exists(board_id):
        return 0

    owner_clause, owner_params = _source_owner_match_clause(source_ref)
    now = datetime.now(timezone.utc)
    with guarded_board_write(
        board_id,
        operation="kg.source_cancellation_decay",
        owner_id="system:source_lifecycle_handler",
        mutation_ref=source_ref,
    ) as lease:
        try:
            async def _run() -> int:
                total = 0
                async with await registry.graph_transaction.begin(
                    board_id
                ) as scope:
                    for node_type in NODE_TYPES:
                        cypher = (
                            f"MATCH (n:{node_type}) "
                            "WHERE n.source_artifact_ref IS NOT NULL "
                            "  AND n.revocation_reason IS NULL "
                            f"{owner_clause}"
                            "SET n.pre_cancellation_relevance_score = "
                            "n.relevance_score, "
                            "    n.relevance_score = "
                            "      CASE WHEN n.relevance_score - $penalty < 0.0 "
                            "           THEN 0.0 "
                            "           ELSE n.relevance_score - $penalty END, "
                            "    n.revocation_reason = $reason, "
                            "    n.superseded_by = $reason, "
                            "    n.superseded_at = $now "
                            "RETURN n.id"
                        )
                        result = scope.execute(
                            cypher,
                            {
                                **owner_params,
                                "reason": REVOCATION_REASON,
                                "penalty": DECAY_PENALTY,
                                "now": now,
                            },
                        )
                        total += len(result.rows)
                return total

            return run_async_blocking(_run())
        finally:
            # The backend auto-commits each SET. Even a later node-type
            # failure must drain/checkpoint every write that may have landed.
            lease.ensure_durable()


async def _apply_source_decay(board_id: str, source_ref: str) -> int:
    """Dispatch the complete fenced mutation away from the delivery loop."""

    return await run_blocking_graph_io(
        lambda: _apply_source_decay_sync(board_id, source_ref),
        task_name="kg.source_cancellation_decay",
    )


def _revert_source_decay_sync(board_id: str, source_ref: str) -> int:
    """Blocking half of :func:`_revert_source_decay`, including its fence."""

    registry = get_kg_registry()
    if not registry.graph_runtime_store.exists(board_id):
        return 0

    owner_clause, owner_params = _source_owner_match_clause(source_ref)
    with guarded_board_write(
        board_id,
        operation="kg.source_cancellation_restore",
        owner_id="system:source_lifecycle_handler",
        mutation_ref=source_ref,
    ) as lease:
        try:
            async def _run() -> int:
                total = 0
                async with await registry.graph_transaction.begin(
                    board_id
                ) as scope:
                    for node_type in NODE_TYPES:
                        cypher = (
                            f"MATCH (n:{node_type}) "
                            "WHERE n.source_artifact_ref IS NOT NULL "
                            "  AND n.revocation_reason = $reason "
                            "  AND (n.superseded_by IS NULL "
                            "OR n.superseded_by = $reason) "
                            f"{owner_clause}"
                            "SET n.relevance_score = "
                            "      CASE WHEN "
                            "n.pre_cancellation_relevance_score IS NULL "
                            "           THEN n.relevance_score + $penalty "
                            "           ELSE "
                            "n.pre_cancellation_relevance_score END, "
                            "    n.pre_cancellation_relevance_score = NULL, "
                            "    n.revocation_reason = NULL, "
                            "    n.superseded_by = NULL, "
                            "    n.superseded_at = NULL "
                            "RETURN n.id"
                        )
                        result = scope.execute(
                            cypher,
                            {
                                **owner_params,
                                "reason": REVOCATION_REASON,
                                "penalty": DECAY_PENALTY,
                            },
                        )
                        total += len(result.rows)
                return total

            return run_async_blocking(_run())
        finally:
            lease.ensure_durable()


async def _revert_source_decay(board_id: str, source_ref: str) -> int:
    """Dispatch the complete fenced mutation away from the delivery loop."""

    return await run_blocking_graph_io(
        lambda: _revert_source_decay_sync(board_id, source_ref),
        task_name="kg.source_cancellation_restore",
    )


async def _apply_decay(board_id: str, card_id: str) -> int:
    """Backward-compatible card cancellation helper."""

    return await _apply_source_decay(board_id, f"card:{card_id}")


async def _revert_decay(board_id: str, card_id: str) -> int:
    """Backward-compatible card restoration helper."""

    return await _revert_source_decay(board_id, f"card:{card_id}")


async def _load_source_record(
    session: object,
    *,
    artifact_type: str,
    artifact_id: str,
) -> object | None:
    """Read the current relational lifecycle state before clearing a tombstone.

    Archive and cancellation intentionally share the same graph marker so all
    active-node reads keep one exclusion contract. Their restore events can
    therefore overlap: the marker may only be cleared after the *last*
    authoritative relational exclusion has gone away.
    """

    return await get_application_persistence_port().get(
        session,
        entity=artifact_type,
        record_id=artifact_id,
    )


def _record_is_cancelled(record: object | None) -> bool:
    if record is None:
        return False
    raw_status = getattr(record, "status", None)
    status = getattr(raw_status, "value", raw_status)
    return str(status or "").strip().lower() in TERMINAL_CANCELLED_STATUSES


def _record_is_archived(record: object | None) -> bool:
    return bool(record is not None and getattr(record, "archived", False))


async def _converge_source_lifecycle(
    session: object,
    *,
    board_id: str,
    artifact_type: str,
    artifact_id: str,
) -> tuple[int, str, str]:
    """Converge graph visibility to current SQL state, not event arrival order.

    Handler retries can be delayed behind newer lifecycle events. Reading the
    authoritative row makes an old cancel/archive retry harmless after restore,
    and makes an old restore retry harmless after a later cancel/archive.
    A missing source row is authoritative deletion and therefore fail-closed:
    a delayed restore/archive-clear event must never revive orphaned graph
    material after the relational source has been hard-deleted.
    """

    record = await _load_source_record(
        session,
        artifact_type=artifact_type,
        artifact_id=artifact_id,
    )
    if record is None:
        should_revoke = True
        authority = "relational_absent_fail_closed"
    else:
        should_revoke = _record_is_archived(record) or _record_is_cancelled(record)
        authority = "relational_current"

    source_ref = f"{artifact_type}:{artifact_id}"
    if should_revoke:
        return (
            await _apply_source_decay(board_id, source_ref),
            "applied",
            authority,
        )
    return (
        await _revert_source_decay(board_id, source_ref),
        "reverted",
        authority,
    )


async def _enqueue_global_lifecycle_reconciliation(
    session: object,
    *,
    event: CardCancelled
    | CardRestored
    | IdeationMoved
    | RefinementMoved
    | SpecMoved
    | SprintMoved
    | ArtifactArchiveChanged,
) -> None:
    """Durably prune or republish Global Discovery from current board truth."""

    if not get_kg_registry().graph_runtime_store.exists(event.board_id):
        return
    await enqueue_digest_layer_reconciliation(
        session,
        board_id=event.board_id,
        reason=f"source_lifecycle.{event.event_type}",
        idempotency_key=f"{event.event_type}:{event.event_id}",
    )


@register_handler("card.cancelled")
class CancellationDecayHandler:
    """Apply decay penalty to KG nodes derived from a cancelled card."""

    async def handle(self, event: CardCancelled, session: object) -> None:
        nodes_affected, action, authority = await _converge_source_lifecycle(
            session,
            board_id=event.board_id,
            artifact_type="card",
            artifact_id=event.card_id,
        )
        await _enqueue_global_lifecycle_reconciliation(session, event=event)
        logger.info(
            "kg.cancellation_decay.%s",
            action,
            extra={
                "event": f"kg.cancellation_decay.{action}",
                "card_id": event.card_id,
                "board_id": event.board_id,
                "nodes_affected": nodes_affected,
                "decay_penalty": DECAY_PENALTY,
                "authority": authority,
            },
        )


@register_handler("card.restored")
class CancellationRestoreHandler:
    """Revert the decay penalty when a cancelled card is restored."""

    async def handle(self, event: CardRestored, session: object) -> None:
        nodes_affected, action, authority = await _converge_source_lifecycle(
            session,
            board_id=event.board_id,
            artifact_type="card",
            artifact_id=event.card_id,
        )
        await _enqueue_global_lifecycle_reconciliation(session, event=event)
        logger.info(
            "kg.cancellation_decay.%s",
            action,
            extra={
                "event": f"kg.cancellation_decay.{action}",
                "card_id": event.card_id,
                "board_id": event.board_id,
                "nodes_affected": nodes_affected,
                "authority": authority,
            },
        )


_LIFECYCLE_SOURCE_FIELDS: dict[str, tuple[str, str]] = {
    "ideation.moved": ("ideation", "ideation_id"),
    "refinement.moved": ("refinement", "refinement_id"),
    "spec.moved": ("spec", "spec_id"),
    "sprint.moved": ("sprint", "sprint_id"),
}


@register_handler(
    "ideation.moved",
    "refinement.moved",
    "spec.moved",
    "sprint.moved",
)
class SourceCancellationLifecycleHandler:
    """Tombstone and restore every reversible cancellable KG source."""

    async def handle(
        self,
        event: IdeationMoved | RefinementMoved | SpecMoved | SprintMoved,
        session: object,
    ) -> None:
        source_type, id_field = _LIFECYCLE_SOURCE_FIELDS[event.event_type]
        source_ref = f"{source_type}:{getattr(event, id_field)}"
        from_status = str(event.from_status or "").strip().lower()
        to_status = str(event.to_status or "").strip().lower()
        event_touches_cancellation = (
            to_status in TERMINAL_CANCELLED_STATUSES
            or from_status in TERMINAL_CANCELLED_STATUSES
        )
        if not event_touches_cancellation:
            return
        affected, action, authority = await _converge_source_lifecycle(
            session,
            board_id=event.board_id,
            artifact_type=source_type,
            artifact_id=str(getattr(event, id_field)),
        )
        await _enqueue_global_lifecycle_reconciliation(session, event=event)

        logger.info(
            "kg.source_cancellation.%s",
            action,
            extra={
                "event": f"kg.source_cancellation.{action}",
                "board_id": event.board_id,
                "source_artifact_ref": source_ref,
                "nodes_affected": affected,
                "from_status": from_status,
                "to_status": to_status,
                "authority": authority,
            },
        )


@register_handler("artifact.archive_changed")
class SourceArchiveLifecycleHandler:
    """Tombstone archived sources and restore/rematerialize them reversibly."""

    async def handle(
        self,
        event: ArtifactArchiveChanged,
        session: object,
    ) -> None:
        source_ref = f"{event.artifact_type}:{event.artifact_id}"
        affected, action, authority = await _converge_source_lifecycle(
            session,
            board_id=event.board_id,
            artifact_type=event.artifact_type,
            artifact_id=event.artifact_id,
        )
        await _enqueue_global_lifecycle_reconciliation(session, event=event)
        logger.info(
            "kg.source_archive.%s",
            action,
            extra={
                "event": f"kg.source_archive.{action}",
                "board_id": event.board_id,
                "source_artifact_ref": source_ref,
                "nodes_affected": affected,
                "authority": authority,
            },
        )


__all__ = [
    "CancellationDecayHandler",
    "CancellationRestoreHandler",
    "SourceCancellationLifecycleHandler",
    "SourceArchiveLifecycleHandler",
    "DECAY_PENALTY",
    "REVOCATION_REASON",
]
