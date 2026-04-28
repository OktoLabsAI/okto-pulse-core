"""ConsolidationEnqueuer — first (and so far only) event handler.

Subscribes to the 12 MVP event types and inserts the matching row in
ConsolidationQueue so the existing consolidation_worker can pick it up
and push the artifact into the Knowledge Graph.

Replaces the ad-hoc `db.add(ConsolidationQueue(...))` calls that used to
live scattered across services/main.py. New handlers (activity log,
notifications, webhooks) follow the same pattern — subscribe, map, do.

Idempotency: before inserting, we query the queue for an existing
pending/claimed row for the same (board, artifact_type, artifact_id).
If one exists we return silently; the existing row will serve the work.
"""

from __future__ import annotations

import logging

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import DomainEvent
from okto_pulse.core.infra.config import get_settings
from okto_pulse.core.models.db import ConsolidationQueue

logger = logging.getLogger("okto_pulse.core.events.consolidation_enqueuer")


_CARD_EVENT_PREFIX = "card."
_SPEC_EVENT_PREFIX = "spec."
_SPRINT_EVENT_PREFIX = "sprint."
_REFINEMENT_EVENT_PREFIX = "refinement."
_DERIVED_EVENTS = {
    "ideation.derived_to_spec",
    "refinement.derived_to_spec",
}

# Spec eaf78891 (Ideação #2): card.linked_to_spec / card.unlinked_from_spec
# re-enqueue the SPEC, not the card. The card extractor in
# deterministic_worker does not reference spec_id, so a card re-enqueue
# would be wasted work; the spec extractor is the one that reflects the
# updated cards list.
_CARD_TO_SPEC_EVENTS = {"card.linked_to_spec", "card.unlinked_from_spec"}

# Spec 4007e4a3 (Ideação #3): card.moved / card.conclusion_added re-enqueue
# BOTH the card itself (status/conclusion lives on the card node) AND the
# parent spec (aggregated children-state on the spec node). Orphan cards
# (spec_id is None) skip the spec-side enqueue gracefully.
_CARD_DUAL_TARGET_EVENTS = {"card.moved", "card.conclusion_added"}

_HIGH_PRIORITY_EVENTS = {"card.cancelled", "spec.version_bumped"}


@register_handler(
    "card.created",
    "card.moved",
    "card.conclusion_added",
    "card.cancelled",
    "card.restored",
    "card.linked_to_spec",
    "card.unlinked_from_spec",
    "spec.created",
    "spec.moved",
    "spec.version_bumped",
    "spec.semantic_changed",
    "refinement.semantic_changed",
    "sprint.created",
    "sprint.moved",
    "sprint.closed",
    "ideation.derived_to_spec",
    "refinement.derived_to_spec",
)
class ConsolidationEnqueuer:
    """Maps domain events to ConsolidationQueue rows with dedup + priority."""

    async def handle(self, event: DomainEvent, session: AsyncSession) -> None:
        targets = self._map_targets(event)
        if not targets:
            # Defensive: unknown event_type or missing payload field.
            return

        priority = "high" if event.event_type in _HIGH_PRIORITY_EVENTS else "normal"

        for artifact_type, artifact_id in targets:
            await self._enqueue_one(event, artifact_type, artifact_id, priority, session)

    async def _enqueue_one(
        self,
        event: DomainEvent,
        artifact_type: str,
        artifact_id: str,
        priority: str,
        session: AsyncSession,
    ) -> None:
        # NC-11 fix: dedup must cover ALL statuses, not just pending/claimed.
        # The table has UNIQUE(board_id, artifact_type, artifact_id) without
        # status — so if a previous trigger left a row in 'done' or 'failed',
        # naively inserting a new pending row throws IntegrityError. The
        # handler retries 5x and DLQs the event, losing the consolidation.
        # Pattern: full lookup → if in-flight (pending/claimed) skip; if
        # terminal (done/failed/paused) reset row for re-processing.
        existing = (
            await session.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == event.board_id,
                    ConsolidationQueue.artifact_type == artifact_type,
                    ConsolidationQueue.artifact_id == artifact_id,
                ).limit(1)
            )
        ).scalar_one_or_none()
        if existing is not None:
            if existing.status in ("pending", "claimed"):
                # Worker will pick up the in-flight row; new event is a no-op.
                return
            # Terminal state — reset for re-processing under the new event.
            existing.status = "pending"
            existing.attempts = 0
            existing.last_error = None
            existing.priority = priority
            existing.source = f"event:{event.event_type}"
            existing.triggered_by_event = event.event_type
            existing.claimed_by_session_id = None
            existing.claimed_at = None
            existing.worker_id = None
            existing.claim_timeout_at = None
            existing.next_retry_at = None
            return

        # Spec bdcda842 (TR4 + BR1 zero-loss): NEVER reject the enqueue. The
        # queue is now a zero-loss store; backpressure flows from the
        # consumer (worker pool throttling) rather than from admission. We
        # still emit an alert when the depth crosses the configurable
        # alert_threshold so operators can tune the worker pool — but the
        # INSERT proceeds unconditionally.
        settings = get_settings()
        alert_threshold = settings.kg_queue_alert_threshold
        depth_before_insert = await session.scalar(
            select(func.count()).where(
                ConsolidationQueue.board_id == event.board_id,
                ConsolidationQueue.status.in_(["pending", "claimed"]),
            )
        )
        if (
            depth_before_insert is not None
            and depth_before_insert + 1 >= alert_threshold
            and depth_before_insert < alert_threshold
        ):
            # Crossing edge only — fired exactly once per low→high transition
            # so log volume stays bounded under sustained backlog.
            logger.warning(
                "consolidation.queue.alert_fired board=%s depth=%d threshold=%d "
                "event=%s",
                event.board_id, depth_before_insert + 1, alert_threshold,
                event.event_type,
                extra={
                    "event": "kg.queue.alert_fired",
                    "board_id": event.board_id,
                    "queue_depth": depth_before_insert + 1,
                    "alert_threshold": alert_threshold,
                    "trigger_event": event.event_type,
                },
            )
            # Spec bdcda842 (TR13): in-process counter exposed via
            # /api/v1/kg/queue/health.alert_fired_total.
            from okto_pulse.core.services.queue_health_service import (
                record_alert_fired,
            )
            record_alert_fired()

        session.add(
            ConsolidationQueue(
                board_id=event.board_id,
                artifact_type=artifact_type,
                artifact_id=artifact_id,
                priority=priority,
                source=f"event:{event.event_type}",
                triggered_by_event=event.event_type,
                status="pending",
            )
        )

        # Spec 4007e4a3 (Ideação #3, FR5): structured counter for dual-target
        # spec re-enqueue. Emitted only when the spec-side enqueue actually
        # fires (after dedup short-circuit for orphan and duplicate paths).
        if (
            artifact_type == "spec"
            and event.event_type in _CARD_DUAL_TARGET_EVENTS
        ):
            logger.info(
                "kg.consolidation.reenqueue.fired event_type=%s board=%s "
                "spec_id=%s card_id=%s",
                event.event_type, event.board_id, artifact_id,
                getattr(event, "card_id", None),
                extra={
                    "event": "kg.consolidation.reenqueue.fired",
                    "event_type": event.event_type,
                    "board_id": event.board_id,
                    "spec_id": artifact_id,
                    "card_id": getattr(event, "card_id", None),
                },
            )

    def _map_targets(
        self, event: DomainEvent
    ) -> list[tuple[str, str]]:
        """Return one or more (artifact_type, artifact_id) targets per event.

        Most events map to a single target. Spec 4007e4a3 (Ideação #3)
        introduces dual-target events (card.moved, card.conclusion_added)
        that re-enqueue both the card AND the parent spec. Orphan cards
        (spec_id is None) skip the spec-side target gracefully and emit a
        debug log instead of raising.
        """
        et = event.event_type
        targets: list[tuple[str, str]] = []

        # Dual-target spec-only events (Ideação #2): spec re-enqueue, no card.
        if et in _CARD_TO_SPEC_EVENTS:
            spec_id = getattr(event, "spec_id", None)
            if spec_id:
                targets.append(("spec", spec_id))
            return targets

        # Dual-target card+spec events (Ideação #3): both targets.
        if et in _CARD_DUAL_TARGET_EVENTS:
            card_id = getattr(event, "card_id", None)
            if card_id:
                targets.append(("card", card_id))
            spec_id = getattr(event, "spec_id", None)
            if spec_id:
                targets.append(("spec", spec_id))
            else:
                logger.debug(
                    "kg.consolidation.reenqueue.skipped reason=orphan_card "
                    "event_type=%s board=%s card_id=%s",
                    et, event.board_id, card_id,
                    extra={
                        "event": "kg.consolidation.reenqueue.skipped",
                        "reason": "orphan_card",
                        "event_type": et,
                        "board_id": event.board_id,
                        "card_id": card_id,
                    },
                )
            return targets

        # Single-target legacy paths.
        if et.startswith(_CARD_EVENT_PREFIX):
            cid = getattr(event, "card_id", None)
            if cid:
                targets.append(("card", cid))
            return targets
        if et in _DERIVED_EVENTS:
            sid = getattr(event, "spec_id", None)
            if sid:
                targets.append(("spec", sid))
            return targets
        if et.startswith(_SPEC_EVENT_PREFIX):
            sid = getattr(event, "spec_id", None)
            if sid:
                targets.append(("spec", sid))
            return targets
        if et.startswith(_REFINEMENT_EVENT_PREFIX):
            rid = getattr(event, "refinement_id", None)
            if rid:
                targets.append(("refinement", rid))
            return targets
        if et.startswith(_SPRINT_EVENT_PREFIX):
            spid = getattr(event, "sprint_id", None)
            if spid:
                targets.append(("sprint", spid))
            return targets
        return targets
