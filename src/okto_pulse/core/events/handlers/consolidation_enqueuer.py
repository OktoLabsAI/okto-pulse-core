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
_DERIVED_EVENTS = {
    "ideation.derived_to_spec",
    "refinement.derived_to_spec",
}

_HIGH_PRIORITY_EVENTS = {"card.cancelled", "spec.version_bumped"}


@register_handler(
    "card.created",
    "card.moved",
    "card.cancelled",
    "card.restored",
    "spec.created",
    "spec.moved",
    "spec.version_bumped",
    "sprint.created",
    "sprint.moved",
    "sprint.closed",
    "ideation.derived_to_spec",
    "refinement.derived_to_spec",
)
class ConsolidationEnqueuer:
    """Maps domain events to ConsolidationQueue rows with dedup + priority."""

    async def handle(self, event: DomainEvent, session: AsyncSession) -> None:
        artifact_type, artifact_id = self._map_artifact(event)
        if artifact_type is None or artifact_id is None:
            # Defensive: unknown event_type or missing payload field.
            return

        priority = "high" if event.event_type in _HIGH_PRIORITY_EVENTS else "normal"

        dup = await session.scalar(
            select(ConsolidationQueue.id).where(
                ConsolidationQueue.board_id == event.board_id,
                ConsolidationQueue.artifact_type == artifact_type,
                ConsolidationQueue.artifact_id == artifact_id,
                ConsolidationQueue.status.in_(["pending", "claimed"]),
            ).limit(1)
        )
        if dup is not None:
            return

        # Queue depth limit — reject new enqueues when the board queue is full.
        max_depth = get_settings().kg_max_queue_depth
        count = await session.scalar(
            select(func.count()).where(
                ConsolidationQueue.board_id == event.board_id,
                ConsolidationQueue.status.in_(["pending", "claimed"]),
            )
        )
        if count >= max_depth:
            logger.warning(
                "consolidation.queue_full board=%s depth=%d rejecting event=%s",
                event.board_id, count, event.event_type,
            )
            return

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

    def _map_artifact(self, event: DomainEvent) -> tuple[str | None, str | None]:
        et = event.event_type
        if et.startswith(_CARD_EVENT_PREFIX):
            return "card", getattr(event, "card_id", None)
        if et in _DERIVED_EVENTS:
            return "spec", getattr(event, "spec_id", None)
        if et.startswith(_SPEC_EVENT_PREFIX):
            return "spec", getattr(event, "spec_id", None)
        if et.startswith(_SPRINT_EVENT_PREFIX):
            return "sprint", getattr(event, "sprint_id", None)
        return None, None
