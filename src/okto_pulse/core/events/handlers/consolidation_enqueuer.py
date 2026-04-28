"""ConsolidationEnqueuer — first (and so far only) event handler.

Subscribes to the 12 MVP event types and inserts the matching row in
ConsolidationQueue so the existing consolidation_worker can pick it up
and push the artifact into the Knowledge Graph.

Replaces the ad-hoc `db.add(ConsolidationQueue(...))` calls that used to
live scattered across services/main.py. New handlers (activity log,
notifications, webhooks) follow the same pattern — subscribe, map, do.

Idempotency: enqueue is a single dialect-aware UPSERT
(`INSERT ... ON CONFLICT DO UPDATE`) that atomically merges concurrent
events for the same (board, artifact_type, artifact_id). The WHERE clause
on the conflict-update branch limits the update to terminal-state rows
(``done``/``failed``/``paused``); rows already in ``pending``/``claimed``
are left untouched (the worker will pick them up). This eliminates the
SELECT-then-INSERT TOCTOU race that the previous v1 path had — see bug
card 4a430c6d.
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
        # Bug 4a430c6d (race fix): dialect-aware UPSERT atomically merges
        # concurrent events for the same (board_id, artifact_type, artifact_id)
        # without the SELECT-then-INSERT TOCTOU race that the previous v1 path
        # had. Semantics preserved bit-for-bit:
        #   - row inexistente → INSERT (status=pending, attempts=0)
        #   - row em pending/claimed → no-op (the WHERE on the conflict_update
        #     branch filters those out — the existing row keeps its identity)
        #   - row em terminal (done/failed/paused) → reset to pending so the
        #     worker re-processes the artifact under the new event
        # Earlier dedup was implemented by the SELECT block at lines 104-129
        # of the v1 file — see git history before bug 4a430c6d.

        # Spec bdcda842 (TR4 + BR1 zero-loss): NEVER reject the enqueue. The
        # queue is now a zero-loss store; backpressure flows from the
        # consumer (worker pool throttling) rather than from admission. We
        # still emit an alert when the depth crosses the configurable
        # alert_threshold so operators can tune the worker pool — but the
        # UPSERT proceeds unconditionally. Alert is fired BEFORE the upsert
        # so the depth count reflects current state without including the
        # row this call is about to add (no-op cases would otherwise inflate
        # the count).
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

        # Dialect dispatch: SQLite and PostgreSQL both expose
        # `insert().on_conflict_do_update(...)` but via different module paths.
        # Detect at runtime so the same handler works in both prod (Postgres,
        # planned) and dev/local (SQLite, current default).
        dialect_name = session.bind.dialect.name if session.bind else None
        if dialect_name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as upsert_insert
        else:
            from sqlalchemy.dialects.sqlite import insert as upsert_insert

        stmt = upsert_insert(ConsolidationQueue).values(
            board_id=event.board_id,
            artifact_type=artifact_type,
            artifact_id=artifact_id,
            priority=priority,
            source=f"event:{event.event_type}",
            triggered_by_event=event.event_type,
            status="pending",
        ).on_conflict_do_update(
            index_elements=["board_id", "artifact_type", "artifact_id"],
            set_={
                "status": "pending",
                "attempts": 0,
                "last_error": None,
                "priority": priority,
                "source": f"event:{event.event_type}",
                "triggered_by_event": event.event_type,
                "claimed_by_session_id": None,
                "claimed_at": None,
                "worker_id": None,
                "claim_timeout_at": None,
                "next_retry_at": None,
            },
            where=ConsolidationQueue.status.notin_(("pending", "claimed")),
        )
        await session.execute(stmt)

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
