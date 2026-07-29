"""ConsolidationEnqueuer — first (and so far only) event handler.

Subscribes to the 12 MVP event types and inserts the matching row in
ConsolidationQueue so the existing consolidation_worker can pick it up
and push the artifact into the Knowledge Graph.

Replaces the ad-hoc `db.add(ConsolidationQueue(...))` calls that used to
live scattered across services/main.py. New handlers (activity log,
notifications, webhooks) follow the same pattern — subscribe, map, do.

Idempotency is delegated to the registered relational effects port. The
Community SQLAlchemy adapter owns database-specific conflict mechanics; this
core handler only maps domain events to logical queue requests.
"""

from __future__ import annotations

import logging


from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import DomainEvent
from okto_pulse.core.infra.config import get_settings
from okto_pulse.core.ports.relational_effects import (
    ConsolidationQueueUpsert,
    get_relational_effects_port,
)

logger = logging.getLogger("okto_pulse.core.events.consolidation_enqueuer")


_CARD_EVENT_PREFIX = "card."
_SPEC_EVENT_PREFIX = "spec."
_STRUCTURED_ENTITY_EVENT_PREFIX = "structured_entity."
_SPRINT_EVENT_PREFIX = "sprint."
_REFINEMENT_EVENT_PREFIX = "refinement."
_STORY_EVENT_PREFIX = "story."
_IDEATION_EVENT_PREFIX = "ideation."
_QUALITY_ASSESSMENT_RECORDED_EVENT = "quality.assessment_recorded.v1"
_QUALITY_CLARIFICATION_CHANGED_EVENT = "quality.clarification_changed.v1"
_RESEARCH_DECISION_EVENTS = {
    "research_decision.appended",
    "research_decision.superseded",
}
_DERIVED_EVENTS = {
    "ideation.derived_to_spec",
    "refinement.derived_to_spec",
}
_BUG_REGRESSION_DECISION_EVENT = "bug_regression_scenario_reuse_decision"

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
    "artifact.archive_changed",
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
    "structured_entity.created",
    "structured_entity.updated",
    "structured_entity.revoked",
    "refinement.semantic_changed",
    "refinement.moved",
    "quality.assessment_recorded.v1",
    "quality.clarification_changed.v1",
    "research_decision.appended",
    "research_decision.superseded",
    "sprint.created",
    "sprint.moved",
    "sprint.closed",
    "ideation.moved",
    "ideation.derived_to_spec",
    "refinement.derived_to_spec",
    "story.created",
    "story.updated",
    "story.moved",
    "story.linked_to_ideation",
    "bug_regression_scenario_reuse_decision",
)
class ConsolidationEnqueuer:
    """Maps domain events to ConsolidationQueue rows with dedup + priority."""

    async def handle(self, event: DomainEvent, session: object) -> None:
        targets = self._map_targets(event)
        if not targets:
            # Defensive: unknown event_type or missing payload field.
            return

        priority = "high" if event.event_type in _HIGH_PRIORITY_EVENTS else "normal"

        for artifact_type, artifact_id in targets:
            await self._enqueue_one(
                event, artifact_type, artifact_id, priority, session
            )

    async def _enqueue_one(
        self,
        event: DomainEvent,
        artifact_type: str,
        artifact_id: str,
        priority: str,
        session: object,
    ) -> None:
        # Bug 4a430c6d (race fix): the relational port atomically merges
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

        # Spec bdcda842 (TR4 + BR1 zero-loss): every non-tombstoned event is
        # admitted regardless of depth; backpressure flows from the consumer
        # rather than admission. Permanent deletion fences and active-row
        # coalescing are successful no-ops and emit no enqueue telemetry.
        relational_effects = get_relational_effects_port()
        queue_changed = (
            await relational_effects.upsert_consolidation_queue_unless_tombstoned(
                session,
                ConsolidationQueueUpsert(
                    board_id=event.board_id,
                    artifact_type=artifact_type,
                    artifact_id=artifact_id,
                    priority=priority,
                    source=f"event:{event.event_type}",
                    triggered_by_event=event.event_type,
                ),
            )
        )

        # Count only after the atomic write. A pre-write SELECT can pin a
        # SQLite/WAL read snapshot and make the subsequent writer promotion
        # fail if a governed delete commits between both statements.
        depth_after_insert = None
        alert_threshold = None
        if queue_changed:
            alert_threshold = get_settings().kg_queue_alert_threshold
            depth_after_insert = (
                await relational_effects.count_active_consolidation_queue(
                    session,
                    board_id=event.board_id,
                )
            )

        if alert_threshold is not None and depth_after_insert == alert_threshold:
            # Crossing edge only — fired exactly once per low→high transition
            # so log volume stays bounded under sustained backlog.
            logger.warning(
                "consolidation.queue.alert_fired board=%s depth=%d threshold=%d "
                "event=%s",
                event.board_id,
                depth_after_insert,
                alert_threshold,
                event.event_type,
                extra={
                    "event": "kg.queue.alert_fired",
                    "board_id": event.board_id,
                    "queue_depth": depth_after_insert,
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

        # Spec 4007e4a3 (Ideação #3, FR5): structured counter for dual-target
        # spec re-enqueue. Emitted only when the spec-side enqueue actually
        # fires (after dedup short-circuit for orphan and duplicate paths).
        if (
            queue_changed
            and artifact_type == "spec"
            and event.event_type in _CARD_DUAL_TARGET_EVENTS
        ):
            logger.info(
                "kg.consolidation.reenqueue.fired event_type=%s board=%s "
                "spec_id=%s card_id=%s",
                event.event_type,
                event.board_id,
                artifact_id,
                getattr(event, "card_id", None),
                extra={
                    "event": "kg.consolidation.reenqueue.fired",
                    "event_type": event.event_type,
                    "board_id": event.board_id,
                    "spec_id": artifact_id,
                    "card_id": getattr(event, "card_id", None),
                },
            )
        if (
            queue_changed
            and artifact_type == "spec"
            and event.event_type.startswith(_STRUCTURED_ENTITY_EVENT_PREFIX)
        ):
            logger.info(
                "spec_structured_entity_kg_reenqueue_total event_type=%s board=%s "
                "spec_id=%s child_ref=%s outcome=enqueued",
                event.event_type,
                event.board_id,
                artifact_id,
                getattr(event, "child_ref", None),
                extra={
                    "event": "spec_structured_entity_kg_reenqueue_total",
                    "metric_name": "spec_structured_entity_kg_reenqueue_total",
                    "board_id": event.board_id,
                    "spec_id": artifact_id,
                    "child_ref": getattr(event, "child_ref", None),
                    "entity_type": getattr(event, "entity_type", "unknown"),
                    "operation": getattr(event, "operation", "unknown"),
                    "outcome": "enqueued",
                    "reason": "structured_entity_event",
                },
            )

    def _map_targets(self, event: DomainEvent) -> list[tuple[str, str]]:
        """Return one or more (artifact_type, artifact_id) targets per event.

        Most events map to a single target. Spec 4007e4a3 (Ideação #3)
        introduces dual-target events (card.moved, card.conclusion_added)
        that re-enqueue both the card AND the parent spec. Orphan cards
        (spec_id is None) skip the spec-side target gracefully and emit a
        debug log instead of raising.
        """
        et = event.event_type
        targets: list[tuple[str, str]] = []

        if et == "artifact.archive_changed":
            # Archive is handled synchronously by the reversible KG tombstone
            # handler. Restore re-enqueues the authoritative source so it also
            # rematerializes after an intervening rebuild physically omitted it.
            if bool(getattr(event, "archived", False)):
                return targets
            artifact_type = str(getattr(event, "artifact_type", ""))
            artifact_id = getattr(event, "artifact_id", None)
            if artifact_type and artifact_id:
                targets.append((artifact_type, artifact_id))
            return targets

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
                    et,
                    event.board_id,
                    card_id,
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
        if et.startswith(_STRUCTURED_ENTITY_EVENT_PREFIX):
            sid = getattr(event, "spec_id", None)
            if sid:
                targets.append(("spec", sid))
            return targets
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
        if et.startswith(_IDEATION_EVENT_PREFIX):
            iid = getattr(event, "ideation_id", None)
            if iid:
                targets.append(("ideation", iid))
            return targets
        if et == _BUG_REGRESSION_DECISION_EVENT:
            bug_id = getattr(event, "bug_id", None)
            if bug_id:
                targets.append(("card", bug_id))
            spec_id = getattr(event, "spec_id", None)
            if spec_id:
                targets.append(("spec", spec_id))
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
        if et in {
            _QUALITY_ASSESSMENT_RECORDED_EVENT,
            _QUALITY_CLARIFICATION_CHANGED_EVENT,
        }:
            subject_type = getattr(event, "subject_type", None)
            subject_id = getattr(event, "subject_id", None)
            if subject_type in {"ideation", "refinement", "spec"} and subject_id:
                targets.append((subject_type, subject_id))
            return targets
        if et in _RESEARCH_DECISION_EVENTS:
            refinement_id = getattr(event, "refinement_id", None)
            if refinement_id:
                targets.append(("refinement", refinement_id))
            return targets
        if et == "story.linked_to_ideation":
            story_id = getattr(event, "story_id", None)
            if story_id:
                targets.append(("story", story_id))
            ideation_id = getattr(event, "ideation_id", None)
            if ideation_id:
                targets.append(("ideation", ideation_id))
            return targets
        if et.startswith(_STORY_EVENT_PREFIX):
            story_id = getattr(event, "story_id", None)
            if story_id:
                targets.append(("story", story_id))
            return targets
        if et.startswith(_SPRINT_EVENT_PREFIX):
            spid = getattr(event, "sprint_id", None)
            if spid:
                targets.append(("sprint", spid))
            return targets
        return targets
