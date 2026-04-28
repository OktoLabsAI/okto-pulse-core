"""Domain event types for the internal event bus.

Every state change in the domain (card created, spec moved, sprint closed,
etc.) is modelled as a typed event. Publishers in services/main.py publish
these via EventBus.publish(); handlers react asynchronously via the
EventDispatcher worker.

See core/events/README.md for the full list of events and how to add a
new one.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import ClassVar, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DomainEvent(BaseModel):
    """Base class for every domain event.

    Common fields (stored as dedicated columns on domain_events, NOT in
    payload_json): event_id, event_type, board_id, actor_id, actor_type,
    occurred_at. Subclasses add event-specific payload fields.
    """

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=False)

    event_type: ClassVar[str] = ""

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    board_id: str
    actor_id: Optional[str] = None
    actor_type: str = "user"
    occurred_at: datetime = Field(default_factory=_utcnow)

    def payload_for_storage(self) -> dict:
        """Return only the event-specific fields for the payload_json column.

        The top-level columns (event_id/board_id/actor_id/actor_type/
        occurred_at) live in dedicated columns and are excluded here.
        event_type is also excluded since it's a dedicated column populated
        from the ClassVar.
        """
        return self.model_dump(
            mode="json",
            exclude={
                "event_id",
                "board_id",
                "actor_id",
                "actor_type",
                "occurred_at",
            },
        )


# --- Card lifecycle ---


class CardCreated(DomainEvent):
    event_type: ClassVar[str] = "card.created"
    card_id: str
    spec_id: str
    sprint_id: Optional[str] = None
    card_type: str = "normal"
    priority: str = "none"


class CardMoved(DomainEvent):
    event_type: ClassVar[str] = "card.moved"
    card_id: str
    from_status: str
    to_status: str
    spec_id: Optional[str] = None
    moved_by: Optional[str] = None


class CardConclusionAdded(DomainEvent):
    """Fired when submit_task_validation registers a non-empty conclusion_text.

    The handler enqueues the parent spec consolidation so that the KG
    reflects the card's narrative outcome alongside its final state.
    """

    event_type: ClassVar[str] = "card.conclusion_added"
    card_id: str
    spec_id: Optional[str] = None
    conclusion_excerpt: str = ""
    added_by: Optional[str] = None


class CardCancelled(DomainEvent):
    event_type: ClassVar[str] = "card.cancelled"
    card_id: str
    previous_status: str


class CardRestored(DomainEvent):
    event_type: ClassVar[str] = "card.restored"
    card_id: str
    to_status: str
    from_status: str = "cancelled"


# --- Spec lifecycle ---


class SpecCreated(DomainEvent):
    event_type: ClassVar[str] = "spec.created"
    spec_id: str
    source: Literal["manual", "derived_ideation", "derived_refinement"] = "manual"
    origin_id: Optional[str] = None


class SpecMoved(DomainEvent):
    event_type: ClassVar[str] = "spec.moved"
    spec_id: str
    from_status: str
    to_status: str


class SpecVersionBumped(DomainEvent):
    event_type: ClassVar[str] = "spec.version_bumped"
    spec_id: str
    old_version: int
    new_version: int
    changed_fields: list[str] = Field(default_factory=list)


class SpecSemanticChanged(DomainEvent):
    """Fired when semantic spec content changes WITHOUT bumping version.

    Covers fields that affect KG extraction but are intentionally excluded
    from `content_fields` in update_spec (decisions, business_rules,
    api_contracts, test_scenarios, screen_mockups). The ConsolidationEnqueuer
    handler maps this event to a spec consolidation enqueue so the KG stays
    in sync with structured-section mutations.
    """

    event_type: ClassVar[str] = "spec.semantic_changed"
    spec_id: str
    changed_fields: list[str] = Field(default_factory=list)


class RefinementSemanticChanged(DomainEvent):
    """Fired when semantic refinement content changes.

    Mirrors SpecSemanticChanged for refinements. Triggers re-consolidation
    via ConsolidationEnqueuer → consolidation_worker (artifact_type=refinement).
    """

    event_type: ClassVar[str] = "refinement.semantic_changed"
    refinement_id: str
    changed_fields: list[str] = Field(default_factory=list)


class CardLinkedToSpec(DomainEvent):
    """Fired when a card is linked to a spec via link_card_to_spec.

    The handler enqueues a spec consolidation (NOT card) — the card extractor
    does not reference spec_id, but the spec extractor must reflect the new
    cards list.
    """

    event_type: ClassVar[str] = "card.linked_to_spec"
    card_id: str
    spec_id: str


class CardUnlinkedFromSpec(DomainEvent):
    """Fired when a card is unlinked from a spec.

    Symmetric to CardLinkedToSpec — spec re-consolidation reflects the
    removal in the cards list.
    """

    event_type: ClassVar[str] = "card.unlinked_from_spec"
    card_id: str
    spec_id: str


# --- Sprint lifecycle ---


class SprintCreated(DomainEvent):
    event_type: ClassVar[str] = "sprint.created"
    sprint_id: str
    spec_id: str


class SprintMoved(DomainEvent):
    event_type: ClassVar[str] = "sprint.moved"
    sprint_id: str
    from_status: str
    to_status: str


class SprintClosed(DomainEvent):
    event_type: ClassVar[str] = "sprint.closed"
    sprint_id: str


# --- Derivation events ---


class IdeationDerivedToSpec(DomainEvent):
    event_type: ClassVar[str] = "ideation.derived_to_spec"
    ideation_id: str
    spec_id: str


class RefinementDerivedToSpec(DomainEvent):
    event_type: ClassVar[str] = "refinement.derived_to_spec"
    refinement_id: str
    spec_id: str


# --- KG operational events (spec 28583299 — Ideação #4) ---


class KGHitFlushed(DomainEvent):
    """Fired when KGService._flush_hits persists a batch of query_hits to Kùzu.

    The handler reacts by recomputing the node's relevance_score so the
    refreshed hit count immediately participates in ranking. Decoupling
    via DomainEvent (vs sync recompute on the read path) keeps the search
    hot path free of Kùzu MATCH/COUNT pressure — see dec_3a6eb8ad.
    """

    event_type: ClassVar[str] = "kg.hit_flushed"
    node_type: str
    node_id: str
    hits_delta: int
    flushed_at: str  # ISO datetime string for replay determinism


class CardPriorityChanged(DomainEvent):
    """Fired when a card's priority changes via update_card.

    The handler recomputes the priority_boost on the card's root KG entity
    node and triggers a relevance_score recompute. Auditoria of significant
    boost changes (|delta| > 0.05) is recorded as a Decision node in the KG
    rather than a SQL audit table — see dec_cb956457.
    """

    event_type: ClassVar[str] = "card.priority_changed"
    card_id: str
    old_priority: Optional[str] = None
    new_priority: Optional[str] = None
    spec_id: Optional[str] = None
    changed_by: Optional[str] = None


class CardSeverityChanged(DomainEvent):
    """Fired when a Bug card's severity changes via update_card.

    Only emitted for ``card_type == 'bug'`` (BR1) — feature/task/chore cards
    have no severity semantics. Handler symmetry with CardPriorityChanged:
    recomputes priority_boost via MAX(priority, severity) and persists.
    """

    event_type: ClassVar[str] = "card.severity_changed"
    card_id: str
    old_severity: Optional[str] = None
    new_severity: Optional[str] = None
    spec_id: Optional[str] = None
    changed_by: Optional[str] = None


class KGDailyTick(DomainEvent):
    """Fired by the APScheduler cron at 03:00 UTC to drive global decay.

    Uses ``board_id="*"`` as a global sentinel because the handler iterates
    every active board. Only the leader replica emits the event (advisory
    lock); other replicas log a skip — see dec_bc0eaeec.
    """

    event_type: ClassVar[str] = "kg.tick.daily"
    tick_id: str  # uuid4 per tick run, propagates into kg_tick_runs row
    scheduled_at: str  # ISO datetime when APScheduler fired the trigger


# Ordered list of all event_type strings known to the MVP. The dispatcher
# uses this to resolve DomainEventRow → subclass during reconstruction.
EVENT_TYPES: list[str] = [
    CardCreated.event_type,
    CardMoved.event_type,
    CardConclusionAdded.event_type,
    CardCancelled.event_type,
    CardRestored.event_type,
    CardLinkedToSpec.event_type,
    CardUnlinkedFromSpec.event_type,
    SpecCreated.event_type,
    SpecMoved.event_type,
    SpecVersionBumped.event_type,
    SpecSemanticChanged.event_type,
    RefinementSemanticChanged.event_type,
    SprintCreated.event_type,
    SprintMoved.event_type,
    SprintClosed.event_type,
    IdeationDerivedToSpec.event_type,
    RefinementDerivedToSpec.event_type,
    KGHitFlushed.event_type,
    CardPriorityChanged.event_type,
    CardSeverityChanged.event_type,
    KGDailyTick.event_type,
]


_EVENT_CLASS_BY_TYPE: dict[str, type[DomainEvent]] = {
    CardCreated.event_type: CardCreated,
    CardMoved.event_type: CardMoved,
    CardConclusionAdded.event_type: CardConclusionAdded,
    CardCancelled.event_type: CardCancelled,
    CardRestored.event_type: CardRestored,
    CardLinkedToSpec.event_type: CardLinkedToSpec,
    CardUnlinkedFromSpec.event_type: CardUnlinkedFromSpec,
    SpecCreated.event_type: SpecCreated,
    SpecMoved.event_type: SpecMoved,
    SpecVersionBumped.event_type: SpecVersionBumped,
    SpecSemanticChanged.event_type: SpecSemanticChanged,
    RefinementSemanticChanged.event_type: RefinementSemanticChanged,
    SprintCreated.event_type: SprintCreated,
    SprintMoved.event_type: SprintMoved,
    SprintClosed.event_type: SprintClosed,
    IdeationDerivedToSpec.event_type: IdeationDerivedToSpec,
    RefinementDerivedToSpec.event_type: RefinementDerivedToSpec,
    KGHitFlushed.event_type: KGHitFlushed,
    CardPriorityChanged.event_type: CardPriorityChanged,
    CardSeverityChanged.event_type: CardSeverityChanged,
    KGDailyTick.event_type: KGDailyTick,
}


def resolve_event_class(event_type: str) -> type[DomainEvent] | None:
    """Return the DomainEvent subclass that matches event_type, or None."""
    return _EVENT_CLASS_BY_TYPE.get(event_type)
