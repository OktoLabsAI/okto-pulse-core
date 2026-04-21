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


# Ordered list of all event_type strings known to the MVP. The dispatcher
# uses this to resolve DomainEventRow → subclass during reconstruction.
EVENT_TYPES: list[str] = [
    CardCreated.event_type,
    CardMoved.event_type,
    CardCancelled.event_type,
    CardRestored.event_type,
    SpecCreated.event_type,
    SpecMoved.event_type,
    SpecVersionBumped.event_type,
    SprintCreated.event_type,
    SprintMoved.event_type,
    SprintClosed.event_type,
    IdeationDerivedToSpec.event_type,
    RefinementDerivedToSpec.event_type,
]


_EVENT_CLASS_BY_TYPE: dict[str, type[DomainEvent]] = {
    CardCreated.event_type: CardCreated,
    CardMoved.event_type: CardMoved,
    CardCancelled.event_type: CardCancelled,
    CardRestored.event_type: CardRestored,
    SpecCreated.event_type: SpecCreated,
    SpecMoved.event_type: SpecMoved,
    SpecVersionBumped.event_type: SpecVersionBumped,
    SprintCreated.event_type: SprintCreated,
    SprintMoved.event_type: SprintMoved,
    SprintClosed.event_type: SprintClosed,
    IdeationDerivedToSpec.event_type: IdeationDerivedToSpec,
    RefinementDerivedToSpec.event_type: RefinementDerivedToSpec,
}


def resolve_event_class(event_type: str) -> type[DomainEvent] | None:
    """Return the DomainEvent subclass that matches event_type, or None."""
    return _EVENT_CLASS_BY_TYPE.get(event_type)
