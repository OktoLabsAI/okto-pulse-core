"""Internal event bus for okto-pulse.

Publishers (services/main.py) emit typed DomainEvents atomically with
their data change. The EventDispatcher worker drains the outbox and
invokes registered handlers asynchronously. See README.md for the full
architecture, observability queries and guide to adding a new handler.

Import order matters:
    1. bus    — registers the registry singleton + publish() API
    2. types  — DomainEvent base + 12 concrete event classes
    3. handlers — side-effect: populates registry via @register_handler
"""

from okto_pulse.core.events import bus  # noqa: F401
from okto_pulse.core.events import types  # noqa: F401
from okto_pulse.core.events import handlers  # noqa: F401 — triggers registration

from okto_pulse.core.events.bus import EventBus, publish, register_handler
from okto_pulse.core.events.dispatcher import (
    EventDispatcher,
    get_dispatcher,
    set_dispatcher,
)
from okto_pulse.core.events.types import (
    CardCancelled,
    CardConclusionAdded,
    CardCreated,
    CardLinkedToSpec,
    CardMoved,
    CardRestored,
    CardUnlinkedFromSpec,
    DomainEvent,
    EVENT_TYPES,
    IdeationDerivedToSpec,
    RefinementDerivedToSpec,
    RefinementSemanticChanged,
    SpecCreated,
    SpecMoved,
    SpecSemanticChanged,
    SpecVersionBumped,
    SprintClosed,
    SprintCreated,
    SprintMoved,
)

__all__ = [
    "EventBus",
    "EventDispatcher",
    "DomainEvent",
    "EVENT_TYPES",
    # Event classes
    "CardCancelled",
    "CardConclusionAdded",
    "CardCreated",
    "CardLinkedToSpec",
    "CardMoved",
    "CardRestored",
    "CardUnlinkedFromSpec",
    "IdeationDerivedToSpec",
    "RefinementDerivedToSpec",
    "RefinementSemanticChanged",
    "SpecCreated",
    "SpecMoved",
    "SpecSemanticChanged",
    "SpecVersionBumped",
    "SprintClosed",
    "SprintCreated",
    "SprintMoved",
    # Functions
    "publish",
    "register_handler",
    "get_dispatcher",
    "set_dispatcher",
]
