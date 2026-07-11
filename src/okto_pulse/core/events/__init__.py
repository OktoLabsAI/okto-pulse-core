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

from okto_pulse.core.events.bus import (
    EventBus,
    publish,
    register_handler,
    resolve_handler,
)
from okto_pulse.core.events.types import (
    CardCancelled,
    CardConclusionAdded,
    CardCreated,
    CardLinkedToSpec,
    CardMoved,
    CardRestored,
    CardUnlinkedFromSpec,
    BugRegressionScenarioReuseDecision,
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

# Handler registration may import services that use this facade. Expose the bus
# and event symbols first so those imports never observe a partial public module.
from okto_pulse.core.events import handlers  # noqa: E402,F401

__all__ = [
    "EventBus",
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
    "BugRegressionScenarioReuseDecision",
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
    "resolve_handler",
]
