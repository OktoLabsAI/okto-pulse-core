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
    ArtifactArchiveChanged,
    CardCancelled,
    CardConclusionAdded,
    CardCreated,
    CardLinkedToSpec,
    CardMoved,
    CardRestored,
    CardUnlinkedFromSpec,
    BugRegressionScenarioReuseDecision,
    ChecklistBindingChanged,
    CODE_TRACEABILITY_EVENT_TYPES,
    CodeEvidenceCreated,
    CodeEvidenceDispositionChanged,
    CodeEvidenceLinked,
    CodeEvidenceRevoked,
    CodeEvidenceSuperseded,
    CodeEvidenceUnlinked,
    CodeInvestigationReceiptRevoked,
    CodeInvestigationReceiptSubmitted,
    CodeInvestigationRequested,
    CodeTraceabilityDomainEvent,
    CodeTraceabilityWaiverCleared,
    CodeTraceabilityWaiverCreated,
    DomainEvent,
    EVENT_TYPES,
    IdeationDerivedToSpec,
    IdeationMoved,
    ImplementationOverlapAcknowledged,
    ImplementationTargetCreated,
    ImplementationTargetExecutionReceiptSubmitted,
    ImplementationTargetResolutionSubmitted,
    ImplementationTargetRevoked,
    ImplementationTargetUpdated,
    KGDailyTick,
    KGDeliveryRedriveTick,
    KGFullRebuildTick,
    PolicyAdoptionChanged,
    PolicyBindingMaterialized,
    PolicyConstraintChanged,
    PolicyRetirementChanged,
    RefinementDerivedToSpec,
    RefinementMoved,
    RefinementSemanticChanged,
    QualityAssessmentRecorded,
    QualityClarificationChanged,
    ResearchDecisionAppended,
    ResearchDecisionSuperseded,
    SpecCreated,
    SpecDependencyAdded,
    SpecDependencyRemoved,
    SpecMoved,
    SpecSemanticChanged,
    SpecVersionBumped,
    SprintClosed,
    SprintCreated,
    SprintMoved,
)
from okto_pulse.core.events.code_traceability import (
    code_traceability_event_digest,
    make_code_traceability_event,
    publish_code_traceability_mutation,
)

# Handler registration may import services that use this facade. Expose the bus
# and event symbols first so those imports never observe a partial public module.
from okto_pulse.core.events import handlers  # noqa: E402,F401

__all__ = [
    "EventBus",
    "DomainEvent",
    "EVENT_TYPES",
    # Event classes
    "ArtifactArchiveChanged",
    "CardCancelled",
    "CardConclusionAdded",
    "CardCreated",
    "CardLinkedToSpec",
    "CardMoved",
    "CardRestored",
    "CardUnlinkedFromSpec",
    "BugRegressionScenarioReuseDecision",
    "ChecklistBindingChanged",
    "CODE_TRACEABILITY_EVENT_TYPES",
    "CodeEvidenceCreated",
    "CodeEvidenceDispositionChanged",
    "CodeEvidenceLinked",
    "CodeEvidenceRevoked",
    "CodeEvidenceSuperseded",
    "CodeEvidenceUnlinked",
    "CodeInvestigationReceiptRevoked",
    "CodeInvestigationReceiptSubmitted",
    "CodeInvestigationRequested",
    "CodeTraceabilityDomainEvent",
    "CodeTraceabilityWaiverCleared",
    "CodeTraceabilityWaiverCreated",
    "IdeationDerivedToSpec",
    "IdeationMoved",
    "ImplementationOverlapAcknowledged",
    "ImplementationTargetCreated",
    "ImplementationTargetExecutionReceiptSubmitted",
    "ImplementationTargetResolutionSubmitted",
    "ImplementationTargetRevoked",
    "ImplementationTargetUpdated",
    "KGDailyTick",
    "KGDeliveryRedriveTick",
    "KGFullRebuildTick",
    "PolicyAdoptionChanged",
    "PolicyBindingMaterialized",
    "PolicyConstraintChanged",
    "PolicyRetirementChanged",
    "RefinementDerivedToSpec",
    "RefinementMoved",
    "RefinementSemanticChanged",
    "QualityAssessmentRecorded",
    "QualityClarificationChanged",
    "ResearchDecisionAppended",
    "ResearchDecisionSuperseded",
    "SpecCreated",
    "SpecDependencyAdded",
    "SpecDependencyRemoved",
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
    "code_traceability_event_digest",
    "make_code_traceability_event",
    "publish_code_traceability_mutation",
]
