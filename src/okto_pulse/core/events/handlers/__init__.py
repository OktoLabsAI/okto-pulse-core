"""Event handlers package.

Importing this module triggers the @register_handler decorators on each
handler class, populating EventBus._registry. The app lifespan imports
this package BEFORE starting the dispatcher, guaranteeing the registry
is complete when drain begins.
"""

from okto_pulse.core.events.handlers.cancellation_decay import (  # noqa: F401
    CancellationDecayHandler,
    CancellationRestoreHandler,
    SourceArchiveLifecycleHandler,
    SourceCancellationLifecycleHandler,
)
from okto_pulse.core.events.handlers.cognitive_extraction import (  # noqa: F401
    CognitiveExtractionHandler,
)
from okto_pulse.core.events.handlers.checklist_binding_audit import (  # noqa: F401
    ChecklistBindingAuditHandler,
)
from okto_pulse.core.events.handlers.consolidation_enqueuer import (  # noqa: F401
    ConsolidationEnqueuer,
)
from okto_pulse.core.events.handlers.code_traceability_effects import (  # noqa: F401
    CodeTraceabilityEventEffectsHandler,
)
from okto_pulse.core.events.handlers.discovery_selector_cache import (  # noqa: F401
    DiscoverySelectorCacheInvalidationHandler,
)
from okto_pulse.core.events.handlers.kg_hit_recompute import (  # noqa: F401
    KGHitRecomputeHandler,
)
from okto_pulse.core.events.handlers.policy_constraint_projection import (  # noqa: F401
    PolicyConstraintProjectionHandler,
)
from okto_pulse.core.events.handlers.card_boost_recompute import (  # noqa: F401
    CardPriorityChangedHandler,
    CardSeverityChangedHandler,
)
from okto_pulse.core.events.handlers.kg_decay_tick import (  # noqa: F401
    KGDailyTickHandler,
    KGDeliveryRedriveTickHandler,
)

__all__ = [
    "CancellationDecayHandler",
    "CancellationRestoreHandler",
    "SourceArchiveLifecycleHandler",
    "SourceCancellationLifecycleHandler",
    "CognitiveExtractionHandler",
    "ChecklistBindingAuditHandler",
    "ConsolidationEnqueuer",
    "CodeTraceabilityEventEffectsHandler",
    "DiscoverySelectorCacheInvalidationHandler",
    "KGHitRecomputeHandler",
    "PolicyConstraintProjectionHandler",
    "CardPriorityChangedHandler",
    "CardSeverityChangedHandler",
    "KGDailyTickHandler",
    "KGDeliveryRedriveTickHandler",
]
