"""Event handlers package.

Importing this module triggers the @register_handler decorators on each
handler class, populating EventBus._registry. The app lifespan imports
this package BEFORE starting the dispatcher, guaranteeing the registry
is complete when drain begins.
"""

from okto_pulse.core.events.handlers.cancellation_decay import (  # noqa: F401
    CancellationDecayHandler,
    CancellationRestoreHandler,
)
from okto_pulse.core.events.handlers.cognitive_extraction import (  # noqa: F401
    CognitiveExtractionHandler,
)
from okto_pulse.core.events.handlers.consolidation_enqueuer import (  # noqa: F401
    ConsolidationEnqueuer,
)
from okto_pulse.core.events.handlers.kg_hit_recompute import (  # noqa: F401
    KGHitRecomputeHandler,
)
from okto_pulse.core.events.handlers.card_boost_recompute import (  # noqa: F401
    CardPriorityChangedHandler,
    CardSeverityChangedHandler,
)
from okto_pulse.core.events.handlers.kg_decay_tick import (  # noqa: F401
    KGDailyTickHandler,
)

__all__ = [
    "CancellationDecayHandler",
    "CancellationRestoreHandler",
    "CognitiveExtractionHandler",
    "ConsolidationEnqueuer",
    "KGHitRecomputeHandler",
    "CardPriorityChangedHandler",
    "CardSeverityChangedHandler",
    "KGDailyTickHandler",
]
