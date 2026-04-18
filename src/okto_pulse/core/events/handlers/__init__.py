"""Event handlers package.

Importing this module triggers the @register_handler decorators on each
handler class, populating EventBus._registry. The app lifespan imports
this package BEFORE starting the dispatcher, guaranteeing the registry
is complete when drain begins.
"""

from okto_pulse.core.events.handlers.consolidation_enqueuer import (  # noqa: F401
    ConsolidationEnqueuer,
)

__all__ = ["ConsolidationEnqueuer"]
