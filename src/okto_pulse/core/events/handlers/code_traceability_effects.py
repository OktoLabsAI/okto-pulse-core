"""Durable edition-owned projections for Code Traceability events."""

from __future__ import annotations

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import (
    CODE_TRACEABILITY_EVENT_TYPES,
    CodeTraceabilityDomainEvent,
)
from okto_pulse.core.ports.code_traceability_event_effects import (
    get_code_traceability_event_effects_port,
)


@register_handler(*CODE_TRACEABILITY_EVENT_TYPES)
class CodeTraceabilityEventEffectsHandler:
    """Project activity/cache/validation effects through the edition port."""

    async def handle(
        self,
        event: CodeTraceabilityDomainEvent,
        session: object,
    ) -> None:
        if not isinstance(event, CodeTraceabilityDomainEvent):
            raise TypeError("code_traceability_event_invalid")
        await get_code_traceability_event_effects_port().apply(session, event)


__all__ = ["CodeTraceabilityEventEffectsHandler"]
