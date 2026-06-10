"""Discovery selector cache invalidation handlers."""

from __future__ import annotations

import logging
import time

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import DomainEvent
from okto_pulse.core.services.discovery_selector_catalog import (
    SELECTOR_EVENT_SPEC_UPDATED,
    SELECTOR_EVENT_STRUCTURED_CREATED,
    SELECTOR_EVENT_STRUCTURED_REVOKED,
    SELECTOR_EVENT_STRUCTURED_UPDATED,
    get_default_discovery_selector_cache,
)

logger = logging.getLogger(__name__)


_STRUCTURED_SELECTOR_EVENTS = {
    SELECTOR_EVENT_STRUCTURED_CREATED,
    SELECTOR_EVENT_STRUCTURED_UPDATED,
    SELECTOR_EVENT_STRUCTURED_REVOKED,
}


@register_handler(
    "spec.version_bumped",
    "spec.semantic_changed",
    SELECTOR_EVENT_STRUCTURED_CREATED,
    SELECTOR_EVENT_STRUCTURED_UPDATED,
    SELECTOR_EVENT_STRUCTURED_REVOKED,
)
class DiscoverySelectorCacheInvalidationHandler:
    """Invalidate selector metadata when structured spec content changes."""

    async def handle(self, event: DomainEvent, session: AsyncSession) -> None:
        del session
        started = time.perf_counter()
        spec_id = getattr(event, "spec_id", None)
        selector_event_type = (
            event.event_type
            if event.event_type in _STRUCTURED_SELECTOR_EVENTS
            else SELECTOR_EVENT_SPEC_UPDATED
        )
        child_type = (
            getattr(event, "entity_type", None)
            if event.event_type in _STRUCTURED_SELECTOR_EVENTS
            else None
        )
        result = get_default_discovery_selector_cache().invalidate_event(
            {
                "event_type": selector_event_type,
                "board_id": event.board_id,
                "spec_id": spec_id,
                "child_type": child_type,
            }
        )
        duration_ms = round((time.perf_counter() - started) * 1000, 3)
        logger.info(
            "discovery.selector_cache.spec_event_invalidated count=%s",
            result.invalidated_count,
            extra={
                "event": "discovery.selector_cache.spec_event_invalidated",
                "board_id": event.board_id,
                "spec_id": spec_id or "none",
                "entity_type": child_type or "none",
                "outcome": result.outcome,
                "invalidated_count": result.invalidated_count,
                "duration_ms": duration_ms,
            },
        )
        logger.info(
            "spec_structured_entity_discovery_invalidation_total event_type=%s "
            "board=%s spec_id=%s entity_type=%s outcome=%s count=%s duration_ms=%s",
            selector_event_type,
            event.board_id,
            spec_id or "none",
            child_type or "none",
            result.outcome,
            result.invalidated_count,
            duration_ms,
            extra={
                "event": "spec_structured_entity_discovery_invalidation_total",
                "metric_name": "spec_structured_entity_discovery_invalidation_total",
                "board_id": event.board_id,
                "spec_id": spec_id or "none",
                "entity_type": child_type or "none",
                "outcome": result.outcome,
                "duration_ms": duration_ms,
            },
        )
