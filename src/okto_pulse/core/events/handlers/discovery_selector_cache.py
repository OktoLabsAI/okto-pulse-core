"""Discovery selector cache invalidation handlers."""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import DomainEvent
from okto_pulse.core.services.discovery_selector_catalog import (
    SELECTOR_EVENT_SPEC_UPDATED,
    get_default_discovery_selector_cache,
)

logger = logging.getLogger(__name__)


@register_handler("spec.version_bumped", "spec.semantic_changed")
class DiscoverySelectorCacheInvalidationHandler:
    """Invalidate selector metadata when structured spec content changes."""

    async def handle(self, event: DomainEvent, session: AsyncSession) -> None:
        del session
        spec_id = getattr(event, "spec_id", None)
        result = get_default_discovery_selector_cache().invalidate_event(
            {
                "event_type": SELECTOR_EVENT_SPEC_UPDATED,
                "board_id": event.board_id,
                "spec_id": spec_id,
            }
        )
        logger.info(
            "discovery.selector_cache.spec_event_invalidated count=%s",
            result.invalidated_count,
            extra={
                "event": "discovery.selector_cache.spec_event_invalidated",
                "board_id": event.board_id,
                "spec_id": spec_id or "none",
                "outcome": result.outcome,
                "invalidated_count": result.invalidated_count,
            },
        )
