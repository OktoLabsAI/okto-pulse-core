"""Durable acknowledgement for the A3 checklist-binding audit outbox."""

from __future__ import annotations

import logging

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import ChecklistBindingChanged

logger = logging.getLogger(__name__)


@register_handler(ChecklistBindingChanged.event_type)
class ChecklistBindingAuditHandler:
    """Acknowledge an immutable binding event without mutating derived state."""

    async def handle(
        self,
        event: ChecklistBindingChanged,
        session: object,
    ) -> None:
        del session
        logger.info(
            "checklist.binding_changed board_id=%s version=%s mode=%s",
            event.board_id,
            event.binding_version,
            event.mode,
            extra={
                "event": ChecklistBindingChanged.event_type,
                "board_id": event.board_id,
                "binding_version": event.binding_version,
                "mode": event.mode,
                "change_source": event.change_source,
            },
        )


__all__ = ["ChecklistBindingAuditHandler"]
