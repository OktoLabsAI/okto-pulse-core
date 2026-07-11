"""Compatibility import for the storage-neutral event-delivery processor."""

from okto_pulse.core.application.domain_event_delivery import (
    BACKOFF_BASE,
    BACKOFF_CAP_SECONDS,
    DRAIN_BATCH_SIZE,
    MAX_ATTEMPTS,
    DomainEventDeliveryProcessor,
    event_from_stored,
)


EventDeliveryProcessor = DomainEventDeliveryProcessor
POLL_INTERVAL_SECONDS = 5.0

__all__ = [
    "BACKOFF_BASE",
    "BACKOFF_CAP_SECONDS",
    "DRAIN_BATCH_SIZE",
    "EventDeliveryProcessor",
    "MAX_ATTEMPTS",
    "POLL_INTERVAL_SECONDS",
    "event_from_stored",
]
