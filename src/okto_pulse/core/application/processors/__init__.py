"""Public, task-free application processors for edition-owned runners."""

from .board_erasure import BoardErasureProcessor
from .consolidation import ConsolidationProcessor
from .event_delivery import EventDeliveryProcessor
from .global_outbox import GlobalOutboxProcessor
from .session_cleanup import SessionCleanupProcessor

__all__ = [
    "BoardErasureProcessor",
    "ConsolidationProcessor",
    "EventDeliveryProcessor",
    "GlobalOutboxProcessor",
    "SessionCleanupProcessor",
]
