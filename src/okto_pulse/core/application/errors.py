"""Public application errors shared by inbound adapters."""

from okto_pulse.core.kg.governance import BoostPersistError
from okto_pulse.core.services.board_governance import QASelfAnsweringNotAllowedError
from okto_pulse.core.services.cancellation import CancellationReasonRequiredError
from okto_pulse.core.services.cognitive_effectiveness_service import (
    CognitiveEffectivenessError,
)
from okto_pulse.core.services.kg_health_service import BoardNotFoundError
from okto_pulse.core.services.main import (
    AmbiguityGateError,
    CARD_RESOURCE_READ_ONLY_MESSAGE,
    CardOperationError,
    CardResourceReadOnlyError,
    InvalidTopicMergeError,
    SprintOperationError,
    TopicNameConflictError,
    TopicNotEmptyError,
    TopicOperationError,
)
from okto_pulse.core.services.resource_gate import ResourceGateError

__all__ = [
    "AmbiguityGateError",
    "BoardNotFoundError",
    "BoostPersistError",
    "CARD_RESOURCE_READ_ONLY_MESSAGE",
    "CancellationReasonRequiredError",
    "CardOperationError",
    "CardResourceReadOnlyError",
    "CognitiveEffectivenessError",
    "InvalidTopicMergeError",
    "QASelfAnsweringNotAllowedError",
    "ResourceGateError",
    "SprintOperationError",
    "TopicNameConflictError",
    "TopicNotEmptyError",
    "TopicOperationError",
]
