"""One human verdict over canonical delivery facts, never a transition token."""

from typing import Literal

from pydantic import StrictBool

from .delivery_readiness import Count, Digest, Identity, ReadinessDTO, Version


TaskDeliveryState = Literal[
    "updating_or_unknown", "optional_or_waived", "verification_failed", "historical_done",
    "implementation_pending", "verification_pending", "completion_pending", "delivered", "ready",
]
TaskDeliveryReason = Literal[
    "updating", "unavailable", "assessment_incomplete", "optional", "waived",
    "verification_failed", "historical_done", "implementation_pending", "not_started",
    "paused", "cancelled", "verification_pending", "completion_pending",
    "completion_not_evaluated", "delivered", "ready",
]


class TaskDeliveryAction(ReadinessDTO):
    kind: Literal["record_progress", "submit_report", "conclude", "open_details", "retry"]
    label: str
    owner: Literal["user", "technical_owner", "system"]
    # Rechecked at mutation time; this only controls the current presentation.
    mutation_reauthorization_required: Literal[True] = True


class TaskDeliveryUpdate(ReadinessDTO):
    record_id: Identity
    content: str
    remaining: str | None
    submitted_at: str
    origin: Literal["human_submission", "technical_checkpoint"]
    text_truncated: StrictBool


class TaskDeliverySummary(ReadinessDTO):
    contract_version: Literal["task-delivery-summary/v1"] = "task-delivery-summary/v1"
    board_id: Identity
    spec_id: Identity
    card_id: Identity
    spec_edition: Version
    card_version: Version
    snapshot_id: Digest | None
    population_known: StrictBool
    evaluation_complete: StrictBool
    state: TaskDeliveryState
    reason_code: TaskDeliveryReason
    reason: str
    next_action: TaskDeliveryAction | None = None
    pending_count: Count | None
    historical_status: Identity
    last_update: TaskDeliveryUpdate | None = None
