"""Shared cancellation-justification policy (ITEM 17).

Single source of truth applied by the five status-transition flows
(``move_ideation`` / ``move_refinement`` / ``move_spec`` / ``move_sprint`` /
``move_card``):

- transition TO ``cancelled`` REQUIRES a non-empty ``cancellation_reason``
  (otherwise the structured ``cancellation_reason_required`` error is raised
  and no state is mutated). The reason + actor + timestamp are persisted on
  the entity, REPLACING any previous cancellation record.
- transition FROM ``cancelled`` to any other status (reopen) CLEARS the three
  ``cancellation_*`` fields.

The entity only needs the three nullable columns ``cancellation_reason`` /
``cancelled_at`` / ``cancelled_by`` (Ideation, Refinement, Spec, Sprint, Card).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

__all__ = [
    "CANCELLED_STATUS",
    "CancellationReasonRequiredError",
    "apply_cancellation_policy",
]

CANCELLED_STATUS = "cancelled"


class CancellationReasonRequiredError(ValueError):
    """Structured error: caller tried to cancel without a justification.

    Subclasses ``ValueError`` so untouched legacy handlers keep working;
    dedicated handlers map it to HTTP 400 / the MCP error envelope with the
    stable ``cancellation_reason_required`` code (same to_dict pattern as
    ``CardOperationError``).
    """

    code = "cancellation_reason_required"

    def __init__(self, entity_type: str) -> None:
        self.entity_type = entity_type
        self.message = (
            f"A non-empty cancellation_reason is required to move this "
            f"{entity_type} to 'cancelled'. Provide the justification for the "
            f"cancellation in the cancellation_reason field."
        )
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "entity_type": self.entity_type,
        }


def _status_value(status: Any) -> str | None:
    """Normalize an enum or raw string status to its string value."""
    if status is None:
        return None
    return getattr(status, "value", status)


def apply_cancellation_policy(
    entity: Any,
    *,
    entity_type: str,
    from_status: Any,
    to_status: Any,
    reason: str | None,
    actor_id: str,
) -> None:
    """Enforce and persist the cancellation-justification policy on ``entity``.

    Call from the single move/transition flow of each entity BEFORE the status
    write (after the state machine validated the transition). Mutates the
    entity in place; raises :class:`CancellationReasonRequiredError` when the
    target is ``cancelled`` and no non-empty reason was provided.
    """
    from_value = _status_value(from_status)
    to_value = _status_value(to_status)

    if to_value == CANCELLED_STATUS:
        cleaned = (reason or "").strip()
        if not cleaned:
            raise CancellationReasonRequiredError(entity_type)
        # A new cancellation always REPLACES the previous record.
        entity.cancellation_reason = cleaned
        entity.cancelled_at = datetime.now(timezone.utc)
        entity.cancelled_by = actor_id
    elif from_value == CANCELLED_STATUS and to_value != CANCELLED_STATUS:
        # Reopen: clear the justification entirely.
        entity.cancellation_reason = None
        entity.cancelled_at = None
        entity.cancelled_by = None
