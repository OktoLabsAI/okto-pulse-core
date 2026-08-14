"""Single operational-freeze policy for cards awaiting human rework."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.domain.card_completion import card_is_rejected
from okto_pulse.core.services.card_errors import CardOperationError


def require_card_operational_mutation_allowed(
    card: Any,
    *,
    operation: str,
) -> None:
    """Reject implementation/evidence/resource writes while a card is Rejected.

    Collaboration, reads, same-column ordering, investigation preflight and
    implementation-target resolution renewal deliberately do not call this
    guard.  Every other card-owned writer must call it before its first effect.
    """

    if not card_is_rejected(card):
        return
    raise CardOperationError(
        "card_rejected_rework_handoff_required",
        (
            "This card is Rejected and its implementation is frozen until an "
            "executor moves it to In Progress and starts a new execution attempt."
        ),
        remediation="move_rejected_card_to_in_progress_before_editing",
        facts={
            "card_id": str(getattr(card, "id", "")),
            "operation": operation,
            "status": "rejected",
        },
    )


__all__ = ["require_card_operational_mutation_allowed"]
