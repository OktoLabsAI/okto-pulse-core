"""Single operational-freeze policy for cards awaiting human rework."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.domain.card_completion import card_is_rejected
from okto_pulse.core.services.card_errors import CardOperationError
from okto_pulse.core.domain.card_transition import completed_spec_normal_work_block
from okto_pulse.core.domain.enums import CardType
from okto_pulse.core.ports.application_persistence import get_application_persistence_port
from okto_pulse.core.ports.relational_application import require_relational_application_adapter


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


async def require_normal_card_spec_content_allowed(
    context: Any, *, board_id: str, card_type: CardType | str,
    spec_ids: tuple[str | None, ...], operation: str, card: Any | None = None,
) -> None:
    """Fence normal content against Spec completion through public ports.

    Both the current and proposed parent must be checked before reparenting.
    The caller owns the transaction. The existing Board lifecycle lock precedes
    Spec and Card locks, just as it does for execution/dependency admission.
    Reads/collaboration and Bug/Test work do not acquire this additional fence.
    """
    if (getattr(card_type, "value", card_type) or "normal") != CardType.NORMAL:
        return
    parent_ids = sorted({str(value) for value in spec_ids if value})
    if not parent_ids:
        return
    application = get_application_persistence_port()

    def require_open(spec: Any, spec_id: str) -> None:
        if spec is None or spec.board_id != board_id:
            raise ValueError("Spec not found on this board")
        block = completed_spec_normal_work_block(
            card_type=CardType.NORMAL, spec_status=spec.status,
            card_id=getattr(card, "id", None), spec_id=spec_id,
        )
        if block is not None:
            raise CardOperationError(
                block.code, block.detail, remediation=block.remediation,
                facts={**block.facts, "operation": operation},
            )

    # An already closed parent is rejected before any lock write or audit.
    for spec_id in parent_ids:
        require_open(await application.get(context, entity="spec", record_id=spec_id), spec_id)

    persistence = require_relational_application_adapter().spec_dependencies(context)
    await persistence.acquire_board_graph_lock(board_id)
    for spec_id in parent_ids:
        require_open(await persistence.get_spec_snapshot(
            board_id=board_id, spec_id=spec_id, for_update=True,
        ), spec_id)
    if card is not None and not await application.fence(
        context, entity="card", record_id=card.id,
        expected_values={
            "board_id": board_id, "spec_id": card.spec_id,
            "card_type": card.card_type,
        },
    ):
        raise CardOperationError(
            "card_spec_state_conflict", "The task's type or Spec changed before the content write.",
            remediation="refresh_card", facts={"card_id": card.id, "operation": operation},
        )


__all__ = ["require_card_operational_mutation_allowed", "require_normal_card_spec_content_allowed"]
