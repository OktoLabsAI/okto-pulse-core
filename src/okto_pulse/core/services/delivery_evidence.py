"""Provider-neutral inventory and the single delivery readiness gate."""

from collections.abc import Mapping

from okto_pulse.core.domain.delivery_evidence import (
    CardDeliveryScope,
    DeliveryScope,
)
from okto_pulse.core.domain.delivery_assessment import (
    assess_delivery_gate, assess_obligation_delivery, assess_test_delivery,
    resolve_delivery_gate_policy,
)
from okto_pulse.core.application.delivery_errors import DeliveryGateBlocked
from okto_pulse.core.ports.relational_application import (
    require_relational_application_adapter,
)


def delivery_store(session: object):
    factory = getattr(
        require_relational_application_adapter(), "delivery_evidence", None
    )
    if factory is None:
        raise ValueError("delivery_evidence_adapter_unavailable")
    return factory(session)


def card_delivery_store(session: object):
    """Resolve the card-scoped seam of the delivery adapter, failing closed."""
    store = delivery_store(session)
    if not hasattr(store, "load_card_snapshot") or not hasattr(
        store, "record_card"
    ):
        raise ValueError("card_delivery_evidence_adapter_unavailable")
    return store


async def require_spec_delivery(
    session: object,
    spec: object,
    *,
    for_update: bool = False,
    board: object | None = None,
) -> None:
    """Spec→done delivery gate (FR-4).

    The public snapshot port derives the Spec rollup from the canonical Card
    ledgers under the requested edition; no alternate adapter method is used.
    The same board setting (delivery_evidence_gate) governs this seam: in
    ``advisory`` the aggregated verdict is surfaced through the rollup read
    without blocking the transition.
    """
    if resolve_delivery_gate_mode(board) != "blocking":
        return
    if getattr(spec, "skip_delivery_evidence", False):
        # Spec-level override following the Tests-tab skip-flag pattern:
        # the transition is allowed while the rollup projection keeps
        # showing the truthful coverage verdict. The evaluator is untouched.
        return
    store = delivery_store(session)
    scope = DeliveryScope(spec.board_id, spec.id, int(spec.edition))
    if for_update:
        await store.lock_scope(scope)
    snapshot = await store.load_snapshot(scope)
    assessment = assess_obligation_delivery(snapshot)
    result = assess_delivery_gate(assessment)
    if not result.allowed:
        raise DeliveryGateBlocked(assessment, result)


def resolve_delivery_gate_mode(board: object | None) -> str:
    """Resolve the current policy; refuse incompatible values without conversion."""
    settings = getattr(board, "settings", None) if board is not None else None
    return resolve_delivery_gate_policy(settings)


async def require_card_delivery(
    session: object,
    card: object,
    spec: object,
    *,
    board: object | None = None,
    prospective_report: Mapping | None = None,
) -> None:
    """Card→done delivery gate (FR-3).

    In ``blocking`` mode, normal/bug cards require accepted implementation
    proof. Test cards require current verification proof for every assigned
    scenario, including the cross-card implementation association. The
    ``advisory`` mode leaves the verdict visible without blocking.

    The shared factual assessment accepts current complete implementation
    before DONE. Verification and lifecycle retain their own typed phases;
    neither is reclassified through a list of exception strings here.
    """
    if resolve_delivery_gate_mode(board) != "blocking":
        return
    raw_type = getattr(card, "card_type", None)
    card_type = str(getattr(raw_type, "value", raw_type or "normal"))
    if card_type not in {"normal", "bug", "test"}:
        return
    spec_id = getattr(card, "spec_id", None)
    if not spec_id or spec_id != getattr(spec, "id", None):
        return
    if card_type == "test":
        # The Spec rollup includes implementation records on sibling cards.
        snapshot = await delivery_store(session).load_snapshot(
            DeliveryScope(getattr(card, "board_id"), spec_id, int(getattr(spec, "edition")))
        )
        assessment = assess_test_delivery(snapshot, card_id=getattr(card, "id"),
            scenario_ids=tuple(getattr(card, "test_scenario_ids", None) or ()))
        result = assess_delivery_gate(assessment)
        if not result.allowed:
            raise DeliveryGateBlocked(assessment, result)
        return

    store = card_delivery_store(session)
    scope = CardDeliveryScope(
        card.board_id, card.id, spec_id, int(spec.edition)
    )
    snapshot = await store.load_card_snapshot(scope, **(
        {"prospective_report": prospective_report} if prospective_report is not None else {}
    ))
    assessment = assess_obligation_delivery(snapshot, card_id=card.id, card_type=card_type)
    result = assess_delivery_gate(assessment)
    if not result.allowed:
        raise DeliveryGateBlocked(assessment, result)
