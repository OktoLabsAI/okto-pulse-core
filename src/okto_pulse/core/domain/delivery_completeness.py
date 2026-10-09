"""Informational implementation coverage, independent of lifecycle approval."""

from dataclasses import dataclass

from okto_pulse.core.domain.delivery_evidence import (
    DeliveryEvidenceSnapshot, implementation_binding_ready,
)
from okto_pulse.core.domain.effective_delivery_coverage import (
    EffectiveDeliveryContext, implementation_scope_current,
)
from okto_pulse.core.domain.enums import CardStatus, CardType


@dataclass(frozen=True, slots=True)
class DeliveryCompleteness:
    percent: int | None
    completed: int
    total: int
    reason: str | None = None


def calculate_delivery_completeness(snapshot: DeliveryEvidenceSnapshot, card_id: str) -> DeliveryCompleteness:
    """Count each assigned obligation once; partial claims/waivers give no credit.

    Inputs are the current selected, unrevoked, server-authenticated snapshot.
    This projection deliberately does not require Done and never authorizes it.
    The undivided card fallback is not a measurable requirement inventory.
    """
    context = snapshot.effective_context
    bindings = tuple(obligation.binding for obligation in snapshot.obligations)
    if not bindings or any(binding.obligation_ref.startswith('card:') for binding in bindings):
        return DeliveryCompleteness(None, 0, len(bindings), 'scope_missing')
    if (snapshot.complete is not True or not isinstance(context, EffectiveDeliveryContext)
        or not context.inventory.complete or len(set(bindings)) != len(bindings)
        or len({binding.obligation_ref for binding in bindings}) != len(bindings)):
        return DeliveryCompleteness(None, 0, len(bindings), 'scope_incomplete')
    for binding in bindings:
        rows = [row for row in context.inventory.rows if row.binding == binding]
        if len(rows) != 1 or len([item for item in rows[0].contributions if item.card_id == card_id]) != 1:
            return DeliveryCompleteness(None, 0, len(bindings), 'scope_incomplete')
    ready = {
        binding for binding in bindings
        if any(fact.card_id == card_id and fact.card_type == CardType.NORMAL
               and fact.scope == snapshot.scope
               and fact.card_status not in {CardStatus.CANCELLED, CardStatus.REJECTED}
               and all(isinstance(value, str) and value.strip() for value in (fact.id, fact.actor_id, fact.explanation))
               and implementation_binding_ready(fact, binding)
               and implementation_scope_current(snapshot, fact, binding)
               for fact in snapshot.implementations)
    }
    # Floor prevents a rounded 100% while any obligation remains pending.
    return DeliveryCompleteness(len(ready) * 100 // len(bindings), len(ready), len(bindings))
