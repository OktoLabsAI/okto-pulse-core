"""Project qualified inherited BR ownership without inventing task links."""

from collections.abc import Mapping

from okto_pulse.core.domain.delivery_inventory import COLLECTIONS
from okto_pulse.core.domain.implementation_responsibility import (
    resolve_implementation_responsibility,
)
from okto_pulse.core.domain.requirement_verification_resolution import (
    resolve_requirement_verification,
)


def inherited_business_rule_task_ids(spec, cards, *, collections=None):
    """Reuse delivery responsibility; an incomplete population earns no credit.

    Direct links retain their existing coverage semantics. This projection only
    supplies the inherited path already authored through qualified FRs and ACs;
    it neither verifies delivery nor mutates those canonical relationships.
    """
    if cards is None or not getattr(spec, "id", None) or not getattr(spec, "board_id", None):
        return {}
    values = {field: getattr(spec, field, None) for _, field in COLLECTIONS}
    values.update(collections or {})
    qualification = resolve_requirement_verification(spec_id=spec.id, collections=values)
    fields = ("id", "board_id", "spec_id", "card_type", "status", "archived")
    facts = [
        {
            field: (card.get(field) if isinstance(card, Mapping) else getattr(card, field, None))
            for field in fields
        }
        for card in cards
    ]
    plan = resolve_implementation_responsibility(
        board_id=spec.board_id, spec_id=spec.id,
        collections=values, cards=facts, qualification=qualification,
    )
    if not plan.population_complete:
        return {}
    return {
        row.requirement_id: {fact.card_id for fact in row.contributions}
        for row in plan.rows
        if row.requirement_type == "business_rule"
        and not row.blockers
        and row.contributions
        and all(fact.origin == "inherited" for fact in row.contributions)
    }
