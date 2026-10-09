"""Informational implementation coverage, independent of lifecycle approval."""

from dataclasses import dataclass, replace
from hashlib import sha256
import json

from okto_pulse.core.domain.delivery_evidence import (
    DeliveryEvidenceSnapshot, implementation_binding_ready,
)
from okto_pulse.core.domain.effective_delivery_coverage import (
    EffectiveDeliveryContext, implementation_scope_current, evaluate_effective_snapshot,
)
from okto_pulse.core.domain.enums import CardStatus, CardType


@dataclass(frozen=True, slots=True)
class DeliveryCompleteness:
    percent: int | None
    total: int
    planned: int = 0
    implemented: int = 0
    verified: int = 0
    accepted: int = 0
    scope_sha256: str | None = None
    reason: str | None = None


def calculate_delivery_completeness(snapshot: DeliveryEvidenceSnapshot, card_id: str) -> DeliveryCompleteness:
    """Exclusive maturity stages 0/50/80/100 over the current Spec snapshot.

    This read model never authorizes a transition. Partial claims, progress and
    waivers give no credit. Verification reads other Cards' exact-base evidence;
    acceptance additionally requires the real lifecycle completion gates.
    """
    context = snapshot.effective_context
    if (snapshot.complete is not True or not isinstance(context, EffectiveDeliveryContext)
        or not context.inventory.complete):
        return DeliveryCompleteness(None, 0, reason='scope_incomplete')
    population = tuple(o.binding for o in snapshot.obligations)
    inventory_bindings = tuple(row.binding for row in context.inventory.rows)
    if (len(set(population)) != len(population)
        or len({b.obligation_ref for b in population}) != len(population)
        or len(set(inventory_bindings)) != len(inventory_bindings)
        or set(population) != set(inventory_bindings)):
        return DeliveryCompleteness(None, 0, reason='scope_incomplete')
    rows = tuple(replace(row, contributions=tuple(c for c in row.contributions if c.card_id == card_id))
        for row in context.inventory.rows if any(c.card_id == card_id for c in row.contributions))
    if not rows or any(row.binding.obligation_ref.startswith('card:') for row in rows):
        return DeliveryCompleteness(None, len(rows), reason='scope_missing')
    if any(row.blockers or len(row.contributions) != 1 for row in rows):
        return DeliveryCompleteness(None, len(rows), reason='scope_incomplete')
    for facts, scoped in ((snapshot.implementations, context.implementations), (snapshot.tests, context.tests)):
        if (len({f.id for f in facts}) != len(facts) or len(scoped) != len(facts)
            or {s.fact.id: s.fact for s in scoped} != {f.id: f for f in facts}):
            return DeliveryCompleteness(None, len(rows), reason='scope_incomplete')
    bindings = {row.binding for row in rows}
    digest = sha256(json.dumps([(row.binding.obligation_ref, row.binding.semantic_sha256,
        row.contributions[0].scope_sha256) for row in sorted(rows, key=lambda row: row.binding.obligation_ref)],
        separators=(',', ':')).encode()).hexdigest()
    eligible = {CardStatus.STARTED, CardStatus.IN_PROGRESS, CardStatus.VALIDATION, CardStatus.ON_HOLD, CardStatus.DONE}
    implementations = tuple(s for s in context.implementations if s.fact.card_id == card_id
        and s.fact.card_type == CardType.NORMAL and s.fact.card_status in eligible)
    tests = tuple(s for s in context.tests if s.fact.card_status in eligible)
    snapshot = replace(snapshot, obligations=tuple(o for o in snapshot.obligations if o.binding in bindings),
        implementations=tuple(s.fact for s in implementations), tests=tuple(s.fact for s in tests), waivers=(),
        effective_context=replace(context, inventory=replace(context.inventory, rows=rows), implementations=implementations, tests=tests))
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
    # Private informational copies ignore only completion, reusing the same
    # receipt/scope/method/criterion checks. Never persist or expose these as
    # gate authorization; the actual acceptance evaluation keeps real statuses.
    pre_impl = tuple(replace(s, fact=replace(s.fact, card_status=CardStatus.DONE)) for s in implementations)
    pre_test = tuple(replace(s, fact=replace(s.fact, card_status=CardStatus.DONE)) for s in tests)
    pre_acceptance = replace(snapshot, implementations=tuple(s.fact for s in pre_impl), tests=tuple(s.fact for s in pre_test),
        effective_context=replace(snapshot.effective_context, implementations=pre_impl, tests=pre_test))
    verified = {r.obligation.binding for r in evaluate_effective_snapshot(pre_acceptance).rows if r.test_satisfied} & ready
    accepted = {r.obligation.binding for r in evaluate_effective_snapshot(snapshot).rows if r.test_satisfied} & verified
    counts = (len(bindings - ready), len(ready - verified), len(verified - accepted), len(accepted))
    # Floor prevents rounded 100% while any obligation remains below acceptance.
    percent = (counts[1] * 50 + counts[2] * 80 + counts[3] * 100) // len(bindings)
    return DeliveryCompleteness(percent, len(bindings), *counts, scope_sha256=digest)
