from dataclasses import replace

import pytest

from okto_pulse.core.domain.delivery_completeness import calculate_delivery_completeness
from okto_pulse.core.domain.delivery_evidence import DeliveryBinding, DeliveryContribution, DeliveryObligation, evaluate_delivery_coverage
from okto_pulse.core.domain.effective_delivery_coverage import EffectiveDeliveryContext, DeliveryScopeAttestation
from okto_pulse.core.domain.enums import CardStatus
from test_effective_delivery_coverage import case
from test_delivery_evidence_domain import SNAPSHOT


def snapshot(completed=3, total=5):
    inventory, implementations, _ = case()
    template = implementations[0]
    bindings = tuple(DeliveryBinding(f'fr:r{i}', 'a' * 64) for i in range(total))
    rows = tuple(replace(inventory.rows[0], binding=binding, contributions=(inventory.rows[0].contributions[0],)) for binding in bindings)
    proofs = tuple(replace(template,
        fact=replace(template.fact, id=f'proof{i}', card_status=CardStatus.IN_PROGRESS, bindings=(binding,),
                     contributions=(DeliveryContribution(binding, 'complete' if i < completed else 'partial', ('execution-receipt',)),)),
        scopes=(DeliveryScopeAttestation(binding, 'a' * 64),)) for i, binding in enumerate(bindings))
    return replace(SNAPSHOT, obligations=tuple(DeliveryObligation(b, b.obligation_ref) for b in bindings),
        implementations=tuple(p.fact for p in proofs), tests=(), waivers=(),
        effective_context=EffectiveDeliveryContext(replace(inventory, rows=rows), proofs, (), frozenset({'automated_test'})))


@pytest.mark.parametrize('completed,total,percent', [(0,5,0),(3,5,60),(5,5,100),(2,3,66),(200,201,99)])
def test_counts_equal_obligations_before_done_without_approving_delivery(completed,total,percent):
    value = snapshot(completed,total)
    result = calculate_delivery_completeness(value, 'ui')
    assert (result.percent,result.completed,result.total) == (percent,completed,total)
    assert not evaluate_delivery_coverage(value).allowed


@pytest.mark.parametrize('change', ['stale','scope','foreign','rejected','partial','revoked'])
def test_invalid_or_partial_evidence_loses_credit(change):
    value = snapshot(1,1)
    scoped = value.effective_context.implementations[0]
    fact = scoped.fact
    if change == 'stale': fact = replace(fact, executions=tuple(replace(p,current_accepted_execution=False) for p in fact.executions))
    if change == 'scope': scoped = replace(scoped, scopes=(replace(scoped.scopes[0],scope_sha256='b'*64),))
    if change == 'foreign': fact = replace(fact,card_id='other')
    if change == 'rejected': fact = replace(fact,card_status=CardStatus.REJECTED)
    if change == 'partial': fact = replace(fact,contributions=tuple(replace(c,contribution='partial') for c in fact.contributions))
    facts = () if change == 'revoked' else (fact,)
    value = replace(value, implementations=facts,effective_context=replace(value.effective_context,implementations=() if change=='revoked' else (replace(scoped,fact=fact),)))
    assert calculate_delivery_completeness(value,'ui').percent == 0


def test_no_fake_percentage_for_missing_incomplete_or_duplicate_scope():
    value=snapshot()
    for invalid in [replace(value,complete=False),replace(value,effective_context=False),replace(value,obligations=()),
                    replace(value,obligations=value.obligations*2),
                    replace(value,obligations=(DeliveryObligation(DeliveryBinding('card:ui','a'*64),'Card'),))]:
        assert calculate_delivery_completeness(invalid,'ui').percent is None


def test_multiple_records_do_not_inflate_obligation_count():
    value=snapshot(1,1)
    duplicate=replace(value.effective_context.implementations[0],fact=replace(value.implementations[0],id='another'))
    value=replace(value,implementations=(*value.implementations,duplicate.fact),effective_context=replace(value.effective_context,implementations=(*value.effective_context.implementations,duplicate)))
    assert calculate_delivery_completeness(value,'ui').completed == 1
