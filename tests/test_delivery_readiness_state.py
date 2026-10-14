"""P06 current qualification never borrows authority from historical Done."""
from dataclasses import replace

import pytest

from okto_pulse.core.domain.delivery_assessment import (
    aggregate_delivery_metrics, assess_delivery_gate, assess_obligation_delivery,
    qualify_delivery, resolve_delivery_gate_policy,
)
from okto_pulse.core.services.delivery_evidence import resolve_delivery_gate_mode
from okto_pulse.core.services.delivery_readiness import EvaluatedDeliveryReadiness, PreparedDeliveryReadiness
from okto_pulse.core.models.delivery_readiness import ReadinessReadQuery
from test_delivery_obligation_assessment import snapshot, with_facts
from test_delivery_readiness_shared_evaluation import changed, prepared
from test_delivery_readiness_paging import Cursor


@pytest.mark.parametrize('kind,state,proof', [
    ('current', 'ready', True), ('missing_contribution', 'implementation_pending', False),
    ('missing_association', 'verification_pending', False), ('failed', 'verification_failed', False),
    ('incomplete', 'unknown', None), ('partial', 'implementation_pending', False),
    ('stale', 'implementation_pending', False), ('missing_topology', 'unknown', None),
])
@pytest.mark.parametrize('stored', ['in_progress', 'done'])
def test_current_qualification_never_uses_done_as_proof(kind, state, proof, stored):
    assessment = assess_obligation_delivery(changed(kind))
    original = assessment
    result = qualify_delivery(assessment, gate=assess_delivery_gate(assessment), stored_status=stored,
        lifecycle_check='not_evaluated')
    assert result.state == state and result.proof_complete is proof
    assert result.qualification_is_current is (False if proof is False else None)
    assert result.historical_divergence == (stored == 'done' and proof is not True)
    assert assessment == original


@pytest.mark.parametrize('check,qualified', [('allowed', True), ('blocked', False), ('not_evaluated', None)])
def test_full_proof_does_not_claim_an_unevaluated_or_blocked_native_closure(check, qualified):
    assessment = assess_obligation_delivery(snapshot())
    result = qualify_delivery(assessment, gate=assess_delivery_gate(assessment), stored_status='done',
        lifecycle_check=check)
    assert result.proof_complete is True and result.qualification_is_current is qualified
    assert result.state == ('delivered' if qualified is True else 'ready')
    assert result.historical_divergence == (qualified is False)


@pytest.mark.parametrize('stored', ['in_progress', 'done'])
@pytest.mark.parametrize('policy', [{'mode': 'advisory'}, {'spec_override': True}, {}])
def test_only_effective_exceptions_change_label_without_changing_proof(stored, policy):
    assessment = assess_obligation_delivery(changed('waived') if not policy else changed('failed'))
    metrics = aggregate_delivery_metrics(assessment)
    gate = assess_delivery_gate(assessment, **policy)
    result = qualify_delivery(assessment, gate=gate, stored_status=stored, lifecycle_check='allowed')
    assert result.state == ('delivered_with_exceptions' if stored == 'done' else 'ready_with_exceptions')
    assert result.proof_complete is False and result.qualification_is_current is True
    assert not result.historical_divergence
    assert aggregate_delivery_metrics(assessment) == metrics
    complete = assess_obligation_delivery(snapshot())
    result = qualify_delivery(complete, gate=assess_delivery_gate(complete, **policy),
        stored_status=stored, lifecycle_check='allowed')
    assert result.state == ('delivered' if stored == 'done' else 'ready')


def test_unknown_has_priority_over_advisory_and_failure_has_priority_over_implementation_gap():
    unknown = assess_obligation_delivery(changed('incomplete'))
    result = qualify_delivery(unknown, gate=assess_delivery_gate(unknown, mode='advisory'),
        stored_status='done', lifecycle_check='allowed')
    assert result.state == 'unknown' and result.proof_complete is None
    assert result.qualification_is_current is None and result.historical_divergence
    failed = assess_obligation_delivery(changed('failed'))
    missing = assess_obligation_delivery(changed('missing_contribution'))
    both = replace(failed, obligations=(replace(failed.obligations[0],
        implementation=missing.obligations[0].implementation),))
    assert qualify_delivery(both, gate=assess_delivery_gate(both), stored_status='in_progress',
        lifecycle_check='allowed').state == 'verification_failed'


def test_normal_card_can_finish_implementation_while_technical_delivery_is_pending():
    value = with_facts(snapshot(), tests=())
    assessment = assess_obligation_delivery(value, card_id='ui', card_type='normal')
    gate = assess_delivery_gate(assessment)
    assert gate.allowed
    result = qualify_delivery(assessment, gate=gate, stored_status='done', lifecycle_check='allowed')
    assert result.state == 'verification_pending' and result.proof_complete is False
    assert result.qualification_is_current is False and result.historical_divergence


@pytest.mark.parametrize('identity', [None, 'ui', 'test-card-1'])
def test_authorized_context_and_paged_response_share_facts_without_claiming_native_transition(identity):
    request = prepared(snapshot())
    reader = EvaluatedDeliveryReadiness(request, actor_id='reader')
    context = reader.context(identity)
    assert context.proof_complete is True and context.delivery_state == 'ready'
    assert context.lifecycle.check_status == 'not_evaluated'
    assert context.lifecycle.qualification_is_current is None
    assert context.lifecycle.other_blocker_count is None
    assert context.lifecycle.reason_codes == ('native_transition_context_required',)
    assert context.lifecycle.context_refs == (f'card:{identity}' if identity else 'spec:spec',)
    assert context.implementation.percent == (None if identity == 'test-card-1' else 100)
    assert context.verification.percent == 100
    response = PreparedDeliveryReadiness(context, reader.diagnostics(identity), cursor=Cursor()).page(
        ReadinessReadQuery(board_id='board', spec_id='spec', card_id=identity), actor_id='reader')
    assert response.snapshot_id == request.snapshot_id
    assert response.summary.root_causes == len(response.issues)


def test_context_keeps_spec_override_scope_and_counts_pending_units_once():
    request = replace(prepared(with_facts(snapshot(), implementations=(), tests=())), skip_delivery_evidence=True)
    reader = EvaluatedDeliveryReadiness(request, actor_id='reader')
    spec = reader.context()
    assert spec.summary.unresolved_units == 1
    assert spec.delivery_gate.exception_applied and spec.delivery_gate.status == 'allowed'
    assert spec.proof_complete is False and spec.delivery_state == 'ready_with_exceptions'
    card = reader.context('ui')
    assert not card.delivery_gate.exception_applied and card.delivery_gate.status == 'blocked'
    assert card.delivery_state == 'implementation_pending'


@pytest.mark.parametrize('settings,expected', [(None, 'blocking'), ({}, 'blocking'),
    ({'delivery_evidence_gate': 'advisory'}, 'advisory'), ({'delivery_evidence_gate': 'blocking'}, 'blocking')])
def test_read_and_mutation_share_policy_resolver(settings, expected):
    from types import SimpleNamespace
    assert resolve_delivery_gate_policy(settings) == resolve_delivery_gate_mode(SimpleNamespace(settings=settings)) == expected


@pytest.mark.parametrize('settings', [[], 'advisory', {'delivery_evidence_gate': True}, {'delivery_evidence_gate': None}])
def test_invalid_policy_is_not_coerced_into_authority(settings):
    with pytest.raises(ValueError, match='delivery_evidence_policy_invalid'):
        resolve_delivery_gate_policy(settings)
