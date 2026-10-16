"""P06: gates consume typed phases, not percentages or string exclusions."""
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.domain.delivery_assessment import (
    assess_delivery_gate, assess_obligation_delivery, assess_test_delivery, aggregate_delivery_metrics,
    FactualReason, ReadinessReason, UnitState,
)
from okto_pulse.core.domain.delivery_evidence import DeliveryPhase, DeliveryWaiverFact
from okto_pulse.core.services import delivery_evidence as service
from test_delivery_obligation_assessment import snapshot, with_facts
from test_delivery_scenario_assessment import outcome


@pytest.mark.parametrize('failure', ['missing', 'failed', 'inconclusive', 'unknown', 'new_code'])
def test_G01_G02_verification_cannot_acquire_implementation_authority(failure):
    value = snapshot()
    if failure == 'missing':
        value = with_facts(value, tests=())
    elif failure in {'failed', 'inconclusive'}:
        value = outcome(value, 'scenario-ui', failure)
    elif failure == 'unknown':
        value = replace(value, effective_context=replace(value.effective_context, admitted_methods=None))
    result = assess_obligation_delivery(value, card_id='ui', card_type='normal')
    if failure == 'new_code':
        row = result.obligations[0]
        verification = replace(row.verification, state=UnitState.FAILED, intrinsic_state=UnitState.FAILED,
            proof_satisfied=False, reasons=(FactualReason(ReadinessReason.VERIFICATION_FAILED,
                native_reason_codes=('new_native_verification_failure',)),))
        result = replace(result, obligations=(replace(row, verification=verification),))
    before = aggregate_delivery_metrics(result)
    gate = assess_delivery_gate(result)
    assert gate.allowed and not gate.blockers and not gate.exception_refs
    assert before == aggregate_delivery_metrics(result)
    assert before.implementation.percent == 100


def test_G03_G04_spec_requires_verification_and_native_evidence_lifecycle():
    value = with_facts(snapshot(), tests=())
    blocked = assess_delivery_gate(assess_obligation_delivery(value))
    assert blocked.status == 'blocked'
    assert {b.phase for b in blocked.blockers} == {'verification'}
    value = snapshot()
    value = with_facts(value, tests=tuple(replace(t, fact=replace(t.fact, card_status='in_progress'))
        for t in value.effective_context.tests))
    assessment = assess_obligation_delivery(value)
    metrics = aggregate_delivery_metrics(assessment)
    assert metrics.implementation.percent == metrics.verification.percent == 100
    gate = assess_delivery_gate(assessment)
    assert gate.status == 'blocked' and {b.phase for b in gate.blockers} == {'lifecycle'}
    assert all(r.code == 'lifecycle_card_open' for b in gate.blockers for r in b.reasons)


@pytest.mark.parametrize('policy', [{'mode': 'advisory'}, {'spec_override': True}])
def test_G05_G06_effective_override_does_not_change_metrics_or_fabricate_exception(policy):
    complete = assess_obligation_delivery(snapshot())
    assert assess_delivery_gate(complete, **policy).exception_refs == ()
    pending = assess_obligation_delivery(with_facts(snapshot(), tests=()))
    metrics = aggregate_delivery_metrics(pending)
    gate = assess_delivery_gate(pending, **policy)
    assert gate.allowed and gate.blockers and len(gate.exception_refs) == 1
    assert aggregate_delivery_metrics(pending) == metrics and metrics.verification.percent == 0


def test_spec_override_does_not_extend_to_card_and_test_seam_does_not_require_pass():
    missing = assess_obligation_delivery(with_facts(snapshot(), implementations=()), card_id='ui', card_type='normal')
    assert not assess_delivery_gate(missing, spec_override=True).allowed
    tests = assess_test_delivery(outcome(snapshot(), 'scenario-ui', 'failed'), card_id='test-card-1', scenario_ids=('scenario-ui',))
    assert assess_delivery_gate(tests).allowed
    assert aggregate_delivery_metrics(tests).verification.percent == 0


@pytest.mark.parametrize('disposition', ['risk_accepted', 'not_applicable'])
def test_human_exception_retains_its_native_phase_and_never_creates_proof(disposition):
    value = with_facts(snapshot(), tests=())
    waiver = DeliveryWaiverFact('waiver', value.scope, value.obligations[0].binding, DeliveryPhase.TEST,
        'Human decision', 'human', 'authority', True, disposition)
    assessment = assess_obligation_delivery(replace(value, waivers=(waiver,)))
    gate = assess_delivery_gate(assessment)
    assert gate.allowed and gate.exception_refs == (('waiver',) if disposition == 'risk_accepted' else ())
    assert aggregate_delivery_metrics(assessment).verification.percent == (0 if disposition == 'risk_accepted' else None)


def test_absent_population_is_unknown_not_vacuous_success():
    result = assess_obligation_delivery(replace(snapshot(), effective_context=None))
    assert assess_delivery_gate(result).status == 'unknown'


def test_risk_waiver_does_not_turn_an_unavailable_verifier_into_known_gate_authority():
    value = snapshot()
    waiver = DeliveryWaiverFact('waiver', value.scope, value.obligations[0].binding, DeliveryPhase.TEST,
        'Human decision', 'human', 'authority', True, 'risk_accepted')
    value = replace(value, waivers=(waiver,), effective_context=replace(value.effective_context, admitted_methods=None))
    result = assess_delivery_gate(assess_obligation_delivery(value))
    assert result.status == 'unknown' and result.exception_refs == ()


@pytest.mark.asyncio
async def test_G10_spec_write_fence_precedes_loading_and_no_readiness_cache_is_accepted(monkeypatch):
    observed = []
    async def lock(scope):
        observed.append('fence')
    async def load(scope):
        observed.append('current_facts')
        return with_facts(snapshot(), tests=())
    monkeypatch.setattr(service, 'delivery_store', lambda _: SimpleNamespace(lock_scope=lock, load_snapshot=load))
    spec = SimpleNamespace(board_id='board', id='spec', edition=2)
    with pytest.raises(service.DeliveryGateBlocked) as error:
        await service.require_spec_delivery(None, spec, for_update=True)
    assert observed == ['fence', 'current_facts']
    assert error.value.gate.status == 'blocked'
    assert aggregate_delivery_metrics(error.value.assessment).verification.percent == 0


@pytest.mark.asyncio
async def test_prospective_report_is_passed_to_native_loader_without_persistence(monkeypatch):
    value = snapshot()
    load = AsyncMock(return_value=value)
    monkeypatch.setattr(service, 'card_delivery_store', lambda _: SimpleNamespace(load_card_snapshot=load))
    report = {'delivery_manifest': {'selection': 'already-validated-by-native-path'}}
    await service.require_card_delivery(None, SimpleNamespace(id='ui', board_id='board', spec_id='spec', card_type='normal'),
        SimpleNamespace(id='spec', edition=2), prospective_report=report)
    assert load.call_args.kwargs['prospective_report'] is report
