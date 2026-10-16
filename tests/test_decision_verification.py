from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.decision_verification import (
    DecisionVerification, OBLIGATION_FIELDS, decision_verification_plans,
    validate_decision_verification_references,
)
from okto_pulse.core.domain.delivery_evidence import DeliveryBinding, DeliveryObligation
from okto_pulse.core.domain.effective_delivery_inventory import EffectiveDeliveryObligation
from okto_pulse.core.services.main import CardService
from test_effective_delivery_coverage import case, evaluate


def spec(verification=None, **overrides):
    fields = {field: [] for field in OBLIGATION_FIELDS.values()}
    fields['functional_requirements'] = [{'id': 'fr-one', 'title': 'Behavior'}]
    return SimpleNamespace(id='spec', board_id='board',
        decisions=[{'id': 'dec-one', 'title': 'Choice', 'verification': verification}],
        **fields, **overrides)


def inspection():
    return {'inspection': {'condition': 'Deliver only the documented mock.',
        'scope_refs': [{'kind': 'spec', 'id': 'spec'}]}}


@pytest.mark.parametrize('value', [
    {}, {'obligation_refs': ['decision:other']}, {'obligation_refs': ['card:any']},
    {'obligation_refs': ['fr:one', 'fr:one']}, {'mode': 'none'},
    {'inspection': {'condition': ' ', 'scope_refs': []}},
])
def test_closed_contract_rejects_bypass_and_ambiguous_paths(value):
    with pytest.raises(ValidationError):
        DecisionVerification.model_validate(value)


def test_scope_is_inspected_without_inventing_task_or_implementation():
    current = spec(inspection())
    plan, = decision_verification_plans(current)
    assert plan.complete and plan.verification.method == 'inspection'
    assert not hasattr(current, 'cards')


@pytest.mark.parametrize('verification', [
    {'obligation_refs': ['fr:missing']},
    {'inspection': {'condition': 'Check scope', 'scope_refs': [{'kind': 'spec', 'id': 'other'}]}},
    {'inspection': {'condition': 'Check scope', 'scope_refs': [{'kind': 'obligation', 'id': 'decision:dec-one'}]}},
])
def test_cross_scope_or_unresolved_reference_is_not_a_plan(verification):
    current = spec(verification)
    assert not decision_verification_plans(current)[0].complete
    with pytest.raises(ValueError, match='unresolved'):
        validate_decision_verification_references(spec_id=current.id, decisions=current.decisions,
            collections={field: getattr(current, field) for field in OBLIGATION_FIELDS.values()})


def test_inactive_and_duplicate_targets_are_not_current_proof_routes():
    current = spec({'obligation_refs': ['fr:fr-one']})
    assert decision_verification_plans(current)[0].complete
    current.functional_requirements[0]['status'] = 'revoked'
    assert not decision_verification_plans(current)[0].complete
    current.functional_requirements[0]['status'] = 'active'
    current.functional_requirements.append(deepcopy(current.functional_requirements[0]))
    assert not decision_verification_plans(current)[0].complete


@pytest.mark.asyncio
async def test_planning_gate_does_not_require_result_or_ceremonial_task():
    await CardService.check_decisions_coverage(None, spec(inspection()), None)
    with pytest.raises(ValueError, match='decision_verification_plan_incomplete'):
        await CardService.check_decisions_coverage(None,
            spec(skip_decisions_coverage=True), SimpleNamespace(settings={'skip_decisions_coverage_global': True}))


def test_obligation_proof_is_reused_without_additional_test_or_implementation():
    from test_delivery_evidence_domain import SNAPSHOT, BINDING
    inventory, implementations, tests = case()
    source = replace(BINDING, obligation_ref='fr:fr-one')
    inventory = replace(inventory, rows=tuple(replace(row, binding=source) for row in inventory.rows))
    implementations = tuple(replace(row,
        fact=replace(row.fact, bindings=(source,), contributions=tuple(replace(c, binding=source) for c in row.fact.contributions)),
        scopes=tuple(replace(scope, binding=source) for scope in row.scopes)) for row in implementations)
    tests = tuple(replace(row, fact=replace(row.fact, bindings=(source,))) for row in tests)
    current = spec({'obligation_refs': [source.obligation_ref]})
    plan, = decision_verification_plans(current)
    binding = DeliveryBinding('decision:dec-one', 'd' * 64)
    inventory = replace(inventory, rows=(*inventory.rows,
        EffectiveDeliveryObligation(binding, 'decision', (), (), (), plan)))
    snapshot = replace(SNAPSHOT, obligations=(DeliveryObligation(source, 'Behavior'), DeliveryObligation(binding, 'Choice')),
        implementations=tuple(row.fact for row in implementations), tests=tuple(row.fact for row in tests))
    result = evaluate(inventory, implementations, tests, snapshot=snapshot)
    assert result.allowed
    row = next(r for r in result.rows if r.binding == binding)
    assert row.decision_verification_status == 'verified'
    assert row.implementation_ids == row.required_card_ids == ()
    assert set(row.test_ids) == {test.fact.id for test in tests}
    combined = DecisionVerification.model_validate({**plan.verification.model_dump(), **inspection()})
    inventory = replace(inventory, rows=(*inventory.rows[:-1],
        replace(inventory.rows[-1], decision_plan=replace(plan, verification=combined))))
    result = evaluate(inventory, implementations, tests, snapshot=snapshot)
    assert not result.allowed and 'decision_verification_incomplete' in result.blockers
    assert next(r for r in result.rows if r.binding == binding).decision_verification_status == 'inspection_pending'


@pytest.mark.asyncio
@pytest.mark.parametrize('reference,success', [('fr:fr_existing', True), ('fr:foreign', False)])
async def test_structured_decision_writer_resolves_and_preserves_plan(db_factory, reference, success):
    from test_spec_structured_entities import _seed_spec, _permission_set
    from okto_pulse.core.services.spec_structured_entities import StructuredSpecEntityService, StructuredSpecEntityCommand
    from sqlalchemy_test_models import Spec
    async with db_factory() as db:
        await _seed_spec(db, board_id='dv-board', spec_id='dv-spec', actor_id='author')
        result = await StructuredSpecEntityService(db).mutate(StructuredSpecEntityCommand(
            board_id='dv-board', spec_id='dv-spec', actor_id='author', entity_type='decision',
            operation='create', expected_spec_version=1, permission_set=_permission_set('Spec'),
            payload={'id': 'dec-choice', 'title': 'Choice', 'rationale': 'Reason',
                     'verification': {'obligation_refs': [reference]}},
        ))
        assert result.success is success
        current = await db.get(Spec, 'dv-spec')
        if success:
            assert current.decisions[0]['verification']['obligation_refs'] == [reference]
        else:
            assert current.decisions == [] and current.version == 1
