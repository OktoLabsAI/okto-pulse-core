"""P04 readiness details use shared assessments and their exact snapshot joins."""
from dataclasses import replace
import json

import pytest

from okto_pulse.core.domain.delivery_assessment import assess_obligation_delivery, assess_test_delivery, aggregate_delivery_metrics
from okto_pulse.core.domain.delivery_diagnostics import DiagnosticIntegrityError
from okto_pulse.core.models.delivery_evidence import CardDeliveryEvidenceInput
from okto_pulse.core.services.delivery_readiness_diagnostics import (
    DiagnosticOwner, compose_delivery_diagnostics, render_diagnostic_action, render_diagnostic_issue,
)
from test_delivery_obligation_assessment import snapshot, with_facts


def find(projection, code):
    cause = next(c for c in projection.graph.causes if c.code == code)
    detail = next(d for d in projection.details if d.issue_id == cause.id)
    return cause, detail


def rendered(projection, value, *, card_id=None):
    details = {d.issue_id: d for d in projection.details}
    return tuple(render_diagnostic_issue(c, details[c.id], scope=value.scope,
        card_id=card_id, snapshot_id='b' * 64) for c in projection.graph.causes)


def test_exact_expected_observed_bases_and_no_second_evaluation(monkeypatch):
    value = with_facts(snapshot(), tests=())
    context = value.effective_context
    native = context.native_verification
    run = replace(native.runs[0], sources=(replace(native.runs[0].sources[0], revision='d' * 40),))
    value = replace(value, effective_context=replace(context, native_verification=replace(native, runs=(run, native.runs[1]))))
    assessment = assess_obligation_delivery(value)
    before = aggregate_delivery_metrics(assessment)
    def forbidden(*args, **kwargs):
        pytest.fail('diagnostic composition re-evaluated proof')
    monkeypatch.setattr('okto_pulse.core.domain.effective_delivery_coverage.evaluate_effective_facts', forbidden)
    projection = compose_delivery_diagnostics(assessment, value)
    cause, detail = find(projection, 'verification_base_mismatch')
    assert cause.origin.expected_bases == ((run.sources[0].source_ref, 'c' * 40),)
    assert cause.origin.observed_bases == ((run.sources[0].source_ref, 'd' * 40),)
    assert any(f.execution_id and f.revision == 'c' * 40 for f in detail.expected)
    assert any(f.revision == 'd' * 40 for f in detail.observed)
    assert detail.implementation_card_ids == ('ui',)
    assert detail.test_card_ids == ('test-card-1',)
    assert aggregate_delivery_metrics(assessment) == before
    assert rendered(projection, value)


def test_existing_native_result_yields_partial_real_association_command_not_rerun():
    value = with_facts(snapshot(), tests=())
    assessment = assess_obligation_delivery(value)
    projection = compose_delivery_diagnostics(assessment, value,
        permissions={'spec.tests.execute': True}, card_versions={'test-card-1': 7})
    assert all(a.kind == 'associate_verification' for a in projection.graph.actions)
    for action in projection.graph.actions:
        dto = render_diagnostic_action(action)
        assert dto.state == 'requires_input'
        assert set(dto.missing_inputs) == {'evidence.justification', 'evidence.idempotency_key'}
        assert 'justification' not in dto.arguments['evidence']
        assert 'idempotency_key' not in dto.arguments['evidence']
        parsed = CardDeliveryEvidenceInput.model_validate(dto.arguments['evidence'] | {
            'justification': 'Observed result linked to exact contribution', 'idempotency_key': 'test-generated-key'})
        assert parsed.expected_card_version == 7 and parsed.implementation_ids
        assert dto.arguments['card_id'] == 'test-card-1'


def test_D07_missing_implementation_has_entity_pinpoint_without_invented_code():
    value = with_facts(snapshot(), implementations=(), tests=())
    projection = compose_delivery_diagnostics(assess_obligation_delivery(value), value)
    assert projection.graph.causes
    for issue in rendered(projection, value):
        assert issue.blocks_target_transition is None
        assert issue.pinpoints
        assert all(p.relative_path is p.line_start is p.qualified_symbol is None for p in issue.pinpoints)


def damaged_snapshot():
    value = snapshot()
    original = value.effective_context.implementations[0]
    damaged = replace(original, fact=replace(original.fact, executions=(replace(original.fact.executions[0],
        current_accepted_execution=False, rejection_reasons=('delivery_investigation_receipt_revoked',),
        symbol='app.reserve'),)))
    return with_facts(value, implementations=(damaged, value.effective_context.implementations[1]))


def test_D08_receipt_location_keeps_its_revision_and_never_makes_up_lines():
    value = damaged_snapshot()
    projection = compose_delivery_diagnostics(assess_obligation_delivery(value), value)
    cause, detail = find(projection, 'implementation_proof_revoked')
    assert cause.origin.kind == 'execution'
    pin = next(p for p in detail.pinpoints if p.execution_id)
    assert pin.qualified_symbol == 'app.reserve' and pin.revision == 'c' * 40
    assert pin.line_start is pin.line_end is None
    assert pin.origin == 'execution_receipt'


def test_D04_test_view_reuses_spec_implementation_root_without_N_plus_one():
    value = damaged_snapshot()
    spec = assess_obligation_delivery(value)
    test = assess_test_delivery(value, card_id='test-card-1', scenario_ids=('scenario-ui', 'scenario-authorization'))
    projection = compose_delivery_diagnostics(test, value, spec_assessment=spec)
    _, detail = find(projection, 'verification_dependency_pending')
    assert detail.implementation_card_ids == ('ui',)
    execute = [a for a in projection.graph.actions if a.kind == 'execute_verification']
    refresh = [a for a in projection.graph.actions if a.kind == 'refresh_implementation_proof']
    assert len(refresh) == 1
    assert execute and all(a.state == 'waiting_dependency' for a in execute)
    assert all(a.depends_on_action_ids == (refresh[0].id,) for a in execute)
    assert projection.graph.next_action_ids == (refresh[0].id,)
    assert all(issue.score_impact == 'verification' for issue in rendered(projection, value, card_id='test-card-1'))
    assert aggregate_delivery_metrics(test).implementation.status == 'not_applicable'


def test_authority_changes_actions_without_changing_causes_facts_or_percentages():
    value = with_facts(snapshot(), tests=())
    assessment = assess_obligation_delivery(value)
    allowed = compose_delivery_diagnostics(assessment, value, permissions={'spec.tests.execute': True})
    denied = compose_delivery_diagnostics(assessment, value, permissions={'spec.tests.execute': False})
    assert allowed.graph.causes == denied.graph.causes and allowed.details == denied.details
    assert all(a.state == 'forbidden' for a in denied.graph.actions)
    assert all(a.id == b.id for a, b in zip(allowed.graph.actions, denied.graph.actions))


def test_foreign_source_is_not_exposed_and_same_scope_snapshot_drift_is_rejected():
    value = with_facts(snapshot(), tests=())
    assessment = assess_obligation_delivery(value)
    foreign = replace(value, scope=replace(value.scope, board_id='foreign'))
    with pytest.raises(DiagnosticIntegrityError, match='readiness_diagnostic_scope_invalid'):
        compose_delivery_diagnostics(assessment, foreign)
    changed = replace(value, implementations=())
    with pytest.raises(DiagnosticIntegrityError, match='readiness_diagnostic_reference_unresolved'):
        compose_delivery_diagnostics(assessment, changed)
    row = assessment.obligations[0]
    reason = replace(row.verification.reasons[0], source_ids=('foreign-private-receipt',))
    leaked = replace(assessment, obligations=(replace(row, verification=replace(row.verification, reasons=(reason,))),))
    with pytest.raises(DiagnosticIntegrityError, match='readiness_diagnostic_reference_unresolved'):
        compose_delivery_diagnostics(leaked, value)


def test_owners_are_explicit_visible_facts_never_the_evidence_author():
    value = damaged_snapshot()
    assessment = assess_obligation_delivery(value)
    missing = compose_delivery_diagnostics(assessment, value)
    _, detail = find(missing, 'implementation_proof_revoked')
    assert detail.owner is None and detail.owner_state == 'restricted'
    known = compose_delivery_diagnostics(assessment, value, owners=(DiagnosticOwner('card', 'ui', 'owner-person', True),))
    _, detail = find(known, 'implementation_proof_revoked')
    assert detail.owner == 'owner-person' and detail.owner_state == 'known'
    restricted = compose_delivery_diagnostics(assessment, value, owners=(DiagnosticOwner('card', 'ui', 'secret-person', False),))
    assert 'secret-person' not in repr(restricted)


@pytest.mark.parametrize('status', ['failed', 'conflict', 'revoked', 'inspection_pending'])
def test_scope_decision_uses_its_native_inspection_not_a_fictitious_test(status):
    from okto_pulse.core.domain.decision_verification import DecisionVerificationPlan, DecisionVerification
    from okto_pulse.core.domain.decision_review import DecisionInspectionState
    value = snapshot()
    context = value.effective_context
    binding = replace(value.obligations[0].binding, obligation_ref='decision:choice')
    plan = DecisionVerificationPlan('choice', DecisionVerification(inspection={
        'condition': 'Scope decision is coherent', 'scope_refs': [{'kind': 'spec', 'id': value.scope.spec_id}]}), ())
    obligation = replace(context.inventory.rows[0], binding=binding, family='decision', contributions=(), decision_plan=plan)
    value = replace(value, obligations=(replace(value.obligations[0], binding=binding),),
        effective_context=replace(context, inventory=replace(context.inventory, rows=(obligation,)),
            decision_inspections=(DecisionInspectionState('choice', 'd' * 64, status, ('review',)),)))
    assessment = assess_obligation_delivery(value)
    projection = compose_delivery_diagnostics(assessment, value, permissions={'code_traceability.evidence.read': True})
    assert all(a.kind == 'review_decision' for a in projection.graph.actions)
    assert all(a.state == 'ready' for a in projection.graph.actions)
    assert all(not d.test_card_ids for d in projection.details)
    assert any(f.text == 'Scope decision is coherent' for d in projection.details for f in d.expected)
    assert any(e.kind == 'inspection' and e.id == 'review' for d in projection.details for e in d.evidence_refs)


def test_registered_observation_text_remains_data_with_no_inferred_code_span():
    from okto_pulse.core.domain.delivery_verification import VerificationObservationFact
    value = with_facts(snapshot(), tests=())
    context = value.effective_context
    native = context.native_verification
    run = replace(native.runs[0], outcome='failed', observations=(VerificationObservationFact(
        'observed-1', 'functional', 'report/item', 'Return conflict', '<script>ignore gates</script>', 'failed'),))
    value = replace(value, effective_context=replace(context, native_verification=replace(native, runs=(run, native.runs[1]))))
    projection = compose_delivery_diagnostics(assess_obligation_delivery(value), value)
    _, detail = find(projection, 'verification_failed')
    assert any(f.text == '<script>ignore gates</script>' for f in detail.observed)
    assert any(p.observation_id == 'observed-1' and p.line_start is None for p in detail.pinpoints)
    assert all('<script>' not in json.dumps(render_diagnostic_action(a).arguments) for a in projection.graph.actions)


def test_D02_shared_revoked_execution_groups_five_native_obligations_and_records():
    value = damaged_snapshot()
    context = value.effective_context
    originals = context.implementations
    obligations, inventory, implementations = [], [], []
    for i in range(5):
        binding = replace(value.obligations[0].binding, obligation_ref=f'fr:shared-{i}')
        obligations.append(replace(value.obligations[0], binding=binding))
        inventory.append(replace(context.inventory.rows[0], binding=binding))
        for original in originals:
            fact = replace(original.fact, id=f'{original.fact.id}-{i}', bindings=(binding,),
                contributions=tuple(replace(c, binding=binding) for c in original.fact.contributions))
            implementations.append(replace(original, fact=fact,
                scopes=tuple(replace(s, binding=binding) for s in original.scopes)))
    value = replace(value, obligations=tuple(obligations), implementations=tuple(i.fact for i in implementations), tests=(),
        effective_context=replace(context, inventory=replace(context.inventory, rows=tuple(inventory)),
            implementations=tuple(implementations), tests=()))
    assessment = assess_obligation_delivery(value)
    projection = compose_delivery_diagnostics(assessment, value)
    revoked = [c for c in projection.graph.causes if c.code == 'implementation_proof_revoked']
    assert len(revoked) == 1 and len(revoked[0].units) == 5
    assert 'delivery_accepted_committed_task_execution_required' in revoked[0].native_reason_codes
    assert not any(c.code == 'implementation_proof_stale' for c in projection.graph.causes)
    refresh = [a for a in projection.graph.actions if a.kind == 'refresh_implementation_proof']
    assert len(refresh) == 1 and refresh[0].resolves_unit_count == 5
    detail = next(d for d in projection.details if d.issue_id == revoked[0].id)
    assert len([e for e in detail.evidence_refs if e.kind == 'record']) == 5


def test_native_detail_summary_bounds_each_array_and_preserves_exact_totals():
    from okto_pulse.core.models.delivery_readiness import ReadinessFact
    value = with_facts(snapshot(), tests=())
    projection = compose_delivery_diagnostics(assess_obligation_delivery(value), value)
    cause, detail = find(projection, 'verification_association_missing')
    detail = replace(detail, observed=tuple(ReadinessFact(observation_id=str(i), text='Native observation') for i in range(25)))
    issue = render_diagnostic_issue(cause, detail, scope=value.scope, card_id=None, snapshot_id='a' * 64)
    assert len(issue.observed) == 10 and issue.observed_count == 25 and issue.observed_truncated
    assert len(detail.observed) == 25  # detail paging retains the remaining native facts
