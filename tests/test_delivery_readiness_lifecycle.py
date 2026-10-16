"""P06 native gate deficits are same-snapshot diagnostics, not proof deficits."""
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from okto_pulse.core.domain.enums import CardStatus, SpecStatus
from okto_pulse.core.domain.guideline_policy import GuidelineEnforcement, PolicyEntityType, PolicySubjectRef
from okto_pulse.core.domain.guideline_semantic_currentness import NativeSemanticAssessmentCurrentSnapshot
from okto_pulse.core.domain.guideline_semantic_transition import (
    PolicyTransitionSnapshot, SemanticBindingComplianceSnapshot, evaluate_policy_transition,
)
from okto_pulse.core.models.delivery_readiness import ReadinessReadQuery
from okto_pulse.core.ports.delivery_readiness import ReadinessAccessDenied, ReadinessReadError
from okto_pulse.core.ports.guideline_policy import GuidelinePolicySubjectConflict
from okto_pulse.core.services.delivery_readiness import EvaluatedDeliveryReadiness, PreparedDeliveryReadiness
from okto_pulse.core.services.delivery_readiness_lifecycle import DeliveryReadinessLifecycleService
from okto_pulse.core.services.gate_contracts import GateContractError
from okto_pulse.core.services.main import CompletionInfrastructureUnavailable, GovernedCompletionBlocked
from okto_pulse.core.services.resource_gate_contracts import ResourceGateViolation
from okto_pulse.core.services.critical_context_guard import CriticalContextDecision, FullContextUnavailableError
from test_delivery_obligation_assessment import snapshot
from test_delivery_readiness_shared_evaluation import prepared
from test_delivery_readiness_paging import Cursor


def test_native_cognitive_factory_failure_is_typed_unavailability():
    from okto_pulse.core.services.main import _evaluate_cognitive_closeout_or_raise

    def unavailable():
        raise RuntimeError('Private infrastructure configuration')

    with pytest.raises(CompletionInfrastructureUnavailable) as caught:
        _evaluate_cognitive_closeout_or_raise(gate_factory=unavailable, board=None,
            board_id='board', entity_type='spec', entity_id='spec', entity=object(), target_label='Spec')
    assert 'Private infrastructure configuration' not in str(caught.value)
    assert isinstance(caught.value.__cause__, RuntimeError)


def policy_decision(mode='ready', *, subject_id='ui', digest='a' * 64):
    bindings = ()
    if mode != 'ready':
        current = NativeSemanticAssessmentCurrentSnapshot(
            subject=PolicySubjectRef(board_id='board', entity_type=PolicyEntityType.CARD,
                subject_id=subject_id, subject_version=1), subject_content_digest=digest,
            guideline_id='private-guideline', guideline_revision_id='private-revision',
            guideline_revision_digest=digest, binding_id='private-binding', binding_revision=1,
            binding_configuration_digest=digest)
        bindings = (SemanticBindingComplianceSnapshot(binding_id='private-binding',
            guideline_id='private-guideline', enforcement=GuidelineEnforcement.BLOCKING,
            applicable_metric_count=1, current_snapshot=current, assessment_available=mode != 'unavailable',
            assessment_error_code='source_unavailable' if mode == 'unavailable' else None),)
    return evaluate_policy_transition(PolicyTransitionSnapshot(board_id='board',
        entity_type=PolicyEntityType.CARD, subject_id=subject_id, expected_from_status='in_progress',
        bindings=bindings, evaluated_at=datetime(2026, 10, 14, tzinfo=timezone.utc)), 'done')


def environment(error=None):
    batch = prepared(snapshot())
    batch = replace(batch, access=replace(batch.access, completion_states=frozenset({('spec', 'in_progress')})))
    spec = SimpleNamespace(id='spec', board_id='board', edition=2, version=1, status=SpecStatus.IN_PROGRESS)
    checks = AsyncMock(side_effect=error)
    services = SimpleNamespace(specs=SimpleNamespace(get_spec=AsyncMock(return_value=spec),
        validate_completion_gates=checks, validate_transition_source_context=Mock()),
        boards=SimpleNamespace(get_board=AsyncMock(return_value=object())),
        cards=SimpleNamespace(validate_completion_admission=AsyncMock(), _validate_cognitive_done=AsyncMock(),
            validate_completion_references=AsyncMock(), validate_dependency_admission=AsyncMock()),
        resource_gate=SimpleNamespace(validate_or_raise_entity_completion=AsyncMock()),
        guidelines=SimpleNamespace(preview_policy_transition=AsyncMock(return_value=policy_decision()),
            enforce_policy_transition=AsyncMock()))
    async def get(_context, *, entity, record_id):
        if entity == 'card':
            return services.card_subject
        return services.specs.get_spec.return_value if entity == 'spec' else services.boards.get_board.return_value
    services.persistence = SimpleNamespace(get=AsyncMock(side_effect=get))
    services.traceability = AsyncMock(return_value=None)
    def critical_context(_context, **kwargs):
        return CriticalContextDecision(**kwargs, outcome='allow', reason='guard_disabled',
            context_profile='not_required', context_fingerprint=None, context_resolved_at=None, latency_ms=0)
    services.critical_context = AsyncMock(side_effect=critical_context)
    return batch, services, DeliveryReadinessLifecycleService(services,
        persistence=services.persistence, context=object(), traceability_evaluator=services.traceability,
        critical_context_evaluator=services.critical_context)


def card_environment():
    batch, services, evaluator = environment()
    batch = replace(batch, access=replace(batch.access, completion_states=frozenset({('card', 'in_progress')})))
    services.card_subject = SimpleNamespace(id='ui', spec_id='spec', board_id='board',
        policy_version=1, status=CardStatus.IN_PROGRESS)
    return batch, services, evaluator


def completed_environment(card_id):
    batch, services, evaluator = card_environment() if card_id else environment()
    batch = replace(batch, access=replace(batch.access, spec_write_access=False, completion_states=frozenset()))
    if card_id:
        batch = replace(batch, cards=tuple(replace(c, status=CardStatus.DONE) if c.id == card_id else c
            for c in batch.cards))
        services.card_subject.status = CardStatus.DONE
    else:
        batch = replace(batch, spec_status='done')
        services.specs.get_spec.return_value.status = SpecStatus.DONE
    return batch, services, evaluator


@pytest.mark.asyncio
@pytest.mark.parametrize('card_id', [None, 'ui'])
async def test_done_is_observed_without_new_transition_permission_or_gate_evaluation(card_id):
    batch, services, evaluator = completed_environment(card_id)
    native = await evaluator.evaluate(batch, card_id=card_id)
    assert native.completion_recorded and native.check_status == 'not_evaluated'
    assert native.reason_codes == ('native_completion_recorded',) and not native.required_inputs
    reader = EvaluatedDeliveryReadiness(replace(batch, native_lifecycle=native), actor_id='reader')
    context = reader.context(card_id)
    assert context.delivery_state == 'delivered' and context.lifecycle.qualification_is_current is True
    assert context.lifecycle.stored_status == 'done' and context.lifecycle.check_status == 'not_evaluated'
    assert context.implementation.percent == context.verification.percent == 100
    assert not reader.diagnostics(card_id).graph.actions
    services.specs.validate_completion_gates.assert_not_awaited()
    services.cards.validate_completion_admission.assert_not_awaited()
    services.guidelines.preview_policy_transition.assert_not_awaited()
    services.traceability.assert_not_awaited()
    services.critical_context.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('card_id', [None, 'ui'])
async def test_done_observation_requires_current_read_authority_and_same_snapshot(card_id):
    batch, services, evaluator = completed_environment(card_id)
    operation = 'card.entity.read' if card_id else 'spec.entity.read'
    denied = replace(batch, access=replace(batch.access,
        allowed_operations=batch.access.allowed_operations - {operation}))
    with pytest.raises(ReadinessAccessDenied):
        await evaluator.evaluate(denied, card_id=card_id)
    services.persistence.get.assert_not_awaited()
    if card_id:
        services.card_subject.policy_version += 1
    else:
        services.specs.get_spec.return_value.version += 1
    with pytest.raises(ReadinessReadError, match='readiness_snapshot_changed'):
        await evaluator.evaluate(batch, card_id=card_id)


@pytest.mark.asyncio
@pytest.mark.parametrize('error,code', [
    (GateContractError(code='cards_incomplete', message='Private task title', gate_type='spec_completion'), 'cards_incomplete'),
    (GovernedCompletionBlocked('cognitive_pending', 'Private cognitive content'), 'cognitive_pending'),
    (ResourceGateViolation('resource_gate_blocked', 'Private resource'), 'resource_gate_blocked'),
])
async def test_native_blocker_keeps_full_proof_and_is_visible_without_private_error_text(error, code):
    batch, services, evaluator = environment(error)
    native = await evaluator.evaluate(batch)
    assert native.check_status == 'blocked' and native.blocking_codes == (code,)
    assert native.other_blocker_count is None  # Short-circuit is not an exact universe.
    assert services.specs.validate_completion_gates.await_args.kwargs == {'read_only_preview': True}
    reader = EvaluatedDeliveryReadiness(replace(batch, native_lifecycle=native), actor_id='reader')
    context = reader.context()
    assert context.proof_complete is True
    assert context.implementation.percent == context.verification.percent == 100
    assert context.delivery_gate.status == 'allowed'
    assert context.lifecycle.check_status == 'blocked' and context.lifecycle.qualification_is_current is False
    assert context.summary.message_code == 'lifecycle_gate_pending'
    page = PreparedDeliveryReadiness(context, reader.diagnostics(), cursor=Cursor()).page(
        ReadinessReadQuery(board_id='board', spec_id='spec'), actor_id='reader')
    issue = next(i for i in page.issues if i.code == 'lifecycle_gate_pending')
    assert issue.native_reason_codes == (code,) and issue.score_impact == 'none'
    assert not issue.blocks_proof and issue.blocks_target_transition is True
    assert issue.affected_unit_count == 0
    assert 'Private' not in page.model_dump_json()
    assert page.next_actions[0].kind == 'resolve_native_gate'


@pytest.mark.asyncio
@pytest.mark.parametrize('error,code', [
    (CompletionInfrastructureUnavailable('opaque engine detail'), 'native_completion_infrastructure_unavailable'),
    (GateContractError(code='missing_link_source_unavailable', message='Private source detail',
        gate_type='missing_link_gate', extra_details={'nature': 'source_unavailable'}), 'missing_link_source_unavailable'),
])
async def test_technical_unavailability_is_not_a_negative_native_judgment(error, code):
    batch, _, evaluator = environment(error)
    native = await evaluator.evaluate(batch)
    assert native.check_status == 'not_evaluated' and native.reason_codes == (code,)
    assert native.blocking_codes == ()
    context = EvaluatedDeliveryReadiness(replace(batch, native_lifecycle=native), actor_id='reader').context()
    assert context.proof_complete is True and context.lifecycle.qualification_is_current is None


@pytest.mark.asyncio
@pytest.mark.parametrize('missing', ['authority', 'membership', 'access'])
async def test_unauthorized_native_preview_performs_no_gate_reads(missing):
    batch, services, evaluator = environment()
    access = (None if missing == 'access' else replace(batch.access,
        **({'spec_write_access': False} if missing == 'membership' else {'completion_states': frozenset()})))
    native = await evaluator.evaluate(replace(batch, access=access))
    assert native.check_status == 'not_evaluated'
    services.persistence.get.assert_not_awaited()
    services.specs.validate_completion_gates.assert_not_awaited()


@pytest.mark.asyncio
async def test_spec_native_prerequisites_allow_read_preview_without_enforcing_a_mutation():
    batch, services, evaluator = environment()
    native = await evaluator.evaluate(batch)
    services.specs.validate_completion_gates.assert_awaited_once()
    assert native.check_status == 'allowed' and native.blocking_codes == ()
    assert native.reason_codes == () and native.other_blocker_count == 0
    services.specs.validate_transition_source_context.assert_called_once_with(
        services.specs.get_spec.return_value, SpecStatus.DONE)
    services.critical_context.assert_awaited_once()
    assert native.basis_digest is None
    services.guidelines.preview_policy_transition.assert_not_awaited()
    services.guidelines.enforce_policy_transition.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('details', [False, True])
@pytest.mark.parametrize('mode', ['missing', 'unavailable', 'persistence'])
async def test_native_policy_preview_preserves_proof_and_its_detail_authorization(mode, details):
    batch, services, evaluator = card_environment()
    operations = batch.access.allowed_operations - {'guidelines.assessments.read'}
    if details:
        operations |= {'guidelines.assessments.read'}
    batch = replace(batch, access=replace(batch.access, allowed_operations=frozenset(operations)))
    if mode == 'persistence':
        services.guidelines.preview_policy_transition.side_effect = GuidelinePolicySubjectConflict('Private source error')
    else:
        services.guidelines.preview_policy_transition.return_value = policy_decision(mode)
    native = await evaluator.evaluate(batch, card_id='ui')
    services.guidelines.preview_policy_transition.assert_awaited_once_with(board_id='board',
        entity_type='card', subject_id='ui', from_status='in_progress', to_status='done')
    services.guidelines.enforce_policy_transition.assert_not_awaited()
    expected = ('policy_compliance_receipt_missing' if details else 'policy_compliance_blocked') if mode == 'missing' else (
        'policy_assessment_unavailable' if details else 'native_completion_infrastructure_unavailable')
    assert native.reason_codes == (expected,)
    assert native.check_status == ('blocked' if mode == 'missing' else 'not_evaluated')
    assert native.blocking_codes == ((expected,) if mode == 'missing' else ())
    reader = EvaluatedDeliveryReadiness(replace(batch, native_lifecycle=native), actor_id='reader')
    context = reader.context('ui')
    assert context.implementation.percent == context.verification.percent == 100
    assert context.delivery_gate.status == 'allowed'
    assert context.lifecycle.qualification_is_current is (False if mode == 'missing' else None)
    page = PreparedDeliveryReadiness(context, reader.diagnostics('ui'), cursor=Cursor()).page(
        ReadinessReadQuery(board_id='board', spec_id='spec', card_id='ui'), actor_id='reader')
    assert 'private-' not in page.model_dump_json()
    if mode == 'missing':
        issue = next(i for i in page.issues if i.code == 'lifecycle_gate_pending')
        assert issue.score_impact == 'none' and not issue.blocks_proof
        assert issue.blocks_target_transition


@pytest.mark.asyncio
async def test_native_policy_basis_changes_even_when_result_codes_do_not():
    batch, services, evaluator = card_environment()
    services.guidelines.preview_policy_transition.return_value = policy_decision('missing')
    before = await evaluator.evaluate(batch, card_id='ui')
    services.guidelines.preview_policy_transition.return_value = policy_decision('missing', digest='b' * 64)
    after = await evaluator.evaluate(batch, card_id='ui')
    assert before.reason_codes == after.reason_codes
    assert before.basis_digest != after.basis_digest


@pytest.mark.asyncio
@pytest.mark.parametrize('invalid', [None, 'foreign', 'untyped'])
async def test_missing_or_foreign_policy_preview_never_grants_permission(invalid):
    batch, services, evaluator = card_environment()
    services.guidelines.preview_policy_transition.return_value = (
        policy_decision(subject_id='foreign') if invalid == 'foreign' else invalid)
    with pytest.raises(ReadinessReadError):
        await evaluator.evaluate(batch, card_id='ui')


@pytest.mark.asyncio
async def test_card_policy_success_still_requires_native_report_and_other_inputs():
    batch, services, evaluator = card_environment()
    native = await evaluator.evaluate(batch, card_id='ui')
    assert native.check_status == 'not_evaluated'
    assert native.reason_codes == ('native_completion_report_required',)
    assert native.basis_digest == services.guidelines.preview_policy_transition.return_value.observation_digest
    services.specs.validate_completion_gates.assert_not_awaited()
    services.guidelines.enforce_policy_transition.assert_not_awaited()
    services.cards._validate_cognitive_done.assert_awaited_once_with(services.card_subject,
        services.boards.get_board.return_value, read_only_preview=True)
    services.resource_gate.validate_or_raise_entity_completion.assert_awaited_once_with('board', 'card', 'ui', phase='card_done')


@pytest.mark.asyncio
@pytest.mark.parametrize('gate', ['cognitive', 'resource', 'unavailable'])
async def test_card_native_cognitive_and_resource_checks_preserve_policy_basis_and_proof(gate):
    batch, services, evaluator = card_environment()
    if gate == 'resource':
        services.resource_gate.validate_or_raise_entity_completion.side_effect = ResourceGateViolation('resource_gate_blocked', 'Private resource')
    else:
        services.cards._validate_cognitive_done.side_effect = (CompletionInfrastructureUnavailable('Private engine')
            if gate == 'unavailable' else GovernedCompletionBlocked('cognitive_pending', 'Private cognitive content'))
    native = await evaluator.evaluate(batch, card_id='ui')
    assert native.basis_digest == services.guidelines.preview_policy_transition.return_value.observation_digest
    assert native.check_status == ('not_evaluated' if gate == 'unavailable' else 'blocked')
    assert native.reason_codes == ({'cognitive': 'cognitive_pending', 'resource': 'resource_gate_blocked',
        'unavailable': 'native_completion_infrastructure_unavailable'}[gate],)
    context = EvaluatedDeliveryReadiness(replace(batch, native_lifecycle=native), actor_id='reader').context('ui')
    assert context.implementation.percent == context.verification.percent == 100
    assert 'Private' not in context.model_dump_json()


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['blocking', 'advisory', 'unavailable'])
async def test_native_traceability_reuses_policy_without_leaking_details_or_reducing_proof(mode):
    from okto_pulse.core.domain.code_traceability import CodeTraceabilityEnforcement
    from okto_pulse.core.services.code_traceability_gate import (
        CodeTraceabilityGateEvaluation, CodeTraceabilityGateBlocker, CodeTraceabilityGatePhase,
        EvidenceDispositionCoverage, TargetEntityCoverage,
    )
    batch, services, evaluator = card_environment()
    code = 'code_investigation_currentness_unknown' if mode == 'unavailable' else 'implementation_target_execution_missing'
    services.traceability.return_value = CodeTraceabilityGateEvaluation(
        mode=CodeTraceabilityEnforcement.ADVISORY if mode == 'advisory' else CodeTraceabilityEnforcement.BLOCKING,
        phases=(CodeTraceabilityGatePhase.CARD_EXECUTION,), allowed=mode == 'advisory', passed=False,
        blockers=(CodeTraceabilityGateBlocker(code, 'Private source details', mode != 'advisory', {'target': 'private-id'}),),
        evidence_coverage=EvidenceDispositionCoverage(0, 0, 0, ()),
        target_coverage=TargetEntityCoverage(0, 0, ()), receipt_currentness={}, resolution_freshness={})
    native = await evaluator.evaluate(batch, card_id='ui')
    assert services.traceability.await_args.kwargs['enforce'] is False
    assert native.check_status == ('blocked' if mode == 'blocking' else 'not_evaluated')
    assert native.blocking_codes == ((code,) if mode == 'blocking' else ())
    assert native.basis_digest
    if mode != 'advisory':
        services.guidelines.preview_policy_transition.assert_not_awaited()
        assert native.reason_codes == (code,)
    context = EvaluatedDeliveryReadiness(replace(batch, native_lifecycle=native), actor_id='reader').context('ui')
    assert context.implementation.percent == context.verification.percent == 100
    assert 'private-id' not in context.model_dump_json() and 'Private source' not in context.model_dump_json()
    services.traceability.return_value = replace(services.traceability.return_value,
        receipt_currentness={'another-private-id': 'stale'})
    changed = await evaluator.evaluate(batch, card_id='ui')
    assert native.reason_codes == changed.reason_codes and native.basis_digest != changed.basis_digest


@pytest.mark.asyncio
@pytest.mark.parametrize('card_type,learning_mode,impact_mode', [
    ('normal', 'blocking', 'off'), ('bug', 'advisory', 'off'),
    ('bug', 'blocking', 'off'), ('bug', 'blocking', 'require'),
])
async def test_report_inputs_explain_native_handoff_without_demanding_new_proof(card_type, learning_mode, impact_mode):
    batch, services, evaluator = card_environment()
    batch = replace(batch, cards=tuple(replace(card, card_type=card_type) if card.id == 'ui' else card for card in batch.cards))
    services.boards.get_board.return_value = SimpleNamespace(settings={
        'bug_learning_closeout': learning_mode, 'impact_evidence_mode': impact_mode})
    native = await evaluator.evaluate(batch, card_id='ui')
    assert native.check_status == 'not_evaluated' and not native.blocking_codes
    assert 'conclusion' in native.required_inputs
    assert ('learning_submission' in native.required_inputs) == (card_type == 'bug' and learning_mode == 'blocking')
    assert ('impact_evidence_or_delivery_selection_reuse_impact' in native.required_inputs) == (impact_mode == 'require')
    assert ('bug_learning_capture_required' in native.reason_codes) == (card_type == 'bug' and learning_mode == 'blocking')
    evaluated = EvaluatedDeliveryReadiness(replace(batch, native_lifecycle=native), actor_id='reader')
    context = evaluated.context('ui')
    assert context.implementation.percent == context.verification.percent == 100
    page = PreparedDeliveryReadiness(context, evaluated.diagnostics('ui'), cursor=Cursor()).page(
        ReadinessReadQuery(board_id='board', spec_id='spec', card_id='ui'), actor_id='reader')
    issue = next(item for item in page.issues if item.code == 'lifecycle_card_open')
    assert issue.score_impact == 'none' and not issue.blocks_proof
    assert issue.blocks_target_transition is None and issue.affected_unit_count == 0
    action = next(item for item in page.next_actions if item.kind == 'complete_lifecycle')
    assert action.state == 'requires_input'
    assert set(native.required_inputs) <= set(action.missing_inputs)
    assert action.context_refs == ('card:ui',)
    services.guidelines.enforce_policy_transition.assert_not_awaited()


@pytest.mark.asyncio
async def test_unavailable_native_context_has_read_guidance_without_proof_penalty():
    batch, services, evaluator = environment(CompletionInfrastructureUnavailable('Private native storage'))
    native = await evaluator.evaluate(batch)
    evaluated = EvaluatedDeliveryReadiness(replace(batch, native_lifecycle=native), actor_id='reader')
    context = evaluated.context()
    page = PreparedDeliveryReadiness(context, evaluated.diagnostics(), cursor=Cursor()).page(
        ReadinessReadQuery(board_id='board', spec_id='spec'), actor_id='reader')
    issue = next(item for item in page.issues if item.code == 'assessment_unavailable')
    assert issue.phase == 'lifecycle' and issue.score_impact == 'none'
    assert not issue.blocks_proof and issue.blocks_target_transition is None
    action = next(item for item in page.next_actions if item.kind == 'restore_evaluation')
    assert action.arguments == {'board_id': 'board', 'spec_id': 'spec', 'view': 'readiness'}
    assert 'Private native' not in page.model_dump_json()


@pytest.mark.asyncio
async def test_native_critical_context_unavailable_keeps_scores_and_private_details_out():
    batch, services, evaluator = card_environment()
    decision = await services.critical_context(None, board_id='board', actor_id=batch.access.actor_id,
        entity_type='card', entity_id='ui', critical_action='card.closeout', surface='service')
    services.critical_context.side_effect = FullContextUnavailableError('Private context failure',
        decision=replace(decision, outcome='deny', reason='full_context_unavailable'))
    native = await evaluator.evaluate(batch, card_id='ui')
    assert native.check_status == 'not_evaluated' and native.reason_codes == ('full_context_unavailable',)
    assert not native.blocking_codes
    context = EvaluatedDeliveryReadiness(replace(batch, native_lifecycle=native), actor_id='reader').context('ui')
    assert context.implementation.percent == context.verification.percent == 100
    assert 'Private context' not in context.model_dump_json()
    evaluated = EvaluatedDeliveryReadiness(replace(batch, native_lifecycle=native), actor_id='reader')
    page = PreparedDeliveryReadiness(context, evaluated.diagnostics('ui'), cursor=Cursor()).page(
        ReadinessReadQuery(board_id='board', spec_id='spec', card_id='ui'), actor_id='reader')
    action = next(item for item in page.next_actions if item.kind == 'restore_evaluation')
    assert action.arguments == {
        'board_id': 'board', 'spec_id': 'spec', 'card_id': 'ui', 'view': 'readiness'}


@pytest.mark.asyncio
async def test_native_critical_context_fingerprint_changes_snapshot_basis_without_time_noise():
    batch, services, evaluator = card_environment()
    decision = await services.critical_context(None, board_id='board', actor_id=batch.access.actor_id,
        entity_type='card', entity_id='ui', critical_action='card.closeout', surface='service')
    services.critical_context.side_effect = None
    services.critical_context.return_value = replace(decision, reason='full_context_resolved',
        context_fingerprint='ctx_sha256_v1:' + 'a' * 64, context_profile='full')
    before = await evaluator.evaluate(batch, card_id='ui')
    services.critical_context.return_value = replace(services.critical_context.return_value,
        latency_ms=100, context_resolved_at='2026-10-14T12:00:00Z')
    assert (await evaluator.evaluate(batch, card_id='ui')).basis_digest == before.basis_digest
    services.critical_context.return_value = replace(services.critical_context.return_value,
        context_fingerprint='ctx_sha256_v1:' + 'b' * 64)
    assert (await evaluator.evaluate(batch, card_id='ui')).basis_digest != before.basis_digest


@pytest.mark.asyncio
async def test_task_validation_admission_is_native_lifecycle_deficit_not_proof_deficit():
    from okto_pulse.core.services.card_errors import CardOperationError
    batch, services, evaluator = card_environment()
    services.cards.validate_completion_admission.side_effect = CardOperationError(
        'task_validation_required', 'Private gate context')
    native = await evaluator.evaluate(batch, card_id='ui')
    assert native.blocking_codes == ('task_validation_required',)
    context = EvaluatedDeliveryReadiness(replace(batch, native_lifecycle=native), actor_id='reader').context('ui')
    assert context.implementation.percent == context.verification.percent == 100
    assert context.lifecycle.check_status == 'blocked'
    assert context.summary.message_code == 'lifecycle_gate_pending'
    services.guidelines.preview_policy_transition.assert_not_awaited()
    assert 'Private' not in repr(native)


@pytest.mark.asyncio
async def test_card_version_is_checked_before_policy_preview():
    batch, services, evaluator = card_environment()
    services.card_subject.policy_version = 2
    with pytest.raises(ReadinessReadError, match='readiness_snapshot_changed'):
        await evaluator.evaluate(batch, card_id='ui')
    services.guidelines.preview_policy_transition.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_subject_version_must_match_snapshot():
    batch, services, evaluator = environment()
    services.specs.get_spec.return_value.version += 1
    with pytest.raises(ReadinessReadError, match='readiness_snapshot_changed'):
        await evaluator.evaluate(batch)
    services.specs.validate_completion_gates.assert_not_awaited()


@pytest.mark.asyncio
async def test_spec_native_blocker_is_not_inherited_by_card_context():
    batch, _, evaluator = environment(ResourceGateViolation('resource_gate_blocked', 'Missing resource'))
    native = await evaluator.evaluate(batch)
    reader = EvaluatedDeliveryReadiness(replace(batch, native_lifecycle=native), actor_id='reader')
    assert reader.context('ui').lifecycle.check_status == 'not_evaluated'
    assert not any(c.code == 'lifecycle_gate_pending' for c in reader.diagnostics('ui').graph.causes)


@pytest.mark.asyncio
@pytest.mark.parametrize('card_type', ['normal', 'bug', 'test'])
async def test_unstarted_card_guides_native_transitions_without_permission_retry_or_proof_penalty(card_type):
    batch, services, evaluator = card_environment()
    batch = replace(batch, cards=tuple(replace(c, status=CardStatus.NOT_STARTED, card_type=card_type)
        if c.id == 'ui' else c for c in batch.cards),
        access=replace(batch.access, completion_states=frozenset()))
    baseline = EvaluatedDeliveryReadiness(batch, actor_id='reader').context('ui')
    native = await evaluator.evaluate(batch, card_id='ui')
    assert native.check_status == 'blocked'
    assert native.blocking_codes == ('native_completion_transition_required',)
    evaluated = EvaluatedDeliveryReadiness(replace(batch, native_lifecycle=native), actor_id='reader')
    context = evaluated.context('ui')
    assert context.implementation == baseline.implementation
    assert context.verification == baseline.verification
    page = PreparedDeliveryReadiness(context, evaluated.diagnostics('ui'), cursor=Cursor()).page(
        ReadinessReadQuery(board_id='board', spec_id='spec', card_id='ui'), actor_id='reader')
    issue = next(item for item in page.issues if item.code == 'lifecycle_transition_required')
    assert issue.phase == 'lifecycle' and issue.score_impact == 'none'
    assert not issue.blocks_proof and issue.blocks_target_transition
    action = next(item for item in page.next_actions if item.kind == 'inspect_lifecycle')
    assert action.arguments == {'board_id': 'board', 'entity_type': 'card', 'entity_id': 'ui'}
    assert action.pulse_tool == 'okto_pulse_get_allowed_transitions'
    assert action.execution_surface == 'read_only' and action.state == 'ready'
    assert not any(item.kind == 'restore_evaluation' and issue.id in item.issue_ids for item in page.next_actions)
    services.persistence.get.assert_not_awaited()
    services.cards.validate_completion_admission.assert_not_awaited()
    services.guidelines.preview_policy_transition.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('card_id', [None, 'ui'])
async def test_missing_completion_authority_on_a_public_edge_remains_not_evaluated(card_id):
    batch, services, evaluator = card_environment() if card_id else environment()
    batch = replace(batch, access=replace(batch.access, completion_states=frozenset()))
    native = await evaluator.evaluate(batch, card_id=card_id)
    assert native.check_status == 'not_evaluated'
    assert native.reason_codes == ('native_transition_permission_required',)
    assert not native.blocking_codes
    services.persistence.get.assert_not_awaited()
