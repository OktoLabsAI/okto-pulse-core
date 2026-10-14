"""Native completion diagnostics inside the edition's authorized read snapshot."""
from okto_pulse.core.domain.enums import CardStatus, SpecStatus
from okto_pulse.core.domain.code_traceability import CodeTraceabilitySubjectType, CodeTraceabilityContractError, CodeDeliveryContextRequired
from okto_pulse.core.domain.delivery_inventory import delivery_digest
from okto_pulse.core.domain.bug_learning_policy import LEARNING_CAPTURE_REQUIRED, requires_bug_learning_capture
from okto_pulse.core.domain.guideline_semantic_transition import PolicyTransitionDecision, PolicyTransitionReasonCode
from okto_pulse.core.domain.sdlc_registry import transition_requires_policy_compliance
from okto_pulse.core.ports.delivery_readiness import ReadinessNativeLifecycle, ReadinessReadError
from okto_pulse.core.ports.guideline_policy import GuidelinePolicyPersistenceError
from okto_pulse.core.ports.code_traceability import CodeTraceabilityPersistenceError
from okto_pulse.core.services.gate_contracts import GateContractError
from okto_pulse.core.services.card_errors import CardOperationError
from okto_pulse.core.services.main import (
    GovernedCompletionBlocked, CompletionInfrastructureUnavailable,
    evaluate_code_traceability_transition, evaluate_critical_context,
)
from okto_pulse.core.services.critical_context_guard import CriticalAction, CriticalContextDecision, FullContextGuardError
from okto_pulse.core.services.code_traceability_gate import CodeTraceabilityGateEvaluation
from okto_pulse.core.services.resource_gate_contracts import ResourceGateViolation
from okto_pulse.core.services.impact_evidence import resolve_impact_evidence_mode


class DeliveryReadinessLifecycleService:
    def __init__(self, services, *, persistence, context, traceability_evaluator=evaluate_code_traceability_transition,
                 critical_context_evaluator=evaluate_critical_context):
        self.services, self.persistence, self.context = services, persistence, context
        self.traceability_evaluator = traceability_evaluator
        self.critical_context_evaluator = critical_context_evaluator

    async def evaluate(self, batch, *, card_id=None):
        """Read existing gates; do not dry-run a move or recompute delivery proof.

        A known blocker is authoritative negative evidence. Spec completion
        reads share its native prerequisites; Card report inputs still require
        the transition surface. Every mutation must independently reauthorize
        and reload under its fence; a historical Done cannot authorize itself.
        """
        scope, access = batch.facts.scope, batch.access
        card = next((c for c in batch.cards if c.id == card_id), None) if card_id else None
        if card_id and card is None:
            raise ReadinessReadError('readiness_scope_mismatch')
        status = card.status if card else batch.spec_status
        status = getattr(status, 'value', status)
        traceability_basis = None
        critical_basis = None
        def result(check, reasons, blockers=(), *, basis_digest=None, required_inputs=()):
            if traceability_basis is not None:
                basis_digest = delivery_digest({'traceability': traceability_basis, 'policy': basis_digest})
            if critical_basis is not None:
                basis_digest = delivery_digest({'critical_context': critical_basis, 'native': basis_digest})
            return ReadinessNativeLifecycle(card_id, status, check, tuple(reasons),
                other_blocker_count=0 if check == 'allowed' else None,
                blocking_codes=tuple(blockers), basis_digest=basis_digest, required_inputs=tuple(required_inputs))
        if access is None:
            return result('not_evaluated', ('native_transition_permission_required',))
        entity = 'card' if card else 'spec'
        if not access.spec_write_access or (entity, str(status)) not in access.completion_states:
            return result('not_evaluated', ('native_transition_permission_required',))
        if batch.unavailable_reason_codes:
            return result('not_evaluated', batch.unavailable_reason_codes)
        # Scalar records only: readiness does not need the modal's relationship
        # graph (all Board Cards, comments, attachments and resources).
        spec = await self.persistence.get(self.context, entity='spec', record_id=scope.spec_id)
        board = await self.persistence.get(self.context, entity='board', record_id=scope.board_id)
        if (spec is None or board is None or spec.board_id != scope.board_id
            or int(spec.edition) != scope.edition or int(spec.version) != batch.spec_version
            or str(spec.status.value) != batch.spec_status):
            raise ReadinessReadError('readiness_snapshot_changed')
        if card:
            subject = await self.persistence.get(self.context, entity='card', record_id=card_id)
            if (subject is None or subject.board_id != scope.board_id or subject.spec_id != scope.spec_id
                or int(subject.policy_version) != card.version or str(subject.status.value) != status):
                raise ReadinessReadError('readiness_snapshot_changed')
        else:
            subject = spec
        try:
            if card:
                await self.services.cards.validate_completion_references(subject, board)
                await self.services.cards.validate_completion_admission(subject, board)
                await self.services.cards.validate_dependency_admission(subject, CardStatus.DONE)
            else:
                self.services.specs.validate_transition_source_context(spec, SpecStatus.DONE)
                await self.services.specs.validate_completion_gates(spec, board, read_only_preview=True)
        except CompletionInfrastructureUnavailable:
            return result('not_evaluated', ('native_completion_infrastructure_unavailable',))
        except GateContractError as exc:
            # Native missing-link authority explicitly classifies unavailability.
            details = exc.to_dict().get('details', {})
            if details.get('nature') == 'source_unavailable':
                return result('not_evaluated', (exc.code,))
            return result('blocked', (exc.code,), (exc.code,))
        except (GovernedCompletionBlocked, ResourceGateViolation, CardOperationError, CodeDeliveryContextRequired) as exc:
            return result('blocked', (exc.code,), (exc.code,))
        # Native read evaluation consumes admitted relational attestations only;
        # it never investigates files, invokes a runner or enforces a mutation.
        try:
            traceability = await self.traceability_evaluator(self.context, board=board, subject=subject,
                subject_type=CodeTraceabilitySubjectType(entity), from_status=status, to_status='done', enforce=False)
        except (CodeTraceabilityPersistenceError, CodeTraceabilityContractError) as exc:
            return result('not_evaluated', (getattr(exc, 'code', 'code_investigation_currentness_unknown'),))
        if traceability is not None:
            if not isinstance(traceability, CodeTraceabilityGateEvaluation):
                raise ReadinessReadError('readiness_assessment_unavailable', native_code='code_traceability_preview_invalid')
            traceability_basis = delivery_digest(traceability.as_dict())
            if not traceability.allowed:
                codes = tuple(sorted({item.code for item in traceability.blockers if item.blocking}))
                if not codes:
                    raise ReadinessReadError('readiness_assessment_unavailable', native_code='code_traceability_blocker_missing')
                if 'code_investigation_currentness_unknown' in codes:
                    return result('not_evaluated', codes)
                return result('blocked', codes, codes)
        policy = None
        if transition_requires_policy_compliance(entity, status, 'done'):
            try:
                policy = await self.services.guidelines.preview_policy_transition(
                    board_id=scope.board_id, entity_type=entity, subject_id=subject.id,
                    from_status=status, to_status='done')
            except GuidelinePolicyPersistenceError:
                return result('not_evaluated', ('policy_assessment_unavailable' if access.allows(
                    'guidelines.assessments.read') else 'native_completion_infrastructure_unavailable',))
            if policy is None:
                raise ReadinessReadError('readiness_assessment_unavailable',
                    native_code='policy_transition_preview_missing')
        if policy is not None:
            if (not isinstance(policy, PolicyTransitionDecision) or policy.entity_type.value != entity
                or policy.subject_id != subject.id or policy.from_status != status or policy.to_status != 'done'):
                raise ReadinessReadError('readiness_snapshot_changed')
            if policy.observation_digest is None:
                raise ReadinessReadError('readiness_assessment_unavailable', native_code='policy_observation_basis_missing')
            # Same redaction boundary as native allowed-transitions: transition
            # authority alone does not expose assessment reasons or evidence.
            can_read = access.allows('guidelines.assessments.read')
            if PolicyTransitionReasonCode.POLICY_ASSESSMENT_UNAVAILABLE in policy.reason_codes:
                codes = (('policy_assessment_unavailable',) if can_read
                    else ('native_completion_infrastructure_unavailable',))
                return result('not_evaluated', codes, basis_digest=policy.observation_digest)
            if not policy.allowed:
                codes = tuple(c.value for c in policy.reason_codes) if can_read else ('policy_compliance_blocked',)
                return result('blocked', codes, codes, basis_digest=policy.observation_digest)
        if card:
            # These are the same native read checks used by allowed-transitions
            # and move_card. No report is submitted and no gate is dry-run.
            basis = policy.observation_digest if policy else None
            try:
                await self.services.cards._validate_cognitive_done(subject, board, read_only_preview=True)
                await self.services.resource_gate.validate_or_raise_entity_completion(
                    scope.board_id, 'card', subject.id, phase='card_done')
            except CompletionInfrastructureUnavailable:
                return result('not_evaluated', ('native_completion_infrastructure_unavailable',), basis_digest=basis)
            except (GovernedCompletionBlocked, ResourceGateViolation) as exc:
                return result('blocked', (exc.code,), (exc.code,), basis_digest=basis)
        try:
            critical = await self.critical_context_evaluator(self.context, board_id=scope.board_id,
                actor_id=access.actor_id, entity_type=entity, entity_id=subject.id,
                critical_action=CriticalAction.CARD_CLOSEOUT if card else CriticalAction.SPEC_CLOSEOUT,
                surface='service')
        except FullContextGuardError as exc:
            return result('not_evaluated', (exc.reason,), basis_digest=policy.observation_digest if policy else None)
        if (not isinstance(critical, CriticalContextDecision) or critical.board_id != scope.board_id
            or critical.actor_id != access.actor_id or critical.entity_type != entity
            or critical.entity_id != subject.id or critical.outcome != 'allow'):
            raise ReadinessReadError('readiness_assessment_unavailable', native_code='critical_context_preview_invalid')
        critical_basis = critical.context_fingerprint
        required_inputs, reasons = [], []
        if card:
            reasons.append('native_completion_report_required')
            required_inputs.extend(('conclusion', 'completeness', 'completeness_justification',
                'drift', 'drift_justification'))
            if requires_bug_learning_capture(getattr(board, 'settings', None), card.card_type):
                reasons.append(LEARNING_CAPTURE_REQUIRED)
                required_inputs.append('learning_submission')
            if resolve_impact_evidence_mode(board)[0] == 'require':
                reasons.append('impact_evidence_required')
                required_inputs.append('impact_evidence_or_delivery_selection_reuse_impact')
        return result('not_evaluated' if card else 'allowed', reasons,
            basis_digest=policy.observation_digest if policy else None, required_inputs=required_inputs)
