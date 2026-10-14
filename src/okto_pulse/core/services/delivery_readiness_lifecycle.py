"""Native completion diagnostics inside the edition's authorized read snapshot."""
from okto_pulse.core.domain.guideline_semantic_transition import PolicyTransitionDecision, PolicyTransitionReasonCode
from okto_pulse.core.domain.sdlc_registry import transition_requires_policy_compliance
from okto_pulse.core.ports.delivery_readiness import ReadinessNativeLifecycle, ReadinessReadError
from okto_pulse.core.ports.guideline_policy import GuidelinePolicyPersistenceError
from okto_pulse.core.services.gate_contracts import GateContractError
from okto_pulse.core.services.card_errors import CardOperationError
from okto_pulse.core.services.main import GovernedCompletionBlocked, CompletionInfrastructureUnavailable
from okto_pulse.core.services.resource_gate_contracts import ResourceGateViolation


class DeliveryReadinessLifecycleService:
    def __init__(self, services, *, persistence, context):
        self.services, self.persistence, self.context = services, persistence, context

    async def evaluate(self, batch, *, card_id=None):
        """Read existing gates; do not dry-run a move or recompute delivery proof.

        A known blocker is authoritative negative evidence. Passing the subset
        evaluated here is not final permission: source-context, critical-context
        and other transition inputs still require the native transition surface.
        This distinction also prevents a historical Done from authorizing itself.
        """
        scope, access = batch.facts.scope, batch.access
        card = next((c for c in batch.cards if c.id == card_id), None) if card_id else None
        if card_id and card is None:
            raise ReadinessReadError('readiness_scope_mismatch')
        status = card.status if card else batch.spec_status
        status = getattr(status, 'value', status)
        def result(check, reasons, blockers=(), *, basis_digest=None):
            return ReadinessNativeLifecycle(card_id, status, check, tuple(reasons),
                blocking_codes=tuple(blockers), basis_digest=basis_digest)
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
                await self.services.cards.validate_completion_admission(subject, board)
            else:
                await self.services.specs.validate_completion_gates(spec, board, read_only_preview=True)
        except CompletionInfrastructureUnavailable:
            return result('not_evaluated', ('native_completion_infrastructure_unavailable',))
        except GateContractError as exc:
            # Native missing-link authority explicitly classifies unavailability.
            details = exc.to_dict().get('details', {})
            if details.get('nature') == 'source_unavailable':
                return result('not_evaluated', (exc.code,))
            return result('blocked', (exc.code,), (exc.code,))
        except (GovernedCompletionBlocked, ResourceGateViolation, CardOperationError) as exc:
            return result('blocked', (exc.code,), (exc.code,))
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
        return result('not_evaluated', ('native_completion_report_required' if card else 'native_transition_context_required',),
            basis_digest=policy.observation_digest if policy else None)
