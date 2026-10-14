"""Native completion diagnostics inside the edition's authorized read snapshot."""
from okto_pulse.core.ports.delivery_readiness import ReadinessNativeLifecycle, ReadinessReadError
from okto_pulse.core.services.gate_contracts import GateContractError
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
        def result(check, reasons, blockers=()):
            return ReadinessNativeLifecycle(card_id, status, check, tuple(reasons), blocking_codes=tuple(blockers))
        if access is None:
            return result('not_evaluated', ('native_transition_permission_required',))
        entity = 'card' if card else 'spec'
        if not access.spec_write_access or (entity, str(status)) not in access.completion_states:
            return result('not_evaluated', ('native_transition_permission_required',))
        if card:
            return result('not_evaluated', ('native_completion_report_required',))
        if batch.unavailable_reason_codes:
            return result('not_evaluated', batch.unavailable_reason_codes)
        # Scalar records only: readiness does not need the modal's relationship
        # graph (all Board Cards, comments, attachments and resources).
        spec = await self.persistence.get(self.context, entity='spec', record_id=scope.spec_id)
        board = await self.persistence.get(self.context, entity='board', record_id=scope.board_id)
        if (spec is None or board is None or spec.board_id != scope.board_id
            or int(spec.edition) != scope.edition or int(spec.version) != batch.spec_version
            or str(spec.status.value) != status):
            raise ReadinessReadError('readiness_snapshot_changed')
        try:
            await self.services.specs.validate_completion_gates(spec, board, read_only_preview=True)
        except CompletionInfrastructureUnavailable:
            return result('not_evaluated', ('native_completion_infrastructure_unavailable',))
        except GateContractError as exc:
            # Native missing-link authority explicitly classifies unavailability.
            details = exc.to_dict().get('details', {})
            if details.get('nature') == 'source_unavailable':
                return result('not_evaluated', (exc.code,))
            return result('blocked', (exc.code,), (exc.code,))
        except (GovernedCompletionBlocked, ResourceGateViolation) as exc:
            return result('blocked', (exc.code,), (exc.code,))
        return result('not_evaluated', ('native_transition_context_required',))
