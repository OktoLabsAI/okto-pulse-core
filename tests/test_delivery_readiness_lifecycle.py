"""P06 native gate deficits are same-snapshot diagnostics, not proof deficits."""
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.models.delivery_readiness import ReadinessReadQuery
from okto_pulse.core.ports.delivery_readiness import ReadinessReadError
from okto_pulse.core.services.delivery_readiness import EvaluatedDeliveryReadiness, PreparedDeliveryReadiness
from okto_pulse.core.services.delivery_readiness_lifecycle import DeliveryReadinessLifecycleService
from okto_pulse.core.services.gate_contracts import GateContractError
from okto_pulse.core.services.main import CompletionInfrastructureUnavailable, GovernedCompletionBlocked
from okto_pulse.core.services.resource_gate_contracts import ResourceGateViolation
from test_delivery_obligation_assessment import snapshot
from test_delivery_readiness_shared_evaluation import prepared
from test_delivery_readiness_paging import Cursor


def environment(error=None):
    batch = prepared(snapshot())
    batch = replace(batch, access=replace(batch.access, completion_states=frozenset({('spec', 'in_progress')})))
    spec = SimpleNamespace(id='spec', board_id='board', edition=2, version=1, status=SpecStatus.IN_PROGRESS)
    checks = AsyncMock(side_effect=error)
    services = SimpleNamespace(specs=SimpleNamespace(get_spec=AsyncMock(return_value=spec),
        validate_completion_gates=checks), boards=SimpleNamespace(get_board=AsyncMock(return_value=object())))
    async def get(_context, *, entity, record_id):
        return services.specs.get_spec.return_value if entity == 'spec' else services.boards.get_board.return_value
    services.persistence = SimpleNamespace(get=AsyncMock(side_effect=get))
    return batch, services, DeliveryReadinessLifecycleService(services,
        persistence=services.persistence, context=object())


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
async def test_successful_subset_does_not_advertise_final_transition_authority():
    batch, services, evaluator = environment()
    native = await evaluator.evaluate(batch)
    services.specs.validate_completion_gates.assert_awaited_once()
    assert native.check_status == 'not_evaluated' and native.blocking_codes == ()
    assert native.reason_codes == ('native_transition_context_required',)


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
