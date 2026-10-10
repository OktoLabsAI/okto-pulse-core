"""SIM-04: another Card observing identical source is not a source change."""
from dataclasses import replace
from datetime import timedelta
import pytest
from test_issue_92_sequential_receipts import Scenario
import test_code_traceability_application as support
from okto_pulse.core.domain.code_traceability import (
    code_investigation_receipt_currentness, CodeInvestigationReceiptCurrentness as State,
    CodeInvestigationSubjectVersionConflict, CodeInvestigationAttestorMismatch,
    CodeInvestigationReceiptRevocation, CodeInvestigationHeadState,
)


async def second_card(scenario, **changes):
    scenario.scope=support.selector_scope_digest_for_card_targets(
        board_id='board-1',card_id='card-2',card_version=4,targets=(('target-2',1),))
    return await scenario.observe(subject='card-2',**changes)


@pytest.mark.asyncio
async def test_two_cards_keep_their_own_receipts_and_authority():
    scenario=Scenario(); first=await scenario.observe(); second=await second_card(scenario)
    for receipt in (first,second):
        inspected=await scenario.service.inspect_receipt(board_id='board-1',receipt_id=receipt.id,store=scenario.store)
        assert inspected.currentness is State.CURRENT
        accepted=await scenario.service.require_current_receipt(
            board_id='board-1',receipt_id=receipt.id,store=scenario.store,
            subject_id=receipt.subject_id,subject_version=4,actor_id='agent-1')
        assert accepted.receipt.id==receipt.id
    with pytest.raises(CodeInvestigationSubjectVersionConflict):
        await scenario.service.require_current_receipt(board_id='board-1',receipt_id=first.id,store=scenario.store,subject_version=5)
    with pytest.raises(CodeInvestigationAttestorMismatch):
        await scenario.service.require_current_receipt(board_id='board-1',receipt_id=first.id,store=scenario.store,actor_id='agent-2')
    assert code_investigation_receipt_currentness(first,head=scenario.head,at=scenario.clock.value) is State.OUTDATED


@pytest.mark.asyncio
async def test_original_card_resolution_and_gate_survive_other_card_observation():
    scenario=Scenario(); first=await scenario.observe()
    traces=support.FakeTraceabilityStore(scenario.store)
    traces.targets['target-1']=support.sample_target()
    targets=support.ImplementationTargetService(clock=scenario.clock,id_factory=support.StableIds())
    resolution=await targets.submit_resolution(
        support.valid_resolution_submission(receipt_id=first.id).model_copy(update={'agent_observed_at':first.observed_at}),
        actor_id='agent-1',actor_kind='agent',current_card_version=4,
        minimum_trust=support.CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
        require_committed_state=True,investigation_service=scenario.service,
        investigation_store=scenario.store,store=traces)
    second=await second_card(scenario)
    with pytest.raises(support.CodeInvestigationHeadConflict):
        await scenario.service.require_current_receipt(
            board_id='board-1',receipt_id=first.id,store=scenario.store,
            require_write_head=True)
    context=support.CodeTraceabilityContext(
        board_id='board-1',subject_type=support.CodeTraceabilitySubjectType.CARD,
        subject_id='card-1',subject_version=4,
        profile=support.CodeTraceabilityProjectionProfile.FULL,
        context_scope=support.CodeTraceabilityContextScope.GATE,
        heads=(scenario.head,),receipts=(first,second),
        targets=tuple(traces.targets.values()),resolutions=(resolution.resolution,))
    from okto_pulse.core.services.code_traceability_gate import CodeTraceabilityGateEvaluator
    from okto_pulse.core.models.schemas import CodeTraceabilitySettings
    result=CodeTraceabilityGateEvaluator(clock=scenario.clock).evaluate_transition(
        context,CodeTraceabilitySettings(mode='blocking'),from_status='started',to_status='in_progress')
    assert result.allowed,result.blockers


@pytest.mark.asyncio
@pytest.mark.parametrize('changes',[
    {'revision':'revision-B'},
    {'workspace':{'manifest_digest':support.H2}},
    {'workspace':{'manifest_entry_count':999}},
    {'workspace':{'workspace_state_id':'different-workspace'}},
    {'workspace':{'fingerprint_algorithm':'another-algorithm'}},
    {'workspace':{'declared_dirty':True,'reproducibility_claim':'worktree_snapshot'}},
    {'source_identity_digest':support.H2},
])
async def test_different_source_state_never_preserves_old_card(changes):
    scenario=Scenario();first=await scenario.observe();await second_card(scenario,**changes)
    result=await scenario.service.inspect_receipt(board_id='board-1',receipt_id=first.id,store=scenario.store)
    assert result.currentness is State.OUTDATED


@pytest.mark.asyncio
async def test_expiry_revocation_conflict_and_same_card_remain_effective():
    scenario=Scenario();first=await scenario.observe();second=await second_card(scenario)
    revocation=CodeInvestigationReceiptRevocation(
        id='rev-1',receipt_id=first.id,board_id='board-1',reason_code='incorrect',
        justification='Retract observation',revoked_by='human-1',revoked_at=scenario.clock.value)
    base=dict(head=scenario.head,head_receipt=second,at=scenario.clock.value,latest_subject_receipt_id=first.id)
    assert code_investigation_receipt_currentness(first,**base,revocation=revocation) is State.REVOKED
    assert code_investigation_receipt_currentness(first,**{**base,'at':first.expires_at}) is State.EXPIRED
    assert code_investigation_receipt_currentness(first,**{**base,'head':replace(scenario.head,state=CodeInvestigationHeadState.CONFLICTED)}) is State.CONFLICTED
    assert code_investigation_receipt_currentness(first,**base,head_receipt_revocation=replace(revocation,receipt_id=second.id)) is State.OUTDATED
    same=Scenario();old=await same.observe();new=await same.observe()
    assert code_investigation_receipt_currentness(old,head=same.head,head_receipt=new,at=same.clock.value) is State.OUTDATED
    await second_card(same)
    inspected=await same.service.inspect_receipt(board_id='board-1',receipt_id=old.id,store=same.store)
    assert inspected.currentness is State.OUTDATED  # another Card cannot resurrect a superseded observation
