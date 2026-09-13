"""Issue #92: state advancement is not contradictory self-corroboration."""

from datetime import timedelta

import pytest

import test_code_traceability_application as support
from okto_pulse.core.models.code_traceability import (
    CodeInvestigationReceiptSubmissionV2,
)
from okto_pulse.core.domain.code_traceability import (
    CodeInvestigationReceiptCurrentness,
    CodeInvestigationTrustInsufficient,
    code_investigation_receipt_currentness,
)
from okto_pulse.core.models.schemas import CodeTraceabilitySettings
from okto_pulse.core.services.code_traceability_gate import (
    CodeTraceabilityGateEvaluator,
)


class Scenario:
    def __init__(self, *, version=2):
        self.clock = support.MutableClock()
        self.store = support.FakeInvestigationStore()
        self.service = support.CodeInvestigationService(
            challenge_policy=support.challenge_policy(),
            clock=self.clock,
            id_factory=support.StableIds(),
        )
        self.version = version
        self.count = 0
        self.source_ref = None
        self.scope = support.selector_scope_digest_for_card_targets(
            board_id="board-1",
            card_id="card-1",
            card_version=4,
            targets=(("target-1", 2),),
        )

    async def observe(self, revision="revision-A", *, actor="agent-1", **changes):
        self.count += 1
        self.clock.value += timedelta(seconds=1)
        caps = support.required_capabilities_for_subject(
            support.CodeTraceabilitySubjectType.CARD
        )
        start = await self.service.start(
            support.StartCodeInvestigationInput(
                board_id="board-1",
                subject_type="card",
                subject_id="card-1",
                expected_subject_version=4,
                source_ref=self.source_ref,
                idempotency_key=f"start-{self.count}",
            ),
            actor_id=actor,
            actor_kind="agent",
            selector_scope_digest=self.scope,
            required_capabilities=caps,
            store=self.store,
        )
        command = support.receipt_submission(
            board_id="board-1",
            request_id=start.request.id,
            token=start.challenge_token,
            observed_at=self.clock.value,
            capabilities=caps,
            declared_revision=revision,
            idempotency_key=f"receipt-{self.count}",
        )
        payload = command.model_dump(mode="python")
        workspace_changes = changes.pop("workspace", {})
        payload["workspace_state"].update(workspace_changes)
        payload.update(changes)
        kwargs = {}
        if self.version == 2:
            payload.update(contract_version=2, outcome="evidence_applicable")
            command = CodeInvestigationReceiptSubmissionV2.model_validate(payload)
            kwargs["delivery_context"] = support.DeliveryContext.BROWNFIELD
        else:
            command = support.CodeInvestigationReceiptSubmission.model_validate(payload)
        result = await self.service.submit_receipt(
            command,
            actor_id=actor,
            actor_kind="agent",
            freshness_seconds=1800,
            store=self.store,
            **kwargs,
        )
        self.source_ref = result.receipt.source_ref
        return result.receipt

    @property
    def head(self):
        return self.store.heads[("board-1", self.source_ref)]


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [1, 2])
async def test_new_revision_accepts_real_resolution_execution_and_replay(version):
    scenario = Scenario(version=version)
    traces = support.FakeTraceabilityStore(scenario.store)
    traces.targets["target-1"] = support.sample_target()
    targets = support.ImplementationTargetService(
        clock=scenario.clock, id_factory=support.StableIds()
    )
    common = dict(
        actor_id="agent-1",
        actor_kind="agent",
        current_card_version=4,
        minimum_trust=support.CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
        require_committed_state=True,
        investigation_service=scenario.service,
        investigation_store=scenario.store,
        store=traces,
    )
    before = await scenario.observe()
    first_resolution = await targets.submit_resolution(
        support.valid_resolution_submission(receipt_id=before.id).model_copy(
            update={"agent_observed_at": before.observed_at}
        ),
        **common,
    )
    after = await scenario.observe(
        "revision-B", workspace={"manifest_digest": support.H2}
    )
    assert scenario.head.current_receipt_id == after.id
    assert after.trust_level is support.CodeInvestigationTrustLevel.SINGLE_ATTESTATION
    assert (
        code_investigation_receipt_currentness(
            before, head=scenario.head, at=scenario.clock.value
        )
        is CodeInvestigationReceiptCurrentness.OUTDATED
    )
    assert first_resolution.resolution.workspace_state.declared_revision == "revision-A"
    command = support.ImplementationTargetExecutionSubmission(
        board_id="board-1",
        card_id="card-1",
        target_id="target-1",
        result_investigation_receipt_id=after.id,
        disposition="touched",
        actual_relative_path="src/service.py",
        actual_qualified_symbol="Service.run",
        justification="Result-state preflight confirms the implemented target.",
        idempotency_key="execution-after-commit",
    )
    executed = await targets.submit_execution(command, **common)
    replay = await targets.submit_execution(command, **common)
    assert executed.record.result_declared_revision == "revision-B"
    assert replay.replayed and replay.record == executed.record
    assert len(traces.executions) == 1
    # Refreshing the now-outdated resolution against B is also possible.
    refreshed = await targets.submit_resolution(
        support.valid_resolution_submission(receipt_id=after.id).model_copy(
            update={"agent_observed_at": after.observed_at}
        ),
        **common,
    )
    assert refreshed.resolution.workspace_state.declared_revision == "revision-B"
    if version == 2:
        context = support.CodeTraceabilityContext(
            board_id="board-1",
            subject_type=support.CodeTraceabilitySubjectType.CARD,
            subject_id="card-1",
            subject_version=4,
            profile=support.CodeTraceabilityProjectionProfile.FULL,
            context_scope=support.CodeTraceabilityContextScope.GATE,
            heads=(scenario.head,),
            receipts=(before, after),
            targets=tuple(traces.targets.values()),
            resolutions=(refreshed.resolution,),
            executions=(executed.record,),
        )
        gate = CodeTraceabilityGateEvaluator(clock=scenario.clock)
        for destination in ("validation", "done"):
            result = gate.evaluate_transition(
                context,
                CodeTraceabilitySettings(mode="blocking"),
                from_status="in_progress",
                to_status=destination,
            )
            assert result.allowed, result.blockers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"workspace": {"manifest_digest": support.H2}},
        {"workspace": {"workspace_state_id": "renamed", "manifest_digest": support.H2}},
        {"source_identity_digest": support.H2},
    ],
)
async def test_same_revision_contradiction_is_still_conflicted(change):
    scenario = Scenario()
    before = await scenario.observe()
    after = await scenario.observe(**change)
    assert after.trust_level is support.CodeInvestigationTrustLevel.CONFLICTED
    assert scenario.head.current_receipt_id == before.id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"actor": "agent-2"},
        {"source_identity_digest": support.H2},
        {"observed_at": support.NOW - timedelta(seconds=1)},
    ],
)
async def test_revision_change_does_not_bypass_actor_source_or_observation_fences(
    change,
):
    scenario = Scenario()
    await scenario.observe()
    after = await scenario.observe("revision-B", **change)
    assert after.trust_level is support.CodeInvestigationTrustLevel.CONFLICTED


@pytest.mark.asyncio
async def test_existing_conflict_cannot_be_self_healed_by_changing_revision():
    scenario = Scenario()
    await scenario.observe()
    await scenario.observe(workspace={"manifest_digest": support.H2})
    after = await scenario.observe("revision-B")
    repeat = await scenario.observe("revision-B")
    assert (
        after.trust_level
        is repeat.trust_level
        is support.CodeInvestigationTrustLevel.CONFLICTED
    )
    corroborated = await scenario.observe("revision-B", actor="agent-2")
    assert corroborated.trust_level is support.CodeInvestigationTrustLevel.CORROBORATED
    assert scenario.head.current_receipt_id == corroborated.id


@pytest.mark.asyncio
async def test_new_revision_does_not_inherit_corroboration():
    scenario = Scenario()
    await scenario.observe()
    await scenario.observe(actor="agent-2")
    after = await scenario.observe("revision-B", actor="agent-2")
    assert after.trust_level is support.CodeInvestigationTrustLevel.SINGLE_ATTESTATION
    with pytest.raises(CodeInvestigationTrustInsufficient):
        await scenario.service.require_current_receipt(
            board_id="board-1",
            receipt_id=after.id,
            store=scenario.store,
            minimum_trust=support.CodeInvestigationTrustLevel.CORROBORATED,
        )


@pytest.mark.asyncio
async def test_explicit_dirty_workspace_transition_advances_without_claiming_a_commit():
    scenario = Scenario()
    before = await scenario.observe()
    after = await scenario.observe(
        workspace={
            "workspace_state_id": "workspace-B",
            "manifest_digest": support.H2,
            "declared_dirty": True,
            "reproducibility_claim": "worktree_snapshot",
        }
    )
    assert after.trust_level is support.CodeInvestigationTrustLevel.SINGLE_ATTESTATION
    assert after.declared_revision == before.declared_revision
    assert scenario.head.current_receipt_id == after.id
    with pytest.raises(support.CodeTraceabilityContractError):
        await scenario.service.require_current_receipt(
            board_id="board-1",
            receipt_id=after.id,
            store=scenario.store,
            require_committed_state=True,
        )
