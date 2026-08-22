from dataclasses import replace
from types import SimpleNamespace

import pytest
from pydantic import SecretStr, ValidationError

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.code_traceability import (
    StartCodeInvestigationUseCase,
    SubmitCodeInvestigationReceiptUseCase,
    _subject_delivery_context,
)
from okto_pulse.core.domain.code_traceability import (
    CodeEvidenceBaselinePresence,
    CodeEvidenceBaselineProvenance,
    CodeEvidenceSourceRole,
    CodeInvestigationCapability,
    CodeInvestigationCapabilityMissing,
    CodeInvestigationNoRelevantExistingImplementationInvalid,
    CodeInvestigationOmissionReason,
    CodeInvestigationOutcome,
    CodeInvestigationSubjectVersionConflict,
    CodeTraceabilityContext,
    CodeTraceabilityContextScope,
    CodeTraceabilityProjectionProfile,
    CodeTraceabilitySubjectType,
    CodeTraceabilityWaiver,
    CodeTraceabilityWaiverEntityType,
    CodeTraceabilityWaiverReason,
    CodeTraceabilityWaiverScope,
    ContextualInvestigationOutcomeV2,
    DeliveryContext,
)
from okto_pulse.core.models.code_traceability import (
    CodeInvestigationOmissionInput,
    CodeInvestigationReceiptSubmissionV2,
    CodeInvestigationToolingInput,
    ObservedWorkspaceStateSubmission,
    StartCodeInvestigationInput,
)
from okto_pulse.core.models.schemas import CodeTraceabilitySettings
from okto_pulse.core.events.types import CodeInvestigationReceiptSubmitted
from okto_pulse.core.ports.code_investigation import CodeInvestigationReceiptQuery
from okto_pulse.core.services.code_investigation import (
    CodeInvestigationService,
    required_capabilities_for_subject,
    selector_scope_digest_for_subject,
)
from okto_pulse.core.services.code_traceability_gate import (
    CodeTraceabilityGateEvaluator,
    CodeTraceabilityGatePhase,
)
from test_code_traceability_application import (
    H1,
    H2,
    NOW,
    FakeInvestigationStore,
    FakeTraceabilityStore,
    FakeUnitOfWork,
    MutableClock,
    StableIds,
    accepted_receipt,
    challenge_policy,
)
from test_code_traceability_gate import _evidence


async def _submit_contextual_receipt(
    *,
    outcome: ContextualInvestigationOutcomeV2,
    delivery_context: DeliveryContext,
    capabilities: tuple[CodeInvestigationCapability, ...] | None = None,
    omission: bool = False,
    service: CodeInvestigationService | None = None,
    store: FakeInvestigationStore | None = None,
    clock: MutableClock | None = None,
    key: str = "contextual",
):
    resolved_clock = clock or MutableClock()
    resolved_store = store or FakeInvestigationStore()
    resolved_service = service or CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=resolved_clock,
        id_factory=StableIds(),
    )
    required = required_capabilities_for_subject(
        CodeTraceabilitySubjectType.REFINEMENT
    )
    submitted_capabilities = capabilities if capabilities is not None else required
    scope = selector_scope_digest_for_subject(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
    )
    started = await resolved_service.start(
        StartCodeInvestigationInput(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
            subject_id="refinement-1",
            expected_subject_version=3,
            idempotency_key=f"{key}-start",
        ),
        actor_id="agent-1",
        actor_kind="agent",
        selector_scope_digest=scope,
        required_capabilities=required,
        store=resolved_store,
    )
    unavailable = outcome is ContextualInvestigationOutcomeV2.UNAVAILABLE
    submission = CodeInvestigationReceiptSubmissionV2(
        contract_version=2,
        board_id="board-1",
        request_id=started.request.id,
        challenge_token=SecretStr(started.challenge_token or ""),
        outcome=outcome,
        capabilities=submitted_capabilities,
        source_identity_digest=None if unavailable else H1,
        declared_revision=None if unavailable else "revision-1",
        workspace_state=(
            None
            if unavailable
            else ObservedWorkspaceStateSubmission(
                workspace_state_id="workspace-1",
                declared_dirty=False,
                reproducibility_claim="committed",
                fingerprint_algorithm="agent-manifest-v1",
                manifest_digest=H1,
                manifest_entry_count=7,
            )
        ),
        omission_manifest=(
            (
                CodeInvestigationOmissionInput(
                    reason_code=CodeInvestigationOmissionReason.PERMISSION_DENIED,
                    affected_scope_digest=H2,
                    count=1,
                ),
            )
            if omission
            else ()
        ),
        tooling=CodeInvestigationToolingInput(
            tool_id="codex",
            tool_version="1",
            method_id="contextual-code-check/v2",
        ),
        observed_at=resolved_clock.value,
        idempotency_key=f"{key}-receipt",
    )
    submitted = await resolved_service.submit_receipt(
        submission,
        actor_id="agent-1",
        actor_kind="agent",
        freshness_seconds=1800,
        store=resolved_store,
        delivery_context=delivery_context,
    )
    return submitted, resolved_service, resolved_store, resolved_clock


def _gate_context(receipt, store, *, evidence=(), waivers=()):
    return CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
        heads=(store.heads[(receipt.board_id, receipt.source_ref)],),
        receipts=(receipt,),
        evidence=tuple(evidence),
        waivers=tuple(waivers),
    )


def _access_waiver(reason: CodeTraceabilityWaiverReason) -> CodeTraceabilityWaiver:
    return CodeTraceabilityWaiver(
        id=f"waiver-{reason.value}",
        board_id="board-1",
        entity_type=CodeTraceabilityWaiverEntityType.REFINEMENT,
        entity_id="refinement-1",
        scope=CodeTraceabilityWaiverScope.CODE_EVIDENCE,
        reason_code=reason,
        justification="The external source is unavailable to the agent.",
        active=True,
        created_by="user-1",
        created_at=NOW,
        cleared_by=None,
        cleared_at=None,
    )


def test_v2_submission_is_unambiguous_and_keeps_context_server_owned() -> None:
    assert CodeInvestigationReceiptSubmissionV2.model_fields[
        "contract_version"
    ].is_required()
    payload = {
        "contract_version": 2,
        "board_id": "board-1",
        "request_id": "request-1",
        "challenge_token": "token",
        "outcome": "accessible",
        "capabilities": [],
        "omission_manifest": [],
        "tooling": {
            "tool_id": "codex",
            "tool_version": "1",
            "method_id": "contextual-code-check/v2",
        },
        "observed_at": NOW,
        "idempotency_key": "receipt-1",
        "delivery_context": "greenfield",
    }
    with pytest.raises(ValidationError) as invalid:
        CodeInvestigationReceiptSubmissionV2.model_validate(payload)
    errors = invalid.value.errors()
    assert any(item["loc"] == ("outcome",) for item in errors)
    assert any(item["loc"] == ("delivery_context",) for item in errors)


@pytest.mark.asyncio
async def test_card_delivery_context_is_resolved_from_its_owning_spec() -> None:
    class Specs:
        async def get_spec(self, spec_id: str):
            return SimpleNamespace(
                id=spec_id,
                board_id="board-1",
                version=1,
                delivery_context=DeliveryContext.GREENFIELD,
                delivery_context_provenance={
                    "value": "greenfield",
                    "source_spec_id": spec_id,
                    "source_spec_version": 1,
                },
            )

    card = SimpleNamespace(
        id="card-1",
        board_id="board-1",
        spec_id="spec-1",
        delivery_context=DeliveryContext.BROWNFIELD,
    )
    uow = SimpleNamespace(services=SimpleNamespace(specs=Specs()))
    resolved = await _subject_delivery_context(
        card,
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.CARD,
        uow=uow,  # type: ignore[arg-type]
    )
    assert resolved is DeliveryContext.GREENFIELD


@pytest.mark.asyncio
async def test_greenfield_complete_absence_is_accepted_and_passes_without_evidence() -> (
    None
):
    submitted, _service, store, clock = await _submit_contextual_receipt(
        outcome=(
            ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
        ),
        delivery_context=DeliveryContext.GREENFIELD,
    )
    receipt = submitted.receipt
    assert receipt.outcome is CodeInvestigationOutcome.ACCESSIBLE
    assert (
        receipt.contextual_outcome
        is ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
    )
    assert receipt.delivery_context is DeliveryContext.GREENFIELD
    assert receipt.omission_count == 0

    evaluation = CodeTraceabilityGateEvaluator(clock=clock).evaluate(
        _gate_context(receipt, store),
        CodeTraceabilitySettings(mode="blocking", evidence_attestation="required"),
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
    )
    assert evaluation.allowed is True
    assert evaluation.passed is True
    assert evaluation.blockers == ()
    assert evaluation.receipt_currentness == {receipt.id: "current"}

    contradictory = _evidence(
        receipt,
        evidence_id="contradictory-current-implementation",
        parent_version=3,
    )
    conflict = CodeTraceabilityGateEvaluator(clock=clock).evaluate(
        _gate_context(receipt, store, evidence=(contradictory,)),
        CodeTraceabilitySettings(mode="blocking", evidence_attestation="required"),
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
    )
    assert conflict.allowed is False
    assert conflict.blockers[0].code == (
        "code_investigation_no_relevant_existing_implementation_invalid"
    )


@pytest.mark.asyncio
async def test_legitimate_absence_rejects_non_greenfield_and_incomplete_receipts() -> (
    None
):
    with pytest.raises(CodeInvestigationNoRelevantExistingImplementationInvalid):
        await _submit_contextual_receipt(
            outcome=(
                ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
            ),
            delivery_context=DeliveryContext.BROWNFIELD,
        )

    required = required_capabilities_for_subject(
        CodeTraceabilitySubjectType.REFINEMENT
    )
    incomplete = tuple(
        item
        for item in required
        if item is not CodeInvestigationCapability.SYMLINK_CONTAINMENT
    )
    with pytest.raises(CodeInvestigationCapabilityMissing):
        await _submit_contextual_receipt(
            outcome=(
                ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
            ),
            delivery_context=DeliveryContext.GREENFIELD,
            capabilities=incomplete,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    (
        ContextualInvestigationOutcomeV2.PARTIAL,
        ContextualInvestigationOutcomeV2.UNAVAILABLE,
    ),
)
async def test_access_failure_never_becomes_absence_and_needs_specific_waiver(
    outcome: ContextualInvestigationOutcomeV2,
) -> None:
    submitted, _service, store, clock = await _submit_contextual_receipt(
        outcome=outcome,
        delivery_context=DeliveryContext.GREENFIELD,
        omission=True,
    )
    receipt = submitted.receipt
    evaluator = CodeTraceabilityGateEvaluator(clock=clock)
    settings = CodeTraceabilitySettings(
        mode="blocking",
        evidence_attestation="required",
    )

    blocked = evaluator.evaluate(
        _gate_context(receipt, store),
        settings,
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
    )
    assert blocked.allowed is False
    assert blocked.blockers[0].code == "code_investigation_unavailable"

    unrelated_waiver = evaluator.evaluate(
        _gate_context(
            receipt,
            store,
            waivers=(_access_waiver(CodeTraceabilityWaiverReason.NO_CODE_CHANGE),),
        ),
        settings,
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
    )
    assert unrelated_waiver.allowed is False

    access_waiver = evaluator.evaluate(
        _gate_context(
            receipt,
            store,
            waivers=(
                _access_waiver(
                    CodeTraceabilityWaiverReason.EXTERNAL_SOURCE_UNAVAILABLE
                ),
            ),
        ),
        settings,
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
    )
    assert access_waiver.allowed is True
    assert access_waiver.passed is True


@pytest.mark.asyncio
async def test_multi_source_absence_and_waiver_do_not_hide_applicable_mapping() -> (
    None
):
    clock = MutableClock()
    store = FakeInvestigationStore()
    service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=StableIds(),
    )
    absence, _, _, _ = await _submit_contextual_receipt(
        outcome=(
            ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
        ),
        delivery_context=DeliveryContext.GREENFIELD,
        service=service,
        store=store,
        clock=clock,
        key="absence-source",
    )
    applicable, _, _, _ = await _submit_contextual_receipt(
        outcome=ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
        delivery_context=DeliveryContext.GREENFIELD,
        service=service,
        store=store,
        clock=clock,
        key="applicable-source",
    )
    applicable_two, _, _, _ = await _submit_contextual_receipt(
        outcome=ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
        delivery_context=DeliveryContext.GREENFIELD,
        service=service,
        store=store,
        clock=clock,
        key="second-applicable-source",
    )
    partial, _, _, _ = await _submit_contextual_receipt(
        outcome=ContextualInvestigationOutcomeV2.PARTIAL,
        delivery_context=DeliveryContext.GREENFIELD,
        omission=True,
        service=service,
        store=store,
        clock=clock,
        key="partial-source",
    )
    settings = CodeTraceabilitySettings(
        mode="blocking",
        evidence_attestation="required",
    )
    evaluator = CodeTraceabilityGateEvaluator(clock=clock)
    waiver = _access_waiver(
        CodeTraceabilityWaiverReason.EXTERNAL_SOURCE_UNAVAILABLE
    )
    base_context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
        heads=tuple(store.heads.values()),
        receipts=(
            absence.receipt,
            applicable.receipt,
            applicable_two.receipt,
            partial.receipt,
        ),
        waivers=(waiver,),
    )

    missing_mapping = evaluator.evaluate(
        base_context,
        settings,
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
    )
    assert missing_mapping.allowed is False
    assert missing_mapping.blockers[0].code == (
        "code_evidence_materiality_link_required"
    )

    current = _evidence(
        applicable.receipt,
        evidence_id="applicable-current-implementation",
        parent_version=3,
    )
    partially_mapped = evaluator.evaluate(
        replace(base_context, evidence=(current,)),
        settings,
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
        referenced_evidence_ids=(current.id,),
    )
    assert partially_mapped.allowed is False
    assert partially_mapped.blockers[0].details[
        "unmapped_applicable_receipt_ids"
    ] == [applicable_two.receipt.id]

    current_two = _evidence(
        applicable_two.receipt,
        evidence_id="second-applicable-current-implementation",
        parent_version=3,
    )
    mapped = evaluator.evaluate(
        replace(base_context, evidence=(current, current_two)),
        settings,
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
        referenced_evidence_ids=(current.id, current_two.id),
    )
    assert mapped.allowed is True
    assert mapped.passed is True


@pytest.mark.asyncio
async def test_legitimate_absence_does_not_grandfather_another_legacy_source() -> (
    None
):
    clock = MutableClock()
    store = FakeInvestigationStore()
    service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=StableIds(),
    )
    absence, _, _, _ = await _submit_contextual_receipt(
        outcome=(
            ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
        ),
        delivery_context=DeliveryContext.GREENFIELD,
        service=service,
        store=store,
        clock=clock,
        key="absence-with-legacy",
    )
    legacy = await accepted_receipt(
        service=service,
        store=store,
        clock=clock,
        actor_id="agent-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
        source_ref=None,
        selector_scope_digest=selector_scope_digest_for_subject(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
            subject_id="refinement-1",
            subject_version=3,
        ),
        capabilities=required_capabilities_for_subject(
            CodeTraceabilitySubjectType.REFINEMENT
        ),
        request_key="legacy-source-start",
        receipt_key="legacy-source-receipt",
    )
    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
        heads=tuple(store.heads.values()),
        receipts=(absence.receipt, legacy.receipt),
    )
    evaluation = CodeTraceabilityGateEvaluator(clock=clock).evaluate(
        context,
        CodeTraceabilitySettings(mode="blocking", evidence_attestation="required"),
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
    )
    assert evaluation.allowed is False
    assert evaluation.blockers[0].code == "code_evidence_materiality_link_required"
    assert evaluation.blockers[0].details["legacy_receipt_ids"] == [
        legacy.receipt.id
    ]


@pytest.mark.asyncio
async def test_v2_evidence_requires_current_implementation_materiality() -> None:
    submitted, _service, store, clock = await _submit_contextual_receipt(
        outcome=ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
        delivery_context=DeliveryContext.BROWNFIELD,
    )
    receipt = submitted.receipt
    baseline = CodeEvidenceBaselineProvenance(
        presence=CodeEvidenceBaselinePresence.COMMITTED_SNAPSHOT,
        workspace_state_id=receipt.workspace_state.workspace_state_id,
    )
    scaffold = replace(
        _evidence(receipt, evidence_id="scaffold-1", parent_version=3),
        source_role=CodeEvidenceSourceRole.EXISTING_SCAFFOLD,
        relevance_summary="Existing bootstrap structure only.",
        scope_relation="same delivery scope",
        source_origin="repository baseline",
        interpretation_limit="Does not prove implemented feature behavior.",
        baseline_provenance=baseline,
    )
    evaluator = CodeTraceabilityGateEvaluator(clock=clock)
    settings = CodeTraceabilitySettings(
        mode="blocking",
        evidence_attestation="required",
    )

    blocked = evaluator.evaluate(
        _gate_context(receipt, store, evidence=(scaffold,)),
        settings,
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
        referenced_evidence_ids=(scaffold.id,),
    )
    assert blocked.allowed is False
    assert blocked.blockers[0].code == "code_evidence_materiality_link_required"
    assert blocked.blockers[0].details["required_source_role"] == (
        "current_implementation"
    )

    current = replace(
        scaffold,
        id="current-1",
        source_role=CodeEvidenceSourceRole.CURRENT_IMPLEMENTATION,
        interpretation_limit=None,
    )
    unreferenced = evaluator.evaluate(
        _gate_context(receipt, store, evidence=(current,)),
        settings,
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
    )
    assert unreferenced.allowed is False
    assert unreferenced.blockers[0].details["reason"] == (
        "referenced_evidence_mapping_required"
    )
    wrong_reference = evaluator.evaluate(
        _gate_context(receipt, store, evidence=(scaffold, current)),
        settings,
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
        referenced_evidence_ids=(scaffold.id,),
    )
    assert wrong_reference.allowed is False
    assert wrong_reference.blockers[0].code == (
        "code_evidence_materiality_link_required"
    )
    passed = evaluator.evaluate(
        _gate_context(receipt, store, evidence=(scaffold, current)),
        settings,
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
        referenced_evidence_ids=(current.id,),
    )
    assert passed.allowed is True
    assert passed.passed is True


def test_receipt_query_exposes_distinct_contextual_outcome_filter() -> None:
    query = CodeInvestigationReceiptQuery(
        board_id="board-1",
        outcome=CodeInvestigationOutcome.ACCESSIBLE,
        contextual_outcome=(
            ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
        ),
    )
    assert query.outcome is CodeInvestigationOutcome.ACCESSIBLE
    assert (
        query.contextual_outcome
        is ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
    )


def test_legacy_receipt_event_payload_is_byte_shape_compatible() -> None:
    event = CodeInvestigationReceiptSubmitted(
        board_id="board-1",
        actor_id="agent-1",
        actor_type="agent",
        investigation_request_id="request-1",
        investigation_receipt_id="receipt-1",
        acceptance_status="accepted",
        outcome="accessible",
        trust_level="single_attestation",
        generation=1,
        omission_count=0,
        observation_sha256=H1,
        payload_sha256=H2,
    )
    payload = event.payload_for_storage()
    assert payload["outcome"] == "accessible"
    assert "delivery_context" not in payload
    assert "contextual_outcome" not in payload


@pytest.mark.asyncio
async def test_receipt_use_case_binds_v2_to_server_context_and_exact_version() -> None:
    class ContextualRefinements:
        def __init__(self) -> None:
            self.version = 3

        async def get_refinement(self, refinement_id: str):
            if refinement_id != "refinement-1":
                return None
            return SimpleNamespace(
                id=refinement_id,
                board_id="board-1",
                version=self.version,
                delivery_context=DeliveryContext.GREENFIELD,
            )

    clock = MutableClock()
    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    uow = FakeUnitOfWork(investigations, traceability)
    contextual_refinements = ContextualRefinements()
    uow.services.refinements = contextual_refinements
    service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=StableIds(),
    )
    actor = ActorContext(
        "agent-1",
        "mcp",
        actor_kind="agent",
        board_id="board-1",
        permissions=(
            "code_traceability.investigation.start",
            "code_traceability.investigation.receipt_submit",
        ),
    )
    started = await StartCodeInvestigationUseCase(service).execute(
        StartCodeInvestigationInput(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
            subject_id="refinement-1",
            expected_subject_version=3,
            idempotency_key="use-case-start",
        ),
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )
    required = required_capabilities_for_subject(
        CodeTraceabilitySubjectType.REFINEMENT
    )
    command = CodeInvestigationReceiptSubmissionV2(
        contract_version=2,
        board_id="board-1",
        request_id=started.request.id,
        challenge_token=SecretStr(started.challenge_token or ""),
        outcome=(
            ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
        ),
        capabilities=required,
        source_identity_digest=H1,
        declared_revision="revision-1",
        workspace_state=ObservedWorkspaceStateSubmission(
            workspace_state_id="workspace-1",
            declared_dirty=False,
            reproducibility_claim="committed",
            fingerprint_algorithm="agent-manifest-v1",
            manifest_digest=H1,
            manifest_entry_count=7,
        ),
        omission_manifest=(),
        tooling=CodeInvestigationToolingInput(
            tool_id="codex",
            tool_version="1",
            method_id="contextual-code-check/v2",
        ),
        observed_at=clock.value,
        idempotency_key="use-case-receipt",
    )

    contextual_refinements.version = 4
    with pytest.raises(CodeInvestigationSubjectVersionConflict):
        await SubmitCodeInvestigationReceiptUseCase(service).execute(
            command,
            actor=actor,
            uow=uow,  # type: ignore[arg-type]
        )

    contextual_refinements.version = 3
    result = await SubmitCodeInvestigationReceiptUseCase(service).execute(
        command,
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )
    assert result.receipt.delivery_context is DeliveryContext.GREENFIELD
    assert (
        result.receipt.contextual_outcome
        is ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
    )
    event = uow.published_events[-1]
    assert event.outcome == "accessible"
    assert event.delivery_context == "greenfield"
    assert event.contextual_outcome == "no_relevant_existing_implementation"
