from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from types import SimpleNamespace

import pytest

from okto_pulse.core.domain.code_traceability import (
    CODE_TRACEABILITY_CONTEXT_COLLECTION_LIMITS,
    CodeEvidence,
    CodeEvidenceAttestationBasis,
    CodeEvidenceAttestationState,
    CodeEvidenceDisposition,
    CodeEvidenceDispositionKind,
    CodeEvidenceSelectorKind,
    CodeEvidenceSpecLink,
    CodeEvidenceSpecRelationType,
    CodeEvidenceType,
    CodeInvestigationCapability,
    CodeInvestigationReceiptCurrentness,
    CodeTraceabilityContext,
    CodeTraceabilityContextScope,
    CodeTraceabilityContractError,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilityOmittedContent,
    CodeTraceabilityProjectionProfile,
    CodeTraceabilitySubjectType,
    CodeInvestigationSubmissionLimitExceeded,
    CodeTraceabilityWaiver,
    CodeTraceabilityWaiverEntityType,
    CodeTraceabilityWaiverReason,
    CodeTraceabilityWaiverScope,
    ImplementationTarget,
    ImplementationTargetExecutionDisposition,
    ImplementationTargetExecutionRecord,
    ImplementationTargetResolution,
    ImplementationTargetResolutionState,
    ImplementationTargetRole,
    ImplementationTargetSelectorKind,
    ResolutionCandidate,
    SpecEntityType,
    TargetOverlap,
    TargetOverlapAcknowledgement,
    TargetOverlapDisposition,
    TargetOverlapSeverity,
    WorkspaceReproducibilityClaim,
    code_investigation_receipt_currentness,
)
from okto_pulse.core.models.schemas import BoardSettings, CodeTraceabilitySettings
from okto_pulse.core.services.code_investigation import (
    CodeInvestigationService,
    effective_required_capabilities_for_subject,
    required_capabilities_for_subject,
    selector_scope_digest_for_subject,
)
from okto_pulse.core.services.code_traceability_gate import (
    CodeTraceabilityGateEvaluator,
    CodeTraceabilityGatePhase,
    resolve_code_evidence_coverage_skip,
)
from okto_pulse.core.services.traceability import project_code_traceability_report
from test_code_traceability_application import (
    H1,
    H2,
    NOW,
    FakeInvestigationStore,
    MutableClock,
    StableIds,
    accepted_receipt,
    challenge_policy,
)


UTC = timezone.utc


def test_code_evidence_coverage_skip_resolves_board_then_spec() -> None:
    local_skip = SimpleNamespace(skip_code_evidence_coverage=True)
    no_local_skip = SimpleNamespace(skip_code_evidence_coverage=False)

    defaults = BoardSettings()
    assert defaults.skip_code_evidence_coverage_global is False
    assert (
        defaults.model_dump(mode="json")["skip_code_evidence_coverage_global"] is False
    )
    assert (
        resolve_code_evidence_coverage_skip(
            board_settings=defaults,
            spec=no_local_skip,
        )
        is False
    )
    assert (
        resolve_code_evidence_coverage_skip(
            board_settings=defaults,
            spec=local_skip,
        )
        is True
    )
    assert (
        resolve_code_evidence_coverage_skip(
            board_settings={"skip_code_evidence_coverage_global": True},
            spec=no_local_skip,
        )
        is True
    )


async def _accepted(
    *,
    subject_type: CodeTraceabilitySubjectType,
    subject_id: str,
    subject_version: int,
    source_ref: str | None = None,
    request_key: str = "request-1",
    receipt_key: str = "receipt-1",
    clock: MutableClock | None = None,
    store: FakeInvestigationStore | None = None,
):
    resolved_clock = clock or MutableClock()
    resolved_store = store or FakeInvestigationStore()
    service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=resolved_clock,
        id_factory=StableIds(),
    )
    scope = selector_scope_digest_for_subject(
        board_id="board-1",
        subject_type=subject_type,
        subject_id=subject_id,
        subject_version=subject_version,
    )
    result = await accepted_receipt(
        service=service,
        store=resolved_store,
        clock=resolved_clock,
        actor_id="agent-1",
        subject_type=subject_type,
        subject_id=subject_id,
        subject_version=subject_version,
        source_ref=source_ref,
        selector_scope_digest=scope,
        capabilities=required_capabilities_for_subject(subject_type),
        request_key=request_key,
        receipt_key=receipt_key,
    )
    return result, service, resolved_store, resolved_clock


async def _next_card_receipt(
    *,
    service: CodeInvestigationService,
    store: FakeInvestigationStore,
    clock: MutableClock,
    card_id: str,
    source_ref: str,
    key: str,
    manifest_digest: str = H1,
    workspace_state_id: str = "workspace-1",
    declared_dirty: bool = False,
):
    return await accepted_receipt(
        service=service,
        store=store,
        clock=clock,
        actor_id="agent-1",
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id=card_id,
        subject_version=4,
        source_ref=source_ref,
        selector_scope_digest=selector_scope_digest_for_subject(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.CARD,
            subject_id=card_id,
            subject_version=4,
        ),
        capabilities=required_capabilities_for_subject(
            CodeTraceabilitySubjectType.CARD
        ),
        request_key=f"request-{key}",
        receipt_key=f"receipt-{key}",
        manifest_digest=manifest_digest,
        workspace_state_id=workspace_state_id,
        declared_dirty=declared_dirty,
    )


def _evidence(
    receipt,
    *,
    evidence_id: str,
    parent_version: int,
) -> CodeEvidence:
    return CodeEvidence(
        id=evidence_id,
        board_id="board-1",
        investigation_receipt_id=receipt.id,
        source_ref=receipt.source_ref,
        parent_type=CodeTraceabilitySubjectType.REFINEMENT,
        parent_id="refinement-1",
        parent_version=parent_version,
        evidence_type=CodeEvidenceType.BEHAVIOR,
        claim=f"Claim {evidence_id}",
        workspace_state=receipt.workspace_state,
        selector_kind=CodeEvidenceSelectorKind.FILE,
        relative_path="src/service.py",
        language="python",
        symbol_kind=None,
        qualified_symbol=None,
        symbol_signature=None,
        snapshot_line_start=None,
        snapshot_line_end=None,
        excerpt=None,
        excerpt_sha256=None,
        declared_file_blob_sha256=H1,
        declared_source_content_sha256=H1,
        excerpt_omitted_reason="not_requested",
        attestation_state=CodeEvidenceAttestationState.AGENT_ATTESTED,
        attestation_basis=(CodeEvidenceAttestationBasis.AUTHENTICATED_AGENT_RECEIPT),
        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
        supersedes_evidence_id=None,
        revocation_reason=None,
        submitted_by="agent-1",
        received_at=NOW,
        payload_sha256=H2,
        idempotency_key=f"idem-{evidence_id}",
    )


def _head(store: FakeInvestigationStore, receipt):
    return store.heads[(receipt.board_id, receipt.source_ref)]


def _waiver(
    *,
    entity_type: CodeTraceabilityWaiverEntityType,
    entity_id: str,
    scope: CodeTraceabilityWaiverScope,
) -> CodeTraceabilityWaiver:
    return CodeTraceabilityWaiver(
        id=f"waiver-{scope.value}",
        board_id="board-1",
        entity_type=entity_type,
        entity_id=entity_id,
        scope=scope,
        reason_code=CodeTraceabilityWaiverReason.NO_CODE_CHANGE,
        justification="Explicitly not applicable for this transition.",
        active=True,
        created_by="user-1",
        created_at=NOW,
        cleared_by=None,
        cleared_at=None,
    )


def _target(
    *,
    target_id: str,
    card_id: str,
    source_ref: str,
    resolution_id: str,
) -> ImplementationTarget:
    return ImplementationTarget(
        id=target_id,
        board_id="board-1",
        card_id=card_id,
        source_ref=source_ref,
        selector_kind=ImplementationTargetSelectorKind.SYMBOL,
        relative_path_hint="src/service.py",
        language="python",
        symbol_kind="method",
        qualified_symbol="Service.run",
        symbol_signature="Service.run(value)",
        role=ImplementationTargetRole.MODIFY,
        intent="Preserve behavior.",
        required=True,
        source_spec_version=7,
        baseline_evidence_id=None,
        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
        revision=2,
        current_resolution_id=resolution_id,
        last_change_reason_sha256=None,
        created_by="user-1",
        created_at=NOW,
        updated_at=NOW,
    )


def _resolution(
    target: ImplementationTarget,
    receipt,
    *,
    resolution_id: str,
) -> ImplementationTargetResolution:
    return ImplementationTargetResolution(
        id=resolution_id,
        board_id="board-1",
        target_id=target.id,
        investigation_receipt_id=receipt.id,
        source_ref=receipt.source_ref,
        receipt_generation=receipt.generation,
        subject_version=4,
        target_revision=target.revision,
        workspace_state=receipt.workspace_state,
        state=ImplementationTargetResolutionState.RESOLVED,
        resolved_relative_path="src/service.py",
        resolved_language="python",
        resolved_symbol_kind="method",
        resolved_qualified_symbol="Service.run",
        resolved_symbol_signature="Service.run(value)",
        resolved_line_start=10,
        resolved_line_end=11,
        symbol_fingerprint=H1,
        declared_file_blob_sha256=H1,
        selector_fingerprint=H1,
        confidence=0.99,
        reason_code=None,
        candidate_count=0,
        candidates=(),
        declared_tool_id="codex",
        declared_tool_version="1",
        submitted_by="agent-1",
        agent_observed_at=receipt.observed_at,
        received_at=receipt.received_at,
        payload_sha256=H2,
        idempotency_key=f"idem-{resolution_id}",
    )


@pytest.mark.asyncio
async def test_refinement_gate_uses_current_agent_receipt_and_explicit_waiver() -> None:
    committed, service, store, clock = await _accepted(
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
    )
    evidence = _evidence(
        committed.receipt,
        evidence_id="evidence-1",
        parent_version=3,
    )
    evaluator = CodeTraceabilityGateEvaluator(clock=clock)
    settings = CodeTraceabilitySettings(
        mode="blocking",
        evidence_attestation="required",
    )
    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
        heads=(_head(store, committed.receipt),),
        receipts=(committed.receipt,),
        evidence=(evidence,),
    )
    passed = evaluator.evaluate(
        context,
        settings,
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
    )
    assert passed.allowed is True
    missing_reference = evaluator.evaluate(
        context,
        settings,
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
        referenced_evidence_ids=("evidence-1", "evidence-does-not-exist"),
    )
    assert missing_reference.allowed is False
    assert missing_reference.blockers[0].details["missing_evidence_ids"] == [
        "evidence-does-not-exist"
    ]

    newer = await accepted_receipt(
        service=service,
        store=store,
        clock=clock,
        actor_id="agent-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
        source_ref=committed.receipt.source_ref,
        selector_scope_digest=committed.receipt.selector_scope_digest,
        capabilities=required_capabilities_for_subject(
            CodeTraceabilitySubjectType.REFINEMENT
        ),
        request_key="request-2",
        receipt_key="receipt-2",
        manifest_digest="3" * 64,
    )
    outdated = replace(
        context,
        heads=(_head(store, newer.receipt),),
        receipts=(committed.receipt, newer.receipt),
    )
    blocked = evaluator.evaluate(
        outdated,
        settings,
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
    )
    assert blocked.allowed is False
    assert blocked.receipt_currentness[committed.receipt.id] == "conflicted"

    waived = evaluator.evaluate(
        replace(
            outdated,
            waivers=(
                _waiver(
                    entity_type=CodeTraceabilityWaiverEntityType.REFINEMENT,
                    entity_id="refinement-1",
                    scope=CodeTraceabilityWaiverScope.CODE_EVIDENCE,
                ),
            ),
        ),
        settings,
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
    )
    assert waived.allowed is True


@pytest.mark.asyncio
async def test_metadata_only_receipt_uses_same_effective_capabilities_in_gate() -> None:
    clock = MutableClock()
    store = FakeInvestigationStore()
    service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=StableIds(),
    )
    capabilities = effective_required_capabilities_for_subject(
        CodeTraceabilitySubjectType.REFINEMENT,
        receipt_content="metadata_only",
    )
    assert CodeInvestigationCapability.SAFE_EXCERPT not in capabilities
    committed = await accepted_receipt(
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
        capabilities=capabilities,
        request_key="metadata-request",
        receipt_key="metadata-receipt",
    )
    evidence = _evidence(
        committed.receipt,
        evidence_id="metadata-evidence",
        parent_version=3,
    )
    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
        heads=(_head(store, committed.receipt),),
        receipts=(committed.receipt,),
        evidence=(evidence,),
    )

    evaluation = CodeTraceabilityGateEvaluator(clock=clock).evaluate(
        context,
        CodeTraceabilitySettings(
            mode="blocking",
            receipt_content="metadata_only",
            evidence_attestation="required",
        ),
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
    )
    assert evaluation.allowed is True
    assert evaluation.receipt_currentness[committed.receipt.id] == "current"


@pytest.mark.asyncio
async def test_spec_inheritance_includes_prior_active_evidence_from_snapshot() -> None:
    committed, _service, store, _clock = await _accepted(
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
    )
    inherited = _evidence(
        committed.receipt,
        evidence_id="evidence-v3",
        parent_version=3,
    )
    future = _evidence(
        committed.receipt,
        evidence_id="evidence-v5",
        parent_version=5,
    )
    deferred = CodeEvidenceDisposition(
        id="disposition-1",
        board_id="board-1",
        spec_id="spec-1",
        evidence_id=inherited.id,
        disposition=CodeEvidenceDispositionKind.DEFERRED,
        justification="Needs later review.",
        spec_version=5,
        active=True,
        created_by="user-1",
        created_at=NOW,
        cleared_by=None,
        cleared_at=None,
    )
    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-1",
        subject_version=5,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
        evidence=(inherited, future),
        evidence_dispositions=(deferred,),
        source_refinement_id="refinement-1",
        source_refinement_snapshot_id="snapshot-v4",
        source_refinement_version=4,
    )
    evaluator = CodeTraceabilityGateEvaluator(clock=lambda: NOW)
    blocked = evaluator.evaluate(
        context,
        CodeTraceabilitySettings(mode="blocking"),
        phases=(CodeTraceabilityGatePhase.SPEC_EVIDENCE_DISPOSITION,),
    )
    assert blocked.allowed is False
    assert blocked.evidence_coverage.total == 1
    assert blocked.evidence_coverage.pending_ids == ("evidence-v3",)

    linked = CodeEvidenceSpecLink(
        id="link-1",
        board_id="board-1",
        spec_id="spec-1",
        evidence_id=inherited.id,
        entity_type=SpecEntityType.TECHNICAL_REQUIREMENT,
        entity_id="tr-1",
        relation_type=CodeEvidenceSpecRelationType.SUPPORTS,
        rationale="Evidence supports the requirement.",
        evidence_content_sha256=inherited.content_sha256,
        source_refinement_version=4,
        spec_version=5,
        created_by="user-1",
        created_at=NOW,
    )
    passed = evaluator.evaluate(
        replace(context, evidence_links=(linked,)),
        CodeTraceabilitySettings(mode="blocking"),
        phases=(CodeTraceabilityGatePhase.SPEC_EVIDENCE_DISPOSITION,),
    )
    assert passed.allowed is True
    assert passed.evidence_coverage.coverage_pct == 100.0

    skipped = evaluator.evaluate(
        context,
        CodeTraceabilitySettings(mode="blocking"),
        phases=(CodeTraceabilityGatePhase.SPEC_EVIDENCE_DISPOSITION,),
        skip_evidence_coverage=True,
    )
    assert skipped.allowed is True
    assert skipped.passed is True
    assert skipped.evidence_coverage.coverage_pct == 0.0
    assert skipped.evidence_coverage_skipped is True
    assert skipped.as_dict()["evidence_coverage_skipped"] is True


@pytest.mark.asyncio
async def test_card_gate_checks_current_resolution_overlap_and_advisory_mode() -> None:
    committed, _service, store, _clock = await _accepted(
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        subject_version=4,
    )
    own = _target(
        target_id="target-a",
        card_id="card-1",
        source_ref=committed.receipt.source_ref,
        resolution_id="resolution-a",
    )
    other = _target(
        target_id="target-b",
        card_id="card-2",
        source_ref=committed.receipt.source_ref,
        resolution_id="resolution-b",
    )
    own_resolution = _resolution(own, committed.receipt, resolution_id="resolution-a")
    other_resolution = _resolution(
        other,
        committed.receipt,
        resolution_id="resolution-b",
    )
    overlap = TargetOverlap(
        board_id="board-1",
        target_a_id=own.id,
        target_b_id=other.id,
        resolution_a_id=own_resolution.id,
        resolution_b_id=other_resolution.id,
        severity=TargetOverlapSeverity.HIGH,
        reason_code="same_symbol_mutation",
        relative_path="src/service.py",
        qualified_symbol="Service.run",
    )
    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        subject_version=4,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
        heads=(_head(store, committed.receipt),),
        receipts=(committed.receipt,),
        targets=(own, other),
        resolutions=(own_resolution, other_resolution),
        overlaps=(overlap,),
    )
    evaluator = CodeTraceabilityGateEvaluator(clock=lambda: NOW)
    settings = CodeTraceabilitySettings(
        mode="blocking",
        target_resolution="required_current_receipt",
        overlap_policy="block_parallel",
    )
    blocked = evaluator.evaluate(
        context,
        settings,
        phases=(CodeTraceabilityGatePhase.CARD_TARGET,),
        blocking_card_ids=("card-2",),
    )
    assert blocked.allowed is False
    assert {item.code for item in blocked.blockers} == {
        "implementation_overlap_blocking"
    }

    ordered = evaluator.evaluate(
        context,
        settings,
        phases=(CodeTraceabilityGatePhase.CARD_TARGET,),
        dependency_card_ids=("card-2",),
        blocking_card_ids=("card-2",),
    )
    assert ordered.allowed is True

    acknowledged = TargetOverlapAcknowledgement(
        id="ack-1",
        board_id="board-1",
        target_a_id=own.id,
        target_b_id=other.id,
        resolution_a_id=own_resolution.id,
        resolution_b_id=other_resolution.id,
        disposition=TargetOverlapDisposition.ACCEPTED_PARALLEL,
        justification="The changes are coordinated by the two agents.",
        created_by="user-1",
        created_at=NOW,
    )
    accepted = evaluator.evaluate(
        replace(context, overlaps=(replace(overlap, acknowledgement=acknowledged),)),
        settings,
        phases=(CodeTraceabilityGatePhase.CARD_TARGET,),
        blocking_card_ids=("card-2",),
    )
    assert accepted.allowed is True

    advisory = evaluator.evaluate(
        replace(
            context, resolutions=(), targets=(replace(own, current_resolution_id=None),)
        ),
        CodeTraceabilitySettings(
            mode="advisory",
            target_resolution="required_current_receipt",
        ),
        phases=(CodeTraceabilityGatePhase.CARD_TARGET,),
    )
    assert advisory.allowed is True
    assert advisory.passed is False
    assert advisory.blockers[0].blocking is False


@pytest.mark.asyncio
async def test_ac03_dirty_w2_with_same_revision_outdates_w1_resolution_and_evidence() -> (
    None
):
    w1, service, store, clock = await _accepted(
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-b",
        subject_version=4,
        request_key="ac03-card-b-w1-request",
        receipt_key="ac03-card-b-w1-receipt",
    )
    target_b = _target(
        target_id="target-b",
        card_id="card-b",
        source_ref=w1.receipt.source_ref,
        resolution_id="resolution-b-w1",
    )
    resolution_w1 = _resolution(
        target_b,
        w1.receipt,
        resolution_id="resolution-b-w1",
    )
    evidence_w1 = replace(
        _evidence(w1.receipt, evidence_id="evidence-b-w1", parent_version=4),
        parent_type=CodeTraceabilitySubjectType.CARD,
        parent_id="card-b",
    )

    clock.value += timedelta(seconds=1)
    w2 = await _next_card_receipt(
        service=service,
        store=store,
        clock=clock,
        card_id="card-a",
        source_ref=w1.receipt.source_ref,
        key="ac03-card-a-dirty-w2",
        manifest_digest=H2,
        workspace_state_id="workspace-w2-dirty",
        declared_dirty=True,
    )
    head_w2 = _head(store, w2.receipt)

    assert w1.receipt.declared_revision == w2.receipt.declared_revision
    assert w1.receipt.workspace_state.workspace_state_id != (
        w2.receipt.workspace_state.workspace_state_id
    )
    assert w2.receipt.workspace_state.declared_dirty is True
    assert w2.receipt.workspace_state.reproducibility_claim is (
        WorkspaceReproducibilityClaim.WORKTREE_SNAPSHOT
    )
    assert head_w2.current_receipt_id == w2.receipt.id
    assert (
        code_investigation_receipt_currentness(
            w1.receipt,
            head=head_w2,
            at=clock.value,
        )
        is CodeInvestigationReceiptCurrentness.OUTDATED
    )

    # Immutable W1 artifacts retain the exact source attestation they were born with.
    assert resolution_w1.workspace_state == w1.receipt.workspace_state
    assert evidence_w1.workspace_state == w1.receipt.workspace_state
    assert resolution_w1.workspace_state != w2.receipt.workspace_state
    assert evidence_w1.workspace_state != w2.receipt.workspace_state

    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-b",
        subject_version=4,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
        heads=(head_w2,),
        receipts=(w1.receipt, w2.receipt),
        evidence=(evidence_w1,),
        targets=(target_b,),
        resolutions=(resolution_w1,),
    )
    evaluation = CodeTraceabilityGateEvaluator(clock=clock).evaluate(
        context,
        CodeTraceabilitySettings(
            mode="blocking",
            target_resolution="required_current_receipt",
        ),
        phases=(CodeTraceabilityGatePhase.CARD_TARGET,),
    )
    assert evaluation.allowed is False
    assert evaluation.receipt_currentness[w1.receipt.id] == "outdated"
    assert any(
        blocker.details.get("currentness") == "outdated"
        for blocker in evaluation.blockers
    )
    assert project_code_traceability_report((context,))["targets_outdated"] == 1


@pytest.mark.asyncio
async def test_ac10_dependency_refreshes_b_after_a_advances_the_source_head() -> None:
    a_preflight, service, store, clock = await _accepted(
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-a",
        subject_version=4,
        request_key="ac10-card-a-preflight-request",
        receipt_key="ac10-card-a-preflight-receipt",
    )
    clock.value += timedelta(seconds=1)
    b_preflight = await _next_card_receipt(
        service=service,
        store=store,
        clock=clock,
        card_id="card-b",
        source_ref=a_preflight.receipt.source_ref,
        key="ac10-card-b-preflight",
    )
    target_a = _target(
        target_id="target-a",
        card_id="card-a",
        source_ref=a_preflight.receipt.source_ref,
        resolution_id="resolution-a",
    )
    target_b = _target(
        target_id="target-b",
        card_id="card-b",
        source_ref=b_preflight.receipt.source_ref,
        resolution_id="resolution-b-w1",
    )
    resolution_a = _resolution(
        target_a,
        a_preflight.receipt,
        resolution_id="resolution-a",
    )
    resolution_b_w1 = _resolution(
        target_b,
        b_preflight.receipt,
        resolution_id="resolution-b-w1",
    )
    overlap_w1 = TargetOverlap(
        board_id="board-1",
        target_a_id=target_a.id,
        target_b_id=target_b.id,
        resolution_a_id=resolution_a.id,
        resolution_b_id=resolution_b_w1.id,
        severity=TargetOverlapSeverity.HIGH,
        reason_code="same_symbol_mutation",
        relative_path="src/service.py",
        qualified_symbol="Service.run",
    )
    settings = CodeTraceabilitySettings(
        mode="blocking",
        target_resolution="required_current_receipt",
        overlap_policy="block_parallel",
    )
    evaluator = CodeTraceabilityGateEvaluator(clock=clock)
    b_context_w1 = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-b",
        subject_version=4,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
        heads=(_head(store, b_preflight.receipt),),
        receipts=(a_preflight.receipt, b_preflight.receipt),
        targets=(target_a, target_b),
        resolutions=(resolution_a, resolution_b_w1),
        overlaps=(overlap_w1,),
    )
    ordered = evaluator.evaluate(
        b_context_w1,
        settings,
        phases=(CodeTraceabilityGatePhase.CARD_TARGET,),
        dependency_card_ids=("card-a",),
        blocking_card_ids=("card-a",),
    )
    assert ordered.allowed is True
    assert all(
        blocker.code != "implementation_overlap_blocking"
        for blocker in ordered.blockers
    )

    clock.value += timedelta(seconds=1)
    a_result = await _next_card_receipt(
        service=service,
        store=store,
        clock=clock,
        card_id="card-a",
        source_ref=a_preflight.receipt.source_ref,
        key="ac10-card-a-result",
        manifest_digest=H2,
        workspace_state_id="workspace-after-a",
        declared_dirty=True,
    )
    execution_a = ImplementationTargetExecutionRecord(
        id="execution-a",
        board_id="board-1",
        card_id="card-a",
        target_id=target_a.id,
        target_revision=target_a.revision,
        result_investigation_receipt_id=a_result.receipt.id,
        disposition=ImplementationTargetExecutionDisposition.TOUCHED,
        source_ref=a_result.receipt.source_ref,
        result_declared_revision=a_result.receipt.declared_revision,
        result_workspace_state_id=(a_result.receipt.workspace_state.workspace_state_id),
        actual_relative_path="src/service.py",
        actual_qualified_symbol="Service.run",
        replacement_target_id=None,
        justification="A finished and supplied its externally observed result receipt.",
        submitted_by="agent-1",
        received_at=a_result.receipt.received_at,
        payload_sha256=H1,
        idempotency_key="execution-a",
    )
    a_finished = evaluator.evaluate(
        CodeTraceabilityContext(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.CARD,
            subject_id="card-a",
            subject_version=4,
            profile=CodeTraceabilityProjectionProfile.FULL,
            context_scope=CodeTraceabilityContextScope.GATE,
            heads=(_head(store, a_result.receipt),),
            receipts=(a_result.receipt,),
            targets=(target_a,),
            executions=(execution_a,),
        ),
        CodeTraceabilitySettings(mode="blocking"),
        phases=(CodeTraceabilityGatePhase.CARD_EXECUTION,),
    )
    assert a_finished.allowed is True
    assert _head(store, a_result.receipt).current_receipt_id == a_result.receipt.id

    b_outdated_context = replace(
        b_context_w1,
        heads=(_head(store, a_result.receipt),),
        receipts=(
            a_preflight.receipt,
            b_preflight.receipt,
            a_result.receipt,
        ),
        executions=(execution_a,),
    )
    b_outdated = evaluator.evaluate(
        b_outdated_context,
        settings,
        phases=(CodeTraceabilityGatePhase.CARD_TARGET,),
        dependency_card_ids=("card-a",),
        blocking_card_ids=("card-a",),
    )
    assert b_outdated.allowed is False
    assert b_outdated.receipt_currentness[b_preflight.receipt.id] == "outdated"
    assert all(
        blocker.code != "implementation_overlap_blocking"
        for blocker in b_outdated.blockers
    )

    clock.value += timedelta(seconds=1)
    b_refresh = await _next_card_receipt(
        service=service,
        store=store,
        clock=clock,
        card_id="card-b",
        source_ref=b_preflight.receipt.source_ref,
        key="ac10-card-b-refresh",
        manifest_digest=H2,
        workspace_state_id="workspace-after-a",
        declared_dirty=True,
    )
    resolution_b_w2 = _resolution(
        target_b,
        b_refresh.receipt,
        resolution_id="resolution-b-w2",
    )
    refreshed_target_b = replace(
        target_b,
        current_resolution_id=resolution_b_w2.id,
    )
    refreshed_overlap = replace(
        overlap_w1,
        resolution_b_id=resolution_b_w2.id,
    )
    refreshed_context = replace(
        b_outdated_context,
        heads=(_head(store, b_refresh.receipt),),
        receipts=(
            a_preflight.receipt,
            b_preflight.receipt,
            a_result.receipt,
            b_refresh.receipt,
        ),
        targets=(target_a, refreshed_target_b),
        resolutions=(resolution_a, resolution_b_w1, resolution_b_w2),
        overlaps=(refreshed_overlap,),
    )
    refreshed = evaluator.evaluate(
        refreshed_context,
        settings,
        phases=(CodeTraceabilityGatePhase.CARD_TARGET,),
        dependency_card_ids=("card-a",),
        blocking_card_ids=("card-a",),
    )
    assert refreshed.allowed is True
    assert refreshed.receipt_currentness[b_refresh.receipt.id] == "current"
    assert refreshed.resolution_freshness[target_b.id] == {
        "state": "resolved",
        "currentness": "current",
        "resolution_id": resolution_b_w2.id,
        "target_revision": target_b.revision,
    }
    assert all(
        blocker.code != "implementation_overlap_blocking"
        for blocker in refreshed.blockers
    )


@pytest.mark.asyncio
async def test_traceability_report_summary_is_core_owned_and_deduplicated() -> None:
    committed, _service, store, _clock = await _accepted(
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        subject_version=4,
    )
    evidence = _evidence(
        committed.receipt,
        evidence_id="evidence-1",
        parent_version=3,
    )
    evidence_link = CodeEvidenceSpecLink(
        id="link-1",
        board_id="board-1",
        spec_id="spec-1",
        evidence_id=evidence.id,
        entity_type=SpecEntityType.TECHNICAL_REQUIREMENT,
        entity_id="tr-1",
        relation_type=CodeEvidenceSpecRelationType.SUPPORTS,
        rationale="Evidence supports the requirement.",
        evidence_content_sha256=evidence.content_sha256,
        source_refinement_version=3,
        spec_version=5,
        created_by="user-1",
        created_at=NOW,
    )
    target_a = _target(
        target_id="target-a",
        card_id="card-1",
        source_ref=committed.receipt.source_ref,
        resolution_id="resolution-a",
    )
    target_b = _target(
        target_id="target-b",
        card_id="card-2",
        source_ref=committed.receipt.source_ref,
        resolution_id="resolution-b",
    )
    resolution_a = _resolution(
        target_a,
        committed.receipt,
        resolution_id="resolution-a",
    )
    resolution_b = replace(
        _resolution(
            target_b,
            committed.receipt,
            resolution_id="resolution-b",
        ),
        subject_version=3,
    )
    overlap = TargetOverlap(
        board_id="board-1",
        target_a_id=target_a.id,
        target_b_id=target_b.id,
        resolution_a_id=resolution_a.id,
        resolution_b_id=resolution_b.id,
        severity=TargetOverlapSeverity.HIGH,
        reason_code="same_symbol_mutation",
        relative_path="src/service.py",
        qualified_symbol="Service.run",
    )
    shared = {
        "board_id": "board-1",
        "profile": CodeTraceabilityProjectionProfile.SUMMARY,
        "heads": (_head(store, committed.receipt),),
        "receipts": (committed.receipt,),
        "targets": (target_a, target_b),
        "resolutions": (resolution_a, resolution_b),
        "overlaps": (overlap,),
    }
    contexts = (
        CodeTraceabilityContext(
            **shared,
            subject_type=CodeTraceabilitySubjectType.SPEC,
            subject_id="spec-1",
            subject_version=5,
            evidence=(evidence,),
            evidence_links=(evidence_link,),
        ),
        CodeTraceabilityContext(
            **shared,
            subject_type=CodeTraceabilitySubjectType.CARD,
            subject_id="card-1",
            subject_version=4,
        ),
        CodeTraceabilityContext(
            **shared,
            subject_type=CodeTraceabilitySubjectType.CARD,
            subject_id="card-2",
            subject_version=4,
        ),
    )

    assert project_code_traceability_report(contexts) == {
        "evidence_total": 1,
        "evidence_linked": 1,
        "targets_total": 2,
        "targets_resolved": 1,
        "targets_outdated": 1,
        "high_overlaps": 1,
    }

    evidence_limit = CODE_TRACEABILITY_CONTEXT_COLLECTION_LIMITS["evidence"]
    clipped = replace(
        contexts[0],
        evidence=tuple(evidence for _ in range(evidence_limit)),
        omitted_content_manifest=(
            CodeTraceabilityOmittedContent(
                collection="evidence",
                hard_limit=evidence_limit,
                included_count=evidence_limit,
            ),
        ),
    )
    with pytest.raises(CodeTraceabilityContractError) as incomplete:
        project_code_traceability_report((clipped, *contexts[1:]))
    assert incomplete.value.code == "code_traceability_report_incomplete"
    assert incomplete.value.details == {"collections": ["evidence"]}


@pytest.mark.asyncio
async def test_card_execution_requires_current_agent_result_receipt() -> None:
    committed, _service, store, _clock = await _accepted(
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        subject_version=4,
    )
    target = _target(
        target_id="target-1",
        card_id="card-1",
        source_ref=committed.receipt.source_ref,
        resolution_id="resolution-1",
    )
    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        subject_version=4,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
        heads=(_head(store, committed.receipt),),
        receipts=(committed.receipt,),
        targets=(target,),
    )
    evaluator = CodeTraceabilityGateEvaluator(clock=lambda: NOW)
    settings = CodeTraceabilitySettings(mode="blocking")
    missing = evaluator.evaluate(
        context,
        settings,
        phases=(CodeTraceabilityGatePhase.CARD_EXECUTION,),
    )
    assert missing.allowed is False
    assert missing.blockers[0].code == "target_execution_disposition_required"

    execution = ImplementationTargetExecutionRecord(
        id="execution-1",
        board_id="board-1",
        card_id="card-1",
        target_id=target.id,
        target_revision=target.revision,
        result_investigation_receipt_id=committed.receipt.id,
        disposition=ImplementationTargetExecutionDisposition.TOUCHED,
        source_ref=committed.receipt.source_ref,
        result_declared_revision=committed.receipt.declared_revision,
        result_workspace_state_id=committed.receipt.workspace_state.workspace_state_id,
        actual_relative_path="src/service.py",
        actual_qualified_symbol="Service.run",
        replacement_target_id=None,
        justification="Target implemented and validated by the external agent.",
        submitted_by="agent-1",
        received_at=NOW,
        payload_sha256=H1,
        idempotency_key="execution-1",
    )
    passed = evaluator.evaluate(
        replace(context, executions=(execution,)),
        settings,
        phases=(CodeTraceabilityGatePhase.CARD_EXECUTION,),
    )
    assert passed.allowed is True


def test_transition_projection_defaults_to_non_blocking_advisory() -> None:
    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        subject_version=1,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
    )
    evaluation = CodeTraceabilityGateEvaluator(
        clock=lambda: datetime(2026, 8, 9, tzinfo=UTC)
    ).evaluate_transition(
        context,
        CodeTraceabilitySettings(),
        from_status="not_started",
        to_status="started",
    )
    assert evaluation.mode == "advisory"
    assert evaluation.allowed is True
    assert evaluation.passed is False
    assert evaluation.blockers
    assert all(item.blocking is False for item in evaluation.blockers)


@pytest.mark.asyncio
async def test_transition_runtime_surfaces_advisory_when_adapter_is_unavailable(
    monkeypatch,
) -> None:
    import okto_pulse.core.ports.relational_application as relational_application
    from okto_pulse.core.services.main import evaluate_code_traceability_transition

    def unavailable():
        raise RuntimeError("adapter unavailable")

    monkeypatch.setattr(
        relational_application,
        "require_relational_application_adapter",
        unavailable,
    )
    evaluation = await evaluate_code_traceability_transition(
        object(),
        board=SimpleNamespace(settings={}),
        subject=SimpleNamespace(id="refinement-1", board_id="board-1", version=1),
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        from_status="review",
        to_status="approved",
    )

    assert evaluation is not None
    assert evaluation.as_dict()["mode"] == "advisory"
    assert evaluation.allowed is True
    assert evaluation.passed is False
    assert [item.code for item in evaluation.blockers] == [
        "code_investigation_currentness_unknown"
    ]


@pytest.mark.asyncio
async def test_spec_code_evidence_coverage_is_deterministic_and_skippable(
    monkeypatch,
) -> None:
    import okto_pulse.core.ports.relational_application as relational_application
    from okto_pulse.core.services.main import (
        evaluate_code_evidence_coverage_gate,
        evaluate_code_traceability_transition,
    )

    committed, _service, _store, _clock = await _accepted(
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
    )
    inherited = _evidence(
        committed.receipt,
        evidence_id="evidence-pending",
        parent_version=3,
    )
    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-1",
        subject_version=5,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
        evidence=(inherited,),
        source_refinement_id="refinement-1",
        source_refinement_snapshot_id="snapshot-v3",
        source_refinement_version=3,
    )

    class ReadPort:
        async def refinement_context(self, _query):
            raise AssertionError("unexpected refinement projection")

        async def spec_context(self, query):
            assert query.subject_id == "spec-1"
            return context

        async def card_context(self, _query):
            raise AssertionError("unexpected card projection")

    monkeypatch.setattr(
        relational_application,
        "require_relational_application_adapter",
        lambda: SimpleNamespace(code_traceability_read=lambda _db: ReadPort()),
    )
    board = SimpleNamespace(settings={"code_traceability": {"mode": "advisory"}})
    subject = SimpleNamespace(
        id="spec-1",
        board_id="board-1",
        version=5,
        skip_code_evidence_coverage=False,
    )

    with pytest.raises(CodeTraceabilityContractError) as blocked:
        await evaluate_code_evidence_coverage_gate(
            object(), board=board, spec=subject, enforce=True
        )
    assert blocked.value.code == "code_evidence_disposition_required"
    assert blocked.value.details["evidence_disposition_coverage_pct"] == 0.0

    subject.skip_code_evidence_coverage = True
    skipped = await evaluate_code_evidence_coverage_gate(
        object(), board=board, spec=subject, enforce=True
    )
    assert skipped.allowed is True
    assert skipped.evidence_coverage_skipped is True
    assert skipped.evidence_coverage.pending_ids == ("evidence-pending",)

    transition = await evaluate_code_traceability_transition(
        object(),
        board=SimpleNamespace(settings={"code_traceability": {"mode": "blocking"}}),
        subject=subject,
        subject_type=CodeTraceabilitySubjectType.SPEC,
        from_status="draft",
        to_status="review",
        enforce=True,
    )
    assert transition is not None
    assert transition.allowed is True
    assert transition.evidence_coverage_skipped is True

    subject.skip_code_evidence_coverage = False
    global_board = SimpleNamespace(
        settings={
            "code_traceability": {"mode": "blocking"},
            "skip_code_evidence_coverage_global": True,
        }
    )
    globally_skipped = await evaluate_code_evidence_coverage_gate(
        object(), board=global_board, spec=subject, enforce=True
    )
    assert globally_skipped.allowed is True
    assert globally_skipped.evidence_coverage_skipped is True
    assert globally_skipped.evidence_coverage.pending_ids == ("evidence-pending",)

    global_transition = await evaluate_code_traceability_transition(
        object(),
        board=global_board,
        subject=subject,
        subject_type=CodeTraceabilitySubjectType.SPEC,
        from_status="draft",
        to_status="review",
        enforce=True,
    )
    assert global_transition is not None
    assert global_transition.allowed is True
    assert global_transition.evidence_coverage_skipped is True


@pytest.mark.asyncio
async def test_spec_code_evidence_skip_does_not_mask_incomplete_projection(
    monkeypatch,
) -> None:
    import okto_pulse.core.ports.relational_application as relational_application
    from okto_pulse.core.services.main import evaluate_code_evidence_coverage_gate

    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-1",
        subject_version=5,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
        omitted_content_manifest=(
            CodeTraceabilityOmittedContent(
                collection="evidence",
                hard_limit=CODE_TRACEABILITY_CONTEXT_COLLECTION_LIMITS["evidence"],
                included_count=0,
            ),
        ),
    )

    class ReadPort:
        async def refinement_context(self, _query):
            raise AssertionError("unexpected refinement projection")

        async def spec_context(self, _query):
            return context

        async def card_context(self, _query):
            raise AssertionError("unexpected card projection")

    monkeypatch.setattr(
        relational_application,
        "require_relational_application_adapter",
        lambda: SimpleNamespace(code_traceability_read=lambda _db: ReadPort()),
    )
    subject = SimpleNamespace(
        id="spec-1",
        board_id="board-1",
        version=5,
        skip_code_evidence_coverage=False,
    )
    with pytest.raises(CodeTraceabilityContractError) as blocked:
        await evaluate_code_evidence_coverage_gate(
            object(),
            board=SimpleNamespace(
                settings={"skip_code_evidence_coverage_global": True}
            ),
            spec=subject,
            enforce=True,
        )
    assert blocked.value.code == "code_traceability_projection_incomplete"


@pytest.mark.asyncio
async def test_spec_code_evidence_skip_does_not_mask_read_adapter_failure(
    monkeypatch,
) -> None:
    import okto_pulse.core.ports.relational_application as relational_application
    from okto_pulse.core.ports.relational_application import (
        RelationalApplicationAdapterMissing,
    )
    from okto_pulse.core.services.main import evaluate_code_evidence_coverage_gate

    def unavailable_adapter():
        raise RelationalApplicationAdapterMissing("adapter unavailable")

    monkeypatch.setattr(
        relational_application,
        "require_relational_application_adapter",
        unavailable_adapter,
    )
    subject = SimpleNamespace(
        id="spec-1",
        board_id="board-1",
        version=5,
        skip_code_evidence_coverage=False,
    )
    global_board = SimpleNamespace(
        settings={"skip_code_evidence_coverage_global": True}
    )

    with pytest.raises(CodeTraceabilityContractError) as blocked:
        await evaluate_code_evidence_coverage_gate(
            object(), board=global_board, spec=subject, enforce=True
        )

    assert blocked.value.code == "code_investigation_currentness_unknown"
    assert blocked.value.details == {
        "reason": "code_traceability_read_adapter_unavailable"
    }

    def broken_adapter():
        raise RuntimeError("internal composition defect")

    monkeypatch.setattr(
        relational_application,
        "require_relational_application_adapter",
        broken_adapter,
    )
    with pytest.raises(RuntimeError, match="internal composition defect"):
        await evaluate_code_evidence_coverage_gate(
            object(), board=global_board, spec=subject, enforce=True
        )


@pytest.mark.asyncio
async def test_projection_profiles_have_distinct_content_and_budgets() -> None:
    committed, _service, store, _clock = await _accepted(
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        subject_version=4,
    )
    excerpt = "safe implementation detail\n" * 140
    evidence = replace(
        _evidence(
            committed.receipt,
            evidence_id="evidence-profile",
            parent_version=3,
        ),
        excerpt=excerpt,
        excerpt_sha256=hashlib.sha256(excerpt.encode("utf-8")).hexdigest(),
        excerpt_omitted_reason=None,
    )
    evidence_link = CodeEvidenceSpecLink(
        id="link-profile",
        board_id="board-1",
        spec_id="spec-1",
        evidence_id=evidence.id,
        entity_type=SpecEntityType.TECHNICAL_REQUIREMENT,
        entity_id="tr-1",
        relation_type=CodeEvidenceSpecRelationType.SUPPORTS,
        rationale="The claim supports this normative requirement.",
        evidence_content_sha256=evidence.content_sha256,
        source_refinement_version=3,
        spec_version=5,
        created_by="user-1",
        created_at=NOW,
    )
    target = _target(
        target_id="target-profile",
        card_id="card-1",
        source_ref=committed.receipt.source_ref,
        resolution_id="resolution-profile",
    )
    candidate = ResolutionCandidate(
        relative_path="src/service.py",
        qualified_symbol="Service.run",
        symbol_signature="Service.run(value)",
        symbol_fingerprint=H1,
        confidence=0.99,
        reason_code="exact_symbol_match",
    )
    resolution = replace(
        _resolution(
            target,
            committed.receipt,
            resolution_id="resolution-profile",
        ),
        candidate_count=1,
        candidates=(candidate,),
    )
    execution = ImplementationTargetExecutionRecord(
        id="execution-profile",
        board_id="board-1",
        card_id="card-1",
        target_id=target.id,
        target_revision=target.revision,
        result_investigation_receipt_id=committed.receipt.id,
        disposition=ImplementationTargetExecutionDisposition.TOUCHED,
        source_ref=committed.receipt.source_ref,
        result_declared_revision=committed.receipt.declared_revision,
        result_workspace_state_id=committed.receipt.workspace_state.workspace_state_id,
        actual_relative_path="src/service.py",
        actual_qualified_symbol="Service.run",
        replacement_target_id=None,
        justification="Target implemented and validated by the external agent.",
        submitted_by="agent-1",
        received_at=NOW,
        payload_sha256=H1,
        idempotency_key="execution-profile",
    )
    base = {
        "board_id": "board-1",
        "subject_type": CodeTraceabilitySubjectType.CARD,
        "subject_id": "card-1",
        "subject_version": 4,
        "heads": (_head(store, committed.receipt),),
        "receipts": (committed.receipt,),
        "evidence_links": (evidence_link,),
        "targets": (target,),
        "resolutions": (resolution,),
        "executions": (execution,),
    }
    evaluator = CodeTraceabilityGateEvaluator(clock=lambda: NOW)
    settings = CodeTraceabilitySettings(mode="advisory")

    def render(context: CodeTraceabilityContext) -> dict[str, object]:
        return evaluator.project(context, settings).as_dict()

    summary = render(
        CodeTraceabilityContext(
            **base,
            profile=CodeTraceabilityProjectionProfile.SUMMARY,
            evidence=(
                replace(
                    evidence,
                    excerpt=None,
                    excerpt_sha256=None,
                    excerpt_omitted_reason="projection_profile_redacted",
                ),
            ),
        )
    )
    detail = render(
        CodeTraceabilityContext(
            **base,
            profile=CodeTraceabilityProjectionProfile.DETAIL,
            evidence=(evidence,),
        )
    )
    full = render(
        CodeTraceabilityContext(
            **base,
            profile=CodeTraceabilityProjectionProfile.FULL,
            evidence=(evidence,),
        )
    )
    gate = render(
        CodeTraceabilityContext(
            **base,
            profile=CodeTraceabilityProjectionProfile.FULL,
            context_scope=CodeTraceabilityContextScope.GATE,
            evidence=(
                replace(
                    evidence,
                    excerpt=None,
                    excerpt_sha256=None,
                    excerpt_omitted_reason="projection_profile_redacted",
                ),
            ),
        )
    )

    assert "claim" not in summary["evidence"][0]
    assert "excerpt" not in summary["evidence"][0]
    assert "intent" not in summary["targets"][0]
    assert "rationale" not in summary["links"][0]
    assert "receipts" not in summary
    assert "executions" not in summary

    assert detail["evidence"][0]["claim"] == evidence.claim
    assert detail["evidence"][0]["excerpt_truncated"] is True
    assert len(detail["evidence"][0]["excerpt"].encode("utf-8")) <= 2048
    assert detail["targets"][0]["intent"] == target.intent
    assert detail["links"][0]["rationale"] == evidence_link.rationale
    assert set(detail["resolutions"][0]["candidates"][0]) == {
        "relative_path",
        "qualified_symbol",
        "confidence",
        "reason_code",
    }
    assert detail["executions"][0]["id"] == "execution-profile"
    assert detail["executions"][0]["disposition"] == "touched"
    assert detail["executions"][0]["actual_relative_path"] == "src/service.py"
    assert detail["executions"][0]["justification"] == execution.justification
    assert "payload_sha256" not in detail["executions"][0]
    assert "idempotency_key" not in detail["executions"][0]

    assert full["evidence"][0]["excerpt"] == excerpt
    assert full["resolutions"][0]["candidates"][0]["symbol_signature"] == (
        "Service.run(value)"
    )
    assert full["executions"][0]["payload_sha256"] == H1
    assert gate["current_receipts"][0]["omission_manifest"] == []
    assert gate["evidence"][0]["claim"] == evidence.claim
    assert "selector_fingerprint" in gate["resolutions"][0]
    assert gate["executions"][0]["id"] == "execution-profile"
    assert '"excerpt"' not in json.dumps(gate)

    sizes = {
        name: len(json.dumps(value, separators=(",", ":"), default=str))
        for name, value in {
            "summary": summary,
            "detail": detail,
            "full": full,
            "gate": gate,
        }.items()
    }
    assert sizes["summary"] < sizes["detail"] < sizes["full"]
    assert sizes["gate"] < sizes["full"]

    evidence_limit = CODE_TRACEABILITY_CONTEXT_COLLECTION_LIMITS["evidence"]
    bounded_evidence = replace(
        evidence,
        excerpt=None,
        excerpt_sha256=None,
        excerpt_omitted_reason="projection_profile_redacted",
    )
    with pytest.raises(CodeInvestigationSubmissionLimitExceeded) as overflow:
        CodeTraceabilityContext(
            **base,
            profile=CodeTraceabilityProjectionProfile.SUMMARY,
            evidence=tuple(bounded_evidence for _ in range(evidence_limit + 1)),
        )
    assert overflow.value.details["max_items"] == evidence_limit
    assert excerpt[:100] not in str(overflow.value.as_dict())

    clipped_context = CodeTraceabilityContext(
        **base,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
        evidence=tuple(bounded_evidence for _ in range(evidence_limit)),
        omitted_content_manifest=(
            CodeTraceabilityOmittedContent(
                collection="evidence",
                hard_limit=evidence_limit,
                included_count=evidence_limit,
            ),
        ),
    )
    clipped = evaluator.project(
        clipped_context,
        CodeTraceabilitySettings(mode="blocking"),
    ).as_dict()
    omission_blockers = [
        item
        for item in clipped["gate_readiness"]["blockers"]
        if item["code"] == "code_traceability_projection_incomplete"
    ]
    assert clipped["gate_readiness"]["allowed"] is False
    assert omission_blockers == [
        {
            "code": "code_traceability_projection_incomplete",
            "message": ("Gate context exceeded a server-owned projection budget."),
            "blocking": True,
            "details": {
                "collection": "evidence",
                "hard_limit": evidence_limit,
                "omitted_at_least": 1,
                "reason": "projection_budget",
            },
            "remediation": [
                {
                    "action": (
                        "request_a_narrower_gate_context_or_reduce_traceability_scope"
                    ),
                    "tool": None,
                }
            ],
        }
    ]
    assert clipped["omitted_content_manifest"] == [
        {
            "collection": "evidence",
            "hard_limit": evidence_limit,
            "included_count": evidence_limit,
            "omitted_at_least": 1,
            "reason_code": "projection_budget",
        }
    ]

    compound_clipped = replace(
        clipped_context,
        evidence=(bounded_evidence,),
        omitted_content_manifest=(
            CodeTraceabilityOmittedContent(
                collection="evidence",
                hard_limit=evidence_limit,
                included_count=1,
            ),
        ),
    )
    assert compound_clipped.omitted_content_manifest[0].included_count == 1
    with pytest.raises(CodeTraceabilityContractError):
        replace(
            compound_clipped,
            omitted_content_manifest=(
                CodeTraceabilityOmittedContent(
                    collection="evidence",
                    hard_limit=evidence_limit,
                    included_count=2,
                ),
            ),
        )
