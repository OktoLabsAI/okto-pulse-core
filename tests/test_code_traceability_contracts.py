from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import hashlib
import inspect
from pathlib import Path

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.code_traceability import (
    CODE_INVESTIGATION_CANONICALIZATION_PROFILE,
    CODE_INVESTIGATION_LIMITS_PROFILE,
    CodeEvidence,
    CodeEvidenceAttestationBasis,
    CodeEvidenceAttestationState,
    CodeEvidenceDisposition,
    CodeEvidenceDispositionKind,
    CodeEvidenceSelectorKind,
    CodeEvidenceType,
    CodeInvestigationAcceptanceStatus,
    CodeInvestigationCapability,
    CodeInvestigationHead,
    CodeInvestigationHeadState,
    CodeInvestigationOmission,
    CodeInvestigationOmissionReason,
    CodeInvestigationOutcome,
    CodeInvestigationPayloadDigestMismatch,
    CodeInvestigationReceipt,
    CodeInvestigationReceiptCommitResult,
    CodeInvestigationReceiptCurrentness,
    CodeInvestigationRequest,
    CodeInvestigationRequestStatus,
    CodeInvestigationSubmissionLimitExceeded,
    CodeInvestigationTooling,
    CodeInvestigationTrustLevel,
    CodePathDenied,
    CodePathInvalid,
    CodeTraceabilityContext,
    CodeTraceabilityContextScope,
    CodeTraceabilityContractError,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilityProjectionProfile,
    CodeTraceabilityRemediation,
    CodeTraceabilitySubjectType,
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
    ObservedWorkspaceStateRef,
    ResolutionCandidate,
    TargetOverlapAcknowledgement,
    TargetOverlapDisposition,
    TargetOverlapSeverity,
    WorkspaceReproducibilityClaim,
    canonical_code_traceability_json_bytes,
    canonical_code_traceability_sha256,
    classify_implementation_target_overlap,
    code_investigation_observation_sha256,
    code_investigation_omission_digest,
    code_investigation_receipt_currentness,
    normalize_code_relative_path,
    parse_code_traceability_json,
)
from okto_pulse.core.models.code_traceability import (
    CodeEvidenceSubmission,
    CodeEvidenceView,
    CodeInvestigationReceiptSubmission,
    CodeInvestigationRequestView,
    ImplementationTargetResolutionSubmission,
    StartCodeInvestigationInput,
)
from okto_pulse.core.ports.code_investigation import (
    CodeInvestigationAdapterMissing,
    CodeInvestigationReadPort,
    CodeInvestigationStore,
)
from okto_pulse.core.ports.code_traceability import (
    CodeTraceabilityAdapterMissing,
    CodeTraceabilityProjectionQuery,
    CodeTraceabilityReadPort,
    CodeTraceabilityStore,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 9, 16, 0, tzinfo=UTC)
H = "a" * 64


def workspace(*, dirty: bool = False) -> ObservedWorkspaceStateRef:
    return ObservedWorkspaceStateRef(
        declared_revision="revision-1",
        workspace_state_id="workspace-state-1",
        declared_dirty=dirty,
        observed_at=NOW,
        reproducibility_claim=(
            WorkspaceReproducibilityClaim.WORKTREE_SNAPSHOT
            if dirty
            else WorkspaceReproducibilityClaim.COMMITTED
        ),
        fingerprint_algorithm="agent-manifest-v1",
        manifest_digest=H,
        manifest_entry_count=12,
    )


def request(
    *,
    status: CodeInvestigationRequestStatus = CodeInvestigationRequestStatus.OPEN,
) -> CodeInvestigationRequest:
    consumed_at = NOW + timedelta(seconds=10) if status.value == "consumed" else None
    return CodeInvestigationRequest(
        id="request-1",
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        subject_version=4,
        issued_to_actor_id="agent-1",
        source_ref="source-ref-1",
        required_capabilities=(
            CodeInvestigationCapability.FILE_READ,
            CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
        ),
        selector_scope_digest=H,
        expected_head_generation=0,
        expected_predecessor_receipt_id=None,
        canonicalization_profile=CODE_INVESTIGATION_CANONICALIZATION_PROFILE,
        limits_profile=CODE_INVESTIGATION_LIMITS_PROFILE,
        challenge_key_id="challenge-key-1",
        challenge_token_hash=H,
        status=status,
        single_use=True,
        expires_at=NOW + timedelta(minutes=10),
        requested_by="agent-1",
        created_at=NOW,
        consumed_at=consumed_at,
        request_payload_sha256=H,
        idempotency_key="idem-1",
    )


def receipt(
    *,
    outcome: CodeInvestigationOutcome = CodeInvestigationOutcome.ACCESSIBLE,
    omissions: tuple[CodeInvestigationOmission, ...] = (),
) -> CodeInvestigationReceipt:
    observed_workspace = workspace()
    capabilities = (
        CodeInvestigationCapability.FILE_READ,
        CodeInvestigationCapability.REVISION_IDENTITY,
        CodeInvestigationCapability.SOURCE_IDENTITY,
        CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
    )
    observation_sha256 = code_investigation_observation_sha256(
        source_ref="source-ref-1",
        selector_scope_digest=H,
        outcome=outcome,
        capabilities=capabilities,
        source_identity_digest=H,
        declared_revision="revision-1",
        workspace_state=observed_workspace,
        omission_manifest=omissions,
    )
    return CodeInvestigationReceipt(
        id="receipt-1",
        request_id="request-1",
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        subject_version=4,
        attestor_actor_id="agent-1",
        generation=1,
        predecessor_receipt_id=None,
        trust_level=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
        acceptance_status=CodeInvestigationAcceptanceStatus.ACCEPTED,
        outcome=outcome,
        capabilities=capabilities,
        source_ref="source-ref-1",
        source_identity_digest=H,
        canonicalization_profile=CODE_INVESTIGATION_CANONICALIZATION_PROFILE,
        limits_profile=CODE_INVESTIGATION_LIMITS_PROFILE,
        selector_scope_digest=H,
        declared_revision="revision-1",
        workspace_state=observed_workspace,
        omission_manifest=omissions,
        omission_digest=code_investigation_omission_digest(omissions),
        omission_count=sum(item.count for item in omissions),
        tooling=CodeInvestigationTooling(
            tool_id="codex",
            tool_version="1",
            method_id="source-preflight/v1",
        ),
        observed_at=NOW,
        received_at=NOW + timedelta(seconds=1),
        expires_at=NOW + timedelta(hours=1),
        observation_sha256=observation_sha256,
        payload_sha256=H,
        idempotency_key="receipt-idem-1",
    )


def evidence(
    *, dirty: bool = False, excerpt: str | None = "return value\n"
) -> CodeEvidence:
    excerpt_hash = (
        None if excerpt is None else hashlib.sha256(excerpt.encode()).hexdigest()
    )
    return CodeEvidence(
        id="evidence-1",
        board_id="board-1",
        investigation_receipt_id="receipt-1",
        source_ref="source-ref-1",
        parent_type=CodeTraceabilitySubjectType.REFINEMENT,
        parent_id="refinement-1",
        parent_version=3,
        evidence_type=CodeEvidenceType.BEHAVIOR,
        claim="The service preserves the analysis context.",
        workspace_state=workspace(dirty=dirty),
        selector_kind=CodeEvidenceSelectorKind.SYMBOL,
        relative_path="src/service.py",
        language="python",
        symbol_kind="method",
        qualified_symbol="Service.run",
        symbol_signature="Service.run(value)",
        snapshot_line_start=10,
        snapshot_line_end=11,
        excerpt=excerpt,
        excerpt_sha256=excerpt_hash,
        declared_file_blob_sha256=H,
        declared_source_content_sha256=H,
        excerpt_omitted_reason=None if excerpt is not None else "not_required",
        attestation_state=(
            CodeEvidenceAttestationState.AGENT_ATTESTED_WORKTREE
            if dirty
            else CodeEvidenceAttestationState.AGENT_ATTESTED
        ),
        attestation_basis=(CodeEvidenceAttestationBasis.AUTHENTICATED_AGENT_RECEIPT),
        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
        supersedes_evidence_id=None,
        revocation_reason=None,
        submitted_by="agent-1",
        received_at=NOW + timedelta(seconds=1),
        payload_sha256=H,
        idempotency_key="evidence-idem-1",
    )


def target(*, current_resolution_id: str | None = None) -> ImplementationTarget:
    return ImplementationTarget(
        id="target-1",
        board_id="board-1",
        card_id="card-1",
        source_ref="source-ref-1",
        selector_kind=ImplementationTargetSelectorKind.SYMBOL,
        relative_path_hint="src/service.py",
        language="python",
        symbol_kind="method",
        qualified_symbol="Service.run",
        symbol_signature="Service.run(value)",
        role=ImplementationTargetRole.MODIFY,
        intent="Preserve the attested behavior.",
        required=True,
        source_spec_version=7,
        baseline_evidence_id="evidence-1",
        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
        revision=2,
        current_resolution_id=current_resolution_id,
        last_change_reason_sha256=H,
        created_by="agent-1",
        created_at=NOW,
        updated_at=NOW + timedelta(seconds=1),
    )


def resolution(
    *,
    state: ImplementationTargetResolutionState = (
        ImplementationTargetResolutionState.RESOLVED
    ),
) -> ImplementationTargetResolution:
    terminal = state in {
        ImplementationTargetResolutionState.MISSING,
        ImplementationTargetResolutionState.UNAVAILABLE,
    }
    return ImplementationTargetResolution(
        id="resolution-1",
        board_id="board-1",
        target_id="target-1",
        investigation_receipt_id="receipt-1",
        source_ref="source-ref-1",
        receipt_generation=1,
        subject_version=4,
        target_revision=2,
        workspace_state=workspace(),
        state=state,
        resolved_relative_path=None if terminal else "src/service.py",
        resolved_language=None if terminal else "python",
        resolved_symbol_kind=None if terminal else "method",
        resolved_qualified_symbol=None if terminal else "Service.run",
        resolved_symbol_signature=None if terminal else "Service.run(value)",
        resolved_line_start=None if terminal else 10,
        resolved_line_end=None if terminal else 11,
        symbol_fingerprint=None if terminal else H,
        declared_file_blob_sha256=None if terminal else H,
        selector_fingerprint=H,
        confidence=None if terminal else 0.98,
        reason_code="source_unavailable" if terminal else None,
        candidate_count=0,
        candidates=(),
        declared_tool_id="codex",
        declared_tool_version="1",
        submitted_by="agent-1",
        agent_observed_at=NOW,
        received_at=NOW + timedelta(seconds=1),
        payload_sha256=H,
        idempotency_key="resolution-idem-1",
    )


def test_canonical_json_is_sorted_nfc_and_deterministic() -> None:
    composed = {"b": 2, "a": "caf\u00e9"}
    decomposed = {"a": "cafe\u0301", "b": 2}
    assert canonical_code_traceability_json_bytes(composed) == (
        b'{"a":"caf\xc3\xa9","b":2}'
    )
    assert canonical_code_traceability_sha256(composed) == (
        canonical_code_traceability_sha256(decomposed)
    )
    assert canonical_code_traceability_json_bytes({"number": 1.0}) == (b'{"number":1}')


@pytest.mark.parametrize(
    "raw,code",
    [
        ('{"a":1,"a":2}', "code_traceability_canonical_duplicate_key"),
        ('{"x":NaN}', "code_traceability_canonical_number_invalid"),
        ('{"x":Infinity}', "code_traceability_canonical_number_invalid"),
    ],
    ids=("duplicate-key", "nan", "infinity"),
)
def test_canonical_parser_rejects_ambiguous_json(raw: str, code: str) -> None:
    with pytest.raises(CodeTraceabilityContractError) as captured:
        parse_code_traceability_json(raw)
    assert captured.value.code == code


def test_relative_path_validation_is_pure_and_fail_closed() -> None:
    assert normalize_code_relative_path("src\\service.py") == "src/service.py"
    for unsafe in (
        "/etc/passwd",
        "C:\\repo\\file.py",
        "src/../secret.py",
        ".git/config",
    ):
        with pytest.raises((CodePathInvalid, CodePathDenied)):
            normalize_code_relative_path(unsafe)
    with pytest.raises(CodePathDenied):
        normalize_code_relative_path("config/.env.production")


def test_path_and_symbol_limits_reject_without_truncation() -> None:
    with pytest.raises(CodeInvestigationSubmissionLimitExceeded):
        normalize_code_relative_path("a" * 1025)
    with pytest.raises(CodeInvestigationSubmissionLimitExceeded):
        replace(evidence(), qualified_symbol="x" * 2049)


def test_typed_error_has_required_transport_envelope() -> None:
    error = CodePathInvalid(
        message="relative path required",
        details={"field": "relative_path"},
        remediation=(
            CodeTraceabilityRemediation(
                action="submit a relative path",
                tool="okto_pulse_submit_code_evidence",
            ),
        ),
    )
    assert error.as_dict() == {
        "code": "code_path_invalid",
        "message": "relative path required",
        "details": {"field": "relative_path"},
        "remediation": [
            {
                "action": "submit a relative path",
                "tool": "okto_pulse_submit_code_evidence",
            }
        ],
    }
    assert CodePathInvalid("bad path").code == "code_path_invalid"


def test_workspace_claim_rejects_dirty_committed_state() -> None:
    with pytest.raises(CodeTraceabilityContractError) as captured:
        replace(
            workspace(),
            declared_dirty=True,
            reproducibility_claim=WorkspaceReproducibilityClaim.COMMITTED,
        )
    assert captured.value.code == "code_investigation_workspace_claim_incoherent"


def test_request_is_single_use_bounded_and_status_coherent() -> None:
    assert request().required_capabilities == (
        CodeInvestigationCapability.FILE_READ,
        CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
    )
    with pytest.raises(CodeTraceabilityContractError):
        replace(request(), single_use=False)
    with pytest.raises(CodeTraceabilityContractError):
        replace(request(), expires_at=NOW + timedelta(seconds=601))
    with pytest.raises(CodeTraceabilityContractError):
        replace(request(), status=CodeInvestigationRequestStatus.CONSUMED)


def test_request_and_evidence_records_are_immutable() -> None:
    with pytest.raises(FrozenInstanceError):
        request().status = CodeInvestigationRequestStatus.CONSUMED  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        evidence().claim = "changed"  # type: ignore[misc]


def test_receipt_validates_outcome_omissions_and_server_digest() -> None:
    omission = CodeInvestigationOmission(
        reason_code=CodeInvestigationOmissionReason.SIZE_CAP,
        affected_scope_digest=H,
        count=2,
    )
    partial = receipt(
        outcome=CodeInvestigationOutcome.PARTIAL,
        omissions=(omission,),
    )
    assert partial.omission_count == 2
    with pytest.raises(CodeTraceabilityContractError):
        receipt(outcome=CodeInvestigationOutcome.PARTIAL)
    with pytest.raises(CodeInvestigationPayloadDigestMismatch):
        replace(partial, omission_digest="b" * 64)


def test_receipt_workspace_revision_and_observed_time_are_single_source() -> None:
    with pytest.raises(CodeTraceabilityContractError) as captured:
        replace(receipt(), declared_revision="different")
    assert captured.value.code == "code_investigation_workspace_state_mismatch"


def test_atomic_commit_bundle_enforces_request_receipt_head_lineage() -> None:
    committed_request = request(status=CodeInvestigationRequestStatus.CONSUMED)
    accepted_receipt = receipt()
    head = CodeInvestigationHead(
        board_id="board-1",
        source_ref="source-ref-1",
        generation=1,
        latest_receipt_id="receipt-1",
        current_receipt_id="receipt-1",
        state=CodeInvestigationHeadState.CURRENT,
        revision=1,
        updated_at=NOW + timedelta(seconds=1),
    )
    result = CodeInvestigationReceiptCommitResult(
        request=committed_request,
        receipt=accepted_receipt,
        head=head,
    )
    assert result.head.latest_receipt_id == result.receipt.id
    with pytest.raises(CodeTraceabilityContractError):
        replace(result, head=replace(head, latest_receipt_id="other"))


def test_receipt_currentness_is_pure_ledger_classification() -> None:
    accepted = receipt()
    head = CodeInvestigationHead(
        board_id="board-1",
        source_ref="source-ref-1",
        generation=1,
        latest_receipt_id="receipt-1",
        current_receipt_id="receipt-1",
        state=CodeInvestigationHeadState.CURRENT,
        revision=1,
        updated_at=NOW + timedelta(seconds=1),
    )
    assert (
        code_investigation_receipt_currentness(
            accepted,
            head=head,
            at=NOW + timedelta(minutes=1),
        )
        is CodeInvestigationReceiptCurrentness.CURRENT
    )
    assert (
        code_investigation_receipt_currentness(
            accepted,
            head=replace(head, state=CodeInvestigationHeadState.CONFLICTED),
            at=NOW + timedelta(minutes=1),
        )
        is CodeInvestigationReceiptCurrentness.CONFLICTED
    )
    assert (
        code_investigation_receipt_currentness(
            accepted,
            head=None,
            at=NOW + timedelta(minutes=1),
        )
        is CodeInvestigationReceiptCurrentness.UNKNOWN
    )


def test_evidence_accepts_bounded_multiline_excerpt_and_dirty_label() -> None:
    clean = evidence()
    dirty = evidence(dirty=True)
    assert clean.excerpt == "return value\n"
    assert dirty.attestation_state is (
        CodeEvidenceAttestationState.AGENT_ATTESTED_WORKTREE
    )
    with pytest.raises(CodeTraceabilityContractError):
        replace(dirty, attestation_state=CodeEvidenceAttestationState.AGENT_ATTESTED)


def test_evidence_selector_and_excerpt_hash_are_coherent() -> None:
    with pytest.raises(CodeTraceabilityContractError):
        replace(evidence(), qualified_symbol=None)
    with pytest.raises(CodeInvestigationPayloadDigestMismatch):
        replace(evidence(), excerpt_sha256="b" * 64)


def test_disposition_clearance_state_is_coherent() -> None:
    active = CodeEvidenceDisposition(
        id="disposition-1",
        board_id="board-1",
        spec_id="spec-1",
        evidence_id="evidence-1",
        disposition=CodeEvidenceDispositionKind.NOT_RELEVANT,
        justification="The legacy path is outside this specification.",
        spec_version=4,
        active=True,
        created_by="user-1",
        created_at=NOW,
        cleared_by=None,
        cleared_at=None,
    )
    cleared = replace(
        active,
        active=False,
        cleared_by="user-2",
        cleared_at=NOW + timedelta(seconds=1),
    )
    assert not cleared.active
    with pytest.raises(CodeTraceabilityContractError):
        replace(active, active=False)


def test_target_selector_requires_semantic_identity() -> None:
    assert target().qualified_symbol == "Service.run"
    with pytest.raises(CodeTraceabilityContractError):
        replace(target(), qualified_symbol=None)
    with pytest.raises(CodeTraceabilityContractError):
        replace(
            target(),
            selector_kind=ImplementationTargetSelectorKind.FILE,
            relative_path_hint=None,
        )


def test_resolution_enforces_closed_state_shapes_and_candidate_cap() -> None:
    assert resolution().state is ImplementationTargetResolutionState.RESOLVED
    assert (
        resolution(state=ImplementationTargetResolutionState.UNAVAILABLE).reason_code
        == "source_unavailable"
    )
    candidate = ResolutionCandidate(
        relative_path="src/other.py",
        qualified_symbol="Other.run",
        symbol_signature=None,
        symbol_fingerprint=H,
        confidence=0.6,
        reason_code="possible_rename",
    )
    with pytest.raises(CodeTraceabilityContractError):
        replace(
            resolution(),
            state=ImplementationTargetResolutionState.AMBIGUOUS,
            resolved_relative_path=None,
            resolved_line_start=None,
            resolved_line_end=None,
            candidate_count=1,
            candidates=(candidate,),
        )
    with pytest.raises(CodeInvestigationSubmissionLimitExceeded):
        replace(
            resolution(),
            candidate_count=21,
            candidates=tuple(candidate for _ in range(21)),
        )


def test_overlap_classification_uses_only_attested_coordinates() -> None:
    target_a = target()
    resolution_a = resolution()
    target_b = replace(target(), id="target-2")
    resolution_b = replace(
        resolution(),
        id="resolution-2",
        target_id="target-2",
    )
    overlap = classify_implementation_target_overlap(
        target_a,
        resolution_a,
        target_b,
        resolution_b,
    )
    assert overlap.severity is TargetOverlapSeverity.HIGH
    assert overlap.reason_code == "same_symbol_mutation"
    informational = classify_implementation_target_overlap(
        target_a,
        resolution_a,
        replace(target_b, role=ImplementationTargetRole.READ),
        resolution_b,
    )
    assert informational.severity is TargetOverlapSeverity.INFORMATIONAL


def test_execution_replacement_requires_another_target() -> None:
    base = ImplementationTargetExecutionRecord(
        id="execution-1",
        board_id="board-1",
        card_id="card-1",
        target_id="target-1",
        target_revision=2,
        result_investigation_receipt_id="receipt-2",
        disposition=ImplementationTargetExecutionDisposition.TOUCHED,
        source_ref="source-ref-1",
        result_declared_revision="revision-2",
        result_workspace_state_id="workspace-state-2",
        actual_relative_path="src/service.py",
        actual_qualified_symbol="Service.run",
        replacement_target_id=None,
        justification="The target was updated and checked by the agent.",
        submitted_by="agent-1",
        received_at=NOW,
        payload_sha256=H,
        idempotency_key="execution-idem-1",
    )
    assert base.disposition is ImplementationTargetExecutionDisposition.TOUCHED
    with pytest.raises(CodeTraceabilityContractError):
        replace(
            base,
            disposition=ImplementationTargetExecutionDisposition.REPLACED,
        )


def test_overlap_acknowledgement_canonicalizes_pair_with_resolutions() -> None:
    acknowledgement = TargetOverlapAcknowledgement(
        id="ack-1",
        board_id="board-1",
        target_a_id="target-z",
        target_b_id="target-a",
        resolution_a_id="resolution-z",
        resolution_b_id="resolution-a",
        disposition=TargetOverlapDisposition.ORDERED_BY_DEPENDENCY,
        justification="The second card depends on the first.",
        created_by="user-1",
        created_at=NOW,
    )
    assert acknowledgement.target_a_id == "target-a"
    assert acknowledgement.resolution_a_id == "resolution-a"


def test_waiver_clear_is_monotonic_and_auditable() -> None:
    active = CodeTraceabilityWaiver(
        id="waiver-1",
        board_id="board-1",
        entity_type=CodeTraceabilityWaiverEntityType.CARD,
        entity_id="card-1",
        scope=CodeTraceabilityWaiverScope.IMPLEMENTATION_TARGET,
        reason_code=CodeTraceabilityWaiverReason.DOCUMENTATION_ONLY,
        justification="This card changes documentation only.",
        active=True,
        created_by="user-1",
        created_at=NOW,
        cleared_by=None,
        cleared_at=None,
    )
    assert (
        replace(
            active,
            active=False,
            cleared_by="user-2",
            cleared_at=NOW + timedelta(seconds=1),
        ).cleared_by
        == "user-2"
    )
    with pytest.raises(CodeTraceabilityContractError):
        replace(active, cleared_by="user-2")


def test_summary_and_gate_contexts_cannot_carry_excerpts() -> None:
    with pytest.raises(CodeTraceabilityContractError):
        CodeTraceabilityContext(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
            subject_id="refinement-1",
            subject_version=3,
            profile=CodeTraceabilityProjectionProfile.SUMMARY,
            evidence=(evidence(),),
        )
    with pytest.raises(CodeTraceabilityContractError):
        CodeTraceabilityContext(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
            subject_id="refinement-1",
            subject_version=3,
            profile=CodeTraceabilityProjectionProfile.FULL,
            context_scope=CodeTraceabilityContextScope.GATE,
            evidence=(evidence(),),
        )


def test_projection_query_requires_full_for_gate_scope() -> None:
    with pytest.raises(CodeTraceabilityContractError):
        CodeTraceabilityProjectionQuery(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.CARD,
            subject_id="card-1",
            subject_version=4,
            profile=CodeTraceabilityProjectionProfile.DETAIL,
            context_scope=CodeTraceabilityContextScope.GATE,
        )


def test_submission_models_forbid_server_owned_fields() -> None:
    with pytest.raises(ValidationError) as captured:
        CodeInvestigationReceiptSubmission.model_validate(
            {
                "board_id": "board-1",
                "request_id": "request-1",
                "challenge_token": "secret-token",
                "outcome": "accessible",
                "capabilities": ["file_read"],
                "workspace_state": None,
                "omission_manifest": [],
                "tooling": {
                    "tool_id": "codex",
                    "tool_version": "1",
                    "method_id": "source-preflight/v1",
                },
                "observed_at": NOW,
                "idempotency_key": "idem-1",
                "source_ref": "forged-source-ref",
                "attestor_actor_id": "forged-actor",
            }
        )
    forbidden_fields = {item["loc"][0] for item in captured.value.errors()}
    assert forbidden_fields == {"attestor_actor_id", "source_ref"}


def test_evidence_submission_forbids_attestation_and_actor_fields() -> None:
    excerpt = "safe excerpt"
    payload = {
        "board_id": "board-1",
        "investigation_receipt_id": "receipt-1",
        "parent_type": "refinement",
        "parent_id": "refinement-1",
        "evidence_type": "behavior",
        "claim": "The service preserves context.",
        "selector": {
            "kind": "symbol",
            "relative_path": "src/service.py",
            "qualified_symbol": "Service.run",
        },
        "excerpt": excerpt,
        "excerpt_sha256": hashlib.sha256(excerpt.encode()).hexdigest(),
        "declared_source_content_sha256": H,
        "idempotency_key": "idem-1",
        "submitted_by": "forged-actor",
        "attestation_state": "agent_attested",
    }
    with pytest.raises(ValidationError) as captured:
        CodeEvidenceSubmission.model_validate(payload)
    assert {item["loc"][0] for item in captured.value.errors()} == {
        "attestation_state",
        "submitted_by",
    }


def test_evidence_envelope_limit_rejects_whole_submission() -> None:
    record = evidence(excerpt=None)
    with pytest.raises(CodeInvestigationSubmissionLimitExceeded):
        replace(record, claim="x" * (129 * 1024))
    with pytest.raises(ValidationError):
        CodeEvidenceSubmission.model_validate(
            {
                "board_id": "board-1",
                "investigation_receipt_id": "receipt-1",
                "parent_type": "refinement",
                "parent_id": "refinement-1",
                "evidence_type": "behavior",
                "claim": "x" * (129 * 1024),
                "selector": {
                    "kind": "symbol",
                    "relative_path": "src/service.py",
                    "qualified_symbol": "Service.run",
                },
                "declared_source_content_sha256": H,
                "idempotency_key": "idem-1",
            }
        )


def test_resolution_submission_has_no_server_lineage_fields() -> None:
    payload = {
        "board_id": "board-1",
        "card_id": "card-1",
        "target_id": "target-1",
        "investigation_receipt_id": "receipt-1",
        "state": "unavailable",
        "reason_code": "source_unavailable",
        "tooling": {
            "tool_id": "codex",
            "tool_version": "1",
            "method_id": "agent-resolution/v1",
        },
        "agent_observed_at": NOW,
        "idempotency_key": "idem-1",
        "source_ref": "forged",
        "receipt_generation": 999,
        "target_revision": 999,
    }
    with pytest.raises(ValidationError) as captured:
        ImplementationTargetResolutionSubmission.model_validate(payload)
    assert {item["loc"][0] for item in captured.value.errors()} == {
        "receipt_generation",
        "source_ref",
        "target_revision",
    }


def test_request_view_never_projects_challenge_hash() -> None:
    view = CodeInvestigationRequestView.from_domain(request())
    assert "challenge_token_hash" not in view.model_dump()


def test_evidence_view_enforces_profile_excerpt_policy() -> None:
    record = evidence(excerpt="x" * 3000)
    summary = CodeEvidenceView.project(
        record,
        profile=CodeTraceabilityProjectionProfile.SUMMARY,
    )
    detail = CodeEvidenceView.project(
        record,
        profile=CodeTraceabilityProjectionProfile.DETAIL,
    )
    full = CodeEvidenceView.project(
        record,
        profile=CodeTraceabilityProjectionProfile.FULL,
    )
    summary_payload = summary.model_dump(mode="json", exclude_none=True)
    detail_payload = detail.model_dump(mode="json", exclude_none=True)
    assert summary.excerpt is None
    assert {
        "claim",
        "workspace_state",
        "symbol_signature",
        "snapshot_line_start",
        "snapshot_line_end",
        "excerpt_sha256",
        "declared_file_blob_sha256",
        "declared_source_content_sha256",
        "submitted_by",
        "received_at",
        "payload_sha256",
    }.isdisjoint(summary_payload)
    assert "claim" in detail_payload
    assert "symbol_signature" in detail_payload
    assert "submitted_by" in detail_payload
    assert "payload_sha256" not in detail_payload
    assert detail.excerpt_truncated
    assert len(detail.excerpt.encode()) <= 2048  # type: ignore[union-attr]
    assert full.excerpt == record.excerpt


def test_start_input_is_closed_and_version_fenced() -> None:
    start = StartCodeInvestigationInput(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        expected_subject_version=4,
        idempotency_key="idem-1",
    )
    assert start.expected_subject_version == 4
    with pytest.raises(ValidationError):
        StartCodeInvestigationInput.model_validate(
            {**start.model_dump(), "repository_path": "C:/repo"}
        )
    with pytest.raises(ValidationError):
        StartCodeInvestigationInput.model_validate(
            {**start.model_dump(), "source_ref": "https://provider/repository"}
        )


def test_ports_are_runtime_checkable_and_transaction_bound() -> None:
    assert getattr(CodeInvestigationReadPort, "_is_runtime_protocol")
    assert getattr(CodeInvestigationStore, "_is_runtime_protocol")
    assert getattr(CodeTraceabilityReadPort, "_is_runtime_protocol")
    assert getattr(CodeTraceabilityStore, "_is_runtime_protocol")
    signature = inspect.signature(
        CodeInvestigationStore.consume_request_append_receipt_and_advance_head
    )
    assert "context" not in signature.parameters
    assert set(signature.parameters) == {
        "self",
        "request",
        "receipt",
        "head",
        "expected_head_revision",
    }


def test_missing_adapters_fail_closed_with_typed_errors() -> None:
    investigation = CodeInvestigationAdapterMissing()
    traceability = CodeTraceabilityAdapterMissing()
    assert investigation.as_dict()["code"] == "code_investigation_adapter_missing"
    assert traceability.as_dict()["code"] == "code_traceability_adapter_missing"


def test_public_contracts_do_not_import_repository_access_mechanisms() -> None:
    root = Path(__file__).parents[1] / "src" / "okto_pulse" / "core"
    files = (
        root / "domain" / "code_traceability.py",
        root / "models" / "code_traceability.py",
        root / "ports" / "code_investigation.py",
        root / "ports" / "code_traceability.py",
    )
    forbidden_import_roots = {
        "dulwich",
        "git",
        "gitpython",
        "httpx",
        "pathlib",
        "requests",
        "subprocess",
    }
    for file in files:
        tree = ast.parse(file.read_text(encoding="utf-8"))
        imported = {
            node.module.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        }
        imported.update(
            alias.name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        )
        assert imported.isdisjoint(forbidden_import_roots), (file, imported)
