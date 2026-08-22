from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.code_traceability import (
    SOURCE_CONTEXT_INTERPRETATION_RULE_V2,
    CodeDeliveryContextOverrideReasonRequired,
    CodeEvidenceBaselinePresence,
    CodeEvidenceBaselineProvenance,
    CodeEvidenceBaselineProvenanceInvalid,
    CodeEvidenceLegacyRoleWriteForbidden,
    CodeEvidencePostBaselineSourceForbidden,
    CodeEvidenceSourceRole,
    CodeTraceabilityContractError,
    CodeTraceabilityProjectionProfile,
    ContextualInvestigationOutcomeV2,
    DeliveryContext,
    DirectSpecDeliveryContextProvenance,
    ObservedWorkspaceStateRef,
    SourceContextClassificationStateV2,
    SourceContextRoleCountsV2,
    SourceContextSummaryV2,
    RefinementDeliveryContextProvenance,
    RefinementSourceContextManifestV2,
    SpecDeliveryContextProvenance,
    WorkspaceReproducibilityClaim,
    authored_code_evidence_source_role,
    parse_refinement_source_context_manifest_v2,
    source_context_classification_input_v2,
)
from okto_pulse.core.events.types import (
    CodeEvidenceCreated,
    CodeEvidenceSuperseded,
)
from okto_pulse.core.models.code_traceability import (
    CodeEvidenceSubmission,
    CodeEvidenceSubmissionV2,
    CodeEvidenceSupersessionSubmissionV2,
    CodeEvidenceView,
)
from okto_pulse.core.models.schemas import RefinementCreate
from okto_pulse.core.services.code_evidence import CodeEvidenceService
from okto_pulse.core.services.code_evidence_sanitization import (
    sanitize_code_evidence_submission,
)


NOW = datetime(2026, 8, 21, 18, 0, tzinfo=timezone.utc)
SHA = "a" * 64


def _workspace(*, dirty: bool = False, workspace_state_id: str = "ws-1"):
    return ObservedWorkspaceStateRef(
        declared_revision="revision-1",
        workspace_state_id=workspace_state_id,
        declared_dirty=dirty,
        observed_at=NOW,
        reproducibility_claim=(
            WorkspaceReproducibilityClaim.WORKTREE_SNAPSHOT
            if dirty
            else WorkspaceReproducibilityClaim.COMMITTED
        ),
        fingerprint_algorithm="agent-manifest-v1",
        manifest_digest=SHA,
        manifest_entry_count=10,
    )


def _submission_payload() -> dict[str, object]:
    return {
        "board_id": "board-1",
        "investigation_receipt_id": "receipt-1",
        "parent_type": "spec",
        "parent_id": "spec-1",
        "evidence_type": "structure",
        "claim": "The baseline contains a reusable service scaffold.",
        "selector": {
            "kind": "file",
            "relative_path": "src/service.py",
        },
        "declared_source_content_sha256": SHA,
        "idempotency_key": "evidence-idem-1",
    }


def _contextual_submission(
    *,
    role: CodeEvidenceSourceRole = CodeEvidenceSourceRole.CURRENT_IMPLEMENTATION,
    presence: CodeEvidenceBaselinePresence = (
        CodeEvidenceBaselinePresence.COMMITTED_SNAPSHOT
    ),
    workspace_state_id: str = "ws-1",
    interpretation_limit: str | None = None,
) -> CodeEvidenceSubmissionV2:
    payload = {
        **_submission_payload(),
        "source_role": role,
        "relevance_summary": "  Existing source constrains the delivery.  ",
        "scope_relation": "  Directly inside the governed delivery scope.  ",
        "source_origin": "  Repository baseline selected by the receipt.  ",
        "interpretation_limit": interpretation_limit,
        "baseline_provenance": {
            "presence": presence,
            "workspace_state_id": workspace_state_id,
            "provenance_note": (
                "Observed before implementation in the dirty worktree."
                if presence is CodeEvidenceBaselinePresence.PREEXISTING_WORKTREE
                else None
            ),
        },
    }
    return CodeEvidenceSubmissionV2.model_validate(payload)


def _accepted(workspace: ObservedWorkspaceStateRef) -> SimpleNamespace:
    return SimpleNamespace(
        receipt=SimpleNamespace(
            id="receipt-1",
            source_ref="source-1",
            subject_version=3,
            workspace_state=workspace,
        )
    )


def _materialize(
    submission: CodeEvidenceSubmission,
    *,
    workspace: ObservedWorkspaceStateRef | None = None,
):
    service = CodeEvidenceService(
        clock=lambda: NOW,
        id_factory=lambda prefix: f"{prefix}-1",
    )
    return service._materialize(
        submission,
        accepted=_accepted(workspace or _workspace()),
        actor_id="agent-1",
        payload_sha256=SHA,
        supersedes_evidence_id=None,
    )


def test_delivery_context_contract_preserves_inherited_and_effective_values() -> None:
    inherited = SpecDeliveryContextProvenance(
        value=DeliveryContext.GREENFIELD,
        inherited_value=DeliveryContext.GREENFIELD,
        source_refinement_id="refinement-1",
        source_refinement_version=7,
    )
    overridden = SpecDeliveryContextProvenance(
        value=DeliveryContext.HYBRID,
        inherited_value=DeliveryContext.GREENFIELD,
        source_refinement_id="refinement-1",
        source_refinement_version=7,
        override_reason="A committed scaffold was added before Spec execution.",
    )

    assert not inherited.overridden
    assert overridden.overridden
    assert overridden.inherited_value is DeliveryContext.GREENFIELD
    with pytest.raises(CodeDeliveryContextOverrideReasonRequired):
        SpecDeliveryContextProvenance(
            value=DeliveryContext.BROWNFIELD,
            inherited_value=DeliveryContext.GREENFIELD,
            source_refinement_id="refinement-1",
            source_refinement_version=7,
        )


def test_direct_spec_context_has_typed_authorship_provenance() -> None:
    provenance = DirectSpecDeliveryContextProvenance(
        value=DeliveryContext.GREENFIELD,
        source_spec_id="spec-1",
        source_spec_version=1,
    )

    assert provenance.value is DeliveryContext.GREENFIELD
    assert provenance.source_spec_id == "spec-1"


def test_refinement_source_context_manifest_digest_seals_exact_version() -> None:
    provenance = RefinementDeliveryContextProvenance(
        value=DeliveryContext.BROWNFIELD,
        source_refinement_id="refinement-1",
        source_refinement_version=3,
    )
    manifest = RefinementSourceContextManifestV2(
        refinement_id="refinement-1",
        refinement_version=3,
        summary=SourceContextSummaryV2(
            delivery_context=DeliveryContext.BROWNFIELD,
            delivery_context_provenance=provenance,
            investigation_outcome=None,
            role_counts=SourceContextRoleCountsV2(),
            classification_state=SourceContextClassificationStateV2(
                classified_count=0,
                uncategorized_legacy_count=0,
            ),
            evidence_applicable=None,
            interpretation_rule=SOURCE_CONTEXT_INTERPRETATION_RULE_V2,
            items_not_current_implementation_count=0,
            technical_details_available=False,
        ),
        current_receipts=(),
    )

    assert manifest.as_dict()["subject_version"] == 3
    assert len(manifest.payload_sha256) == 64
    assert parse_refinement_source_context_manifest_v2(manifest.as_dict()) == manifest

    noncanonical = {**manifest.as_dict(), "unexpected": True}
    with pytest.raises(
        CodeTraceabilityContractError,
        match="source_context_manifest_structure_invalid",
    ):
        parse_refinement_source_context_manifest_v2(noncanonical)


def test_contextual_enums_are_closed_and_legacy_role_is_projection_only() -> None:
    assert {item.value for item in DeliveryContext} == {
        "brownfield",
        "greenfield",
        "hybrid",
    }
    assert {item.value for item in ContextualInvestigationOutcomeV2} == {
        "evidence_applicable",
        "no_relevant_existing_implementation",
        "partial",
        "unavailable",
    }
    with pytest.raises(CodeEvidenceLegacyRoleWriteForbidden):
        authored_code_evidence_source_role("uncategorized_legacy")


def test_new_refinement_requires_explicit_delivery_context() -> None:
    with pytest.raises(ValidationError) as raised:
        RefinementCreate(ideation_id="ideation-1", title="Missing context")

    assert raised.value.errors()[0]["loc"] == ("delivery_context",)


def test_source_context_summary_is_closed_coherent_and_human_oriented() -> None:
    provenance = SpecDeliveryContextProvenance(
        value=DeliveryContext.HYBRID,
        inherited_value=DeliveryContext.GREENFIELD,
        source_refinement_id="refinement-1",
        source_refinement_version=7,
        override_reason="A baseline scaffold makes this delivery hybrid.",
    )
    counts = SourceContextRoleCountsV2(
        current_implementation_count=2,
        existing_scaffold_count=1,
        existing_constraint_count=1,
        reference_pattern_count=1,
        uncategorized_legacy_count=1,
    )
    classification = SourceContextClassificationStateV2(
        classified_count=5,
        uncategorized_legacy_count=1,
    )
    summary = SourceContextSummaryV2(
        delivery_context=DeliveryContext.HYBRID,
        delivery_context_provenance=provenance,
        investigation_outcome=(ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE),
        role_counts=counts,
        classification_state=classification,
        evidence_applicable=True,
        interpretation_rule=(
            "  Treat only current implementation as delivered behavior.  "
        ),
        items_not_current_implementation_count=4,
        technical_details_available=True,
    )

    assert counts.total_count == 6
    assert counts.count_for(CodeEvidenceSourceRole.REFERENCE_PATTERN) == 1
    assert classification.has_unclassified_legacy
    assert not classification.fully_classified
    assert summary.interpretation_rule == (
        "Treat only current implementation as delivered behavior."
    )
    with pytest.raises(ValueError, match="source_context_role_count_invalid"):
        SourceContextRoleCountsV2(existing_scaffold_count=-1)
    with pytest.raises(
        ValueError,
        match="source_context_items_not_current_implementation_count_invalid",
    ):
        SourceContextSummaryV2(
            delivery_context=DeliveryContext.HYBRID,
            delivery_context_provenance=provenance,
            investigation_outcome=(
                ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE
            ),
            role_counts=counts,
            classification_state=classification,
            evidence_applicable=True,
            interpretation_rule="Use role-aware interpretation.",
            items_not_current_implementation_count=3,
            technical_details_available=True,
        )


def test_no_relevant_outcome_rejects_current_implementation_count() -> None:
    provenance = SpecDeliveryContextProvenance(
        value=DeliveryContext.GREENFIELD,
        inherited_value=DeliveryContext.GREENFIELD,
        source_refinement_id="refinement-1",
        source_refinement_version=1,
    )
    with pytest.raises(ValueError, match="source_context_investigation_outcome_invalid"):
        SourceContextSummaryV2(
            delivery_context=DeliveryContext.GREENFIELD,
            delivery_context_provenance=provenance,
            investigation_outcome=(
                ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
            ),
            role_counts=SourceContextRoleCountsV2(
                current_implementation_count=1,
            ),
            classification_state=SourceContextClassificationStateV2(
                classified_count=1,
                uncategorized_legacy_count=0,
            ),
            evidence_applicable=False,
            interpretation_rule="Use role-aware interpretation.",
            items_not_current_implementation_count=0,
            technical_details_available=True,
        )


def test_worktree_baseline_requires_a_human_readable_provenance_note() -> None:
    with pytest.raises(CodeEvidenceBaselineProvenanceInvalid):
        CodeEvidenceBaselineProvenance(
            presence=CodeEvidenceBaselinePresence.PREEXISTING_WORKTREE,
            workspace_state_id="ws-1",
        )

    provenance = CodeEvidenceBaselineProvenance(
        presence=CodeEvidenceBaselinePresence.PREEXISTING_WORKTREE,
        workspace_state_id="ws-1",
        provenance_note="Observed before the implementation session began.",
    )
    assert provenance.workspace_state_id == "ws-1"


def test_v2_submission_requires_authored_role_and_interpretation_boundary() -> None:
    scaffold_payload = {
        **_submission_payload(),
        "source_role": "existing_scaffold",
        "relevance_summary": "The scaffold fixes the package layout.",
        "scope_relation": "In scope.",
        "source_origin": "Committed repository baseline.",
        "baseline_provenance": {
            "presence": "committed_snapshot",
            "workspace_state_id": "ws-1",
        },
    }
    with pytest.raises(
        ValidationError,
        match="code_evidence_source_role_required",
    ):
        CodeEvidenceSubmissionV2.model_validate(
            {
                key: value
                for key, value in scaffold_payload.items()
                if key != "source_role"
            }
        )
    with pytest.raises(
        ValidationError,
        match="code_evidence_interpretation_limit_required",
    ):
        CodeEvidenceSubmissionV2.model_validate(scaffold_payload)

    with pytest.raises(
        ValidationError,
        match="code_evidence_legacy_role_write_forbidden",
    ):
        CodeEvidenceSubmissionV2.model_validate(
            {**scaffold_payload, "source_role": "uncategorized_legacy"}
        )

    scaffold = _contextual_submission(
        role=CodeEvidenceSourceRole.EXISTING_SCAFFOLD,
        interpretation_limit=(
            "  Layout reference only; it is not delivered behavior.  "
        ),
    )
    constraint = _contextual_submission(
        role=CodeEvidenceSourceRole.EXISTING_CONSTRAINT,
    )
    assert scaffold.interpretation_limit == (
        "Layout reference only; it is not delivered behavior."
    )
    assert constraint.interpretation_limit is None
    assert scaffold.relevance_summary == ("Existing source constrains the delivery.")


def test_v2_supersession_carries_the_complete_context_instead_of_a_patch() -> None:
    payload = _contextual_submission().model_dump(mode="python")
    command = CodeEvidenceSupersessionSubmissionV2.model_validate(
        {
            **payload,
            "supersedes_evidence_id": "evidence-old",
            "supersession_reason": "The baseline classification was corrected.",
        }
    )
    assert command.source_role is CodeEvidenceSourceRole.CURRENT_IMPLEMENTATION
    assert command.baseline_provenance.workspace_state_id == "ws-1"


def test_sanitization_preserves_the_v2_context_contract() -> None:
    submission = _contextual_submission(
        role=CodeEvidenceSourceRole.REFERENCE_PATTERN,
        interpretation_limit="Reference only; do not infer delivered behavior.",
    )
    sanitized = sanitize_code_evidence_submission(submission)

    assert isinstance(sanitized, CodeEvidenceSubmissionV2)
    assert sanitized.source_role is CodeEvidenceSourceRole.REFERENCE_PATTERN
    assert sanitized.baseline_provenance == submission.baseline_provenance


def test_materialization_preserves_context_and_hashes_it_canonically() -> None:
    contextual = _contextual_submission()
    legacy = CodeEvidenceSubmission.model_validate(_submission_payload())
    service = CodeEvidenceService()
    contextual_hash = service._payload_sha256(
        contextual,
        actor_id="agent-1",
        source_ref="source-1",
        parent_version=3,
        supersedes_evidence_id=None,
    )
    legacy_hash = service._payload_sha256(
        legacy,
        actor_id="agent-1",
        source_ref="source-1",
        parent_version=3,
        supersedes_evidence_id=None,
    )
    record = _materialize(contextual)
    view = CodeEvidenceView.project(
        record,
        profile=CodeTraceabilityProjectionProfile.SUMMARY,
    )

    assert contextual_hash != legacy_hash
    assert record.source_role is CodeEvidenceSourceRole.CURRENT_IMPLEMENTATION
    assert record.relevance_summary == contextual.relevance_summary
    assert view.source_role is CodeEvidenceSourceRole.CURRENT_IMPLEMENTATION
    assert view.baseline_provenance is not None
    assert view.baseline_provenance.presence is (
        CodeEvidenceBaselinePresence.COMMITTED_SNAPSHOT
    )


def test_legacy_submission_remains_readable_but_explicitly_unclassified() -> None:
    record = _materialize(CodeEvidenceSubmission.model_validate(_submission_payload()))
    view = CodeEvidenceView.project(
        record,
        profile=CodeTraceabilityProjectionProfile.SUMMARY,
    )

    assert record.source_role is CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY
    assert record.baseline_provenance is None
    assert view.source_role is CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY


def test_legacy_classification_input_derives_clean_and_dirty_baselines() -> None:
    clean = source_context_classification_input_v2(
        _materialize(CodeEvidenceSubmission.model_validate(_submission_payload()))
    )
    dirty_workspace = _workspace(dirty=True, workspace_state_id="ws-dirty")
    dirty = source_context_classification_input_v2(
        _materialize(
            CodeEvidenceSubmission.model_validate(_submission_payload()),
            workspace=dirty_workspace,
        )
    )

    assert clean.expected_classification_revision == 0
    assert clean.expected_evidence_payload_sha256 == SHA
    assert clean.baseline_provenance.presence is (
        CodeEvidenceBaselinePresence.COMMITTED_SNAPSHOT
    )
    assert clean.baseline_provenance.provenance_note_required is False
    assert clean.baseline_provenance.provenance_note is None
    assert dirty.baseline_provenance.presence is (
        CodeEvidenceBaselinePresence.PREEXISTING_WORKTREE
    )
    assert dirty.baseline_provenance.workspace_state_id == "ws-dirty"
    assert dirty.baseline_provenance.provenance_note_required is True
    assert dirty.baseline_provenance.provenance_note is None


def test_baseline_must_match_receipt_workspace_identity_and_presence() -> None:
    with pytest.raises(CodeEvidencePostBaselineSourceForbidden):
        _materialize(_contextual_submission(workspace_state_id="different-ws"))

    worktree_claim = _contextual_submission(
        presence=CodeEvidenceBaselinePresence.PREEXISTING_WORKTREE,
    )
    with pytest.raises(CodeEvidenceBaselineProvenanceInvalid):
        _materialize(worktree_claim, workspace=_workspace(dirty=False))

    worktree_record = _materialize(
        worktree_claim,
        workspace=_workspace(dirty=True),
    )
    assert worktree_record.baseline_provenance is not None
    assert worktree_record.baseline_provenance.presence is (
        CodeEvidenceBaselinePresence.PREEXISTING_WORKTREE
    )


def test_contextual_event_metadata_is_bounded_and_legacy_payload_is_stable() -> None:
    base = {
        "board_id": "board-1",
        "actor_id": "agent-1",
        "actor_type": "agent",
        "evidence_id": "evidence-1",
        "investigation_receipt_id": "receipt-1",
        "parent_type": "spec",
        "parent_id": "spec-1",
        "lifecycle_status": "active",
        "attestation_state": "agent_attested",
        "payload_sha256": SHA,
    }
    legacy_payload = CodeEvidenceCreated(**base).payload_for_storage()
    contextual_payload = CodeEvidenceCreated(
        **base,
        source_role="existing_scaffold",
        baseline_presence="committed_snapshot",
        relevance_summary="The scaffold defines the package boundary.",
        scope_relation="Inside the governed delivery scope.",
        source_origin="Committed source baseline.",
        interpretation_limit="Scaffold only; not delivered behavior.",
        baseline_workspace_state_id="ws-1",
    ).payload_for_storage()
    superseded_payload = CodeEvidenceSuperseded(
        board_id="board-1",
        actor_id="agent-1",
        actor_type="agent",
        superseded_evidence_id="evidence-1",
        superseding_evidence_id="evidence-2",
        investigation_receipt_id="receipt-2",
        payload_sha256=SHA,
        source_role="reference_pattern",
        baseline_presence="preexisting_worktree",
        relevance_summary="The pattern constrains integration shape.",
        scope_relation="Adjacent reference.",
        source_origin="Preexisting worktree baseline.",
        interpretation_limit="Reference only; not delivered behavior.",
        baseline_workspace_state_id="ws-2",
        baseline_provenance_note="Observed before implementation began.",
    ).payload_for_storage()

    assert "source_role" not in legacy_payload
    assert "baseline_presence" not in legacy_payload
    assert contextual_payload["source_role"] == "existing_scaffold"
    assert contextual_payload["baseline_presence"] == "committed_snapshot"
    assert contextual_payload["baseline_workspace_state_id"] == "ws-1"
    assert contextual_payload["relevance_summary"] == (
        "The scaffold defines the package boundary."
    )
    assert superseded_payload["source_role"] == "reference_pattern"
