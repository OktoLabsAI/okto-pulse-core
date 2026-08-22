"""AC-02/AC-04 acceptance of externally investigated resolutions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.code_traceability import (
    CODE_INVESTIGATION_CANONICALIZATION_PROFILE,
    CODE_INVESTIGATION_LIMITS_PROFILE,
    CodeInvestigationAcceptanceStatus,
    CodeInvestigationCapability,
    CodeInvestigationHead,
    CodeInvestigationHeadState,
    CodeInvestigationOutcome,
    CodeInvestigationReceipt,
    CodeInvestigationReceiptCurrentness,
    CodeInvestigationTooling,
    CodeInvestigationTrustLevel,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilityPage,
    CodeTraceabilitySubjectType,
    ImplementationTarget,
    ImplementationTargetResolution,
    ImplementationTargetResolutionState,
    ImplementationTargetRole,
    ImplementationTargetSelectorKind,
    ObservedWorkspaceStateRef,
    WorkspaceReproducibilityClaim,
    code_investigation_observation_sha256,
    code_investigation_omission_digest,
    code_investigation_receipt_currentness,
)
from okto_pulse.core.models.code_traceability import (
    CodeInvestigationToolingInput,
    ImplementationTargetResolutionSubmission,
)
from okto_pulse.core.ports.code_traceability import (
    ImplementationTargetResolutionCommitResult,
)
from okto_pulse.core.services.code_investigation import (
    CodeInvestigationService,
    selector_scope_digest_for_card_targets,
)
from okto_pulse.core.services.implementation_targets import (
    ImplementationTargetService,
)


NOW = datetime(2026, 8, 9, 18, 0, tzinfo=timezone.utc)
SHA_A = "a" * 64
SHA_B = "b" * 64
OLD_PATH = "src/security/token_service.py"
NEW_PATH = "src/auth/token_service.py"
QUALIFIED_SYMBOL = "TokenService.refresh"


class InvestigationLedger:
    def __init__(
        self,
        *,
        receipt: CodeInvestigationReceipt,
        head: CodeInvestigationHead,
    ) -> None:
        self.receipt = receipt
        self.head = head

    async def get_receipt(self, *, board_id: str, receipt_id: str):
        if board_id == self.receipt.board_id and receipt_id == self.receipt.id:
            return self.receipt
        return None

    async def get_receipt_revocation(self, *, board_id: str, receipt_id: str):
        return None

    async def get_current_head(self, *, board_id: str, source_ref: str):
        if board_id == self.head.board_id and source_ref == self.head.source_ref:
            return self.head
        return None


class TargetLedger:
    def __init__(self, target: ImplementationTarget) -> None:
        self.target = target
        self.resolution: ImplementationTargetResolution | None = None
        self.append_calls = 0

    async def resolve_resolution_replay(self, **_criteria):
        return None

    async def get_target(self, *, board_id: str, target_id: str):
        if board_id == self.target.board_id and target_id == self.target.id:
            return self.target
        return None

    async def list_targets(self, query):
        items = (
            (self.target,)
            if query.board_id == self.target.board_id
            and query.card_id == self.target.card_id
            else ()
        )
        return CodeTraceabilityPage(items=items, limit=query.limit)

    async def append_resolution(
        self,
        *,
        target: ImplementationTarget,
        resolution: ImplementationTargetResolution,
        expected_target_revision: int,
        expected_head_revision: int,
    ) -> ImplementationTargetResolutionCommitResult:
        assert expected_target_revision == self.target.revision
        assert expected_head_revision == 2
        self.append_calls += 1
        self.target = target
        self.resolution = resolution
        return ImplementationTargetResolutionCommitResult(
            target=target,
            resolution=resolution,
        )


def target() -> ImplementationTarget:
    return ImplementationTarget(
        id="target-token-refresh",
        board_id="board-1",
        card_id="card-1",
        source_ref="source-opaque-1",
        selector_kind=ImplementationTargetSelectorKind.SYMBOL,
        relative_path_hint=OLD_PATH,
        language="python",
        symbol_kind="method",
        qualified_symbol=QUALIFIED_SYMBOL,
        symbol_signature="TokenService.refresh(token)",
        role=ImplementationTargetRole.MODIFY,
        intent="Refresh an access token without changing its public contract.",
        required=True,
        source_spec_version=3,
        baseline_evidence_id="evidence-token-refresh",
        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
        revision=3,
        current_resolution_id=None,
        last_change_reason_sha256=None,
        created_by="owner-1",
        created_at=NOW - timedelta(days=1),
        updated_at=NOW - timedelta(days=1),
    )


def current_receipt(
    selected_target: ImplementationTarget,
) -> tuple[CodeInvestigationReceipt, CodeInvestigationHead]:
    workspace = ObservedWorkspaceStateRef(
        declared_revision="revision-after-agent-check",
        workspace_state_id="workspace-after-agent-check",
        declared_dirty=False,
        observed_at=NOW,
        reproducibility_claim=WorkspaceReproducibilityClaim.COMMITTED,
        fingerprint_algorithm="agent-manifest-v1",
        manifest_digest=SHA_A,
        manifest_entry_count=24,
    )
    capabilities = tuple(
        sorted(
            {
                CodeInvestigationCapability.FILE_READ,
                CodeInvestigationCapability.PATH_CONTAINMENT,
                CodeInvestigationCapability.RENAME_OBSERVATION,
                CodeInvestigationCapability.REVISION_IDENTITY,
                CodeInvestigationCapability.SOURCE_IDENTITY,
                CodeInvestigationCapability.SYMLINK_CONTAINMENT,
                CodeInvestigationCapability.SYMBOL_RESOLUTION,
                CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
            },
            key=lambda item: item.value,
        )
    )
    selector_scope_digest = selector_scope_digest_for_card_targets(
        board_id=selected_target.board_id,
        card_id=selected_target.card_id,
        card_version=4,
        targets=((selected_target.id, selected_target.revision),),
    )
    observation_sha256 = code_investigation_observation_sha256(
        source_ref=selected_target.source_ref,
        selector_scope_digest=selector_scope_digest,
        outcome=CodeInvestigationOutcome.ACCESSIBLE,
        capabilities=capabilities,
        source_identity_digest=SHA_A,
        declared_revision=workspace.declared_revision,
        workspace_state=workspace,
        omission_manifest=(),
    )
    receipt = CodeInvestigationReceipt(
        id="receipt-after-agent-check",
        request_id="request-after-agent-check",
        board_id=selected_target.board_id,
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id=selected_target.card_id,
        subject_version=4,
        attestor_actor_id="agent-1",
        generation=2,
        predecessor_receipt_id="receipt-before-agent-check",
        trust_level=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
        acceptance_status=CodeInvestigationAcceptanceStatus.ACCEPTED,
        outcome=CodeInvestigationOutcome.ACCESSIBLE,
        capabilities=capabilities,
        source_ref=selected_target.source_ref,
        source_identity_digest=SHA_A,
        canonicalization_profile=CODE_INVESTIGATION_CANONICALIZATION_PROFILE,
        limits_profile=CODE_INVESTIGATION_LIMITS_PROFILE,
        selector_scope_digest=selector_scope_digest,
        declared_revision=workspace.declared_revision,
        workspace_state=workspace,
        omission_manifest=(),
        omission_digest=code_investigation_omission_digest(()),
        omission_count=0,
        tooling=CodeInvestigationTooling(
            tool_id="external-agent-check",
            tool_version="1",
            method_id="symbol-resolution/v1",
        ),
        observed_at=NOW,
        received_at=NOW + timedelta(seconds=1),
        expires_at=NOW + timedelta(hours=1),
        observation_sha256=observation_sha256,
        payload_sha256=SHA_B,
        idempotency_key="receipt-idempotency-2",
    )
    head = CodeInvestigationHead(
        board_id=selected_target.board_id,
        source_ref=selected_target.source_ref,
        generation=receipt.generation,
        latest_receipt_id=receipt.id,
        current_receipt_id=receipt.id,
        state=CodeInvestigationHeadState.CURRENT,
        revision=2,
        updated_at=NOW + timedelta(seconds=1),
    )
    return receipt, head


def resolution_submission(
    *,
    state: ImplementationTargetResolutionState,
    path: str | None,
    line_start: int | None,
    line_end: int | None,
    idempotency_key: str,
) -> ImplementationTargetResolutionSubmission:
    return ImplementationTargetResolutionSubmission(
        board_id="board-1",
        card_id="card-1",
        target_id="target-token-refresh",
        investigation_receipt_id="receipt-after-agent-check",
        state=state,
        resolved_relative_path=path,
        resolved_language="python",
        resolved_symbol_kind="method",
        resolved_qualified_symbol=QUALIFIED_SYMBOL,
        resolved_symbol_signature="TokenService.refresh(token)",
        resolved_line_start=line_start,
        resolved_line_end=line_end,
        symbol_fingerprint=SHA_A,
        declared_file_blob_sha256=SHA_B,
        confidence=0.98,
        reason_code="symbol_moved"
        if state is ImplementationTargetResolutionState.MOVED
        else None,
        candidates=(),
        tooling=CodeInvestigationToolingInput(
            tool_id="external-agent-check",
            tool_version="1",
            method_id="symbol-resolution/v1",
        ),
        agent_observed_at=NOW,
        idempotency_key=idempotency_key,
    )


async def accept(
    submission: ImplementationTargetResolutionSubmission,
) -> tuple[
    ImplementationTargetResolution,
    CodeInvestigationReceipt,
    CodeInvestigationHead,
    TargetLedger,
]:
    selected_target = target()
    receipt, head = current_receipt(selected_target)
    investigation_store = InvestigationLedger(receipt=receipt, head=head)
    target_store = TargetLedger(selected_target)
    result = await ImplementationTargetService(
        clock=lambda: NOW + timedelta(seconds=2),
        id_factory=lambda _kind: "resolution-after-agent-check",
    ).submit_resolution(
        submission,
        actor_id="agent-1",
        actor_kind="agent",
        current_card_version=4,
        minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
        require_committed_state=False,
        investigation_service=CodeInvestigationService(
            clock=lambda: NOW + timedelta(seconds=2)
        ),
        investigation_store=investigation_store,
        store=target_store,
    )
    return result.resolution, receipt, head, target_store


@pytest.mark.asyncio
async def test_ac02_line_shift_keeps_semantic_target_resolved_and_current() -> None:
    resolution, receipt, head, store = await accept(
        resolution_submission(
            state=ImplementationTargetResolutionState.RESOLVED,
            path=OLD_PATH,
            line_start=30,
            line_end=34,
            idempotency_key="resolution-line-shift",
        )
    )

    assert resolution.state is ImplementationTargetResolutionState.RESOLVED
    assert resolution.resolved_qualified_symbol == QUALIFIED_SYMBOL
    assert resolution.resolved_relative_path == OLD_PATH
    assert (resolution.resolved_line_start, resolution.resolved_line_end) == (30, 34)
    assert resolution.target_revision == store.target.revision == 3
    assert store.target.current_resolution_id == resolution.id
    assert store.append_calls == 1
    assert (
        code_investigation_receipt_currentness(
            receipt,
            head=head,
            at=NOW + timedelta(seconds=2),
        )
        is CodeInvestigationReceiptCurrentness.CURRENT
    )


@pytest.mark.asyncio
async def test_ac04_moved_resolution_requires_and_preserves_new_relative_path() -> None:
    with pytest.raises(
        ValidationError,
        match="implementation_target_resolution_resolved_threshold_invalid",
    ):
        resolution_submission(
            state=ImplementationTargetResolutionState.MOVED,
            path=None,
            line_start=None,
            line_end=None,
            idempotency_key="resolution-moved-without-path",
        )

    resolution, receipt, head, store = await accept(
        resolution_submission(
            state=ImplementationTargetResolutionState.MOVED,
            path=NEW_PATH,
            line_start=8,
            line_end=12,
            idempotency_key="resolution-moved",
        )
    )

    assert resolution.state is ImplementationTargetResolutionState.MOVED
    assert resolution.resolved_relative_path == NEW_PATH
    assert resolution.resolved_relative_path != OLD_PATH
    assert resolution.resolved_qualified_symbol == QUALIFIED_SYMBOL
    assert store.target.current_resolution_id == resolution.id
    assert store.append_calls == 1
    assert (
        code_investigation_receipt_currentness(
            receipt,
            head=head,
            at=NOW + timedelta(seconds=2),
        )
        is CodeInvestigationReceiptCurrentness.CURRENT
    )
