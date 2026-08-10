from __future__ import annotations

import ast
import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import SecretStr, ValidationError

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.code_traceability import (
    AcknowledgeImplementationOverlapUseCase,
    ApplySpecCodeEvidenceRebaseUseCase,
    ClearCodeTraceabilityNotApplicableUseCase,
    CreateImplementationTargetUseCase,
    GetCodeTraceabilityProjectionUseCase,
    GetImplementationOverlapsUseCase,
    MarkCodeTraceabilityNotApplicableUseCase,
    PreviewSpecCodeEvidenceRebaseUseCase,
    StartCodeInvestigationUseCase,
    SubmitCodeEvidenceUseCase,
    SubmitCodeInvestigationReceiptUseCase,
)
from okto_pulse.core.domain.code_traceability import (
    CodeEvidenceAttestationBasis,
    CodeEvidenceAttestationState,
    CodeEvidenceDisposition,
    CodeEvidenceDispositionKind,
    CodeEvidenceLinkInvalid,
    CodeEvidenceSelectorKind,
    CodeEvidenceSpecLink,
    CodeEvidenceSubmissionFailed,
    CodeEvidenceSpecRelationType,
    CodeEvidenceType,
    CodeInvestigationActorKindRequired,
    CodeInvestigationCapability,
    CodeInvestigationCapabilityMissing,
    CodeInvestigationHeadConflict,
    CodeInvestigationHeadState,
    CodeInvestigationIdempotencyConflict,
    CodeInvestigationOutcome,
    CodeInvestigationOmissionReason,
    CodeInvestigationReceiptCommitResult,
    CodeInvestigationRequestStatus,
    CodeInvestigationSourceScopeMismatch,
    CodeInvestigationSubmissionLimitExceeded,
    CodeInvestigationTrustLevel,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilityContextScope,
    CodeTraceabilityPage,
    CodeTraceabilityProjectionProfile,
    CodeTraceabilitySubjectType,
    CodeTraceabilityContractError,
    CodeTraceabilityWaiverEntityType,
    CodeTraceabilityWaiverReason,
    CodeTraceabilityWaiverScope,
    ImplementationTarget,
    ImplementationTargetEvidenceRelationType,
    ImplementationTargetResolution,
    ImplementationTargetExecutionDisposition,
    ImplementationTargetInvalid,
    ImplementationTargetResolutionOutdated,
    ImplementationTargetResolutionState,
    ImplementationTargetRole,
    ImplementationTargetSelectorKind,
    SpecEntityType,
    ObservedWorkspaceStateRef,
    TargetOverlap,
    TargetOverlapDisposition,
    TargetOverlapSeverity,
    WorkspaceReproducibilityClaim,
)
from okto_pulse.core.models.code_traceability import (
    CodeEvidenceSelectorInput,
    CodeEvidenceRevokeInput,
    CodeEvidenceSubmission,
    CodeEvidenceSupersessionSubmission,
    CodeInvestigationReceiptSubmission,
    CodeInvestigationOmissionInput,
    CodeInvestigationToolingInput,
    CodeTraceabilityWaiverClearInput,
    CodeTraceabilityWaiverInput,
    ImplementationTargetCreateInput,
    ImplementationTargetEvidenceLinkInput,
    ImplementationTargetExecutionSubmission,
    ImplementationTargetResolutionSubmission,
    ImplementationTargetSpecLinkInput,
    ImplementationTargetUpdateInput,
    ObservedWorkspaceStateSubmission,
    StartCodeInvestigationInput,
    TargetOverlapAcknowledgementInput,
    SpecCodeEvidenceRebaseApplyInput,
    SpecCodeEvidenceRebasePreviewInput,
)
from okto_pulse.core.ports.code_investigation import (
    CodeInvestigationRequestCreateResult,
    CodeInvestigationRequestReplay,
)
from okto_pulse.core.ports.code_traceability import (
    CodeEvidenceSupersessionCommitResult,
    CodeTraceabilityProjectionQuery,
    ImplementationTargetResolutionCommitResult,
    TargetOverlapQuery,
)
from okto_pulse.core.services.code_overlap import CodeOverlapService
from okto_pulse.core.services.code_evidence import CodeEvidenceService
from okto_pulse.core.services.code_evidence_rebase import (
    SpecCodeEvidenceRebaseService,
)
from okto_pulse.core.services.code_investigation import (
    CodeInvestigationService,
    HmacCodeInvestigationChallengePolicy,
    required_capabilities_for_subject,
    selector_scope_digest_for_card_targets,
    selector_scope_digest_for_subject,
)
from okto_pulse.core.services.implementation_targets import (
    ImplementationTargetService,
)
from okto_pulse.core.services.main import RefinementService
from okto_pulse.core.services.code_traceability_waivers import (
    CodeTraceabilityWaiverService,
)
from okto_pulse.core.services.code_traceability_observability import (
    METRIC_CODE_INVESTIGATION_RECEIPT_AGE_SECONDS,
    METRIC_CODE_INVESTIGATION_RECEIPT_REJECTED_TOTAL,
    METRIC_CODE_INVESTIGATION_RECEIPT_TOTAL,
    get_code_traceability_metric_samples,
    reset_code_traceability_observability_for_tests,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 9, 18, 0, tzinfo=UTC)
H1 = "1" * 64
H2 = "2" * 64


class MutableClock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class StableIds:
    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    def __call__(self, prefix: str) -> str:
        current = self.counts.get(prefix, 0) + 1
        self.counts[prefix] = current
        return f"{prefix}-{current}"


class FakeInvestigationStore:
    def __init__(self) -> None:
        self.requests: dict[str, object] = {}
        self.receipts: dict[str, object] = {}
        self.heads: dict[tuple[str, str], object] = {}
        self.revocations: dict[str, object] = {}
        self.fail_next_commit = False
        self.return_mismatched_create_once = False
        self._request_admission_lock = asyncio.Lock()

    async def get_request(self, *, board_id: str, request_id: str):
        item = self.requests.get(request_id)
        return item if item is not None and item.board_id == board_id else None

    async def resolve_request_replay(
        self,
        *,
        board_id: str,
        issued_to_actor_id: str,
        subject_type,
        subject_id: str,
        subject_version: int,
        idempotency_key: str,
    ):
        for item in self.requests.values():
            if (
                item.board_id == board_id
                and item.issued_to_actor_id == issued_to_actor_id
                and item.subject_type is subject_type
                and item.subject_id == subject_id
                and item.subject_version == subject_version
                and item.idempotency_key == idempotency_key
            ):
                consumed = next(
                    (
                        receipt.id
                        for receipt in self.receipts.values()
                        if receipt.request_id == item.id
                    ),
                    None,
                )
                return CodeInvestigationRequestReplay(
                    request=item,
                    consumed_receipt_id=consumed,
                )
        return None

    async def count_open_requests(
        self,
        *,
        board_id: str,
        issued_to_actor_id: str,
        at: datetime,
    ) -> int:
        return sum(
            item.board_id == board_id
            and item.issued_to_actor_id == issued_to_actor_id
            and item.status is CodeInvestigationRequestStatus.OPEN
            and item.expires_at > at
            for item in self.requests.values()
        )

    async def create_request_if_below_open_limit(
        self,
        *,
        request,
        at: datetime,
        max_open_requests: int,
    ):
        async with self._request_admission_lock:
            replay = await self.resolve_request_replay(
                board_id=request.board_id,
                issued_to_actor_id=request.issued_to_actor_id,
                subject_type=request.subject_type,
                subject_id=request.subject_id,
                subject_version=request.subject_version,
                idempotency_key=request.idempotency_key,
            )
            if replay is not None:
                return CodeInvestigationRequestCreateResult(
                    request=replay.request,
                    consumed_receipt_id=replay.consumed_receipt_id,
                    replayed=True,
                )
            if (
                await self.count_open_requests(
                    board_id=request.board_id,
                    issued_to_actor_id=request.issued_to_actor_id,
                    at=at,
                )
                >= max_open_requests
            ):
                raise CodeInvestigationSubmissionLimitExceeded(
                    details={"reason": "open_request_limit"}
                )
            self.requests[request.id] = request
            if self.return_mismatched_create_once:
                self.return_mismatched_create_once = False
                return CodeInvestigationRequestCreateResult(
                    request=request,
                    replayed=True,
                )
            return CodeInvestigationRequestCreateResult(request=request)

    async def get_receipt(self, *, board_id: str, receipt_id: str):
        item = self.receipts.get(receipt_id)
        return item if item is not None and item.board_id == board_id else None

    async def resolve_receipt_replay(
        self,
        *,
        board_id: str,
        attestor_actor_id: str,
        request_id: str,
        idempotency_key: str,
    ):
        return next(
            (
                item
                for item in self.receipts.values()
                if item.board_id == board_id
                and item.attestor_actor_id == attestor_actor_id
                and item.request_id == request_id
                and item.idempotency_key == idempotency_key
            ),
            None,
        )

    async def list_receipts(self, query):
        items = tuple(
            item for item in self.receipts.values() if item.board_id == query.board_id
        )[: query.limit]
        return CodeTraceabilityPage(items=items, limit=query.limit)

    async def get_current_head(self, *, board_id: str, source_ref: str):
        return self.heads.get((board_id, source_ref))

    async def get_receipt_revocation(self, *, board_id: str, receipt_id: str):
        item = self.revocations.get(receipt_id)
        return item if item is not None and item.board_id == board_id else None

    async def consume_request_append_receipt_and_advance_head(
        self,
        *,
        request,
        receipt,
        head,
        expected_head_revision: int | None,
    ):
        if self.fail_next_commit:
            self.fail_next_commit = False
            raise CodeInvestigationHeadConflict()
        current = self.heads.get((head.board_id, head.source_ref))
        actual_revision = None if current is None else current.revision
        if actual_revision != expected_head_revision:
            raise CodeInvestigationHeadConflict()
        stored_request = self.requests[request.id]
        if stored_request.status is not CodeInvestigationRequestStatus.OPEN:
            raise CodeInvestigationHeadConflict()
        self.requests[request.id] = request
        self.receipts[receipt.id] = receipt
        self.heads[(head.board_id, head.source_ref)] = head
        return CodeInvestigationReceiptCommitResult(
            request=request,
            receipt=receipt,
            head=head,
        )

    async def append_receipt_revocation(self, revocation):
        self.revocations[revocation.receipt_id] = revocation
        return revocation


class FakeTraceabilityStore:
    def __init__(self, investigations: FakeInvestigationStore) -> None:
        self.investigations = investigations
        self.evidence: dict[str, object] = {}
        self.targets: dict[str, ImplementationTarget] = {}
        self.resolutions: dict[str, object] = {}
        self.executions: dict[str, object] = {}
        self.links: dict[str, object] = {}
        self.target_spec_links: dict[str, object] = {}
        self.target_evidence_links: dict[str, object] = {}
        self.dispositions: dict[tuple[str, str], object] = {}
        self.overlap_acknowledgements: list[object] = []
        self.overlap_projection: tuple[TargetOverlap, ...] = ()
        self.waivers: dict[str, object] = {}
        self.fail_next_target_create_cas = False
        self.fail_next_rebase_cas = False
        self.rebase_apply_count = 0
        self.spec_rebase_state: dict[str, tuple[str, int, int]] = {}

    async def get_evidence(self, *, board_id: str, evidence_id: str):
        item = self.evidence.get(evidence_id)
        return item if item is not None and item.board_id == board_id else None

    async def list_evidence(self, query):
        items = tuple(
            item for item in self.evidence.values() if item.board_id == query.board_id
        )[: query.limit]
        return CodeTraceabilityPage(items=items, limit=query.limit)

    async def resolve_evidence_replay(
        self,
        *,
        board_id: str,
        submitted_by: str,
        parent_id: str,
        idempotency_key: str,
    ):
        return next(
            (
                item
                for item in self.evidence.values()
                if item.board_id == board_id
                and item.submitted_by == submitted_by
                and item.parent_id == parent_id
                and item.idempotency_key == idempotency_key
            ),
            None,
        )

    def _require_head(self, *, board_id: str, source_ref: str, revision: int) -> None:
        head = self.investigations.heads.get((board_id, source_ref))
        if head is None or head.revision != revision:
            raise CodeInvestigationHeadConflict()

    async def create_evidence(self, *, evidence, expected_head_revision: int):
        self._require_head(
            board_id=evidence.board_id,
            source_ref=evidence.source_ref,
            revision=expected_head_revision,
        )
        self.evidence[evidence.id] = evidence
        return evidence

    async def supersede_evidence(
        self,
        *,
        predecessor,
        replacement,
        expected_head_revision: int,
    ):
        self._require_head(
            board_id=replacement.board_id,
            source_ref=replacement.source_ref,
            revision=expected_head_revision,
        )
        self.evidence[predecessor.id] = predecessor
        self.evidence[replacement.id] = replacement
        return CodeEvidenceSupersessionCommitResult(
            predecessor=predecessor,
            replacement=replacement,
        )

    async def revoke_evidence(self, *, evidence, expected_lifecycle_status):
        current = self.evidence.get(evidence.id)
        if current is None or current.lifecycle_status is not expected_lifecycle_status:
            raise CodeEvidenceLinkInvalid(details={"reason": "evidence_cas_conflict"})
        self.evidence[evidence.id] = evidence
        return evidence

    async def list_spec_links(
        self,
        *,
        board_id: str,
        spec_id: str,
        evidence_id: str | None = None,
    ):
        return tuple(
            item
            for item in self.links.values()
            if item.board_id == board_id
            and item.spec_id == spec_id
            and (evidence_id is None or item.evidence_id == evidence_id)
        )

    async def list_spec_dispositions(
        self,
        *,
        board_id: str,
        spec_id: str,
        active_only: bool = True,
    ):
        return tuple(
            item
            for item in self.dispositions.values()
            if item.board_id == board_id
            and item.spec_id == spec_id
            and (not active_only or item.active)
        )

    async def apply_spec_evidence_rebase(
        self,
        *,
        board_id: str,
        spec_id: str,
        current_refinement_snapshot_id: str,
        current_refinement_version: int,
        target_refinement_snapshot_id: str,
        target_refinement_version: int,
        stale_link_ids: tuple[str, ...],
        invalid_disposition_ids: tuple[str, ...],
        cleared_by: str,
        cleared_at: datetime,
        expected_spec_version: int,
        next_spec_version: int,
    ):
        current_state = self.spec_rebase_state.get(spec_id)
        if self.fail_next_rebase_cas:
            self.fail_next_rebase_cas = False
            raise CodeEvidenceLinkInvalid(details={"reason": "spec_version_conflict"})
        if current_state != (
            current_refinement_snapshot_id,
            current_refinement_version,
            expected_spec_version,
        ):
            raise CodeEvidenceLinkInvalid(details={"reason": "spec_version_conflict"})
        if any(link_id not in self.links for link_id in stale_link_ids) or any(
            disposition_id not in {item.id for item in self.dispositions.values()}
            for disposition_id in invalid_disposition_ids
        ):
            raise CodeEvidenceLinkInvalid(details={"reason": "rebase_set_conflict"})

        for link_id in stale_link_ids:
            del self.links[link_id]
        invalid_ids = set(invalid_disposition_ids)
        self.dispositions = {
            key: (
                replace(
                    item,
                    active=False,
                    spec_version=next_spec_version,
                    cleared_by=cleared_by,
                    cleared_at=cleared_at,
                )
                if item.id in invalid_ids
                else item
            )
            for key, item in self.dispositions.items()
        }
        self.spec_rebase_state[spec_id] = (
            target_refinement_snapshot_id,
            target_refinement_version,
            next_spec_version,
        )
        self.rebase_apply_count += 1
        return next_spec_version

    async def list_targets(self, query):
        items = tuple(
            item
            for item in self.targets.values()
            if item.board_id == query.board_id
            and (query.card_id is None or item.card_id == query.card_id)
            and (
                query.lifecycle_status is None
                or item.lifecycle_status is query.lifecycle_status
            )
        )[: query.limit]
        return CodeTraceabilityPage(items=items, limit=query.limit)

    async def get_target(self, *, board_id: str, target_id: str):
        item = self.targets.get(target_id)
        return item if item is not None and item.board_id == board_id else None

    async def get_resolution(self, *, board_id: str, resolution_id: str):
        item = self.resolutions.get(resolution_id)
        return item if item is not None and item.board_id == board_id else None

    async def create_target(
        self,
        *,
        target,
        expected_head_revision: int,
        expected_spec_version: int,
    ):
        if self.fail_next_target_create_cas:
            self.fail_next_target_create_cas = False
            raise CodeInvestigationHeadConflict()
        self._require_head(
            board_id=target.board_id,
            source_ref=target.source_ref,
            revision=expected_head_revision,
        )
        if target.source_spec_version != expected_spec_version:
            raise ImplementationTargetResolutionOutdated()
        self.targets[target.id] = target
        return target

    async def update_target(self, *, target, expected_revision: int):
        current = self.targets.get(target.id)
        if current is None or current.revision != expected_revision:
            raise ImplementationTargetResolutionOutdated()
        self.targets[target.id] = target
        return target

    async def list_target_spec_links(self, *, board_id: str, target_id: str):
        del board_id
        return tuple(
            item
            for item in self.target_spec_links.values()
            if item.target_id == target_id
        )

    async def list_target_evidence_links(self, *, board_id: str, target_id: str):
        del board_id
        return tuple(
            item
            for item in self.target_evidence_links.values()
            if item.target_id == target_id
        )

    async def replace_target_links(
        self,
        *,
        board_id: str,
        target_id: str,
        spec_links,
        evidence_links,
        expected_target_revision: int,
    ):
        target = self.targets.get(target_id)
        if (
            target is None
            or target.board_id != board_id
            or target.revision != expected_target_revision
        ):
            raise ImplementationTargetResolutionOutdated()
        self.target_spec_links = {
            item.id: item
            for item in self.target_spec_links.values()
            if item.target_id != target_id
        }
        self.target_spec_links.update({item.id: item for item in spec_links})
        self.target_evidence_links = {
            item.id: item
            for item in self.target_evidence_links.values()
            if item.target_id != target_id
        }
        self.target_evidence_links.update({item.id: item for item in evidence_links})
        return spec_links, evidence_links

    async def resolve_resolution_replay(self, **criteria):
        return next(
            (
                item
                for item in self.resolutions.values()
                if all(getattr(item, key) == value for key, value in criteria.items())
            ),
            None,
        )

    async def append_resolution(
        self,
        *,
        target,
        resolution,
        expected_target_revision: int,
        expected_head_revision: int,
    ):
        current = self.targets[target.id]
        if current.revision != expected_target_revision:
            raise ImplementationTargetResolutionOutdated()
        self._require_head(
            board_id=target.board_id,
            source_ref=target.source_ref,
            revision=expected_head_revision,
        )
        self.targets[target.id] = target
        self.resolutions[resolution.id] = resolution
        return ImplementationTargetResolutionCommitResult(
            target=target,
            resolution=resolution,
        )

    async def resolve_execution_replay(self, **criteria):
        return next(
            (
                item
                for item in self.executions.values()
                if all(getattr(item, key) == value for key, value in criteria.items())
            ),
            None,
        )

    async def append_execution_record(self, *, record, expected_head_revision: int):
        self._require_head(
            board_id=record.board_id,
            source_ref=record.source_ref,
            revision=expected_head_revision,
        )
        self.executions[record.id] = record
        return record

    async def overlap_report(self, query):
        return tuple(
            item
            for item in self.overlap_projection
            if item.board_id == query.board_id
            and (
                query.include_informational
                or item.severity is not TargetOverlapSeverity.INFORMATIONAL
            )
        )

    async def list_overlap_acknowledgements(self, *, board_id: str, card_id: str):
        target_ids = {
            item.id
            for item in self.targets.values()
            if item.board_id == board_id and item.card_id == card_id
        }
        return tuple(
            item
            for item in self.overlap_acknowledgements
            if item.board_id == board_id
            and ({item.target_a_id, item.target_b_id} & target_ids)
        )

    async def add_overlap_acknowledgement(self, acknowledgement):
        target_a = self.targets.get(acknowledgement.target_a_id)
        target_b = self.targets.get(acknowledgement.target_b_id)
        if (
            target_a is None
            or target_b is None
            or target_a.current_resolution_id != acknowledgement.resolution_a_id
            or target_b.current_resolution_id != acknowledgement.resolution_b_id
        ):
            raise CodeInvestigationHeadConflict()
        self.overlap_acknowledgements.append(acknowledgement)
        return acknowledgement

    async def get_active_waiver(
        self,
        *,
        board_id: str,
        entity_type,
        entity_id: str,
        scope,
    ):
        return next(
            (
                item
                for item in self.waivers.values()
                if item.board_id == board_id
                and item.entity_type is entity_type
                and item.entity_id == entity_id
                and item.scope is scope
                and item.active
            ),
            None,
        )

    async def get_waiver(self, *, board_id: str, waiver_id: str):
        item = self.waivers.get(waiver_id)
        return item if item is not None and item.board_id == board_id else None

    async def create_waiver(self, waiver):
        self.waivers[waiver.id] = waiver
        return waiver

    async def clear_waiver(self, waiver):
        current = self.waivers.get(waiver.id)
        if current is None or not current.active:
            raise CodeInvestigationHeadConflict()
        self.waivers[waiver.id] = waiver
        return waiver


def challenge_policy() -> HmacCodeInvestigationChallengePolicy:
    return HmacCodeInvestigationChallengePolicy(
        keys={"challenge-v1": b"k" * 32},
        active_key_id="challenge-v1",
    )


def receipt_submission(
    *,
    board_id: str,
    request_id: str,
    token: str,
    observed_at: datetime,
    capabilities: tuple[CodeInvestigationCapability, ...],
    manifest_digest: str = H1,
    idempotency_key: str = "receipt-idem",
    tooling_version: str = "1",
    declared_revision: str | None = "revision-1",
    workspace_state_id: str = "workspace-1",
    declared_dirty: bool = False,
    reproducibility_claim: WorkspaceReproducibilityClaim | None = None,
) -> CodeInvestigationReceiptSubmission:
    resolved_reproducibility_claim = reproducibility_claim or (
        WorkspaceReproducibilityClaim.WORKTREE_SNAPSHOT
        if declared_dirty
        else WorkspaceReproducibilityClaim.COMMITTED
    )
    return CodeInvestigationReceiptSubmission(
        board_id=board_id,
        request_id=request_id,
        challenge_token=SecretStr(token),
        outcome=CodeInvestigationOutcome.ACCESSIBLE,
        capabilities=capabilities,
        source_identity_digest=H1,
        declared_revision=declared_revision,
        workspace_state=ObservedWorkspaceStateSubmission(
            workspace_state_id=workspace_state_id,
            declared_dirty=declared_dirty,
            reproducibility_claim=resolved_reproducibility_claim,
            fingerprint_algorithm="agent-manifest-v1",
            manifest_digest=manifest_digest,
            manifest_entry_count=7,
        ),
        omission_manifest=(),
        tooling=CodeInvestigationToolingInput(
            tool_id="codex",
            tool_version=tooling_version,
            method_id="code-check/v1",
        ),
        observed_at=observed_at,
        idempotency_key=idempotency_key,
    )


async def accepted_receipt(
    *,
    service: CodeInvestigationService,
    store: FakeInvestigationStore,
    clock: MutableClock,
    actor_id: str,
    subject_type: CodeTraceabilitySubjectType,
    subject_id: str,
    subject_version: int,
    source_ref: str | None,
    selector_scope_digest: str,
    capabilities: tuple[CodeInvestigationCapability, ...],
    request_key: str,
    receipt_key: str,
    manifest_digest: str = H1,
    declared_revision: str | None = "revision-1",
    workspace_state_id: str = "workspace-1",
    declared_dirty: bool = False,
    reproducibility_claim: WorkspaceReproducibilityClaim | None = None,
):
    started = await service.start(
        StartCodeInvestigationInput(
            board_id="board-1",
            subject_type=subject_type,
            subject_id=subject_id,
            expected_subject_version=subject_version,
            source_ref=source_ref,
            idempotency_key=request_key,
        ),
        actor_id=actor_id,
        actor_kind="agent",
        selector_scope_digest=selector_scope_digest,
        required_capabilities=capabilities,
        store=store,
    )
    assert started.challenge_token is not None
    return await service.submit_receipt(
        receipt_submission(
            board_id="board-1",
            request_id=started.request.id,
            token=started.challenge_token,
            observed_at=clock.value,
            capabilities=capabilities,
            manifest_digest=manifest_digest,
            idempotency_key=receipt_key,
            declared_revision=declared_revision,
            workspace_state_id=workspace_state_id,
            declared_dirty=declared_dirty,
            reproducibility_claim=reproducibility_claim,
        ),
        actor_id=actor_id,
        actor_kind="agent",
        freshness_seconds=1800,
        store=store,
    )


@pytest.mark.asyncio
async def test_challenge_is_deterministic_single_use_and_replay_safe() -> None:
    clock = MutableClock()
    store = FakeInvestigationStore()
    service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=StableIds(),
    )
    command = StartCodeInvestigationInput(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-1",
        expected_subject_version=3,
        source_ref=None,
        idempotency_key="start-idem",
    )
    scope = selector_scope_digest_for_subject(
        board_id="board-1",
        subject_type=command.subject_type,
        subject_id=command.subject_id,
        subject_version=command.expected_subject_version,
    )
    capabilities = (
        CodeInvestigationCapability.SOURCE_IDENTITY,
        CodeInvestigationCapability.REVISION_IDENTITY,
        CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
    )

    with pytest.raises(CodeInvestigationActorKindRequired):
        await service.start(
            command,
            actor_id="human-1",
            actor_kind="user",
            selector_scope_digest=scope,
            required_capabilities=capabilities,
            store=store,
        )
    assert store.requests == {}

    first = await service.start(
        command,
        actor_id="agent-1",
        actor_kind="agent",
        selector_scope_digest=scope,
        required_capabilities=capabilities,
        store=store,
    )
    replay = await service.start(
        command,
        actor_id="agent-1",
        actor_kind="agent",
        selector_scope_digest=scope,
        required_capabilities=capabilities,
        store=store,
    )
    assert first.challenge_token == replay.challenge_token
    assert replay.replayed is True
    assert first.challenge_token not in repr(first.request)

    submission = receipt_submission(
        board_id="board-1",
        request_id=first.request.id,
        token=first.challenge_token or "",
        observed_at=clock.value,
        capabilities=capabilities,
    )
    committed = await service.submit_receipt(
        submission,
        actor_id="agent-1",
        actor_kind="agent",
        freshness_seconds=1800,
        store=store,
    )
    receipt_replay = await service.submit_receipt(
        submission,
        actor_id="agent-1",
        actor_kind="agent",
        freshness_seconds=1800,
        store=store,
    )
    assert receipt_replay.receipt.id == committed.receipt.id
    assert receipt_replay.replayed is True
    receipt_count = len(store.receipts)
    with pytest.raises(CodeInvestigationActorKindRequired):
        await service.submit_receipt(
            submission,
            actor_id="agent-1",
            actor_kind="user",
            freshness_seconds=1800,
            store=store,
        )
    assert len(store.receipts) == receipt_count
    consumed_start = await service.start(
        command,
        actor_id="agent-1",
        actor_kind="agent",
        selector_scope_digest=scope,
        required_capabilities=capabilities,
        store=store,
    )
    assert consumed_start.challenge_token is None
    assert consumed_start.consumed_receipt_id == committed.receipt.id

    with pytest.raises(CodeInvestigationIdempotencyConflict):
        await service.submit_receipt(
            receipt_submission(
                board_id="board-1",
                request_id=first.request.id,
                token=first.challenge_token or "",
                observed_at=clock.value,
                capabilities=capabilities,
                tooling_version="different",
            ),
            actor_id="agent-1",
            actor_kind="agent",
            freshness_seconds=1800,
            store=store,
        )


@pytest.mark.asyncio
async def test_partial_receipt_records_missing_capability_but_cannot_authorize_it() -> (
    None
):
    clock = MutableClock()
    store = FakeInvestigationStore()
    service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=StableIds(),
    )
    required = required_capabilities_for_subject(CodeTraceabilitySubjectType.REFINEMENT)
    partial_capabilities = tuple(
        item
        for item in required
        if item is not CodeInvestigationCapability.SYMLINK_CONTAINMENT
    )
    scope = selector_scope_digest_for_subject(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
    )
    started = await service.start(
        StartCodeInvestigationInput(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
            subject_id="refinement-1",
            expected_subject_version=3,
            idempotency_key="partial-request",
        ),
        actor_id="agent-1",
        actor_kind="agent",
        selector_scope_digest=scope,
        required_capabilities=required,
        store=store,
    )
    base = receipt_submission(
        board_id="board-1",
        request_id=started.request.id,
        token=started.challenge_token or "",
        observed_at=clock.value,
        capabilities=partial_capabilities,
        idempotency_key="partial-receipt",
    ).model_dump(mode="python")
    base.update(
        outcome=CodeInvestigationOutcome.PARTIAL,
        omission_manifest=(
            CodeInvestigationOmissionInput(
                reason_code=CodeInvestigationOmissionReason.PATH_POLICY,
                affected_scope_digest=H2,
                count=1,
            ),
        ),
    )
    submitted = await service.submit_receipt(
        CodeInvestigationReceiptSubmission.model_validate(base),
        actor_id="agent-1",
        actor_kind="agent",
        freshness_seconds=1800,
        store=store,
    )

    assert submitted.receipt.outcome is CodeInvestigationOutcome.PARTIAL
    assert submitted.receipt.omission_count == 1
    assert store.heads[("board-1", submitted.receipt.source_ref)].state.value == (
        "current"
    )
    with pytest.raises(CodeInvestigationCapabilityMissing) as missing:
        await service.require_current_receipt(
            board_id="board-1",
            receipt_id=submitted.receipt.id,
            store=store,
            actor_id="agent-1",
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
            subject_id="refinement-1",
            subject_version=3,
            required_capabilities=(CodeInvestigationCapability.SYMLINK_CONTAINMENT,),
            minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
            require_committed_state=False,
        )
    assert missing.value.details == {"capabilities": ("symlink_containment",)}

    unavailable = dict(base)
    unavailable.update(
        outcome=CodeInvestigationOutcome.UNAVAILABLE,
        source_identity_digest=H1,
    )
    with pytest.raises(ValidationError) as incoherent:
        CodeInvestigationReceiptSubmission.model_validate(unavailable)
    assert "code_investigation_unavailable_claims_incoherent" in str(incoherent.value)


@pytest.mark.asyncio
async def test_new_source_ref_is_server_allocated_and_start_race_replays_winner() -> (
    None
):
    clock = MutableClock()
    store = FakeInvestigationStore()
    store.return_mismatched_create_once = True
    service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=StableIds(),
    )
    scope = selector_scope_digest_for_subject(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-1",
        subject_version=3,
    )
    capabilities = (
        CodeInvestigationCapability.SOURCE_IDENTITY,
        CodeInvestigationCapability.REVISION_IDENTITY,
        CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
    )
    with pytest.raises(CodeInvestigationSourceScopeMismatch):
        await service.start(
            StartCodeInvestigationInput(
                board_id="board-1",
                subject_type=CodeTraceabilitySubjectType.SPEC,
                subject_id="spec-1",
                expected_subject_version=3,
                source_ref="client-selected-source",
                idempotency_key="unauthorized-source",
            ),
            actor_id="agent-1",
            actor_kind="agent",
            selector_scope_digest=scope,
            required_capabilities=capabilities,
            store=store,
        )
    assert store.requests == {}

    raced = await service.start(
        StartCodeInvestigationInput(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.SPEC,
            subject_id="spec-1",
            expected_subject_version=3,
            source_ref=None,
            idempotency_key="race-safe",
        ),
        actor_id="agent-1",
        actor_kind="agent",
        selector_scope_digest=scope,
        required_capabilities=capabilities,
        store=store,
    )
    assert raced.replayed is True
    assert raced.request.source_ref == "source-1"
    assert raced.challenge_token is not None
    assert len(store.requests) == 1


@pytest.mark.asyncio
async def test_open_request_cap_is_admitted_atomically_under_concurrency() -> None:
    clock = MutableClock()
    store = FakeInvestigationStore()
    service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=StableIds(),
    )
    capabilities = (
        CodeInvestigationCapability.SOURCE_IDENTITY,
        CodeInvestigationCapability.REVISION_IDENTITY,
        CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
    )

    async def start(index: int):
        command = StartCodeInvestigationInput(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.SPEC,
            subject_id="spec-1",
            expected_subject_version=3,
            source_ref=None,
            idempotency_key=f"start-cap-{index}",
        )
        return await service.start(
            command,
            actor_id="agent-1",
            actor_kind="agent",
            selector_scope_digest=selector_scope_digest_for_subject(
                board_id="board-1",
                subject_type=command.subject_type,
                subject_id=command.subject_id,
                subject_version=command.expected_subject_version,
            ),
            required_capabilities=capabilities,
            store=store,
        )

    for index in range(7):
        await start(index)

    results = await asyncio.gather(start(7), start(8), return_exceptions=True)

    assert sum(not isinstance(item, BaseException) for item in results) == 1
    rejected = next(item for item in results if isinstance(item, BaseException))
    assert isinstance(rejected, CodeInvestigationSubmissionLimitExceeded)
    assert rejected.details == {"reason": "open_request_limit"}
    assert (
        await store.count_open_requests(
            board_id="board-1",
            issued_to_actor_id="agent-1",
            at=clock.value,
        )
        == 8
    )


@pytest.mark.asyncio
async def test_receipt_head_cas_failure_has_zero_mutation() -> None:
    clock = MutableClock()
    store = FakeInvestigationStore()
    service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=StableIds(),
    )
    scope = selector_scope_digest_for_subject(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=2,
    )
    caps = (
        CodeInvestigationCapability.SOURCE_IDENTITY,
        CodeInvestigationCapability.REVISION_IDENTITY,
        CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
    )
    started = await service.start(
        StartCodeInvestigationInput(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
            subject_id="refinement-1",
            expected_subject_version=2,
            source_ref=None,
            idempotency_key="start-1",
        ),
        actor_id="agent-1",
        actor_kind="agent",
        selector_scope_digest=scope,
        required_capabilities=caps,
        store=store,
    )
    store.fail_next_commit = True
    with pytest.raises(CodeInvestigationHeadConflict):
        await service.submit_receipt(
            receipt_submission(
                board_id="board-1",
                request_id=started.request.id,
                token=started.challenge_token or "",
                observed_at=clock.value,
                capabilities=caps,
            ),
            actor_id="agent-1",
            actor_kind="agent",
            freshness_seconds=1800,
            store=store,
        )
    assert (
        store.requests[started.request.id].status is CodeInvestigationRequestStatus.OPEN
    )
    assert store.receipts == {}
    assert store.heads == {}


@pytest.mark.asyncio
async def test_receipt_trust_requires_other_actor_and_conflict_fails_closed() -> None:
    clock = MutableClock()
    store = FakeInvestigationStore()
    service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=StableIds(),
    )
    scope = selector_scope_digest_for_subject(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-1",
        subject_version=5,
    )
    caps = (
        CodeInvestigationCapability.SOURCE_IDENTITY,
        CodeInvestigationCapability.REVISION_IDENTITY,
        CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
    )
    first = await accepted_receipt(
        service=service,
        store=store,
        clock=clock,
        actor_id="agent-1",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-1",
        subject_version=5,
        source_ref=None,
        selector_scope_digest=scope,
        capabilities=caps,
        request_key="request-1",
        receipt_key="receipt-1",
    )
    clock.value += timedelta(seconds=1)
    second = await accepted_receipt(
        service=service,
        store=store,
        clock=clock,
        actor_id="agent-2",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-1",
        subject_version=5,
        source_ref="source-1",
        selector_scope_digest=scope,
        capabilities=caps,
        request_key="request-2",
        receipt_key="receipt-2",
    )
    assert first.receipt.trust_level is CodeInvestigationTrustLevel.SINGLE_ATTESTATION
    assert second.receipt.trust_level is CodeInvestigationTrustLevel.CORROBORATED

    clock.value += timedelta(seconds=1)
    conflicted = await accepted_receipt(
        service=service,
        store=store,
        clock=clock,
        actor_id="agent-3",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-1",
        subject_version=5,
        source_ref="source-1",
        selector_scope_digest=scope,
        capabilities=caps,
        request_key="request-3",
        receipt_key="receipt-3",
        manifest_digest=H2,
    )
    assert conflicted.receipt.trust_level is CodeInvestigationTrustLevel.CONFLICTED
    conflicted_head = await store.get_current_head(
        board_id="board-1",
        source_ref="source-1",
    )
    assert conflicted_head.current_receipt_id == second.receipt.id
    with pytest.raises(Exception) as captured:
        await service.require_current_receipt(
            board_id="board-1",
            receipt_id=second.receipt.id,
            store=store,
        )
    assert (
        getattr(captured.value, "code", None) == "code_investigation_receipt_conflicted"
    )

    clock.value += timedelta(seconds=1)
    self_repeat = await accepted_receipt(
        service=service,
        store=store,
        clock=clock,
        actor_id="agent-3",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-1",
        subject_version=5,
        source_ref="source-1",
        selector_scope_digest=scope,
        capabilities=caps,
        request_key="request-4",
        receipt_key="receipt-4",
        manifest_digest=H2,
    )
    assert self_repeat.receipt.trust_level is CodeInvestigationTrustLevel.CONFLICTED
    self_repeat_head = await store.get_current_head(
        board_id="board-1",
        source_ref="source-1",
    )
    assert self_repeat_head.current_receipt_id == second.receipt.id

    clock.value += timedelta(seconds=1)
    independently_resolved = await accepted_receipt(
        service=service,
        store=store,
        clock=clock,
        actor_id="agent-4",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-1",
        subject_version=5,
        source_ref="source-1",
        selector_scope_digest=scope,
        capabilities=caps,
        request_key="request-5",
        receipt_key="receipt-5",
        manifest_digest=H2,
    )
    assert independently_resolved.receipt.trust_level is (
        CodeInvestigationTrustLevel.CORROBORATED
    )
    resolved_head = await store.get_current_head(
        board_id="board-1",
        source_ref="source-1",
    )
    assert resolved_head.current_receipt_id == independently_resolved.receipt.id


@pytest.mark.asyncio
async def test_first_receipt_in_new_scope_recovers_global_head_from_conflict() -> None:
    clock = MutableClock()
    store = FakeInvestigationStore()
    service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=StableIds(),
    )
    scope_a = selector_scope_digest_for_subject(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-a",
        subject_version=5,
    )
    scope_b = selector_scope_digest_for_subject(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-b",
        subject_version=3,
    )
    assert scope_a != scope_b
    capabilities = (
        CodeInvestigationCapability.SOURCE_IDENTITY,
        CodeInvestigationCapability.REVISION_IDENTITY,
        CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
    )

    current_a = await accepted_receipt(
        service=service,
        store=store,
        clock=clock,
        actor_id="agent-1",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-a",
        subject_version=5,
        source_ref=None,
        selector_scope_digest=scope_a,
        capabilities=capabilities,
        request_key="scope-a-current-request",
        receipt_key="scope-a-current-receipt",
    )
    clock.value += timedelta(seconds=1)
    conflicted_a = await accepted_receipt(
        service=service,
        store=store,
        clock=clock,
        actor_id="agent-2",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-a",
        subject_version=5,
        source_ref=current_a.receipt.source_ref,
        selector_scope_digest=scope_a,
        capabilities=capabilities,
        request_key="scope-a-conflict-request",
        receipt_key="scope-a-conflict-receipt",
        manifest_digest=H2,
    )
    conflicted_head = await store.get_current_head(
        board_id="board-1",
        source_ref=current_a.receipt.source_ref,
    )
    assert conflicted_a.receipt.trust_level is CodeInvestigationTrustLevel.CONFLICTED
    assert conflicted_head.state is CodeInvestigationHeadState.CONFLICTED
    assert conflicted_head.current_receipt_id == current_a.receipt.id
    assert conflicted_head.latest_receipt_id == conflicted_a.receipt.id

    clock.value += timedelta(seconds=1)
    first_b = await accepted_receipt(
        service=service,
        store=store,
        clock=clock,
        actor_id="agent-2",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-b",
        subject_version=3,
        source_ref=current_a.receipt.source_ref,
        selector_scope_digest=scope_b,
        capabilities=capabilities,
        request_key="scope-b-first-request",
        receipt_key="scope-b-first-receipt",
        manifest_digest=H2,
    )
    recovered_head = await store.get_current_head(
        board_id="board-1",
        source_ref=current_a.receipt.source_ref,
    )

    assert first_b.receipt.predecessor_receipt_id == conflicted_a.receipt.id
    assert first_b.receipt.trust_level is (
        CodeInvestigationTrustLevel.SINGLE_ATTESTATION
    )
    assert recovered_head.state is CodeInvestigationHeadState.CURRENT
    assert recovered_head.current_receipt_id == first_b.receipt.id
    assert recovered_head.latest_receipt_id == first_b.receipt.id
    assert recovered_head.generation == first_b.receipt.generation == 3
    accepted_b = await service.require_current_receipt(
        board_id="board-1",
        receipt_id=first_b.receipt.id,
        store=store,
    )
    assert accepted_b.receipt == first_b.receipt
    assert accepted_b.currentness.value == "current"


def evidence_submission(
    *,
    receipt_id: str,
    idempotency_key: str,
) -> CodeEvidenceSubmission:
    excerpt = "return value\n"
    return CodeEvidenceSubmission(
        board_id="board-1",
        investigation_receipt_id=receipt_id,
        parent_type=CodeTraceabilitySubjectType.REFINEMENT,
        parent_id="refinement-1",
        evidence_type=CodeEvidenceType.BEHAVIOR,
        claim="The service preserves the validated behavior.",
        selector=CodeEvidenceSelectorInput(
            kind=CodeEvidenceSelectorKind.SYMBOL,
            relative_path="src/service.py",
            language="python",
            symbol_kind="method",
            qualified_symbol="Service.run",
            symbol_signature="Service.run(value)",
            line_start=10,
            line_end=11,
        ),
        excerpt=excerpt,
        excerpt_sha256=hashlib.sha256(excerpt.encode()).hexdigest(),
        declared_file_blob_sha256=H1,
        declared_source_content_sha256=H1,
        idempotency_key=idempotency_key,
    )


def test_evidence_omission_reason_is_not_agent_controlled() -> None:
    assert "excerpt_omitted_reason" not in CodeEvidenceSubmission.model_fields
    payload = evidence_submission(
        receipt_id="receipt-1",
        idempotency_key="evidence-extra-field",
    ).model_dump(mode="python")
    payload["excerpt_omitted_reason"] = "agent_supplied_reason"

    with pytest.raises(ValidationError):
        CodeEvidenceSubmission.model_validate(payload)


@pytest.mark.asyncio
async def test_evidence_is_server_owned_immutable_and_superseded_atomically() -> None:
    clock = MutableClock()
    ids = StableIds()
    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    investigation_service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=ids,
    )
    evidence_service = CodeEvidenceService(clock=clock, id_factory=ids)
    scope = selector_scope_digest_for_subject(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
    )
    caps = tuple(
        sorted(
            {
                *required_capabilities_for_subject(
                    CodeTraceabilitySubjectType.REFINEMENT
                ),
                CodeInvestigationCapability.SYMBOL_RESOLUTION,
            },
            key=lambda item: item.value,
        )
    )
    first_receipt = await accepted_receipt(
        service=investigation_service,
        store=investigations,
        clock=clock,
        actor_id="agent-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
        source_ref=None,
        selector_scope_digest=scope,
        capabilities=caps,
        request_key="request-1",
        receipt_key="receipt-1",
    )
    with pytest.raises(CodeEvidenceSubmissionFailed) as metadata_excerpt:
        await evidence_service.submit(
            evidence_submission(
                receipt_id=first_receipt.receipt.id,
                idempotency_key="metadata-excerpt-rejected",
            ),
            actor_id="agent-1",
            actor_kind="agent",
            current_parent_version=3,
            minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
            require_committed_state=False,
            investigation_service=investigation_service,
            investigation_store=investigations,
            store=traceability,
            receipt_content="metadata_only",
        )
    assert metadata_excerpt.value.details == {
        "reason": "excerpt_forbidden_by_receipt_content",
        "receipt_content": "metadata_only",
    }
    assert traceability.evidence == {}

    submitted = await evidence_service.submit(
        evidence_submission(
            receipt_id=first_receipt.receipt.id,
            idempotency_key="evidence-1",
        ),
        actor_id="agent-1",
        actor_kind="agent",
        current_parent_version=3,
        minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
        require_committed_state=False,
        investigation_service=investigation_service,
        investigation_store=investigations,
        store=traceability,
    )
    original = submitted.evidence
    assert original.excerpt == "return value\n"
    assert original.source_ref == first_receipt.receipt.source_ref
    assert original.submitted_by == "agent-1"
    assert original.parent_version == 3
    assert original.attestation_basis is (
        CodeEvidenceAttestationBasis.AUTHENTICATED_AGENT_RECEIPT
    )
    assert original.attestation_state is CodeEvidenceAttestationState.AGENT_ATTESTED

    omitted_payload = evidence_submission(
        receipt_id=first_receipt.receipt.id,
        idempotency_key="evidence-without-excerpt",
    ).model_dump(mode="python")
    omitted_payload.update(excerpt=None, excerpt_sha256=None)
    omitted = await evidence_service.submit(
        CodeEvidenceSubmission.model_validate(omitted_payload),
        actor_id="agent-1",
        actor_kind="agent",
        current_parent_version=3,
        minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
        require_committed_state=False,
        investigation_service=investigation_service,
        investigation_store=investigations,
        store=traceability,
        receipt_content="metadata_only",
    )
    assert omitted.evidence.excerpt is None
    assert omitted.evidence.excerpt_omitted_reason == "not_submitted"

    clock.value += timedelta(seconds=1)
    second_receipt = await accepted_receipt(
        service=investigation_service,
        store=investigations,
        clock=clock,
        actor_id="agent-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
        source_ref="source-1",
        selector_scope_digest=scope,
        capabilities=caps,
        request_key="request-2",
        receipt_key="receipt-2",
    )
    replacement_payload = evidence_submission(
        receipt_id=second_receipt.receipt.id,
        idempotency_key="evidence-2",
    ).model_dump(mode="python")
    replacement_payload.update(
        supersedes_evidence_id=original.id,
        supersession_reason="The agent observed a newer implementation state.",
    )
    command = CodeEvidenceSupersessionSubmission.model_validate(replacement_payload)
    evidence_count = len(traceability.evidence)
    with pytest.raises(CodeEvidenceSubmissionFailed) as metadata_supersession:
        await evidence_service.supersede(
            command,
            actor_id="agent-1",
            actor_kind="agent",
            current_parent_version=3,
            minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
            require_committed_state=False,
            investigation_service=investigation_service,
            investigation_store=investigations,
            store=traceability,
            receipt_content="metadata_only",
        )
    assert metadata_supersession.value.details["reason"] == (
        "excerpt_forbidden_by_receipt_content"
    )
    assert len(traceability.evidence) == evidence_count
    assert traceability.evidence[original.id].lifecycle_status is (
        CodeTraceabilityLifecycleStatus.ACTIVE
    )

    superseded = await evidence_service.supersede(
        command,
        actor_id="agent-1",
        actor_kind="agent",
        current_parent_version=3,
        minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
        require_committed_state=False,
        investigation_service=investigation_service,
        investigation_store=investigations,
        store=traceability,
    )
    assert original.lifecycle_status is CodeTraceabilityLifecycleStatus.ACTIVE
    assert superseded.predecessor.lifecycle_status is (
        CodeTraceabilityLifecycleStatus.SUPERSEDED
    )
    assert superseded.replacement.supersedes_evidence_id == original.id

    replay = await evidence_service.supersede(
        command,
        actor_id="agent-1",
        actor_kind="agent",
        current_parent_version=3,
        minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
        require_committed_state=False,
        investigation_service=investigation_service,
        investigation_store=investigations,
        store=traceability,
    )
    assert replay.replayed is True
    assert replay.replacement.id == superseded.replacement.id

    revoked = await evidence_service.revoke(
        CodeEvidenceRevokeInput(
            board_id="board-1",
            evidence_id=superseded.replacement.id,
            reason="The attested material was withdrawn by an operator.",
        ),
        store=traceability,
    )
    assert revoked.evidence.lifecycle_status is (
        CodeTraceabilityLifecycleStatus.REVOKED
    )
    revoked_replay = await evidence_service.revoke(
        CodeEvidenceRevokeInput(
            board_id="board-1",
            evidence_id=superseded.replacement.id,
            reason="The attested material was withdrawn by an operator.",
        ),
        store=traceability,
    )
    assert revoked_replay.replayed is True

    evidence_count = len(traceability.evidence)
    with pytest.raises(CodeInvestigationActorKindRequired):
        await evidence_service.submit(
            evidence_submission(
                receipt_id=first_receipt.receipt.id,
                idempotency_key="evidence-1",
            ),
            actor_id="agent-1",
            actor_kind="user",
            current_parent_version=3,
            minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
            require_committed_state=False,
            investigation_service=investigation_service,
            investigation_store=investigations,
            store=traceability,
        )
    assert len(traceability.evidence) == evidence_count

    # A retry of the original Evidence submission is resolved from its ledger
    # identity before checking the now-missing parent or superseded source head.
    uow = FakeUnitOfWork(investigations, traceability)
    uow.services.refinements = FakeMissingRefinementService()
    evidence_replay = await SubmitCodeEvidenceUseCase(
        investigation_service,
        evidence_service,
    ).execute(
        evidence_submission(
            receipt_id=first_receipt.receipt.id,
            idempotency_key="evidence-1",
        ),
        actor=ActorContext(
            "agent-1",
            "mcp",
            actor_kind="agent",
            board_id="board-1",
            permissions=("code_traceability.evidence.submit",),
        ),
        uow=uow,  # type: ignore[arg-type]
    )
    assert evidence_replay.replayed is True
    assert evidence_replay.evidence.id == original.id
    assert uow.commit_count == 0
    assert uow.published_events == []


@pytest.mark.asyncio
async def test_target_creation_requires_current_card_preflight_and_exact_scope() -> (
    None
):
    clock = MutableClock()
    ids = StableIds()
    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    investigation_service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=ids,
    )
    target_service = ImplementationTargetService(clock=clock, id_factory=ids)
    command = ImplementationTargetCreateInput(
        board_id="board-1",
        card_id="card-1",
        source_ref="source-1",
        selector_kind=ImplementationTargetSelectorKind.SYMBOL,
        relative_path_hint="src/service.py",
        language="python",
        symbol_kind="method",
        qualified_symbol="Service.run",
        symbol_signature="Service.run(value)",
        role=ImplementationTargetRole.MODIFY,
        intent="Preserve the externally attested behavior.",
        required=True,
        expected_spec_version=7,
    )
    with pytest.raises(ImplementationTargetInvalid):
        await target_service.create(
            command,
            created_by="user-1",
            card_status="planning",
            current_card_version=4,
            current_spec_version=7,
            minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
            require_committed_state=False,
            investigation_service=investigation_service,
            investigation_store=investigations,
            store=traceability,
        )

    scope = selector_scope_digest_for_card_targets(
        board_id="board-1",
        card_id="card-1",
        card_version=4,
        targets=(),
    )
    caps = required_capabilities_for_subject(CodeTraceabilitySubjectType.CARD)
    await accepted_receipt(
        service=investigation_service,
        store=investigations,
        clock=clock,
        actor_id="agent-1",
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        subject_version=4,
        source_ref=None,
        selector_scope_digest=scope,
        capabilities=caps,
        request_key="request-1",
        receipt_key="receipt-1",
    )
    traceability.fail_next_target_create_cas = True
    with pytest.raises(CodeInvestigationHeadConflict):
        await target_service.create(
            command,
            created_by="user-1",
            card_status="planning",
            current_card_version=4,
            current_spec_version=7,
            minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
            require_committed_state=False,
            investigation_service=investigation_service,
            investigation_store=investigations,
            store=traceability,
        )
    assert traceability.targets == {}
    created = await target_service.create(
        command,
        created_by="user-1",
        card_status="planning",
        current_card_version=4,
        current_spec_version=7,
        minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
        require_committed_state=False,
        investigation_service=investigation_service,
        investigation_store=investigations,
        store=traceability,
    )
    assert created.target.created_by == "user-1"
    assert created.target.revision == 1


def sample_target(
    *,
    target_id: str = "target-1",
    card_id: str = "card-1",
    revision: int = 2,
    current_resolution_id: str | None = None,
    role: ImplementationTargetRole = ImplementationTargetRole.MODIFY,
) -> ImplementationTarget:
    return ImplementationTarget(
        id=target_id,
        board_id="board-1",
        card_id=card_id,
        source_ref="source-1",
        selector_kind=ImplementationTargetSelectorKind.SYMBOL,
        relative_path_hint="src/service.py",
        language="python",
        symbol_kind="method",
        qualified_symbol="Service.run",
        symbol_signature="Service.run(value)",
        role=role,
        intent="Preserve behavior.",
        required=True,
        source_spec_version=7,
        baseline_evidence_id=None,
        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
        revision=revision,
        current_resolution_id=current_resolution_id,
        last_change_reason_sha256=None,
        created_by="user-1",
        created_at=NOW,
        updated_at=NOW,
    )


def sample_resolution(
    *,
    resolution_id: str,
    target_id: str,
    target_revision: int = 2,
    relative_path: str = "src/service.py",
    qualified_symbol: str = "Service.run",
) -> ImplementationTargetResolution:
    observed_workspace = ObservedWorkspaceStateRef(
        declared_revision="revision-1",
        workspace_state_id="workspace-1",
        declared_dirty=False,
        observed_at=NOW,
        reproducibility_claim=WorkspaceReproducibilityClaim.COMMITTED,
        fingerprint_algorithm="agent-manifest-v1",
        manifest_digest=H1,
        manifest_entry_count=7,
    )
    return ImplementationTargetResolution(
        id=resolution_id,
        board_id="board-1",
        target_id=target_id,
        investigation_receipt_id=f"receipt-{resolution_id}",
        source_ref="source-1",
        receipt_generation=1,
        subject_version=4,
        target_revision=target_revision,
        workspace_state=observed_workspace,
        state=ImplementationTargetResolutionState.RESOLVED,
        resolved_relative_path=relative_path,
        resolved_language="python",
        resolved_symbol_kind="method",
        resolved_qualified_symbol=qualified_symbol,
        resolved_symbol_signature=f"{qualified_symbol}(value)",
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
        agent_observed_at=NOW,
        received_at=NOW,
        payload_sha256=H2,
        idempotency_key=f"idem-{resolution_id}",
    )


def valid_resolution_submission(
    *,
    receipt_id: str = "receipt-1",
    idempotency_key: str = "resolution-1",
) -> ImplementationTargetResolutionSubmission:
    return ImplementationTargetResolutionSubmission(
        board_id="board-1",
        card_id="card-1",
        target_id="target-1",
        investigation_receipt_id=receipt_id,
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
        confidence=0.99,
        candidates=(),
        tooling=CodeInvestigationToolingInput(
            tool_id="codex",
            tool_version="1",
            method_id="symbol-resolution/v1",
        ),
        agent_observed_at=NOW,
        idempotency_key=idempotency_key,
    )


def test_resolution_thresholds_are_closed_and_versioned() -> None:
    base = valid_resolution_submission().model_dump(mode="python")
    with pytest.raises(ValidationError):
        ImplementationTargetResolutionSubmission.model_validate(
            {**base, "confidence": 0.94}
        )

    stale = {
        **base,
        "state": ImplementationTargetResolutionState.STALE,
        "confidence": 0.90,
        "reason_code": "signature_changed",
    }
    assert (
        ImplementationTargetResolutionSubmission.model_validate(stale).state
        is ImplementationTargetResolutionState.STALE
    )
    with pytest.raises(ValidationError):
        ImplementationTargetResolutionSubmission.model_validate(
            {**stale, "confidence": 0.95}
        )

    candidate_base = {
        "relative_path": "src/candidate_a.py",
        "qualified_symbol": "Service.run",
        "symbol_signature": "Service.run(value)",
        "symbol_fingerprint": H1,
        "confidence": 0.90,
        "reason_code": "candidate",
    }
    ambiguous = {
        **base,
        "state": ImplementationTargetResolutionState.AMBIGUOUS,
        "resolved_relative_path": None,
        "resolved_language": None,
        "resolved_symbol_kind": None,
        "resolved_qualified_symbol": None,
        "resolved_symbol_signature": None,
        "resolved_line_start": None,
        "resolved_line_end": None,
        "symbol_fingerprint": None,
        "declared_file_blob_sha256": None,
        "confidence": None,
        "reason_code": "multiple_close_candidates",
        "candidates": (
            candidate_base,
            {
                **candidate_base,
                "relative_path": "src/candidate_b.py",
                "confidence": 0.87,
            },
        ),
    }
    assert (
        ImplementationTargetResolutionSubmission.model_validate(ambiguous).state
        is ImplementationTargetResolutionState.AMBIGUOUS
    )
    with pytest.raises(ValidationError):
        ImplementationTargetResolutionSubmission.model_validate(
            {
                **ambiguous,
                "candidates": (
                    candidate_base,
                    {
                        **candidate_base,
                        "relative_path": "src/candidate_b.py",
                        "confidence": 0.80,
                    },
                ),
            }
        )


@pytest.mark.asyncio
async def test_resolution_and_execution_replay_ignore_later_live_target_state() -> None:
    clock = MutableClock()
    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    traceability.targets["target-1"] = sample_target()
    investigation_service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=StableIds(),
    )
    target_service = ImplementationTargetService(
        clock=clock,
        id_factory=StableIds(),
    )
    scope = selector_scope_digest_for_card_targets(
        board_id="board-1",
        card_id="card-1",
        card_version=4,
        targets=(("target-1", 2),),
    )
    accepted = await accepted_receipt(
        service=investigation_service,
        store=investigations,
        clock=clock,
        actor_id="agent-1",
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        subject_version=4,
        source_ref=None,
        selector_scope_digest=scope,
        capabilities=required_capabilities_for_subject(
            CodeTraceabilitySubjectType.CARD
        ),
        request_key="request-resolution",
        receipt_key="receipt-resolution",
    )
    resolution_submission = valid_resolution_submission(
        receipt_id=accepted.receipt.id,
    )
    resolved = await target_service.submit_resolution(
        resolution_submission,
        actor_id="agent-1",
        actor_kind="agent",
        current_card_version=4,
        minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
        require_committed_state=False,
        investigation_service=investigation_service,
        investigation_store=investigations,
        store=traceability,
    )
    execution_submission = ImplementationTargetExecutionSubmission(
        board_id="board-1",
        card_id="card-1",
        target_id="target-1",
        result_investigation_receipt_id=accepted.receipt.id,
        disposition=ImplementationTargetExecutionDisposition.TOUCHED,
        actual_relative_path="src/service.py",
        actual_qualified_symbol="Service.run",
        justification="The attested target was updated.",
        idempotency_key="execution-1",
    )
    executed = await target_service.submit_execution(
        execution_submission,
        actor_id="agent-1",
        actor_kind="agent",
        current_card_version=4,
        minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
        require_committed_state=False,
        investigation_service=investigation_service,
        investigation_store=investigations,
        store=traceability,
    )

    execution_count = len(traceability.executions)
    with pytest.raises(CodeInvestigationActorKindRequired):
        await target_service.submit_execution(
            execution_submission,
            actor_id="agent-1",
            actor_kind="user",
            current_card_version=4,
            minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
            require_committed_state=False,
            investigation_service=investigation_service,
            investigation_store=investigations,
            store=traceability,
        )
    assert len(traceability.executions) == execution_count

    # Idempotent replay is an exact ledger lookup.  It remains available after
    # live state has moved and does not re-authorize against today's target/head.
    del traceability.targets["target-1"]
    investigations.heads.clear()
    resolution_replay = await target_service.submit_resolution(
        resolution_submission,
        actor_id="agent-1",
        actor_kind="agent",
        current_card_version=999,
        minimum_trust=CodeInvestigationTrustLevel.CORROBORATED,
        require_committed_state=True,
        investigation_service=investigation_service,
        investigation_store=investigations,
        store=traceability,
    )
    execution_replay = await target_service.submit_execution(
        execution_submission,
        actor_id="agent-1",
        actor_kind="agent",
        current_card_version=999,
        minimum_trust=CodeInvestigationTrustLevel.CORROBORATED,
        require_committed_state=True,
        investigation_service=investigation_service,
        investigation_store=investigations,
        store=traceability,
    )
    assert resolution_replay.replayed is True
    assert resolution_replay.resolution == resolved.resolution
    assert execution_replay.replayed is True
    assert execution_replay.record == executed.record


@pytest.mark.asyncio
async def test_target_update_replaces_link_sets_without_silent_drops() -> None:
    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    traceability.targets["target-1"] = sample_target()
    traceability.evidence["evidence-1"] = SimpleNamespace(
        id="evidence-1",
        board_id="board-1",
        source_ref="source-1",
        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
    )
    service = ImplementationTargetService(clock=MutableClock(), id_factory=StableIds())

    linked = await service.update(
        ImplementationTargetUpdateInput(
            board_id="board-1",
            card_id="card-1",
            target_id="target-1",
            expected_revision=2,
            change_reason="Bind the target to its normative and evidence lineage.",
            spec_links=(
                ImplementationTargetSpecLinkInput(
                    entity_type=SpecEntityType.TECHNICAL_REQUIREMENT,
                    entity_id="tr-1",
                ),
            ),
            evidence_links=(
                ImplementationTargetEvidenceLinkInput(
                    evidence_id="evidence-1",
                    relation_type=(
                        ImplementationTargetEvidenceRelationType.DERIVED_FROM
                    ),
                ),
            ),
        ),
        card_status="in_progress",
        spec_id="spec-1",
        updated_by="agent-1",
        store=traceability,
    )

    assert linked.target.revision == 3
    assert [(item.spec_id, item.entity_id) for item in linked.spec_links] == [
        ("spec-1", "tr-1")
    ]
    assert [item.evidence_id for item in linked.evidence_links] == ["evidence-1"]

    replaced = await service.update(
        ImplementationTargetUpdateInput(
            board_id="board-1",
            card_id="card-1",
            target_id="target-1",
            expected_revision=3,
            change_reason="Remove the normative link while retaining evidence lineage.",
            spec_links=(),
        ),
        card_status="in_progress",
        spec_id="spec-1",
        updated_by="agent-1",
        store=traceability,
    )
    assert replaced.target.revision == 4
    assert replaced.spec_links == ()
    assert [item.evidence_id for item in replaced.evidence_links] == ["evidence-1"]
    assert traceability.target_spec_links == {}

    before = traceability.targets["target-1"]
    with pytest.raises(ImplementationTargetInvalid) as captured:
        await service.update(
            ImplementationTargetUpdateInput(
                board_id="board-1",
                card_id="card-1",
                target_id="target-1",
                expected_revision=4,
                change_reason="Attempt to bind evidence from another source.",
                evidence_links=(
                    ImplementationTargetEvidenceLinkInput(
                        evidence_id="missing-evidence",
                        relation_type=(
                            ImplementationTargetEvidenceRelationType.VALIDATES
                        ),
                    ),
                ),
            ),
            card_status="in_progress",
            spec_id="spec-1",
            updated_by="agent-1",
            store=traceability,
        )
    assert captured.value.details["reason"] == "active_same_source_evidence_required"
    assert traceability.targets["target-1"] == before


@pytest.mark.asyncio
async def test_target_update_uses_cas_and_resolution_is_agent_only() -> None:
    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    traceability.targets["target-1"] = sample_target()
    service = ImplementationTargetService(clock=MutableClock(), id_factory=StableIds())

    with pytest.raises(ImplementationTargetResolutionOutdated):
        await service.update(
            ImplementationTargetUpdateInput(
                board_id="board-1",
                card_id="card-1",
                target_id="target-1",
                expected_revision=1,
                change_reason="The selector intent changed.",
                intent="New intent.",
            ),
            card_status="in_progress",
            store=traceability,
        )
    updated = await service.update(
        ImplementationTargetUpdateInput(
            board_id="board-1",
            card_id="card-1",
            target_id="target-1",
            expected_revision=2,
            change_reason="The selector intent changed.",
            intent="New intent.",
        ),
        card_status="in_progress",
        store=traceability,
    )
    assert updated.target.revision == 3
    assert updated.target.current_resolution_id is None
    assert updated.target.selector_kind is ImplementationTargetSelectorKind.SYMBOL
    assert updated.target.relative_path_hint == "src/service.py"
    assert updated.target.language == "python"
    assert updated.target.symbol_kind == "method"
    assert updated.target.qualified_symbol == "Service.run"
    assert updated.target.symbol_signature == "Service.run(value)"
    assert updated.target.role is ImplementationTargetRole.MODIFY
    assert updated.target.required is True

    with pytest.raises(ImplementationTargetInvalid):
        await service.update(
            ImplementationTargetUpdateInput(
                board_id="board-1",
                card_id="card-1",
                target_id="target-1",
                expected_revision=3,
                change_reason="Attempted late change.",
                intent="Forbidden.",
            ),
            card_status="validation",
            store=traceability,
        )

    resolution = ImplementationTargetResolutionSubmission(
        board_id="board-1",
        card_id="card-1",
        target_id="target-1",
        investigation_receipt_id="receipt-1",
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
        confidence=0.99,
        candidates=(),
        tooling=CodeInvestigationToolingInput(
            tool_id="codex",
            tool_version="1",
            method_id="symbol-resolution/v1",
        ),
        agent_observed_at=NOW,
        idempotency_key="resolution-1",
    )
    with pytest.raises(CodeInvestigationActorKindRequired):
        await service.submit_resolution(
            resolution,
            actor_id="user-1",
            actor_kind="user",
            current_card_version=4,
            minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
            require_committed_state=False,
            investigation_service=CodeInvestigationService(
                challenge_policy=challenge_policy()
            ),
            investigation_store=investigations,
            store=traceability,
        )


class FakeBoardService:
    async def get_board(self, board_id: str):
        if board_id != "board-1":
            return None
        return SimpleNamespace(
            id="board-1",
            settings={"code_traceability": {"mode": "advisory"}},
        )


class FakeRefinementService:
    def __init__(self) -> None:
        self.snapshots = {
            3: SimpleNamespace(
                id="snapshot-3",
                refinement_id="refinement-1",
                version=3,
                code_evidence_manifest=[],
            ),
            4: SimpleNamespace(
                id="snapshot-4",
                refinement_id="refinement-1",
                version=4,
                code_evidence_manifest=[],
            ),
        }

    async def get_refinement(self, refinement_id: str):
        if refinement_id != "refinement-1":
            return None
        return SimpleNamespace(
            id="refinement-1",
            board_id="board-1",
            version=3,
        )

    async def get_snapshot(self, refinement_id: str, version: int):
        item = self.snapshots.get(version)
        if item is None or item.refinement_id != refinement_id:
            return None
        return item


class FakeMissingRefinementService:
    async def get_refinement(self, refinement_id: str):
        return None


class FakeCardService:
    async def get_card(self, card_id: str):
        if card_id not in {"card-1", "card-2"}:
            return None
        return SimpleNamespace(
            id=card_id,
            board_id="board-1",
            version=4,
            spec_id="spec-1",
            status="planning",
        )


class FakeSpecService:
    async def get_spec(self, spec_id: str):
        if spec_id != "spec-1":
            return None
        return SimpleNamespace(
            id="spec-1",
            board_id="board-1",
            version=7,
            status="draft",
            refinement_id="refinement-1",
            source_refinement_snapshot_id="snapshot-3",
            source_refinement_version=3,
        )


class FakeUnitOfWork:
    def __init__(self, investigations, traceability) -> None:
        self.published_events: list[object] = []
        self.fail_event_publish = False
        self.services = SimpleNamespace(
            boards=FakeBoardService(),
            refinements=FakeRefinementService(),
            specs=FakeSpecService(),
            cards=FakeCardService(),
            code_investigations=investigations,
            code_traceability=traceability,
            code_traceability_read=traceability,
            publish_domain_event=self._publish_domain_event,
        )
        self.commit_count = 0

    async def _publish_domain_event(self, event: object) -> None:
        if self.fail_event_publish:
            raise RuntimeError("event staging failed")
        self.published_events.append(event)

    async def commit(self) -> None:
        self.commit_count += 1


@pytest.mark.asyncio
async def test_target_create_permission_distinguishes_agent_suggestion_from_human() -> (
    None
):
    class RecordingTargetService:
        def __init__(self) -> None:
            self.calls = 0

        async def create(self, *_args, **_kwargs):
            self.calls += 1
            return SimpleNamespace(
                target=sample_target(target_id=f"created-{self.calls}"),
                replayed=False,
            )

    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    target_service = RecordingTargetService()
    use_case = CreateImplementationTargetUseCase(
        CodeInvestigationService(challenge_policy=challenge_policy()),
        target_service,  # type: ignore[arg-type]
    )
    command = ImplementationTargetCreateInput(
        board_id="board-1",
        card_id="card-1",
        source_ref="source-1",
        selector_kind=ImplementationTargetSelectorKind.FILE,
        relative_path_hint="src/service.py",
        role=ImplementationTargetRole.MODIFY,
        intent="Apply the externally investigated change.",
        expected_spec_version=7,
    )

    suggest_uow = FakeUnitOfWork(investigations, traceability)
    suggested = await use_case.execute(
        command,
        actor=ActorContext(
            "agent-1",
            "mcp",
            actor_kind="agent",
            board_id="board-1",
            permissions=("code_traceability.target.suggest",),
        ),
        uow=suggest_uow,  # type: ignore[arg-type]
    )
    assert suggested.target.id == "created-1"
    assert suggest_uow.commit_count == 1

    with pytest.raises(PermissionDeniedError):
        await use_case.execute(
            command,
            actor=ActorContext(
                "agent-1",
                "mcp",
                actor_kind="agent",
                board_id="board-1",
                permissions=("code_traceability.target.create",),
            ),
            uow=FakeUnitOfWork(investigations, traceability),  # type: ignore[arg-type]
        )

    create_uow = FakeUnitOfWork(investigations, traceability)
    created = await use_case.execute(
        command,
        actor=ActorContext(
            "user-1",
            "rest",
            actor_kind="user",
            board_id="board-1",
            permissions=("code_traceability.target.create",),
        ),
        uow=create_uow,  # type: ignore[arg-type]
    )
    assert created.target.id == "created-2"
    assert create_uow.commit_count == 1
    assert target_service.calls == 2


@pytest.mark.asyncio
async def test_aggregate_projection_requires_every_leaf_read_permission() -> None:
    class ProjectionSpy:
        def __init__(self) -> None:
            self.calls = 0

        async def load_context(self, *_args, **_kwargs):
            self.calls += 1
            raise AssertionError("projection data must not be loaded")

    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    uow = FakeUnitOfWork(investigations, traceability)
    projection = ProjectionSpy()
    actor = ActorContext(
        "user-1",
        "rest",
        actor_kind="user",
        board_id="board-1",
        permissions=(
            "code_traceability.investigation.read",
            "code_traceability.evidence.read",
            "code_traceability.target.read",
        ),
    )

    with pytest.raises(PermissionDeniedError):
        await GetCodeTraceabilityProjectionUseCase(
            projection,  # type: ignore[arg-type]
        ).execute(
            CodeTraceabilityProjectionQuery(
                board_id="board-1",
                subject_type=CodeTraceabilitySubjectType.CARD,
                subject_id="card-1",
                subject_version=4,
                profile=CodeTraceabilityProjectionProfile.FULL,
                context_scope=CodeTraceabilityContextScope.GATE,
            ),
            actor=actor,
            uow=uow,  # type: ignore[arg-type]
        )
    assert projection.calls == 0


@pytest.mark.asyncio
async def test_spec_evidence_rebase_preview_apply_cas_and_events_are_atomic() -> None:
    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    replacement = SimpleNamespace(
        id="evidence-4",
        board_id="board-1",
        parent_id="refinement-1",
        parent_version=4,
        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
        content_sha256=H2,
        supersedes_evidence_id="evidence-3",
    )
    traceability.evidence[replacement.id] = replacement
    link = CodeEvidenceSpecLink(
        id="link-stale",
        board_id="board-1",
        spec_id="spec-1",
        evidence_id="evidence-3",
        entity_type=SpecEntityType.TECHNICAL_REQUIREMENT,
        entity_id="tr-1",
        relation_type=CodeEvidenceSpecRelationType.SUPPORTS,
        rationale="Evidence inherited from the frozen Refinement snapshot.",
        evidence_content_sha256=H1,
        source_refinement_version=3,
        spec_version=7,
        created_by="user-1",
        created_at=NOW,
    )
    disposition = CodeEvidenceDisposition(
        id="disposition-stale",
        board_id="board-1",
        spec_id="spec-1",
        evidence_id="evidence-3",
        disposition=CodeEvidenceDispositionKind.NOT_RELEVANT,
        justification="The previous snapshot evidence was not selected.",
        spec_version=7,
        active=True,
        created_by="user-1",
        created_at=NOW,
        cleared_by=None,
        cleared_at=None,
    )
    traceability.links[link.id] = link
    traceability.dispositions[(disposition.spec_id, disposition.evidence_id)] = (
        disposition
    )
    traceability.spec_rebase_state["spec-1"] = ("snapshot-3", 3, 7)
    uow = FakeUnitOfWork(investigations, traceability)
    uow.services.refinements.snapshots[3].code_evidence_manifest = [
        {
            "evidence_id": "evidence-3",
            "content_sha256": H1,
            "lifecycle_status": "active",
        }
    ]
    uow.services.refinements.snapshots[4].code_evidence_manifest = [
        {
            "evidence_id": "evidence-4",
            "content_sha256": H2,
            "lifecycle_status": "active",
        }
    ]
    actor = ActorContext(
        "user-1",
        "rest",
        actor_kind="user",
        board_id="board-1",
        permissions=("code_traceability.spec_link.rebase",),
    )
    preview_command = SpecCodeEvidenceRebasePreviewInput(
        board_id="board-1",
        spec_id="spec-1",
        target_refinement_version=4,
        expected_spec_version=7,
    )
    preview = await PreviewSpecCodeEvidenceRebaseUseCase(
        SpecCodeEvidenceRebaseService(clock=lambda: NOW)
    ).execute(
        preview_command,
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )
    assert preview.added_evidence_ids == ("evidence-4",)
    assert preview.removed_evidence_ids == ("evidence-3",)
    assert preview.superseded_evidence_pairs == (("evidence-3", "evidence-4"),)
    assert preview.stale_link_ids == (link.id,)
    assert preview.invalid_disposition_ids == (disposition.id,)
    assert uow.commit_count == 0
    assert uow.published_events == []

    with pytest.raises(CodeEvidenceLinkInvalid) as not_newer:
        await PreviewSpecCodeEvidenceRebaseUseCase().execute(
            SpecCodeEvidenceRebasePreviewInput(
                board_id="board-1",
                spec_id="spec-1",
                target_refinement_version=3,
                expected_spec_version=7,
            ),
            actor=actor,
            uow=uow,  # type: ignore[arg-type]
        )
    assert not_newer.value.details == {"reason": "newer_refinement_snapshot_required"}

    uow.services.refinements.snapshots[4].code_evidence_manifest[0][
        "content_sha256"
    ] = H1
    with pytest.raises(CodeEvidenceLinkInvalid) as bad_hash:
        await PreviewSpecCodeEvidenceRebaseUseCase().execute(
            preview_command,
            actor=actor,
            uow=uow,  # type: ignore[arg-type]
        )
    assert bad_hash.value.details == {
        "reason": "target_snapshot_evidence_mismatch",
        "evidence_id": "evidence-4",
    }
    uow.services.refinements.snapshots[4].code_evidence_manifest[0][
        "content_sha256"
    ] = H2

    stale = SpecCodeEvidenceRebaseApplyInput(
        **preview_command.model_dump(mode="python"),
        preview_sha256=H1,
    )
    with pytest.raises(CodeEvidenceLinkInvalid) as stale_preview:
        await ApplySpecCodeEvidenceRebaseUseCase(
            SpecCodeEvidenceRebaseService(clock=lambda: NOW)
        ).execute(stale, actor=actor, uow=uow)  # type: ignore[arg-type]
    assert stale_preview.value.details == {"reason": "rebase_preview_stale"}
    assert traceability.rebase_apply_count == 0
    assert link.id in traceability.links
    assert traceability.dispositions[("spec-1", "evidence-3")].active is True
    assert uow.published_events == []

    apply_command = SpecCodeEvidenceRebaseApplyInput(
        **preview_command.model_dump(mode="python"),
        preview_sha256=preview.preview_sha256,
    )
    traceability.fail_next_rebase_cas = True
    with pytest.raises(CodeEvidenceLinkInvalid) as cas_conflict:
        await ApplySpecCodeEvidenceRebaseUseCase(
            SpecCodeEvidenceRebaseService(clock=lambda: NOW)
        ).execute(apply_command, actor=actor, uow=uow)  # type: ignore[arg-type]
    assert cas_conflict.value.details == {"reason": "spec_version_conflict"}
    assert link.id in traceability.links
    assert traceability.dispositions[("spec-1", "evidence-3")].active is True
    assert traceability.spec_rebase_state["spec-1"] == ("snapshot-3", 3, 7)
    assert uow.published_events == []

    applied = await ApplySpecCodeEvidenceRebaseUseCase(
        SpecCodeEvidenceRebaseService(clock=lambda: NOW)
    ).execute(apply_command, actor=actor, uow=uow)  # type: ignore[arg-type]
    assert applied.spec_version == 8
    assert link.id not in traceability.links
    cleared = traceability.dispositions[("spec-1", "evidence-3")]
    assert cleared.active is False
    assert cleared.cleared_by == "user-1"
    assert traceability.spec_rebase_state["spec-1"] == ("snapshot-4", 4, 8)
    assert traceability.rebase_apply_count == 1
    assert uow.commit_count == 1
    assert [event.event_type for event in uow.published_events] == [
        "code_evidence.unlinked",
        "code_evidence.disposition_changed",
    ]
    assert all(event.actor_type == "user" for event in uow.published_events)


@pytest.mark.asyncio
async def test_start_use_case_returns_safe_projection_and_commits_store() -> None:
    clock = MutableClock()
    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    uow = FakeUnitOfWork(investigations, traceability)
    use_case = StartCodeInvestigationUseCase(
        CodeInvestigationService(
            challenge_policy=challenge_policy(),
            clock=clock,
            id_factory=StableIds(),
        )
    )
    actor = ActorContext(
        "agent-1",
        "mcp",
        actor_kind="agent",
        board_id="board-1",
        permissions=("code_traceability.investigation.start",),
    )
    result = await use_case.execute(
        StartCodeInvestigationInput(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
            subject_id="refinement-1",
            expected_subject_version=3,
            source_ref=None,
            idempotency_key="start-1",
        ),
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )
    assert result.challenge_token is not None
    assert "challenge_token_hash" not in type(result.request).model_fields
    assert uow.commit_count == 1
    assert [event.event_type for event in uow.published_events] == [
        "code_investigation.requested"
    ]
    assert uow.published_events[0].actor_type == "agent"

    replay = await use_case.execute(
        StartCodeInvestigationInput(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
            subject_id="refinement-1",
            expected_subject_version=3,
            source_ref=None,
            idempotency_key="start-1",
        ),
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )
    assert replay.request.id == result.request.id
    assert uow.commit_count == 1
    assert len(uow.published_events) == 1


@pytest.mark.asyncio
async def test_event_staging_failure_prevents_use_case_commit() -> None:
    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    uow = FakeUnitOfWork(investigations, traceability)
    uow.fail_event_publish = True
    use_case = StartCodeInvestigationUseCase(
        CodeInvestigationService(
            challenge_policy=challenge_policy(),
            clock=MutableClock(),
            id_factory=StableIds(),
        )
    )
    actor = ActorContext(
        "agent-1",
        "mcp",
        actor_kind="agent",
        board_id="board-1",
        permissions=("code_traceability.investigation.start",),
    )

    with pytest.raises(RuntimeError, match="event staging failed"):
        await use_case.execute(
            StartCodeInvestigationInput(
                board_id="board-1",
                subject_type=CodeTraceabilitySubjectType.REFINEMENT,
                subject_id="refinement-1",
                expected_subject_version=3,
                idempotency_key="event-staging-failure",
            ),
            actor=actor,
            uow=uow,  # type: ignore[arg-type]
        )

    assert uow.commit_count == 0
    assert uow.published_events == []


@pytest.mark.asyncio
async def test_receipt_use_case_observes_accepted_age_and_bounded_rejection() -> None:
    reset_code_traceability_observability_for_tests()
    clock = MutableClock()
    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    uow = FakeUnitOfWork(investigations, traceability)
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
            idempotency_key="observed-start",
        ),
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )
    command = receipt_submission(
        board_id="board-1",
        request_id=started.request.id,
        token=started.challenge_token or "",
        observed_at=clock.value,
        capabilities=required_capabilities_for_subject(
            CodeTraceabilitySubjectType.REFINEMENT
        ),
        idempotency_key="observed-receipt",
    )
    await SubmitCodeInvestigationReceiptUseCase(service).execute(
        command,
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )

    samples = get_code_traceability_metric_samples()
    assert samples == [
        {
            "metric_name": METRIC_CODE_INVESTIGATION_RECEIPT_TOTAL,
            "value": 1,
            "labels": {
                "outcome": "accessible",
                "trust_level": "single_attestation",
            },
        },
        {
            "metric_name": METRIC_CODE_INVESTIGATION_RECEIPT_AGE_SECONDS,
            "value": 0.0,
            "labels": {"outcome": "accessible"},
        },
    ]

    invalid = command.model_copy(
        update={
            "challenge_token": SecretStr("must-never-leak"),
            "idempotency_key": "rejected-receipt",
        }
    )
    with pytest.raises(CodeTraceabilityContractError):
        await SubmitCodeInvestigationReceiptUseCase(service).execute(
            invalid,
            actor=actor,
            uow=uow,  # type: ignore[arg-type]
        )

    rejected = get_code_traceability_metric_samples()[-1]
    assert rejected == {
        "metric_name": METRIC_CODE_INVESTIGATION_RECEIPT_REJECTED_TOTAL,
        "value": 1,
        "labels": {"reason_code": "code_investigation_challenge_consumed"},
    }
    assert "must-never-leak" not in repr(get_code_traceability_metric_samples())


@pytest.mark.asyncio
async def test_snapshot_does_not_swallow_internal_adapter_attribute_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.ports import relational_application

    class BrokenTraceabilityAdapter:
        def code_traceability(self, _session):
            raise AttributeError("internal adapter defect")

    monkeypatch.setattr(
        relational_application,
        "require_relational_application_adapter",
        lambda: BrokenTraceabilityAdapter(),
    )
    refinement = SimpleNamespace(
        id="refinement-1",
        board_id="board-1",
        version=3,
        qa_items=[],
    )

    with pytest.raises(AttributeError, match="internal adapter defect"):
        await RefinementService(object())._create_snapshot(refinement, "user-1")


@pytest.mark.asyncio
async def test_reopened_refinement_snapshot_inherits_prior_active_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.ports import relational_application
    from okto_pulse.core.services import main as service_main

    records = (
        SimpleNamespace(
            id="evidence-v3-active",
            parent_version=3,
            lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
            content_sha256=H1,
        ),
        SimpleNamespace(
            id="evidence-v4-active",
            parent_version=4,
            lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
            content_sha256=H2,
        ),
        SimpleNamespace(
            id="evidence-v3-superseded",
            parent_version=3,
            lifecycle_status=CodeTraceabilityLifecycleStatus.SUPERSEDED,
            content_sha256=H1,
        ),
        SimpleNamespace(
            id="evidence-v3-revoked",
            parent_version=3,
            lifecycle_status=CodeTraceabilityLifecycleStatus.REVOKED,
            content_sha256=H1,
        ),
        SimpleNamespace(
            id="evidence-v5-future",
            parent_version=5,
            lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
            content_sha256=H1,
        ),
    )
    queries: list[object] = []

    class Store:
        async def list_evidence(self, query):
            queries.append(query)
            return CodeTraceabilityPage(
                items=tuple(
                    item
                    for item in records
                    if item.lifecycle_status is query.lifecycle_status
                ),
                limit=query.limit,
            )

    class Adapter:
        def code_traceability(self, _session):
            return Store()

    added: list[object] = []

    async def add_record(_session, record):
        added.append(record)

    monkeypatch.setattr(
        relational_application,
        "require_relational_application_adapter",
        lambda: Adapter(),
    )
    monkeypatch.setattr(
        service_main,
        "_new_application_record",
        lambda _entity, **values: SimpleNamespace(**values),
    )
    monkeypatch.setattr(service_main, "_application_add", add_record)
    refinement = SimpleNamespace(
        id="refinement-1",
        board_id="board-1",
        version=4,
        title="Refinement",
        description="Description",
        in_scope=[],
        out_of_scope=[],
        analysis="Analysis",
        decisions=[],
        labels=[],
        qa_items=[],
    )

    snapshot = await RefinementService(object())._create_snapshot(
        refinement,
        "user-1",
    )

    assert queries[0].lifecycle_status is CodeTraceabilityLifecycleStatus.ACTIVE
    assert [item["evidence_id"] for item in snapshot.code_evidence_manifest] == [
        "evidence-v3-active",
        "evidence-v4-active",
    ]
    assert added == [snapshot]


@pytest.mark.asyncio
async def test_overlap_read_and_acknowledgement_are_bound_to_current_resolutions() -> (
    None
):
    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    resolution_a = sample_resolution(
        resolution_id="resolution-1",
        target_id="target-1",
    )
    resolution_b = sample_resolution(
        resolution_id="resolution-2",
        target_id="target-2",
    )
    traceability.targets.update(
        {
            "target-1": sample_target(
                target_id="target-1",
                card_id="card-1",
                current_resolution_id=resolution_a.id,
            ),
            "target-2": sample_target(
                target_id="target-2",
                card_id="card-2",
                current_resolution_id=resolution_b.id,
            ),
        }
    )
    traceability.resolutions.update(
        {resolution_a.id: resolution_a, resolution_b.id: resolution_b}
    )
    traceability.overlap_projection = (
        TargetOverlap(
            board_id="board-1",
            target_a_id="target-1",
            target_b_id="target-2",
            resolution_a_id=resolution_a.id,
            resolution_b_id=resolution_b.id,
            severity=TargetOverlapSeverity.HIGH,
            reason_code="same_symbol_mutation",
            relative_path="src/service.py",
            qualified_symbol="Service.run",
        ),
    )
    uow = FakeUnitOfWork(investigations, traceability)
    actor = ActorContext(
        "user-1",
        "mcp",
        actor_kind="user",
        board_id="board-1",
        permissions=(
            "code_traceability.overlap.read",
            "code_traceability.overlap.acknowledge",
        ),
    )

    overlaps = await GetImplementationOverlapsUseCase().execute(
        TargetOverlapQuery(board_id="board-1", card_id="card-1"),
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )
    assert overlaps[0].severity is TargetOverlapSeverity.HIGH

    command = TargetOverlapAcknowledgementInput(
        board_id="board-1",
        card_id="card-1",
        target_a_id="target-1",
        target_b_id="target-2",
        resolution_a_id=resolution_a.id,
        resolution_b_id=resolution_b.id,
        disposition=TargetOverlapDisposition.ACCEPTED_PARALLEL,
        justification="The owners agreed on isolated edits.",
    )
    use_case = AcknowledgeImplementationOverlapUseCase(
        CodeOverlapService(clock=MutableClock(), id_factory=StableIds())
    )
    acknowledged = await use_case.execute(
        command,
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )
    assert acknowledged.overlap.acknowledgement == acknowledged.acknowledgement
    assert uow.commit_count == 1
    assert [event.event_type for event in uow.published_events] == [
        "implementation_overlap.acknowledged"
    ]

    replay = await use_case.execute(
        command,
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )
    assert replay.replayed is True
    assert replay.acknowledgement.id == acknowledged.acknowledgement.id
    assert uow.commit_count == 1
    assert len(uow.published_events) == 1

    with pytest.raises(ImplementationTargetInvalid) as wrong_card:
        await use_case._overlap_service.acknowledge(
            command.model_copy(update={"card_id": "card-3"}),
            created_by=actor.actor_id,
            store=traceability,
        )
    assert wrong_card.value.details["reason"] == "overlap_target_scope_mismatch"

    traceability.targets["target-2"] = replace(
        traceability.targets["target-2"],
        current_resolution_id="resolution-3",
    )
    with pytest.raises(CodeTraceabilityContractError) as stale:
        await use_case.execute(
            command,
            actor=actor,
            uow=uow,  # type: ignore[arg-type]
        )
    assert stale.value.code == "implementation_overlap_ack_stale"


@pytest.mark.asyncio
async def test_waiver_mark_and_clear_are_explicit_monotonic_and_idempotent() -> None:
    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    uow = FakeUnitOfWork(investigations, traceability)
    actor = ActorContext(
        "user-1",
        "mcp",
        actor_kind="user",
        board_id="board-1",
        permissions=(
            "code_traceability.waiver.create",
            "code_traceability.waiver.clear",
        ),
    )
    mark = MarkCodeTraceabilityNotApplicableUseCase(
        CodeTraceabilityWaiverService(
            clock=MutableClock(),
            id_factory=StableIds(),
        )
    )
    command = CodeTraceabilityWaiverInput(
        board_id="board-1",
        entity_type=CodeTraceabilityWaiverEntityType.CARD,
        entity_id="card-1",
        scope=CodeTraceabilityWaiverScope.TARGET_RESOLUTION,
        reason_code=CodeTraceabilityWaiverReason.MANUAL_PROCESS,
        justification="This card is completed by a governed manual process.",
    )
    created = await mark.execute(
        command,
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )
    assert created.waiver.active is True
    assert uow.commit_count == 1
    assert [event.event_type for event in uow.published_events] == [
        "code_traceability.waiver_created"
    ]

    replay = await mark.execute(
        command,
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )
    assert replay.replayed is True
    assert replay.waiver.id == created.waiver.id
    assert uow.commit_count == 1
    assert len(uow.published_events) == 1

    with pytest.raises(CodeTraceabilityContractError) as incompatible:
        await mark.execute(
            CodeTraceabilityWaiverInput(
                board_id="board-1",
                entity_type=CodeTraceabilityWaiverEntityType.CARD,
                entity_id="card-1",
                scope=CodeTraceabilityWaiverScope.EVIDENCE_LINKAGE,
                reason_code=CodeTraceabilityWaiverReason.MANUAL_PROCESS,
                justification="Invalid scope for a Card.",
            ),
            actor=actor,
            uow=uow,  # type: ignore[arg-type]
        )
    assert incompatible.value.code == "code_traceability_waiver_scope_incompatible"

    clear = ClearCodeTraceabilityNotApplicableUseCase(
        CodeTraceabilityWaiverService(clock=MutableClock())
    )
    clear_command = CodeTraceabilityWaiverClearInput(
        board_id="board-1",
        waiver_id=created.waiver.id,
    )
    cleared = await clear.execute(
        clear_command,
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )
    assert cleared.waiver.active is False
    assert cleared.waiver.cleared_by == "user-1"
    assert uow.commit_count == 2
    assert [event.event_type for event in uow.published_events] == [
        "code_traceability.waiver_created",
        "code_traceability.waiver_cleared",
    ]

    clear_replay = await clear.execute(
        clear_command,
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )
    assert clear_replay.replayed is True
    assert clear_replay.waiver == cleared.waiver
    assert uow.commit_count == 2
    assert len(uow.published_events) == 2


def test_application_layer_has_no_repository_reader_imports() -> None:
    root = Path(__file__).resolve().parents[1]
    files = (
        root / "src/okto_pulse/core/services/code_investigation.py",
        root / "src/okto_pulse/core/services/code_evidence.py",
        root / "src/okto_pulse/core/services/implementation_targets.py",
        root / "src/okto_pulse/core/services/code_overlap.py",
        root / "src/okto_pulse/core/services/code_traceability_waivers.py",
        root / "src/okto_pulse/core/application/use_cases/code_traceability.py",
    )
    forbidden_roots = {
        "aiofiles",
        "aiohttp",
        "dulwich",
        "git",
        "gitpython",
        "httpx",
        "os",
        "pathlib",
        "pygit2",
        "requests",
        "socket",
        "subprocess",
        "urllib",
    }
    forbidden_module_fragments = {
        "code_search",
        "filesystem_reader",
        "git_adapter",
        "language_resolver",
        "provider_connector",
        "repository_reader",
        "source_reader",
        "workspace_reader",
    }
    for path in files:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported = set()
        imported_modules = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".", 1)[0] for alias in node.names)
                imported_modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".", 1)[0])
                imported_modules.add(node.module)
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    assert node.func.id != "open", path
                elif isinstance(node.func, ast.Attribute):
                    assert node.func.attr not in {
                        "open",
                        "read_bytes",
                        "read_text",
                    }, path
        assert not (imported & forbidden_roots), path
        assert not any(
            fragment in module
            for module in imported_modules
            for fragment in forbidden_module_fragments
        ), path
