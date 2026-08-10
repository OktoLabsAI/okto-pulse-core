"""Application policy for immutable, agent-attested Code Evidence.

The service validates receipt lineage and structured claims, then stages
records through transaction-bound ports.  It never opens or searches source
files and never attempts to verify an agent's declared content independently.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from uuid import uuid4

from okto_pulse.core.domain.code_traceability import (
    CODE_EVIDENCE_EXCERPT_OMITTED_NOT_SUBMITTED,
    CodeEvidence,
    CodeEvidenceAttestationBasis,
    CodeEvidenceAttestationState,
    CodeEvidenceDisposition,
    CodeEvidenceDispositionKind,
    CodeEvidenceLinkInvalid,
    CodeEvidenceSelectorKind,
    CodeEvidenceSpecLink,
    CodeEvidenceSubmissionFailed,
    CodeEvidenceImmutable,
    CodeInvestigationCapability,
    CodeInvestigationIdempotencyConflict,
    CodeInvestigationPayloadDigestMismatch,
    CodeInvestigationSelectorScopeMismatch,
    CodeInvestigationTrustLevel,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilitySubjectType,
    canonical_code_traceability_sha256,
)
from okto_pulse.core.models.code_traceability import (
    CodeEvidenceDispositionClearInput,
    CodeEvidenceDispositionInput,
    CodeEvidenceSpecLinkInput,
    CodeEvidenceSpecUnlinkInput,
    CodeEvidenceRevokeInput,
    CodeEvidenceSubmission,
    CodeEvidenceSupersessionSubmission,
)
from okto_pulse.core.ports.code_investigation import CodeInvestigationStore
from okto_pulse.core.ports.code_traceability import (
    CodeEvidenceSupersessionCommitResult,
    CodeTraceabilityStore,
    ImplementationTargetQuery,
)
from okto_pulse.core.services.code_investigation import (
    AcceptedCodeInvestigation,
    CodeInvestigationService,
    require_code_attestor,
    selector_scope_digest_for_card_targets,
    selector_scope_digest_for_subject,
)
from okto_pulse.core.services.code_evidence_sanitization import (
    sanitize_code_evidence_submission,
)


Clock = Callable[[], datetime]
IdFactory = Callable[[str], str]


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _default_id_factory(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CodeEvidenceLinkInvalid(details={"field": "clock"})
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class CodeEvidenceMutationResult:
    evidence: CodeEvidence
    head_revision: int
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class CodeEvidenceRevocationResult:
    evidence: CodeEvidence
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class CodeEvidenceLinkMutationResult:
    link: CodeEvidenceSpecLink
    spec_version: int
    cleared_disposition: CodeEvidenceDisposition | None = None


@dataclass(frozen=True, slots=True)
class CodeEvidenceUnlinkMutationResult:
    removed_link: CodeEvidenceSpecLink
    spec_version: int


@dataclass(frozen=True, slots=True)
class CodeEvidenceDispositionMutationResult:
    disposition: CodeEvidenceDisposition
    spec_version: int


class CodeEvidenceService:
    def __init__(
        self,
        *,
        clock: Clock = _default_clock,
        id_factory: IdFactory = _default_id_factory,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    def _now(self) -> datetime:
        return _aware_utc(self._clock())

    @staticmethod
    def _enforce_receipt_content_policy(
        submission: CodeEvidenceSubmission,
        *,
        receipt_content: str,
    ) -> None:
        if receipt_content not in {"metadata_only", "safe_excerpt"}:
            raise CodeEvidenceSubmissionFailed(
                details={"reason": "receipt_content_policy_invalid"}
            )
        if receipt_content == "metadata_only" and submission.excerpt is not None:
            raise CodeEvidenceSubmissionFailed(
                details={
                    "reason": "excerpt_forbidden_by_receipt_content",
                    "receipt_content": "metadata_only",
                }
            )

    @staticmethod
    def _required_capabilities(
        submission: CodeEvidenceSubmission,
    ) -> tuple[CodeInvestigationCapability, ...]:
        capabilities = {
            CodeInvestigationCapability.SOURCE_IDENTITY,
            CodeInvestigationCapability.REVISION_IDENTITY,
            CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
            CodeInvestigationCapability.FILE_READ,
            CodeInvestigationCapability.PATH_CONTAINMENT,
            CodeInvestigationCapability.SYMLINK_CONTAINMENT,
            CodeInvestigationCapability.SECRET_SCAN,
            CodeInvestigationCapability.BINARY_DETECTION,
        }
        if submission.selector.kind is CodeEvidenceSelectorKind.SYMBOL:
            capabilities.add(CodeInvestigationCapability.SYMBOL_RESOLUTION)
        if submission.excerpt is not None:
            capabilities.add(CodeInvestigationCapability.SAFE_EXCERPT)
        return tuple(sorted(capabilities, key=lambda item: item.value))

    @staticmethod
    def _payload_sha256(
        submission: CodeEvidenceSubmission,
        *,
        actor_id: str,
        source_ref: str,
        parent_version: int,
        supersedes_evidence_id: str | None,
    ) -> str:
        return canonical_code_traceability_sha256(
            {
                "operation": (
                    "supersede_code_evidence"
                    if supersedes_evidence_id is not None
                    else "submit_code_evidence"
                ),
                "actor_id": actor_id,
                "board_id": submission.board_id,
                "investigation_receipt_id": (submission.investigation_receipt_id),
                "source_ref": source_ref,
                "parent_type": submission.parent_type,
                "parent_id": submission.parent_id,
                "parent_version": parent_version,
                "supersedes_evidence_id": supersedes_evidence_id,
                "server_excerpt_omitted_reason": (
                    CODE_EVIDENCE_EXCERPT_OMITTED_NOT_SUBMITTED
                    if submission.excerpt is None
                    else None
                ),
                "agent_payload": submission.model_dump(mode="python"),
            }
        )

    async def _accepted_receipt(
        self,
        submission: CodeEvidenceSubmission,
        *,
        actor_id: str,
        current_parent_version: int,
        investigation_service: CodeInvestigationService,
        investigation_store: CodeInvestigationStore,
        store: CodeTraceabilityStore,
        minimum_trust: CodeInvestigationTrustLevel,
        require_committed_state: bool,
    ) -> AcceptedCodeInvestigation:
        accepted = await investigation_service.require_current_receipt(
            board_id=submission.board_id,
            receipt_id=submission.investigation_receipt_id,
            store=investigation_store,
            actor_id=actor_id,
            subject_type=submission.parent_type,
            subject_id=submission.parent_id,
            subject_version=current_parent_version,
            required_capabilities=self._required_capabilities(submission),
            minimum_trust=minimum_trust,
            require_committed_state=require_committed_state,
        )
        receipt = accepted.receipt
        if receipt.subject_type is not CodeTraceabilitySubjectType.CARD:
            expected_scope = selector_scope_digest_for_subject(
                board_id=receipt.board_id,
                subject_type=receipt.subject_type,
                subject_id=receipt.subject_id,
                subject_version=receipt.subject_version,
            )
            if receipt.selector_scope_digest != expected_scope:
                raise CodeInvestigationSelectorScopeMismatch()
        else:
            page = await store.list_targets(
                ImplementationTargetQuery(
                    board_id=receipt.board_id,
                    card_id=receipt.subject_id,
                    lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
                    limit=200,
                )
            )
            if page.next_cursor is not None:
                raise CodeInvestigationSelectorScopeMismatch(
                    details={"reason": "target_scope_limit_exceeded"}
                )
            expected_scope = selector_scope_digest_for_card_targets(
                board_id=receipt.board_id,
                card_id=receipt.subject_id,
                card_version=current_parent_version,
                targets=tuple((item.id, item.revision) for item in page.items),
            )
            if receipt.selector_scope_digest != expected_scope:
                raise CodeInvestigationSelectorScopeMismatch()
        return accepted

    def _materialize(
        self,
        submission: CodeEvidenceSubmission,
        *,
        accepted: AcceptedCodeInvestigation,
        actor_id: str,
        payload_sha256: str,
        supersedes_evidence_id: str | None,
    ) -> CodeEvidence:
        receipt = accepted.receipt
        workspace_state = receipt.workspace_state
        if workspace_state is None:
            raise CodeInvestigationSelectorScopeMismatch(
                details={"field": "workspace_state"}
            )
        selector = submission.selector
        return CodeEvidence(
            id=self._id_factory("code_evidence"),
            board_id=submission.board_id,
            investigation_receipt_id=receipt.id,
            source_ref=receipt.source_ref,
            parent_type=submission.parent_type,
            parent_id=submission.parent_id,
            parent_version=receipt.subject_version,
            evidence_type=submission.evidence_type,
            claim=submission.claim,
            workspace_state=workspace_state,
            selector_kind=selector.kind,
            relative_path=selector.relative_path,
            language=selector.language,
            symbol_kind=selector.symbol_kind,
            qualified_symbol=selector.qualified_symbol,
            symbol_signature=selector.symbol_signature,
            snapshot_line_start=selector.line_start,
            snapshot_line_end=selector.line_end,
            excerpt=submission.excerpt,
            excerpt_sha256=submission.excerpt_sha256,
            declared_file_blob_sha256=(submission.declared_file_blob_sha256),
            declared_source_content_sha256=(submission.declared_source_content_sha256),
            excerpt_omitted_reason=(
                CODE_EVIDENCE_EXCERPT_OMITTED_NOT_SUBMITTED
                if submission.excerpt is None
                else None
            ),
            attestation_state=(
                CodeEvidenceAttestationState.AGENT_ATTESTED_WORKTREE
                if workspace_state.declared_dirty
                else CodeEvidenceAttestationState.AGENT_ATTESTED
            ),
            attestation_basis=(
                CodeEvidenceAttestationBasis.AUTHENTICATED_AGENT_RECEIPT
            ),
            lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
            supersedes_evidence_id=supersedes_evidence_id,
            revocation_reason=None,
            submitted_by=actor_id,
            received_at=self._now(),
            payload_sha256=payload_sha256,
            idempotency_key=submission.idempotency_key,
        )

    async def submit(
        self,
        submission: CodeEvidenceSubmission,
        *,
        actor_id: str,
        actor_kind: str,
        current_parent_version: int,
        minimum_trust: CodeInvestigationTrustLevel,
        require_committed_state: bool,
        investigation_service: CodeInvestigationService,
        investigation_store: CodeInvestigationStore,
        store: CodeTraceabilityStore,
        receipt_content: str = "safe_excerpt",
    ) -> CodeEvidenceMutationResult:
        actor = require_code_attestor(actor_id, actor_kind)
        self._enforce_receipt_content_policy(
            submission,
            receipt_content=receipt_content,
        )
        submission = sanitize_code_evidence_submission(submission)
        receipt = await investigation_store.get_receipt(
            board_id=submission.board_id,
            receipt_id=submission.investigation_receipt_id,
        )
        if receipt is None:
            await self._accepted_receipt(
                submission,
                actor_id=actor,
                current_parent_version=current_parent_version,
                investigation_service=investigation_service,
                investigation_store=investigation_store,
                store=store,
                minimum_trust=minimum_trust,
                require_committed_state=require_committed_state,
            )
            raise AssertionError("unreachable")
        payload_sha256 = self._payload_sha256(
            submission,
            actor_id=actor,
            source_ref=receipt.source_ref,
            parent_version=receipt.subject_version,
            supersedes_evidence_id=None,
        )
        replay = await store.resolve_evidence_replay(
            board_id=submission.board_id,
            submitted_by=actor,
            parent_id=submission.parent_id,
            idempotency_key=submission.idempotency_key,
        )
        if replay is not None:
            if replay.payload_sha256 != payload_sha256:
                raise CodeInvestigationIdempotencyConflict()
            return CodeEvidenceMutationResult(
                evidence=replay,
                head_revision=receipt.generation,
                replayed=True,
            )
        accepted = await self._accepted_receipt(
            submission,
            actor_id=actor,
            current_parent_version=current_parent_version,
            investigation_service=investigation_service,
            investigation_store=investigation_store,
            store=store,
            minimum_trust=minimum_trust,
            require_committed_state=require_committed_state,
        )
        item = self._materialize(
            submission,
            accepted=accepted,
            actor_id=actor,
            payload_sha256=payload_sha256,
            supersedes_evidence_id=None,
        )
        persisted = await store.create_evidence(
            evidence=item,
            expected_head_revision=accepted.head.revision,
        )
        if persisted != item:
            raise CodeInvestigationPayloadDigestMismatch(details={"field": "evidence"})
        return CodeEvidenceMutationResult(
            evidence=persisted,
            head_revision=accepted.head.revision,
        )

    async def supersede(
        self,
        submission: CodeEvidenceSupersessionSubmission,
        *,
        actor_id: str,
        actor_kind: str,
        current_parent_version: int,
        minimum_trust: CodeInvestigationTrustLevel,
        require_committed_state: bool,
        investigation_service: CodeInvestigationService,
        investigation_store: CodeInvestigationStore,
        store: CodeTraceabilityStore,
        receipt_content: str = "safe_excerpt",
    ) -> CodeEvidenceSupersessionCommitResult:
        actor = require_code_attestor(actor_id, actor_kind)
        self._enforce_receipt_content_policy(
            submission,
            receipt_content=receipt_content,
        )
        submission = sanitize_code_evidence_submission(submission)
        predecessor = await store.get_evidence(
            board_id=submission.board_id,
            evidence_id=submission.supersedes_evidence_id,
        )
        if predecessor is None:
            raise CodeEvidenceLinkInvalid(
                details={"evidence_id": submission.supersedes_evidence_id}
            )
        receipt = await investigation_store.get_receipt(
            board_id=submission.board_id,
            receipt_id=submission.investigation_receipt_id,
        )
        if receipt is None:
            await self._accepted_receipt(
                submission,
                actor_id=actor,
                current_parent_version=current_parent_version,
                investigation_service=investigation_service,
                investigation_store=investigation_store,
                store=store,
                minimum_trust=minimum_trust,
                require_committed_state=require_committed_state,
            )
            raise AssertionError("unreachable")
        payload_sha256 = self._payload_sha256(
            submission,
            actor_id=actor,
            source_ref=receipt.source_ref,
            parent_version=receipt.subject_version,
            supersedes_evidence_id=predecessor.id,
        )
        replay = await store.resolve_evidence_replay(
            board_id=submission.board_id,
            submitted_by=actor,
            parent_id=submission.parent_id,
            idempotency_key=submission.idempotency_key,
        )
        if replay is not None:
            if (
                replay.payload_sha256 != payload_sha256
                or replay.supersedes_evidence_id != predecessor.id
            ):
                raise CodeInvestigationIdempotencyConflict()
            return CodeEvidenceSupersessionCommitResult(
                predecessor=replace(
                    predecessor,
                    lifecycle_status=CodeTraceabilityLifecycleStatus.SUPERSEDED,
                ),
                replacement=replay,
                replayed=True,
            )
        if predecessor.lifecycle_status is not CodeTraceabilityLifecycleStatus.ACTIVE:
            raise CodeEvidenceLinkInvalid(details={"reason": "predecessor_not_active"})
        if predecessor.investigation_receipt_id == submission.investigation_receipt_id:
            raise CodeEvidenceLinkInvalid(
                details={"reason": "new_receipt_required_for_supersession"}
            )
        accepted = await self._accepted_receipt(
            submission,
            actor_id=actor,
            current_parent_version=current_parent_version,
            investigation_service=investigation_service,
            investigation_store=investigation_store,
            store=store,
            minimum_trust=minimum_trust,
            require_committed_state=require_committed_state,
        )
        if (
            predecessor.board_id != submission.board_id
            or predecessor.parent_type is not submission.parent_type
            or predecessor.parent_id != submission.parent_id
            or predecessor.source_ref != accepted.receipt.source_ref
        ):
            raise CodeEvidenceLinkInvalid(
                details={"reason": "supersession_scope_mismatch"}
            )
        replacement = self._materialize(
            submission,
            accepted=accepted,
            actor_id=actor,
            payload_sha256=payload_sha256,
            supersedes_evidence_id=predecessor.id,
        )
        superseded = replace(
            predecessor,
            lifecycle_status=CodeTraceabilityLifecycleStatus.SUPERSEDED,
        )
        return await store.supersede_evidence(
            predecessor=superseded,
            replacement=replacement,
            expected_head_revision=accepted.head.revision,
        )

    async def revoke(
        self,
        submission: CodeEvidenceRevokeInput,
        *,
        store: CodeTraceabilityStore,
    ) -> CodeEvidenceRevocationResult:
        current = await store.get_evidence(
            board_id=submission.board_id,
            evidence_id=submission.evidence_id,
        )
        if current is None:
            raise CodeEvidenceLinkInvalid(
                details={"evidence_id": submission.evidence_id}
            )
        if current.lifecycle_status is CodeTraceabilityLifecycleStatus.REVOKED:
            if current.revocation_reason == submission.reason:
                return CodeEvidenceRevocationResult(
                    evidence=current,
                    replayed=True,
                )
            raise CodeEvidenceImmutable(
                details={"evidence_id": current.id, "reason": "revocation_conflict"}
            )
        if current.lifecycle_status is not CodeTraceabilityLifecycleStatus.ACTIVE:
            raise CodeEvidenceImmutable(
                details={
                    "evidence_id": current.id,
                    "reason": "active_evidence_required",
                }
            )
        revoked = replace(
            current,
            lifecycle_status=CodeTraceabilityLifecycleStatus.REVOKED,
            revocation_reason=submission.reason,
        )
        persisted = await store.revoke_evidence(
            evidence=revoked,
            expected_lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
        )
        if persisted != revoked:
            raise CodeEvidenceImmutable(details={"evidence_id": current.id})
        return CodeEvidenceRevocationResult(evidence=persisted)

    async def link_to_spec(
        self,
        submission: CodeEvidenceSpecLinkInput,
        *,
        current_spec_version: int,
        created_by: str,
        store: CodeTraceabilityStore,
    ) -> CodeEvidenceLinkMutationResult:
        if current_spec_version != submission.expected_spec_version:
            raise CodeEvidenceLinkInvalid(details={"reason": "spec_version_conflict"})
        evidence = await store.get_evidence(
            board_id=submission.board_id,
            evidence_id=submission.evidence_id,
        )
        if (
            evidence is None
            or evidence.lifecycle_status is not CodeTraceabilityLifecycleStatus.ACTIVE
        ):
            raise CodeEvidenceLinkInvalid()
        new_spec_version = current_spec_version + 1
        cleared: CodeEvidenceDisposition | None = None
        active_disposition = await store.get_active_disposition(
            board_id=submission.board_id,
            spec_id=submission.spec_id,
            evidence_id=submission.evidence_id,
        )
        now = self._now()
        if active_disposition is not None:
            cleared = replace(
                active_disposition,
                active=False,
                cleared_by=created_by,
                cleared_at=now,
            )
            await store.clear_disposition(
                disposition=cleared,
                expected_spec_version=current_spec_version,
            )
        link = CodeEvidenceSpecLink(
            id=self._id_factory("code_evidence_spec_link"),
            board_id=submission.board_id,
            spec_id=submission.spec_id,
            evidence_id=evidence.id,
            entity_type=submission.entity_type,
            entity_id=submission.entity_id,
            relation_type=submission.relation_type,
            rationale=submission.rationale,
            evidence_content_sha256=evidence.content_sha256,
            source_refinement_version=(
                evidence.parent_version
                if evidence.parent_type is CodeTraceabilitySubjectType.REFINEMENT
                else None
            ),
            spec_version=new_spec_version,
            created_by=created_by,
            created_at=now,
        )
        persisted = await store.add_spec_link(
            link=link,
            expected_spec_version=current_spec_version,
        )
        return CodeEvidenceLinkMutationResult(
            link=persisted,
            spec_version=new_spec_version,
            cleared_disposition=cleared,
        )

    async def unlink_from_spec(
        self,
        submission: CodeEvidenceSpecUnlinkInput,
        *,
        current_spec_version: int,
        store: CodeTraceabilityStore,
    ) -> CodeEvidenceUnlinkMutationResult:
        if current_spec_version != submission.expected_spec_version:
            raise CodeEvidenceLinkInvalid(details={"reason": "spec_version_conflict"})
        removed = await store.remove_spec_link(
            board_id=submission.board_id,
            spec_id=submission.spec_id,
            link_id=submission.link_id,
            expected_spec_version=current_spec_version,
        )
        if removed is None:
            raise CodeEvidenceLinkInvalid(details={"link_id": submission.link_id})
        return CodeEvidenceUnlinkMutationResult(
            removed_link=removed,
            spec_version=current_spec_version + 1,
        )

    async def set_disposition(
        self,
        submission: CodeEvidenceDispositionInput,
        *,
        current_spec_version: int,
        spec_status: str,
        created_by: str,
        store: CodeTraceabilityStore,
    ) -> CodeEvidenceDispositionMutationResult:
        if current_spec_version != submission.expected_spec_version:
            raise CodeEvidenceLinkInvalid(details={"reason": "spec_version_conflict"})
        if (
            submission.disposition is CodeEvidenceDispositionKind.DEFERRED
            and str(getattr(spec_status, "value", spec_status)).lower() != "draft"
        ):
            raise CodeEvidenceLinkInvalid(details={"reason": "deferred_requires_draft"})
        links = await store.list_spec_links(
            board_id=submission.board_id,
            spec_id=submission.spec_id,
            evidence_id=submission.evidence_id,
        )
        if links:
            raise CodeEvidenceLinkInvalid(
                details={"reason": "active_link_conflicts_with_disposition"}
            )
        evidence = await store.get_evidence(
            board_id=submission.board_id,
            evidence_id=submission.evidence_id,
        )
        if evidence is None:
            raise CodeEvidenceLinkInvalid()
        if evidence.lifecycle_status is CodeTraceabilityLifecycleStatus.REVOKED:
            raise CodeEvidenceLinkInvalid(details={"reason": "evidence_revoked"})
        if (
            submission.disposition is CodeEvidenceDispositionKind.SUPERSEDED
            and evidence.lifecycle_status
            is not CodeTraceabilityLifecycleStatus.SUPERSEDED
        ):
            raise CodeEvidenceLinkInvalid(
                details={"reason": "replacement_evidence_required"}
            )
        existing = await store.get_active_disposition(
            board_id=submission.board_id,
            spec_id=submission.spec_id,
            evidence_id=submission.evidence_id,
        )
        now = self._now()
        if existing is not None:
            cleared = replace(
                existing,
                active=False,
                cleared_by=created_by,
                cleared_at=now,
            )
            await store.clear_disposition(
                disposition=cleared,
                expected_spec_version=current_spec_version,
            )
        next_version = current_spec_version + 1
        disposition = CodeEvidenceDisposition(
            id=self._id_factory("code_evidence_disposition"),
            board_id=submission.board_id,
            spec_id=submission.spec_id,
            evidence_id=submission.evidence_id,
            disposition=submission.disposition,
            justification=submission.justification,
            spec_version=next_version,
            active=True,
            created_by=created_by,
            created_at=now,
            cleared_by=None,
            cleared_at=None,
        )
        persisted = await store.set_disposition(
            disposition=disposition,
            expected_spec_version=current_spec_version,
        )
        return CodeEvidenceDispositionMutationResult(
            disposition=persisted,
            spec_version=next_version,
        )

    async def clear_disposition(
        self,
        submission: CodeEvidenceDispositionClearInput,
        *,
        current_spec_version: int,
        cleared_by: str,
        store: CodeTraceabilityStore,
    ) -> CodeEvidenceDispositionMutationResult:
        if current_spec_version != submission.expected_spec_version:
            raise CodeEvidenceLinkInvalid(details={"reason": "spec_version_conflict"})
        active = await store.get_active_disposition(
            board_id=submission.board_id,
            spec_id=submission.spec_id,
            evidence_id=submission.evidence_id,
        )
        if active is None:
            raise CodeEvidenceLinkInvalid(
                details={"reason": "active_disposition_not_found"}
            )
        next_version = current_spec_version + 1
        cleared = replace(
            active,
            active=False,
            spec_version=next_version,
            cleared_by=cleared_by,
            cleared_at=self._now(),
        )
        persisted = await store.clear_disposition(
            disposition=cleared,
            expected_spec_version=current_spec_version,
        )
        return CodeEvidenceDispositionMutationResult(
            disposition=persisted,
            spec_version=next_version,
        )


__all__ = [
    "CodeEvidenceDispositionMutationResult",
    "CodeEvidenceLinkMutationResult",
    "CodeEvidenceMutationResult",
    "CodeEvidenceRevocationResult",
    "CodeEvidenceService",
    "CodeEvidenceUnlinkMutationResult",
]
