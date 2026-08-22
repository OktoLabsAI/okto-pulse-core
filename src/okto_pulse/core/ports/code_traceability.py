"""Public persistence and projection boundaries for Code Traceability.

The store is transaction-bound and persists only structured, agent-attested
records.  The read port returns bounded Pulse projections.  Neither contract
contains a repository reader, provider connector, filesystem path root, Git
operation, search API, language resolver, or parsing cache.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Protocol, runtime_checkable

from okto_pulse.core.domain.code_traceability import (
    CodeEvidence,
    CodeEvidenceAttestationState,
    CodeEvidenceDisposition,
    CodeEvidenceLegacyClassification,
    CodeEvidenceLegacyClassificationBatchReceipt,
    CodeEvidenceSpecLink,
    CodeTraceabilityContext,
    CodeTraceabilityContextScope,
    CodeTraceabilityContractError,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilityPage,
    CodeTraceabilityPageCursor,
    CodeTraceabilityProjectionProfile,
    CodeTraceabilityRemediation,
    CodeTraceabilitySubjectType,
    CodeTraceabilityWaiver,
    CodeTraceabilityWaiverEntityType,
    CodeTraceabilityWaiverScope,
    DeliveryContext,
    ImplementationTarget,
    ImplementationTargetEvidenceLink,
    ImplementationTargetExecutionRecord,
    ImplementationTargetResolution,
    ImplementationTargetRole,
    ImplementationTargetSpecLink,
    SpecDeliveryContextProvenance,
    TargetOverlap,
    TargetOverlapAcknowledgement,
)


class CodeTraceabilityPersistenceError(RuntimeError):
    """Stable fail-closed adapter error."""

    code = "code_traceability_persistence_error"

    def __init__(
        self,
        message: str | None = None,
        *,
        details: Mapping[str, object] | None = None,
        remediation: tuple[CodeTraceabilityRemediation, ...] = (),
    ) -> None:
        if details is not None and not isinstance(details, Mapping):
            raise TypeError("code_traceability_persistence_details_invalid")
        if not isinstance(remediation, tuple) or any(
            not isinstance(item, CodeTraceabilityRemediation) for item in remediation
        ):
            raise TypeError("code_traceability_persistence_remediation_invalid")
        self.message = message or self.code
        self.details = MappingProxyType(dict(details or {}))
        self.remediation = remediation
        super().__init__(self.message)

    def as_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "message": self.message,
            "details": dict(self.details),
            "remediation": [item.as_dict() for item in self.remediation],
        }

    def to_error_dict(self) -> dict[str, object]:
        return self.as_dict()


class CodeTraceabilityAdapterMissing(CodeTraceabilityPersistenceError):
    code = "code_traceability_adapter_missing"


class CodeTraceabilityPersistenceConflict(CodeTraceabilityPersistenceError):
    code = "code_traceability_persistence_conflict"


class CodeTraceabilityRevisionConflict(CodeTraceabilityPersistenceError):
    code = "code_traceability_revision_conflict"


class CodeTraceabilityImmutableConflict(CodeTraceabilityPersistenceError):
    code = "code_evidence_immutable"


class CodeTraceabilityIdempotencyConflict(CodeTraceabilityPersistenceError):
    code = "code_investigation_idempotency_conflict"


class CodeTraceabilityCursorInvalid(CodeTraceabilityPersistenceError):
    code = "code_traceability_cursor_invalid"


class LegacyEvidenceClassificationPersistenceConflict(
    CodeTraceabilityPersistenceError
):
    code = "code_evidence_legacy_classification_persistence_conflict"


class LegacyEvidenceClassificationRevisionConflict(
    CodeTraceabilityPersistenceError
):
    code = "code_evidence_legacy_classification_revision_conflict"


class LegacyEvidenceClassificationIdempotencyConflict(
    CodeTraceabilityPersistenceError
):
    code = "code_evidence_legacy_classification_idempotency_conflict"


def _required(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CodeTraceabilityContractError(code)
    return value.strip()


def _limit(value: object, code: str) -> int:
    if type(value) is not int or not 1 <= value <= 200:
        raise CodeTraceabilityContractError(code)
    return value


def _enum_or_none(value: object, enum_type: type, code: str) -> object | None:
    if value is None or isinstance(value, enum_type):
        return value
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        raise CodeTraceabilityContractError(code) from exc


@dataclass(frozen=True, slots=True)
class CodeEvidenceQuery:
    board_id: str
    parent_type: CodeTraceabilitySubjectType | None = None
    parent_id: str | None = None
    lifecycle_status: CodeTraceabilityLifecycleStatus | None = None
    attestation_state: CodeEvidenceAttestationState | None = None
    cursor: CodeTraceabilityPageCursor | None = None
    limit: int = 50

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required(self.board_id, "code_evidence_query_board_id_invalid"),
        )
        parent_type = _enum_or_none(
            self.parent_type,
            CodeTraceabilitySubjectType,
            "code_evidence_query_parent_type_invalid",
        )
        parent_id = (
            None
            if self.parent_id is None
            else _required(self.parent_id, "code_evidence_query_parent_id_invalid")
        )
        if (parent_type is None) != (parent_id is None):
            raise CodeTraceabilityContractError("code_evidence_query_parent_incoherent")
        object.__setattr__(self, "parent_type", parent_type)
        object.__setattr__(self, "parent_id", parent_id)
        object.__setattr__(
            self,
            "lifecycle_status",
            _enum_or_none(
                self.lifecycle_status,
                CodeTraceabilityLifecycleStatus,
                "code_evidence_query_lifecycle_invalid",
            ),
        )
        object.__setattr__(
            self,
            "attestation_state",
            _enum_or_none(
                self.attestation_state,
                CodeEvidenceAttestationState,
                "code_evidence_query_attestation_state_invalid",
            ),
        )
        if self.cursor is not None and not isinstance(
            self.cursor, CodeTraceabilityPageCursor
        ):
            raise CodeTraceabilityContractError("code_evidence_query_cursor_invalid")
        object.__setattr__(
            self,
            "limit",
            _limit(self.limit, "code_evidence_query_limit_invalid"),
        )


@dataclass(frozen=True, slots=True)
class ImplementationTargetQuery:
    board_id: str
    card_id: str | None = None
    source_ref: str | None = None
    lifecycle_status: CodeTraceabilityLifecycleStatus | None = None
    role: ImplementationTargetRole | None = None
    cursor: CodeTraceabilityPageCursor | None = None
    limit: int = 50

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required(self.board_id, "implementation_target_query_board_id_invalid"),
        )
        for name in ("card_id", "source_ref"):
            object.__setattr__(
                self,
                name,
                (
                    None
                    if getattr(self, name) is None
                    else _required(
                        getattr(self, name),
                        f"implementation_target_query_{name}_invalid",
                    )
                ),
            )
        object.__setattr__(
            self,
            "lifecycle_status",
            _enum_or_none(
                self.lifecycle_status,
                CodeTraceabilityLifecycleStatus,
                "implementation_target_query_lifecycle_invalid",
            ),
        )
        object.__setattr__(
            self,
            "role",
            _enum_or_none(
                self.role,
                ImplementationTargetRole,
                "implementation_target_query_role_invalid",
            ),
        )
        if self.cursor is not None and not isinstance(
            self.cursor, CodeTraceabilityPageCursor
        ):
            raise CodeTraceabilityContractError(
                "implementation_target_query_cursor_invalid"
            )
        object.__setattr__(
            self,
            "limit",
            _limit(self.limit, "implementation_target_query_limit_invalid"),
        )


@dataclass(frozen=True, slots=True)
class TargetOverlapQuery:
    board_id: str
    card_id: str
    include_informational: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required(self.board_id, "target_overlap_query_board_id_invalid"),
        )
        object.__setattr__(
            self,
            "card_id",
            _required(self.card_id, "target_overlap_query_card_id_invalid"),
        )
        if not isinstance(self.include_informational, bool):
            raise CodeTraceabilityContractError(
                "target_overlap_query_include_informational_invalid"
            )


@dataclass(frozen=True, slots=True)
class CodeTraceabilityProjectionQuery:
    board_id: str
    subject_type: CodeTraceabilitySubjectType
    subject_id: str
    subject_version: int
    profile: CodeTraceabilityProjectionProfile = (
        CodeTraceabilityProjectionProfile.SUMMARY
    )
    context_scope: CodeTraceabilityContextScope = CodeTraceabilityContextScope.DEFAULT

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required(self.board_id, "code_traceability_query_board_id_invalid"),
        )
        object.__setattr__(
            self,
            "subject_type",
            _enum_or_none(
                self.subject_type,
                CodeTraceabilitySubjectType,
                "code_traceability_query_subject_type_invalid",
            ),
        )
        object.__setattr__(
            self,
            "subject_id",
            _required(self.subject_id, "code_traceability_query_subject_id_invalid"),
        )
        if type(self.subject_version) is not int or self.subject_version < 1:
            raise CodeTraceabilityContractError(
                "code_traceability_query_subject_version_invalid"
            )
        object.__setattr__(
            self,
            "profile",
            _enum_or_none(
                self.profile,
                CodeTraceabilityProjectionProfile,
                "code_traceability_query_profile_invalid",
            ),
        )
        object.__setattr__(
            self,
            "context_scope",
            _enum_or_none(
                self.context_scope,
                CodeTraceabilityContextScope,
                "code_traceability_query_context_scope_invalid",
            ),
        )
        if (
            self.context_scope is CodeTraceabilityContextScope.GATE
            and self.profile is not CodeTraceabilityProjectionProfile.FULL
        ):
            raise CodeTraceabilityContractError(
                "code_traceability_gate_profile_full_required"
            )


@dataclass(frozen=True, slots=True)
class CodeEvidenceSupersessionCommitResult:
    predecessor: CodeEvidence
    replacement: CodeEvidence
    replayed: bool = False

    def __post_init__(self) -> None:
        if (
            not isinstance(self.predecessor, CodeEvidence)
            or not isinstance(self.replacement, CodeEvidence)
            or self.predecessor.board_id != self.replacement.board_id
            or self.replacement.supersedes_evidence_id != self.predecessor.id
            or self.predecessor.lifecycle_status
            is not CodeTraceabilityLifecycleStatus.SUPERSEDED
        ):
            raise CodeTraceabilityContractError(
                "code_evidence_supersession_commit_invalid"
            )
        if not isinstance(self.replayed, bool):
            raise CodeTraceabilityContractError(
                "code_evidence_supersession_replayed_invalid"
            )


@dataclass(frozen=True, slots=True)
class ImplementationTargetResolutionCommitResult:
    target: ImplementationTarget
    resolution: ImplementationTargetResolution
    replayed: bool = False

    def __post_init__(self) -> None:
        if (
            not isinstance(self.target, ImplementationTarget)
            or not isinstance(self.resolution, ImplementationTargetResolution)
            or self.target.board_id != self.resolution.board_id
            or self.target.id != self.resolution.target_id
            or self.target.revision != self.resolution.target_revision
            or self.target.current_resolution_id != self.resolution.id
        ):
            raise CodeTraceabilityContractError(
                "implementation_target_resolution_commit_invalid"
            )
        if not isinstance(self.replayed, bool):
            raise CodeTraceabilityContractError(
                "implementation_target_resolution_replayed_invalid"
            )


@runtime_checkable
class CodeTraceabilityStore(Protocol):
    """Structured record persistence inside a caller-owned unit of work."""

    async def get_evidence(
        self,
        *,
        board_id: str,
        evidence_id: str,
    ) -> CodeEvidence | None: ...

    async def list_evidence(
        self,
        query: CodeEvidenceQuery,
    ) -> CodeTraceabilityPage[CodeEvidence]: ...

    async def resolve_evidence_replay(
        self,
        *,
        board_id: str,
        submitted_by: str,
        parent_id: str,
        idempotency_key: str,
    ) -> CodeEvidence | None: ...

    async def create_evidence(
        self,
        *,
        evidence: CodeEvidence,
        expected_head_revision: int,
    ) -> CodeEvidence: ...

    async def supersede_evidence(
        self,
        *,
        predecessor: CodeEvidence,
        replacement: CodeEvidence,
        expected_head_revision: int,
    ) -> CodeEvidenceSupersessionCommitResult: ...

    async def revoke_evidence(
        self,
        *,
        evidence: CodeEvidence,
        expected_lifecycle_status: CodeTraceabilityLifecycleStatus,
    ) -> CodeEvidence: ...

    async def get_latest_evidence_classification(
        self,
        *,
        board_id: str,
        evidence_id: str,
    ) -> CodeEvidenceLegacyClassification | None: ...

    async def get_evidence_classification(
        self,
        *,
        board_id: str,
        evidence_id: str,
        revision: int,
    ) -> CodeEvidenceLegacyClassification | None: ...

    async def list_latest_evidence_classifications(
        self,
        *,
        board_id: str,
        evidence_ids: tuple[str, ...],
    ) -> tuple[CodeEvidenceLegacyClassification, ...]: ...

    async def resolve_legacy_classification_batch_replay(
        self,
        *,
        board_id: str,
        classified_by: str,
        idempotency_key: str,
    ) -> CodeEvidenceLegacyClassificationBatchReceipt | None: ...

    async def append_legacy_evidence_classification_batch(
        self,
        *,
        receipt: CodeEvidenceLegacyClassificationBatchReceipt,
        expected_revisions: Mapping[str, int],
    ) -> CodeEvidenceLegacyClassificationBatchReceipt:
        """Atomically append all events and CAS every per-Evidence head.

        Adapters must verify each referenced Evidence payload digest and every
        expected classification revision in the same transaction.  A failure
        rolls back the complete batch; no mutable batch row is implied.
        """
        ...

    async def get_spec_link(
        self,
        *,
        board_id: str,
        link_id: str,
    ) -> CodeEvidenceSpecLink | None: ...

    async def list_spec_links(
        self,
        *,
        board_id: str,
        spec_id: str,
        evidence_id: str | None = None,
    ) -> tuple[CodeEvidenceSpecLink, ...]: ...

    async def add_spec_link(
        self,
        *,
        link: CodeEvidenceSpecLink,
        expected_spec_version: int,
    ) -> CodeEvidenceSpecLink: ...

    async def remove_spec_link(
        self,
        *,
        board_id: str,
        spec_id: str,
        link_id: str,
        expected_spec_version: int,
    ) -> CodeEvidenceSpecLink | None: ...

    async def get_active_disposition(
        self,
        *,
        board_id: str,
        spec_id: str,
        evidence_id: str,
    ) -> CodeEvidenceDisposition | None: ...

    async def list_spec_dispositions(
        self,
        *,
        board_id: str,
        spec_id: str,
        active_only: bool = True,
    ) -> tuple[CodeEvidenceDisposition, ...]: ...

    async def set_disposition(
        self,
        *,
        disposition: CodeEvidenceDisposition,
        expected_spec_version: int,
    ) -> CodeEvidenceDisposition: ...

    async def clear_disposition(
        self,
        *,
        disposition: CodeEvidenceDisposition,
        expected_spec_version: int,
    ) -> CodeEvidenceDisposition: ...

    async def apply_spec_evidence_rebase(
        self,
        *,
        board_id: str,
        spec_id: str,
        current_refinement_snapshot_id: str,
        current_refinement_version: int,
        target_refinement_snapshot_id: str,
        target_refinement_version: int,
        expected_delivery_context: DeliveryContext,
        expected_delivery_context_provenance: SpecDeliveryContextProvenance,
        next_delivery_context: DeliveryContext,
        next_delivery_context_provenance: SpecDeliveryContextProvenance,
        expected_source_context_manifest: dict[str, object],
        expected_source_context_sha256: str,
        next_source_context_manifest: dict[str, object],
        next_source_context_sha256: str,
        stale_link_ids: tuple[str, ...],
        invalid_disposition_ids: tuple[str, ...],
        cleared_by: str,
        cleared_at: datetime,
        expected_spec_version: int,
        next_spec_version: int,
    ) -> int:
        """Atomically CAS Spec lineage/context and affected Evidence mappings."""
        ...

    async def effective_spec_evidence(
        self,
        *,
        board_id: str,
        spec_id: str,
        spec_version: int,
    ) -> tuple[CodeEvidence, ...]: ...

    async def get_target(
        self,
        *,
        board_id: str,
        target_id: str,
    ) -> ImplementationTarget | None: ...

    async def list_targets(
        self,
        query: ImplementationTargetQuery,
    ) -> CodeTraceabilityPage[ImplementationTarget]: ...

    async def create_target(
        self,
        *,
        target: ImplementationTarget,
        expected_head_revision: int,
        expected_spec_version: int,
    ) -> ImplementationTarget: ...

    async def update_target(
        self,
        *,
        target: ImplementationTarget,
        expected_revision: int,
    ) -> ImplementationTarget: ...

    async def add_target_spec_link(
        self,
        link: ImplementationTargetSpecLink,
    ) -> ImplementationTargetSpecLink: ...

    async def list_target_spec_links(
        self,
        *,
        board_id: str,
        target_id: str,
    ) -> tuple[ImplementationTargetSpecLink, ...]: ...

    async def add_target_evidence_link(
        self,
        link: ImplementationTargetEvidenceLink,
    ) -> ImplementationTargetEvidenceLink: ...

    async def list_target_evidence_links(
        self,
        *,
        board_id: str,
        target_id: str,
    ) -> tuple[ImplementationTargetEvidenceLink, ...]: ...

    async def replace_target_links(
        self,
        *,
        board_id: str,
        target_id: str,
        spec_links: tuple[ImplementationTargetSpecLink, ...],
        evidence_links: tuple[ImplementationTargetEvidenceLink, ...],
        expected_target_revision: int,
    ) -> tuple[
        tuple[ImplementationTargetSpecLink, ...],
        tuple[ImplementationTargetEvidenceLink, ...],
    ]:
        """Replace both target-link sets under the target revision CAS.

        Editions must apply deletion, insertion and the revision guard in the
        caller-owned transaction.  This prevents a partially replaced link
        graph when one of the two collections fails validation or persistence.
        """
        ...

    async def get_resolution(
        self,
        *,
        board_id: str,
        resolution_id: str,
    ) -> ImplementationTargetResolution | None: ...

    async def list_resolutions(
        self,
        *,
        board_id: str,
        target_id: str,
    ) -> tuple[ImplementationTargetResolution, ...]: ...

    async def resolve_resolution_replay(
        self,
        *,
        board_id: str,
        submitted_by: str,
        investigation_receipt_id: str,
        target_id: str,
        idempotency_key: str,
    ) -> ImplementationTargetResolution | None: ...

    async def append_resolution(
        self,
        *,
        target: ImplementationTarget,
        resolution: ImplementationTargetResolution,
        expected_target_revision: int,
        expected_head_revision: int,
    ) -> ImplementationTargetResolutionCommitResult: ...

    async def list_execution_records(
        self,
        *,
        board_id: str,
        target_id: str,
    ) -> tuple[ImplementationTargetExecutionRecord, ...]: ...

    async def resolve_execution_replay(
        self,
        *,
        board_id: str,
        submitted_by: str,
        result_investigation_receipt_id: str,
        target_id: str,
        idempotency_key: str,
    ) -> ImplementationTargetExecutionRecord | None: ...

    async def append_execution_record(
        self,
        *,
        record: ImplementationTargetExecutionRecord,
        expected_head_revision: int,
    ) -> ImplementationTargetExecutionRecord: ...

    async def list_overlap_acknowledgements(
        self,
        *,
        board_id: str,
        card_id: str,
    ) -> tuple[TargetOverlapAcknowledgement, ...]: ...

    async def add_overlap_acknowledgement(
        self,
        acknowledgement: TargetOverlapAcknowledgement,
    ) -> TargetOverlapAcknowledgement: ...

    async def get_active_waiver(
        self,
        *,
        board_id: str,
        entity_type: CodeTraceabilityWaiverEntityType,
        entity_id: str,
        scope: CodeTraceabilityWaiverScope,
    ) -> CodeTraceabilityWaiver | None: ...

    async def get_waiver(
        self,
        *,
        board_id: str,
        waiver_id: str,
    ) -> CodeTraceabilityWaiver | None: ...

    async def create_waiver(
        self,
        waiver: CodeTraceabilityWaiver,
    ) -> CodeTraceabilityWaiver: ...

    async def clear_waiver(
        self,
        waiver: CodeTraceabilityWaiver,
    ) -> CodeTraceabilityWaiver: ...


@runtime_checkable
class CodeTraceabilityReadPort(Protocol):
    """Bounded aggregate projections; summary/gate never expose excerpts."""

    async def refinement_context(
        self,
        query: CodeTraceabilityProjectionQuery,
    ) -> CodeTraceabilityContext: ...

    async def spec_context(
        self,
        query: CodeTraceabilityProjectionQuery,
    ) -> CodeTraceabilityContext: ...

    async def card_context(
        self,
        query: CodeTraceabilityProjectionQuery,
    ) -> CodeTraceabilityContext: ...

    async def overlap_report(
        self,
        query: TargetOverlapQuery,
    ) -> tuple[TargetOverlap, ...]: ...

    async def traceability_projection(
        self,
        query: CodeTraceabilityProjectionQuery,
    ) -> CodeTraceabilityContext: ...


__all__ = [
    "CodeEvidenceQuery",
    "CodeEvidenceSupersessionCommitResult",
    "CodeTraceabilityAdapterMissing",
    "CodeTraceabilityCursorInvalid",
    "CodeTraceabilityIdempotencyConflict",
    "CodeTraceabilityImmutableConflict",
    "CodeTraceabilityPersistenceConflict",
    "CodeTraceabilityPersistenceError",
    "CodeTraceabilityProjectionQuery",
    "CodeTraceabilityReadPort",
    "CodeTraceabilityRevisionConflict",
    "CodeTraceabilityStore",
    "ImplementationTargetQuery",
    "ImplementationTargetResolutionCommitResult",
    "LegacyEvidenceClassificationIdempotencyConflict",
    "LegacyEvidenceClassificationPersistenceConflict",
    "LegacyEvidenceClassificationRevisionConflict",
    "TargetOverlapQuery",
]
