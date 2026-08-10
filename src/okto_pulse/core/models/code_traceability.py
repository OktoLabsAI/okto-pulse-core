"""Closed transport DTOs for agent-attested code traceability.

Submission models deliberately omit server-owned actor, source-lineage,
currentness, trust, timestamps, and digest fields.  ``extra='forbid'`` makes a
client attempt to forge any of those fields fail before application handling.
No model in this module is a repository locator or authorizes Pulse to inspect
source code.
"""

from __future__ import annotations

from datetime import datetime
import hashlib
from typing import ClassVar, Self
import unicodedata

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)

from okto_pulse.core.domain.code_traceability import (
    DEFAULT_CODE_TRACEABILITY_LIMITS,
    CodeEvidence,
    CodeEvidenceAttestationBasis,
    CodeEvidenceAttestationState,
    CodeEvidenceDisposition,
    CodeEvidenceDispositionKind,
    CodeEvidenceSelectorKind,
    CodeEvidenceSpecLink,
    CodeEvidenceSpecRelationType,
    CodeEvidenceType,
    CodeInvestigationAcceptanceStatus,
    CodeInvestigationCapability,
    CodeInvestigationHead,
    CodeInvestigationHeadState,
    CodeInvestigationOmission,
    CodeInvestigationOmissionReason,
    CodeInvestigationOutcome,
    CodeInvestigationReceipt,
    CodeInvestigationRequest,
    CodeInvestigationRequestStatus,
    CodeInvestigationTooling,
    CodeInvestigationTrustLevel,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilityContextScope,
    CodeTraceabilityProjectionProfile,
    CodeTraceabilitySubjectType,
    CodeTraceabilityWaiver,
    CodeTraceabilityWaiverEntityType,
    CodeTraceabilityWaiverReason,
    CodeTraceabilityWaiverScope,
    ImplementationTarget,
    ImplementationTargetExecutionDisposition,
    ImplementationTargetExecutionRecord,
    ImplementationTargetEvidenceRelationType,
    ImplementationTargetResolution,
    ImplementationTargetResolutionState,
    ImplementationTargetRole,
    ImplementationTargetSelectorKind,
    ResolutionCandidate,
    SpecEntityType,
    TargetOverlapAcknowledgement,
    TargetOverlapDisposition,
    WorkspaceReproducibilityClaim,
    normalize_code_relative_path,
    normalize_code_source_ref,
)


SHA256_PATTERN = r"^[0-9a-fA-F]{64}$"


class _ClosedModel(BaseModel):
    envelope_limit: ClassVar[int | None] = None

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

    @model_validator(mode="after")
    def _bounded_envelope(self) -> Self:
        limit = self.envelope_limit
        if limit is not None and len(self.model_dump_json().encode("utf-8")) > limit:
            raise ValueError("code_investigation_submission_limit_exceeded")
        return self


class _DomainView(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        from_attributes=True,
    )

    @classmethod
    def from_domain(cls, value: object) -> Self:
        return cls.model_validate(value, from_attributes=True)


def _bounded_utf8(value: str, *, max_bytes: int, field_name: str) -> str:
    normalized = unicodedata.normalize("NFC", value)
    if len(normalized.encode("utf-8")) > max_bytes:
        raise ValueError(f"{field_name}_limit_exceeded")
    return normalized


def _validated_path(value: str | None) -> str | None:
    if value is None:
        return None
    return normalize_code_relative_path(value)


class ObservedWorkspaceStateSubmission(_ClosedModel):
    """Workspace fields supplied once; revision/time live on the receipt."""

    workspace_state_id: str = Field(min_length=1, max_length=512)
    declared_dirty: bool
    reproducibility_claim: WorkspaceReproducibilityClaim
    fingerprint_algorithm: str = Field(min_length=1, max_length=256)
    manifest_digest: str = Field(pattern=SHA256_PATTERN)
    manifest_entry_count: int = Field(ge=0)


class CodeInvestigationOmissionInput(_ClosedModel):
    reason_code: CodeInvestigationOmissionReason
    affected_scope_digest: str = Field(pattern=SHA256_PATTERN)
    count: int = Field(ge=1)


class CodeInvestigationToolingInput(_ClosedModel):
    tool_id: str = Field(min_length=1, max_length=256)
    tool_version: str = Field(min_length=1, max_length=256)
    method_id: str = Field(min_length=1, max_length=256)


class StartCodeInvestigationInput(_ClosedModel):
    """Agent-facing request; head, profiles, scope and challenge are server-owned."""

    board_id: str = Field(min_length=1)
    subject_type: CodeTraceabilitySubjectType
    subject_id: str = Field(min_length=1)
    expected_subject_version: int = Field(ge=1)
    source_ref: str | None = Field(default=None, min_length=1)
    idempotency_key: str = Field(min_length=1, max_length=512)

    @field_validator("source_ref")
    @classmethod
    def _opaque_source_ref(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_code_source_ref(value)


class CodeInvestigationReceiptSubmission(_ClosedModel):
    """Only an authenticated agent may submit this inbound attestation."""

    envelope_limit: ClassVar[int] = (
        DEFAULT_CODE_TRACEABILITY_LIMITS.receipt_envelope_bytes
    )

    board_id: str = Field(min_length=1)
    request_id: str = Field(min_length=1)
    challenge_token: SecretStr = Field(min_length=1, max_length=4096)
    outcome: CodeInvestigationOutcome
    capabilities: tuple[CodeInvestigationCapability, ...]
    source_identity_digest: str | None = Field(
        default=None,
        pattern=SHA256_PATTERN,
    )
    declared_revision: str | None = Field(default=None, max_length=2048)
    workspace_state: ObservedWorkspaceStateSubmission | None = None
    omission_manifest: tuple[CodeInvestigationOmissionInput, ...] = Field(
        default=(),
        max_length=DEFAULT_CODE_TRACEABILITY_LIMITS.omission_entries,
    )
    tooling: CodeInvestigationToolingInput
    observed_at: datetime
    idempotency_key: str = Field(min_length=1, max_length=512)

    @field_validator("capabilities")
    @classmethod
    def _unique_capabilities(
        cls,
        value: tuple[CodeInvestigationCapability, ...],
    ) -> tuple[CodeInvestigationCapability, ...]:
        if len(value) != len(set(value)):
            raise ValueError("code_investigation_capabilities_duplicate")
        return tuple(sorted(value, key=lambda item: item.value))

    @model_validator(mode="after")
    def _coherent_outcome(self) -> Self:
        if self.outcome is CodeInvestigationOutcome.ACCESSIBLE:
            if self.omission_manifest:
                raise ValueError("code_investigation_outcome_omissions_incoherent")
        elif not self.omission_manifest:
            raise ValueError("code_investigation_omission_reason_required")
        if self.outcome is CodeInvestigationOutcome.UNAVAILABLE and any(
            value is not None
            for value in (
                self.source_identity_digest,
                self.declared_revision,
                self.workspace_state,
            )
        ):
            raise ValueError("code_investigation_unavailable_claims_incoherent")
        return self


class CodeEvidenceSelectorInput(_ClosedModel):
    kind: CodeEvidenceSelectorKind
    relative_path: str | None = None
    language: str | None = Field(default=None, max_length=256)
    symbol_kind: str | None = Field(default=None, max_length=256)
    qualified_symbol: str | None = None
    symbol_signature: str | None = None
    line_start: int | None = Field(default=None, ge=1)
    line_end: int | None = Field(default=None, ge=1)

    @field_validator("relative_path")
    @classmethod
    def _relative_path(cls, value: str | None) -> str | None:
        return _validated_path(value)

    @field_validator("qualified_symbol", "symbol_signature")
    @classmethod
    def _symbol_text(cls, value: str | None, info: object) -> str | None:
        if value is None:
            return None
        return _bounded_utf8(
            value,
            max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.symbol_text_bytes,
            field_name="code_evidence_symbol_text",
        )

    @model_validator(mode="after")
    def _coherent_selector(self) -> Self:
        if self.kind is CodeEvidenceSelectorKind.SYMBOL and not self.qualified_symbol:
            raise ValueError("code_evidence_qualified_symbol_required")
        if self.kind is CodeEvidenceSelectorKind.FILE and not self.relative_path:
            raise ValueError("code_evidence_relative_path_required")
        if (self.line_start is None) != (self.line_end is None):
            raise ValueError("code_evidence_snapshot_lines_invalid")
        if (
            self.line_start is not None
            and self.line_end is not None
            and self.line_end < self.line_start
        ):
            raise ValueError("code_evidence_snapshot_lines_invalid")
        if self.line_start is not None and self.relative_path is None:
            raise ValueError("code_evidence_snapshot_path_required")
        return self


class CodeEvidenceSubmission(_ClosedModel):
    """Agent-owned Evidence fields; receipt supplies every attestation field."""

    # Source excerpts are exact evidence.  Global whitespace stripping would
    # silently alter their bytes before hash validation.
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=False,
    )

    envelope_limit: ClassVar[int] = (
        DEFAULT_CODE_TRACEABILITY_LIMITS.evidence_envelope_bytes
    )

    board_id: str = Field(min_length=1)
    investigation_receipt_id: str = Field(min_length=1)
    parent_type: CodeTraceabilitySubjectType
    parent_id: str = Field(min_length=1)
    evidence_type: CodeEvidenceType
    claim: str = Field(min_length=1)
    selector: CodeEvidenceSelectorInput
    excerpt: str | None = None
    excerpt_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    declared_file_blob_sha256: str | None = Field(
        default=None,
        pattern=SHA256_PATTERN,
    )
    declared_source_content_sha256: str = Field(pattern=SHA256_PATTERN)
    idempotency_key: str = Field(min_length=1, max_length=512)

    @field_validator("excerpt")
    @classmethod
    def _bounded_excerpt(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = unicodedata.normalize("NFC", value).replace("\r\n", "\n")
        normalized = normalized.replace("\r", "\n")
        return _bounded_utf8(
            normalized,
            max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.evidence_excerpt_bytes,
            field_name="code_evidence_excerpt",
        )

    @model_validator(mode="after")
    def _coherent_excerpt(self) -> Self:
        if self.excerpt is None:
            if self.excerpt_sha256 is not None:
                raise ValueError("code_evidence_excerpt_hash_incoherent")
        else:
            expected = hashlib.sha256(self.excerpt.encode("utf-8")).hexdigest()
            if self.excerpt_sha256 is None or self.excerpt_sha256.lower() != expected:
                raise ValueError("code_evidence_excerpt_hash_incoherent")
        return self


class CodeEvidenceSupersessionSubmission(CodeEvidenceSubmission):
    """Distinct immutable replacement command, never a partial dict patch."""

    supersedes_evidence_id: str = Field(min_length=1)
    supersession_reason: str = Field(min_length=1)


class CodeEvidenceRevokeInput(_ClosedModel):
    board_id: str = Field(min_length=1)
    evidence_id: str = Field(min_length=1)
    reason: str = Field(min_length=1, max_length=4_000)


class CodeEvidenceSpecLinkInput(_ClosedModel):
    board_id: str = Field(min_length=1)
    spec_id: str = Field(min_length=1)
    evidence_id: str = Field(min_length=1)
    entity_type: SpecEntityType
    entity_id: str = Field(min_length=1)
    relation_type: CodeEvidenceSpecRelationType
    rationale: str = Field(min_length=1)
    expected_spec_version: int = Field(ge=1)


class CodeEvidenceSpecUnlinkInput(_ClosedModel):
    board_id: str = Field(min_length=1)
    spec_id: str = Field(min_length=1)
    link_id: str = Field(min_length=1)
    expected_spec_version: int = Field(ge=1)


class CodeEvidenceDispositionInput(_ClosedModel):
    board_id: str = Field(min_length=1)
    spec_id: str = Field(min_length=1)
    evidence_id: str = Field(min_length=1)
    disposition: CodeEvidenceDispositionKind
    justification: str = Field(min_length=1)
    expected_spec_version: int = Field(ge=1)


class CodeEvidenceDispositionClearInput(_ClosedModel):
    board_id: str = Field(min_length=1)
    spec_id: str = Field(min_length=1)
    evidence_id: str = Field(min_length=1)
    expected_spec_version: int = Field(ge=1)


class SpecCodeEvidenceRebasePreviewInput(_ClosedModel):
    board_id: str = Field(min_length=1)
    spec_id: str = Field(min_length=1)
    target_refinement_version: int = Field(ge=1)
    expected_spec_version: int = Field(ge=1)


class SpecCodeEvidenceRebaseApplyInput(SpecCodeEvidenceRebasePreviewInput):
    preview_sha256: str = Field(pattern=SHA256_PATTERN)


class ImplementationTargetSpecLinkInput(_ClosedModel):
    entity_type: SpecEntityType
    entity_id: str = Field(min_length=1)


class ImplementationTargetEvidenceLinkInput(_ClosedModel):
    evidence_id: str = Field(min_length=1)
    relation_type: ImplementationTargetEvidenceRelationType


class ImplementationTargetCreateInput(_ClosedModel):
    board_id: str = Field(min_length=1)
    card_id: str = Field(min_length=1)
    source_ref: str = Field(min_length=1)
    selector_kind: ImplementationTargetSelectorKind
    relative_path_hint: str | None = None
    language: str | None = Field(default=None, max_length=256)
    symbol_kind: str | None = Field(default=None, max_length=256)
    qualified_symbol: str | None = None
    symbol_signature: str | None = None
    role: ImplementationTargetRole
    intent: str = Field(min_length=1)
    required: bool = True
    expected_spec_version: int = Field(ge=1)
    baseline_evidence_id: str | None = None
    spec_links: tuple[ImplementationTargetSpecLinkInput, ...] = Field(
        default=(),
        max_length=100,
    )
    evidence_links: tuple[ImplementationTargetEvidenceLinkInput, ...] = Field(
        default=(),
        max_length=100,
    )

    @field_validator("source_ref")
    @classmethod
    def _target_source_ref(cls, value: str) -> str:
        return normalize_code_source_ref(value)

    @field_validator("relative_path_hint")
    @classmethod
    def _relative_path_hint(cls, value: str | None) -> str | None:
        return _validated_path(value)

    @field_validator("qualified_symbol", "symbol_signature")
    @classmethod
    def _target_symbol_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _bounded_utf8(
            value,
            max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.symbol_text_bytes,
            field_name="implementation_target_symbol_text",
        )

    @model_validator(mode="after")
    def _coherent_selector(self) -> Self:
        if (
            self.selector_kind is ImplementationTargetSelectorKind.SYMBOL
            and not self.qualified_symbol
        ):
            raise ValueError("implementation_target_qualified_symbol_required")
        if (
            self.selector_kind
            in {
                ImplementationTargetSelectorKind.FILE,
                ImplementationTargetSelectorKind.NEW_FILE,
            }
            and not self.relative_path_hint
        ):
            raise ValueError("implementation_target_relative_path_hint_required")
        return self


class ImplementationTargetUpdateInput(_ClosedModel):
    board_id: str = Field(min_length=1)
    card_id: str = Field(min_length=1)
    target_id: str = Field(min_length=1)
    expected_revision: int = Field(ge=1)
    change_reason: str = Field(min_length=1)
    selector_kind: ImplementationTargetSelectorKind | None = None
    relative_path_hint: str | None = None
    language: str | None = Field(default=None, max_length=256)
    symbol_kind: str | None = Field(default=None, max_length=256)
    qualified_symbol: str | None = None
    symbol_signature: str | None = None
    role: ImplementationTargetRole | None = None
    intent: str | None = Field(default=None, min_length=1)
    required: bool | None = None
    baseline_evidence_id: str | None = None
    lifecycle_status: CodeTraceabilityLifecycleStatus | None = None
    spec_links: tuple[ImplementationTargetSpecLinkInput, ...] | None = Field(
        default=None,
        max_length=100,
    )
    evidence_links: tuple[ImplementationTargetEvidenceLinkInput, ...] | None = Field(
        default=None,
        max_length=100,
    )

    @field_validator("relative_path_hint")
    @classmethod
    def _update_relative_path_hint(cls, value: str | None) -> str | None:
        return _validated_path(value)

    @field_validator("qualified_symbol", "symbol_signature")
    @classmethod
    def _update_symbol_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _bounded_utf8(
            value,
            max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.symbol_text_bytes,
            field_name="implementation_target_symbol_text",
        )

    @model_validator(mode="after")
    def _has_mutation(self) -> Self:
        mutable_fields = {
            "selector_kind",
            "relative_path_hint",
            "language",
            "symbol_kind",
            "qualified_symbol",
            "symbol_signature",
            "role",
            "intent",
            "required",
            "baseline_evidence_id",
            "lifecycle_status",
            "spec_links",
            "evidence_links",
        }
        if not (self.model_fields_set & mutable_fields):
            raise ValueError("implementation_target_update_empty")
        return self


class ResolutionCandidateInput(_ClosedModel):
    relative_path: str
    qualified_symbol: str | None = None
    symbol_signature: str | None = None
    symbol_fingerprint: str | None = Field(default=None, pattern=SHA256_PATTERN)
    confidence: float = Field(ge=0.0, le=1.0, allow_inf_nan=False)
    reason_code: str = Field(min_length=1, max_length=256)

    @field_validator("relative_path")
    @classmethod
    def _candidate_path(cls, value: str) -> str:
        return normalize_code_relative_path(value)

    @field_validator("qualified_symbol", "symbol_signature")
    @classmethod
    def _candidate_symbol_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _bounded_utf8(
            value,
            max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.symbol_text_bytes,
            field_name="implementation_resolution_candidate_symbol_text",
        )


class ImplementationTargetResolutionSubmission(_ClosedModel):
    """Agent resolution claim; lineage/currentness fields are absent by design."""

    envelope_limit: ClassVar[int] = (
        DEFAULT_CODE_TRACEABILITY_LIMITS.resolution_envelope_bytes
    )

    board_id: str = Field(min_length=1)
    card_id: str = Field(min_length=1)
    target_id: str = Field(min_length=1)
    investigation_receipt_id: str = Field(min_length=1)
    state: ImplementationTargetResolutionState
    resolved_relative_path: str | None = None
    resolved_language: str | None = Field(default=None, max_length=256)
    resolved_symbol_kind: str | None = Field(default=None, max_length=256)
    resolved_qualified_symbol: str | None = None
    resolved_symbol_signature: str | None = None
    resolved_line_start: int | None = Field(default=None, ge=1)
    resolved_line_end: int | None = Field(default=None, ge=1)
    symbol_fingerprint: str | None = Field(default=None, pattern=SHA256_PATTERN)
    declared_file_blob_sha256: str | None = Field(
        default=None,
        pattern=SHA256_PATTERN,
    )
    confidence: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        allow_inf_nan=False,
    )
    reason_code: str | None = Field(default=None, max_length=256)
    candidates: tuple[ResolutionCandidateInput, ...] = Field(
        default=(),
        max_length=DEFAULT_CODE_TRACEABILITY_LIMITS.resolution_candidates,
    )
    tooling: CodeInvestigationToolingInput
    agent_observed_at: datetime
    idempotency_key: str = Field(min_length=1, max_length=512)

    @field_validator("resolved_relative_path")
    @classmethod
    def _resolved_path(cls, value: str | None) -> str | None:
        return _validated_path(value)

    @field_validator("resolved_qualified_symbol", "resolved_symbol_signature")
    @classmethod
    def _resolved_symbol_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _bounded_utf8(
            value,
            max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.symbol_text_bytes,
            field_name="implementation_resolution_symbol_text",
        )

    @model_validator(mode="after")
    def _coherent_state(self) -> Self:
        has_path = self.resolved_relative_path is not None
        if (self.resolved_line_start is None) != (self.resolved_line_end is None):
            raise ValueError("implementation_target_resolution_lines_invalid")
        if (
            self.resolved_line_start is not None
            and self.resolved_line_end is not None
            and self.resolved_line_end < self.resolved_line_start
        ):
            raise ValueError("implementation_target_resolution_lines_invalid")
        if self.resolved_line_start is not None and not has_path:
            raise ValueError("implementation_target_resolution_path_required")
        if self.state in {
            ImplementationTargetResolutionState.RESOLVED,
            ImplementationTargetResolutionState.MOVED,
        }:
            if (
                not has_path
                or self.confidence is None
                or self.confidence < 0.95
                or len(self.candidates) > 1
            ):
                raise ValueError(
                    "implementation_target_resolution_resolved_threshold_invalid"
                )
        if self.state is ImplementationTargetResolutionState.STALE and (
            not self.reason_code
            or not (has_path or self.candidates)
            or self.confidence is None
            or not 0.80 <= self.confidence < 0.95
        ):
            raise ValueError("implementation_target_resolution_stale_threshold_invalid")
        if self.state is ImplementationTargetResolutionState.AMBIGUOUS:
            ranked = sorted(
                (item.confidence for item in self.candidates),
                reverse=True,
            )
            if (
                len(ranked) < 2
                or has_path
                or self.confidence is not None
                or ranked[0] - ranked[1] > 0.05
            ):
                raise ValueError(
                    "implementation_target_resolution_ambiguous_threshold_invalid"
                )
        if self.state in {
            ImplementationTargetResolutionState.MISSING,
            ImplementationTargetResolutionState.UNAVAILABLE,
        } and (
            not self.reason_code
            or has_path
            or self.candidates
            or self.confidence is not None
        ):
            raise ValueError("implementation_target_resolution_terminal_incoherent")
        return self


class ImplementationTargetExecutionSubmission(_ClosedModel):
    envelope_limit: ClassVar[int] = (
        DEFAULT_CODE_TRACEABILITY_LIMITS.execution_envelope_bytes
    )

    board_id: str = Field(min_length=1)
    card_id: str = Field(min_length=1)
    target_id: str = Field(min_length=1)
    result_investigation_receipt_id: str = Field(min_length=1)
    disposition: ImplementationTargetExecutionDisposition
    actual_relative_path: str | None = None
    actual_qualified_symbol: str | None = None
    replacement_target_id: str | None = None
    justification: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1, max_length=512)

    @field_validator("actual_relative_path")
    @classmethod
    def _actual_path(cls, value: str | None) -> str | None:
        return _validated_path(value)

    @field_validator("actual_qualified_symbol")
    @classmethod
    def _actual_symbol(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _bounded_utf8(
            value,
            max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.symbol_text_bytes,
            field_name="implementation_execution_symbol_text",
        )

    @model_validator(mode="after")
    def _coherent_replacement(self) -> Self:
        if self.disposition is ImplementationTargetExecutionDisposition.REPLACED:
            if (
                not self.replacement_target_id
                or self.replacement_target_id == self.target_id
            ):
                raise ValueError("target_execution_replacement_required")
        elif self.replacement_target_id is not None:
            raise ValueError("target_execution_replacement_incoherent")
        return self


class TargetOverlapAcknowledgementInput(_ClosedModel):
    board_id: str = Field(min_length=1)
    card_id: str = Field(min_length=1)
    target_a_id: str = Field(min_length=1)
    target_b_id: str = Field(min_length=1)
    resolution_a_id: str = Field(min_length=1)
    resolution_b_id: str = Field(min_length=1)
    disposition: TargetOverlapDisposition
    justification: str = Field(min_length=1)

    @model_validator(mode="after")
    def _distinct_pair(self) -> Self:
        if self.target_a_id == self.target_b_id:
            raise ValueError("target_overlap_targets_duplicate")
        if self.resolution_a_id == self.resolution_b_id:
            raise ValueError("target_overlap_resolutions_duplicate")
        return self


class CodeTraceabilityWaiverInput(_ClosedModel):
    board_id: str = Field(min_length=1)
    entity_type: CodeTraceabilityWaiverEntityType
    entity_id: str = Field(min_length=1)
    scope: CodeTraceabilityWaiverScope
    reason_code: CodeTraceabilityWaiverReason
    justification: str = Field(min_length=1)


class CodeTraceabilityWaiverClearInput(_ClosedModel):
    board_id: str = Field(min_length=1)
    waiver_id: str = Field(min_length=1)


class ObservedWorkspaceStateView(_DomainView):
    declared_revision: str | None
    workspace_state_id: str
    declared_dirty: bool
    observed_at: datetime
    reproducibility_claim: WorkspaceReproducibilityClaim
    fingerprint_algorithm: str
    manifest_digest: str
    manifest_entry_count: int


class CodeInvestigationOmissionView(_DomainView):
    reason_code: CodeInvestigationOmissionReason
    affected_scope_digest: str
    count: int


class CodeInvestigationToolingView(_DomainView):
    tool_id: str
    tool_version: str
    method_id: str


class CodeInvestigationRequestView(_DomainView):
    """Safe request projection: persisted challenge hash is never exposed."""

    id: str
    board_id: str
    subject_type: CodeTraceabilitySubjectType
    subject_id: str
    subject_version: int
    issued_to_actor_id: str
    source_ref: str
    required_capabilities: tuple[CodeInvestigationCapability, ...]
    selector_scope_digest: str
    expected_head_generation: int
    expected_predecessor_receipt_id: str | None
    canonicalization_profile: str
    limits_profile: str
    challenge_key_id: str
    status: CodeInvestigationRequestStatus
    single_use: bool
    expires_at: datetime
    requested_by: str
    created_at: datetime
    consumed_at: datetime | None
    request_payload_sha256: str
    idempotency_key: str


class StartCodeInvestigationResult(_ClosedModel):
    """Challenge is returned only while the idempotent request remains open."""

    request: CodeInvestigationRequestView
    challenge_token: str | None = Field(default=None, min_length=1)
    consumed_receipt_id: str | None = None

    @model_validator(mode="after")
    def _challenge_visibility(self) -> Self:
        if self.request.status is CodeInvestigationRequestStatus.OPEN:
            if self.challenge_token is None:
                raise ValueError("code_investigation_challenge_required")
        elif self.challenge_token is not None:
            raise ValueError("code_investigation_challenge_must_not_be_reexposed")
        if (
            self.request.status is CodeInvestigationRequestStatus.CONSUMED
            and self.consumed_receipt_id is None
        ):
            raise ValueError("code_investigation_consumed_receipt_required")
        return self


class CodeInvestigationReceiptView(_DomainView):
    id: str
    request_id: str
    board_id: str
    subject_type: CodeTraceabilitySubjectType
    subject_id: str
    subject_version: int
    attestor_actor_id: str
    generation: int
    predecessor_receipt_id: str | None
    trust_level: CodeInvestigationTrustLevel
    acceptance_status: CodeInvestigationAcceptanceStatus
    outcome: CodeInvestigationOutcome
    capabilities: tuple[CodeInvestigationCapability, ...]
    source_ref: str
    source_identity_digest: str | None
    canonicalization_profile: str
    limits_profile: str
    selector_scope_digest: str
    declared_revision: str | None
    workspace_state: ObservedWorkspaceStateView | None
    omission_manifest: tuple[CodeInvestigationOmissionView, ...]
    omission_digest: str
    omission_count: int
    tooling: CodeInvestigationToolingView
    observed_at: datetime
    received_at: datetime
    expires_at: datetime
    observation_sha256: str
    payload_sha256: str
    idempotency_key: str


class CodeInvestigationHeadView(_DomainView):
    board_id: str
    source_ref: str
    generation: int
    latest_receipt_id: str
    current_receipt_id: str | None
    state: CodeInvestigationHeadState
    revision: int
    updated_at: datetime


class CodeEvidenceView(_DomainView):
    id: str
    board_id: str
    investigation_receipt_id: str
    source_ref: str
    parent_type: CodeTraceabilitySubjectType
    parent_id: str
    parent_version: int
    evidence_type: CodeEvidenceType
    claim: str | None = None
    workspace_state: ObservedWorkspaceStateView | None = None
    selector_kind: CodeEvidenceSelectorKind
    relative_path: str | None
    language: str | None
    symbol_kind: str | None
    qualified_symbol: str | None
    symbol_signature: str | None
    snapshot_line_start: int | None
    snapshot_line_end: int | None
    excerpt: str | None
    excerpt_sha256: str | None
    excerpt_truncated: bool | None = None
    declared_file_blob_sha256: str | None
    declared_source_content_sha256: str | None = None
    excerpt_omitted_reason: str | None
    attestation_state: CodeEvidenceAttestationState
    attestation_basis: CodeEvidenceAttestationBasis
    lifecycle_status: CodeTraceabilityLifecycleStatus
    supersedes_evidence_id: str | None
    revocation_reason: str | None
    submitted_by: str | None = None
    received_at: datetime | None = None
    payload_sha256: str | None = None

    @classmethod
    def project(
        cls,
        evidence: CodeEvidence,
        *,
        profile: CodeTraceabilityProjectionProfile,
        context_scope: CodeTraceabilityContextScope = (
            CodeTraceabilityContextScope.DEFAULT
        ),
    ) -> Self:
        payload = {
            name: getattr(evidence, name)
            for name in cls.model_fields
            if name not in {"excerpt", "excerpt_truncated"}
        }
        excerpt = evidence.excerpt
        truncated: bool | None = False
        if (
            profile is CodeTraceabilityProjectionProfile.SUMMARY
            or context_scope is CodeTraceabilityContextScope.GATE
        ):
            excerpt = None
            truncated = None
        if profile is CodeTraceabilityProjectionProfile.SUMMARY:
            for field_name in (
                "claim",
                "workspace_state",
                "symbol_signature",
                "snapshot_line_start",
                "snapshot_line_end",
                "excerpt_sha256",
                "declared_file_blob_sha256",
                "declared_source_content_sha256",
                "excerpt_omitted_reason",
                "revocation_reason",
                "submitted_by",
                "received_at",
                "payload_sha256",
            ):
                payload[field_name] = None
        elif profile is CodeTraceabilityProjectionProfile.DETAIL:
            payload["payload_sha256"] = None
            if (
                excerpt is not None
                and len(excerpt.encode("utf-8"))
                > DEFAULT_CODE_TRACEABILITY_LIMITS.evidence_detail_excerpt_bytes
            ):
                encoded = excerpt.encode("utf-8")[
                    : DEFAULT_CODE_TRACEABILITY_LIMITS.evidence_detail_excerpt_bytes
                ]
                excerpt = encoded.decode("utf-8", errors="ignore")
                truncated = True
        payload["excerpt"] = excerpt
        payload["excerpt_truncated"] = truncated
        return cls.model_validate(payload)


class CodeEvidenceSpecLinkView(_DomainView):
    id: str
    board_id: str
    spec_id: str
    evidence_id: str
    entity_type: SpecEntityType
    entity_id: str
    relation_type: CodeEvidenceSpecRelationType
    rationale: str
    evidence_content_sha256: str
    source_refinement_version: int | None
    spec_version: int
    created_by: str
    created_at: datetime


class CodeEvidenceDispositionView(_DomainView):
    id: str
    board_id: str
    spec_id: str
    evidence_id: str
    disposition: CodeEvidenceDispositionKind
    justification: str
    spec_version: int
    active: bool
    created_by: str
    created_at: datetime
    cleared_by: str | None
    cleared_at: datetime | None


class ImplementationTargetView(_DomainView):
    id: str
    board_id: str
    card_id: str
    source_ref: str
    selector_kind: ImplementationTargetSelectorKind
    relative_path_hint: str | None
    language: str | None
    symbol_kind: str | None
    qualified_symbol: str | None
    symbol_signature: str | None
    role: ImplementationTargetRole
    intent: str
    required: bool
    source_spec_version: int
    baseline_evidence_id: str | None
    lifecycle_status: CodeTraceabilityLifecycleStatus
    revision: int
    current_resolution_id: str | None
    last_change_reason_sha256: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime


class ResolutionCandidateView(_DomainView):
    relative_path: str
    qualified_symbol: str | None
    symbol_signature: str | None
    symbol_fingerprint: str | None
    confidence: float
    reason_code: str


class ImplementationTargetResolutionView(_DomainView):
    id: str
    board_id: str
    target_id: str
    investigation_receipt_id: str
    source_ref: str
    receipt_generation: int
    subject_version: int
    target_revision: int
    workspace_state: ObservedWorkspaceStateView
    state: ImplementationTargetResolutionState
    resolved_relative_path: str | None
    resolved_language: str | None
    resolved_symbol_kind: str | None
    resolved_qualified_symbol: str | None
    resolved_symbol_signature: str | None
    resolved_line_start: int | None
    resolved_line_end: int | None
    symbol_fingerprint: str | None
    declared_file_blob_sha256: str | None
    selector_fingerprint: str
    confidence: float | None
    reason_code: str | None
    candidate_count: int
    candidates: tuple[ResolutionCandidateView, ...]
    declared_tool_id: str
    declared_tool_version: str
    submitted_by: str
    agent_observed_at: datetime
    received_at: datetime
    payload_sha256: str
    idempotency_key: str


class ImplementationTargetExecutionRecordView(_DomainView):
    id: str
    board_id: str
    card_id: str
    target_id: str
    target_revision: int
    result_investigation_receipt_id: str
    disposition: ImplementationTargetExecutionDisposition
    source_ref: str
    result_declared_revision: str | None
    result_workspace_state_id: str | None
    actual_relative_path: str | None
    actual_qualified_symbol: str | None
    replacement_target_id: str | None
    justification: str
    submitted_by: str
    received_at: datetime
    payload_sha256: str
    idempotency_key: str


class TargetOverlapAcknowledgementView(_DomainView):
    id: str
    board_id: str
    target_a_id: str
    target_b_id: str
    resolution_a_id: str
    resolution_b_id: str
    disposition: TargetOverlapDisposition
    justification: str
    created_by: str
    created_at: datetime


class CodeTraceabilityWaiverView(_DomainView):
    id: str
    board_id: str
    entity_type: CodeTraceabilityWaiverEntityType
    entity_id: str
    scope: CodeTraceabilityWaiverScope
    reason_code: CodeTraceabilityWaiverReason
    justification: str
    active: bool
    created_by: str
    created_at: datetime
    cleared_by: str | None
    cleared_at: datetime | None


# Import assertions intentionally keep public model/domain drift visible during
# module import without re-exporting server-owned persistence implementation.
_PUBLIC_DOMAIN_RECORD_TYPES = (
    CodeInvestigationRequest,
    CodeInvestigationReceipt,
    CodeInvestigationHead,
    CodeInvestigationOmission,
    CodeInvestigationTooling,
    CodeEvidence,
    CodeEvidenceSpecLink,
    CodeEvidenceDisposition,
    ImplementationTarget,
    ResolutionCandidate,
    ImplementationTargetResolution,
    ImplementationTargetExecutionRecord,
    TargetOverlapAcknowledgement,
    CodeTraceabilityWaiver,
)
