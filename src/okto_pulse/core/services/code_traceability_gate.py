"""Pure Code Traceability policy evaluation over persisted Pulse records.

This module deliberately has no repository, filesystem, Git, provider, or
language-resolver dependency.  The authenticated external agent performs the
investigation; Core only evaluates the structured attestations that were
accepted and materialized by a :class:`CodeTraceabilityReadPort`.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import re
from typing import Any, Callable, Mapping

from pydantic import ValidationError

from okto_pulse.core.domain.code_traceability import (
    DEFAULT_CODE_TRACEABILITY_LIMITS,
    CodeEvidence,
    CodeEvidenceDispositionKind,
    CodeTraceabilityEnforcement,
    CodeInvestigationOutcome,
    CodeInvestigationReceipt,
    CodeInvestigationReceiptCurrentness,
    CodeInvestigationTrustLevel,
    CodeTraceabilityContext,
    CodeTraceabilityContextScope,
    CodeTraceabilityContractError,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilityRemediation,
    CodeTraceabilityProjectionProfile,
    CodeTraceabilitySubjectType,
    CodeTraceabilityWaiver,
    CodeTraceabilityWaiverEntityType,
    CodeTraceabilityWaiverScope,
    ImplementationTargetResolutionState,
    ImplementationTargetRole,
    TargetOverlapSeverity,
    WorkspaceReproducibilityClaim,
    code_investigation_receipt_currentness,
)
from okto_pulse.core.models.schemas import BoardSettings, CodeTraceabilitySettings
from okto_pulse.core.ports.code_traceability import (
    CodeTraceabilityProjectionQuery,
    CodeTraceabilityReadPort,
)
from okto_pulse.core.services.code_investigation import (
    effective_required_capabilities_for_subject,
)


Clock = Callable[[], datetime]

_EVIDENCE_REFERENCE_RE = re.compile(
    r"(?<![A-Za-z0-9._-])evidence:([A-Za-z0-9][A-Za-z0-9._-]{0,127})"
    r"(?![A-Za-z0-9._-])"
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class CodeTraceabilityGatePhase(str, Enum):
    REFINEMENT_EVIDENCE = "refinement_evidence"
    SPEC_EVIDENCE_DISPOSITION = "spec_evidence_disposition"
    CARD_TARGET = "card_target"
    CARD_EXECUTION = "card_execution"


@dataclass(frozen=True, slots=True)
class CodeTraceabilityGateBlocker:
    code: str
    message: str
    blocking: bool
    details: Mapping[str, object]
    remediation: tuple[CodeTraceabilityRemediation, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "message": self.message,
            "blocking": self.blocking,
            "details": dict(self.details),
            "remediation": [item.as_dict() for item in self.remediation],
        }


@dataclass(frozen=True, slots=True)
class EvidenceDispositionCoverage:
    total: int
    linked: int
    dispositioned: int
    pending_ids: tuple[str, ...]

    @property
    def coverage_pct(self) -> float:
        if self.total == 0:
            return 100.0
        return round(((self.linked + self.dispositioned) / self.total) * 100, 2)

    def as_dict(self) -> dict[str, object]:
        return {
            "total": self.total,
            "linked": self.linked,
            "dispositioned": self.dispositioned,
            "pending": len(self.pending_ids),
            "pending_ids": list(self.pending_ids),
            "coverage_pct": self.coverage_pct,
        }


@dataclass(frozen=True, slots=True)
class TargetEntityCoverage:
    total: int
    covered: int
    pending_entity_ids: tuple[str, ...]

    @property
    def coverage_pct(self) -> float:
        if self.total == 0:
            return 100.0
        return round((self.covered / self.total) * 100, 2)

    def as_dict(self) -> dict[str, object]:
        return {
            "total": self.total,
            "covered": self.covered,
            "pending": len(self.pending_entity_ids),
            "pending_entity_ids": list(self.pending_entity_ids),
            "coverage_pct": self.coverage_pct,
        }


@dataclass(frozen=True, slots=True)
class CodeTraceabilityGateEvaluation:
    mode: CodeTraceabilityEnforcement
    phases: tuple[CodeTraceabilityGatePhase, ...]
    allowed: bool
    passed: bool
    blockers: tuple[CodeTraceabilityGateBlocker, ...]
    evidence_coverage: EvidenceDispositionCoverage
    target_coverage: TargetEntityCoverage
    receipt_currentness: Mapping[str, str]
    resolution_freshness: Mapping[str, Mapping[str, object]]
    evidence_coverage_skipped: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode.value,
            "phases": [item.value for item in self.phases],
            "allowed": self.allowed,
            "passed": self.passed,
            "blockers": [item.as_dict() for item in self.blockers],
            "evidence_disposition_coverage": self.evidence_coverage.as_dict(),
            "evidence_coverage_skipped": self.evidence_coverage_skipped,
            "target_entity_coverage": self.target_coverage.as_dict(),
            "receipt_currentness": dict(self.receipt_currentness),
            "resolution_freshness": {
                key: dict(value) for key, value in self.resolution_freshness.items()
            },
        }


@dataclass(frozen=True, slots=True)
class CodeTraceabilityProjection:
    context: CodeTraceabilityContext
    inherited_evidence: tuple[CodeEvidence, ...]
    direct_evidence: tuple[CodeEvidence, ...]
    referenced_evidence_ids: tuple[str, ...]
    gate_readiness: CodeTraceabilityGateEvaluation

    def as_dict(self) -> dict[str, object]:
        context = self.context
        result = self._base_payload()
        if context.context_scope is CodeTraceabilityContextScope.GATE:
            result.update(self._gate_payload())
        elif context.profile is CodeTraceabilityProjectionProfile.SUMMARY:
            result.update(self._summary_payload())
        elif context.profile is CodeTraceabilityProjectionProfile.DETAIL:
            result.update(self._detail_payload())
        else:
            result.update(self._full_payload())
        result["coverage"] = {
            **self.gate_readiness.evidence_coverage.as_dict(),
            "skipped": self.gate_readiness.evidence_coverage_skipped,
        }
        result["target_coverage"] = self.gate_readiness.target_coverage.as_dict()
        result["resolution_freshness"] = dict(self.gate_readiness.resolution_freshness)
        return result

    def _base_payload(self) -> dict[str, object]:
        context = self.context
        return {
            "subject_type": context.subject_type.value,
            "subject_id": context.subject_id,
            "subject_version": context.subject_version,
            "profile": context.profile.value,
            "context_scope": context.context_scope.value,
            "source_refinement_id": context.source_refinement_id,
            "source_refinement_snapshot_id": (context.source_refinement_snapshot_id),
            "source_refinement_version": context.source_refinement_version,
            "referenced_evidence_ids": list(self.referenced_evidence_ids),
            "gate_readiness": self.gate_readiness.as_dict(),
        }

    def _summary_payload(self) -> dict[str, object]:
        context = self.context
        return {
            "heads": [
                _selected_public_value(
                    item,
                    "source_ref",
                    "generation",
                    "current_receipt_id",
                    "state",
                    "revision",
                )
                for item in context.heads
            ],
            "evidence": [_summary_evidence(item) for item in context.evidence],
            "inherited_evidence_ids": [item.id for item in self.inherited_evidence],
            "direct_evidence_ids": [item.id for item in self.direct_evidence],
            "links": [
                _selected_public_value(
                    item,
                    "id",
                    "evidence_id",
                    "spec_id",
                    "entity_type",
                    "entity_id",
                    "relation_type",
                )
                for item in context.evidence_links
            ],
            "dispositions": [
                _selected_public_value(
                    item,
                    "id",
                    "evidence_id",
                    "disposition",
                    "active",
                )
                for item in context.evidence_dispositions
            ],
            "targets": [_summary_target(item) for item in context.targets],
            "target_spec_links": [
                _selected_public_value(
                    item,
                    "id",
                    "target_id",
                    "spec_id",
                    "entity_type",
                    "entity_id",
                )
                for item in context.target_spec_links
            ],
            "target_evidence_links": [
                _selected_public_value(
                    item,
                    "id",
                    "target_id",
                    "evidence_id",
                    "relation_type",
                )
                for item in context.target_evidence_links
            ],
            "resolutions": [_summary_resolution(item) for item in context.resolutions],
            "overlaps": [_summary_overlap(item) for item in context.overlaps],
            "waivers": [
                _selected_public_value(
                    item,
                    "id",
                    "entity_type",
                    "entity_id",
                    "scope",
                    "reason_code",
                    "active",
                )
                for item in context.waivers
            ],
            "counts": _projection_counts(context),
        }

    def _detail_payload(self) -> dict[str, object]:
        context = self.context
        return {
            "heads": [
                _selected_public_value(
                    item,
                    "source_ref",
                    "generation",
                    "current_receipt_id",
                    "state",
                    "revision",
                )
                for item in context.heads
            ],
            "evidence": [_detail_evidence(item) for item in context.evidence],
            "inherited_evidence_ids": [item.id for item in self.inherited_evidence],
            "direct_evidence_ids": [item.id for item in self.direct_evidence],
            "links": [_public_value(item) for item in context.evidence_links],
            "dispositions": [
                _public_value(item) for item in context.evidence_dispositions
            ],
            "targets": [_detail_target(item) for item in context.targets],
            "target_spec_links": [
                _public_value(item) for item in context.target_spec_links
            ],
            "target_evidence_links": [
                _public_value(item) for item in context.target_evidence_links
            ],
            "resolutions": [_detail_resolution(item) for item in context.resolutions],
            "executions": [_detail_execution(item) for item in context.executions],
            "overlaps": [_public_value(item) for item in context.overlaps],
            "waivers": [_public_value(item) for item in context.waivers],
            "counts": _projection_counts(context),
        }

    def _gate_payload(self) -> dict[str, object]:
        context = self.context
        current_receipt_ids = {
            item.current_receipt_id
            for item in context.heads
            if item.current_receipt_id is not None
        }
        return {
            "omitted_content_manifest": [
                _public_value(item) for item in context.omitted_content_manifest
            ],
            "heads": [_public_value(item) for item in context.heads],
            "current_receipts": [
                _gate_receipt(item)
                for item in context.receipts
                if item.id in current_receipt_ids
            ],
            "receipt_revocations": [
                _public_value(item) for item in context.receipt_revocations
            ],
            "evidence": [_gate_evidence(item) for item in context.evidence],
            "inherited_evidence_ids": [item.id for item in self.inherited_evidence],
            "direct_evidence_ids": [item.id for item in self.direct_evidence],
            "links": [_public_value(item) for item in context.evidence_links],
            "dispositions": [
                _public_value(item) for item in context.evidence_dispositions
            ],
            "targets": [_summary_target(item) for item in context.targets],
            "target_spec_links": [
                _public_value(item) for item in context.target_spec_links
            ],
            "target_evidence_links": [
                _public_value(item) for item in context.target_evidence_links
            ],
            "resolutions": [_gate_resolution(item) for item in context.resolutions],
            "executions": [
                _selected_public_value(
                    item,
                    "id",
                    "card_id",
                    "target_id",
                    "target_revision",
                    "result_investigation_receipt_id",
                    "disposition",
                    "source_ref",
                )
                for item in context.executions
            ],
            "overlaps": [_public_value(item) for item in context.overlaps],
            "waivers": [_public_value(item) for item in context.waivers],
            "counts": _projection_counts(context),
        }

    def _full_payload(self) -> dict[str, object]:
        context = self.context
        return {
            "heads": [_public_value(item) for item in context.heads],
            "receipts": [_public_value(item) for item in context.receipts],
            "receipt_revocations": [
                _public_value(item) for item in context.receipt_revocations
            ],
            "evidence": [_public_value(item) for item in context.evidence],
            "inherited_evidence": [
                _public_value(item) for item in self.inherited_evidence
            ],
            "direct_evidence": [_public_value(item) for item in self.direct_evidence],
            "referenced_evidence_ids": list(self.referenced_evidence_ids),
            "links": [_public_value(item) for item in context.evidence_links],
            "dispositions": [
                _public_value(item) for item in context.evidence_dispositions
            ],
            "targets": [_public_value(item) for item in context.targets],
            "target_spec_links": [
                _public_value(item) for item in context.target_spec_links
            ],
            "target_evidence_links": [
                _public_value(item) for item in context.target_evidence_links
            ],
            "resolutions": [_public_value(item) for item in context.resolutions],
            "executions": [_public_value(item) for item in context.executions],
            "overlaps": [_public_value(item) for item in context.overlaps],
            "waivers": [_public_value(item) for item in context.waivers],
            "counts": _projection_counts(context),
        }


def _public_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if is_dataclass(value):
        return {
            item.name: _public_value(getattr(value, item.name))
            for item in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): _public_value(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_public_value(item) for item in value]
    return value


def _selected_public_value(value: Any, *names: str) -> dict[str, Any]:
    projected = _public_value(value)
    return {name: projected[name] for name in names if name in projected}


def _summary_evidence(value: CodeEvidence) -> dict[str, Any]:
    projected = _selected_public_value(
        value,
        "id",
        "source_ref",
        "parent_type",
        "parent_id",
        "parent_version",
        "evidence_type",
        "selector_kind",
        "relative_path",
        "language",
        "symbol_kind",
        "qualified_symbol",
        "attestation_state",
        "lifecycle_status",
        "supersedes_evidence_id",
    )
    projected["content_sha256"] = value.content_sha256
    return projected


def _summary_target(value: Any) -> dict[str, Any]:
    return _selected_public_value(
        value,
        "id",
        "card_id",
        "source_ref",
        "selector_kind",
        "relative_path_hint",
        "language",
        "symbol_kind",
        "qualified_symbol",
        "role",
        "required",
        "source_spec_version",
        "lifecycle_status",
        "revision",
        "current_resolution_id",
    )


def _summary_resolution(value: Any) -> dict[str, Any]:
    return _selected_public_value(
        value,
        "id",
        "target_id",
        "investigation_receipt_id",
        "source_ref",
        "subject_version",
        "target_revision",
        "state",
        "resolved_relative_path",
        "resolved_language",
        "resolved_symbol_kind",
        "resolved_qualified_symbol",
        "confidence",
        "reason_code",
        "candidate_count",
    )


def _summary_overlap(value: Any) -> dict[str, Any]:
    return _selected_public_value(
        value,
        "target_a_id",
        "target_b_id",
        "resolution_a_id",
        "resolution_b_id",
        "severity",
        "reason_code",
        "relative_path",
        "qualified_symbol",
    )


def _utf8_preview(value: str, max_bytes: int) -> tuple[str, bool]:
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value, False
    return encoded[:max_bytes].decode("utf-8", errors="ignore"), True


def _detail_evidence(value: CodeEvidence) -> dict[str, Any]:
    projected = _public_value(value)
    excerpt = value.excerpt
    truncated = False
    if excerpt is not None:
        excerpt, truncated = _utf8_preview(
            excerpt,
            DEFAULT_CODE_TRACEABILITY_LIMITS.evidence_detail_excerpt_bytes,
        )
    projected["excerpt"] = excerpt
    projected["excerpt_truncated"] = truncated
    projected.pop("payload_sha256", None)
    projected.pop("idempotency_key", None)
    return projected


def _detail_target(value: Any) -> dict[str, Any]:
    projected = _public_value(value)
    projected.pop("last_change_reason_sha256", None)
    return projected


def _detail_resolution(value: Any) -> dict[str, Any]:
    projected = _public_value(value)
    projected["candidates"] = [
        {
            key: candidate[key]
            for key in (
                "relative_path",
                "qualified_symbol",
                "confidence",
                "reason_code",
            )
            if key in candidate
        }
        for candidate in projected.get("candidates", [])[:5]
    ]
    projected.pop("payload_sha256", None)
    projected.pop("idempotency_key", None)
    return projected


def _detail_execution(value: Any) -> dict[str, Any]:
    """Project the bounded execution receipt without replay-only identifiers."""

    projected = _public_value(value)
    projected.pop("payload_sha256", None)
    projected.pop("idempotency_key", None)
    return projected


def _gate_receipt(value: Any) -> dict[str, Any]:
    return _selected_public_value(
        value,
        "id",
        "subject_type",
        "subject_id",
        "subject_version",
        "generation",
        "trust_level",
        "acceptance_status",
        "outcome",
        "capabilities",
        "source_ref",
        "selector_scope_digest",
        "workspace_state",
        "omission_manifest",
        "omission_digest",
        "omission_count",
        "observed_at",
        "received_at",
        "expires_at",
    )


def _gate_evidence(value: CodeEvidence) -> dict[str, Any]:
    projected = _summary_evidence(value)
    projected.update(
        {
            "investigation_receipt_id": value.investigation_receipt_id,
            "claim": value.claim,
            "workspace_state": _public_value(value.workspace_state),
            "declared_file_blob_sha256": value.declared_file_blob_sha256,
            "declared_source_content_sha256": value.declared_source_content_sha256,
        }
    )
    return projected


def _gate_resolution(value: Any) -> dict[str, Any]:
    projected = _summary_resolution(value)
    full = _public_value(value)
    projected.update(
        {
            "workspace_state": full.get("workspace_state"),
            "selector_fingerprint": full.get("selector_fingerprint"),
            "symbol_fingerprint": full.get("symbol_fingerprint"),
            "declared_file_blob_sha256": full.get("declared_file_blob_sha256"),
        }
    )
    return projected


def _projection_counts(context: CodeTraceabilityContext) -> dict[str, int]:
    return {
        "heads": len(context.heads),
        "receipts": len(context.receipts),
        "evidence": len(context.evidence),
        "links": len(context.evidence_links),
        "dispositions": len(context.evidence_dispositions),
        "targets": len(context.targets),
        "resolutions": len(context.resolutions),
        "executions": len(context.executions),
        "overlaps": len(context.overlaps),
        "waivers": len(context.waivers),
    }


_REMEDIATIONS: dict[str, tuple[CodeTraceabilityRemediation, ...]] = {
    "code_traceability_projection_incomplete": (
        CodeTraceabilityRemediation(
            "request_a_narrower_gate_context_or_reduce_traceability_scope"
        ),
    ),
    "code_evidence_attestation_required": (
        CodeTraceabilityRemediation(
            "run_agent_preflight_and_submit_evidence",
            "okto_pulse_start_code_investigation",
        ),
        CodeTraceabilityRemediation(
            "mark_not_applicable",
            "okto_pulse_mark_code_traceability_not_applicable",
        ),
    ),
    "code_evidence_disposition_required": (
        CodeTraceabilityRemediation(
            "link_evidence_or_set_final_disposition",
            "okto_pulse_link_code_evidence",
        ),
    ),
    "implementation_target_invalid": (
        CodeTraceabilityRemediation(
            "create_agent_investigated_implementation_target",
            "okto_pulse_create_implementation_target",
        ),
    ),
    "implementation_target_resolution_required": (
        CodeTraceabilityRemediation(
            "run_agent_preflight_and_submit_resolution",
            "okto_pulse_submit_implementation_target_resolution",
        ),
    ),
    "implementation_overlap_blocking": (
        CodeTraceabilityRemediation(
            "create_dependency_or_acknowledge_overlap",
            "okto_pulse_acknowledge_implementation_overlap",
        ),
    ),
    "target_execution_disposition_required": (
        CodeTraceabilityRemediation(
            "submit_agent_execution_receipt",
            "okto_pulse_submit_implementation_target_execution_receipt",
        ),
    ),
}


def _waiver_matches(
    waiver: CodeTraceabilityWaiver,
    *,
    entity_type: CodeTraceabilityWaiverEntityType,
    entity_id: str,
    scope: CodeTraceabilityWaiverScope,
) -> bool:
    return (
        waiver.active
        and waiver.entity_type is entity_type
        and waiver.entity_id == entity_id
        and waiver.scope is scope
    )


def _subject_waiver_type(
    subject_type: CodeTraceabilitySubjectType,
) -> CodeTraceabilityWaiverEntityType:
    return CodeTraceabilityWaiverEntityType(subject_type.value)


def _spec_entity_waiver_id(spec_id: str, entity_type: str, entity_id: str) -> str:
    return f"spec:{spec_id}:{entity_type}:{entity_id}"


def phases_for_transition(
    subject_type: CodeTraceabilitySubjectType,
    from_status: str,
    to_status: str,
) -> tuple[CodeTraceabilityGatePhase, ...]:
    """Return the Code Traceability gates bound to one canonical SDLC edge."""

    source = from_status.strip().lower()
    target = to_status.strip().lower()
    if subject_type is CodeTraceabilitySubjectType.REFINEMENT and (
        (source, target) in {("review", "approved"), ("approved", "done")}
    ):
        return (CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,)
    if subject_type is CodeTraceabilitySubjectType.SPEC and (
        (source, target) in {("draft", "review"), ("review", "approved")}
    ):
        return (CodeTraceabilityGatePhase.SPEC_EVIDENCE_DISPOSITION,)
    if subject_type is CodeTraceabilitySubjectType.CARD:
        if target in {"started", "in_progress"} and source != target:
            return (CodeTraceabilityGatePhase.CARD_TARGET,)
        if target in {"validation", "done"} and source != target:
            return (CodeTraceabilityGatePhase.CARD_EXECUTION,)
    return ()


def extract_code_evidence_references(value: object) -> tuple[str, ...]:
    """Extract the closed ``evidence:<id>`` citation form from Core prose."""

    if value is None:
        return ()
    if not isinstance(value, str):
        raise CodeTraceabilityContractError("code_evidence_reference_source_invalid")
    return tuple(sorted(set(_EVIDENCE_REFERENCE_RE.findall(value))))


def resolve_code_traceability_settings(
    raw_board_settings: object,
) -> CodeTraceabilitySettings:
    """Resolve known legacy persistence as Advisory and reject other drift."""

    if raw_board_settings is None:
        return CodeTraceabilitySettings()
    if isinstance(raw_board_settings, CodeTraceabilitySettings):
        return raw_board_settings
    if isinstance(raw_board_settings, BoardSettings):
        return raw_board_settings.code_traceability
    if not isinstance(raw_board_settings, Mapping):
        raise CodeTraceabilityContractError(
            "code_traceability_settings_invalid",
            details={"reason": "board_settings_not_mapping"},
        )
    try:
        settings = CodeTraceabilitySettings.from_persisted(
            raw_board_settings.get("code_traceability")
        )
    except ValidationError as exc:
        raise CodeTraceabilityContractError(
            "code_traceability_settings_invalid",
            details={"reason": "board_policy_invalid"},
        ) from exc
    return settings


class CodeTraceabilityGateEvaluator:
    """Evaluate gates from an immutable relational projection only."""

    def __init__(self, *, clock: Clock = _utc_now) -> None:
        self._clock = clock

    def evaluate_transition(
        self,
        context: CodeTraceabilityContext,
        settings: CodeTraceabilitySettings,
        *,
        from_status: str,
        to_status: str,
        card_type: str = "normal",
        dependency_card_ids: tuple[str, ...] = (),
        blocking_card_ids: tuple[str, ...] = (),
        referenced_evidence_ids: tuple[str, ...] = (),
        skip_evidence_coverage: bool = False,
    ) -> CodeTraceabilityGateEvaluation:
        phases = phases_for_transition(
            context.subject_type,
            from_status,
            to_status,
        )
        return self.evaluate(
            context,
            settings,
            phases=phases,
            card_type=card_type,
            dependency_card_ids=dependency_card_ids,
            blocking_card_ids=blocking_card_ids,
            referenced_evidence_ids=referenced_evidence_ids,
            skip_evidence_coverage=skip_evidence_coverage,
        )

    def evaluate(
        self,
        context: CodeTraceabilityContext,
        settings: CodeTraceabilitySettings,
        *,
        phases: tuple[CodeTraceabilityGatePhase, ...] | None = None,
        card_type: str = "normal",
        dependency_card_ids: tuple[str, ...] = (),
        blocking_card_ids: tuple[str, ...] = (),
        referenced_evidence_ids: tuple[str, ...] = (),
        skip_evidence_coverage: bool = False,
    ) -> CodeTraceabilityGateEvaluation:
        if not isinstance(context, CodeTraceabilityContext):
            raise CodeTraceabilityContractError("code_traceability_context_invalid")
        if not isinstance(settings, CodeTraceabilitySettings):
            raise CodeTraceabilityContractError("code_traceability_settings_invalid")
        selected = phases
        if selected is None:
            selected = {
                CodeTraceabilitySubjectType.REFINEMENT: (
                    CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,
                ),
                CodeTraceabilitySubjectType.SPEC: (
                    CodeTraceabilityGatePhase.SPEC_EVIDENCE_DISPOSITION,
                ),
                CodeTraceabilitySubjectType.CARD: (
                    CodeTraceabilityGatePhase.CARD_TARGET,
                    CodeTraceabilityGatePhase.CARD_EXECUTION,
                ),
            }[context.subject_type]
        selected = tuple(dict.fromkeys(selected))
        allowed_phases = {
            CodeTraceabilitySubjectType.REFINEMENT: {
                CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE
            },
            CodeTraceabilitySubjectType.SPEC: {
                CodeTraceabilityGatePhase.SPEC_EVIDENCE_DISPOSITION
            },
            CodeTraceabilitySubjectType.CARD: {
                CodeTraceabilityGatePhase.CARD_TARGET,
                CodeTraceabilityGatePhase.CARD_EXECUTION,
            },
        }[context.subject_type]
        if any(item not in allowed_phases for item in selected):
            raise CodeTraceabilityContractError(
                "code_traceability_gate_phase_subject_mismatch"
            )

        evidence_coverage = self._evidence_coverage(context)
        target_coverage = self._target_coverage(context)
        receipt_currentness: dict[str, str] = {}
        resolution_freshness: dict[str, Mapping[str, object]] = {}
        blockers: list[CodeTraceabilityGateBlocker] = []

        if (
            context.context_scope is CodeTraceabilityContextScope.GATE
            and context.omitted_content_manifest
        ):
            blockers.extend(
                self._blocker(
                    settings,
                    "code_traceability_projection_incomplete",
                    "Gate context exceeded a server-owned projection budget.",
                    collection=item.collection,
                    hard_limit=item.hard_limit,
                    omitted_at_least=item.omitted_at_least,
                    reason=item.reason_code,
                )
                for item in context.omitted_content_manifest
            )
        if CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE in selected:
            blockers.extend(
                self._refinement_evidence_blockers(
                    context,
                    settings,
                    receipt_currentness,
                    referenced_evidence_ids=referenced_evidence_ids,
                )
            )
        if CodeTraceabilityGatePhase.SPEC_EVIDENCE_DISPOSITION in selected:
            if not skip_evidence_coverage:
                blockers.extend(
                    self._spec_disposition_blockers(
                        context,
                        settings,
                        evidence_coverage,
                    )
                )
        if CodeTraceabilityGatePhase.CARD_TARGET in selected:
            blockers.extend(
                self._card_target_blockers(
                    context,
                    settings,
                    card_type=card_type,
                    dependency_card_ids=set(dependency_card_ids),
                    blocking_card_ids=set(blocking_card_ids),
                    target_coverage=target_coverage,
                    receipt_currentness=receipt_currentness,
                    resolution_freshness=resolution_freshness,
                )
            )
        if CodeTraceabilityGatePhase.CARD_EXECUTION in selected:
            blockers.extend(
                self._card_execution_blockers(
                    context,
                    settings,
                    receipt_currentness=receipt_currentness,
                )
            )

        blockers = sorted(
            {
                (
                    item.code,
                    tuple(
                        sorted((key, str(value)) for key, value in item.details.items())
                    ),
                ): item
                for item in blockers
            }.values(),
            key=lambda item: (
                item.code,
                str(item.details.get("evidence_id", "")),
                str(item.details.get("target_id", "")),
                str(item.details.get("entity_id", "")),
            ),
        )
        blocking = tuple(item for item in blockers if item.blocking)
        return CodeTraceabilityGateEvaluation(
            mode=settings.mode,
            phases=selected,
            allowed=not blocking,
            passed=not blockers,
            blockers=tuple(blockers),
            evidence_coverage=evidence_coverage,
            target_coverage=target_coverage,
            receipt_currentness=receipt_currentness,
            resolution_freshness=resolution_freshness,
            evidence_coverage_skipped=(
                skip_evidence_coverage
                and CodeTraceabilityGatePhase.SPEC_EVIDENCE_DISPOSITION in selected
            ),
        )

    def project(
        self,
        context: CodeTraceabilityContext,
        settings: CodeTraceabilitySettings,
        *,
        card_type: str = "normal",
        dependency_card_ids: tuple[str, ...] = (),
        blocking_card_ids: tuple[str, ...] = (),
        referenced_evidence_ids: tuple[str, ...] = (),
        skip_evidence_coverage: bool = False,
    ) -> CodeTraceabilityProjection:
        inherited = self._inherited_evidence(context)
        direct = tuple(
            item
            for item in context.evidence
            if item.lifecycle_status is CodeTraceabilityLifecycleStatus.ACTIVE
            and item not in inherited
        )
        return CodeTraceabilityProjection(
            context=context,
            inherited_evidence=inherited,
            direct_evidence=direct,
            referenced_evidence_ids=tuple(referenced_evidence_ids),
            gate_readiness=self.evaluate(
                context,
                settings,
                card_type=card_type,
                dependency_card_ids=dependency_card_ids,
                blocking_card_ids=blocking_card_ids,
                referenced_evidence_ids=referenced_evidence_ids,
                skip_evidence_coverage=skip_evidence_coverage,
            ),
        )

    def validate_or_raise(
        self,
        evaluation: CodeTraceabilityGateEvaluation,
    ) -> None:
        if evaluation.allowed:
            return
        blocking = [item for item in evaluation.blockers if item.blocking]
        first = blocking[0]
        raise CodeTraceabilityContractError(
            first.code,
            first.message,
            details={
                **dict(first.details),
                "gate_phases": [item.value for item in evaluation.phases],
                "blockers": [item.as_dict() for item in blocking],
            },
            remediation=first.remediation,
        )

    def _blocker(
        self,
        settings: CodeTraceabilitySettings,
        code: str,
        message: str,
        *,
        enforce: bool = True,
        **details: object,
    ) -> CodeTraceabilityGateBlocker:
        return CodeTraceabilityGateBlocker(
            code=code,
            message=message,
            blocking=(
                settings.mode is CodeTraceabilityEnforcement.BLOCKING and enforce
            ),
            details=details,
            remediation=_REMEDIATIONS.get(code, ()),
        )

    @staticmethod
    def _inherited_evidence(
        context: CodeTraceabilityContext,
    ) -> tuple[CodeEvidence, ...]:
        if context.source_refinement_id is None:
            return ()
        inherited_snapshot_version = context.source_refinement_version
        if inherited_snapshot_version is None:
            return ()
        return tuple(
            item
            for item in context.evidence
            if item.lifecycle_status is CodeTraceabilityLifecycleStatus.ACTIVE
            and item.parent_type is CodeTraceabilitySubjectType.REFINEMENT
            and item.parent_id == context.source_refinement_id
            # The reader seals membership from the immutable refinement snapshot.
            # A v4 snapshot may therefore inherit still-active Evidence authored
            # at v3; equality would silently discard that canonical member.
            and item.parent_version <= inherited_snapshot_version
        )

    def _evidence_coverage(
        self,
        context: CodeTraceabilityContext,
    ) -> EvidenceDispositionCoverage:
        inherited = self._inherited_evidence(context)
        evidence_by_id = {item.id: item for item in inherited}
        linked_ids = {
            item.evidence_id
            for item in context.evidence_links
            if item.evidence_id in evidence_by_id
            and item.evidence_content_sha256
            == evidence_by_id[item.evidence_id].content_sha256
        }
        dispositioned_ids = {
            item.evidence_id
            for item in context.evidence_dispositions
            if item.evidence_id in evidence_by_id
            and item.active
            and item.disposition
            in {
                CodeEvidenceDispositionKind.NOT_RELEVANT,
                CodeEvidenceDispositionKind.SUPERSEDED,
            }
            and item.evidence_id not in linked_ids
        }
        pending = tuple(sorted(set(evidence_by_id) - linked_ids - dispositioned_ids))
        return EvidenceDispositionCoverage(
            total=len(evidence_by_id),
            linked=len(linked_ids),
            dispositioned=len(dispositioned_ids),
            pending_ids=pending,
        )

    def _target_coverage(
        self, context: CodeTraceabilityContext
    ) -> TargetEntityCoverage:
        if context.subject_type is not CodeTraceabilitySubjectType.CARD:
            return TargetEntityCoverage(0, 0, ())
        active_targets = {
            item.id: item
            for item in context.targets
            if item.card_id == context.subject_id
            and item.lifecycle_status is CodeTraceabilityLifecycleStatus.ACTIVE
        }
        active_evidence_ids = {
            item.id
            for item in context.evidence
            if item.lifecycle_status is CodeTraceabilityLifecycleStatus.ACTIVE
        }
        relevant_links = tuple(
            item
            for item in context.evidence_links
            if item.evidence_id in active_evidence_ids
        )
        supported_entities = {
            (item.spec_id, item.entity_type.value, item.entity_id)
            for item in relevant_links
        }
        directly_covered = {
            (item.spec_id, item.entity_type.value, item.entity_id)
            for item in context.target_spec_links
            if item.target_id in active_targets
        }
        target_ids_by_evidence: dict[str, set[str]] = {}
        for item in context.target_evidence_links:
            if item.target_id in active_targets:
                target_ids_by_evidence.setdefault(item.evidence_id, set()).add(
                    item.target_id
                )
        evidence_covered = {
            (item.spec_id, item.entity_type.value, item.entity_id)
            for item in relevant_links
            if target_ids_by_evidence.get(item.evidence_id)
        }
        waived = {
            entity
            for entity in supported_entities
            if any(
                _waiver_matches(
                    item,
                    entity_type=CodeTraceabilityWaiverEntityType.SPEC_ENTITY,
                    entity_id=_spec_entity_waiver_id(*entity),
                    scope=CodeTraceabilityWaiverScope.IMPLEMENTATION_TARGET,
                )
                for item in context.waivers
            )
        }
        covered_entities = directly_covered | evidence_covered | waived
        pending = tuple(
            f"{spec_id}:{entity_type}:{entity_id}"
            for spec_id, entity_type, entity_id in sorted(
                supported_entities - covered_entities
            )
        )
        return TargetEntityCoverage(
            total=len(supported_entities),
            covered=len(supported_entities) - len(pending),
            pending_entity_ids=pending,
        )

    def _receipt_policy_status(
        self,
        context: CodeTraceabilityContext,
        settings: CodeTraceabilitySettings,
        receipt: CodeInvestigationReceipt,
        *,
        subject_type: CodeTraceabilitySubjectType,
        subject_id: str,
        subject_version: int,
        source_ref: str,
    ) -> tuple[str, str | None]:
        if (
            receipt.board_id != context.board_id
            or receipt.subject_type is not subject_type
            or receipt.subject_id != subject_id
            or receipt.subject_version != subject_version
            or receipt.source_ref != source_ref
        ):
            return "subject_mismatch", "code_evidence_receipt_mismatch"
        head = next(
            (item for item in context.heads if item.source_ref == source_ref),
            None,
        )
        revocation = next(
            (
                item
                for item in context.receipt_revocations
                if item.receipt_id == receipt.id
            ),
            None,
        )
        now = self._clock()
        currentness = code_investigation_receipt_currentness(
            receipt,
            head=head,
            at=now,
            revocation=revocation,
        )
        if currentness is not CodeInvestigationReceiptCurrentness.CURRENT:
            return currentness.value, "code_investigation_currentness_unknown"
        policy_expiry = receipt.received_at + timedelta(
            seconds=settings.preflight_freshness_seconds
        )
        if now >= policy_expiry:
            return "expired", "code_investigation_receipt_expired"
        if receipt.outcome is CodeInvestigationOutcome.UNAVAILABLE:
            return "unavailable", "code_investigation_unavailable"
        if any(
            item.affected_scope_digest == receipt.selector_scope_digest
            for item in receipt.omission_manifest
        ):
            return "blocking_omission", "code_investigation_unavailable"
        required = set(
            effective_required_capabilities_for_subject(
                subject_type,
                receipt_content=settings.receipt_content,
            )
        )
        if not required.issubset(receipt.capabilities):
            missing = sorted(
                item.value for item in required - set(receipt.capabilities)
            )
            return (
                f"capability_missing:{','.join(missing)}",
                "code_investigation_capability_missing",
            )
        if receipt.trust_level is CodeInvestigationTrustLevel.CONFLICTED:
            return "conflicted", "code_investigation_currentness_unknown"
        if (
            settings.minimum_trust == "corroborated"
            and receipt.trust_level is not CodeInvestigationTrustLevel.CORROBORATED
        ):
            return "trust_insufficient", "code_investigation_trust_insufficient"
        if settings.observed_state_policy == "require_committed_attestation" and (
            receipt.workspace_state is None
            or receipt.workspace_state.reproducibility_claim
            is not WorkspaceReproducibilityClaim.COMMITTED
            or receipt.workspace_state.declared_dirty
        ):
            return "committed_attestation_required", "code_investigation_unavailable"
        return "current", None

    def _refinement_evidence_blockers(
        self,
        context: CodeTraceabilityContext,
        settings: CodeTraceabilitySettings,
        receipt_currentness: dict[str, str],
        *,
        referenced_evidence_ids: tuple[str, ...],
    ) -> list[CodeTraceabilityGateBlocker]:
        enforce = settings.evidence_attestation == "required"
        active_evidence_ids = {
            item.id
            for item in context.evidence
            if item.lifecycle_status is CodeTraceabilityLifecycleStatus.ACTIVE
        }
        missing_references = tuple(
            sorted(set(referenced_evidence_ids) - active_evidence_ids)
        )
        blockers = (
            [
                self._blocker(
                    settings,
                    "code_evidence_attestation_required",
                    "Refinement analysis cites Evidence that is not active or does not exist.",
                    missing_evidence_ids=list(missing_references),
                    reason="analysis_evidence_reference_missing",
                )
            ]
            if missing_references
            else []
        )
        if any(
            _waiver_matches(
                item,
                entity_type=CodeTraceabilityWaiverEntityType.REFINEMENT,
                entity_id=context.subject_id,
                scope=CodeTraceabilityWaiverScope.CODE_EVIDENCE,
            )
            for item in context.waivers
        ):
            return blockers
        candidates = tuple(
            item
            for item in context.evidence
            if item.lifecycle_status is CodeTraceabilityLifecycleStatus.ACTIVE
            and item.parent_type is CodeTraceabilitySubjectType.REFINEMENT
            and item.parent_id == context.subject_id
            and item.parent_version == context.subject_version
        )
        receipt_by_id = {item.id: item for item in context.receipts}
        valid_count = 0
        invalid: list[tuple[CodeEvidence, str, str]] = []
        for evidence in candidates:
            receipt = receipt_by_id.get(evidence.investigation_receipt_id)
            if receipt is None:
                invalid.append(
                    (
                        evidence,
                        "unknown",
                        "code_investigation_currentness_unknown",
                    )
                )
                continue
            status, code = self._receipt_policy_status(
                context,
                settings,
                receipt,
                subject_type=CodeTraceabilitySubjectType.REFINEMENT,
                subject_id=context.subject_id,
                subject_version=context.subject_version,
                source_ref=evidence.source_ref,
            )
            receipt_currentness[receipt.id] = status
            if code is None and evidence.workspace_state == receipt.workspace_state:
                valid_count += 1
            else:
                invalid.append(
                    (
                        evidence,
                        status,
                        code or "code_evidence_receipt_mismatch",
                    )
                )
        if valid_count:
            return blockers
        details: dict[str, object] = {
            "refinement_id": context.subject_id,
            "refinement_version": context.subject_version,
            "active_evidence_count": len(candidates),
            "invalid_evidence_ids": [item.id for item, _, _ in invalid],
        }
        code = invalid[0][2] if invalid else "code_evidence_attestation_required"
        blockers.append(
            self._blocker(
                settings,
                code,
                "Current agent-attested Code Evidence or an explicit waiver is required.",
                enforce=enforce,
                **details,
            )
        )
        return blockers

    def _spec_disposition_blockers(
        self,
        context: CodeTraceabilityContext,
        settings: CodeTraceabilitySettings,
        coverage: EvidenceDispositionCoverage,
    ) -> list[CodeTraceabilityGateBlocker]:
        if not coverage.pending_ids:
            return []
        return [
            self._blocker(
                settings,
                "code_evidence_disposition_required",
                "Every active inherited Evidence needs a link or final disposition.",
                evidence_pending_ids=list(coverage.pending_ids),
                evidence_disposition_coverage_pct=coverage.coverage_pct,
                source_refinement_snapshot_id=(context.source_refinement_snapshot_id),
            )
        ]

    def _card_target_blockers(
        self,
        context: CodeTraceabilityContext,
        settings: CodeTraceabilitySettings,
        *,
        card_type: str,
        dependency_card_ids: set[str],
        blocking_card_ids: set[str],
        target_coverage: TargetEntityCoverage,
        receipt_currentness: dict[str, str],
        resolution_freshness: dict[str, Mapping[str, object]],
    ) -> list[CodeTraceabilityGateBlocker]:
        blockers: list[CodeTraceabilityGateBlocker] = []
        active_targets = tuple(
            item
            for item in context.targets
            if item.card_id == context.subject_id
            and item.lifecycle_status is CodeTraceabilityLifecycleStatus.ACTIVE
        )
        target_waiver = any(
            _waiver_matches(
                item,
                entity_type=CodeTraceabilityWaiverEntityType.CARD,
                entity_id=context.subject_id,
                scope=CodeTraceabilityWaiverScope.IMPLEMENTATION_TARGET,
            )
            for item in context.waivers
        )
        if not active_targets and not target_waiver:
            blockers.append(
                self._blocker(
                    settings,
                    "implementation_target_invalid",
                    "An active implementation target or explicit waiver is required.",
                    card_id=context.subject_id,
                    reason="active_target_required",
                )
            )
        if (
            card_type == "test"
            and active_targets
            and not target_waiver
            and not any(
                item.role
                in {ImplementationTargetRole.TEST, ImplementationTargetRole.VALIDATE}
                for item in active_targets
            )
        ):
            blockers.append(
                self._blocker(
                    settings,
                    "implementation_target_invalid",
                    "An automated test card needs a test or validate target.",
                    card_id=context.subject_id,
                    reason="automated_test_target_required",
                )
            )
        if target_coverage.pending_entity_ids and not target_waiver:
            blockers.append(
                self._blocker(
                    settings,
                    "code_traceability_waiver_required",
                    "Evidence-supported Spec entities need target coverage or a waiver.",
                    card_id=context.subject_id,
                    pending_entity_ids=list(target_coverage.pending_entity_ids),
                    target_entity_coverage_pct=target_coverage.coverage_pct,
                )
            )

        resolution_waiver = any(
            _waiver_matches(
                item,
                entity_type=CodeTraceabilityWaiverEntityType.CARD,
                entity_id=context.subject_id,
                scope=CodeTraceabilityWaiverScope.TARGET_RESOLUTION,
            )
            for item in context.waivers
        )
        resolution_by_id = {item.id: item for item in context.resolutions}
        receipt_by_id = {item.id: item for item in context.receipts}
        enforce_resolution = settings.target_resolution != "advisory"
        enforce_currentness = settings.target_resolution == "required_current_receipt"
        for target in active_targets:
            if not target.required or resolution_waiver:
                continue
            resolution = (
                resolution_by_id.get(target.current_resolution_id)
                if target.current_resolution_id
                else None
            )
            if resolution is None:
                resolution_freshness[target.id] = {
                    "state": "unresolved",
                    "currentness": "unknown",
                    "resolution_id": target.current_resolution_id,
                }
                blockers.append(
                    self._blocker(
                        settings,
                        "implementation_target_resolution_required",
                        "A required target has no current resolution.",
                        enforce=enforce_resolution,
                        target_id=target.id,
                    )
                )
                continue
            current_revision = (
                resolution.target_revision == target.revision
                and resolution.subject_version == context.subject_version
                and resolution.target_id == target.id
            )
            accepted_state = resolution.state in {
                ImplementationTargetResolutionState.RESOLVED,
                ImplementationTargetResolutionState.MOVED,
            } or (
                target.role is ImplementationTargetRole.CREATE
                and resolution.state is ImplementationTargetResolutionState.MISSING
                and resolution.reason_code == "missing_expected"
            )
            receipt = receipt_by_id.get(resolution.investigation_receipt_id)
            receipt_status = "unknown"
            receipt_code: str | None = "code_investigation_currentness_unknown"
            if receipt is not None:
                receipt_status, receipt_code = self._receipt_policy_status(
                    context,
                    settings,
                    receipt,
                    subject_type=CodeTraceabilitySubjectType.CARD,
                    subject_id=context.subject_id,
                    subject_version=context.subject_version,
                    source_ref=target.source_ref,
                )
                receipt_currentness[receipt.id] = receipt_status
            resolution_freshness[target.id] = {
                "state": resolution.state.value,
                "currentness": receipt_status,
                "resolution_id": resolution.id,
                "target_revision": resolution.target_revision,
            }
            if not current_revision:
                blockers.append(
                    self._blocker(
                        settings,
                        "implementation_target_resolution_outdated",
                        "The target resolution does not match the current Card/target version.",
                        enforce=enforce_resolution,
                        target_id=target.id,
                        resolution_id=resolution.id,
                    )
                )
            if not accepted_state:
                state_code = {
                    ImplementationTargetResolutionState.STALE: (
                        "implementation_target_stale"
                    ),
                    ImplementationTargetResolutionState.AMBIGUOUS: (
                        "implementation_target_ambiguous"
                    ),
                    ImplementationTargetResolutionState.MISSING: (
                        "implementation_target_missing"
                    ),
                    ImplementationTargetResolutionState.UNAVAILABLE: (
                        "code_investigation_unavailable"
                    ),
                }.get(
                    resolution.state,
                    "implementation_target_resolution_required",
                )
                blockers.append(
                    self._blocker(
                        settings,
                        state_code,
                        "The required target is not resolved to an executable coordinate.",
                        enforce=enforce_resolution,
                        target_id=target.id,
                        resolution_id=resolution.id,
                        resolution_state=resolution.state.value,
                    )
                )
            if receipt_code is not None:
                blockers.append(
                    self._blocker(
                        settings,
                        receipt_code,
                        "The target resolution receipt is not current and compatible.",
                        enforce=enforce_currentness,
                        target_id=target.id,
                        receipt_id=(None if receipt is None else receipt.id),
                        currentness=receipt_status,
                    )
                )

        if settings.overlap_policy == "block_parallel" and not any(
            _waiver_matches(
                item,
                entity_type=CodeTraceabilityWaiverEntityType.CARD,
                entity_id=context.subject_id,
                scope=CodeTraceabilityWaiverScope.TARGET_OVERLAP,
            )
            for item in context.waivers
        ):
            targets = {item.id: item for item in context.targets}
            active_ids = {item.id for item in active_targets}
            for overlap in context.overlaps:
                if overlap.severity is not TargetOverlapSeverity.HIGH:
                    continue
                own_ids = {overlap.target_a_id, overlap.target_b_id} & active_ids
                if not own_ids or overlap.acknowledgement is not None:
                    continue
                other_target_id = next(
                    iter({overlap.target_a_id, overlap.target_b_id} - own_ids),
                    None,
                )
                other = targets.get(other_target_id) if other_target_id else None
                if (
                    other is None
                    or other.card_id in dependency_card_ids
                    or other.card_id not in blocking_card_ids
                ):
                    continue
                blockers.append(
                    self._blocker(
                        settings,
                        "implementation_overlap_blocking",
                        "Another active Card has a high overlap on the current target.",
                        card_id=context.subject_id,
                        other_card_id=other.card_id,
                        target_ids=[overlap.target_a_id, overlap.target_b_id],
                        resolution_ids=[
                            overlap.resolution_a_id,
                            overlap.resolution_b_id,
                        ],
                    )
                )
        return blockers

    def _card_execution_blockers(
        self,
        context: CodeTraceabilityContext,
        settings: CodeTraceabilitySettings,
        *,
        receipt_currentness: dict[str, str],
    ) -> list[CodeTraceabilityGateBlocker]:
        blockers: list[CodeTraceabilityGateBlocker] = []
        active_targets = tuple(
            item
            for item in context.targets
            if item.card_id == context.subject_id
            and item.lifecycle_status is CodeTraceabilityLifecycleStatus.ACTIVE
        )
        receipt_by_id = {item.id: item for item in context.receipts}
        for target in active_targets:
            records = tuple(
                item
                for item in context.executions
                if item.target_id == target.id
                and item.target_revision == target.revision
                and item.card_id == context.subject_id
                and item.source_ref == target.source_ref
            )
            valid = False
            failed_receipt_id: str | None = None
            failed_status = "unknown"
            failed_code = "target_execution_disposition_required"
            for record in records:
                receipt = receipt_by_id.get(record.result_investigation_receipt_id)
                if receipt is None:
                    failed_receipt_id = record.result_investigation_receipt_id
                    continue
                status, code = self._receipt_policy_status(
                    context,
                    settings,
                    receipt,
                    subject_type=CodeTraceabilitySubjectType.CARD,
                    subject_id=context.subject_id,
                    subject_version=context.subject_version,
                    source_ref=target.source_ref,
                )
                receipt_currentness[receipt.id] = status
                if code is None:
                    valid = True
                    break
                failed_receipt_id = receipt.id
                failed_status = status
                failed_code = code
            if valid:
                continue
            blockers.append(
                self._blocker(
                    settings,
                    failed_code,
                    "Every active target needs a current agent execution receipt.",
                    target_id=target.id,
                    target_revision=target.revision,
                    receipt_id=failed_receipt_id,
                    currentness=failed_status,
                )
            )
        return blockers


class CodeTraceabilityProjectionService:
    """Load a bounded relational context and apply the pure Core evaluator."""

    def __init__(
        self,
        evaluator: CodeTraceabilityGateEvaluator | None = None,
    ) -> None:
        self._evaluator = evaluator or CodeTraceabilityGateEvaluator()

    async def load_context(
        self,
        query: CodeTraceabilityProjectionQuery,
        *,
        read_port: CodeTraceabilityReadPort,
    ) -> CodeTraceabilityContext:
        loaders = {
            CodeTraceabilitySubjectType.REFINEMENT: read_port.refinement_context,
            CodeTraceabilitySubjectType.SPEC: read_port.spec_context,
            CodeTraceabilitySubjectType.CARD: read_port.card_context,
        }
        context = await loaders[query.subject_type](query)
        if (
            not isinstance(context, CodeTraceabilityContext)
            or context.board_id != query.board_id
            or context.subject_type is not query.subject_type
            or context.subject_id != query.subject_id
            or context.subject_version != query.subject_version
            or context.profile is not query.profile
            or context.context_scope is not query.context_scope
        ):
            raise CodeTraceabilityContractError(
                "code_traceability_projection_context_mismatch"
            )
        return context

    async def project(
        self,
        query: CodeTraceabilityProjectionQuery,
        settings: CodeTraceabilitySettings,
        *,
        read_port: CodeTraceabilityReadPort,
        card_type: str = "normal",
        dependency_card_ids: tuple[str, ...] = (),
        blocking_card_ids: tuple[str, ...] = (),
        referenced_evidence_ids: tuple[str, ...] = (),
        skip_evidence_coverage: bool = False,
    ) -> CodeTraceabilityProjection:
        context = await self.load_context(query, read_port=read_port)
        return self._evaluator.project(
            context,
            settings,
            card_type=card_type,
            dependency_card_ids=dependency_card_ids,
            blocking_card_ids=blocking_card_ids,
            referenced_evidence_ids=referenced_evidence_ids,
            skip_evidence_coverage=skip_evidence_coverage,
        )

    def project_context(
        self,
        context: CodeTraceabilityContext,
        settings: CodeTraceabilitySettings,
        *,
        card_type: str = "normal",
        dependency_card_ids: tuple[str, ...] = (),
        blocking_card_ids: tuple[str, ...] = (),
        referenced_evidence_ids: tuple[str, ...] = (),
        skip_evidence_coverage: bool = False,
    ) -> CodeTraceabilityProjection:
        return self._evaluator.project(
            context,
            settings,
            card_type=card_type,
            dependency_card_ids=dependency_card_ids,
            blocking_card_ids=blocking_card_ids,
            referenced_evidence_ids=referenced_evidence_ids,
            skip_evidence_coverage=skip_evidence_coverage,
        )

    async def evaluate_transition(
        self,
        query: CodeTraceabilityProjectionQuery,
        settings: CodeTraceabilitySettings,
        *,
        read_port: CodeTraceabilityReadPort,
        from_status: str,
        to_status: str,
        card_type: str = "normal",
        dependency_card_ids: tuple[str, ...] = (),
        blocking_card_ids: tuple[str, ...] = (),
        referenced_evidence_ids: tuple[str, ...] = (),
        skip_evidence_coverage: bool = False,
    ) -> CodeTraceabilityGateEvaluation:
        context = await self.load_context(query, read_port=read_port)
        return self._evaluator.evaluate_transition(
            context,
            settings,
            from_status=from_status,
            to_status=to_status,
            card_type=card_type,
            dependency_card_ids=dependency_card_ids,
            blocking_card_ids=blocking_card_ids,
            referenced_evidence_ids=referenced_evidence_ids,
            skip_evidence_coverage=skip_evidence_coverage,
        )

    def evaluate_transition_context(
        self,
        context: CodeTraceabilityContext,
        settings: CodeTraceabilitySettings,
        *,
        from_status: str,
        to_status: str,
        card_type: str = "normal",
        dependency_card_ids: tuple[str, ...] = (),
        blocking_card_ids: tuple[str, ...] = (),
        referenced_evidence_ids: tuple[str, ...] = (),
        skip_evidence_coverage: bool = False,
    ) -> CodeTraceabilityGateEvaluation:
        return self._evaluator.evaluate_transition(
            context,
            settings,
            from_status=from_status,
            to_status=to_status,
            card_type=card_type,
            dependency_card_ids=dependency_card_ids,
            blocking_card_ids=blocking_card_ids,
            referenced_evidence_ids=referenced_evidence_ids,
            skip_evidence_coverage=skip_evidence_coverage,
        )

    def validate_or_raise(
        self,
        evaluation: CodeTraceabilityGateEvaluation,
    ) -> None:
        self._evaluator.validate_or_raise(evaluation)


__all__ = [
    "CodeTraceabilityGateBlocker",
    "CodeTraceabilityGateEvaluation",
    "CodeTraceabilityGateEvaluator",
    "CodeTraceabilityGatePhase",
    "CodeTraceabilityProjection",
    "CodeTraceabilityProjectionService",
    "EvidenceDispositionCoverage",
    "TargetEntityCoverage",
    "phases_for_transition",
    "extract_code_evidence_references",
    "resolve_code_traceability_settings",
]
