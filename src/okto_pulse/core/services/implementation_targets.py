"""Application policy for semantic implementation targets and agent receipts.

Targets describe intent.  Resolution and execution records are structured
claims submitted by an authenticated external agent.  This module performs no
repository, Git, filesystem, provider, search, or language-resolution work.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from uuid import uuid4

from okto_pulse.core.domain.code_traceability import (
    CodeInvestigationCapability,
    CodeInvestigationIdempotencyConflict,
    CodeInvestigationPayloadDigestMismatch,
    CodeInvestigationSelectorScopeMismatch,
    CodeInvestigationTrustLevel,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilitySubjectType,
    ImplementationTarget,
    ImplementationTargetEvidenceLink,
    ImplementationTargetExecutionDisposition,
    ImplementationTargetExecutionRecord,
    ImplementationTargetSpecLink,
    ImplementationTargetInvalid,
    ImplementationTargetResolution,
    ImplementationTargetResolutionOutdated,
    ImplementationTargetResolutionState,
    ImplementationTargetRole,
    ImplementationTargetSelectorKind,
    ResolutionCandidate,
    TargetExecutionDispositionRequired,
    canonical_code_traceability_sha256,
)
from okto_pulse.core.models.code_traceability import (
    ImplementationTargetCreateInput,
    ImplementationTargetEvidenceLinkInput,
    ImplementationTargetExecutionSubmission,
    ImplementationTargetResolutionSubmission,
    ImplementationTargetSpecLinkInput,
    ImplementationTargetUpdateInput,
)
from okto_pulse.core.ports.code_investigation import CodeInvestigationStore
from okto_pulse.core.ports.code_traceability import (
    CodeTraceabilityStore,
    ImplementationTargetQuery,
)
from okto_pulse.core.services.code_investigation import (
    AcceptedCodeInvestigation,
    CodeInvestigationService,
    required_capabilities_for_subject,
    require_code_attestor,
    selector_scope_digest_for_card_targets,
)


Clock = Callable[[], datetime]
IdFactory = Callable[[str], str]


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _default_id_factory(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ImplementationTargetInvalid(details={"field": "clock"})
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class ImplementationTargetMutationResult:
    target: ImplementationTarget
    spec_links: tuple[ImplementationTargetSpecLink, ...] = ()
    evidence_links: tuple[ImplementationTargetEvidenceLink, ...] = ()
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class ImplementationTargetExecutionResult:
    record: ImplementationTargetExecutionRecord
    head_revision: int
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class ImplementationTargetResolutionResult:
    resolution: ImplementationTargetResolution
    replayed: bool = False


class ImplementationTargetService:
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
    async def _active_targets(
        *,
        board_id: str,
        card_id: str,
        store: CodeTraceabilityStore,
    ) -> tuple[ImplementationTarget, ...]:
        page = await store.list_targets(
            ImplementationTargetQuery(
                board_id=board_id,
                card_id=card_id,
                lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
                limit=200,
            )
        )
        if page.next_cursor is not None:
            raise ImplementationTargetInvalid(
                details={"reason": "target_scope_limit_exceeded"}
            )
        return page.items

    @staticmethod
    def _unique_link_inputs(
        values: tuple[object, ...],
        *,
        key: Callable[[object], tuple[object, ...]],
        field: str,
    ) -> None:
        keys = tuple(key(item) for item in values)
        if len(set(keys)) != len(keys):
            raise ImplementationTargetInvalid(
                details={"field": field, "reason": "duplicate_link"}
            )

    async def _validate_link_inputs(
        self,
        *,
        target: ImplementationTarget,
        spec_link_inputs: tuple[ImplementationTargetSpecLinkInput, ...] | None,
        evidence_link_inputs: tuple[
            ImplementationTargetEvidenceLinkInput, ...
        ]
        | None,
        store: CodeTraceabilityStore,
    ) -> None:
        if spec_link_inputs is not None:
            self._unique_link_inputs(
                spec_link_inputs,
                key=lambda item: (item.entity_type, item.entity_id),
                field="spec_links",
            )
        if evidence_link_inputs is None:
            return
        self._unique_link_inputs(
            evidence_link_inputs,
            key=lambda item: (item.evidence_id, item.relation_type),
            field="evidence_links",
        )
        evidence_by_id: dict[str, object] = {}
        for item in evidence_link_inputs:
            evidence = evidence_by_id.get(item.evidence_id)
            if evidence is None:
                evidence = await store.get_evidence(
                    board_id=target.board_id,
                    evidence_id=item.evidence_id,
                )
                if evidence is not None:
                    evidence_by_id[item.evidence_id] = evidence
            if (
                evidence is None
                or evidence.source_ref != target.source_ref
                or evidence.lifecycle_status
                is not CodeTraceabilityLifecycleStatus.ACTIVE
            ):
                raise ImplementationTargetInvalid(
                    details={
                        "field": "evidence_links",
                        "evidence_id": item.evidence_id,
                        "reason": "active_same_source_evidence_required",
                    }
                )

    async def _replace_links(
        self,
        *,
        target: ImplementationTarget,
        spec_id: str,
        spec_link_inputs: tuple[ImplementationTargetSpecLinkInput, ...] | None,
        evidence_link_inputs: tuple[
            ImplementationTargetEvidenceLinkInput, ...
        ]
        | None,
        created_by: str,
        store: CodeTraceabilityStore,
    ) -> tuple[
        tuple[ImplementationTargetSpecLink, ...],
        tuple[ImplementationTargetEvidenceLink, ...],
    ]:
        """Build and atomically replace the persisted target lineage graph."""

        await self._validate_link_inputs(
            target=target,
            spec_link_inputs=spec_link_inputs,
            evidence_link_inputs=evidence_link_inputs,
            store=store,
        )

        current_spec_links = await store.list_target_spec_links(
            board_id=target.board_id,
            target_id=target.id,
        )
        current_evidence_links = await store.list_target_evidence_links(
            board_id=target.board_id,
            target_id=target.id,
        )
        if spec_link_inputs is None:
            desired_spec_links = current_spec_links
        else:
            existing_by_key = {
                (item.entity_type, item.entity_id): item
                for item in current_spec_links
            }
            now = self._now()
            desired_spec_links = tuple(
                existing_by_key.get((item.entity_type, item.entity_id))
                or ImplementationTargetSpecLink(
                    id=self._id_factory("target_spec_link"),
                    target_id=target.id,
                    spec_id=spec_id,
                    entity_type=item.entity_type,
                    entity_id=item.entity_id,
                    created_by=created_by,
                    created_at=now,
                )
                for item in spec_link_inputs
            )

        if evidence_link_inputs is None:
            desired_evidence_links = current_evidence_links
        else:
            existing_by_key = {
                (item.evidence_id, item.relation_type): item
                for item in current_evidence_links
            }
            now = self._now()
            desired_evidence_links = tuple(
                existing_by_key.get((item.evidence_id, item.relation_type))
                or ImplementationTargetEvidenceLink(
                    id=self._id_factory("target_evidence_link"),
                    target_id=target.id,
                    evidence_id=item.evidence_id,
                    relation_type=item.relation_type,
                    created_by=created_by,
                    created_at=now,
                )
                for item in evidence_link_inputs
            )

        return await store.replace_target_links(
            board_id=target.board_id,
            target_id=target.id,
            spec_links=desired_spec_links,
            evidence_links=desired_evidence_links,
            expected_target_revision=target.revision,
        )

    async def create(
        self,
        submission: ImplementationTargetCreateInput,
        *,
        created_by: str,
        spec_id: str | None = None,
        card_status: str,
        current_card_version: int,
        current_spec_version: int,
        minimum_trust: CodeInvestigationTrustLevel,
        require_committed_state: bool,
        investigation_service: CodeInvestigationService,
        investigation_store: CodeInvestigationStore,
        store: CodeTraceabilityStore,
    ) -> ImplementationTargetMutationResult:
        if current_spec_version != submission.expected_spec_version:
            raise ImplementationTargetInvalid(
                details={"reason": "spec_version_conflict"}
            )
        normalized_status = str(getattr(card_status, "value", card_status)).lower()
        if normalized_status in {"validation", "done"}:
            raise ImplementationTargetInvalid(
                details={"reason": "target_read_only", "card_status": normalized_status}
            )
        head = await investigation_store.get_current_head(
            board_id=submission.board_id,
            source_ref=submission.source_ref,
        )
        if head is None or head.current_receipt_id is None:
            raise ImplementationTargetInvalid(
                details={"reason": "current_preflight_required"}
            )
        accepted = await investigation_service.require_current_receipt(
            board_id=submission.board_id,
            receipt_id=head.current_receipt_id,
            store=investigation_store,
            source_ref=submission.source_ref,
            subject_type=CodeTraceabilitySubjectType.CARD,
            subject_id=submission.card_id,
            subject_version=current_card_version,
            required_capabilities=required_capabilities_for_subject(
                CodeTraceabilitySubjectType.CARD
            ),
            minimum_trust=minimum_trust,
            require_committed_state=require_committed_state,
        )
        existing_targets = await self._active_targets(
            board_id=submission.board_id,
            card_id=submission.card_id,
            store=store,
        )
        expected_scope = selector_scope_digest_for_card_targets(
            board_id=submission.board_id,
            card_id=submission.card_id,
            card_version=current_card_version,
            targets=tuple((item.id, item.revision) for item in existing_targets),
        )
        if accepted.receipt.selector_scope_digest != expected_scope:
            raise CodeInvestigationSelectorScopeMismatch()
        duplicate_key = (
            submission.selector_kind,
            submission.relative_path_hint,
            submission.qualified_symbol,
            submission.symbol_signature,
            submission.role,
        )
        for item in existing_targets:
            item_key = (
                item.selector_kind,
                item.relative_path_hint,
                item.qualified_symbol,
                item.symbol_signature,
                item.role,
            )
            if item_key == duplicate_key:
                raise ImplementationTargetInvalid(
                    details={"reason": "duplicate_target", "target_id": item.id}
                )
        if submission.baseline_evidence_id is not None:
            baseline = await store.get_evidence(
                board_id=submission.board_id,
                evidence_id=submission.baseline_evidence_id,
            )
            if (
                baseline is None
                or baseline.source_ref != submission.source_ref
                or baseline.lifecycle_status
                is not CodeTraceabilityLifecycleStatus.ACTIVE
            ):
                raise ImplementationTargetInvalid(
                    details={"field": "baseline_evidence_id"}
                )
        now = self._now()
        target = ImplementationTarget(
            id=self._id_factory("implementation_target"),
            board_id=submission.board_id,
            card_id=submission.card_id,
            source_ref=submission.source_ref,
            selector_kind=submission.selector_kind,
            relative_path_hint=submission.relative_path_hint,
            language=submission.language,
            symbol_kind=submission.symbol_kind,
            qualified_symbol=submission.qualified_symbol,
            symbol_signature=submission.symbol_signature,
            role=submission.role,
            intent=submission.intent,
            required=submission.required,
            source_spec_version=current_spec_version,
            baseline_evidence_id=submission.baseline_evidence_id,
            lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
            revision=1,
            current_resolution_id=None,
            last_change_reason_sha256=None,
            created_by=created_by,
            created_at=now,
            updated_at=now,
        )
        await self._validate_link_inputs(
            target=target,
            spec_link_inputs=submission.spec_links,
            evidence_link_inputs=submission.evidence_links,
            store=store,
        )
        persisted = await store.create_target(
            target=target,
            expected_head_revision=accepted.head.revision,
            expected_spec_version=current_spec_version,
        )
        if persisted != target:
            raise CodeInvestigationPayloadDigestMismatch(
                details={"field": "implementation_target"}
            )
        spec_links: tuple[ImplementationTargetSpecLink, ...] = ()
        evidence_links: tuple[ImplementationTargetEvidenceLink, ...] = ()
        if submission.spec_links or submission.evidence_links:
            if not spec_id:
                raise ImplementationTargetInvalid(
                    details={"reason": "target_link_context_required"}
                )
            spec_links, evidence_links = await self._replace_links(
                target=persisted,
                spec_id=spec_id,
                spec_link_inputs=submission.spec_links,
                evidence_link_inputs=submission.evidence_links,
                created_by=created_by,
                store=store,
            )
        return ImplementationTargetMutationResult(
            target=persisted,
            spec_links=spec_links,
            evidence_links=evidence_links,
        )

    async def update(
        self,
        submission: ImplementationTargetUpdateInput,
        *,
        card_status: str,
        spec_id: str | None = None,
        updated_by: str | None = None,
        store: CodeTraceabilityStore,
    ) -> ImplementationTargetMutationResult:
        current = await store.get_target(
            board_id=submission.board_id,
            target_id=submission.target_id,
        )
        if (
            current is None
            or current.card_id != submission.card_id
            or current.lifecycle_status is not CodeTraceabilityLifecycleStatus.ACTIVE
        ):
            raise ImplementationTargetInvalid(
                details={"target_id": submission.target_id}
            )
        if current.revision != submission.expected_revision:
            raise ImplementationTargetResolutionOutdated(
                details={
                    "expected_revision": submission.expected_revision,
                    "actual_revision": current.revision,
                }
            )
        normalized_status = str(getattr(card_status, "value", card_status)).lower()
        if normalized_status in {"validation", "done"}:
            raise ImplementationTargetInvalid(
                details={"reason": "target_read_only", "card_status": normalized_status}
            )
        values: dict[str, object] = {}
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
        }
        for field_name in submission.model_fields_set & mutable_fields:
            values[field_name] = getattr(submission, field_name)
        if (
            "baseline_evidence_id" in values
            and values["baseline_evidence_id"] is not None
        ):
            baseline = await store.get_evidence(
                board_id=submission.board_id,
                evidence_id=str(values["baseline_evidence_id"]),
            )
            if (
                baseline is None
                or baseline.source_ref != current.source_ref
                or baseline.lifecycle_status
                is not CodeTraceabilityLifecycleStatus.ACTIVE
            ):
                raise ImplementationTargetInvalid(
                    details={"field": "baseline_evidence_id"}
                )
        updated = replace(
            current,
            **values,
            revision=current.revision + 1,
            current_resolution_id=None,
            last_change_reason_sha256=canonical_code_traceability_sha256(
                {"change_reason": submission.change_reason}
            ),
            updated_at=self._now(),
        )
        await self._validate_link_inputs(
            target=updated,
            spec_link_inputs=submission.spec_links,
            evidence_link_inputs=submission.evidence_links,
            store=store,
        )
        persisted = await store.update_target(
            target=updated,
            expected_revision=current.revision,
        )
        spec_links: tuple[ImplementationTargetSpecLink, ...] = ()
        evidence_links: tuple[ImplementationTargetEvidenceLink, ...] = ()
        if submission.spec_links is not None or submission.evidence_links is not None:
            if not spec_id or not updated_by:
                raise ImplementationTargetInvalid(
                    details={"reason": "target_link_context_required"}
                )
            spec_links, evidence_links = await self._replace_links(
                target=persisted,
                spec_id=spec_id,
                spec_link_inputs=submission.spec_links,
                evidence_link_inputs=submission.evidence_links,
                created_by=updated_by,
                store=store,
            )
        return ImplementationTargetMutationResult(
            target=persisted,
            spec_links=spec_links,
            evidence_links=evidence_links,
        )

    @staticmethod
    def _resolution_capabilities(
        target: ImplementationTarget,
        submission: ImplementationTargetResolutionSubmission,
    ) -> tuple[CodeInvestigationCapability, ...]:
        capabilities = {
            CodeInvestigationCapability.SOURCE_IDENTITY,
            CodeInvestigationCapability.REVISION_IDENTITY,
            CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
            CodeInvestigationCapability.FILE_READ,
            CodeInvestigationCapability.PATH_CONTAINMENT,
            CodeInvestigationCapability.SYMLINK_CONTAINMENT,
        }
        if target.selector_kind is ImplementationTargetSelectorKind.SYMBOL:
            capabilities.add(CodeInvestigationCapability.SYMBOL_RESOLUTION)
        if submission.state in {
            ImplementationTargetResolutionState.MOVED,
            ImplementationTargetResolutionState.STALE,
            ImplementationTargetResolutionState.AMBIGUOUS,
        }:
            capabilities.add(CodeInvestigationCapability.RENAME_OBSERVATION)
        return tuple(sorted(capabilities, key=lambda item: item.value))

    @staticmethod
    def _selector_fingerprint(target: ImplementationTarget) -> str:
        return canonical_code_traceability_sha256(
            {
                "source_ref": target.source_ref,
                "selector_kind": target.selector_kind,
                "relative_path_hint": target.relative_path_hint,
                "language": target.language,
                "symbol_kind": target.symbol_kind,
                "qualified_symbol": target.qualified_symbol,
                "symbol_signature": target.symbol_signature,
                "role": target.role,
                "revision": target.revision,
            }
        )

    @staticmethod
    def _resolution_payload_sha256(
        submission: ImplementationTargetResolutionSubmission,
        *,
        actor_id: str,
        target_id: str,
        target_revision: int,
        receipt_source_ref: str,
    ) -> str:
        return canonical_code_traceability_sha256(
            {
                "operation": "submit_implementation_target_resolution",
                "actor_id": actor_id,
                "board_id": submission.board_id,
                "card_id": submission.card_id,
                "target_id": target_id,
                "target_revision": target_revision,
                "investigation_receipt_id": (submission.investigation_receipt_id),
                "source_ref": receipt_source_ref,
                "agent_payload": submission.model_dump(mode="python"),
            }
        )

    async def _validate_card_scope(
        self,
        *,
        accepted: AcceptedCodeInvestigation,
        card_id: str,
        card_version: int,
        store: CodeTraceabilityStore,
    ) -> None:
        targets = await self._active_targets(
            board_id=accepted.receipt.board_id,
            card_id=card_id,
            store=store,
        )
        expected_scope = selector_scope_digest_for_card_targets(
            board_id=accepted.receipt.board_id,
            card_id=card_id,
            card_version=card_version,
            targets=tuple((item.id, item.revision) for item in targets),
        )
        if accepted.receipt.selector_scope_digest != expected_scope:
            raise CodeInvestigationSelectorScopeMismatch()

    async def submit_resolution(
        self,
        submission: ImplementationTargetResolutionSubmission,
        *,
        actor_id: str,
        actor_kind: str,
        current_card_version: int,
        minimum_trust: CodeInvestigationTrustLevel,
        require_committed_state: bool,
        investigation_service: CodeInvestigationService,
        investigation_store: CodeInvestigationStore,
        store: CodeTraceabilityStore,
    ) -> ImplementationTargetResolutionResult:
        actor = require_code_attestor(actor_id, actor_kind)
        raw_receipt = await investigation_store.get_receipt(
            board_id=submission.board_id,
            receipt_id=submission.investigation_receipt_id,
        )
        if raw_receipt is None:
            await investigation_service.require_current_receipt(
                board_id=submission.board_id,
                receipt_id=submission.investigation_receipt_id,
                store=investigation_store,
            )
            raise AssertionError("unreachable")
        replay = await store.resolve_resolution_replay(
            board_id=submission.board_id,
            submitted_by=actor,
            investigation_receipt_id=submission.investigation_receipt_id,
            target_id=submission.target_id,
            idempotency_key=submission.idempotency_key,
        )
        if replay is not None:
            payload_sha256 = self._resolution_payload_sha256(
                submission,
                actor_id=actor,
                target_id=replay.target_id,
                target_revision=replay.target_revision,
                receipt_source_ref=replay.source_ref,
            )
            if replay.payload_sha256 != payload_sha256:
                raise CodeInvestigationIdempotencyConflict()
            return ImplementationTargetResolutionResult(
                resolution=replay,
                replayed=True,
            )
        target = await store.get_target(
            board_id=submission.board_id,
            target_id=submission.target_id,
        )
        if (
            target is None
            or target.card_id != submission.card_id
            or target.lifecycle_status is not CodeTraceabilityLifecycleStatus.ACTIVE
        ):
            raise ImplementationTargetInvalid()
        if (
            submission.state is ImplementationTargetResolutionState.MISSING
            and target.role is ImplementationTargetRole.CREATE
            and submission.reason_code != "missing_expected"
        ):
            raise ImplementationTargetInvalid(
                details={"reason": "create_target_missing_expected_required"}
            )
        payload_sha256 = self._resolution_payload_sha256(
            submission,
            actor_id=actor,
            target_id=target.id,
            target_revision=target.revision,
            receipt_source_ref=raw_receipt.source_ref,
        )
        accepted = await investigation_service.require_current_receipt(
            board_id=submission.board_id,
            receipt_id=submission.investigation_receipt_id,
            store=investigation_store,
            actor_id=actor,
            subject_type=CodeTraceabilitySubjectType.CARD,
            subject_id=submission.card_id,
            subject_version=current_card_version,
            source_ref=target.source_ref,
            required_capabilities=self._resolution_capabilities(
                target,
                submission,
            ),
            minimum_trust=minimum_trust,
            require_committed_state=require_committed_state,
        )
        await self._validate_card_scope(
            accepted=accepted,
            card_id=submission.card_id,
            card_version=current_card_version,
            store=store,
        )
        workspace_state = accepted.receipt.workspace_state
        if workspace_state is None:
            raise CodeInvestigationSelectorScopeMismatch(
                details={"field": "workspace_state"}
            )
        candidates = tuple(
            ResolutionCandidate(
                relative_path=item.relative_path,
                qualified_symbol=item.qualified_symbol,
                symbol_signature=item.symbol_signature,
                symbol_fingerprint=item.symbol_fingerprint,
                confidence=item.confidence,
                reason_code=item.reason_code,
            )
            for item in submission.candidates
        )
        now = self._now()
        resolution = ImplementationTargetResolution(
            id=self._id_factory("target_resolution"),
            board_id=submission.board_id,
            target_id=target.id,
            investigation_receipt_id=accepted.receipt.id,
            source_ref=target.source_ref,
            receipt_generation=accepted.receipt.generation,
            subject_version=current_card_version,
            target_revision=target.revision,
            workspace_state=workspace_state,
            state=submission.state,
            resolved_relative_path=submission.resolved_relative_path,
            resolved_language=submission.resolved_language,
            resolved_symbol_kind=submission.resolved_symbol_kind,
            resolved_qualified_symbol=submission.resolved_qualified_symbol,
            resolved_symbol_signature=submission.resolved_symbol_signature,
            resolved_line_start=submission.resolved_line_start,
            resolved_line_end=submission.resolved_line_end,
            symbol_fingerprint=submission.symbol_fingerprint,
            declared_file_blob_sha256=submission.declared_file_blob_sha256,
            selector_fingerprint=self._selector_fingerprint(target),
            confidence=submission.confidence,
            reason_code=submission.reason_code,
            candidate_count=len(candidates),
            candidates=candidates,
            declared_tool_id=submission.tooling.tool_id,
            declared_tool_version=submission.tooling.tool_version,
            submitted_by=actor,
            agent_observed_at=submission.agent_observed_at,
            received_at=now,
            payload_sha256=payload_sha256,
            idempotency_key=submission.idempotency_key,
        )
        updated_target = replace(
            target,
            current_resolution_id=resolution.id,
            updated_at=now,
        )
        committed = await store.append_resolution(
            target=updated_target,
            resolution=resolution,
            expected_target_revision=target.revision,
            expected_head_revision=accepted.head.revision,
        )
        return ImplementationTargetResolutionResult(
            resolution=committed.resolution,
            replayed=committed.replayed,
        )

    @staticmethod
    def _execution_payload_sha256(
        submission: ImplementationTargetExecutionSubmission,
        *,
        actor_id: str,
        target_id: str,
        target_revision: int,
        receipt_source_ref: str,
    ) -> str:
        return canonical_code_traceability_sha256(
            {
                "operation": "submit_implementation_target_execution",
                "actor_id": actor_id,
                "board_id": submission.board_id,
                "card_id": submission.card_id,
                "target_id": target_id,
                "target_revision": target_revision,
                "result_investigation_receipt_id": (
                    submission.result_investigation_receipt_id
                ),
                "source_ref": receipt_source_ref,
                "agent_payload": submission.model_dump(mode="python"),
            }
        )

    async def submit_execution(
        self,
        submission: ImplementationTargetExecutionSubmission,
        *,
        actor_id: str,
        actor_kind: str,
        current_card_version: int,
        minimum_trust: CodeInvestigationTrustLevel,
        require_committed_state: bool,
        investigation_service: CodeInvestigationService,
        investigation_store: CodeInvestigationStore,
        store: CodeTraceabilityStore,
    ) -> ImplementationTargetExecutionResult:
        actor = require_code_attestor(actor_id, actor_kind)
        raw_receipt = await investigation_store.get_receipt(
            board_id=submission.board_id,
            receipt_id=submission.result_investigation_receipt_id,
        )
        if raw_receipt is None:
            await investigation_service.require_current_receipt(
                board_id=submission.board_id,
                receipt_id=submission.result_investigation_receipt_id,
                store=investigation_store,
            )
            raise AssertionError("unreachable")
        replay = await store.resolve_execution_replay(
            board_id=submission.board_id,
            submitted_by=actor,
            result_investigation_receipt_id=(
                submission.result_investigation_receipt_id
            ),
            target_id=submission.target_id,
            idempotency_key=submission.idempotency_key,
        )
        if replay is not None:
            payload_sha256 = self._execution_payload_sha256(
                submission,
                actor_id=actor,
                target_id=replay.target_id,
                target_revision=replay.target_revision,
                receipt_source_ref=replay.source_ref,
            )
            if replay.payload_sha256 != payload_sha256:
                raise CodeInvestigationIdempotencyConflict()
            return ImplementationTargetExecutionResult(
                record=replay,
                head_revision=raw_receipt.generation,
                replayed=True,
            )
        target = await store.get_target(
            board_id=submission.board_id,
            target_id=submission.target_id,
        )
        if (
            target is None
            or target.card_id != submission.card_id
            or target.lifecycle_status is not CodeTraceabilityLifecycleStatus.ACTIVE
        ):
            raise ImplementationTargetInvalid()
        if (
            submission.disposition is ImplementationTargetExecutionDisposition.CREATED
            and target.role is not ImplementationTargetRole.CREATE
        ):
            raise TargetExecutionDispositionRequired(
                details={"reason": "created_requires_create_role"}
            )
        if (
            submission.disposition is ImplementationTargetExecutionDisposition.DELETED
            and target.role is not ImplementationTargetRole.DELETE
        ):
            raise TargetExecutionDispositionRequired(
                details={"reason": "deleted_requires_delete_role"}
            )
        if submission.replacement_target_id is not None:
            replacement_target = await store.get_target(
                board_id=submission.board_id,
                target_id=submission.replacement_target_id,
            )
            if (
                replacement_target is None
                or replacement_target.card_id != submission.card_id
                or replacement_target.lifecycle_status
                is not CodeTraceabilityLifecycleStatus.ACTIVE
            ):
                raise ImplementationTargetInvalid(
                    details={"field": "replacement_target_id"}
                )
        payload_sha256 = self._execution_payload_sha256(
            submission,
            actor_id=actor,
            target_id=target.id,
            target_revision=target.revision,
            receipt_source_ref=raw_receipt.source_ref,
        )
        accepted = await investigation_service.require_current_receipt(
            board_id=submission.board_id,
            receipt_id=submission.result_investigation_receipt_id,
            store=investigation_store,
            actor_id=actor,
            subject_type=CodeTraceabilitySubjectType.CARD,
            subject_id=submission.card_id,
            subject_version=current_card_version,
            source_ref=target.source_ref,
            required_capabilities=(
                CodeInvestigationCapability.SOURCE_IDENTITY,
                CodeInvestigationCapability.REVISION_IDENTITY,
                CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
                CodeInvestigationCapability.PATH_CONTAINMENT,
                CodeInvestigationCapability.SYMLINK_CONTAINMENT,
            ),
            minimum_trust=minimum_trust,
            require_committed_state=require_committed_state,
        )
        await self._validate_card_scope(
            accepted=accepted,
            card_id=submission.card_id,
            card_version=current_card_version,
            store=store,
        )
        workspace_state = accepted.receipt.workspace_state
        if workspace_state is None:
            raise CodeInvestigationSelectorScopeMismatch(
                details={"field": "workspace_state"}
            )
        record = ImplementationTargetExecutionRecord(
            id=self._id_factory("target_execution"),
            board_id=submission.board_id,
            card_id=submission.card_id,
            target_id=target.id,
            target_revision=target.revision,
            result_investigation_receipt_id=accepted.receipt.id,
            disposition=submission.disposition,
            source_ref=target.source_ref,
            result_declared_revision=workspace_state.declared_revision,
            result_workspace_state_id=workspace_state.workspace_state_id,
            actual_relative_path=submission.actual_relative_path,
            actual_qualified_symbol=submission.actual_qualified_symbol,
            replacement_target_id=submission.replacement_target_id,
            justification=submission.justification,
            submitted_by=actor,
            received_at=self._now(),
            payload_sha256=payload_sha256,
            idempotency_key=submission.idempotency_key,
        )
        persisted = await store.append_execution_record(
            record=record,
            expected_head_revision=accepted.head.revision,
        )
        return ImplementationTargetExecutionResult(
            record=persisted,
            head_revision=accepted.head.revision,
        )


__all__ = [
    "ImplementationTargetExecutionResult",
    "ImplementationTargetMutationResult",
    "ImplementationTargetResolutionResult",
    "ImplementationTargetService",
]
