"""Transport-neutral orchestration for inbound Code Traceability claims.

These use cases authorize actors, bind claims to current Pulse entity versions,
apply board policy, and commit transaction-bound stores.  They never inspect a
repository, filesystem, provider, Git object, or source file.  The external
authenticated agent performs the deterministic check before submitting a
structured attestation to these commands.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.domain.code_traceability import (
    CodeEvidenceLinkInvalid,
    CodeInvestigationAttestorMismatch,
    CodeInvestigationRequestNotFound,
    CodeInvestigationReceiptCurrentness,
    CodeInvestigationSubjectVersionConflict,
    CodeInvestigationTrustLevel,
    CodeTraceabilityContractError,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilityLocked,
    CodeTraceabilityPage,
    CodeTraceabilityProjectionProfile,
    CodeTraceabilitySubjectType,
    CodeTraceabilityWaiverEntityType,
    ImplementationTarget,
    ImplementationTargetInvalid,
    SpecEntityType,
    TargetOverlap,
    canonical_code_traceability_sha256,
)
from okto_pulse.core.events.code_traceability import (
    TraceabilityActorType,
    code_traceability_event_digest,
    make_code_traceability_event,
    publish_code_traceability_mutation,
)
from okto_pulse.core.events.types import (
    CodeEvidenceCreated,
    CodeEvidenceDispositionChanged,
    CodeEvidenceLinked,
    CodeEvidenceRevoked,
    CodeEvidenceSuperseded,
    CodeEvidenceUnlinked,
    CodeInvestigationReceiptRevoked,
    CodeInvestigationReceiptSubmitted,
    CodeInvestigationRequested,
    CodeTraceabilityDomainEvent,
    CodeTraceabilityWaiverCleared,
    CodeTraceabilityWaiverCreated,
    ImplementationOverlapAcknowledged,
    ImplementationTargetCreated,
    ImplementationTargetExecutionReceiptSubmitted,
    ImplementationTargetResolutionSubmitted,
    ImplementationTargetRevoked,
    ImplementationTargetUpdated,
)
from okto_pulse.core.models.code_traceability import (
    CodeEvidenceDispositionClearInput,
    CodeEvidenceDispositionInput,
    CodeEvidenceRevokeInput,
    CodeEvidenceSpecLinkInput,
    CodeEvidenceSpecUnlinkInput,
    CodeEvidenceSubmission,
    CodeEvidenceSupersessionSubmission,
    CodeEvidenceView,
    CodeInvestigationReceiptSubmission,
    CodeInvestigationReceiptView,
    CodeInvestigationRequestView,
    CodeTraceabilityWaiverClearInput,
    CodeTraceabilityWaiverInput,
    ImplementationTargetCreateInput,
    ImplementationTargetExecutionSubmission,
    ImplementationTargetResolutionSubmission,
    ImplementationTargetUpdateInput,
    ImplementationTargetView,
    SpecCodeEvidenceRebaseApplyInput,
    SpecCodeEvidenceRebasePreviewInput,
    StartCodeInvestigationInput,
    StartCodeInvestigationResult,
    TargetOverlapAcknowledgementInput,
)
from okto_pulse.core.models.schemas import CodeTraceabilitySettings
from okto_pulse.core.ports.code_traceability import (
    CodeEvidenceQuery,
    CodeTraceabilityProjectionQuery,
    ImplementationTargetQuery,
    TargetOverlapQuery,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.services.code_evidence import (
    CodeEvidenceDispositionMutationResult,
    CodeEvidenceLinkMutationResult,
    CodeEvidenceMutationResult,
    CodeEvidenceRevocationResult,
    CodeEvidenceService,
    CodeEvidenceUnlinkMutationResult,
)
from okto_pulse.core.services.code_evidence_rebase import (
    SpecCodeEvidenceRebasePlan,
    SpecCodeEvidenceRebaseResult,
    SpecCodeEvidenceRebaseService,
)
from okto_pulse.core.services.code_investigation import (
    CodeInvestigationService,
    effective_required_capabilities_for_subject,
    selector_scope_digest_for_card_targets,
    selector_scope_digest_for_subject,
)
from okto_pulse.core.services.implementation_targets import (
    ImplementationTargetExecutionResult,
    ImplementationTargetMutationResult,
    ImplementationTargetResolutionResult,
    ImplementationTargetService,
)
from okto_pulse.core.services.code_overlap import (
    CodeOverlapService,
    TargetOverlapAcknowledgementResult,
)
from okto_pulse.core.services.code_traceability_waivers import (
    CodeTraceabilityWaiverMutationResult,
    CodeTraceabilityWaiverService,
)
from okto_pulse.core.services.code_traceability_gate import (
    CodeTraceabilityProjection,
    CodeTraceabilityProjectionService,
    extract_code_evidence_references,
    resolve_code_traceability_settings,
)
from okto_pulse.core.services.code_traceability_observability import (
    METRIC_CODE_INVESTIGATION_RECEIPT_AGE_SECONDS,
    METRIC_CODE_INVESTIGATION_RECEIPT_REJECTED_TOTAL,
    METRIC_CODE_INVESTIGATION_RECEIPT_TOTAL,
    observe_code_traceability_metric,
)
from okto_pulse.core.services.card_operational_freeze import (
    require_card_operational_mutation_allowed,
)
from okto_pulse.core.ports.code_investigation import (
    CodeInvestigationReceiptQuery,
)
from okto_pulse.core.ports.code_traceability import (
    CodeEvidenceSupersessionCommitResult,
)


@dataclass(frozen=True, slots=True)
class SubmittedCodeInvestigationReceiptResult:
    receipt: CodeInvestigationReceiptView
    generation: int
    head_revision: int
    replayed: bool


@dataclass(frozen=True, slots=True)
class CodeInvestigationReceiptReadResult:
    receipt: CodeInvestigationReceiptView
    currentness: CodeInvestigationReceiptCurrentness


@dataclass(frozen=True, slots=True)
class RevokeCodeInvestigationReceiptCommand:
    board_id: str
    receipt_id: str
    reason_code: str
    justification: str


@dataclass(frozen=True, slots=True)
class GetCodeInvestigationReceiptCommand:
    board_id: str
    receipt_id: str


@dataclass(frozen=True, slots=True)
class GetCodeEvidenceCommand:
    board_id: str
    evidence_id: str
    profile: CodeTraceabilityProjectionProfile = (
        CodeTraceabilityProjectionProfile.DETAIL
    )


@dataclass(frozen=True, slots=True)
class ListCodeEvidenceCommand:
    query: CodeEvidenceQuery
    profile: CodeTraceabilityProjectionProfile = (
        CodeTraceabilityProjectionProfile.SUMMARY
    )


@dataclass(frozen=True, slots=True)
class GetImplementationTargetCommand:
    board_id: str
    target_id: str


@dataclass(frozen=True, slots=True)
class _ResolvedPolicy:
    settings: CodeTraceabilitySettings

    @property
    def minimum_trust(self) -> CodeInvestigationTrustLevel:
        return CodeInvestigationTrustLevel(self.settings.minimum_trust)

    @property
    def require_committed_state(self) -> bool:
        return self.settings.observed_state_policy == "require_committed_attestation"


def _record_version(record: object, *, entity_type: str) -> int:
    version = (
        getattr(record, "policy_version", None)
        if entity_type == "card"
        else getattr(record, "version", None)
    )
    if version is None:
        # Compatibility for transport-neutral test doubles and older external
        # adapters.  Community's canonical Card fence is ``policy_version``.
        version = getattr(record, "version", None)
    if type(version) is not int or version < 1:
        raise CodeTraceabilityLocked(
            details={"reason": "subject_version_unavailable", "entity": entity_type}
        )
    return version


def _require_board_record(record: object, *, board_id: str, entity_type: str) -> None:
    if getattr(record, "board_id", None) != board_id:
        raise EntityNotFoundError(entity_type, str(getattr(record, "id", "")))


async def _load_policy(
    *,
    board_id: str,
    uow: PulseUnitOfWork,
) -> _ResolvedPolicy:
    board = await uow.services.boards.get_board(board_id)
    if board is None:
        raise EntityNotFoundError("board", board_id)
    raw_settings = getattr(board, "settings", None) or {}
    try:
        settings = resolve_code_traceability_settings(raw_settings)
    except CodeTraceabilityContractError as exc:
        raise CodeTraceabilityLocked(
            details={"reason": "board_policy_invalid"}
        ) from exc
    return _ResolvedPolicy(settings=settings)


async def _authorize(
    actor: ActorContext,
    uow: PulseUnitOfWork,
    *,
    board_id: str,
    operation: str,
) -> None:
    await require_authorization(
        actor,
        PermissionRequirement(operation),
        uow=uow,
        board_id=board_id,
    )


def _traceability_actor_type(actor: ActorContext) -> TraceabilityActorType:
    if actor.actor_kind == "agent":
        return "agent"
    if actor.actor_kind in {"human", "user"}:
        return "user"
    return "system"


async def _publish_mutation_event(
    uow: PulseUnitOfWork,
    event_class: type[CodeTraceabilityDomainEvent],
    *,
    actor: ActorContext,
    board_id: str,
    replayed: bool = False,
    **metadata: Any,
) -> None:
    event = make_code_traceability_event(
        event_class,
        board_id=board_id,
        actor_id=actor.actor_id,
        actor_type=_traceability_actor_type(actor),
        **metadata,
    )
    await publish_code_traceability_mutation(uow, event, replayed=replayed)


def _observe_receipt_rejection(exc: CodeTraceabilityContractError) -> None:
    """Emit process telemetry only; durable rejection audit is edition-owned."""

    observe_code_traceability_metric(
        METRIC_CODE_INVESTIGATION_RECEIPT_REJECTED_TOTAL,
        labels={"reason_code": exc.code},
    )


async def _require_attestor_policy(
    policy: _ResolvedPolicy,
    actor: ActorContext,
    uow: PulseUnitOfWork,
    *,
    board_id: str,
) -> None:
    if (
        policy.settings.accepted_attestor_policy
        != "granular_permission_and_board_allowlist"
    ):
        return
    checker = getattr(uow.services.agents, "agent_has_board_access", None)
    if not callable(checker) or not await checker(actor.actor_id, board_id):
        raise CodeInvestigationAttestorMismatch(
            details={"reason": "actor_not_in_board_allowlist"}
        )


async def _load_subject(
    *,
    board_id: str,
    subject_type: CodeTraceabilitySubjectType,
    subject_id: str,
    uow: PulseUnitOfWork,
) -> object:
    if subject_type is CodeTraceabilitySubjectType.REFINEMENT:
        record = await uow.services.refinements.get_refinement(subject_id)
    elif subject_type is CodeTraceabilitySubjectType.SPEC:
        record = await uow.services.specs.get_spec(subject_id)
    else:
        record = await uow.services.cards.get_card(subject_id)
    if record is None:
        raise EntityNotFoundError(subject_type.value, subject_id)
    _require_board_record(
        record,
        board_id=board_id,
        entity_type=subject_type.value,
    )
    return record


async def _load_card_and_spec(
    *,
    board_id: str,
    card_id: str,
    uow: PulseUnitOfWork,
) -> tuple[object, object]:
    card = await _load_subject(
        board_id=board_id,
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id=card_id,
        uow=uow,
    )
    spec_id = getattr(card, "spec_id", None)
    if not isinstance(spec_id, str) or not spec_id:
        raise ImplementationTargetInvalid(details={"reason": "card_spec_required"})
    spec = await uow.services.specs.get_spec(spec_id)
    if spec is None:
        raise EntityNotFoundError("spec", spec_id)
    _require_board_record(spec, board_id=board_id, entity_type="spec")
    return card, spec


async def _evidence_parent_version_or_replay(
    command: CodeEvidenceSubmission,
    *,
    actor: ActorContext,
    uow: PulseUnitOfWork,
) -> int:
    replay = await uow.services.code_traceability.resolve_evidence_replay(
        board_id=command.board_id,
        submitted_by=actor.actor_id,
        parent_id=command.parent_id,
        idempotency_key=command.idempotency_key,
    )
    if replay is not None:
        return replay.parent_version
    parent = await _load_subject(
        board_id=command.board_id,
        subject_type=command.parent_type,
        subject_id=command.parent_id,
        uow=uow,
    )
    if command.parent_type is CodeTraceabilitySubjectType.CARD:
        require_card_operational_mutation_allowed(
            parent,
            operation="code_traceability.evidence.submit",
        )
    return _record_version(parent, entity_type=command.parent_type.value)


async def _require_evidence_card_parent_mutable(
    *,
    board_id: str,
    evidence_id: str,
    operation: str,
    uow: PulseUnitOfWork,
) -> object | None:
    """Freeze a Card-owned evidence mutation; non-Card evidence is unaffected."""

    evidence = await uow.services.code_traceability.get_evidence(
        board_id=board_id,
        evidence_id=evidence_id,
    )
    if (
        evidence is None
        or evidence.parent_type is not CodeTraceabilitySubjectType.CARD
    ):
        return evidence
    card = await _load_subject(
        board_id=board_id,
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id=evidence.parent_id,
        uow=uow,
    )
    require_card_operational_mutation_allowed(card, operation=operation)
    return evidence


async def _resolution_card_version_or_replay(
    command: ImplementationTargetResolutionSubmission,
    *,
    actor: ActorContext,
    uow: PulseUnitOfWork,
) -> int:
    replay = await uow.services.code_traceability.resolve_resolution_replay(
        board_id=command.board_id,
        submitted_by=actor.actor_id,
        investigation_receipt_id=command.investigation_receipt_id,
        target_id=command.target_id,
        idempotency_key=command.idempotency_key,
    )
    if replay is not None:
        return replay.subject_version
    card = await _load_subject(
        board_id=command.board_id,
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id=command.card_id,
        uow=uow,
    )
    # A rejection bumps the card subject version.  Resolution is the bounded,
    # non-execution operation that may renew an existing target against that
    # Current version before the card can leave Rejected under blocking mode.
    # Target create/update and execution receipt writers remain frozen.
    return _record_version(card, entity_type="card")


async def _execution_card_version_or_replay(
    command: ImplementationTargetExecutionSubmission,
    *,
    actor: ActorContext,
    uow: PulseUnitOfWork,
) -> int:
    replay = await uow.services.code_traceability.resolve_execution_replay(
        board_id=command.board_id,
        submitted_by=actor.actor_id,
        result_investigation_receipt_id=command.result_investigation_receipt_id,
        target_id=command.target_id,
        idempotency_key=command.idempotency_key,
    )
    if replay is not None:
        receipt = await uow.services.code_investigations.get_receipt(
            board_id=command.board_id,
            receipt_id=command.result_investigation_receipt_id,
        )
        if receipt is None:
            raise CodeInvestigationRequestNotFound(
                details={
                    "entity": "receipt",
                    "receipt_id": command.result_investigation_receipt_id,
                }
            )
        return receipt.subject_version
    card = await _load_subject(
        board_id=command.board_id,
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id=command.card_id,
        uow=uow,
    )
    if str(getattr(getattr(card, "status", None), "value", getattr(card, "status", ""))).lower() == "rejected":
        raise ImplementationTargetInvalid(
            details={"reason": "card_rejected_rework_handoff_required"}
        )
    return _record_version(card, entity_type="card")


def _entity_id(value: object) -> str | None:
    if isinstance(value, str):
        candidate = value
    elif isinstance(value, dict):
        candidate = value.get("id")
    else:
        candidate = getattr(value, "id", None)
    return candidate if isinstance(candidate, str) and candidate else None


def _entity_is_active(value: object) -> bool:
    if isinstance(value, dict):
        status = value.get("status") or "active"
    else:
        status = getattr(value, "status", None) or "active"
    return str(getattr(status, "value", status)).lower() == "active"


def _require_spec_entity(
    spec: object, entity_type: SpecEntityType, entity_id: str
) -> None:
    if entity_type is SpecEntityType.SPEC:
        if getattr(spec, "id", None) != entity_id:
            raise CodeEvidenceLinkInvalid(details={"reason": "spec_entity_not_found"})
        return
    collection_by_type = {
        SpecEntityType.FUNCTIONAL_REQUIREMENT: "functional_requirements",
        SpecEntityType.TECHNICAL_REQUIREMENT: "technical_requirements",
        SpecEntityType.ACCEPTANCE_CRITERION: "acceptance_criteria",
        SpecEntityType.BUSINESS_RULE: "business_rules",
        SpecEntityType.API_CONTRACT: "api_contracts",
        SpecEntityType.INTEGRATION_REQUIREMENT: "integration_requirements",
        SpecEntityType.OBSERVABILITY_REQUIREMENT: "observability_requirements",
        SpecEntityType.DECISION: "decisions",
        SpecEntityType.TEST_SCENARIO: "test_scenario_ids",
    }
    collection = getattr(spec, collection_by_type[entity_type], None) or ()
    if not any(
        _entity_id(item) == entity_id and _entity_is_active(item) for item in collection
    ):
        raise CodeEvidenceLinkInvalid(details={"reason": "spec_entity_not_found"})


def _advance_spec_version(spec: object, *, expected: int, next_version: int) -> None:
    if _record_version(spec, entity_type="spec") != expected:
        raise CodeEvidenceLinkInvalid(details={"reason": "spec_version_conflict"})
    if next_version != expected + 1:
        raise CodeEvidenceLinkInvalid(details={"reason": "spec_version_invalid"})
    setattr(spec, "version", next_version)


async def _load_waiver_entity(
    *,
    board_id: str,
    entity_type: CodeTraceabilityWaiverEntityType,
    entity_id: str,
    uow: PulseUnitOfWork,
) -> object:
    subject_type_by_waiver_type = {
        CodeTraceabilityWaiverEntityType.REFINEMENT: (
            CodeTraceabilitySubjectType.REFINEMENT
        ),
        CodeTraceabilityWaiverEntityType.SPEC: CodeTraceabilitySubjectType.SPEC,
        CodeTraceabilityWaiverEntityType.CARD: CodeTraceabilitySubjectType.CARD,
    }
    subject_type = subject_type_by_waiver_type.get(entity_type)
    if subject_type is not None:
        return await _load_subject(
            board_id=board_id,
            subject_type=subject_type,
            subject_id=entity_id,
            uow=uow,
        )

    # SPEC_ENTITY uses the repository-wide canonical child identity.  The
    # embedded Spec id lets Core validate board ownership without introducing
    # a global child lookup or any source-code access.
    parts = entity_id.split(":", 3)
    if len(parts) != 4 or parts[0] != "spec":
        raise CodeEvidenceLinkInvalid(details={"reason": "spec_entity_ref_invalid"})
    _, spec_id, raw_entity_type, child_id = parts
    try:
        spec_entity_type = SpecEntityType(raw_entity_type)
    except ValueError as exc:
        raise CodeEvidenceLinkInvalid(
            details={"reason": "spec_entity_ref_invalid"}
        ) from exc
    if spec_entity_type is SpecEntityType.SPEC:
        raise CodeEvidenceLinkInvalid(details={"reason": "spec_entity_ref_invalid"})
    spec = await _load_subject(
        board_id=board_id,
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id=spec_id,
        uow=uow,
    )
    _require_spec_entity(spec, spec_entity_type, child_id)
    return spec


async def _selector_scope_for_start(
    submission: StartCodeInvestigationInput,
    *,
    uow: PulseUnitOfWork,
) -> str:
    if submission.subject_type is not CodeTraceabilitySubjectType.CARD:
        return selector_scope_digest_for_subject(
            board_id=submission.board_id,
            subject_type=submission.subject_type,
            subject_id=submission.subject_id,
            subject_version=submission.expected_subject_version,
        )
    page = await uow.services.code_traceability.list_targets(
        ImplementationTargetQuery(
            board_id=submission.board_id,
            card_id=submission.subject_id,
            lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
            limit=200,
        )
    )
    if page.next_cursor is not None:
        raise ImplementationTargetInvalid(
            details={"reason": "target_scope_limit_exceeded"}
        )
    return selector_scope_digest_for_card_targets(
        board_id=submission.board_id,
        card_id=submission.subject_id,
        card_version=submission.expected_subject_version,
        targets=tuple((item.id, item.revision) for item in page.items),
    )


class StartCodeInvestigationUseCase:
    def __init__(self, investigation_service: CodeInvestigationService) -> None:
        self._investigation_service = investigation_service

    async def execute(
        self,
        command: StartCodeInvestigationInput,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> StartCodeInvestigationResult:
        policy = await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.investigation.start",
        )
        await _require_attestor_policy(
            policy,
            actor,
            uow,
            board_id=command.board_id,
        )
        replay = await uow.services.code_investigations.resolve_request_replay(
            board_id=command.board_id,
            issued_to_actor_id=actor.actor_id,
            subject_type=command.subject_type,
            subject_id=command.subject_id,
            subject_version=command.expected_subject_version,
            idempotency_key=command.idempotency_key,
        )
        if replay is None:
            subject = await _load_subject(
                board_id=command.board_id,
                subject_type=command.subject_type,
                subject_id=command.subject_id,
                uow=uow,
            )
            if (
                _record_version(
                    subject,
                    entity_type=command.subject_type.value,
                )
                != command.expected_subject_version
            ):
                raise CodeInvestigationSubjectVersionConflict()
            capabilities = set(
                effective_required_capabilities_for_subject(
                    command.subject_type,
                    receipt_content=policy.settings.receipt_content,
                )
            )
            selector_scope_digest = await _selector_scope_for_start(command, uow=uow)
        else:
            capabilities = set(replay.request.required_capabilities)
            selector_scope_digest = replay.request.selector_scope_digest
        started = await self._investigation_service.start(
            command,
            actor_id=actor.actor_id,
            actor_kind=actor.actor_kind,
            selector_scope_digest=selector_scope_digest,
            required_capabilities=tuple(
                sorted(capabilities, key=lambda item: item.value)
            ),
            store=uow.services.code_investigations,
        )
        await _publish_mutation_event(
            uow,
            CodeInvestigationRequested,
            actor=actor,
            board_id=command.board_id,
            replayed=started.replayed,
            investigation_request_id=started.request.id,
            subject_type=started.request.subject_type.value,
            subject_id=started.request.subject_id,
            subject_version=started.request.subject_version,
            expected_head_generation=started.request.expected_head_generation,
            required_capability_count=len(started.request.required_capabilities),
            selector_scope_digest=started.request.selector_scope_digest,
            request_payload_sha256=started.request.request_payload_sha256,
        )
        if not started.replayed:
            await commit(uow)
        return StartCodeInvestigationResult(
            request=CodeInvestigationRequestView.from_domain(started.request),
            challenge_token=started.challenge_token,
            consumed_receipt_id=started.consumed_receipt_id,
        )


class SubmitCodeInvestigationReceiptUseCase:
    def __init__(self, investigation_service: CodeInvestigationService) -> None:
        self._investigation_service = investigation_service

    async def execute(
        self,
        command: CodeInvestigationReceiptSubmission,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> SubmittedCodeInvestigationReceiptResult:
        policy = await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.investigation.receipt_submit",
        )
        await _require_attestor_policy(
            policy,
            actor,
            uow,
            board_id=command.board_id,
        )
        request = await uow.services.code_investigations.get_request(
            board_id=command.board_id,
            request_id=command.request_id,
        )
        if request is None:
            exc = CodeInvestigationRequestNotFound()
            _observe_receipt_rejection(exc)
            raise exc
        if request.issued_to_actor_id != actor.actor_id:
            exc = CodeInvestigationAttestorMismatch()
            _observe_receipt_rejection(exc)
            raise exc
        replay = await uow.services.code_investigations.resolve_receipt_replay(
            board_id=command.board_id,
            attestor_actor_id=actor.actor_id,
            request_id=request.id,
            idempotency_key=command.idempotency_key,
        )
        if replay is None:
            subject = await _load_subject(
                board_id=command.board_id,
                subject_type=request.subject_type,
                subject_id=request.subject_id,
                uow=uow,
            )
            if (
                _record_version(
                    subject,
                    entity_type=request.subject_type.value,
                )
                != request.subject_version
            ):
                exc = CodeInvestigationSubjectVersionConflict()
                _observe_receipt_rejection(exc)
                raise exc
        try:
            submitted = await self._investigation_service.submit_receipt(
                command,
                actor_id=actor.actor_id,
                actor_kind=actor.actor_kind,
                freshness_seconds=policy.settings.preflight_freshness_seconds,
                store=uow.services.code_investigations,
            )
        except CodeTraceabilityContractError as exc:
            # This sink is deliberately non-durable and contains one typed,
            # bounded reason only. The rejected payload/challenge never enters
            # the transaction, event bus, audit record, logs, or metric labels.
            _observe_receipt_rejection(exc)
            raise
        receipt = submitted.receipt
        await _publish_mutation_event(
            uow,
            CodeInvestigationReceiptSubmitted,
            actor=actor,
            board_id=command.board_id,
            replayed=submitted.replayed,
            investigation_request_id=receipt.request_id,
            investigation_receipt_id=receipt.id,
            acceptance_status=receipt.acceptance_status.value,
            outcome=receipt.outcome.value,
            trust_level=receipt.trust_level.value,
            generation=receipt.generation,
            omission_count=receipt.omission_count,
            observation_sha256=receipt.observation_sha256,
            payload_sha256=receipt.payload_sha256,
        )
        if not submitted.replayed:
            await commit(uow)
            observe_code_traceability_metric(
                METRIC_CODE_INVESTIGATION_RECEIPT_TOTAL,
                labels={
                    "outcome": receipt.outcome.value,
                    "trust_level": receipt.trust_level.value,
                },
            )
            observe_code_traceability_metric(
                METRIC_CODE_INVESTIGATION_RECEIPT_AGE_SECONDS,
                value=max(
                    0.0,
                    (receipt.received_at - receipt.observed_at).total_seconds(),
                ),
                labels={"outcome": receipt.outcome.value},
            )
        return SubmittedCodeInvestigationReceiptResult(
            receipt=CodeInvestigationReceiptView.from_domain(submitted.receipt),
            generation=submitted.generation,
            head_revision=submitted.head_revision,
            replayed=submitted.replayed,
        )


class RevokeCodeInvestigationReceiptUseCase:
    def __init__(self, investigation_service: CodeInvestigationService) -> None:
        self._investigation_service = investigation_service

    async def execute(
        self,
        command: RevokeCodeInvestigationReceiptCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> object:
        await _load_policy(
            board_id=command.board_id,
            uow=uow,
        )
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.investigation.revoke",
        )
        existing = await uow.services.code_investigations.get_receipt_revocation(
            board_id=command.board_id,
            receipt_id=command.receipt_id,
        )
        result = await self._investigation_service.revoke_receipt(
            board_id=command.board_id,
            receipt_id=command.receipt_id,
            reason_code=command.reason_code,
            justification=command.justification,
            actor_id=actor.actor_id,
            store=uow.services.code_investigations,
        )
        await _publish_mutation_event(
            uow,
            CodeInvestigationReceiptRevoked,
            actor=actor,
            board_id=command.board_id,
            replayed=existing is not None,
            investigation_receipt_id=result.receipt_id,
            revocation_id=result.id,
            reason_code=result.reason_code,
            head_state="revoked",
        )
        if existing is None:
            await commit(uow)
        return result


class GetCodeInvestigationReceiptUseCase:
    def __init__(self, investigation_service: CodeInvestigationService) -> None:
        self._investigation_service = investigation_service

    async def execute(
        self,
        command: GetCodeInvestigationReceiptCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeInvestigationReceiptReadResult:
        await _load_policy(
            board_id=command.board_id,
            uow=uow,
        )
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.investigation.read",
        )
        inspected = await self._investigation_service.inspect_receipt(
            board_id=command.board_id,
            receipt_id=command.receipt_id,
            store=uow.services.code_investigations,
        )
        return CodeInvestigationReceiptReadResult(
            receipt=CodeInvestigationReceiptView.from_domain(inspected.receipt),
            currentness=inspected.currentness,
        )


class SubmitCodeEvidenceUseCase:
    def __init__(
        self,
        investigation_service: CodeInvestigationService,
        evidence_service: CodeEvidenceService,
    ) -> None:
        self._investigation_service = investigation_service
        self._evidence_service = evidence_service

    async def execute(
        self,
        command: CodeEvidenceSubmission,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeEvidenceMutationResult:
        policy = await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.evidence.submit",
        )
        await _require_attestor_policy(
            policy,
            actor,
            uow,
            board_id=command.board_id,
        )
        result = await self._evidence_service.submit(
            command,
            actor_id=actor.actor_id,
            actor_kind=actor.actor_kind,
            current_parent_version=await _evidence_parent_version_or_replay(
                command,
                actor=actor,
                uow=uow,
            ),
            minimum_trust=policy.minimum_trust,
            require_committed_state=policy.require_committed_state,
            investigation_service=self._investigation_service,
            investigation_store=uow.services.code_investigations,
            store=uow.services.code_traceability,
            receipt_content=policy.settings.receipt_content,
        )
        evidence = result.evidence
        await _publish_mutation_event(
            uow,
            CodeEvidenceCreated,
            actor=actor,
            board_id=command.board_id,
            replayed=result.replayed,
            evidence_id=evidence.id,
            investigation_receipt_id=evidence.investigation_receipt_id,
            parent_type=evidence.parent_type.value,
            parent_id=evidence.parent_id,
            lifecycle_status=evidence.lifecycle_status.value,
            attestation_state=evidence.attestation_state.value,
            payload_sha256=evidence.payload_sha256,
        )
        if not result.replayed:
            await commit(uow)
        return result


class SupersedeCodeEvidenceUseCase:
    def __init__(
        self,
        investigation_service: CodeInvestigationService,
        evidence_service: CodeEvidenceService,
    ) -> None:
        self._investigation_service = investigation_service
        self._evidence_service = evidence_service

    async def execute(
        self,
        command: CodeEvidenceSupersessionSubmission,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeEvidenceSupersessionCommitResult:
        policy = await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.evidence.supersede",
        )
        await _require_attestor_policy(
            policy,
            actor,
            uow,
            board_id=command.board_id,
        )
        result = await self._evidence_service.supersede(
            command,
            actor_id=actor.actor_id,
            actor_kind=actor.actor_kind,
            current_parent_version=await _evidence_parent_version_or_replay(
                command,
                actor=actor,
                uow=uow,
            ),
            minimum_trust=policy.minimum_trust,
            require_committed_state=policy.require_committed_state,
            investigation_service=self._investigation_service,
            investigation_store=uow.services.code_investigations,
            store=uow.services.code_traceability,
            receipt_content=policy.settings.receipt_content,
        )
        replacement = result.replacement
        await _publish_mutation_event(
            uow,
            CodeEvidenceSuperseded,
            actor=actor,
            board_id=command.board_id,
            replayed=result.replayed,
            superseded_evidence_id=result.predecessor.id,
            superseding_evidence_id=replacement.id,
            investigation_receipt_id=replacement.investigation_receipt_id,
            payload_sha256=replacement.payload_sha256,
        )
        if not result.replayed:
            await commit(uow)
        return result


class RevokeCodeEvidenceUseCase:
    def __init__(self, evidence_service: CodeEvidenceService | None = None) -> None:
        self._evidence_service = evidence_service or CodeEvidenceService()

    async def execute(
        self,
        command: CodeEvidenceRevokeInput,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeEvidenceRevocationResult:
        await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.evidence.revoke",
        )
        current = await uow.services.code_traceability.get_evidence(
            board_id=command.board_id,
            evidence_id=command.evidence_id,
        )
        exact_replay = (
            current is not None
            and current.lifecycle_status is CodeTraceabilityLifecycleStatus.REVOKED
            and current.revocation_reason == command.reason
        )
        if not exact_replay:
            await _require_evidence_card_parent_mutable(
                board_id=command.board_id,
                evidence_id=command.evidence_id,
                operation="code_traceability.evidence.revoke",
                uow=uow,
            )
        result = await self._evidence_service.revoke(
            command,
            store=uow.services.code_traceability,
        )
        await _publish_mutation_event(
            uow,
            CodeEvidenceRevoked,
            actor=actor,
            board_id=command.board_id,
            replayed=result.replayed,
            evidence_id=result.evidence.id,
            lifecycle_status=result.evidence.lifecycle_status.value,
            reason_code="operator_revoked",
            reason_sha256=code_traceability_event_digest(command.reason),
        )
        if not result.replayed:
            await commit(uow)
        return result


class LinkCodeEvidenceToSpecUseCase:
    def __init__(self, evidence_service: CodeEvidenceService) -> None:
        self._evidence_service = evidence_service

    async def execute(
        self,
        command: CodeEvidenceSpecLinkInput,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeEvidenceLinkMutationResult:
        await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.spec_link.create",
        )
        spec = await _load_subject(
            board_id=command.board_id,
            subject_type=CodeTraceabilitySubjectType.SPEC,
            subject_id=command.spec_id,
            uow=uow,
        )
        _require_spec_entity(spec, command.entity_type, command.entity_id)
        await _require_evidence_card_parent_mutable(
            board_id=command.board_id,
            evidence_id=command.evidence_id,
            operation="code_traceability.spec_link.create",
            uow=uow,
        )
        result = await self._evidence_service.link_to_spec(
            command,
            current_spec_version=_record_version(spec, entity_type="spec"),
            created_by=actor.actor_id,
            store=uow.services.code_traceability,
        )
        _advance_spec_version(
            spec,
            expected=command.expected_spec_version,
            next_version=result.spec_version,
        )
        if result.cleared_disposition is not None:
            cleared = result.cleared_disposition
            await _publish_mutation_event(
                uow,
                CodeEvidenceDispositionChanged,
                actor=actor,
                board_id=command.board_id,
                evidence_id=cleared.evidence_id,
                disposition_id=cleared.id,
                spec_id=cleared.spec_id,
                disposition=cleared.disposition.value,
                active_state="cleared",
                spec_version=result.spec_version,
            )
        await _publish_mutation_event(
            uow,
            CodeEvidenceLinked,
            actor=actor,
            board_id=command.board_id,
            evidence_id=result.link.evidence_id,
            link_id=result.link.id,
            spec_id=result.link.spec_id,
            entity_type=result.link.entity_type.value,
            entity_id=result.link.entity_id,
            relation_type=result.link.relation_type.value,
            evidence_content_sha256=result.link.evidence_content_sha256,
        )
        await commit(uow)
        return result


class UnlinkCodeEvidenceFromSpecUseCase:
    def __init__(self, evidence_service: CodeEvidenceService) -> None:
        self._evidence_service = evidence_service

    async def execute(
        self,
        command: CodeEvidenceSpecUnlinkInput,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeEvidenceUnlinkMutationResult:
        await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.spec_link.delete",
        )
        spec = await _load_subject(
            board_id=command.board_id,
            subject_type=CodeTraceabilitySubjectType.SPEC,
            subject_id=command.spec_id,
            uow=uow,
        )
        link = await uow.services.code_traceability.get_spec_link(
            board_id=command.board_id,
            link_id=command.link_id,
        )
        if link is not None:
            await _require_evidence_card_parent_mutable(
                board_id=command.board_id,
                evidence_id=link.evidence_id,
                operation="code_traceability.spec_link.delete",
                uow=uow,
            )
        result = await self._evidence_service.unlink_from_spec(
            command,
            current_spec_version=_record_version(spec, entity_type="spec"),
            store=uow.services.code_traceability,
        )
        _advance_spec_version(
            spec,
            expected=command.expected_spec_version,
            next_version=result.spec_version,
        )
        removed = result.removed_link
        await _publish_mutation_event(
            uow,
            CodeEvidenceUnlinked,
            actor=actor,
            board_id=command.board_id,
            evidence_id=removed.evidence_id,
            link_id=removed.id,
            spec_id=removed.spec_id,
            entity_type=removed.entity_type.value,
            entity_id=removed.entity_id,
            relation_type=removed.relation_type.value,
            reason_sha256=code_traceability_event_digest("operator_unlinked"),
        )
        await commit(uow)
        return result


class SetCodeEvidenceDispositionUseCase:
    def __init__(self, evidence_service: CodeEvidenceService) -> None:
        self._evidence_service = evidence_service

    async def execute(
        self,
        command: CodeEvidenceDispositionInput,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeEvidenceDispositionMutationResult:
        await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.spec_link.set_disposition",
        )
        spec = await _load_subject(
            board_id=command.board_id,
            subject_type=CodeTraceabilitySubjectType.SPEC,
            subject_id=command.spec_id,
            uow=uow,
        )
        await _require_evidence_card_parent_mutable(
            board_id=command.board_id,
            evidence_id=command.evidence_id,
            operation="code_traceability.spec_link.set_disposition",
            uow=uow,
        )
        result = await self._evidence_service.set_disposition(
            command,
            current_spec_version=_record_version(spec, entity_type="spec"),
            spec_status=getattr(spec, "status", ""),
            created_by=actor.actor_id,
            store=uow.services.code_traceability,
        )
        _advance_spec_version(
            spec,
            expected=command.expected_spec_version,
            next_version=result.spec_version,
        )
        await _publish_mutation_event(
            uow,
            CodeEvidenceDispositionChanged,
            actor=actor,
            board_id=command.board_id,
            evidence_id=result.disposition.evidence_id,
            disposition_id=result.disposition.id,
            spec_id=result.disposition.spec_id,
            disposition=result.disposition.disposition.value,
            active_state=("active" if result.disposition.active else "cleared"),
            spec_version=result.spec_version,
        )
        await commit(uow)
        return result


class ClearCodeEvidenceDispositionUseCase:
    def __init__(self, evidence_service: CodeEvidenceService) -> None:
        self._evidence_service = evidence_service

    async def execute(
        self,
        command: CodeEvidenceDispositionClearInput,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeEvidenceDispositionMutationResult:
        await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.spec_link.set_disposition",
        )
        spec = await _load_subject(
            board_id=command.board_id,
            subject_type=CodeTraceabilitySubjectType.SPEC,
            subject_id=command.spec_id,
            uow=uow,
        )
        await _require_evidence_card_parent_mutable(
            board_id=command.board_id,
            evidence_id=command.evidence_id,
            operation="code_traceability.spec_link.clear_disposition",
            uow=uow,
        )
        result = await self._evidence_service.clear_disposition(
            command,
            current_spec_version=_record_version(spec, entity_type="spec"),
            cleared_by=actor.actor_id,
            store=uow.services.code_traceability,
        )
        _advance_spec_version(
            spec,
            expected=command.expected_spec_version,
            next_version=result.spec_version,
        )
        await _publish_mutation_event(
            uow,
            CodeEvidenceDispositionChanged,
            actor=actor,
            board_id=command.board_id,
            evidence_id=result.disposition.evidence_id,
            disposition_id=result.disposition.id,
            spec_id=result.disposition.spec_id,
            disposition=result.disposition.disposition.value,
            active_state=("active" if result.disposition.active else "cleared"),
            spec_version=result.spec_version,
        )
        await commit(uow)
        return result


async def _preview_spec_code_evidence_rebase(
    command: SpecCodeEvidenceRebasePreviewInput,
    *,
    service: SpecCodeEvidenceRebaseService,
    uow: PulseUnitOfWork,
) -> tuple[object, SpecCodeEvidenceRebasePlan]:
    spec = await _load_subject(
        board_id=command.board_id,
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id=command.spec_id,
        uow=uow,
    )
    refinement_id = getattr(spec, "refinement_id", None)
    current_version = getattr(spec, "source_refinement_version", None)
    if not isinstance(refinement_id, str) or type(current_version) is not int:
        raise CodeEvidenceLinkInvalid(details={"reason": "spec_rebase_scope_invalid"})
    current_snapshot = await uow.services.refinements.get_snapshot(
        refinement_id,
        current_version,
    )
    target_snapshot = await uow.services.refinements.get_snapshot(
        refinement_id,
        command.target_refinement_version,
    )
    if current_snapshot is None or target_snapshot is None:
        raise CodeEvidenceLinkInvalid(
            details={"reason": "refinement_snapshot_not_found"}
        )
    plan = await service.preview(
        board_id=command.board_id,
        spec=spec,
        current_snapshot=current_snapshot,
        target_snapshot=target_snapshot,
        target_refinement_version=command.target_refinement_version,
        expected_spec_version=command.expected_spec_version,
        store=uow.services.code_traceability,
    )
    return spec, plan


class PreviewSpecCodeEvidenceRebaseUseCase:
    def __init__(
        self,
        service: SpecCodeEvidenceRebaseService | None = None,
    ) -> None:
        self._service = service or SpecCodeEvidenceRebaseService()

    async def execute(
        self,
        command: SpecCodeEvidenceRebasePreviewInput,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> SpecCodeEvidenceRebasePlan:
        await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.spec_link.rebase",
        )
        _spec, plan = await _preview_spec_code_evidence_rebase(
            command,
            service=self._service,
            uow=uow,
        )
        return plan


class ApplySpecCodeEvidenceRebaseUseCase:
    def __init__(
        self,
        service: SpecCodeEvidenceRebaseService | None = None,
    ) -> None:
        self._service = service or SpecCodeEvidenceRebaseService()

    async def execute(
        self,
        command: SpecCodeEvidenceRebaseApplyInput,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> SpecCodeEvidenceRebaseResult:
        await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.spec_link.rebase",
        )
        spec, plan = await _preview_spec_code_evidence_rebase(
            command,
            service=self._service,
            uow=uow,
        )
        stale_link_ids = set(plan.stale_link_ids)
        invalid_disposition_ids = set(plan.invalid_disposition_ids)
        stale_links = tuple(
            item
            for item in await uow.services.code_traceability.list_spec_links(
                board_id=command.board_id,
                spec_id=command.spec_id,
            )
            if item.id in stale_link_ids
        )
        invalid_dispositions = tuple(
            item
            for item in await uow.services.code_traceability.list_spec_dispositions(
                board_id=command.board_id,
                spec_id=command.spec_id,
                active_only=True,
            )
            if item.id in invalid_disposition_ids
        )
        result = await self._service.apply(
            plan,
            expected_preview_sha256=command.preview_sha256,
            actor_id=actor.actor_id,
            store=uow.services.code_traceability,
        )
        setattr(
            spec,
            "source_refinement_snapshot_id",
            plan.target_refinement_snapshot_id,
        )
        setattr(spec, "source_refinement_version", plan.target_refinement_version)
        _advance_spec_version(
            spec,
            expected=command.expected_spec_version,
            next_version=result.spec_version,
        )
        for link in stale_links:
            await _publish_mutation_event(
                uow,
                CodeEvidenceUnlinked,
                actor=actor,
                board_id=command.board_id,
                evidence_id=link.evidence_id,
                link_id=link.id,
                spec_id=link.spec_id,
                entity_type=link.entity_type.value,
                entity_id=link.entity_id,
                relation_type=link.relation_type.value,
                reason_sha256=code_traceability_event_digest(
                    "spec_code_evidence_rebase"
                ),
            )
        for disposition in invalid_dispositions:
            await _publish_mutation_event(
                uow,
                CodeEvidenceDispositionChanged,
                actor=actor,
                board_id=command.board_id,
                evidence_id=disposition.evidence_id,
                disposition_id=disposition.id,
                spec_id=disposition.spec_id,
                disposition=disposition.disposition.value,
                active_state="cleared",
                spec_version=result.spec_version,
            )
        await commit(uow)
        return result


class CreateImplementationTargetUseCase:
    def __init__(
        self,
        investigation_service: CodeInvestigationService,
        target_service: ImplementationTargetService,
    ) -> None:
        self._investigation_service = investigation_service
        self._target_service = target_service

    async def execute(
        self,
        command: ImplementationTargetCreateInput,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ImplementationTargetMutationResult:
        policy = await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation=(
                "code_traceability.target.suggest"
                if actor.actor_kind == "agent"
                else "code_traceability.target.create"
            ),
        )
        card, spec = await _load_card_and_spec(
            board_id=command.board_id,
            card_id=command.card_id,
            uow=uow,
        )
        for link in command.spec_links:
            _require_spec_entity(spec, link.entity_type, link.entity_id)
        result = await self._target_service.create(
            command,
            created_by=actor.actor_id,
            spec_id=str(getattr(spec, "id")),
            card_status=getattr(card, "status", ""),
            current_card_version=_record_version(card, entity_type="card"),
            current_spec_version=_record_version(spec, entity_type="spec"),
            minimum_trust=policy.minimum_trust,
            require_committed_state=policy.require_committed_state,
            investigation_service=self._investigation_service,
            investigation_store=uow.services.code_investigations,
            store=uow.services.code_traceability,
        )
        await _publish_mutation_event(
            uow,
            ImplementationTargetCreated,
            actor=actor,
            board_id=command.board_id,
            replayed=result.replayed,
            target_id=result.target.id,
            card_id=result.target.card_id,
            lifecycle_status=result.target.lifecycle_status.value,
            revision=result.target.revision,
            payload_sha256=canonical_code_traceability_sha256(result.target),
        )
        await commit(uow)
        return result


class UpdateImplementationTargetUseCase:
    def __init__(self, target_service: ImplementationTargetService) -> None:
        self._target_service = target_service

    async def execute(
        self,
        command: ImplementationTargetUpdateInput,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ImplementationTargetMutationResult:
        await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.target.edit",
        )
        card, spec = await _load_card_and_spec(
            board_id=command.board_id,
            card_id=command.card_id,
            uow=uow,
        )
        for link in command.spec_links or ():
            _require_spec_entity(spec, link.entity_type, link.entity_id)
        result = await self._target_service.update(
            command,
            card_status=getattr(card, "status", ""),
            spec_id=str(getattr(spec, "id")),
            updated_by=actor.actor_id,
            store=uow.services.code_traceability,
        )
        target = result.target
        event_class: type[CodeTraceabilityDomainEvent]
        if target.lifecycle_status is CodeTraceabilityLifecycleStatus.REVOKED:
            event_class = ImplementationTargetRevoked
            event_metadata = {
                "target_id": target.id,
                "card_id": target.card_id,
                "lifecycle_status": target.lifecycle_status.value,
                "revision": target.revision,
                "reason_sha256": code_traceability_event_digest(command.change_reason),
            }
        else:
            event_class = ImplementationTargetUpdated
            event_metadata = {
                "target_id": target.id,
                "card_id": target.card_id,
                "lifecycle_status": target.lifecycle_status.value,
                "previous_revision": command.expected_revision,
                "revision": target.revision,
                "change_reason_sha256": code_traceability_event_digest(
                    command.change_reason
                ),
            }
        await _publish_mutation_event(
            uow,
            event_class,
            actor=actor,
            board_id=command.board_id,
            replayed=result.replayed,
            **event_metadata,
        )
        await commit(uow)
        return result


class SubmitImplementationTargetResolutionUseCase:
    def __init__(
        self,
        investigation_service: CodeInvestigationService,
        target_service: ImplementationTargetService,
    ) -> None:
        self._investigation_service = investigation_service
        self._target_service = target_service

    async def execute(
        self,
        command: ImplementationTargetResolutionSubmission,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ImplementationTargetResolutionResult:
        policy = await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.target.resolution_submit",
        )
        await _require_attestor_policy(
            policy,
            actor,
            uow,
            board_id=command.board_id,
        )
        result = await self._target_service.submit_resolution(
            command,
            actor_id=actor.actor_id,
            actor_kind=actor.actor_kind,
            current_card_version=await _resolution_card_version_or_replay(
                command,
                actor=actor,
                uow=uow,
            ),
            minimum_trust=policy.minimum_trust,
            require_committed_state=policy.require_committed_state,
            investigation_service=self._investigation_service,
            investigation_store=uow.services.code_investigations,
            store=uow.services.code_traceability,
        )
        resolution = result.resolution
        await _publish_mutation_event(
            uow,
            ImplementationTargetResolutionSubmitted,
            actor=actor,
            board_id=command.board_id,
            replayed=result.replayed,
            target_id=resolution.target_id,
            resolution_id=resolution.id,
            investigation_receipt_id=resolution.investigation_receipt_id,
            resolution_state=resolution.state.value,
            target_revision=resolution.target_revision,
            receipt_generation=resolution.receipt_generation,
            candidate_count=resolution.candidate_count,
            selector_fingerprint=resolution.selector_fingerprint,
            payload_sha256=resolution.payload_sha256,
        )
        if not result.replayed:
            await commit(uow)
        return result


class SubmitImplementationTargetExecutionUseCase:
    def __init__(
        self,
        investigation_service: CodeInvestigationService,
        target_service: ImplementationTargetService,
    ) -> None:
        self._investigation_service = investigation_service
        self._target_service = target_service

    async def execute(
        self,
        command: ImplementationTargetExecutionSubmission,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ImplementationTargetExecutionResult:
        policy = await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.target.execution_submit",
        )
        await _require_attestor_policy(
            policy,
            actor,
            uow,
            board_id=command.board_id,
        )
        result = await self._target_service.submit_execution(
            command,
            actor_id=actor.actor_id,
            actor_kind=actor.actor_kind,
            current_card_version=await _execution_card_version_or_replay(
                command,
                actor=actor,
                uow=uow,
            ),
            minimum_trust=policy.minimum_trust,
            require_committed_state=policy.require_committed_state,
            investigation_service=self._investigation_service,
            investigation_store=uow.services.code_investigations,
            store=uow.services.code_traceability,
        )
        record = result.record
        await _publish_mutation_event(
            uow,
            ImplementationTargetExecutionReceiptSubmitted,
            actor=actor,
            board_id=command.board_id,
            replayed=result.replayed,
            execution_record_id=record.id,
            target_id=record.target_id,
            card_id=record.card_id,
            result_investigation_receipt_id=(record.result_investigation_receipt_id),
            disposition=record.disposition.value,
            target_revision=record.target_revision,
            payload_sha256=record.payload_sha256,
        )
        if not result.replayed:
            await commit(uow)
        return result


class GetCodeEvidenceUseCase:
    async def execute(
        self,
        command: GetCodeEvidenceCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeEvidenceView:
        await _load_policy(
            board_id=command.board_id,
            uow=uow,
        )
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.evidence.read",
        )
        evidence = await uow.services.code_traceability.get_evidence(
            board_id=command.board_id,
            evidence_id=command.evidence_id,
        )
        if evidence is None:
            raise EntityNotFoundError("code_evidence", command.evidence_id)
        return CodeEvidenceView.project(evidence, profile=command.profile)


class ListCodeEvidenceUseCase:
    async def execute(
        self,
        command: ListCodeEvidenceCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeTraceabilityPage[CodeEvidenceView]:
        await _load_policy(
            board_id=command.query.board_id,
            uow=uow,
        )
        await _authorize(
            actor,
            uow,
            board_id=command.query.board_id,
            operation="code_traceability.evidence.read",
        )
        page = await uow.services.code_traceability.list_evidence(command.query)
        return CodeTraceabilityPage(
            items=tuple(
                CodeEvidenceView.project(item, profile=command.profile)
                for item in page.items
            ),
            limit=page.limit,
            next_cursor=page.next_cursor,
        )


class GetImplementationTargetUseCase:
    async def execute(
        self,
        command: GetImplementationTargetCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ImplementationTargetView:
        await _load_policy(
            board_id=command.board_id,
            uow=uow,
        )
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.target.read",
        )
        target = await uow.services.code_traceability.get_target(
            board_id=command.board_id,
            target_id=command.target_id,
        )
        if target is None:
            raise EntityNotFoundError("implementation_target", command.target_id)
        return ImplementationTargetView.from_domain(target)


class ListImplementationTargetsUseCase:
    async def execute(
        self,
        command: ImplementationTargetQuery,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeTraceabilityPage[ImplementationTarget]:
        await _load_policy(
            board_id=command.board_id,
            uow=uow,
        )
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.target.read",
        )
        return await uow.services.code_traceability.list_targets(command)


class GetImplementationOverlapsUseCase:
    def __init__(self, overlap_service: CodeOverlapService | None = None) -> None:
        self._overlap_service = overlap_service or CodeOverlapService()

    async def execute(
        self,
        command: TargetOverlapQuery,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> tuple[TargetOverlap, ...]:
        await _load_policy(
            board_id=command.board_id,
            uow=uow,
        )
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.overlap.read",
        )
        await _load_subject(
            board_id=command.board_id,
            subject_type=CodeTraceabilitySubjectType.CARD,
            subject_id=command.card_id,
            uow=uow,
        )
        return await self._overlap_service.get_overlaps(
            command,
            read_port=uow.services.code_traceability_read,
        )


class AcknowledgeImplementationOverlapUseCase:
    def __init__(self, overlap_service: CodeOverlapService | None = None) -> None:
        self._overlap_service = overlap_service or CodeOverlapService()

    async def execute(
        self,
        command: TargetOverlapAcknowledgementInput,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> TargetOverlapAcknowledgementResult:
        await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.overlap.acknowledge",
        )
        card = await _load_subject(
            board_id=command.board_id,
            subject_type=CodeTraceabilitySubjectType.CARD,
            subject_id=command.card_id,
            uow=uow,
        )
        existing_acknowledgements = (
            await uow.services.code_traceability.list_overlap_acknowledgements(
                board_id=command.board_id,
                card_id=command.card_id,
            )
        )
        exact_replay = any(
            {item.target_a_id, item.target_b_id}
            == {command.target_a_id, command.target_b_id}
            and {item.resolution_a_id, item.resolution_b_id}
            == {command.resolution_a_id, command.resolution_b_id}
            and item.disposition is command.disposition
            and item.justification == command.justification
            and item.created_by == actor.actor_id
            for item in existing_acknowledgements
        )
        if not exact_replay:
            require_card_operational_mutation_allowed(
                card,
                operation="code_traceability.overlap.acknowledge",
            )
        result = await self._overlap_service.acknowledge(
            command,
            created_by=actor.actor_id,
            store=uow.services.code_traceability,
        )
        acknowledgement = result.acknowledgement
        overlap = result.overlap
        await _publish_mutation_event(
            uow,
            ImplementationOverlapAcknowledged,
            actor=actor,
            board_id=command.board_id,
            replayed=result.replayed,
            acknowledgement_id=acknowledgement.id,
            target_a_id=acknowledgement.target_a_id,
            target_b_id=acknowledgement.target_b_id,
            resolution_a_id=acknowledgement.resolution_a_id,
            resolution_b_id=acknowledgement.resolution_b_id,
            disposition=acknowledgement.disposition.value,
            overlap_fingerprint=canonical_code_traceability_sha256(
                {
                    "target_a_id": overlap.target_a_id,
                    "target_b_id": overlap.target_b_id,
                    "resolution_a_id": overlap.resolution_a_id,
                    "resolution_b_id": overlap.resolution_b_id,
                    "severity": overlap.severity,
                    "reason_code": overlap.reason_code,
                }
            ),
        )
        if not result.replayed:
            await commit(uow)
        return result


class MarkCodeTraceabilityNotApplicableUseCase:
    def __init__(
        self,
        waiver_service: CodeTraceabilityWaiverService | None = None,
    ) -> None:
        self._waiver_service = waiver_service or CodeTraceabilityWaiverService()

    async def execute(
        self,
        command: CodeTraceabilityWaiverInput,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeTraceabilityWaiverMutationResult:
        await _load_policy(board_id=command.board_id, uow=uow)
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.waiver.create",
        )
        entity = await _load_waiver_entity(
            board_id=command.board_id,
            entity_type=command.entity_type,
            entity_id=command.entity_id,
            uow=uow,
        )
        if command.entity_type is CodeTraceabilityWaiverEntityType.CARD:
            existing = await uow.services.code_traceability.get_active_waiver(
                board_id=command.board_id,
                entity_type=command.entity_type,
                entity_id=command.entity_id,
                scope=command.scope,
            )
            exact_replay = (
                existing is not None
                and existing.reason_code is command.reason_code
                and existing.justification == command.justification
                and existing.created_by == actor.actor_id
            )
            if not exact_replay:
                require_card_operational_mutation_allowed(
                    entity,
                    operation="code_traceability.waiver.create",
                )
        result = await self._waiver_service.mark_not_applicable(
            command,
            created_by=actor.actor_id,
            store=uow.services.code_traceability,
        )
        await _publish_mutation_event(
            uow,
            CodeTraceabilityWaiverCreated,
            actor=actor,
            board_id=command.board_id,
            replayed=result.replayed,
            waiver_id=result.waiver.id,
            subject_type=result.waiver.entity_type.value,
            subject_id=result.waiver.entity_id,
            subject_version=_record_version(
                entity,
                entity_type=(
                    "card"
                    if result.waiver.entity_type
                    is CodeTraceabilityWaiverEntityType.CARD
                    else "spec"
                ),
            ),
            waiver_state="active",
            justification_sha256=code_traceability_event_digest(
                result.waiver.justification
            ),
        )
        if not result.replayed:
            await commit(uow)
        return result


class ClearCodeTraceabilityNotApplicableUseCase:
    def __init__(
        self,
        waiver_service: CodeTraceabilityWaiverService | None = None,
    ) -> None:
        self._waiver_service = waiver_service or CodeTraceabilityWaiverService()

    async def execute(
        self,
        command: CodeTraceabilityWaiverClearInput,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeTraceabilityWaiverMutationResult:
        await _load_policy(
            board_id=command.board_id,
            uow=uow,
        )
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.waiver.clear",
        )
        existing = await uow.services.code_traceability.get_waiver(
            board_id=command.board_id,
            waiver_id=command.waiver_id,
        )
        entity = None
        if existing is not None:
            entity = await _load_waiver_entity(
                board_id=command.board_id,
                entity_type=existing.entity_type,
                entity_id=existing.entity_id,
                uow=uow,
            )
            if (
                existing.active
                and existing.entity_type is CodeTraceabilityWaiverEntityType.CARD
            ):
                require_card_operational_mutation_allowed(
                    entity,
                    operation="code_traceability.waiver.clear",
                )
        result = await self._waiver_service.clear_not_applicable(
            command,
            cleared_by=actor.actor_id,
            store=uow.services.code_traceability,
        )
        if entity is None:
            entity = await _load_waiver_entity(
                board_id=command.board_id,
                entity_type=result.waiver.entity_type,
                entity_id=result.waiver.entity_id,
                uow=uow,
            )
        await _publish_mutation_event(
            uow,
            CodeTraceabilityWaiverCleared,
            actor=actor,
            board_id=command.board_id,
            replayed=result.replayed,
            waiver_id=result.waiver.id,
            subject_type=result.waiver.entity_type.value,
            subject_id=result.waiver.entity_id,
            subject_version=_record_version(
                entity,
                entity_type=(
                    "card"
                    if result.waiver.entity_type
                    is CodeTraceabilityWaiverEntityType.CARD
                    else "spec"
                ),
            ),
            waiver_state="cleared",
            reason_sha256=code_traceability_event_digest("operator_cleared"),
        )
        if not result.replayed:
            await commit(uow)
        return result


class GetCodeTraceabilityProjectionUseCase:
    """Project gate readiness from Pulse-owned structured records only."""

    def __init__(
        self,
        projection_service: CodeTraceabilityProjectionService | None = None,
    ) -> None:
        self._projection_service = (
            projection_service or CodeTraceabilityProjectionService()
        )

    async def execute(
        self,
        query: CodeTraceabilityProjectionQuery,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeTraceabilityProjection:
        policy = await _load_policy(
            board_id=query.board_id,
            uow=uow,
        )
        for operation in (
            "code_traceability.investigation.read",
            "code_traceability.evidence.read",
            "code_traceability.target.read",
            "code_traceability.overlap.read",
        ):
            await _authorize(
                actor,
                uow,
                board_id=query.board_id,
                operation=operation,
            )
        subject = await _load_subject(
            board_id=query.board_id,
            subject_type=query.subject_type,
            subject_id=query.subject_id,
            uow=uow,
        )
        current_version = _record_version(
            subject,
            entity_type=query.subject_type.value,
        )
        if current_version != query.subject_version:
            raise CodeInvestigationSubjectVersionConflict(
                details={
                    "subject_type": query.subject_type.value,
                    "subject_id": query.subject_id,
                    "expected_subject_version": query.subject_version,
                    "current_subject_version": current_version,
                }
            )
        context = await self._projection_service.load_context(
            query,
            read_port=uow.services.code_traceability_read,
        )
        card_type = "normal"
        dependency_card_ids: tuple[str, ...] = ()
        blocking_card_ids: tuple[str, ...] = ()
        if query.subject_type is CodeTraceabilitySubjectType.CARD:
            raw_card_type = getattr(subject, "card_type", None)
            card_type = str(getattr(raw_card_type, "value", raw_card_type or "normal"))
            dependencies = await uow.services.cards.get_dependencies(query.subject_id)
            dependency_card_ids = tuple(
                sorted(
                    item.id
                    for item in dependencies
                    if isinstance(getattr(item, "id", None), str)
                )
            )
            external_card_ids = {
                item.card_id
                for item in context.targets
                if item.card_id != query.subject_id
            }
            blocking: list[str] = []
            for card_id in sorted(external_card_ids):
                external = await uow.services.cards.get_card(card_id)
                status = getattr(external, "status", None)
                if str(getattr(status, "value", status)).lower() in {
                    "started",
                    "in_progress",
                    "validation",
                    "rejected",
                }:
                    blocking.append(card_id)
            blocking_card_ids = tuple(blocking)
        return self._projection_service.project_context(
            context,
            policy.settings,
            card_type=card_type,
            dependency_card_ids=dependency_card_ids,
            blocking_card_ids=blocking_card_ids,
            referenced_evidence_ids=(
                extract_code_evidence_references(getattr(subject, "analysis", None))
                if query.subject_type is CodeTraceabilitySubjectType.REFINEMENT
                else ()
            ),
            skip_evidence_coverage=(
                query.subject_type is CodeTraceabilitySubjectType.SPEC
                and bool(getattr(subject, "skip_code_evidence_coverage", False))
            ),
        )


class ListCodeInvestigationReceiptsUseCase:
    async def execute(
        self,
        command: CodeInvestigationReceiptQuery,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CodeTraceabilityPage[Any]:
        await _load_policy(
            board_id=command.board_id,
            uow=uow,
        )
        await _authorize(
            actor,
            uow,
            board_id=command.board_id,
            operation="code_traceability.investigation.read",
        )
        return await uow.services.code_investigations.list_receipts(command)


__all__ = [
    "AcknowledgeImplementationOverlapUseCase",
    "ApplySpecCodeEvidenceRebaseUseCase",
    "ClearCodeEvidenceDispositionUseCase",
    "ClearCodeTraceabilityNotApplicableUseCase",
    "CreateImplementationTargetUseCase",
    "GetCodeEvidenceCommand",
    "GetCodeEvidenceUseCase",
    "GetCodeInvestigationReceiptCommand",
    "GetCodeInvestigationReceiptUseCase",
    "GetCodeTraceabilityProjectionUseCase",
    "GetImplementationTargetCommand",
    "GetImplementationTargetUseCase",
    "GetImplementationOverlapsUseCase",
    "LinkCodeEvidenceToSpecUseCase",
    "ListCodeEvidenceUseCase",
    "ListCodeEvidenceCommand",
    "ListCodeInvestigationReceiptsUseCase",
    "ListImplementationTargetsUseCase",
    "MarkCodeTraceabilityNotApplicableUseCase",
    "PreviewSpecCodeEvidenceRebaseUseCase",
    "RevokeCodeInvestigationReceiptCommand",
    "RevokeCodeInvestigationReceiptUseCase",
    "RevokeCodeEvidenceUseCase",
    "SetCodeEvidenceDispositionUseCase",
    "CodeInvestigationReceiptReadResult",
    "StartCodeInvestigationUseCase",
    "SubmitCodeEvidenceUseCase",
    "SubmitCodeInvestigationReceiptUseCase",
    "SubmitImplementationTargetExecutionUseCase",
    "SubmitImplementationTargetResolutionUseCase",
    "SubmittedCodeInvestigationReceiptResult",
    "SupersedeCodeEvidenceUseCase",
    "UnlinkCodeEvidenceFromSpecUseCase",
    "UpdateImplementationTargetUseCase",
]
