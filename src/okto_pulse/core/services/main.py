"""Service layer for business logic."""

from __future__ import annotations

import hashlib
import logging
import secrets
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections.abc import Mapping
from typing import Any, Callable

from okto_pulse.core.application.scope import QueryScope
from okto_pulse.core.application.artifact_propagation import (
    propagate_artifacts,
    validate_artifact_selections,
)
from okto_pulse.core.application.history_pagination import (
    validate_history_window,
    validate_snapshot_version,
)
from okto_pulse.core.domain.amendment_eligibility import evaluate_amendment_eligibility
from okto_pulse.core.domain.card_transition import (
    CARD_STATUS_ORDER,
    CardTransitionFacts,
    PendingScenario,
    archived_card_block,
    bug_regression_evidence_block,
    bug_regression_gate_applies,
    spec_maturity_block,
    sprint_assignment_block,
    test_completion_block,
    validation_gate_block,
)
from okto_pulse.core.domain.card_completion import (
    CardCompletionOutcome,
    CardRejectionCause,
    CardRejectionKind,
    CardRejectionRecord,
    CompletionGateFailure,
    TaskValidationOutcome,
    current_rejection_cause,
    decide_card_completion,
)
from okto_pulse.core.domain.enums import (
    CardStatus,
    CardType,
    IdeationComplexity,
    IdeationStatus,
    RefinementStatus,
    SpecStatus,
    SprintLaneType,
    SprintStatus,
    StoryStatus,
)
from okto_pulse.core.domain.code_traceability import (
    CodeInvestigationCurrentnessUnknown,
    CodeTraceabilityContextScope,
    CodeTraceabilityContractError,
    CodeTraceabilityEnforcement,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilityProjectionProfile,
    CodeTraceabilitySubjectType,
)
from okto_pulse.core.domain.knowledge_governance import (
    normalize_knowledge_governance_metadata,
)
from okto_pulse.core.domain.human_validation_cycle import (
    LifecycleTransitionConflictError,
    is_current_edition,
    next_lifecycle_edition,
    require_draft_mutation,
)
from okto_pulse.core.domain.spec_validation import (
    RequirementLintRequired,
    SpecValidationEditionConflict,
    SpecValidationGateNotReady,
    SpecValidationVersionConflict,
)
from okto_pulse.core.domain.spec_dependency import (
    transition_starts_card_execution,
    transition_starts_spec_execution,
)
from okto_pulse.core.domain.sdlc_registry import (
    is_internal_transition_allowed,
    is_transition_allowed,
    transition_contracts,
    transition_map,
    transition_requires_policy_compliance,
)
from okto_pulse.core.infra.storage import get_storage_provider
from okto_pulse.core.ports.application_persistence import (
    ApplicationFilter,
    ApplicationOperator,
    ApplicationQuery,
    ApplicationRecord,
    ApplicationRecordConflictError,
    GroupCountRequest,
    PageRequest,
    PageResult,
    get_application_persistence_port,
)
from okto_pulse.core.ports.card_repository import (
    ColumnResequenceOp,
    get_card_repository_port,
)
from okto_pulse.core.ports.knowledge_propagation import (
    KnowledgePropagationPort,
)

from okto_pulse.core.models.schemas import (
    AgentCreate,
    AgentUpdate,
    BoardCreate,
    BoardShareCreate,
    BoardShareUpdate,
    BoardUpdate,
    CardCreate,
    CardMove,
    CardUpdate,
    CommentCreate,
    CommentUpdate,
    GuidelineCreate,
    GuidelineUpdate,
    IdeationCreate,
    IdeationKnowledgeCreate,
    IdeationKnowledgeUpdate,
    IdeationMove,
    IdeationQAAnswer,
    IdeationQACreate,
    IdeationUpdate,
    QACreate,
    QAAnswer,
    RefinementCreate,
    RefinementKnowledgeCreate,
    RefinementKnowledgeUpdate,
    RefinementMove,
    RefinementQAAnswer,
    RefinementQACreate,
    RefinementUpdate,
    SpecCreate,
    SpecKnowledgeCreate,
    SpecKnowledgeUpdate,
    SpecMove,
    SpecQAAnswer,
    SpecQACreate,
    SpecUpdate,
    StoryConversionRequest,
    StoryCreate,
    StoryMove,
    StoryUpdate,
    SprintCreate,
    SprintMove,
    SprintUpdate,
    TopicCreate,
    TopicUpdate,
    project_task_validation_public,
)
from okto_pulse.core.services.application_schemas import (
    PersistedTestScenarioSpecUpdate,
)
from okto_pulse.core.services.ambiguity_assessment import (
    AmbiguityGateError as AmbiguityGateError,
    AmbiguityGateService,
    resolve_ambiguity_gate_configuration,
)
from okto_pulse.core.services.activity_log import (
    activity_log_changes,
    activity_log_value,
)
from okto_pulse.core.services.amendment_revision import AmendmentRevisionService
from okto_pulse.core.services.analytics_service import (
    _structured_ref_text,
    resolve_linked_criteria_to_ids,
    resolve_linked_criteria_to_indices,
    resolve_linked_fr_indices,
)
from okto_pulse.core.services.board_governance import (
    BoardGovernanceService,
    QA_SELF_ANSWER_DENIED_ACTION,
    QASelfAnsweringNotAllowedError,
    build_qa_self_answer_denied_details,
)
from okto_pulse.core.services.qa_selection import validate_choice_selection
from okto_pulse.core.services.bug_regression_observability import (
    emit_no_unlock_invariant,
    observe_bug_regression_resolution,
    record_bug_regression_decision,
)
from okto_pulse.core.services.bug_regression_scenarios import (
    AmendmentLineageFact,
    BugRegressionCoverageState,
    BugRegressionGateValidator,
    BugRegressionScenarioEligibilityResolver,
    CoverageConfirmationFact,
    evaluate_coverage_confirmation_consumability,
)
from okto_pulse.core.services.bug_workflow_remediation import (
    BugWorkflowRemediationMessageBuilder,
)
from okto_pulse.core.services.card_errors import CardOperationError
from okto_pulse.core.services.card_operational_freeze import (
    require_card_operational_mutation_allowed,
)
from okto_pulse.core.services.cancellation import apply_cancellation_policy
from okto_pulse.core.services.card_traceability import (
    TraceabilityTargetNotFoundError,
    link_card_traceability,
)
from okto_pulse.core.ports.code_traceability import (
    CodeEvidenceQuery,
    CodeTraceabilityAdapterMissing,
    CodeTraceabilityProjectionQuery,
)
from okto_pulse.core.services.code_traceability_gate import (
    CodeTraceabilityGateBlocker,
    CodeTraceabilityGateEvaluation,
    CodeTraceabilityProjectionService,
    EvidenceDispositionCoverage,
    TargetEntityCoverage,
    extract_code_evidence_references,
    phases_for_transition,
    resolve_code_evidence_coverage_skip,
    resolve_code_traceability_settings,
)
from okto_pulse.core.services.critical_context_guard import (
    CRITICAL_CONTEXT_DECISION_ACTION,
    CriticalAction,
    CriticalContextDecision,
    FullContextCriticalActionGuard,
    FullContextGuardError,
    build_default_full_context_resolvers,
)
from okto_pulse.core.services.governance_observability import (
    build_board_governance_setting_changed_details,
    build_board_missing_context_warning_details,
    emit_governance_metric,
)
from okto_pulse.core.domain.knowledge_fingerprint import (
    knowledge_content_sha256,
)
from okto_pulse.core.services.reference_resolution import (
    compile_ideation_parent_context,
)
from okto_pulse.core.services.resource_gate import ResourceGateService
from okto_pulse.core.services.legacy_knowledge_write_guard import (
    require_legacy_card_knowledge_write_allowed,
)
from okto_pulse.core.services.reviewer_separation import (
    evaluate_reviewer_separation,
    evaluate_task_reviewer_separation,
)
from okto_pulse.core.services.sprint_scope import (
    SprintScopeResolver,
    completion_blockers,
)
from okto_pulse.core.services.spec_entity_canonicalization import (
    canonicalize_fr_ac as canonicalize_fr_ac,  # noqa: F401 - compatibility
    canonicalize_spec_requirement_fields,
)
from okto_pulse.core.services.spec_resource_propagation import (
    SpecResourcePropagationService,
)
from okto_pulse.core.services.test_scenario_lifecycle import (
    GATED_STATUSES,
    StatusNotMutableError,
    VALID_SCENARIO_STATUSES,
    compute_test_scenario_semantic_sha256,
    evidence_invalidated_by_semantic_edit,
    reexecutable_evidence_reference,
    require_test_scenario_status_transition,
    require_test_scenario_status_mutable,
    resolve_scenario_types_for_whole_list_write,
    scenario_has_authenticated_required_evidence,
    scenario_has_required_evidence,
    validate_scenario_type,
    validate_scenario_types_for_write,
    validate_test_scenario_evidence,
)


async def _purge_quality_assessment_subject(
    db: Any,
    *,
    board_id: str,
    subject_type: str,
    subject_id: str,
) -> None:
    """Purge one SK-A relational slice inside the caller-owned transaction."""

    from okto_pulse.core.domain.quality_assessment import (
        AssessmentSubjectType,
    )
    from okto_pulse.core.ports.relational_application import (
        require_relational_application_adapter,
    )
    from okto_pulse.core.services.quality_assessment_lifecycle import (
        QualityAssessmentLifecycleService,
    )

    service = QualityAssessmentLifecycleService()
    plan = service.prepare_subject_purge(
        board_id=board_id,
        subject_type=AssessmentSubjectType(subject_type),
        subject_id=subject_id,
    )
    persistence = require_relational_application_adapter().quality_assessment_lifecycle(
        db
    )
    postcondition = await persistence.apply_purge_plan(plan)
    service.validate_purge_postcondition(
        plan=plan,
        postcondition=postcondition,
    )


async def _apply_quality_assessment_lifecycle_transition(
    db: Any,
    *,
    board_id: str,
    subject_type: str,
    subject_id: str,
    before_version: int,
    before_status: str,
    before_archived: bool,
    after_version: int,
    after_status: str,
    after_archived: bool,
    action: str,
    actor_id: str,
    before_edition: int | None = None,
    after_edition: int | None = None,
) -> None:
    """Reconcile assessment heads and audit one subject lifecycle change."""

    from okto_pulse.core.domain.quality_assessment import (
        AssessmentSubjectRef,
        AssessmentSubjectType,
    )
    from okto_pulse.core.domain.quality_assessment_lifecycle import (
        AssessmentLifecycleAction,
        AssessmentLifecycleCurrentInput,
        AssessmentLifecycleSubjectSnapshot,
        AssessmentLifecycleTransition,
    )
    from okto_pulse.core.domain.quality_canonicalization import (
        canonical_sha256,
    )
    from okto_pulse.core.ports.relational_application import (
        require_relational_application_adapter,
    )
    from okto_pulse.core.services.quality_assessment_lifecycle import (
        QualityAssessmentLifecycleService,
    )

    resolved_subject_type = AssessmentSubjectType(subject_type)
    persistence = require_relational_application_adapter().quality_assessment_lifecycle(
        db
    )
    heads, receipts = await persistence.load_lifecycle_state(
        board_id=board_id,
        subject_type=resolved_subject_type.value,
        subject_id=subject_id,
    )
    latest_by_kind = {}
    for receipt in receipts:
        current = latest_by_kind.get(receipt.assessment_kind)
        if current is None or (
            receipt.created_at,
            receipt.receipt_id,
        ) > (
            current.created_at,
            current.receipt_id,
        ):
            latest_by_kind[receipt.assessment_kind] = receipt
    current_inputs = tuple(
        AssessmentLifecycleCurrentInput(
            assessment_kind=kind,
            input_digest=receipt.input_digest,
        )
        for kind, receipt in sorted(
            latest_by_kind.items(),
            key=lambda item: item[0].value,
        )
    )
    before_subject = AssessmentSubjectRef(
        board_id=board_id,
        subject_type=resolved_subject_type,
        subject_id=subject_id,
        subject_version=before_version,
        subject_edition=before_edition,
    )
    after_subject = AssessmentSubjectRef(
        board_id=board_id,
        subject_type=resolved_subject_type,
        subject_id=subject_id,
        subject_version=after_version,
        subject_edition=after_edition,
    )
    occurred_at = datetime.now(timezone.utc)
    idempotency_digest = canonical_sha256(
        {
            "contract": "quality-assessment-lifecycle-hook/v1",
            "board_id": board_id,
            "subject_type": resolved_subject_type.value,
            "subject_id": subject_id,
            "action": action,
            "before": {
                "version": before_version,
                "edition": before_edition,
                "status": before_status,
                "archived": before_archived,
            },
            "after": {
                "version": after_version,
                "edition": after_edition,
                "status": after_status,
                "archived": after_archived,
            },
            "occurred_at": occurred_at.isoformat(),
        }
    )
    transition = AssessmentLifecycleTransition(
        action=AssessmentLifecycleAction(action),
        before=AssessmentLifecycleSubjectSnapshot(
            subject=before_subject,
            status=before_status,
            archived=before_archived,
            current_inputs=current_inputs,
        ),
        after=AssessmentLifecycleSubjectSnapshot(
            subject=after_subject,
            status=after_status,
            archived=after_archived,
            current_inputs=current_inputs,
        ),
        idempotency_key=f"quality-lifecycle:{idempotency_digest}",
        actor_id=actor_id,
        occurred_at=occurred_at,
    )
    plan = QualityAssessmentLifecycleService().prepare_transition(
        transition,
        heads=heads,
        receipts=receipts,
    )
    await persistence.apply_lifecycle_plan(plan)


async def evaluate_code_traceability_transition(
    db: Any,
    *,
    board: object | None,
    subject: object,
    subject_type: CodeTraceabilitySubjectType,
    from_status: str,
    to_status: str,
    enforce: bool = False,
) -> CodeTraceabilityGateEvaluation | None:
    """Evaluate one SDLC edge from Pulse relational attestations only.

    Community is solely the materializer; all coverage, freshness, overlap and
    waiver rules are evaluated here in Core.
    """

    phases = phases_for_transition(subject_type, from_status, to_status)
    if not phases:
        return None
    settings = resolve_code_traceability_settings(
        getattr(board, "settings", None) if board is not None else None
    )
    board_id = getattr(subject, "board_id", None)
    subject_id = getattr(subject, "id", None)
    version_field = (
        "policy_version"
        if subject_type is CodeTraceabilitySubjectType.CARD
        else "version"
    )
    subject_version = getattr(subject, version_field, None)
    if subject_version is None and subject_type is CodeTraceabilitySubjectType.CARD:
        subject_version = getattr(subject, "version", None)
    if (
        not isinstance(board_id, str)
        or not board_id
        or not isinstance(subject_id, str)
        or not subject_id
        or type(subject_version) is not int
        or subject_version < 1
    ):
        raise CodeInvestigationCurrentnessUnknown(
            details={
                "reason": "subject_version_unavailable",
                "subject_type": subject_type.value,
            }
        )
    try:
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        read_port = require_relational_application_adapter().code_traceability_read(db)
    except (AttributeError, RuntimeError) as exc:
        if settings.mode is CodeTraceabilityEnforcement.ADVISORY:
            return CodeTraceabilityGateEvaluation(
                mode=settings.mode,
                phases=phases,
                allowed=True,
                passed=False,
                blockers=(
                    CodeTraceabilityGateBlocker(
                        code="code_investigation_currentness_unknown",
                        message=(
                            "Structured Code Traceability projection is unavailable."
                        ),
                        blocking=False,
                        details={
                            "reason": ("code_traceability_read_adapter_unavailable")
                        },
                        remediation=(),
                    ),
                ),
                evidence_coverage=EvidenceDispositionCoverage(
                    total=0,
                    linked=0,
                    dispositioned=0,
                    pending_ids=(),
                ),
                target_coverage=TargetEntityCoverage(
                    total=0,
                    covered=0,
                    pending_entity_ids=(),
                ),
                receipt_currentness={},
                resolution_freshness={},
            )
        raise CodeInvestigationCurrentnessUnknown(
            details={"reason": "code_traceability_read_adapter_unavailable"}
        ) from exc
    query = CodeTraceabilityProjectionQuery(
        board_id=board_id,
        subject_type=subject_type,
        subject_id=subject_id,
        subject_version=subject_version,
        profile=CodeTraceabilityProjectionProfile.FULL,
        context_scope=CodeTraceabilityContextScope.GATE,
    )
    projection_service = CodeTraceabilityProjectionService()
    context = await projection_service.load_context(query, read_port=read_port)
    card_type = "normal"
    dependency_card_ids: tuple[str, ...] = ()
    blocking_card_ids: tuple[str, ...] = ()
    if subject_type is CodeTraceabilitySubjectType.CARD:
        raw_card_type = getattr(subject, "card_type", None)
        card_type = str(getattr(raw_card_type, "value", raw_card_type or "normal"))
        dependencies = await _application_list(
            db,
            "card_dependency",
            filters=(_apf("card_id", "eq", subject_id),),
        )
        dependency_card_ids = tuple(
            sorted(
                item.depends_on_id
                for item in dependencies
                if isinstance(getattr(item, "depends_on_id", None), str)
            )
        )
        external_card_ids = {
            item.card_id for item in context.targets if item.card_id != subject_id
        }
        blocking: list[str] = []
        for card_id in sorted(external_card_ids):
            external = await _application_get(db, "card", card_id)
            status = getattr(external, "status", None)
            if str(getattr(status, "value", status)).lower() in {
                "started",
                "in_progress",
                "validation",
                "rejected",
            }:
                blocking.append(card_id)
        blocking_card_ids = tuple(blocking)
    evaluation = projection_service.evaluate_transition_context(
        context,
        settings,
        from_status=from_status,
        to_status=to_status,
        card_type=card_type,
        dependency_card_ids=dependency_card_ids,
        blocking_card_ids=blocking_card_ids,
        referenced_evidence_ids=(
            extract_code_evidence_references(getattr(subject, "analysis", None))
            if subject_type is CodeTraceabilitySubjectType.REFINEMENT
            else ()
        ),
        skip_evidence_coverage=(
            subject_type is CodeTraceabilitySubjectType.SPEC
            and resolve_code_evidence_coverage_skip(
                board_settings=(
                    getattr(board, "settings", None) if board is not None else None
                ),
                spec=subject,
            )
        ),
    )
    if enforce:
        projection_service.validate_or_raise(evaluation)
    return evaluation


async def evaluate_code_evidence_coverage_gate(
    db: Any,
    *,
    board: object | None,
    spec: object,
    enforce: bool = False,
) -> CodeTraceabilityGateEvaluation:
    """Evaluate the deterministic Spec Code Evidence Matrix coverage gate.

    Unlike the board-wide Code Traceability posture, this is an ordinary Spec
    coverage gate: pending inherited Evidence blocks validation by default and
    the board-wide or per-Spec, human-authored skip can bypass that coverage
    obligation. Loading a complete server-owned projection remains mandatory
    even when the coverage obligation is skipped.
    """

    board_id = getattr(spec, "board_id", None)
    spec_id = getattr(spec, "id", None)
    spec_version = getattr(spec, "version", None)
    if (
        not isinstance(board_id, str)
        or not board_id
        or not isinstance(spec_id, str)
        or not spec_id
        or type(spec_version) is not int
        or spec_version < 1
    ):
        raise CodeInvestigationCurrentnessUnknown(
            details={
                "reason": "subject_version_unavailable",
                "subject_type": CodeTraceabilitySubjectType.SPEC.value,
            }
        )

    from okto_pulse.core.ports.relational_application import (
        RelationalApplicationAdapterMissing,
        require_relational_application_adapter,
    )

    try:
        read_port = require_relational_application_adapter().code_traceability_read(db)
    except RelationalApplicationAdapterMissing as exc:
        raise CodeInvestigationCurrentnessUnknown(
            details={"reason": "code_traceability_read_adapter_unavailable"}
        ) from exc
    projection_service = CodeTraceabilityProjectionService()
    context = await projection_service.load_context(
        CodeTraceabilityProjectionQuery(
            board_id=board_id,
            subject_type=CodeTraceabilitySubjectType.SPEC,
            subject_id=spec_id,
            subject_version=spec_version,
            profile=CodeTraceabilityProjectionProfile.FULL,
            context_scope=CodeTraceabilityContextScope.GATE,
        ),
        read_port=read_port,
    )
    policy = resolve_code_traceability_settings(
        getattr(board, "settings", None) if board is not None else None
    )
    deterministic_policy = policy.model_copy(
        update={"mode": CodeTraceabilityEnforcement.BLOCKING}
    )
    evaluation = projection_service.project_context(
        context,
        deterministic_policy,
        skip_evidence_coverage=resolve_code_evidence_coverage_skip(
            board_settings=(
                getattr(board, "settings", None) if board is not None else None
            ),
            spec=spec,
        ),
    ).gate_readiness
    if enforce:
        projection_service.validate_or_raise(evaluation)
    return evaluation


def _claims_test_evidence_v2(evidence: object) -> bool:
    return bool(
        isinstance(evidence, dict)
        and (
            evidence.get("manifest_ref") is not None
            or evidence.get("execution_attestation") is not None
            or evidence.get("execution_receipt") is not None
        )
    )


def _require_trusted_test_evidence_v2_write(
    *,
    board_id: str,
    spec_id: str,
    scenario_id: str,
    scenario_sha256: str,
    status: str,
    actor_id: str | None,
    evidence: object,
) -> None:
    """Authenticate an edition receipt before a scenario write can persist."""

    if not _claims_test_evidence_v2(evidence):
        return
    from okto_pulse.core.ports.test_evidence import (
        resolve_test_evidence_write_verifier,
    )

    verifier = resolve_test_evidence_write_verifier()
    if verifier is None:
        raise ValueError(
            "evidence_unverified: evidence_v2.concrete_verifier_not_configured"
        )
    verification = verifier.verify(
        board_id=board_id,
        spec_id=spec_id,
        scenario_id=scenario_id,
        scenario_sha256=scenario_sha256,
        status=status,
        actor_id=actor_id,
        evidence=evidence,
    )
    if not verification.verified:
        raise ValueError("evidence_unverified: " + ", ".join(verification.reason_codes))


# Preserve the service API's aggregate names without coupling annotations to
# Community persistence models.
AmendmentHotfixRevision = ApplicationRecord
Board = ApplicationRecord
BoardGuideline = ApplicationRecord
Card = ApplicationRecord
Guideline = ApplicationRecord
Ideation = ApplicationRecord
IdeationHistory = ApplicationRecord
IdeationQAItem = ApplicationRecord
Refinement = ApplicationRecord
RefinementHistory = ApplicationRecord
RefinementKnowledgeBase = ApplicationRecord
RefinementQAItem = ApplicationRecord
RefinementSnapshot = ApplicationRecord
Spec = ApplicationRecord
Sprint = ApplicationRecord
SprintHistory = ApplicationRecord
SprintQAItem = ApplicationRecord


def _apf(
    field: str,
    operator: ApplicationOperator,
    value: Any = None,
) -> ApplicationFilter:
    return ApplicationFilter(field, operator, value)


def _new_application_record(entity: str, **values: Any) -> ApplicationRecord:
    return ApplicationRecord(entity=entity, values=values)


def _new_knowledge_application_record(
    entity: str,
    *,
    parent_field: str,
    parent_id: str,
    parent_version: int | None,
    title: str,
    description: str | None,
    content: str,
    mime_type: str,
    governance_metadata: dict[str, Any] | None,
    created_by: str,
) -> ApplicationRecord:
    """Create a forward-stamped, self-rooted knowledge artifact."""

    knowledge_id = str(uuid.uuid4())
    values: dict[str, Any] = {
        "id": knowledge_id,
        parent_field: parent_id,
        "title": title,
        "description": description,
        "content": content,
        "mime_type": mime_type,
        "source_version": parent_version,
        "root_source_kb_id": knowledge_id,
        "governance_metadata": governance_metadata,
        "created_by": created_by,
    }
    values["content_hash"] = knowledge_content_sha256(values)
    return _new_application_record(entity, **values)


def _refresh_knowledge_content_hash(knowledge: ApplicationRecord) -> None:
    """Refresh the artifact fingerprint without changing its lineage revision."""

    knowledge.content_hash = knowledge_content_sha256(knowledge)


async def _application_list(
    context: Any,
    entity: str,
    *,
    filters: tuple[ApplicationFilter, ...] = (),
    any_filters: tuple[ApplicationFilter, ...] = (),
    any_groups: tuple[tuple[ApplicationFilter, ...], ...] = (),
    order_by: tuple[tuple[str, bool], ...] = (),
    offset: int = 0,
    limit: int | None = None,
    includes: tuple[str, ...] = (),
) -> list[ApplicationRecord]:
    rows = await get_application_persistence_port().list(
        context,
        ApplicationQuery(
            entity=entity,
            filters=filters,
            any_filters=any_filters,
            any_groups=any_groups,
            order_by=order_by,
            offset=offset,
            limit=limit,
            includes=includes,
        ),
    )
    return list(rows)


async def _application_count(
    context: Any,
    entity: str,
    *,
    filters: tuple[ApplicationFilter, ...] = (),
    any_filters: tuple[ApplicationFilter, ...] = (),
    any_groups: tuple[tuple[ApplicationFilter, ...], ...] = (),
) -> int:
    """Count rows matching the filters, ignoring any window (offset/limit).

    The paginated read path calls this twice — once for the filtered scope
    (``total_filtered``) and once for the base scope (``total_overall``) — so
    both totals are always server-computed, never inferred from ``len(items)``.
    """
    return await get_application_persistence_port().count(
        context,
        ApplicationQuery(
            entity=entity,
            filters=filters,
            any_filters=any_filters,
            any_groups=any_groups,
        ),
    )


async def list_entities_page(context: Any, request: PageRequest) -> PageResult:
    """Service-facing facade for the application-layer pagination executor.

    The implementation moved to
    ``okto_pulse.core.application.use_cases.entity_pagination`` (purity
    boundary); the import is deferred to call time because the use_cases
    package init imports application.errors, which imports this module.
    """
    from okto_pulse.core.application.use_cases.entity_pagination import (
        list_entities_page as _list_entities_page,
    )

    return await _list_entities_page(context, request)


async def _application_group_count(
    context: Any, request: GroupCountRequest
) -> tuple[Any, ...]:
    """Run a catalog-validated aggregate through the bound persistence port.

    The import is deferred for the same service/use-case cycle avoided by
    :func:`list_entities_page` above.
    """
    from okto_pulse.core.application.use_cases.entity_pagination import (
        group_count_entities,
    )

    return await group_count_entities(context, request)


async def _application_run(
    context: Any, query: ApplicationQuery
) -> list[ApplicationRecord]:
    return list(await get_application_persistence_port().list(context, query))


async def _application_get(
    context: Any,
    entity: str,
    record_id: str,
    *,
    includes: tuple[str, ...] = (),
) -> ApplicationRecord | None:
    return await get_application_persistence_port().get(
        context,
        entity=entity,
        record_id=record_id,
        includes=includes,
    )


async def _application_fence(
    context: Any,
    entity: str,
    record_id: str,
    *,
    expected_values: Mapping[str, object],
) -> bool:
    return await get_application_persistence_port().fence(
        context,
        entity=entity,
        record_id=record_id,
        expected_values=expected_values,
    )


async def _application_add(
    context: Any,
    record: ApplicationRecord,
    *,
    conflict_error: Exception | None = None,
) -> ApplicationRecord:
    return await get_application_persistence_port().add(
        context,
        record,
        conflict_error=conflict_error,
    )


async def _application_delete(context: Any, record: ApplicationRecord) -> None:
    await get_application_persistence_port().delete(context, record)


@dataclass(frozen=True, slots=True)
class GovernedArtifactDeletionReceipt:
    """Stable identities created before the SOT row is deleted.

    The receipt is intentionally transport-neutral.  Callers may expose it as
    additive metadata so an operator can follow the durable intent through the
    queue, delivery ledger and Global Discovery without guessing identifiers.
    """

    board_id: str
    artifact_type: str
    artifact_id: str
    delete_event_id: str
    generation: int
    reconcile_intent_id: str
    delivery_key: str
    attachment_deletions: tuple["AttachmentDeletionReceipt", ...] = ()
    descendant_deletions: tuple["GovernedArtifactDeletionReceipt", ...] = ()

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "board_id": self.board_id,
            "artifact_type": self.artifact_type,
            "artifact_id": self.artifact_id,
            "delete_event_id": self.delete_event_id,
            "generation": self.generation,
            "reconcile_intent_id": self.reconcile_intent_id,
            "delivery_key": self.delivery_key,
        }
        if self.descendant_deletions:
            payload["descendant_deletions"] = [
                receipt.to_dict() for receipt in self.descendant_deletions
            ]
        return payload


async def _prepare_governed_artifact_deletion(
    context: Any,
    *,
    board_id: str,
    artifact_type: str,
    artifact_id: str,
    occurred_at: datetime | None = None,
) -> GovernedArtifactDeletionReceipt:
    """Stage discard, permanent tombstone and reconcile intent in one UoW."""

    from okto_pulse.core.ports.consolidation import (
        get_consolidation_persistence_port,
    )
    from okto_pulse.core.ports.reconcile_intent import (
        ReconcileIntentCreate,
        get_reconcile_intent_port,
    )
    from okto_pulse.core.ports.delivery_ledger import build_delivery_key
    from okto_pulse.core.ports.tombstone import (
        DeletionTombstoneAdvance,
        get_tombstone_port,
    )

    persistence = get_consolidation_persistence_port()
    intent_occurred_at = occurred_at or datetime.now(timezone.utc)
    await persistence.discard_artifact_work(
        context,
        board_id=board_id,
        artifact_type=artifact_type,
        artifact_id=artifact_id,
    )
    delete_event_id = str(uuid.uuid4())
    tombstone = await get_tombstone_port().advance_deletion_tombstone(
        context,
        DeletionTombstoneAdvance(
            board_id=board_id,
            artifact_type=artifact_type,
            artifact_id=artifact_id,
            delete_event_id=delete_event_id,
        ),
    )
    if tombstone.generation < 1 or tombstone.delete_event_id != delete_event_id:
        raise RuntimeError("governed_delete_tombstone_receipt_mismatch")
    intent = await get_reconcile_intent_port().persist_reconcile_intent(
        context,
        ReconcileIntentCreate(
            board_id=board_id,
            artifact_type=artifact_type,
            artifact_id=artifact_id,
            generation=tombstone.generation,
            delete_event_id=delete_event_id,
            source_refs=(f"{artifact_type}:{artifact_id}",),
            occurred_at=intent_occurred_at,
        ),
    )
    if (
        intent.generation != tombstone.generation
        or intent.delete_event_id != delete_event_id
    ):
        raise RuntimeError("governed_delete_intent_receipt_mismatch")
    return GovernedArtifactDeletionReceipt(
        board_id=board_id,
        artifact_type=artifact_type,
        artifact_id=artifact_id,
        delete_event_id=delete_event_id,
        generation=tombstone.generation,
        reconcile_intent_id=intent.intent_id,
        delivery_key=build_delivery_key(
            board_id=board_id,
            artifact_type=artifact_type,
            artifact_id=artifact_id,
            generation=tombstone.generation,
        ),
    )


async def _application_flush(context: Any) -> None:
    await get_application_persistence_port().flush(context)


async def _application_refresh(
    context: Any, record: ApplicationRecord
) -> ApplicationRecord:
    return await get_application_persistence_port().refresh(context, record)


async def _application_commit(context: Any) -> None:
    await get_application_persistence_port().commit(context)


def _scope_actor_id(
    user_id: str | None, query_scope: QueryScope | None = None
) -> str | None:
    return query_scope.actor_id if query_scope is not None else user_id


def _scope_realm_id(
    realm_id: str | None = None,
    query_scope: QueryScope | None = None,
) -> str | None:
    if query_scope is not None and query_scope.realm_id is not None:
        return query_scope.realm_id
    return realm_id


def _board_owner_matches(
    board: ApplicationRecord | None,
    user_id: str | None,
    query_scope: QueryScope | None = None,
) -> bool:
    scoped_actor_id = _scope_actor_id(user_id, query_scope)
    return bool(board and scoped_actor_id and board.owner_id == scoped_actor_id)


def _board_scope_clauses(
    *,
    board_id: str | None = None,
    user_id: str | None = None,
    realm_id: str | None = None,
    query_scope: QueryScope | None = None,
    require_ownership: bool = True,
) -> list[ApplicationFilter] | None:
    if query_scope is not None:
        if (
            query_scope.target_board_id
            and board_id
            and query_scope.target_board_id != board_id
        ):
            return None
        if board_id is not None and (
            query_scope.allowed_board_ids is not None or query_scope.allow_all_boards
        ):
            if not query_scope.allows_board_id(board_id):
                return None
        elif not require_ownership and not query_scope.allow_all_boards:
            return None

    clauses: list[ApplicationFilter] = []
    if board_id:
        clauses.append(_apf("id", "eq", board_id))

    scoped_realm_id = _scope_realm_id(realm_id, query_scope)
    if scoped_realm_id:
        clauses.append(_apf("realm_id", "eq", scoped_realm_id))

    if query_scope is not None and query_scope.allowed_board_ids is not None:
        allowed_board_ids = tuple(query_scope.allowed_board_ids)
        if not allowed_board_ids:
            return None
        clauses.append(_apf("id", "in", allowed_board_ids))

    scoped_actor_id = _scope_actor_id(user_id, query_scope)
    if require_ownership and scoped_actor_id:
        clauses.append(_apf("owner_id", "eq", scoped_actor_id))

    return clauses


def _board_scope_select(
    *,
    board_id: str | None = None,
    user_id: str | None = None,
    realm_id: str | None = None,
    query_scope: QueryScope | None = None,
    require_ownership: bool = True,
):
    clauses = _board_scope_clauses(
        board_id=board_id,
        user_id=user_id,
        realm_id=realm_id,
        query_scope=query_scope,
        require_ownership=require_ownership,
    )
    if clauses is None:
        return None
    return ApplicationQuery(entity="board", filters=tuple(clauses), limit=1)


CARD_RESOURCE_READ_ONLY_MESSAGE = (
    "Card resources are read-only governed snapshots. Copy Knowledge Base, "
    "Mockup, and Architecture resources from the parent spec to refresh card "
    "context, and edit the source ideation, refinement, or spec resource instead."
)
CARD_RESOURCE_FIELDS = {"knowledge_bases", "screen_mockups"}


class CardResourceReadOnlyError(ValueError):
    """Raised when a caller tries to author governed resources directly on a card."""


def _ensure_card_resource_write_allowed(
    update_data: dict[str, Any],
    *,
    allow: bool,
) -> None:
    if allow:
        return
    attempted = sorted(CARD_RESOURCE_FIELDS.intersection(update_data))
    if attempted:
        raise CardResourceReadOnlyError(
            f"{CARD_RESOURCE_READ_ONLY_MESSAGE} Blocked fields: {', '.join(attempted)}."
        )


def _build_default_cognitive_closeout_gate() -> Any:
    """Build the shared cognitive closeout gate lazily.

    The lazy import keeps the service layer from importing KG storage at module
    import time while still letting tests inject a lightweight fake gate.
    """

    from okto_pulse.core.kg.cognitive_closeout_gate import (
        build_default_cognitive_closeout_gate,
    )

    return build_default_cognitive_closeout_gate()


def _board_skip_cognitive_consolidation(board: ApplicationRecord | None) -> bool:
    settings = (board.settings or {}) if board else {}
    return bool(settings.get("skip_cognitive_consolidation", False))


# S1.3 Cognitive Closure rollout — per-board policy + global feature flag.
COGNITIVE_READINESS_POLICY_ADVISORY = "advisory"
COGNITIVE_READINESS_POLICY_BLOCKING = "blocking"


def _board_cognitive_readiness_policy(board: ApplicationRecord | None) -> str:
    """Per-board cognitive readiness policy (fr_9d42c5e2). Default ``advisory``
    so existing boards never begin blocking on rollout."""
    settings = (board.settings or {}) if board else {}
    value = str(
        settings.get("cognitive_readiness_policy", COGNITIVE_READINESS_POLICY_ADVISORY)
    ).lower()
    if value not in (
        COGNITIVE_READINESS_POLICY_ADVISORY,
        COGNITIVE_READINESS_POLICY_BLOCKING,
    ):
        return COGNITIVE_READINESS_POLICY_ADVISORY
    return value


def _cognitive_readiness_blocking_active(board: ApplicationRecord | None) -> bool:
    """True only when BOTH the global feature flag is enabled AND the board
    policy is ``blocking`` — the two-key safe rollout (dec_41db6a36, formalised as
    the auditable RKG-06 policy decision dec_98c9a850: advisory default, blocking
    only on board ``cognitive_readiness_policy=blocking`` + global
    ``cognitive_readiness_blocking_enabled``). Default-off / fail-closed: any
    failure, unset or invalid value resolves to advisory (non-blocking) — a policy
    change never blocks silently and never hides a technical signal."""
    if _board_cognitive_readiness_policy(board) != COGNITIVE_READINESS_POLICY_BLOCKING:
        return False
    try:
        from okto_pulse.core.infra.config import get_settings

        return bool(get_settings().cognitive_readiness_blocking_enabled)
    except Exception:
        return False


async def cognitive_enforcement_active(db, board_id: str) -> bool:
    """Whether the board's done-gate is ACTUALLY enforcing cognitive readiness
    (two-key rollout). Transport-free reader extracted from ``mcp/server.py`` for
    spec R01A MCP-FU3 so the cognitive use cases can resolve enforcement without a
    relational session in their public surface. Never recomputed — delegates to
    :func:`_cognitive_readiness_blocking_active`."""
    board = await _application_get(db, "board", board_id)
    return _cognitive_readiness_blocking_active(board)


async def resolve_user_permissions(db, user_id: str, board_id: str):
    """Resolve a user's best-effort permission set (same model as
    ``/me/permissions``). Transport-free reader extracted from ``api/specs.py`` for
    spec R01A REST-FU3a so the permission guards no longer issue SQL in the HTTP
    adapter (Clean Core). ``board_id`` selects the per-board
    ``AgentBoard.permission_overrides`` layer (spec R01A REST-FU6-S2 rework — the
    legacy stories/specs adapters resolved the board overrides before
    check_permission; restoring it here keeps board-scoped grants/denies intact)."""
    persistence = get_application_persistence_port()
    compact_resolver = getattr(persistence, "resolve_user_permissions", None)
    if callable(compact_resolver):
        # Edition adapters may collapse agent + preset + board override into
        # one relational statement.  Besides avoiding redundant round-trips,
        # this keeps paginated REST authorization inside its <=6 SQL budget.
        return await compact_resolver(db, user_id=user_id, board_id=board_id)

    from okto_pulse.core.infra.permissions import (
        map_legacy_permissions,
        resolve_permissions,
    )

    agents = await _application_list(
        db,
        "agent",
        filters=(_apf("created_by", "eq", user_id),),
        limit=1,
    )
    agent = agents[0] if agents else None

    agent_flags: dict | None = None
    preset_flags: dict | None = None
    board_overrides: dict | None = None

    if agent is not None:
        if isinstance(agent.permission_flags, dict):
            agent_flags = agent.permission_flags
        elif isinstance(agent.permissions, list) and agent.permissions:
            agent_flags = map_legacy_permissions(agent.permissions)

        if agent.preset_id:
            preset = await _application_get(
                db,
                "permission_preset",
                agent.preset_id,
            )
            if preset and preset.flags is not None:
                preset_flags = preset.flags

        if board_id:
            agent_boards = await _application_list(
                db,
                "agent_board",
                filters=(
                    _apf("agent_id", "eq", agent.id),
                    _apf("board_id", "eq", board_id),
                ),
                limit=1,
            )
            agent_board = agent_boards[0] if agent_boards else None
            if agent_board and isinstance(agent_board.permission_overrides, dict):
                board_overrides = agent_board.permission_overrides

    return resolve_permissions(agent_flags, preset_flags, board_overrides)


def _board_qa_require_role_separation(board: ApplicationRecord | None) -> bool:
    """Return True if the board requires that Q&A answers come from a different
    principal than the one who asked the question (qa_require_role_separation)."""
    settings = (board.settings or {}) if board else {}
    return BoardGovernanceService.from_settings(settings).qa_require_role_separation


async def _attach_open_qa_counts(
    db: Any,
    rows: list[Any],
    qa_entity: str,
    fk_name: str,
) -> None:
    """Attach an ``open_qa_count`` attribute to each ORM row for summary projection.

    A Q&A item is OPEN (unanswered) when ``answered_at IS NULL`` — the only reliable
    predicate, because choice/multi_choice answers leave ``answer`` NULL and persist
    ``selected`` instead, yet every answer path sets ``answered_at`` once something is
    saved. The list queries don't eager-load qa_items, so a single grouped COUNT keyed
    by the foreign key avoids both N+1 and an async lazy-load during serialization.
    """
    if not rows:
        return
    ids = [r.id for r in rows]
    qa_rows = await _application_list(
        db,
        qa_entity,
        filters=(
            _apf(fk_name, "in", ids),
            _apf("answered_at", "is_none"),
        ),
    )
    counts: dict[str, int] = {}
    for qa in qa_rows:
        parent_id = getattr(qa, fk_name)
        counts[parent_id] = counts.get(parent_id, 0) + 1
    for r in rows:
        r.attach("open_qa_count", counts.get(r.id, 0))


async def _attach_active_refinement_counts(
    db: Any, rows: list[ApplicationRecord]
) -> None:
    """Attach active child-refinement counts for ideation summary projection."""
    if not rows:
        return
    ids = [r.id for r in rows]
    children = await _application_list(
        db,
        "refinement",
        filters=(
            _apf("ideation_id", "in", ids),
            _apf("archived", "is_false"),
            _apf("status", "ne", RefinementStatus.CANCELLED),
        ),
    )
    counts: dict[str, int] = {}
    for child in children:
        counts[child.ideation_id] = counts.get(child.ideation_id, 0) + 1
    for r in rows:
        r.attach("active_refinement_count", counts.get(r.id, 0))


async def _attach_active_spec_counts(db: Any, rows: list[ApplicationRecord]) -> None:
    """Attach active child-spec counts for refinement summary projection."""
    if not rows:
        return
    ids = [r.id for r in rows]
    children = await _application_list(
        db,
        "spec",
        filters=(
            _apf("refinement_id", "in", ids),
            _apf("archived", "is_false"),
            _apf("status", "ne", SpecStatus.CANCELLED),
        ),
    )
    counts: dict[str, int] = {}
    for child in children:
        counts[child.refinement_id] = counts.get(child.refinement_id, 0) + 1
    for r in rows:
        r.attach("active_spec_count", counts.get(r.id, 0))


async def _attach_active_direct_spec_counts(
    db: Any, rows: list[ApplicationRecord]
) -> None:
    """Attach active direct-spec counts for small ideation derivation badges."""
    if not rows:
        return
    ids = [r.id for r in rows]
    children = await _application_list(
        db,
        "spec",
        filters=(
            _apf("ideation_id", "in", ids),
            _apf("refinement_id", "is_none"),
            _apf("archived", "is_false"),
            _apf("status", "ne", SpecStatus.CANCELLED),
        ),
    )
    counts: dict[str, int] = {}
    for child in children:
        counts[child.ideation_id] = counts.get(child.ideation_id, 0) + 1
    for r in rows:
        r.attach("active_spec_count", counts.get(r.id, 0))


async def backfill_qa_answered_at(db: Any) -> dict[str, int]:
    """One-shot self-heal: carimba ``answered_at`` em Q&A respondidas órfãs.

    A herança de Q&A (``propagate_artifacts``) copiava resposta/seleção sem
    ``answered_at`` — e o badge ``open_qa_count`` define "aberta" como
    ``answered_at IS NULL``, então toda Q&A respondida herdada virava
    falso-aberta em refinements/specs derivados (em campo: 100% dos badges
    do board 0.2.3 eram falsos). Idempotente: só toca linhas com resposta
    (``answer`` ou ``selected`` preenchidos) e timestamp ausente, usando o
    ``created_at`` da própria linha como melhor aproximação histórica.
    Retorna {tabela: linhas_corrigidas} para o log estruturado do boot.
    """
    return await get_application_persistence_port().backfill_qa_answered_at(db)


async def _authorize_qa_answer_or_raise(
    db: Any,
    *,
    board: ApplicationRecord | None,
    qa: Any,
    user_id: str,
    entity_type: str,
    question_id: str,
    card_id: str | None = None,
    actor_type: str = "user",
    surface: str = "service",
) -> None:
    """Authorize a Q&A answer and emit a safe denial event before failing closed."""
    try:
        BoardGovernanceService.authorize_qa_answer(
            (board.settings if board else None),
            asked_by=getattr(qa, "asked_by", None),
            answered_by=user_id,
        )
    except QASelfAnsweringNotAllowedError:
        if board is not None:
            actor_name = await resolve_actor_name(db, user_id, board.id)
            details = build_qa_self_answer_denied_details(
                board_id=board.id,
                actor_id=user_id,
                entity_type=entity_type,
                question_id=question_id,
                surface=surface,
            )
            await _application_add(
                db,
                _new_application_record(
                    "activity_log",
                    board_id=board.id,
                    card_id=card_id,
                    action=QA_SELF_ANSWER_DENIED_ACTION,
                    actor_type=actor_type,
                    actor_id=user_id,
                    actor_name=actor_name,
                    details=details,
                ),
            )
            emit_governance_metric(details, raise_on_violation=False)
            await _application_flush(db)
        raise


async def _publish_quality_clarification_changed(
    db: Any,
    *,
    subject: ApplicationRecord,
    subject_type: str,
    qa_id: object,
    operation: str,
    actor_id: str | None,
    actor_type: str = "user",
) -> None:
    """Stage the Q&A invalidation signal in the caller-owned transaction."""

    from okto_pulse.core.events import publish as event_publish
    from okto_pulse.core.events.types import QualityClarificationChanged

    subject_id = str(getattr(subject, "id", "") or "").strip()
    board_id = str(getattr(subject, "board_id", "") or "").strip()
    subject_version = getattr(subject, "version", None)
    if (
        not subject_id
        or not board_id
        or not isinstance(subject_version, int)
        or isinstance(subject_version, bool)
        or subject_version < 1
    ):
        raise RuntimeError("quality_clarification_subject_invalid")
    normalized_qa_id = str(qa_id or "").strip() or None
    await event_publish(
        QualityClarificationChanged(
            board_id=board_id,
            actor_id=actor_id,
            actor_type=actor_type,
            subject_type=subject_type,
            subject_id=subject_id,
            subject_version=subject_version,
            qa_id=normalized_qa_id,
            operation=operation,
        ),
        session=db,
    )


async def _record_critical_context_decision(
    db: Any,
    *,
    decision: CriticalContextDecision,
    actor_name: str | None = None,
    actor_type: str = "user",
    card_id: str | None = None,
) -> None:
    resolved_name = actor_name or await resolve_actor_name(
        db, decision.actor_id, decision.board_id
    )
    await _application_add(
        db,
        _new_application_record(
            "activity_log",
            board_id=decision.board_id,
            card_id=card_id if decision.entity_type != "card" else decision.entity_id,
            action=CRITICAL_CONTEXT_DECISION_ACTION,
            actor_type=actor_type,
            actor_id=decision.actor_id,
            actor_name=resolved_name,
            details=decision.audit_details(),
        ),
    )
    emit_governance_metric(decision.metric_labels(), raise_on_violation=False)
    emit_governance_metric(
        decision.latency_metric_labels(),
        value=round(float(decision.latency_ms), 3),
        raise_on_violation=False,
    )
    if decision.outcome == "deny" and decision.reason in {
        "full_context_required",
        "full_context_unavailable",
    }:
        emit_governance_metric(
            decision.resolution_failure_metric_labels(),
            raise_on_violation=False,
        )
    await _application_flush(db)


async def _authorize_critical_context_or_raise(
    db: Any,
    *,
    board_id: str,
    actor_id: str,
    entity_type: str,
    entity_id: str,
    critical_action: CriticalAction,
    surface: str = "service",
    actor_type: str = "user",
    actor_name: str | None = None,
    card_id: str | None = None,
    defer_success_audit: bool = False,
) -> CriticalContextDecision:
    """Resolve full context for a critical action and persist a safe audit event.

    Denials are always recorded immediately.  Callers that run potentially
    expensive read-only gates may defer the successful audit until immediately
    before their mutation fence, keeping SQLite's database-wide writer lock out
    of the read phase.
    """

    guard = FullContextCriticalActionGuard(
        db,
        resolvers=build_default_full_context_resolvers(db),
    )
    try:
        decision = await guard.authorize_and_resolve(
            board_id=board_id,
            actor_id=actor_id,
            entity_type=entity_type,
            entity_id=entity_id,
            critical_action=critical_action,
            surface=surface,
        )
    except FullContextGuardError as exc:
        await _record_critical_context_decision(
            db,
            decision=exc.decision,
            actor_name=actor_name,
            actor_type=actor_type,
            card_id=card_id,
        )
        raise

    if not defer_success_audit:
        await _record_critical_context_decision(
            db,
            decision=decision,
            actor_name=actor_name,
            actor_type=actor_type,
            card_id=card_id,
        )
    return decision


def _critical_card_move_action(target_status: CardStatus) -> CriticalAction:
    if target_status == CardStatus.IN_PROGRESS:
        return CriticalAction.CARD_START_IMPLEMENTATION
    if target_status == CardStatus.DONE:
        return CriticalAction.CARD_CLOSEOUT
    if target_status == CardStatus.CANCELLED:
        return CriticalAction.CARD_CANCEL
    return CriticalAction.CARD_MOVE_STATUS


def _critical_spec_move_action(target_status: SpecStatus) -> CriticalAction:
    if target_status == SpecStatus.APPROVED:
        return CriticalAction.SPEC_APPROVE
    if target_status == SpecStatus.DONE:
        return CriticalAction.SPEC_CLOSEOUT
    if target_status == SpecStatus.CANCELLED:
        return CriticalAction.SPEC_CANCEL
    return CriticalAction.SPEC_MOVE_STATUS


def _critical_sprint_move_action(target_status: SprintStatus) -> CriticalAction:
    if target_status == SprintStatus.CLOSED:
        return CriticalAction.SPRINT_CLOSEOUT
    if target_status == SprintStatus.CANCELLED:
        return CriticalAction.SPRINT_CANCEL
    return CriticalAction.SPRINT_MOVE_STATUS


def _critical_ideation_move_action(target_status: IdeationStatus) -> CriticalAction:
    if target_status == IdeationStatus.DONE:
        return CriticalAction.IDEATION_CLOSEOUT
    if target_status == IdeationStatus.CANCELLED:
        return CriticalAction.IDEATION_CANCEL
    return CriticalAction.IDEATION_MOVE_STATUS


def _critical_refinement_move_action(target_status: RefinementStatus) -> CriticalAction:
    if target_status == RefinementStatus.DONE:
        return CriticalAction.REFINEMENT_CLOSEOUT
    if target_status == RefinementStatus.CANCELLED:
        return CriticalAction.REFINEMENT_CANCEL
    return CriticalAction.REFINEMENT_MOVE_STATUS


def _card_cognitive_entity_type(card: Card) -> str:
    card_type = getattr(card, "card_type", CardType.NORMAL)
    card_type_value = getattr(card_type, "value", str(card_type)).lower()
    if card_type_value == CardType.TEST.value:
        return "test"
    if card_type_value == CardType.BUG.value:
        return "bug"
    return "task"


def _cognitive_blocking_count(result: Any) -> int:
    count = getattr(result, "blocking_count", None)
    if count is not None:
        return int(count)
    blocking_items = getattr(result, "blocking_items", ()) or ()
    return len(blocking_items)


class GovernedCompletionBlocked(ValueError):
    """A known, human-actionable gate blocker that may cause Rejected."""

    def __init__(self, code: str, summary: str, *, reason_codes: tuple[str, ...] = ()):
        self.code = code
        self.summary = summary
        self.reason_codes = reason_codes or (code,)
        super().__init__(f"{code}: {summary}")


class CompletionInfrastructureUnavailable(ValueError):
    """Technical gate failure; it must never be persisted as a rejection."""


def _evaluate_cognitive_closeout_or_raise(
    *,
    gate_factory: Callable[[], Any],
    board: Board | None,
    board_id: str,
    entity_type: str,
    entity_id: str,
    entity: Any,
    target_label: str,
    graph_state: str | None = None,
) -> None:
    """Evaluate the shared closeout gate and raise a stable service error.

    This helper is intentionally side-effect free. Callers must invoke it before
    any status assignment, resource-gate side effect, conclusion append, or
    lifecycle activity write for a ``done`` transition.
    """

    skip_enabled = _board_skip_cognitive_consolidation(board)
    gate = gate_factory()
    try:
        result = gate.evaluate(
            board_id=board_id,
            entity_type=entity_type,
            entity_id=entity_id,
            entity=entity,
            target_status="done",
            board_skip_enabled=skip_enabled,
            graph_state=graph_state,
        )
    except Exception as exc:
        raise CompletionInfrastructureUnavailable(
            f"cognitive_status_unavailable: {target_label} done transition "
            f"blocked ({type(exc).__name__})"
        ) from exc

    if getattr(result, "allowed", False):
        return

    reason = str(getattr(result, "reason", "cognitive_consolidation_pending"))
    blocking_count = _cognitive_blocking_count(result)
    if reason == "cognitive_status_unavailable":
        detail = (
            "because cognitive status could not be read. "
            "The KG graph may be in a degraded state (recovery_needed / quarantined). "
            "Per the Degraded-KG Fallback Rule, if the board is confirmed degraded "
            "you may enable the board setting `skip_cognitive_consolidation` to allow "
            "done transitions while the graph is unavailable. "
            "To restore full cognitive closeout, follow the KG Health recovery flow: "
            "call `okto_pulse_kg_health` to confirm the graph_state, then consult "
            "the resource `okto-pulse://reference/kg-health` for the operator-driven "
            "recovery steps."
        )
    else:
        detail = "by active cognitive consolidation items"
    summary = f"{target_label} done transition blocked {detail} ({blocking_count})"
    if reason == "cognitive_status_unavailable":
        raise CompletionInfrastructureUnavailable(f"{reason}: {summary}")
    raise GovernedCompletionBlocked(reason, summary)


def _build_default_cognitive_readiness_service() -> Any:
    """Default ``CognitiveReadinessService`` over the shared cognitive item
    store (S1.2/S1.3). Injectable factory mirrors
    ``_build_default_cognitive_closeout_gate`` so tests can swap a fake."""

    from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessService
    from okto_pulse.core.kg.rebuild_audit import (
        CognitiveConsolidationItemStore,
        require_rebuild_audit_artifact_store,
    )

    return CognitiveReadinessService(
        CognitiveConsolidationItemStore(
            artifact_store=require_rebuild_audit_artifact_store()
        )
    )


async def _evaluate_cognitive_readiness_or_raise(
    *,
    service_factory: Callable[[], Any],
    db: Any,
    board_id: str,
    entity_type: str,
    entity_id: str,
    entity: Any,
    target_label: str,
    policy_blocking: bool,
) -> None:
    """S1.3 production wiring: consult the single ``CognitiveReadinessService``
    on a ``done`` transition and block on the readiness tiers the legacy gate
    does NOT cover — technical DLQ, canonical_debt OPEN, and a lapsed
    revisit-required skip — BEFORE any status / conclusion / snapshot / activity
    mutation. The legacy ``CognitiveCloseoutGate`` still governs active cognitive
    items.

    Rollout safety (fr_9d42c5e2 / dec_41db6a36): when ``policy_blocking`` is
    False (the default for existing boards, or the global flag off) this is a
    NO-OP — readiness stays advisory. Carve-out: a task/test (no reusable
    cognition) never blocks on the cognitive/advisory tiers, but the technical
    no-mask tiers (DLQ / open canonical_debt) still block when policy is active.

    Failure semantics: while ``policy_blocking`` is False this is a NO-OP. Once
    blocking is ACTIVE, a resolution/evaluation failure is fail-CLOSED with a
    visible ``cognitive_readiness_unavailable`` error BEFORE any mutation — a
    silent skip would make the enforcement point an appearance of control
    (validator carry-forward).
    """

    if not policy_blocking:
        return

    from okto_pulse.core.kg.cognitive_closeout_gate import (
        CognitiveCloseoutGateError,
        resolve_cognitive_source_refs,
    )
    from okto_pulse.core.kg.cognitive_readiness import GATE_BLOCKING_TIERS

    def _unavailable(reason: str) -> CompletionInfrastructureUnavailable:
        return CompletionInfrastructureUnavailable(
            f"cognitive_readiness_unavailable: {target_label} done transition "
            f"blocked — {reason} (blocking policy active)"
        )

    try:
        refs = resolve_cognitive_source_refs(
            entity_type=entity_type,
            entity=entity,
            entity_id=entity_id,
        ).source_refs
    except CognitiveCloseoutGateError as exc:
        # An entity type that is genuinely NOT eligible for cognitive closeout is
        # a safe no-op; any other gate error on a covered type is fail-closed.
        if getattr(exc, "code", "") == "unsupported_entity_type":
            return
        raise _unavailable(
            "source resolution failed "
            f"({getattr(exc, 'code', None) or type(exc).__name__})"
        ) from exc
    except Exception as exc:
        raise _unavailable(f"source resolution failed ({type(exc).__name__})") from exc
    if not refs:
        return

    # Carve-out: the entity's own ref is refs[0] (``<normalized_type>:<id>``).
    # task/test carry no reusable cognition → advisory for cognitive tiers (the
    # technical DLQ/debt no-mask tiers still apply via compose_readiness).
    primary_type = refs[0].split(":", 1)[0]
    has_reusable_cognition = primary_type not in ("task", "test")

    try:
        service = service_factory()
    except Exception as exc:
        raise _unavailable(
            f"readiness service unavailable ({type(exc).__name__})"
        ) from exc
    blocking_tiers = GATE_BLOCKING_TIERS
    for ref in refs:
        try:
            verdict = await service.evaluate_artifact(
                db,
                board_id=board_id,
                source_ref=ref,
                has_reusable_cognition=has_reusable_cognition,
            )
        except Exception as exc:
            raise _unavailable(
                f"readiness evaluation failed for {ref} ({type(exc).__name__})"
            ) from exc
        if verdict.blocking and verdict.tier in blocking_tiers:
            raise GovernedCompletionBlocked(
                str(verdict.tier),
                f"{target_label} done transition blocked by cognitive readiness "
                f"({verdict.readiness_signal or verdict.reason_code or verdict.tier})",
            )


async def _resolve_closeout_graph_state(board_id: str, db: Any) -> str | None:
    """Resolve the board's current ``graph_state`` for the cognitive closeout
    gate (F16). Runs in the ASYNC caller where an ``Any`` is in scope
    and threads the result into the SYNC ``gate.evaluate(...)`` so the gate stays
    pure (no I/O).

    Fail-safe (FR6): on ANY failure (e.g. ``BoardNotFoundError``) or a missing
    ``graph_state`` key, return ``None`` — so the gate's ``resolved_generation``-
    is-None liveness check still governs and a degraded signal is never swallowed
    into ALLOWED. Reuses ``get_kg_health`` as-is (no new health-composition logic).
    """
    try:
        from okto_pulse.core.services.kg_health_service import get_kg_health

        health = await get_kg_health(board_id, db)
        state = health.get("graph_state")
    except Exception:
        return None
    return str(state) if state is not None else None


async def _evaluate_entity_cognitive_done_or_raise(
    *,
    db: Any,
    gate_factory: Callable[[], Any],
    readiness_service_factory: Callable[[], Any],
    board: ApplicationRecord | None,
    board_id: str,
    entity_type: str,
    entity_id: str,
    entity: Any,
    target_label: str,
    resolve_graph_state: bool = True,
) -> None:
    """Run the canonical, read-only cognitive gates for a done transition.

    Lifecycle mutation services and the allowed-transition preview both call
    this helper so the preview cannot advertise a transition that the actual
    mutation would reject. The helper performs reads only and must run before
    snapshots, status changes, histories, activities, or outbox writes.
    """

    graph_state = (
        await _resolve_closeout_graph_state(board_id, db)
        if resolve_graph_state
        else None
    )
    _evaluate_cognitive_closeout_or_raise(
        gate_factory=gate_factory,
        board=board,
        board_id=board_id,
        entity_type=entity_type,
        entity_id=entity_id,
        entity=entity,
        target_label=target_label,
        graph_state=graph_state,
    )
    await _evaluate_cognitive_readiness_or_raise(
        service_factory=readiness_service_factory,
        db=db,
        board_id=board_id,
        entity_type=entity_type,
        entity_id=entity_id,
        entity=entity,
        target_label=target_label,
        policy_blocking=_cognitive_readiness_blocking_active(board),
    )


# ---------------------------------------------------------------------------
# Spec Validation Gate — exception and lock helper
# ---------------------------------------------------------------------------


class SpecLockedError(Exception):
    """Raised when a content-edit operation is attempted on a locked spec.

    A spec is locked when its current_validation_id points to a validation
    record with outcome='success'. To edit, the spec must enter ``draft``,
    which starts a new lifecycle edition, atomically clears
    ``current_validation_id`` and preserves validation history. Same-edition
    lifecycle moves, including a move back to ``approved``, preserve Current.
    For an eligible existing scenario, leave spec content unchanged for Path A regression evidence;
    use amendment lineage when expected behavior changed.
    """

    def __init__(
        self,
        spec_id: str,
        current_validation_id: str | None = None,
        message: str | None = None,
    ):
        self.spec_id = spec_id
        self.current_validation_id = current_validation_id
        self.message = message or (
            "Spec is locked because validation passed. "
            "Move the spec to draft to open a new edition "
            "(Current validation will be cleared; history is preserved)."
        )
        super().__init__(self.message)


def spec_is_content_locked(spec: "Spec | None") -> bool:
    """True iff ``spec`` is under the Spec Validation Gate content lock.

    The lock holds when ``current_validation_id`` points to a validation record
    with ``outcome='success'`` in the spec's validations history — independent of
    the spec's nominal status (a ``validated`` spec moved to ``in_progress`` for
    execution stays locked).

    SINGLE source of truth for "is this spec content-locked", reused by Path B
    amendment eligibility (spec 62cf2d36) so the content-lock gate and Path B can
    never contradict: a content-locked ``in_progress`` spec — the exact one that
    cannot be edited directly — is precisely the one Path B must accept.
    """
    if spec is None:
        return False
    current_id = getattr(spec, "current_validation_id", None)
    if not current_id:
        return False
    validations = getattr(spec, "validations", None) or []
    current = next((v for v in validations if v.get("id") == current_id), None)
    return bool(
        current
        and current.get("outcome") == "success"
        and is_current_edition(current.get("edition"), getattr(spec, "edition", None))
    )


async def _require_spec_unlocked(db: Any, spec_id: str) -> None:
    """Raise SpecLockedError if spec has an active passed validation.

    Called at the top of every content-edit method on SpecService to enforce
    the Spec Validation Gate content lock. Skips silently when spec doesn't
    exist (caller handles that) or when no validation is active.
    """
    spec = await _application_get(db, "spec", spec_id)
    if spec_is_content_locked(spec):
        raise SpecLockedError(
            spec_id=spec_id,
            current_validation_id=getattr(spec, "current_validation_id", None),
        )


def _amendment_regression_test_task_ids(amendment_rows: list) -> list[str]:
    """Regression test task ids contributed by ELIGIBLE Path B amendments.

    Spec 62cf2d36 (fr_646e69d2): an AmendmentHotfixRevision formally linked to a
    bug is an ADDITIVE source of regression test tasks for the bug gate — but only
    when its ``(status, lineage_state)`` eligibility verdict is NOT blocked
    (lineage complete + a non-draft status). The deep coverage/lineage decision
    still runs fail-closed in ``BugRegressionGateValidator`` downstream, so this
    never disables ``require_test_task_for_bug`` nor relaxes validator-only
    coverage — a blocked/draft amendment contributes nothing. Order-preserving and
    de-duplicated.
    """
    seen: set[str] = set()
    out: list[str] = []
    for row in amendment_rows:
        verdict = evaluate_amendment_eligibility(
            getattr(row, "status", None), getattr(row, "lineage_state", None)
        )
        if getattr(verdict, "blocked", True):
            continue
        for tid in getattr(row, "regression_test_task_ids", None) or []:
            if tid not in seen:
                seen.add(tid)
                out.append(str(tid))
    return out


# ---------------------------------------------------------------------------
# Artifact propagation utility
# ---------------------------------------------------------------------------


def _legacy_filter_mockups(
    mockups: list[dict] | None,
    mockup_ids: list[str] | None,
) -> list[dict]:
    """Filter and copy mockups, adding origin_id for traceability."""
    if not mockups:
        return []
    source = (
        mockups
        if mockup_ids is None
        else [m for m in mockups if m.get("id") in mockup_ids]
    )
    copied = []
    for m in source:
        new_m = dict(m)
        new_m["origin_id"] = (
            m.get("origin_id") or m.get("source_mockup_id") or m.get("id")
        )
        new_m["source_mockup_id"] = m.get("id")
        origin_token = f"{m.get('id')}{id(new_m)}"
        new_m["id"] = f"sm_{hashlib.md5(origin_token.encode()).hexdigest()[:8]}"
        copied.append(new_m)
    return copied


def _compile_qa_context(qa_items: list) -> str | None:
    """Compile answered Q&A items into a context section."""

    def _selected_labels(qa) -> list[str]:
        selected = getattr(qa, "selected", None)
        choices = getattr(qa, "choices", None)
        if isinstance(qa, dict):
            selected = qa.get("selected")
            choices = qa.get("choices")
        selected_ids = [str(item) for item in (selected or [])]
        labels_by_id = {
            str(choice.get("id")): str(choice.get("label"))
            for choice in (choices or [])
            if isinstance(choice, dict) and choice.get("id") is not None
        }
        return [labels_by_id.get(item, item) for item in selected_ids]

    def _answer_text(qa) -> str | None:
        answer = getattr(qa, "answer", None) or (
            qa.get("answer") if isinstance(qa, dict) else None
        )
        if answer:
            return str(answer)
        labels = _selected_labels(qa)
        if labels:
            return ", ".join(labels)
        return None

    answered = [qa for qa in (qa_items or []) if _answer_text(qa)]
    if not answered:
        return None
    lines = []
    for qa in answered:
        q = getattr(qa, "question", None)
        if isinstance(qa, dict):
            q = q or qa.get("question", "")
        q = q or ""
        a = _answer_text(qa) or ""
        lines.append(f"**Q:** {q}\n**A:** {a}")
    return "## Q&A Decisions\n" + "\n\n".join(lines)


_PROPAGATED_KB_PREFIX = "[propagated from parent]"


def _legacy_propagated_kb_description(description: str | None) -> str:
    """R6-IMP1 (FR1/AC1) — apply the propagation marker AT MOST ONCE.

    In a multi-hop chain (ideation -> refinement -> spec -> card) the source KB
    already carries the prefix from the previous hop, because every hop copies the
    parent's (already-prefixed) description through this same path. Prepending
    again would stack ``[propagated from parent] [propagated from parent] ...``.
    Idempotent: if the stripped description already starts with the marker, return
    it unchanged; otherwise prepend once. Origin metadata (source_*/source_kb_id)
    is untouched — only the human-readable marker is normalized."""
    body = (description or "").strip()
    if body.startswith(_PROPAGATED_KB_PREFIX):
        return body
    return f"{_PROPAGATED_KB_PREFIX} {body}".strip()


async def _legacy_propagate_artifacts(
    db: Any,
    source_mockups: list[dict] | None,
    source_qa_items: list | None,
    source_knowledge_bases: list | None,
    target_entity: Any,
    target_kb_entity: str | None,
    user_id: str,
    mockup_ids: list[str] | None = None,
    kb_ids: list[str] | None = None,
    source_type: str | None = None,
    source_id: str | None = None,
    source_title: str | None = None,
    source_version: int | None = None,
) -> None:
    """Propagate mockups, KBs and Q&A from a parent entity to a target entity.

    - Mockups: copied as JSON with origin_id. Default=all, filter by mockup_ids.
    - KBs: copied as new DB rows with source metadata when the target model supports it.
    - Q&A: compiled into context (appended, not replaced).
    - Existing artifacts on target are preserved (additive, not replacement).
    """
    # Propagate mockups
    copied_mockups = _legacy_filter_mockups(source_mockups, mockup_ids)
    if copied_mockups:
        existing = list(target_entity.screen_mockups or [])
        new_set = existing + copied_mockups
        # MockupDesignSystemGate (spec 3a006f65 / card 0192f58d): a propagated/copied
        # mockup is a NEW entry on the target board — gate it (delta vs the existing set)
        # BEFORE assigning so a non-compliant mockup can't be laundered onto a blocking
        # board via propagation. Covers create_refinement propagation + copy_mockups_to_card.
        from okto_pulse.core.services.design_system import gate_entity_screen_mockups

        target_entity.screen_mockups = existing  # keep baseline for the gate's delta
        await gate_entity_screen_mockups(
            db,
            target_entity,
            new_set,
            entity_type=getattr(
                target_entity,
                "entity",
                type(target_entity).__name__.lower(),
            ),
        )
        target_entity.screen_mockups = new_set

    # Propagate knowledge bases (DB rows) — accepts ORM objects or dicts
    if target_kb_entity and source_knowledge_bases:
        kbs = (
            source_knowledge_bases
            if kb_ids is None
            else [
                kb
                for kb in source_knowledge_bases
                if (kb.get("id") if isinstance(kb, dict) else getattr(kb, "id", None))
                in kb_ids
            ]
        )
        target_id_field = {
            "spec_knowledge_base": "spec_id",
            "refinement_knowledge_base": "refinement_id",
            "ideation_knowledge_base": "ideation_id",
        }.get(target_kb_entity)
        if target_id_field:
            for kb in kbs:
                _get = (
                    (lambda k: kb.get(k))
                    if isinstance(kb, dict)
                    else (lambda k: getattr(kb, k, None))
                )
                target_kb_id = str(uuid.uuid4())
                kb_payload = {
                    "id": target_kb_id,
                    target_id_field: target_entity.id,
                    "title": _get("title"),
                    # R6-IMP1: idempotent prefix — never stack across multi-hop chains.
                    "description": _legacy_propagated_kb_description(
                        _get("description")
                    ),
                    "content": _get("content"),
                    "mime_type": _get("mime_type") or "text/markdown",
                    "created_by": user_id,
                }
                # R6-IMP4: multi-hop KB lineage. The immediate parent is the KB
                # being copied; the root is the parent's OWN root when it already
                # has one (so a 3rd hop keeps the canonical origin), else the
                # parent itself. source_kb_id stays == immediate parent (back-compat).
                parent_kb_id = _get("id")
                parent_root = _get("root_source_kb_id")
                source_values = {
                    "source_type": source_type,
                    "source_id": source_id,
                    "source_title": source_title,
                    "source_version": source_version,
                    "source_kb_id": parent_kb_id,
                    "immediate_parent_kb_id": parent_kb_id,
                    "root_source_kb_id": parent_root or parent_kb_id,
                }
                for attr, value in source_values.items():
                    if value is not None:
                        kb_payload[attr] = value
                kb_payload["content_hash"] = knowledge_content_sha256(kb_payload)
                await _application_add(
                    db,
                    _new_application_record(
                        target_kb_entity,
                        **kb_payload,
                    ),
                )
            await _application_flush(db)

    # Propagate Q&A items as proper QA rows on the target entity
    if source_qa_items:
        # Determine target QA entity based on the target aggregate.
        target_qa_entity = None
        target_fk_field = None
        if target_entity.entity == "spec":
            target_qa_entity = "spec_qa_item"
            target_fk_field = "spec_id"
        elif target_entity.entity == "refinement":
            target_qa_entity = "refinement_qa_item"
            target_fk_field = "refinement_id"

        if target_qa_entity and target_fk_field:
            for qa in source_qa_items:
                _get = (
                    (lambda k: qa.get(k))
                    if isinstance(qa, dict)
                    else (lambda k: getattr(qa, k, None))
                )
                # Only copy ANSWERED Q&A items. Choice questions (choice/
                # single_choice/multi_choice) store the answer in `selected`
                # and leave `answer` as None — the original `if not answer`
                # silently dropped every choice-type response, so derived
                # entities lost the decisions made on the parent. Treat the
                # item as answered when EITHER `answer` OR `selected` is set.
                answer = _get("answer")
                selected = _get("selected")
                has_selection = bool(selected) and len(selected) > 0
                if not answer and not has_selection:
                    continue
                qa_payload: dict[str, Any] = {
                    target_fk_field: target_entity.id,
                    "question": _get("question") or "",
                    "question_type": _get("question_type") or "text",
                    "choices": _get("choices"),
                    "allow_free_text": _get("allow_free_text") or False,
                    "answer": answer,
                    "selected": selected,
                    "asked_by": _get("asked_by") or user_id,
                    "answered_by": _get("answered_by"),
                    # `answered_at` DEVE acompanhar a resposta copiada: o badge
                    # open_qa_count usa `answered_at IS NULL` como definição de
                    # "aberta" (choice answers deixam `answer` NULL), então uma
                    # herança sem o timestamp marcava TODA Q&A respondida
                    # herdada como falso-aberta no refinement/spec derivado.
                    # Fallback para created_at/now cobre pais antigos que já
                    # perderam o timestamp em heranças anteriores ao fix —
                    # este branch só roda para itens RESPONDIDOS.
                    "answered_at": (
                        _get("answered_at")
                        or _get("created_at")
                        or datetime.now(timezone.utc)
                    ),
                }
                # Preserva a data original da pergunta quando disponível
                # (ordenacão/histórico); ausente, o default do modelo cobre.
                if _get("created_at") is not None:
                    qa_payload["created_at"] = _get("created_at")
                await _application_add(
                    db,
                    _new_application_record(target_qa_entity, **qa_payload),
                )
            await _application_flush(db)


async def resolve_actor_name(
    db: Any,
    user_id: str,
    board_id: str,
    *,
    query_scope: QueryScope | None = None,
) -> str:
    """Resolve a user/agent ID to a friendly display name."""
    agent = await _application_get(db, "agent", user_id)
    if agent:
        return agent.name
    board = await _application_get(db, "board", board_id)
    if _board_owner_matches(board, user_id, query_scope):
        return "Owner"
    if user_id == "dev-user":
        return "Owner"
    return user_id[:20]


async def log_card_collaboration_activity(
    db: Any,
    card_id: str,
    action: str,
    *,
    actor_id: str,
    actor_type: str,
    actor_name: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    """Log card collaboration activity from a transport-neutral caller."""
    card = await _application_get(db, "card", card_id)
    if not card:
        return
    resolved_name = actor_name or await resolve_actor_name(db, actor_id, card.board_id)
    await BoardService(db)._log_activity(
        board_id=card.board_id,
        card_id=card_id,
        action=action,
        actor_type=actor_type,
        actor_id=actor_id,
        actor_name=resolved_name,
        details=details,
    )


async def card_belongs_to_board(db: Any, board_id: str, card_id: str) -> bool:
    """Return whether a card exists on the given board."""
    card = await _application_get(db, "card", card_id)
    return bool(card and card.board_id == board_id)


async def update_resource_gate_board_settings(
    db: Any,
    board_id: str,
    user_id: str,
    *,
    require_spec_resource_task_coverage: bool,
) -> dict[str, Any] | None:
    """Update the Resource Gate board setting and return the persisted map.

    AF35-S3 C4 keeps SQLAlchemy JSON mutation mechanics inside services instead
    of the REST wrapper. The caller owns the transaction boundary.
    """

    board = await BoardService(db).get_board(board_id, user_id)
    if not board:
        return None
    settings = dict(board.settings or {})
    settings["require_spec_resource_task_coverage"] = (
        require_spec_resource_task_coverage
    )
    board.settings = settings
    board.mark_dirty("settings")
    return settings


async def comment_card_id(db: Any, comment_id: str) -> str | None:
    """Return a comment's card id, if the comment exists."""
    comment = await _application_get(db, "comment", comment_id)
    return comment.card_id if comment else None


async def resolve_choice_comment_actor_name(
    db: Any, comment_id: str, actor_id: str
) -> str | None:
    """Resolve the display name for a choice-comment response actor."""
    comment = await _application_get(db, "comment", comment_id)
    if not comment:
        return None
    card = await _application_get(db, "card", comment.card_id)
    board_id = card.board_id if card else ""
    return await resolve_actor_name(db, actor_id, board_id)


async def qa_card_id(db: Any, qa_id: str) -> str | None:
    """Return a card Q&A item's card id, if the item exists."""
    qa = await _application_get(db, "qa_item", qa_id)
    return qa.card_id if qa else None


async def compute_card_activity(db: Any, card_id: str, *, limit: int = 50) -> list[Any]:
    """Activity log for a single card, newest first (transport-free reader).

    Extracted verbatim from the legacy ``GET /cards/{id}/activity`` endpoint so the
    ``application/use_cases`` layer never touches ``select``/ORM directly (the
    relational ratchet gate). Runs the same ``ActivityLog`` query (card scope,
    ``created_at`` desc, bounded by ``limit``) and the same presentation via the
    shared ``activity_log_*`` helpers, returning the list of ``ActivityLogResponse``
    rows the REST adapter serializes unchanged. An unknown card id yields an empty
    list — exactly as the endpoint did (no 404).
    """
    from okto_pulse.core.models import ActivityLogResponse
    from okto_pulse.core.services.activity_log import (
        activity_log_summary,
        activity_log_trigger,
        sanitize_activity_details,
    )

    logs = await _application_list(
        db,
        "activity_log",
        filters=(_apf("card_id", "eq", card_id),),
        order_by=(("created_at", True),),
        limit=limit,
    )
    return [
        ActivityLogResponse(
            id=log.id,
            board_id=log.board_id,
            card_id=log.card_id,
            action=log.action,
            actor_type=log.actor_type,
            actor_id=log.actor_id,
            actor_name=log.actor_name,
            trigger=activity_log_trigger(log.details),
            summary=activity_log_summary(log.action, log.details),
            details=sanitize_activity_details(log.details),
            created_at=log.created_at,
        )
        for log in logs
    ]


async def compute_card_seen_status(db: Any, card_id: str) -> dict:
    """Per-item seen status (comments + QA) for a card, grouped by item id
    (transport-free reader).

    Extracted verbatim from the legacy ``GET /cards/{id}/seen`` endpoint so the
    ``application/use_cases`` layer never touches ``select``/ORM directly. Collects
    the card's comment/QA ids, joins ``AgentSeenItem`` to the agent name ordered by
    ``seen_at`` and groups into ``{item_id: [{agent_id, agent_name, seen_at}]}``.
    Returns ``{"items": {}}`` when the card has no comment/QA items.
    """
    comments = await _application_list(
        db,
        "comment",
        filters=(_apf("card_id", "eq", card_id),),
    )
    qa_items = await _application_list(
        db,
        "qa_item",
        filters=(_apf("card_id", "eq", card_id),),
    )
    comment_ids = [item.id for item in comments]
    qa_ids = [item.id for item in qa_items]
    all_ids = set(comment_ids + qa_ids)

    if not all_ids:
        return {"items": {}}

    # Get seen records for these items
    seen_results = await _application_list(
        db,
        "agent_seen_item",
        filters=(_apf("item_id", "in", all_ids),),
        order_by=(("seen_at", False),),
    )
    agent_ids = {seen.agent_id for seen in seen_results}
    agents = await _application_list(
        db,
        "agent",
        filters=(_apf("id", "in", agent_ids),),
    )
    agent_names = {agent.id: agent.name for agent in agents}

    # Group by item_id: {item_id: [{agent_name, seen_at}]}
    items: dict[str, list] = {}
    for seen in seen_results:
        if seen.item_id not in items:
            items[seen.item_id] = []
        items[seen.item_id].append(
            {
                "agent_id": seen.agent_id,
                "agent_name": agent_names.get(seen.agent_id),
                "seen_at": seen.seen_at.isoformat(),
            }
        )

    return {"items": items}


async def propagate_architecture_designs(
    db: Any,
    *,
    source_parent_type: str,
    source_parent_id: str,
    target_parent_type: str,
    target_parent_id: str,
    actor_id: str,
    mode: str | None = "copy",
    design_ids: list[str] | None = None,
) -> list[Any]:
    """Propagate architecture designs between SDLC artifacts.

    Modes:
    - copy/derive: snapshot copy, retaining source_design_id/source_ref.
    - reference_only/none: no snapshot copy; parent linkage carries traceability.
    """
    normalized = (mode or "copy").strip().lower()
    if normalized not in {"copy", "derive", "reference_only", "none"}:
        raise ValueError(
            "architecture_propagation_mode must be one of: copy, derive, "
            "reference_only, none"
        )
    if normalized in {"reference_only", "none"}:
        return []

    from okto_pulse.core.models.schemas import ArchitectureWarningAcknowledgementRequest
    from okto_pulse.core.services.architecture import ArchitecturePropagationService

    # Bug eded2f0e (R3, option B): SDLC artifact propagation is an INTERNAL
    # snapshot copy of an already-acknowledged source architecture design — not a
    # new authoring action. The copy still gets its OWN copy-scoped acknowledgement
    # record (copy_from_parent enforces an explicit ack for warning-bearing copies;
    # the gate is NOT weakened), supplied here by the system on the artifact's
    # behalf so legitimate propagation is not blocked.
    return await ArchitecturePropagationService(db).copy_from_parent(
        source_parent_type=source_parent_type,
        source_parent_id=source_parent_id,
        target_parent_type=target_parent_type,
        target_parent_id=target_parent_id,
        actor_id=actor_id,
        design_ids=design_ids,
        architecture_warning_acknowledgement=ArchitectureWarningAcknowledgementRequest(
            accepted=True,
            statement=(
                f"internal snapshot propagation of an already-acknowledged "
                f"{source_parent_type} architecture design"
            ),
        ),
    )


async def preflight_architecture_designs(
    db: Any,
    *,
    source_parent_type: str,
    source_parent_id: str,
    mode: str | None,
    design_ids: list[str] | None,
) -> None:
    """Fail an invalid/blocked Architecture selection before target creation."""

    normalized = (mode or "copy").strip().lower()
    if normalized not in {"copy", "derive", "reference_only", "none"}:
        raise ValueError(
            "architecture_propagation_mode must be one of: copy, derive, "
            "reference_only, none"
        )
    if normalized in {"reference_only", "none"}:
        return

    from okto_pulse.core.services.architecture import ArchitecturePropagationService

    await ArchitecturePropagationService(db).preflight_copy_from_parent(
        source_parent_type=source_parent_type,
        source_parent_id=source_parent_id,
        design_ids=design_ids,
    )


def _resource_propagation_summary(
    *,
    source_parent_type: str,
    source_parent_id: str,
    target_parent_type: str,
    target_parent_id: str,
    architecture_mode: str | None,
    architecture_requested_ids: list[str] | None,
    architecture_designs: list[Any],
    artifact_counts: dict[str, int] | None,
) -> dict[str, Any]:
    """Stable write-result projection shared by refinement/spec derive flows."""

    counts = {
        "mockup": int((artifact_counts or {}).get("mockup", 0)),
        "knowledge_base": int((artifact_counts or {}).get("knowledge_base", 0)),
        "architecture": len(architecture_designs),
    }
    mode = (architecture_mode or "copy").strip().lower()
    architecture_status = (
        "inherited_reference"
        if mode in {"reference_only", "none"}
        else (
            "created_with_resources"
            if architecture_designs
            else "created_without_required_resources"
        )
    )
    aggregate = (
        "created_with_resources"
        if any(counts.values())
        else (
            "created_with_inherited_references"
            if mode in {"reference_only", "none"}
            else "created_without_required_resources"
        )
    )
    return {
        "status": aggregate,
        "source": {
            "entity_type": source_parent_type,
            "entity_id": source_parent_id,
        },
        "target": {
            "entity_type": target_parent_type,
            "entity_id": target_parent_id,
        },
        "counts": counts,
        "by_type": {
            "mockup": {
                "status": (
                    "created_with_resources"
                    if counts["mockup"]
                    else "created_without_required_resources"
                ),
                "copied": counts["mockup"],
            },
            "knowledge_base": {
                "status": (
                    "created_with_resources"
                    if counts["knowledge_base"]
                    else "created_without_required_resources"
                ),
                "copied": counts["knowledge_base"],
            },
            "architecture": {
                "status": architecture_status,
                "mode": mode,
                "requested_ids": list(architecture_requested_ids or []),
                "copied": counts["architecture"],
                "snapshot_ids": [item.id for item in architecture_designs],
                "source_design_ids": [
                    item.source_design_id or item.id for item in architecture_designs
                ],
            },
        },
    }


class BoardService:
    """Service for board operations."""

    def __init__(self, db: Any):
        self.db = db

    async def create_board(
        self, user_id: str, data: BoardCreate, realm_id: str | None = None
    ) -> ApplicationRecord:
        """Create a new board."""
        from okto_pulse.core.services.default_board_configuration import (
            BOARD_EVENT_APPLIED,
            BOARD_EVENT_FALLBACK,
            DefaultBoardConfigurationService,
        )

        # FR3: the single provider resolves the active default template (if any)
        # and produces the effective settings + snapshot metadata in THIS same
        # transaction. No active template -> graceful forward-safe new-board
        # defaults (including reviewer separation=enforce), no snapshot and no
        # error (AC11). Snapshot metadata is persisted on
        # Board.default_config_snapshot, OUTSIDE Board.settings (FR4).
        _config_service = DefaultBoardConfigurationService(self.db)
        (
            effective_settings,
            snapshot_meta,
        ) = await _config_service.build_snapshot_for_create(
            settings_override=getattr(data, "settings", None), applied_by=user_id
        )
        board = _new_application_record(
            "board",
            name=data.name,
            description=data.description,
            owner_id=user_id,
            realm_id=realm_id,
            settings=effective_settings,
            default_config_snapshot=snapshot_meta,
        )
        await _application_add(self.db, board)
        actor_name = await resolve_actor_name(self.db, user_id, board.id)
        await self._log_activity(
            board_id=board.id,
            action="board_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"name": data.name},
        )
        # FR9: board-scoped audit of which default-config path created the board.
        if snapshot_meta is not None:
            await self._log_activity(
                board_id=board.id,
                action=BOARD_EVENT_APPLIED,
                actor_type="user",
                actor_id=user_id,
                actor_name=actor_name,
                details={
                    "template_id": snapshot_meta["template_id"],
                    "template_version": snapshot_meta["template_version"],
                    "override_summary": snapshot_meta["override_summary"],
                },
            )
        else:
            await self._log_activity(
                board_id=board.id,
                action=BOARD_EVENT_FALLBACK,
                actor_type="user",
                actor_id=user_id,
                actor_name=actor_name,
                details={"reason": "no_active_default_board_configuration"},
            )
        # FR5/FR6/#3/#4: the umbrella service orchestrates every default adapter
        # (guidelines + design system) onto the new board IN THIS transaction. Any
        # adapter failure raises default_materialization_failed so the whole
        # create_board reverts (no partial board/link/snapshot); no active
        # template -> no-op.
        await _config_service.apply_default_config_to_board(
            board.id,
            actor=user_id,
            template_snapshot=snapshot_meta,
        )
        # Eagerly bootstrap the per-board graph backend graph. This keeps board
        # creation on the slow path (~1-2s) so subsequent consolidation /
        # MCP query paths stay on the hot path.
        # Failures are logged but don't abort board creation — the
        # lazy bootstrap in BoardConnection.__init__ is the safety net.
        try:
            # R05-C: migrated off the direct kg.schema symbol onto the #06
            # GraphSchemaManager port (ensure_bootstrapped wraps the same
            # ensure_board_graph_bootstrapped — bit-identical, now via the port).
            from okto_pulse.core.kg.interfaces.registry import get_kg_registry

            await get_kg_registry().graph_schema_manager.ensure_bootstrapped(board.id)
        except Exception as exc:
            import logging

            logging.getLogger("okto_pulse.core.services.main").warning(
                "board_create.bootstrap_failed board=%s err=%s — lazy path will retry",
                board.id,
                exc,
            )
        return board

    async def record_checklist_binding_change(
        self,
        *,
        board_id: str,
        actor_id: str,
        binding: Any,
        previous_binding: Any | None,
        change_source: str,
    ) -> None:
        """Stage the A3 activity history and durable event in this transaction."""

        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import ChecklistBindingChanged

        actor_name = await resolve_actor_name(self.db, actor_id, board_id)
        details = {
            "target_type": binding.target_type.value,
            "phase": binding.phase.value,
            "template_version": binding.template_version,
            "mode": binding.mode.value,
            "binding_version": binding.version,
            "binding_digest": binding.digest,
            "previous_mode": (
                None if previous_binding is None else previous_binding.mode.value
            ),
            "previous_binding_version": (
                None if previous_binding is None else previous_binding.version
            ),
            "change_source": change_source,
        }
        await self._log_activity(
            board_id=board_id,
            action="spec_checklist_binding_changed",
            actor_type="user",
            actor_id=actor_id,
            actor_name=actor_name,
            details=details,
        )
        await event_publish(
            ChecklistBindingChanged(
                board_id=board_id,
                actor_id=actor_id,
                target_type=binding.target_type.value,
                phase=binding.phase.value,
                template_version=binding.template_version,
                mode=binding.mode.value,
                binding_version=binding.version,
                binding_digest=binding.digest,
                previous_mode=details["previous_mode"],
                previous_binding_version=details["previous_binding_version"],
                change_source=change_source,
            ),
            session=self.db,
        )

    async def get_board(
        self,
        board_id: str,
        user_id: str | None = None,
        *,
        query_scope: QueryScope | None = None,
    ) -> ApplicationRecord | None:
        """Get a board by ID with all relationships."""
        clauses = _board_scope_clauses(
            board_id=board_id,
            user_id=user_id,
            query_scope=query_scope,
            require_ownership=(
                query_scope.require_ownership if query_scope is not None else True
            ),
        )
        if clauses is None:
            return None
        rows = await _application_list(
            self.db,
            "board",
            filters=tuple(clauses),
            includes=(
                "cards.attachments",
                "cards.qa_items",
                "cards.comments",
                "cards.architecture_designs",
            ),
            limit=1,
        )
        return rows[0] if rows else None

    async def list_boards(
        self,
        user_id: str,
        offset: int = 0,
        limit: int = 20,
        realm_id: str | None = None,
        view: str = "my",
        query_scope: QueryScope | None = None,
    ) -> tuple[list[ApplicationRecord], int]:
        """List boards for a user.

        view: "my" (owned), "shared" (shared with user), "all" (union)
        """
        scoped_user_id = _scope_actor_id(user_id, query_scope) or user_id
        scoped_realm_id = _scope_realm_id(realm_id, query_scope)
        filters: list[ApplicationFilter] = []
        if scoped_realm_id:
            filters.append(_apf("realm_id", "eq", scoped_realm_id))
        if query_scope is not None:
            if query_scope.target_board_id is not None:
                if not query_scope.allows_board_id(query_scope.target_board_id):
                    return [], 0
            elif not query_scope.require_ownership and not query_scope.allow_all_boards:
                return [], 0
            if query_scope.target_board_id:
                filters.append(_apf("id", "eq", query_scope.target_board_id))
            if query_scope.allowed_board_ids is not None:
                allowed_board_ids = tuple(query_scope.allowed_board_ids)
                if not allowed_board_ids:
                    return [], 0
                filters.append(_apf("id", "in", allowed_board_ids))

        shared_rows = await _application_list(
            self.db,
            "board_share",
            filters=(_apf("user_id", "eq", scoped_user_id),),
        )
        shared_ids = {row.board_id for row in shared_rows}
        if view == "shared":
            filters.append(_apf("id", "in", shared_ids))
        elif view == "all":
            owned_rows = await _application_list(
                self.db,
                "board",
                filters=tuple([*filters, _apf("owner_id", "eq", scoped_user_id)]),
            )
            combined_ids = shared_ids | {row.id for row in owned_rows}
            filters.append(_apf("id", "in", combined_ids))
        else:
            filters.append(_apf("owner_id", "eq", scoped_user_id))

        all_boards = await _application_list(
            self.db,
            "board",
            filters=tuple(filters),
            order_by=(("updated_at", True),),
        )
        total = len(all_boards)
        boards = all_boards[offset : offset + limit]
        return boards, total

    async def update_board(
        self,
        board_id: str,
        user_id: str,
        data: BoardUpdate,
        *,
        query_scope: QueryScope | None = None,
    ) -> ApplicationRecord | None:
        """Update a board."""
        board = await self.get_board(board_id, user_id, query_scope=query_scope)
        if not board:
            return None

        previous_settings = dict(board.settings or {})
        update_data = data.model_dump(exclude_unset=True)
        # Serialize settings if present
        if "settings" in update_data and update_data["settings"] is not None:
            update_data["settings"] = BoardGovernanceService.merge_settings_patch(
                previous_settings,
                update_data["settings"],
            )
        for key, value in update_data.items():
            setattr(board, key, value)
            if key == "settings":
                board.mark_dirty("settings")

        settings_changed = (
            "settings" in update_data and update_data.get("settings") is not None
        )
        if settings_changed:
            next_settings = dict(board.settings or {})
            previous_auto = bool(
                previous_settings.get("auto_derive_spec_resources_enabled", False)
            )
            next_auto = bool(
                next_settings.get("auto_derive_spec_resources_enabled", False)
            )
            previous_types = list(
                previous_settings.get("auto_derive_spec_resource_types") or []
            )
            next_types = list(
                next_settings.get("auto_derive_spec_resource_types") or []
            )
            resource_automation_changed = (
                previous_auto != next_auto or previous_types != next_types
            )
            if next_auto and resource_automation_changed:
                await _application_flush(self.db)
                await SpecResourcePropagationService(self.db).propagate_for_board(
                    board_id=board_id,
                    actor_id=user_id,
                    trigger="board_settings_auto_derive_changed",
                )

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        if settings_changed:
            for (
                setting_key,
                old_value,
                new_value,
            ) in BoardGovernanceService.changed_governance_settings(
                previous_settings,
                board.settings,
            ):
                details = build_board_governance_setting_changed_details(
                    board_id=board_id,
                    actor_id=user_id,
                    setting_key=setting_key,
                    old_effective_value=old_value,
                    new_effective_value=new_value,
                    surface="board_patch",
                )
                await self._log_activity(
                    board_id=board_id,
                    action="board_governance_setting_changed",
                    actor_type="user",
                    actor_id=user_id,
                    actor_name=actor_name,
                    details=details,
                )
                emit_governance_metric(details, raise_on_violation=False)
        if "description" in update_data and not (board.description or "").strip():
            details = build_board_missing_context_warning_details(
                board_id=board_id,
                warning_code="board_summary_missing",
                surface="board_patch",
            )
            emit_governance_metric(details, raise_on_violation=False)
        await self._log_activity(
            board_id=board_id,
            action="board_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details=update_data,
        )
        return board

    async def delete_board(self, board_id: str, user_id: str) -> bool:
        """Delete a board."""
        board = await self.get_board(board_id, user_id)
        if not board:
            return False

        await _application_delete(self.db, board)
        return True

    async def _log_activity(
        self,
        board_id: str,
        action: str,
        actor_type: str,
        actor_id: str,
        actor_name: str,
        card_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Log an activity."""
        log = _new_application_record(
            "activity_log",
            board_id=board_id,
            card_id=card_id,
            action=action,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            details=details,
        )
        await _application_add(self.db, log)


def _structured_item_ids(values: object) -> set[str]:
    if not isinstance(values, list):
        return set()
    return {
        str(item.get("id") if isinstance(item, Mapping) else getattr(item, "id", ""))
        for item in values
        if ((isinstance(item, Mapping) and item.get("id")) or getattr(item, "id", None))
    }


def _validate_card_traceability_targets(
    spec: ApplicationRecord,
    data: CardCreate,
) -> None:
    """Validate every requested traceability token before staging the card."""

    available = {
        "scenario": _structured_item_ids(spec.test_scenarios),
        "fr": _structured_item_ids(spec.functional_requirements),
        "rule": _structured_item_ids(spec.business_rules),
    }
    requested = (
        (
            "scenario",
            tuple(getattr(data, "test_scenario_ids", None) or ()),
        ),
        (
            "fr",
            tuple(getattr(data, "functional_requirement_ids", None) or ()),
        ),
        (
            "rule",
            tuple(getattr(data, "business_rule_ids", None) or ()),
        ),
    )
    for target_type, target_ids in requested:
        for target_id in target_ids:
            if target_id not in available[target_type]:
                raise TraceabilityTargetNotFoundError(target_type, target_id)


def _validate_card_knowledge_relevance_links(
    spec: ApplicationRecord,
    envelope: object,
) -> None:
    """Validate v2 relevance links against the linked Spec before ``add``.

    This is deliberately a pure pre-persistence check.  It prevents a card row
    (and all traceability/event side effects) from being staged when any FR,
    AC, or scenario token is foreign or missing.
    """

    from okto_pulse.core.domain.knowledge_selection import (
        KnowledgeRelevanceEntityType,
    )
    from okto_pulse.core.models.knowledge_propagation import (
        KnowledgePropagationEnvelopeV2,
    )
    from okto_pulse.core.services.knowledge_propagation import (
        KnowledgePropagationServiceError,
    )

    if not isinstance(envelope, KnowledgePropagationEnvelopeV2):
        raise KnowledgePropagationServiceError(
            "knowledge_propagation_envelope_required",
            "the authoritative v2 envelope is required",
        )
    available = {
        KnowledgeRelevanceEntityType.FUNCTIONAL_REQUIREMENT: _structured_item_ids(
            spec.functional_requirements
        ),
        KnowledgeRelevanceEntityType.ACCEPTANCE_CRITERION: _structured_item_ids(
            spec.acceptance_criteria
        ),
        KnowledgeRelevanceEntityType.TEST_SCENARIO: _structured_item_ids(
            spec.test_scenarios
        ),
    }
    requested = [
        (link.entity_type.value, link.entity_id) for link in envelope.relevance_links
    ]
    missing = [
        {"entity_type": link.entity_type.value, "entity_id": link.entity_id}
        for link in envelope.relevance_links
        if link.entity_id not in available[link.entity_type]
    ]
    if missing:
        raise KnowledgePropagationServiceError(
            "knowledge_relevance_invalid",
            "all relevance links must resolve on the card's linked spec",
            details={
                "requested": requested,
                "matched": [
                    item
                    for item in requested
                    if {
                        "entity_type": item[0],
                        "entity_id": item[1],
                    }
                    not in missing
                ],
                "missing": missing,
            },
        )


def _governed_spec_knowledge_parent(
    *,
    ideation_id: str | None,
    refinement_id: str | None,
) -> tuple[str, str] | None:
    """Resolve the one authoritative parent used by Spec Knowledge v2."""

    if refinement_id:
        return ("refinement", str(refinement_id))
    if ideation_id:
        return ("ideation", str(ideation_id))
    return None


async def _reset_v2_knowledge_for_relink(
    db: Any,
    *,
    board_id: str,
    target_type: str,
    target_id: str,
    previous_parent: tuple[str, str] | None,
    next_parent: tuple[str, str] | None,
    actor_id: str,
    port: KnowledgePropagationPort | None = None,
) -> bool:
    """Reset active v2 selection before changing a governed parent link.

    The persistence authority is mandatory.  Tests or Core-only compositions
    that intentionally exercise legacy behavior must inject an explicit
    legacy/null port; absence is not evidence that no durable v2 scope exists.
    """

    if previous_parent == next_parent:
        return False

    from okto_pulse.core.ports.knowledge_propagation import (
        KnowledgeParentKey,
        KnowledgeTargetKey,
    )
    from okto_pulse.core.services.knowledge_propagation import (
        KnowledgePropagationService,
        KnowledgeRelinkResetCommand,
    )

    target = KnowledgeTargetKey(
        board_id=board_id,
        target_type=target_type,
        target_id=target_id,
    )
    service = KnowledgePropagationService(port)
    current = await service.read(db, target)
    if not current.v2_active:
        return False

    def parent_key(value: tuple[str, str] | None) -> KnowledgeParentKey | None:
        if value is None:
            return None
        return KnowledgeParentKey(
            board_id=board_id,
            parent_type=value[0],
            parent_id=value[1],
        )

    idempotency_material = "|".join(
        (
            board_id,
            target_type,
            target_id,
            *(previous_parent or ("none", "none")),
            *(next_parent or ("none", "none")),
            str(current.scope_revision),
        )
    )
    idempotency_key = (
        "knowledge-relink:v2:"
        f"{target_type}:"
        f"{hashlib.sha256(idempotency_material.encode('utf-8')).hexdigest()}"
    )
    await service.reset_for_relink(
        db,
        KnowledgeRelinkResetCommand(
            target=target,
            previous_parent=parent_key(previous_parent),
            next_parent=parent_key(next_parent),
            actor_id=actor_id,
            expected_revision=current.scope_revision,
            idempotency_key=idempotency_key,
        ),
    )
    return True


class CardService:
    """Service for card operations."""

    def __init__(
        self,
        db: Any,
        *,
        knowledge_propagation_port: KnowledgePropagationPort | None = None,
    ):
        self.db = db
        self._knowledge_propagation_port = knowledge_propagation_port
        self._cognitive_closeout_gate_factory: Callable[[], Any] = (
            _build_default_cognitive_closeout_gate
        )
        self._cognitive_readiness_service_factory: Callable[[], Any] = (
            _build_default_cognitive_readiness_service
        )

    async def _validate_cognitive_done(
        self,
        card: ApplicationRecord,
        board: ApplicationRecord | None = None,
        *,
        read_only_preview: bool = False,
    ) -> None:
        if board is None:
            board = await _application_get(self.db, "board", card.board_id)
        await _evaluate_entity_cognitive_done_or_raise(
            db=self.db,
            gate_factory=self._cognitive_closeout_gate_factory,
            readiness_service_factory=self._cognitive_readiness_service_factory,
            board=board,
            board_id=card.board_id,
            entity_type=_card_cognitive_entity_type(card),
            entity_id=card.id,
            entity=card,
            target_label="card",
            # Health composition is read-only and must match the mutation path.
            # Skipping it in previews makes a healthy graph look unavailable.
            resolve_graph_state=True,
        )

    @staticmethod
    def _max_scenarios_per_card(board: ApplicationRecord | None) -> int:
        board_settings = (getattr(board, "settings", None) or {}) if board else {}
        try:
            value = int(board_settings.get("max_scenarios_per_card", 3))
        except (TypeError, ValueError):
            value = 3
        return max(1, value)

    def _raise_max_scenarios_per_card_exceeded(
        self,
        *,
        provided_count: int,
        max_per_card: int,
    ) -> None:
        raise CardOperationError(
            "max_scenarios_per_card_exceeded",
            (
                f"Cannot link {provided_count} scenarios to a single test card. "
                f"Board limit is {max_per_card} scenarios per card. "
                "Create separate test cards for better traceability."
            ),
            remediation=(
                "Create separate test cards and keep each one within the board limit."
            ),
            facts={
                "provided_count": provided_count,
                "max_scenarios_per_card": max_per_card,
            },
        )

    async def _validate_test_card_scenario_ids(
        self,
        *,
        board: ApplicationRecord | None,
        spec: ApplicationRecord,
        scenario_ids: list[str] | None,
    ) -> None:
        """Validate test-card scenario references against spec and board limits."""
        if not scenario_ids:
            return

        max_per_card = self._max_scenarios_per_card(board)
        if len(scenario_ids) > max_per_card:
            self._raise_max_scenarios_per_card_exceeded(
                provided_count=len(scenario_ids),
                max_per_card=max_per_card,
            )

        spec_scenario_ids = {s["id"] for s in (spec.test_scenarios or [])}
        invalid_ids = [sid for sid in scenario_ids if sid not in spec_scenario_ids]
        if invalid_ids:
            raise ValueError(
                f"Test scenario(s) not found in spec '{spec.title}': {invalid_ids}. "
                f"Available scenarios: {sorted(spec_scenario_ids)}"
            )

    async def create_card(
        self,
        board_id: str,
        user_id: str,
        data: CardCreate,
        skip_ownership_check: bool = False,
        *,
        query_scope: QueryScope | None = None,
        target_id: str | None = None,
        knowledge_propagation_v2: bool = False,
        actor_type: str = "user",
        actor_name: str | None = None,
        activity_details: Mapping[str, Any] | None = None,
    ) -> ApplicationRecord | None:
        """Create a new card in a board.

        ``target_id`` and ``knowledge_propagation_v2`` are an opt-in pair used
        by the governed selective-propagation boundary.  Legacy callers omit
        both and retain the original generated identity plus automatic v1
        snapshot behavior.
        """
        if (target_id is None) != (not knowledge_propagation_v2):
            raise ValueError(
                "knowledge_propagation_v2 requires an explicit deterministic target_id"
            )
        board_query = _board_scope_select(
            board_id=board_id,
            user_id=user_id,
            query_scope=None if skip_ownership_check else query_scope,
            require_ownership=not skip_ownership_check,
        )
        if board_query is None:
            return None
        board_rows = await _application_run(self.db, board_query)
        board = board_rows[0] if board_rows else None
        if not board:
            return None

        # --- Bug card validations (before spec check, since spec is auto-resolved) ---
        card_type_val = getattr(data, "card_type", "normal") or "normal"
        origin_task_id = getattr(data, "origin_task_id", None)
        if card_type_val != "bug" and origin_task_id:
            raise ValueError("origin_task_id is only allowed for bug cards")

        # Every card type must enter through the beginning of the lifecycle.
        # Persisting an advanced state here bypasses move_card's transition,
        # resource, validation, audit, and KG event boundaries.
        if data.status not in (CardStatus.NOT_STARTED, CardStatus.STARTED):
            raise CardOperationError(
                "card_initial_status_invalid",
                (
                    "Cards can only be created with status 'not_started' or "
                    "'started'. Use move_card to advance the lifecycle."
                ),
                remediation=(
                    "Create the card in an initial status, then use move_card "
                    "for every subsequent transition."
                ),
                facts={
                    "requested_status": data.status.value,
                    "allowed_statuses": [
                        CardStatus.NOT_STARTED.value,
                        CardStatus.STARTED.value,
                    ],
                    "card_type": card_type_val,
                },
            )

        if card_type_val == "bug":
            if not origin_task_id:
                raise ValueError("origin_task_id is required for bug cards")

            # Resolve the governed lineage before constructing/flushing the bug card.
            # Missing and foreign-board ids intentionally share one response so the
            # caller cannot use this write path to probe another board's cards.
            origin_task = await _application_get(self.db, "card", origin_task_id)
            if not origin_task or origin_task.board_id != board_id:
                raise ValueError("Origin task not found on this board")

            # Validate origin task has a spec
            if not origin_task.spec_id:
                raise ValueError(
                    "Origin task has no linked spec — bug cards require a spec-linked task"
                )

            if knowledge_propagation_v2 and data.spec_id != origin_task.spec_id:
                from okto_pulse.core.services.knowledge_propagation import (
                    KnowledgePropagationServiceError,
                )

                raise KnowledgePropagationServiceError(
                    "knowledge_propagation_parent_changed",
                    "the bug origin moved to another Spec after propagation preflight",
                    details={
                        "origin_task_id": origin_task_id,
                        "expected_spec_id": data.spec_id,
                        "actual_spec_id": origin_task.spec_id,
                    },
                )

            # Preserve the established fail-closed ordering for ordinary bug
            # creation. Direct STARTED creation defers this physical write
            # until after the precedence gate below, so the dependency graph
            # fence remains the first mutation on that execution-start edge.
            if knowledge_propagation_v2 and not transition_starts_card_execution(
                CardStatus.NOT_STARTED,
                data.status,
            ):
                from okto_pulse.core.services.knowledge_propagation import (
                    KnowledgePropagationServiceError,
                )

                expected_spec_id = data.spec_id
                if not expected_spec_id or not await _application_fence(
                    self.db,
                    "card",
                    origin_task_id,
                    expected_values={
                        "board_id": board_id,
                        "spec_id": expected_spec_id,
                    },
                ):
                    raise KnowledgePropagationServiceError(
                        "knowledge_propagation_parent_changed",
                        ("the bug origin changed after propagation preflight"),
                        details={
                            "origin_task_id": origin_task_id,
                            "expected_spec_id": expected_spec_id,
                        },
                    )

            # Auto-resolve spec_id from origin task
            data.spec_id = origin_task.spec_id

            # Validate required bug fields
            if not data.severity:
                raise ValueError(
                    "severity is required for bug cards (critical, major, minor)"
                )
            if not data.expected_behavior:
                raise ValueError("expected_behavior is required for bug cards")
            if not data.observed_behavior:
                raise ValueError("observed_behavior is required for bug cards")

        # Enforce: every card must be linked to a spec
        if not data.spec_id:
            raise ValueError(
                "Every task must be linked to a spec. Provide spec_id when creating a card. "
                "If this task is not related to any spec, create a spec first."
            )

        # --- Test card validations ---
        if card_type_val == "test":
            if not data.test_scenario_ids:
                raise ValueError(
                    "test_scenario_ids is required for test cards and must contain at least one scenario ID"
                )

        # Enforce: spec status rules for card creation
        # - Normal tasks: spec must be 'approved' or 'in_progress'
        # - Bug cards: also allowed when spec is 'done'
        # - Test cards: also allowed when spec is 'validated'
        spec = await _application_get(self.db, "spec", data.spec_id)
        if not spec:
            raise ValueError(f"Spec '{data.spec_id}' not found")

        if card_type_val == "bug":
            allowed_statuses = {
                SpecStatus.APPROVED,
                SpecStatus.IN_PROGRESS,
                SpecStatus.DONE,
            }
            status_msg = "'approved', 'in_progress', or 'done'"
        elif card_type_val == "test":
            allowed_statuses = {
                SpecStatus.APPROVED,
                SpecStatus.VALIDATED,
                SpecStatus.IN_PROGRESS,
                SpecStatus.DONE,
            }
            status_msg = "'approved', 'validated', 'in_progress', or 'done'"
        else:
            allowed_statuses = {
                SpecStatus.APPROVED,
                SpecStatus.IN_PROGRESS,
                SpecStatus.DONE,
            }
            status_msg = "'approved', 'in_progress', or 'done'"

        if spec.status not in allowed_statuses:
            raise ValueError(
                f"{card_type_val.capitalize()} cards can only be created for specs in {status_msg} status. "
                f"Spec '{spec.title}' is currently '{spec.status.value}'."
            )

        # Validate test_scenario_ids against spec and board caps for test cards.
        if card_type_val == "test" and data.test_scenario_ids:
            await self._validate_test_card_scenario_ids(
                board=board,
                spec=spec,
                scenario_ids=list(data.test_scenario_ids),
            )

        if knowledge_propagation_v2:
            _validate_card_traceability_targets(spec, data)
            _validate_card_knowledge_relevance_links(
                spec,
                getattr(data, "knowledge_propagation", None),
            )

        # Direct creation in STARTED is the same execution-start edge as
        # NOT_STARTED -> STARTED in move_card.  Acquire the dependency-graph
        # fence, lock/revalidate the source Spec lifecycle identity, check the
        # current readiness projection and mark this edition started before
        # any card row, audit, propagation or domain event is staged.  The
        # caller-owned transaction retains both the fence and marker through
        # the eventual card write/commit; any later failure rolls them back
        # together.
        if transition_starts_card_execution(CardStatus.NOT_STARTED, data.status):
            from okto_pulse.core.ports.relational_application import (
                require_relational_application_adapter,
            )
            from okto_pulse.core.services.spec_dependency import (
                SpecDependencyService,
            )

            await SpecDependencyService(
                require_relational_application_adapter().spec_dependencies(self.db),
                self.db,
            ).require_ready_for_execution(
                board_id=board_id,
                spec_id=spec.id,
                mark_started=True,
                expected_edition=int(getattr(spec, "edition", 1) or 1),
                expected_status=spec.status,
                expected_archived=bool(getattr(spec, "archived", False)),
            )

        # The propagation parent CAS is intentionally after the precedence
        # gate.  On STARTED creation this keeps the graph/source fence as the
        # first physical write while preserving the existing fail-closed
        # parent-change contract in the same transaction.
        if (
            knowledge_propagation_v2
            and card_type_val == "bug"
            and transition_starts_card_execution(
                CardStatus.NOT_STARTED,
                data.status,
            )
        ):
            from okto_pulse.core.services.knowledge_propagation import (
                KnowledgePropagationServiceError,
            )

            expected_spec_id = data.spec_id
            if not expected_spec_id or not await _application_fence(
                self.db,
                "card",
                origin_task_id,
                expected_values={
                    "board_id": board_id,
                    "spec_id": expected_spec_id,
                },
            ):
                raise KnowledgePropagationServiceError(
                    "knowledge_propagation_parent_changed",
                    ("the bug origin changed after propagation preflight"),
                    details={
                        "origin_task_id": origin_task_id,
                        "expected_spec_id": expected_spec_id,
                    },
                )

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=board_id,
            actor_id=user_id,
            entity_type="spec",
            entity_id=spec.id,
            critical_action=CriticalAction.CARD_CREATE,
            surface="service",
            actor_type=actor_type,
        )

        # Get max position for the status column
        status_cards = await _application_list(
            self.db,
            "card",
            filters=(
                _apf("board_id", "eq", board_id),
                _apf("status", "eq", data.status),
            ),
        )
        max_pos = max((item.position for item in status_cards), default=-1)

        card = _new_application_record(
            "card",
            **({"id": target_id} if target_id is not None else {}),
            board_id=board_id,
            spec_id=data.spec_id,
            title=data.title,
            description=data.description,
            details=data.details,
            status=data.status,
            priority=data.priority,
            position=max_pos + 1,
            assignee_id=data.assignee_id,
            created_by=user_id,
            due_date=data.due_date,
            labels=data.labels,
            test_scenario_ids=data.test_scenario_ids,
            card_type=card_type_val,
            origin_task_id=origin_task_id,
            severity=getattr(data, "severity", None),
            expected_behavior=getattr(data, "expected_behavior", None),
            observed_behavior=getattr(data, "observed_behavior", None),
            steps_to_reproduce=getattr(data, "steps_to_reproduce", None),
            action_plan=getattr(data, "action_plan", None),
        )
        await _application_add(
            self.db,
            card,
            conflict_error=(
                ApplicationRecordConflictError("card", card.id)
                if knowledge_propagation_v2
                else None
            ),
        )

        traceability_targets = [
            *(("scenario", target_id) for target_id in (data.test_scenario_ids or [])),
            *(
                ("fr", target_id)
                for target_id in (
                    getattr(data, "functional_requirement_ids", None) or []
                )
            ),
            *(
                ("rule", target_id)
                for target_id in (getattr(data, "business_rule_ids", None) or [])
            ),
        ]
        traceability = link_card_traceability(
            spec=spec,
            card=card,
            targets=traceability_targets,
        )

        if card_type_val == "bug":
            await self._inherit_bug_origin_traceability(
                bug_card=card,
                origin_task_id=origin_task_id,
                spec=spec,
            )

        await SpecResourcePropagationService(self.db).propagate_for_card(
            board_id=board_id,
            spec_id=card.spec_id,
            card_id=card.id,
            actor_id=user_id,
            trigger="card_created",
            excluded_resource_types=(
                {"knowledge_base"} if knowledge_propagation_v2 else None
            ),
        )
        card = await _application_refresh(self.db, card)

        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import CardCreated

        await event_publish(
            CardCreated(
                board_id=board_id,
                actor_id=user_id,
                card_id=card.id,
                spec_id=card.spec_id,
                sprint_id=card.sprint_id,
                card_type=card_type_val,
                priority=data.priority.value,
            ),
            session=self.db,
        )

        resolved_actor_name = actor_name or await resolve_actor_name(
            self.db,
            user_id,
            board_id,
        )
        extra_activity_details = {
            field: activity_log_value(value)
            for field, value in (activity_details or {}).items()
        }
        await self._log_activity(
            board_id=board_id,
            card_id=card.id,
            action="card_created",
            actor_type=actor_type,
            actor_id=user_id,
            actor_name=resolved_actor_name,
            details={
                **extra_activity_details,
                "title": data.title,
                "status": data.status.value,
                "priority": data.priority.value,
                "traceability": traceability.to_dict(),
            },
        )
        return card

    async def _inherit_bug_origin_traceability(
        self,
        *,
        bug_card: ApplicationRecord,
        origin_task_id: str | None,
        spec: ApplicationRecord | None = None,
    ) -> None:
        """Attach a new bug to the same spec traceability items as its origin task."""
        if not origin_task_id or not bug_card.spec_id:
            return

        if spec is None:
            spec = await _application_get(self.db, "spec", bug_card.spec_id)
        if spec is None:
            return

        inherited_scenario_ids: list[str] = []

        def inherit_linked_task_ids(
            field_name: str, *, collect_scenarios: bool = False
        ) -> None:
            items = getattr(spec, field_name, None) or []
            changed = False
            for item in items:
                if not isinstance(item, dict):
                    continue
                linked_task_ids = list(item.get("linked_task_ids") or [])
                origin_is_linked = origin_task_id in linked_task_ids
                if origin_is_linked and bug_card.id not in linked_task_ids:
                    linked_task_ids.append(bug_card.id)
                    item["linked_task_ids"] = linked_task_ids
                    changed = True
                if collect_scenarios and origin_is_linked:
                    scenario_id = item.get("id")
                    if scenario_id and scenario_id not in inherited_scenario_ids:
                        inherited_scenario_ids.append(scenario_id)
            if changed:
                spec.mark_dirty(field_name)

        inherit_linked_task_ids("test_scenarios", collect_scenarios=True)
        inherit_linked_task_ids("business_rules")
        inherit_linked_task_ids("api_contracts")
        inherit_linked_task_ids("integration_requirements")
        inherit_linked_task_ids("observability_requirements")
        inherit_linked_task_ids("technical_requirements")
        inherit_linked_task_ids("decisions")

        if inherited_scenario_ids:
            current_scenarios = list(bug_card.test_scenario_ids or [])
            merged = current_scenarios + [
                scenario_id
                for scenario_id in inherited_scenario_ids
                if scenario_id not in current_scenarios
            ]
            if merged != current_scenarios:
                bug_card.test_scenario_ids = merged
                bug_card.mark_dirty("test_scenario_ids")

    async def get_card(self, card_id: str) -> ApplicationRecord | None:
        """Get a card by ID with all relationships."""
        return await _application_get(
            self.db,
            "card",
            card_id,
            includes=(
                "attachments",
                "qa_items",
                "comments",
                "architecture_designs",
            ),
        )

    async def update_card(
        self,
        card_id: str,
        user_id: str,
        data: CardUpdate,
        *,
        allow_card_resource_write: bool = False,
        actor_type: str = "user",
        actor_name: str | None = None,
        activity_details: Mapping[str, Any] | None = None,
    ) -> ApplicationRecord | None:
        """Update a card."""
        card = await self.get_card(card_id)
        if not card:
            return None
        require_card_operational_mutation_allowed(card, operation="update_card")

        update_data = data.model_dump(exclude_unset=True)
        if "status" in update_data:
            requested_status = update_data["status"]
            raise CardOperationError(
                "card_status_update_requires_move",
                "Card status cannot be changed through update_card.",
                remediation=(
                    "Use move_card so transition gates, validation evidence, "
                    "audit events, and KG projections are enforced."
                ),
                facts={
                    "card_id": card.id,
                    "current_status": card.status.value,
                    "requested_status": (
                        requested_status.value
                        if isinstance(requested_status, CardStatus)
                        else requested_status
                    ),
                },
            )

        archived_block = archived_card_block(
            CardTransitionFacts(
                card_id=card.id,
                old_status=card.status,
                new_status=getattr(data, "status", None),
                archived=bool(getattr(card, "archived", False)),
            )
        )
        if archived_block is not None:
            raise ValueError(archived_block.detail)

        _ensure_card_resource_write_allowed(
            update_data,
            allow=allow_card_resource_write,
        )
        if "knowledge_bases" in update_data:
            await require_legacy_card_knowledge_write_allowed(
                self.db,
                board_id=card.board_id,
                card_id=card.id,
                port=self._knowledge_propagation_port,
            )

        # Validate relationship changes before authorization auditing, entity
        # mutation, or an implicit/explicit flush.  Besides turning raw FK
        # failures into governed errors, this keeps the card's resulting
        # ``spec_id``/``sprint_id`` pair coherent when only one side changes.
        relation_update = "spec_id" in update_data or "sprint_id" in update_data
        next_spec_id = (
            update_data["spec_id"] if "spec_id" in update_data else card.spec_id
        )
        next_sprint_id = (
            update_data["sprint_id"] if "sprint_id" in update_data else card.sprint_id
        )
        next_spec = None
        if relation_update and next_spec_id is not None:
            next_spec = await _application_get(self.db, "spec", next_spec_id)
            if not next_spec or next_spec.board_id != card.board_id:
                raise ValueError("Spec not found on this board")

        if relation_update and next_sprint_id is not None:
            next_sprint = await _application_get(self.db, "sprint", next_sprint_id)
            if not next_sprint or next_sprint.board_id != card.board_id:
                raise ValueError("Sprint not found on this board")
            if next_spec_id is None or next_sprint.spec_id != next_spec_id:
                raise ValueError("Sprint must belong to the card's resulting spec")

        # A bug referenced by Sprint.origin_bug_id is part of that lane's
        # same-board/same-spec lineage.  Reparenting is allowed only when the
        # resulting spec still matches every dependent lane (which also permits
        # repairing a legacy row without direct SQL).  Run before authorization,
        # audit, propagation, flush, or mutation.
        if "spec_id" in update_data and next_spec_id != card.spec_id:
            origin_bug_dependents = await _application_list(
                self.db,
                "sprint",
                filters=(_apf("origin_bug_id", "eq", card.id),),
            )
            broken_dependents = [
                sprint
                for sprint in origin_bug_dependents
                if next_spec_id is None
                or sprint.board_id != card.board_id
                or sprint.spec_id != next_spec_id
            ]
            if broken_dependents:
                raise CardOperationError(
                    "hotfix_origin_bug_reparent_conflict",
                    "Cannot reparent the bug because it is the origin of one or "
                    "more hotfix lanes in a different resulting spec.",
                    remediation="relineage_hotfix_lanes_before_reparenting_origin_bug",
                    facts={
                        "card_id": card.id,
                        "current_spec_id": card.spec_id,
                        "target_spec_id": next_spec_id,
                        "dependent_sprint_ids": sorted(
                            sprint.id for sprint in broken_dependents
                        )[:20],
                        "dependent_sprint_count": len(broken_dependents),
                    },
                )

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=card.board_id,
            actor_id=user_id,
            entity_type="card",
            entity_id=card.id,
            critical_action=CriticalAction.CARD_UPDATE,
            surface="service",
            actor_type=actor_type,
            card_id=card.id,
        )

        # spec 28583299 (Ideação #4, IMPL-C): snapshot priority/severity BEFORE
        # mutation so the DomainEvent payload carries the actual transition.
        # In-memory mutation may leave enums as raw strings (Pydantic dump);
        # _enum_value handles both shapes uniformly.
        def _enum_value(value):
            if value is None:
                return None
            return getattr(value, "value", value)

        old_priority = _enum_value(card.priority)
        old_severity = _enum_value(getattr(card, "severity", None))
        old_spec_id = card.spec_id
        old_update_data = {field: getattr(card, field, None) for field in update_data}

        if "test_scenario_ids" in update_data:
            next_type = update_data.get("card_type", card.card_type)
            if getattr(next_type, "value", next_type) == CardType.TEST.value:
                board = await _application_get(self.db, "board", card.board_id)
                spec_id = next_spec_id if relation_update else card.spec_id
                spec = next_spec
                if spec is None and spec_id:
                    spec = await _application_get(self.db, "spec", spec_id)
                if not spec:
                    raise ValueError(
                        "Test cards require a linked spec before updating test_scenario_ids"
                    )
                scenario_ids = list(update_data.get("test_scenario_ids") or [])
                if not scenario_ids:
                    raise ValueError(
                        "test_scenario_ids is required for test cards and must contain at least one scenario ID"
                    )
                await self._validate_test_card_scenario_ids(
                    board=board,
                    spec=spec,
                    scenario_ids=scenario_ids,
                )

        # Serialize screen_mockups if present
        if (
            "screen_mockups" in update_data
            and update_data["screen_mockups"] is not None
        ):
            update_data["screen_mockups"] = [
                s.model_dump() if hasattr(s, "model_dump") else s
                for s in update_data["screen_mockups"]
            ]
            # MockupDesignSystemGate (spec 3a006f65) — defense in depth pre-persist.
            from okto_pulse.core.services.design_system import (
                gate_entity_screen_mockups,
            )

            await gate_entity_screen_mockups(
                self.db, card, update_data["screen_mockups"], entity_type="card"
            )

        card_json_fields = {
            "labels",
            "test_scenario_ids",
            "conclusions",
            "screen_mockups",
            "knowledge_bases",
        }
        activity_changes = activity_log_changes(
            old_update_data,
            update_data,
            list(update_data.keys()),
        )
        activity_update_data = {
            field: activity_log_value(value) for field, value in update_data.items()
        }
        knowledge_v2_relinked = False
        if "spec_id" in update_data and next_spec_id != old_spec_id:
            knowledge_v2_relinked = await _reset_v2_knowledge_for_relink(
                self.db,
                board_id=card.board_id,
                target_type="card",
                target_id=card.id,
                previous_parent=(
                    None if old_spec_id is None else ("spec", old_spec_id)
                ),
                next_parent=(None if next_spec_id is None else ("spec", next_spec_id)),
                actor_id=user_id,
                port=self._knowledge_propagation_port,
            )
        for key, value in update_data.items():
            setattr(card, key, value)
            if key in card_json_fields:
                card.mark_dirty(key)

        if "spec_id" in update_data and card.spec_id and card.spec_id != old_spec_id:
            await _application_flush(self.db)
            await SpecResourcePropagationService(self.db).propagate_for_card(
                board_id=card.board_id,
                spec_id=card.spec_id,
                card_id=card.id,
                actor_id=user_id,
                trigger="card_linked_via_update",
                excluded_resource_types=(
                    {"knowledge_base"} if knowledge_v2_relinked else None
                ),
            )
            card = await _application_refresh(self.db, card)

        resolved_actor_name = actor_name or await resolve_actor_name(
            self.db,
            user_id,
            card.board_id,
        )
        extra_activity_details = {
            field: activity_log_value(value)
            for field, value in (activity_details or {}).items()
        }
        await self._log_activity(
            board_id=card.board_id,
            card_id=card_id,
            action="card_updated",
            actor_type=actor_type,
            actor_id=user_id,
            actor_name=resolved_actor_name,
            # Preserve the legacy top-level fields consumed by summaries and
            # existing integrations, while exposing the same structured
            # field-level diff contract used by Spec history.
            details={
                **extra_activity_details,
                **activity_update_data,
                "changes": activity_changes,
            },
        )

        # spec 28583299 (Ideação #4, FR6/FR7 + api_21467ada/api_ff834434):
        # emit a typed event when priority or severity changed so the
        # consolidation worker recomputes priority_boost on the KG node.
        new_priority = _enum_value(card.priority)
        new_severity = _enum_value(getattr(card, "severity", None))
        card_type_value = _enum_value(card.card_type)

        if "priority" in update_data and old_priority != new_priority:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import CardPriorityChanged

            await event_publish(
                CardPriorityChanged(
                    board_id=card.board_id,
                    actor_id=user_id,
                    card_id=card.id,
                    old_priority=old_priority,
                    new_priority=new_priority,
                    spec_id=card.spec_id,
                    changed_by=user_id,
                ),
                session=self.db,
            )

        # BR1: severity transitions only matter for Bug cards.
        if (
            card_type_value == "bug"
            and "severity" in update_data
            and old_severity != new_severity
        ):
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import CardSeverityChanged

            await event_publish(
                CardSeverityChanged(
                    board_id=card.board_id,
                    actor_id=user_id,
                    card_id=card.id,
                    old_severity=old_severity,
                    new_severity=new_severity,
                    spec_id=card.spec_id,
                    changed_by=user_id,
                ),
                session=self.db,
            )

        return card

    # ---- Dependency methods ----

    async def add_dependency(
        self, card_id: str, depends_on_id: str
    ) -> ApplicationRecord:
        """Add a dependency with idempotent duplicate handling.

        A repeated edge returns the existing record. Invalid graph shapes use
        distinct, transport-neutral error codes so adapters do not have to
        infer whether a conflict was a self-reference or a cycle.
        """
        if card_id == depends_on_id:
            raise CardOperationError(
                "dependency_self_reference",
                "A card cannot depend on itself.",
                remediation="choose_a_different_dependency",
                facts={"card_id": card_id, "depends_on_id": depends_on_id},
            )
        existing = await _application_list(
            self.db,
            "card_dependency",
            filters=(
                _apf("card_id", "eq", card_id),
                _apf("depends_on_id", "eq", depends_on_id),
            ),
            limit=1,
        )
        if existing:
            return existing[0]
        card = await self.get_card(card_id)
        if card is None:
            raise ValueError("Card not found")
        require_card_operational_mutation_allowed(
            card,
            operation="add_dependency",
        )
        # Check circular
        if await self._would_create_cycle(card_id, depends_on_id):
            raise CardOperationError(
                "dependency_cycle_detected",
                "Adding this dependency would create a cycle.",
                remediation="remove_or_reverse_a_conflicting_dependency",
                facts={"card_id": card_id, "depends_on_id": depends_on_id},
            )
        dep = _new_application_record(
            "card_dependency",
            card_id=card_id,
            depends_on_id=depends_on_id,
        )
        await _application_add(self.db, dep)
        return dep

    async def remove_dependency(self, card_id: str, depends_on_id: str) -> bool:
        rows = await _application_list(
            self.db,
            "card_dependency",
            filters=(
                _apf("card_id", "eq", card_id),
                _apf("depends_on_id", "eq", depends_on_id),
            ),
        )
        if rows:
            card = await self.get_card(card_id)
            if card is None:
                raise ValueError("Card not found")
            require_card_operational_mutation_allowed(
                card,
                operation="remove_dependency",
            )
        for row in rows:
            await _application_delete(self.db, row)
        return bool(rows)

    async def get_dependencies(self, card_id: str) -> list[ApplicationRecord]:
        """Get cards that this card depends on."""
        dependencies = await _application_list(
            self.db,
            "card_dependency",
            filters=(_apf("card_id", "eq", card_id),),
        )
        return (
            await _application_list(
                self.db,
                "card",
                filters=(
                    _apf("id", "in", [item.depends_on_id for item in dependencies]),
                ),
            )
            if dependencies
            else []
        )

    async def get_dependents(self, card_id: str) -> list[ApplicationRecord]:
        """Get cards that depend on this card."""
        dependencies = await _application_list(
            self.db,
            "card_dependency",
            filters=(_apf("depends_on_id", "eq", card_id),),
        )
        return (
            await _application_list(
                self.db,
                "card",
                filters=(_apf("id", "in", [item.card_id for item in dependencies]),),
            )
            if dependencies
            else []
        )

    async def check_dependencies_met(self, card_id: str) -> tuple[bool, list[str]]:
        """Check if all dependencies are met (done or cancelled).
        Returns (all_met, list_of_blocking_card_titles).
        """
        deps = await self.get_dependencies(card_id)
        blocking = [
            d.title
            for d in deps
            if d.status not in (CardStatus.DONE, CardStatus.CANCELLED)
        ]
        return len(blocking) == 0, blocking

    async def _would_create_cycle(self, card_id: str, new_dep_id: str) -> bool:
        """Check if adding card_id -> new_dep_id would create a cycle.
        A cycle exists if new_dep_id (directly or transitively) depends on card_id.
        """
        visited: set[str] = set()
        stack = [new_dep_id]
        while stack:
            current = stack.pop()
            if current == card_id:
                return True
            if current in visited:
                continue
            visited.add(current)
            # Get what 'current' depends on
            rows = await _application_list(
                self.db,
                "card_dependency",
                filters=(_apf("card_id", "eq", current),),
            )
            for row in rows:
                stack.append(row.depends_on_id)
        return False

    # ---- Status progression order ----
    _STATUS_ORDER = CARD_STATUS_ORDER

    # ---- Task Validation Gate ----

    def _resolve_validation_config(
        self,
        card: ApplicationRecord,
        spec: "ApplicationRecord | None",
        sprint: "ApplicationRecord | None",
        board_settings: dict,
    ) -> dict:
        """Resolve validation gate config from hierarchy: sprint → spec → board.

        Returns the effective values plus both the legacy ``resolved_from``
        value (the source of ``required``) and per-field ``resolved_sources``.
        Threshold overrides are independent, so a single provenance label
        cannot accurately describe a mixed sprint/spec/board configuration.
        """
        # Defaults from board settings
        board_required = board_settings.get("require_task_validation", True)
        board_min_conf = board_settings.get("min_confidence", 70)
        board_min_comp = board_settings.get("min_completeness", 80)
        board_max_drift = board_settings.get("max_drift", 50)

        # Spec overrides
        spec_required = getattr(spec, "require_task_validation", None) if spec else None
        spec_min_conf = (
            getattr(spec, "validation_min_confidence", None) if spec else None
        )
        spec_min_comp = (
            getattr(spec, "validation_min_completeness", None) if spec else None
        )
        spec_max_drift = getattr(spec, "validation_max_drift", None) if spec else None

        # Sprint overrides
        spr_required = (
            getattr(sprint, "require_task_validation", None) if sprint else None
        )
        spr_min_conf = (
            getattr(sprint, "validation_min_confidence", None) if sprint else None
        )
        spr_min_comp = (
            getattr(sprint, "validation_min_completeness", None) if sprint else None
        )
        spr_max_drift = (
            getattr(sprint, "validation_max_drift", None) if sprint else None
        )

        # Resolve with null-coalescing: sprint ?? spec ?? board, retaining the
        # source for every independently overridable value.
        def _resolve_with_source(sprint_value, spec_value, board_value, *, default):
            if sprint_value is not None:
                return sprint_value, "sprint"
            if spec_value is not None:
                return spec_value, "spec"
            if board_value is not None:
                return board_value, "board"
            return default, "default"

        required, required_source = _resolve_with_source(
            spr_required,
            spec_required,
            board_required,
            default=False,
        )
        min_confidence, min_confidence_source = _resolve_with_source(
            spr_min_conf,
            spec_min_conf,
            board_min_conf,
            default=70,
        )
        min_completeness, min_completeness_source = _resolve_with_source(
            spr_min_comp,
            spec_min_comp,
            board_min_comp,
            default=80,
        )
        max_drift, max_drift_source = _resolve_with_source(
            spr_max_drift,
            spec_max_drift,
            board_max_drift,
            default=50,
        )

        return {
            "required": bool(required),
            "min_confidence": min_confidence,
            "min_completeness": min_completeness,
            "max_drift": max_drift,
            # Backwards-compatible aggregate: historically this represented
            # the layer that supplied require_task_validation.
            "resolved_from": required_source,
            "resolved_sources": {
                "required": required_source,
                "min_confidence": min_confidence_source,
                "min_completeness": min_completeness_source,
                "max_drift": max_drift_source,
            },
        }

    @staticmethod
    def _card_subject_version(card: ApplicationRecord) -> int:
        value = getattr(card, "policy_version", None)
        if value is None:
            value = getattr(card, "version", 1)
        return int(value or 1)

    @staticmethod
    def _task_validation_request_digest(
        *,
        card: ApplicationRecord,
        reviewer_id: str,
        expected_subject_version: int,
        data: Mapping[str, Any],
    ) -> str:
        from okto_pulse.core.domain.quality_canonicalization import canonical_sha256

        return canonical_sha256(
            {
                "contract": "task-validation-submit/v2",
                "board_id": card.board_id,
                "card_id": card.id,
                "reviewer_id": reviewer_id,
                "expected_subject_version": expected_subject_version,
                "confidence": data.get("confidence"),
                "confidence_justification": data.get("confidence_justification"),
                "estimated_completeness": data.get("estimated_completeness"),
                "completeness_justification": data.get("completeness_justification"),
                "estimated_drift": data.get("estimated_drift"),
                "drift_justification": data.get("drift_justification"),
                "general_justification": data.get("general_justification"),
                "recommendation": data.get("recommendation"),
            }
        )

    @staticmethod
    def _task_validation_replay(
        card: ApplicationRecord,
        *,
        idempotency_key: str,
        request_digest: str,
    ) -> dict[str, Any] | None:
        for validation in reversed(list(getattr(card, "validations", None) or [])):
            if not isinstance(validation, dict):
                continue
            if validation.get("idempotency_key") != idempotency_key:
                continue
            if validation.get("request_digest") != request_digest:
                raise CardOperationError(
                    "task_validation_idempotency_conflict",
                    "The idempotency key was already used with a different request.",
                    remediation="retry_with_a_new_idempotency_key",
                    facts={"card_id": card.id, "idempotency_key": idempotency_key},
                )
            return project_task_validation_public(
                validation,
                card_id=str(card.id),
                board_id=str(card.board_id),
                replayed=True,
            )
        return None

    async def _bug_regression_completion_failure(
        self,
        *,
        card: ApplicationRecord,
        board: ApplicationRecord | None,
    ) -> CompletionGateFailure | None:
        """Re-evaluate the governed Bug regression gate before completion.

        The ordinary move gate runs when a Bug first enters execution.  Task
        Validation is the completion decision point, so it must evaluate the
        same persisted lineage again: a link/scenario/amendment may have become
        invalid while the Bug was being implemented.  This helper is purposely
        read-only.  Infrastructure failures propagate and roll back the submit;
        only known domain blockers are converted into a rejection cause.
        """

        settings = (getattr(board, "settings", None) or {}) if board else {}
        raw_severity = getattr(card, "severity", None)
        severity_value = getattr(raw_severity, "value", raw_severity) or "minor"
        facts = CardTransitionFacts(
            card_id=card.id,
            old_status=CardStatus.NOT_STARTED,
            new_status=CardStatus.IN_PROGRESS,
            card_type=getattr(card, "card_type", CardType.NORMAL),
            spec_id=getattr(card, "spec_id", None),
            require_test_task_for_bug=bool(
                settings.get("require_test_task_for_bug", True)
            ),
            bug_test_gate_min_severity=str(
                settings.get("bug_test_gate_min_severity", "minor")
            ),
            severity=str(severity_value),
        )
        if not bug_regression_gate_applies(facts):
            return None

        direct_test_ids = [
            str(value) for value in (getattr(card, "linked_test_task_ids", None) or [])
        ]
        amendment_rows = (
            await AmendmentRevisionService(self.db).list_for_bug(
                board_id=card.board_id,
                original_spec_id=card.spec_id,
                origin_bug_id=card.id,
            )
            if getattr(card, "spec_id", None)
            else []
        )
        amendment_facts = [AmendmentLineageFact.from_row(row) for row in amendment_rows]
        effective_test_ids = direct_test_ids or _amendment_regression_test_task_ids(
            amendment_rows
        )
        if not effective_test_ids:
            return CompletionGateFailure(
                code="missing_regression_test_task",
                summary=(
                    "Bug completion requires at least one current regression Test "
                    "card linked directly or through an eligible amendment."
                ),
                reason_codes=("missing_regression_test_task",),
            )

        spec = (
            await _application_get(self.db, "spec", card.spec_id)
            if getattr(card, "spec_id", None)
            else None
        )
        if spec is None:
            return CompletionGateFailure(
                code="bug_spec_missing",
                summary="Bug regression eligibility cannot be evaluated without its Spec.",
                reason_codes=("bug_spec_missing",),
            )
        origin_task = (
            await _application_get(self.db, "card", card.origin_task_id)
            if getattr(card, "origin_task_id", None)
            else None
        )
        if origin_task is None:
            return CompletionGateFailure(
                code="origin_task_missing",
                summary=(
                    "Bug regression eligibility cannot be evaluated without a current "
                    "origin Task."
                ),
                reason_codes=("origin_task_missing",),
            )

        linked_test_tasks: list[ApplicationRecord] = []
        candidate_scenario_ids: list[str] = []
        bug_created_at = getattr(card, "created_at", None)
        for test_task_id in effective_test_ids:
            test_task = await _application_get(self.db, "card", test_task_id)
            if test_task is None:
                return CompletionGateFailure(
                    code="linked_test_task_missing",
                    summary="A linked regression Test card no longer exists.",
                    reason_codes=("linked_test_task_missing",),
                )
            test_card_type = getattr(test_task, "card_type", CardType.NORMAL)
            if getattr(test_card_type, "value", test_card_type) != CardType.TEST.value:
                return CompletionGateFailure(
                    code="linked_test_task_type_invalid",
                    summary="A linked regression card is not a Test card.",
                    reason_codes=("linked_test_task_type_invalid",),
                )
            scenario_ids = [
                str(value)
                for value in (getattr(test_task, "test_scenario_ids", None) or [])
            ]
            if not scenario_ids:
                return CompletionGateFailure(
                    code="linked_test_task_scenarios_missing",
                    summary="A linked regression Test card has no Test Scenario.",
                    reason_codes=("linked_test_task_scenarios_missing",),
                )
            test_created_at = getattr(test_task, "created_at", None)
            if (
                bug_created_at is not None
                and test_created_at is not None
                and test_created_at.isoformat() < bug_created_at.isoformat()
            ):
                return CompletionGateFailure(
                    code="regression_test_predates_bug",
                    summary="A linked regression Test card predates this Bug.",
                    reason_codes=("regression_test_predates_bug",),
                )
            linked_test_tasks.append(test_task)
            candidate_scenario_ids.extend(scenario_ids)

        original_scenario_ids = {
            str(scenario["id"])
            for scenario in (getattr(spec, "test_scenarios", None) or [])
            if isinstance(scenario, dict) and scenario.get("id") is not None
        }
        missing_scenario_ids = {
            scenario_id
            for scenario_id in candidate_scenario_ids
            if scenario_id not in original_scenario_ids
        }
        candidate_spec_ids_by_scenario_id: dict[str, str] = {}
        if missing_scenario_ids:
            other_specs = await _application_list(
                self.db,
                "spec",
                filters=(
                    _apf("board_id", "eq", card.board_id),
                    _apf("id", "ne", card.spec_id),
                ),
            )
            for other_spec in other_specs:
                for scenario in getattr(other_spec, "test_scenarios", None) or []:
                    if not isinstance(scenario, dict) or scenario.get("id") is None:
                        continue
                    scenario_id = str(scenario["id"])
                    if scenario_id in missing_scenario_ids:
                        candidate_spec_ids_by_scenario_id.setdefault(
                            scenario_id, other_spec.id
                        )

        gate_result = BugRegressionGateValidator().validate_linked_test_tasks(
            bug_card=card,
            linked_test_tasks=linked_test_tasks,
            spec=spec,
            origin_task=origin_task,
            candidate_spec_ids_by_scenario_id=candidate_spec_ids_by_scenario_id,
            amendment_facts=amendment_facts,
        )
        if gate_result.allowed:
            return None

        eligibility = gate_result.eligibility
        reason_codes = [gate_result.decision.value]
        reason_codes.extend(
            item.reason.value for item in eligibility.rejected_scenarios
        )
        if eligibility.coverage_pending_scenarios:
            reason_codes.append("coverage_pending")
        reason_codes.extend(str(value) for value in eligibility.missing_links)
        rejected_ids = ", ".join(
            item.scenario_id for item in eligibility.rejected_scenarios
        )
        summary = (
            "Bug regression evidence is not completion-ready: "
            f"{gate_result.decision.value}."
        )
        if rejected_ids:
            summary += f" Rejected scenarios: {rejected_ids}."
        return CompletionGateFailure(
            code=gate_result.decision.value,
            summary=summary,
            reason_codes=tuple(reason_codes),
        )

    async def _task_completion_gate_failures(
        self,
        *,
        card: ApplicationRecord,
        board: ApplicationRecord | None,
    ) -> tuple[CompletionGateFailure, ...]:
        """Evaluate known domain gates without turning technical errors into rejection."""

        failures: list[CompletionGateFailure] = []
        bug_regression_failure = await self._bug_regression_completion_failure(
            card=card,
            board=board,
        )
        if bug_regression_failure is not None:
            failures.append(bug_regression_failure)

        # Re-evaluate the board's declared-impact completion posture against the
        # executor report that admitted this card to Validation.  A missing
        # report remains the explicit legacy compatibility path handled below
        # by the historical task-validation conclusion fallback; a present
        # report, however, cannot bypass a subsequently enforced ``require``
        # setting merely because it crossed the lane earlier.
        from okto_pulse.core.services.impact_evidence import (
            resolve_impact_evidence_mode,
        )

        execution_reports = [
            entry
            for entry in (getattr(card, "conclusions", None) or [])
            if isinstance(entry, Mapping)
            and entry.get("source") == "move_to_validation"
        ]
        impact_mode, _impact_mode_source = resolve_impact_evidence_mode(board)
        if execution_reports and impact_mode == "require":
            impact_evidence = execution_reports[-1].get("impact_evidence")
            impact_populated = isinstance(impact_evidence, Mapping) and any(
                bool(impact_evidence.get(section))
                for section in ("files", "symbols", "surfaces", "tests")
            )
            if not impact_populated:
                failures.append(
                    CompletionGateFailure(
                        code="impact_evidence_required",
                        summary=(
                            "The current executor report lacks the impact evidence "
                            "required by this board for task completion."
                        ),
                        reason_codes=("impact_evidence_required",),
                    )
                )
        spec = (
            await _application_get(self.db, "spec", card.spec_id)
            if getattr(card, "spec_id", None)
            else None
        )
        if spec is not None:
            maturity = spec_maturity_block(
                CardTransitionFacts(
                    card_id=card.id,
                    old_status=CardStatus.VALIDATION,
                    new_status=CardStatus.DONE,
                    card_type=getattr(card, "card_type", CardType.NORMAL),
                    spec_id=card.spec_id,
                    spec_title=spec.title,
                    spec_status=spec.status,
                )
            )
            if maturity is not None:
                failures.append(
                    CompletionGateFailure(
                        code=maturity.code,
                        summary=maturity.detail,
                        reason_codes=(maturity.code,),
                    )
                )
            sprints = await _application_list(
                self.db,
                "sprint",
                filters=(
                    _apf("spec_id", "eq", card.spec_id),
                    _apf("archived", "is_false"),
                ),
            )
            if sprints:
                sprint = (
                    await _application_get(self.db, "sprint", card.sprint_id)
                    if getattr(card, "sprint_id", None)
                    else None
                )
                sprint_block = sprint_assignment_block(
                    CardTransitionFacts(
                        card_id=card.id,
                        old_status=CardStatus.VALIDATION,
                        new_status=CardStatus.DONE,
                        card_type=getattr(card, "card_type", CardType.NORMAL),
                        spec_id=card.spec_id,
                        spec_status=spec.status,
                        sprint_count=len(sprints),
                        sprint_id=getattr(card, "sprint_id", None),
                        sprint_exists=sprint is not None if card.sprint_id else True,
                        sprint_status=sprint.status if sprint is not None else None,
                        sprint_title=sprint.title if sprint is not None else None,
                        sprint_is_hotfix=(
                            sprint.lane_type == SprintLaneType.HOTFIX
                            if sprint is not None
                            else False
                        ),
                        hotfix_count=sum(
                            1
                            for item in sprints
                            if item.lane_type == SprintLaneType.HOTFIX
                        ),
                    )
                )
                if sprint_block is not None:
                    failures.append(
                        CompletionGateFailure(
                            code=sprint_block.code,
                            summary=sprint_block.detail,
                            reason_codes=(sprint_block.code,),
                        )
                    )

        dependencies_met, blocking_dependencies = await self.check_dependencies_met(
            card.id
        )
        if not dependencies_met:
            failures.append(
                CompletionGateFailure(
                    code="dependencies_incomplete",
                    summary=(
                        "Card dependencies must be done or cancelled before completion: "
                        + ", ".join(str(item) for item in blocking_dependencies)
                    ),
                    reason_codes=("dependencies_incomplete",),
                )
            )

        traceability = await evaluate_code_traceability_transition(
            self.db,
            board=board,
            subject=card,
            subject_type=CodeTraceabilitySubjectType.CARD,
            from_status=CardStatus.VALIDATION.value,
            to_status=CardStatus.DONE.value,
            enforce=False,
        )
        if traceability is not None and not traceability.allowed:
            blocking_trace = tuple(
                blocker for blocker in traceability.blockers if blocker.blocking
            )
            failures.extend(
                CompletionGateFailure(
                    code=blocker.code,
                    summary=blocker.message,
                    reason_codes=(blocker.code,),
                )
                for blocker in blocking_trace
            )

        try:
            await self._validate_cognitive_done(card, board)
        except GovernedCompletionBlocked as exc:
            failures.append(
                CompletionGateFailure(
                    code=exc.code,
                    summary=exc.summary,
                    reason_codes=tuple(exc.reason_codes),
                )
            )

        from okto_pulse.core.domain.guideline_semantic_transition import (
            PolicyTransitionRejected,
        )

        try:
            await GuidelineService(self.db).enforce_policy_transition(
                board_id=card.board_id,
                entity_type="card",
                subject_id=card.id,
                from_status=CardStatus.VALIDATION.value,
                to_status=CardStatus.DONE.value,
            )
        except PolicyTransitionRejected as exc:
            reason_codes = tuple(
                str(getattr(code, "value", code)) for code in exc.reason_codes
            )
            failures.append(
                CompletionGateFailure(
                    code=reason_codes[0]
                    if reason_codes
                    else "policy_compliance_blocked",
                    summary="Policy compliance blocked task completion.",
                    reason_codes=reason_codes,
                )
            )

        from okto_pulse.core.services.resource_gate_contracts import (
            ResourceGateViolation,
        )

        try:
            await ResourceGateService(self.db).validate_or_raise_entity_completion(
                card.board_id,
                "card",
                card.id,
                phase="task_validation_success",
            )
        except ResourceGateViolation as exc:
            failures.append(
                CompletionGateFailure(
                    code=exc.code,
                    summary=str(exc),
                    reason_codes=(exc.code,),
                )
            )
        return tuple(failures)

    async def submit_task_validation(
        self,
        card_id: str,
        reviewer_id: str,
        reviewer_name: str,
        data: dict,
    ) -> dict:
        """Submit a task validation for a card in 'validation' status.

        Executes a fenced, idempotent completion decision.  An admitted
        domain rejection is persisted with its cause and routes Normal/Bug
        cards to ``rejected``; technical failures leave both status and
        validation history untouched.
        """
        import uuid as _uuid

        card = await self.get_card(card_id)
        if not card:
            raise ValueError("Card not found")

        current_subject_version = self._card_subject_version(card)
        expected_subject_version = int(
            data.get("expected_subject_version", current_subject_version)
        )
        idempotency_key = str(
            data.get("idempotency_key") or f"legacy:{reviewer_id}:{_uuid.uuid4().hex}"
        ).strip()
        request_digest = self._task_validation_request_digest(
            card=card,
            reviewer_id=reviewer_id,
            expected_subject_version=expected_subject_version,
            data=data,
        )
        replay = self._task_validation_replay(
            card,
            idempotency_key=idempotency_key,
            request_digest=request_digest,
        )
        if replay is not None:
            return replay

        if card.status != CardStatus.VALIDATION:
            raise ValueError(
                f"Card is not in 'validation' status (currently '{card.status.value}'). "
                f"Only cards in 'validation' status can receive validations."
            )
        old_status = card.status

        if expected_subject_version != current_subject_version:
            raise CardOperationError(
                "task_validation_subject_version_conflict",
                "The card changed after the validation request was prepared.",
                remediation="reload_card_and_retry_validation",
                facts={
                    "card_id": card.id,
                    "expected_subject_version": expected_subject_version,
                    "actual_subject_version": current_subject_version,
                },
            )

        if getattr(card, "card_type", CardType.NORMAL) == CardType.TEST:
            # R4-IMP1: normalized contract pointing at the test-card operational
            # path (scenario status update + move_card done). Same rejection.
            from okto_pulse.core.services.gate_contracts import (
                task_validation_unsupported_for_test_card_error,
            )

            raise task_validation_unsupported_for_test_card_error(
                card_id=card.id,
                board_id=card.board_id,
                spec_id=card.spec_id,
            )

        # Resolve thresholds from hierarchy
        board = await _application_get(self.db, "board", card.board_id)
        board_settings = board.settings or {} if board else {}
        spec = (
            await _application_get(self.db, "spec", card.spec_id)
            if card.spec_id
            else None
        )
        sprint = (
            await _application_get(self.db, "sprint", card.sprint_id)
            if card.sprint_id
            else None
        )
        config = self._resolve_validation_config(card, spec, sprint, board_settings)

        # Reviewer independence is board policy, shared with sprint evaluation.
        # The decision happens before authorization, closeout checks, activity
        # writes, or mutation so ``enforce`` is fail-closed and atomic.  Legacy
        # boards with no persisted setting resolve to explicit compatibility
        # mode ``off`` and still record their conflicts/source on the accepted
        # validation below.
        reviewer_separation = evaluate_task_reviewer_separation(
            board=board,
            reviewer_id=reviewer_id,
            card=card,
        )
        if not reviewer_separation.allowed:
            raise CardOperationError(
                "reviewer_separation_required",
                "Task validator must be independent from the task creator, "
                "assignee, and executor.",
                remediation="request_independent_task_validator",
                facts={
                    "board_id": card.board_id,
                    "card_id": card.id,
                    "current_status": card.status.value,
                    "reviewer_separation": reviewer_separation.to_dict(),
                },
            )

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=card.board_id,
            actor_id=reviewer_id,
            entity_type="card",
            entity_id=card.id,
            critical_action=CriticalAction.CARD_SUBMIT_VALIDATION,
            surface="service",
            actor_type="agent",
            actor_name=reviewer_name,
            card_id=card.id,
        )

        # Extract scores
        confidence = data["confidence"]
        completeness = data["estimated_completeness"]
        drift = data["estimated_drift"]
        recommendation = data["recommendation"]

        # Threshold check
        violations = []
        if confidence < config["min_confidence"]:
            violations.append(
                f"confidence {confidence} < min {config['min_confidence']}"
            )
        if completeness < config["min_completeness"]:
            violations.append(
                f"completeness {completeness} < min {config['min_completeness']}"
            )
        if drift > config["max_drift"]:
            violations.append(f"drift {drift} > max {config['max_drift']}")

        # Compute outcome
        if violations or recommendation == "reject":
            outcome = "failed"
        else:
            outcome = "success"

        gate_failures: tuple[CompletionGateFailure, ...] = ()
        if outcome == TaskValidationOutcome.SUCCESS.value:
            gate_failures = await self._task_completion_gate_failures(
                card=card,
                board=board,
            )
        decision = decide_card_completion(
            validation_outcome=outcome,
            gate_failures=gate_failures,
        )

        # Build validation entry.
        # Dual naming: we persist BOTH the legacy names (estimated_*, outcome, reviewer_id,
        # general_justification) and the clean frontend-compatible names (completeness, drift,
        # verdict, evaluator_id, summary). This keeps backward compat for any downstream code
        # that reads the legacy names while allowing the IDE ValidationsTab (which reads the
        # clean names) to render correctly. Going forward, consumers should prefer the clean
        # names; the legacy aliases can be removed in a future cleanup.
        validation_id = f"val_{_uuid.uuid4().hex[:8]}"
        _general = data["general_justification"].strip()
        reviewer_display_name = str(reviewer_name or reviewer_id).strip()[:255]
        validation = {
            "id": validation_id,
            "card_id": card_id,
            "board_id": card.board_id,
            # Reviewer — legacy name + clean alias for frontend
            "reviewer_id": reviewer_id,
            "reviewer_name": reviewer_display_name,
            "evaluator_id": reviewer_id,
            "evaluator_name": reviewer_display_name,
            # Confidence
            "confidence": confidence,
            "confidence_justification": data["confidence_justification"].strip(),
            # Completeness — legacy estimated_* + clean name
            "estimated_completeness": completeness,
            "completeness": completeness,
            "completeness_justification": data["completeness_justification"].strip(),
            # Drift — legacy estimated_* + clean name
            "estimated_drift": drift,
            "drift": drift,
            "drift_justification": data["drift_justification"].strip(),
            # General justification — legacy + frontend "summary" alias
            "general_justification": _general,
            "summary": _general,
            # Recommendation + outcome — legacy "outcome" + frontend "verdict" alias
            "recommendation": recommendation,
            "outcome": outcome,
            "verdict": "pass" if outcome == "success" else "fail",
            "threshold_violations": violations,
            # Persist the effective threshold snapshot with the append-only
            # record. Historical UI must not reinterpret an old validation
            # against board/spec/sprint settings changed later.
            "resolved_thresholds": dict(config),
            "reviewer_separation": reviewer_separation.to_dict(),
            "expected_subject_version": expected_subject_version,
            "idempotency_key": idempotency_key,
            "request_digest": request_digest,
            "validation_outcome": decision.validation_outcome.value,
            "completion_outcome": decision.completion_outcome.value,
            "completion_gate_failures": [
                {
                    "code": failure.code,
                    "summary": failure.summary,
                    "reason_codes": list(failure.reason_codes),
                }
                for failure in decision.gate_failures
            ],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # The card row is the serialization point for both the append-only
        # validation ledger and the lifecycle consequence.  The replay lookup
        # above deliberately precedes this mutable-state fence.
        if not await _application_fence(
            self.db,
            "card",
            card.id,
            expected_values={
                "board_id": card.board_id,
                "status": CardStatus.VALIDATION,
                "policy_version": expected_subject_version,
            },
        ):
            refreshed = await self.get_card(card_id)
            if refreshed is not None:
                replay = self._task_validation_replay(
                    refreshed,
                    idempotency_key=idempotency_key,
                    request_digest=request_digest,
                )
                if replay is not None:
                    return replay
            raise CardOperationError(
                "task_validation_subject_version_conflict",
                "The card changed while the validation was being submitted.",
                remediation="reload_card_and_retry_validation",
                facts={
                    "card_id": card.id,
                    "expected_subject_version": expected_subject_version,
                },
            )

        # Persist validation (append-only)
        validations = list(card.validations or [])
        validations.append(validation)
        card.validations = validations
        card.mark_dirty("validations")

        # Auto-populate conclusion only for legacy cards that reached validation
        # before execution reports were required on the validation handoff.
        conclusions_list = list(card.conclusions or [])
        has_executor_report = any(
            isinstance(entry, dict) and entry.get("source") == "move_to_validation"
            for entry in conclusions_list
        )
        if outcome == "success" and not has_executor_report:
            conclusions_list.append(
                {
                    "text": _general,
                    "author_id": reviewer_id,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "completeness": completeness,
                    "completeness_justification": data[
                        "completeness_justification"
                    ].strip(),
                    "drift": drift,
                    "drift_justification": data["drift_justification"].strip(),
                    "source": "task_validation",
                    "validation_id": validation_id,
                }
            )
            card.conclusions = conclusions_list
            card.mark_dirty("conclusions")

            # Spec 4007e4a3 (Ideação #3): re-enqueue parent spec via
            # CardConclusionAdded so the KG reflects the card's narrative
            # outcome alongside its final state. Orphan cards (spec_id=None)
            # are handled gracefully by the enqueuer.
            if _general:
                from okto_pulse.core.events import publish as event_publish
                from okto_pulse.core.events.types import CardConclusionAdded

                await event_publish(
                    CardConclusionAdded(
                        board_id=card.board_id,
                        actor_id=reviewer_id,
                        card_id=card_id,
                        spec_id=card.spec_id,
                        conclusion_excerpt=_general[:280],
                        added_by=reviewer_id,
                    ),
                    session=self.db,
                )

        target_status = (
            CardStatus.DONE
            if decision.completion_outcome is CardCompletionOutcome.COMPLETED
            else CardStatus.REJECTED
        )
        rejection_cause: CardRejectionCause | None = None
        rejection_record: CardRejectionRecord | None = None
        if target_status is CardStatus.REJECTED:
            card_type = getattr(card, "card_type", CardType.NORMAL)
            card_type_value = getattr(card_type, "value", str(card_type))
            if not is_internal_transition_allowed(
                "card",
                old_status.value,
                target_status.value,
                card_type=card_type_value,
            ):
                raise RuntimeError("task_rejection_internal_edge_not_admitted")
            validation_failed = (
                decision.validation_outcome is TaskValidationOutcome.FAILED
            )
            first_failure = (
                decision.gate_failures[0] if decision.gate_failures else None
            )
            rejection_record = CardRejectionRecord(
                id=f"rej_{_uuid.uuid4().hex[:12]}",
                card_id=card.id,
                board_id=card.board_id,
                kind=(
                    CardRejectionKind.TASK_VALIDATION
                    if validation_failed
                    else CardRejectionKind.COMPLETION_GATE
                ),
                source_id=validation_id,
                code=(
                    "task_validation_failed"
                    if validation_failed
                    else first_failure.code
                ),
                summary=(
                    _general
                    or "Task validation failed its recommendation or score thresholds."
                    if validation_failed
                    else first_failure.summary
                ),
                reason_codes=(
                    tuple(
                        code
                        for code, applies in (
                            ("confidence_below", confidence < config["min_confidence"]),
                            (
                                "completeness_below",
                                completeness < config["min_completeness"],
                            ),
                            ("drift_above", drift > config["max_drift"]),
                            ("reject_recommendation", recommendation == "reject"),
                        )
                        if applies
                    )
                    if validation_failed
                    else tuple(
                        code
                        for failure in decision.gate_failures
                        for code in failure.reason_codes
                    )
                ),
                created_by=reviewer_id,
                created_at=datetime.now(timezone.utc).isoformat(),
                subject_version=expected_subject_version,
            )
            records = list(getattr(card, "rejection_records", None) or [])
            records.append(rejection_record.as_dict())
            card.rejection_records = records
            card.mark_dirty("rejection_records")
            rejection_cause = CardRejectionCause(
                kind=rejection_record.kind,
                id=rejection_record.id,
                code=rejection_record.code,
                summary=rejection_record.summary,
            )
            card.current_rejection_kind = rejection_cause.kind.value
            card.current_rejection_id = rejection_cause.id
            card.current_rejection_code = rejection_cause.code
            card.current_rejection_summary = rejection_cause.summary
            for field_name in (
                "current_rejection_kind",
                "current_rejection_id",
                "current_rejection_code",
                "current_rejection_summary",
            ):
                card.mark_dirty(field_name)
        else:
            for field_name in (
                "current_rejection_kind",
                "current_rejection_id",
                "current_rejection_code",
                "current_rejection_summary",
            ):
                setattr(card, field_name, None)
                card.mark_dirty(field_name)

        response = project_task_validation_public(
            {
                **validation,
                "card_status": target_status.value,
                "resolved_thresholds": config,
                "validation_outcome": decision.validation_outcome.value,
                "completion_outcome": decision.completion_outcome.value,
                "completion_gate_failures": validation["completion_gate_failures"],
                "rejection_cause": (
                    rejection_cause.as_dict() if rejection_cause is not None else None
                ),
                "subject_version": expected_subject_version + 1,
                "replayed": False,
            }
        )
        # Persist the exact business response as part of the same append-only
        # ledger value before the resequencer's single flush.  Adding it after
        # that flush would leave only an in-memory nested JSON mutation and
        # break replay after Done/Rejected in a new transaction.
        validation["response"] = dict(response)
        card.validations = validations
        card.mark_dirty("validations")

        # The Core resequencer owns both status mutation and dense target/source
        # positioning.  Keeping this in the same UoW prevents a visible card
        # from occupying two lifecycle lanes after a rejection.
        await self.resequence_columns(
            card.board_id,
            [
                ColumnResequenceOp(
                    card_id=card.id,
                    from_status=old_status,
                    to_status=target_status,
                    placement="end",
                )
            ],
            extra_columns=(old_status, target_status),
            records={card.id: card},
        )

        if old_status != card.status:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import (
                CardCompletionRejected,
                CardMoved,
            )

            await event_publish(
                CardMoved(
                    board_id=card.board_id,
                    actor_id=reviewer_id,
                    card_id=card.id,
                    from_status=old_status.value,
                    to_status=card.status.value,
                    spec_id=card.spec_id,
                    moved_by=reviewer_id,
                ),
                session=self.db,
            )
            if rejection_cause is not None:
                await event_publish(
                    CardCompletionRejected(
                        board_id=card.board_id,
                        actor_id=reviewer_id,
                        card_id=card.id,
                        spec_id=card.spec_id,
                        cause_kind=rejection_cause.kind.value,
                        cause_id=rejection_cause.id,
                        cause_code=rejection_cause.code,
                        cause_summary=rejection_cause.summary,
                        reason_codes=(
                            tuple(rejection_record.reason_codes)
                            if rejection_record is not None
                            else ("task_validation_failed",)
                        ),
                        rejected_by=reviewer_id,
                    ),
                    session=self.db,
                )

        # Activity log
        await self._log_activity(
            board_id=card.board_id,
            card_id=card_id,
            action="validation_submitted",
            actor_type="agent",
            actor_id=reviewer_id,
            actor_name=reviewer_name,
            details={
                "validation_id": validation_id,
                "outcome": outcome,
                "validation_outcome": decision.validation_outcome.value,
                "completion_outcome": decision.completion_outcome.value,
                "rejection_cause": response["rejection_cause"],
                "recommendation": recommendation,
                "confidence": confidence,
                "estimated_completeness": completeness,
                "estimated_drift": drift,
                "threshold_violations": violations,
                "reviewer_separation": reviewer_separation.to_dict(),
                "card_title": card.title,
            },
        )

        return dict(response)

    async def list_task_validations(self, card_id: str) -> list[dict]:
        """List all validations for a card in reverse chronological order."""
        card = await self.get_card(card_id)
        if not card:
            raise ValueError("Card not found")
        validations = [
            project_task_validation_public(
                validation,
                card_id=str(card.id),
                board_id=str(card.board_id),
            )
            for validation in list(card.validations or [])
        ]
        validations.reverse()
        return validations

    async def get_task_validation(
        self, card_id: str, validation_id: str
    ) -> dict | None:
        """Get a single validation by ID."""
        card = await self.get_card(card_id)
        if not card:
            raise ValueError("Card not found")
        for v in card.validations or []:
            if isinstance(v, Mapping) and v.get("id") == validation_id:
                return project_task_validation_public(
                    v,
                    card_id=str(card.id),
                    board_id=str(card.board_id),
                )
        return None

    async def delete_task_validation(
        self, card_id: str, validation_id: str, user_id: str
    ) -> bool:
        """Reject deletion of admitted validations; causal evidence is append-only."""
        card = await self.get_card(card_id)
        if not card:
            raise ValueError("Card not found")
        validations = list(card.validations or [])
        target = next(
            (
                validation
                for validation in validations
                if validation.get("id") == validation_id
            ),
            None,
        )
        if target is None:
            return False

        raise CardOperationError(
            "task_validation_history_append_only",
            "Admitted task validations are immutable causal history and cannot be deleted.",
            remediation="submit_a_new_validation_attempt_after_rework",
            facts={
                "card_id": card.id,
                "validation_id": validation_id,
                "current_rejection_id": getattr(card, "current_rejection_id", None),
            },
        )

    async def confirm_amendment_coverage(
        self,
        *,
        expected_board_id: str | None = None,
        amendment_id: str,
        regression_test_task_id: str,
        regression_scenario_id: str,
        reviewer_id: str,
        reviewer_name: str,
    ) -> dict:
        """Validator-only writer of the Path B coverage attestation (G2 / c9cf9781).

        Enforces, fail-closed, BEFORE persisting:
        * artifact binding — the test task + scenario MUST be declared by THIS
          amendment (regression_test_task_ids / regression_scenario_ids);
        * real validator identity — the same critical-context authorization the
          task-validation gate uses (not a free-text validator_id);
        * reexecutable evidence (NECESSARY, not sufficient) — the regression test
          task is DONE and its declared scenario is passed/automated with SPEC3
          reexecutable evidence (test_file_path+test_function or test_run_id).
        Persists the bound attestation via the single reserved-key writer. The bug
        gate later DERIVES coverage_confirmed from this record (never a passed
        bool), so a generic/forged metadata write cannot grant coverage."""
        from okto_pulse.core.services.amendment_revision import AmendmentRevisionService

        svc = AmendmentRevisionService(self.db)
        amendment = await svc.get(amendment_id)
        if amendment is None:
            raise ValueError(f"Amendment '{amendment_id}' not found")
        if expected_board_id is not None and amendment.board_id != expected_board_id:
            # Keep cross-board identifiers indistinguishable from missing ones and,
            # critically, fail before validator checks or any attestation write.
            raise ValueError(f"Amendment '{amendment_id}' not found")

        # 1. binding: the artifact MUST be declared by THIS amendment.
        if regression_test_task_id not in (amendment.regression_test_task_ids or []):
            raise CardOperationError(
                "coverage_binding_invalid",
                f"Regression test task '{regression_test_task_id}' is not declared by "
                f"amendment '{amendment_id}'. Coverage can only be confirmed for an "
                "artifact bound to this amendment.",
                remediation="bind_regression_artifact_to_amendment",
                facts={"amendment_id": amendment_id},
            )
        if regression_scenario_id not in (amendment.regression_scenario_ids or []):
            raise CardOperationError(
                "coverage_binding_invalid",
                f"Regression scenario '{regression_scenario_id}' is not declared by "
                f"amendment '{amendment_id}'.",
                remediation="bind_regression_artifact_to_amendment",
                facts={"amendment_id": amendment_id},
            )

        test_task = await _application_get(self.db, "card", regression_test_task_id)
        if not test_task or test_task.board_id != amendment.board_id:
            raise ValueError(
                f"Regression test task '{regression_test_task_id}' not found on this board"
            )

        # 2. real validator identity — same critical-context gate as task validation.
        await _authorize_critical_context_or_raise(
            self.db,
            board_id=amendment.board_id,
            actor_id=reviewer_id,
            entity_type="card",
            entity_id=test_task.id,
            critical_action=CriticalAction.CARD_SUBMIT_VALIDATION,
            surface="service",
            actor_type="agent",
            actor_name=reviewer_name,
            card_id=test_task.id,
        )

        # 3. reexecutable evidence is NECESSARY (not sufficient): test task done +
        #    declared scenario passed/automated with SPEC3 reexecutable evidence.
        if test_task.status != CardStatus.DONE:
            raise CardOperationError(
                "coverage_precondition_unmet",
                f"Regression test task '{regression_test_task_id}' is not done "
                f"(status='{getattr(test_task.status, 'value', test_task.status)}').",
                remediation="complete_regression_test_task",
                facts={"amendment_id": amendment_id},
            )
        evidence_ref, scenario_spec_id = await self._reexecutable_evidence_ref(
            test_task, regression_scenario_id
        )
        if not evidence_ref:
            raise CardOperationError(
                "coverage_precondition_unmet",
                f"Scenario '{regression_scenario_id}' has no reexecutable evidence "
                "(needs status passed/automated with authenticated replayable evidence, "
                "a test_file_path+test_function pointer, or test_run_id). Lineage + a "
                "generic status are NOT sufficient (G2).",
                remediation="attach_reexecutable_evidence",
                facts={"amendment_id": amendment_id},
            )

        # 4. BUG-01 (FR1/FR4): gate-consumability preflight. Binding, validator
        #    authorization and reexecutable evidence are NECESSARY but NOT
        #    sufficient — a syntactically valid tuple can still be inert for the
        #    bug regression gate (e.g. a same-spec unrelated scenario). Fail closed
        #    BEFORE set_coverage_confirmation so success implies the gate will
        #    consume the attestation. Runs AFTER the binding/precondition checks
        #    above to preserve their error order.
        await self._assert_coverage_gate_consumable(
            amendment=amendment,
            regression_test_task_id=regression_test_task_id,
            regression_scenario_id=regression_scenario_id,
            scenario_spec_id=scenario_spec_id,
            evidence_ref=evidence_ref,
            reviewer_id=reviewer_id,
        )

        confirmation = {
            "validator_id": reviewer_id,
            "amendment_revision_id": amendment.id,
            "regression_test_task_id": regression_test_task_id,
            "regression_scenario_id": regression_scenario_id,
            "evidence_ref": evidence_ref,
            "confirmed_at": datetime.now(timezone.utc).isoformat(),
        }
        await svc.set_coverage_confirmation(
            amendment_id, confirmation=confirmation, actor=reviewer_id
        )
        return confirmation

    async def _reexecutable_evidence_ref(
        self, test_task: ApplicationRecord, scenario_id: str
    ) -> tuple[str, str | None]:
        """Reexecutable evidence ref + the spec id the scenario lives on.

        Returns ``(evidence_ref, scenario_spec_id)``. ``evidence_ref`` is the
        SPEC3 ref when the scenario is passed/automated with reexecutable
        evidence, else ``''``. ``scenario_spec_id`` is the spec that declares the
        scenario (``None`` when it is not found on any board spec) — the BUG-01
        consumability preflight needs it to route same-spec (Path A) vs cross-spec
        (Path B). Searches the test task's spec first, then the board's specs (a
        Path B regression scenario may be cross-spec)."""
        spec_ids: list[str] = []
        if test_task.spec_id:
            spec_ids.append(str(test_task.spec_id))
        board_specs = await _application_list(
            self.db,
            "spec",
            filters=(_apf("board_id", "eq", test_task.board_id),),
        )
        spec_ids.extend(str(spec.id) for spec in board_specs)
        seen: set[str] = set()
        for spec_id in spec_ids:
            if spec_id in seen:
                continue
            seen.add(spec_id)
            spec = await _application_get(self.db, "spec", spec_id)
            if not spec:
                continue
            for sc in spec.test_scenarios or []:
                if not isinstance(sc, dict) or str(sc.get("id")) != scenario_id:
                    continue
                if str(sc.get("status") or "").lower() not in ("passed", "automated"):
                    return "", spec_id
                if not scenario_has_authenticated_required_evidence(
                    board_id=str(test_task.board_id),
                    spec_id=spec_id,
                    scenario=sc,
                    acceptance_criteria=list(spec.acceptance_criteria or []),
                ):
                    return "", spec_id
                return reexecutable_evidence_reference(sc), spec_id
        return "", None

    async def _assert_coverage_gate_consumable(
        self,
        *,
        amendment: AmendmentHotfixRevision,
        regression_test_task_id: str,
        regression_scenario_id: str,
        scenario_spec_id: str | None,
        evidence_ref: str,
        reviewer_id: str,
    ) -> None:
        """BUG-01 (FR1/FR2/FR4): fail closed BEFORE persisting when the candidate
        coverage confirmation would be INERT — i.e. the bug regression gate would
        never select this ``(amendment, scenario)`` tuple.

        Reuses the shared, routing-correct consumability predicate (same-spec is
        routed through Path A, so a same-spec ``unrelated_scenario`` is rejected
        here even when the amendment task claim intersects the bug; cross-spec is
        routed through Path B and accepted only when the candidate attestation
        drives ``path_b_ready``). It mirrors the ENFORCED gate inputs exactly (the
        bug done-gate passes ``origin_task`` only; ``affected_tasks`` is not part of
        the authoritative set), never relaxes ``unrelated_scenario`` and never
        mutates the DB."""
        bug_card = (
            await _application_get(self.db, "card", amendment.origin_bug_id)
            if amendment.origin_bug_id
            else None
        )
        original_spec = (
            await _application_get(self.db, "spec", bug_card.spec_id)
            if bug_card and bug_card.spec_id
            else None
        )
        if bug_card is None or original_spec is None or scenario_spec_id is None:
            raise CardOperationError(
                "coverage_not_gate_consumable",
                "Coverage confirmation cannot be persisted: the bug regression "
                "gate would not consume this attestation (bug, original spec or "
                "scenario spec could not be resolved).",
                remediation="create_or_link_authoritative_regression_scenario",
                facts={
                    "amendment_id": amendment.id,
                    "regression_test_task_id": regression_test_task_id,
                    "regression_scenario_id": regression_scenario_id,
                },
            )

        origin_task = (
            await _application_get(self.db, "card", bug_card.origin_task_id)
            if bug_card.origin_task_id
            else None
        )
        candidate = CoverageConfirmationFact(
            validator_id=reviewer_id,
            amendment_revision_id=amendment.id,
            regression_test_task_id=regression_test_task_id,
            regression_scenario_id=regression_scenario_id,
            evidence_ref=evidence_ref,
        )
        verdict = evaluate_coverage_confirmation_consumability(
            bug_card=bug_card,
            original_spec=original_spec,
            origin_task=origin_task,
            affected_tasks=None,
            amendment_fact=AmendmentLineageFact.from_row(amendment),
            candidate_confirmation=candidate,
            scenario_id=regression_scenario_id,
            scenario_spec_id=scenario_spec_id,
        )
        if verdict.consumable:
            return

        remediation = (
            "create_or_link_authoritative_regression_scenario"
            if verdict.routed_path == "path_a"
            else "use_consumable_path_b_amendment"
        )
        raise CardOperationError(
            "coverage_not_gate_consumable",
            "Coverage confirmation was rejected before persistence: the bug "
            "regression gate would not consume this attestation for the given "
            f"(amendment, scenario) tuple (routed_path={verdict.routed_path}, "
            f"coverage_state={verdict.coverage_state.value}). Binding and "
            "reexecutable evidence are necessary but not sufficient; the scenario "
            "must be gate-consumable.",
            remediation=remediation,
            facts={
                "amendment_id": amendment.id,
                "bug_id": bug_card.id,
                "original_spec_id": original_spec.id,
                "regression_test_task_id": regression_test_task_id,
                "regression_scenario_id": regression_scenario_id,
                "scenario_spec_id": scenario_spec_id,
                "routed_path": verdict.routed_path,
                "resolver_reason": (
                    verdict.reject_reason.value if verdict.reject_reason else None
                ),
                "coverage_state": verdict.coverage_state.value,
                "missing_links": list(verdict.missing_links),
            },
        )

    # ---- Coverage gate functions (used by SpecService.move_spec) ----

    async def check_code_evidence_coverage(
        self, spec: "Spec", board: "Board | None"
    ) -> CodeTraceabilityGateEvaluation:
        """Require complete inherited Code Evidence disposition coverage.

        The board's Agent-mediated Code Traceability mode remains responsible
        for its broader advisory/blocking policy.  Matrix coverage is a
        deterministic Spec validation prerequisite and is bypassed only by the
        effective board-wide/per-Spec Code Evidence coverage skip.
        """

        return await evaluate_code_evidence_coverage_gate(
            self.db,
            board=board,
            spec=spec,
            enforce=True,
        )

    async def check_ac_scenario_coverage(
        self, spec: "Spec", board: "Board | None"
    ) -> None:
        """Check that every acceptance criterion is covered by at least one test scenario.

        Mirrors the AC→Scenario gate enforced at move_spec→done, but runs at
        submit_spec_validation time so the failure surfaces BEFORE the spec is
        locked. Without this pre-check, validation could succeed (locking the
        spec) and then move→done would fail because uncovered ACs cannot be
        addressed without first unlocking and resubmitting validation.
        """
        skip_global = (
            (board.settings or {}).get("skip_test_coverage_global", False)
            if board
            else False
        )
        if spec.skip_test_coverage or skip_global:
            return
        criteria = list(spec.acceptance_criteria or [])
        scenarios = list(spec.test_scenarios or [])
        if not criteria:
            return
        covered_indices: set[int] = set()
        for scenario in scenarios:
            covered_indices |= resolve_linked_criteria_to_indices(
                scenario.get("linked_criteria"),
                criteria,
            )
        uncovered = [
            f"[{i}] {_structured_ref_text(criterion)[:80]}..."
            for i, criterion in enumerate(criteria)
            if i not in covered_indices
        ]
        if uncovered:
            raise ValueError(
                f"Cannot validate spec: {len(uncovered)} acceptance criteria lack test scenarios. "
                f"Uncovered: {'; '.join(uncovered[:5])}"
                f"{f' (and {len(uncovered) - 5} more)' if len(uncovered) > 5 else ''}. "
                f"Create test scenarios linked to each AC BEFORE submitting validation — "
                f"once validation passes the spec is locked and scenarios cannot be added. "
                f"Alternatively, enable 'skip test coverage' on the spec or board."
            )

    async def check_test_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every test scenario has at least one linked card of type TEST."""
        skip_global = (
            (board.settings or {}).get("skip_test_coverage_global", False)
            if board
            else False
        )
        if spec.skip_test_coverage or skip_global:
            return
        scenarios = list(spec.test_scenarios or [])
        if not scenarios:
            return
        # Collect all card IDs from linked_task_ids across scenarios
        all_card_ids: set[str] = set()
        for s in scenarios:
            for cid in s.get("linked_task_ids") or []:
                all_card_ids.add(cid)
        # Batch query to get effective test cards for all historical links.
        # Cancelled/archived rows remain referenced for audit, but cannot satisfy
        # execution readiness.
        test_card_ids: set[str] = set()
        if all_card_ids:
            linked_cards = await _application_list(
                self.db,
                "card",
                filters=(_apf("id", "in", all_card_ids),),
            )
            for linked_card in linked_cards:
                card_type = getattr(
                    linked_card.card_type,
                    "value",
                    linked_card.card_type,
                )
                card_status = getattr(
                    linked_card.status,
                    "value",
                    linked_card.status,
                )
                if (
                    card_type == CardType.TEST.value
                    and card_status != CardStatus.CANCELLED.value
                    and not getattr(linked_card, "archived", False)
                ):
                    test_card_ids.add(linked_card.id)
        # Check each scenario has at least one TEST card
        unlinked = []
        for s in scenarios:
            task_ids = s.get("linked_task_ids") or []
            has_test = any(tid in test_card_ids for tid in task_ids)
            if not has_test:
                unlinked.append(s)
        if unlinked:
            titles = ", ".join(f'"{s["title"]}"' for s in unlinked[:3])
            suffix = f" and {len(unlinked) - 3} more" if len(unlinked) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked)} test scenario(s) "
                f"in spec '{spec.title}' have no linked test cards "
                f"({titles}{suffix}). "
                f"REQUIRED ACTION: Create test cards (card_type='test') with test_scenario_ids "
                f"for each uncovered scenario. Only non-cancelled, non-archived cards "
                f"of type 'test' count for coverage. "
                f"Alternatively, enable 'skip test coverage' on the spec or board."
            )

    async def check_rules_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every FR has a BR and every BR has a linked task."""
        skip_global = (
            (board.settings or {}).get("skip_rules_coverage_global", False)
            if board
            else False
        )
        if getattr(spec, "skip_rules_coverage", False) or skip_global:
            return
        frs = list(spec.functional_requirements or [])
        brs = list(spec.business_rules or [])
        if not frs:
            return
        # Check FR → BR coverage. Structured-FR aware: resolve linked_requirements
        # (0-based index, exact/substring FR text, or fr_ id) to FR indices via the
        # shared resolver, so the gate works whether FRs are structured dicts
        # {id,text,status} or legacy strings (the old inline loop did `ref in fr`
        # where `fr` could be a dict -> TypeError / missed coverage).
        covered_fr_indices: set[int] = set()
        for br in brs:
            if isinstance(br, dict):
                covered_fr_indices |= resolve_linked_fr_indices(
                    br.get("linked_requirements") or [], frs
                )
        uncovered = [
            (i, _structured_ref_text(fr))
            for i, fr in enumerate(frs)
            if i not in covered_fr_indices
        ]
        if uncovered:
            previews = ", ".join(
                f'"[{i}] {text[:40]}..."' if len(text) > 40 else f'"[{i}] {text}"'
                for i, text in uncovered[:3]
            )
            suffix = f" and {len(uncovered) - 3} more" if len(uncovered) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(uncovered)} functional requirement(s) "
                f"in spec '{spec.title}' have no linked business rules "
                f"({previews}{suffix}). "
                f"REQUIRED ACTION: Create business rules with linked_requirements "
                f"for each uncovered FR. "
                f"Alternatively, enable 'skip rules coverage' on the spec or board."
            )
        # Check BR → Task coverage
        unlinked_rules = [
            br for br in brs if isinstance(br, dict) and not br.get("linked_task_ids")
        ]
        if unlinked_rules:
            titles = ", ".join(
                f'"{br.get("title", br.get("id", "?"))}"' for br in unlinked_rules[:3]
            )
            suffix = (
                f" and {len(unlinked_rules) - 3} more"
                if len(unlinked_rules) > 3
                else ""
            )
            raise ValueError(
                f"Cannot validate spec: {len(unlinked_rules)} business rule(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({titles}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each business rule via "
                f"okto_pulse_link_task(target_type='rule', target_id=<rule_id>, "
                f"card_id=<card_id>, spec_id=<spec_id>). "
                f"Alternatively, enable 'skip rules coverage' on the spec or board."
            )

    async def check_trs_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every structured TR has a linked task."""
        skip_global = (
            (board.settings or {}).get("skip_trs_coverage_global", False)
            if board
            else False
        )
        if getattr(spec, "skip_trs_coverage", False) or skip_global:
            return
        trs = list(spec.technical_requirements or [])
        structured_trs = [tr for tr in trs if isinstance(tr, dict) and tr.get("id")]
        if not structured_trs:
            return
        unlinked_trs = [tr for tr in structured_trs if not tr.get("linked_task_ids")]
        if unlinked_trs:
            previews = ", ".join(
                f'"{tr.get("text", tr.get("id", "?"))[:40]}"' for tr in unlinked_trs[:3]
            )
            suffix = (
                f" and {len(unlinked_trs) - 3} more" if len(unlinked_trs) > 3 else ""
            )
            raise ValueError(
                f"Cannot validate spec: {len(unlinked_trs)} technical requirement(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({previews}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each TR via "
                f"okto_pulse_link_task(target_type='tr', target_id=<tr_id>, "
                f"card_id=<card_id>, spec_id=<spec_id>). "
                f"Alternatively, enable 'skip TRs coverage' on the spec or board."
            )

    async def check_contract_coverage(
        self, spec: "Spec", board: "Board | None"
    ) -> None:
        """Check that every API contract has a linked task."""
        skip_global = (
            (board.settings or {}).get("skip_contract_coverage_global", False)
            if board
            else False
        )
        if getattr(spec, "skip_contract_coverage", False) or skip_global:
            return
        contracts = [
            contract
            for contract in (spec.api_contracts or [])
            if isinstance(contract, dict)
            and contract.get("status", "active") == "active"
        ]
        if not contracts:
            return
        unlinked = [c for c in contracts if not c.get("linked_task_ids")]
        if unlinked:
            previews = ", ".join(
                f'"{c.get("method", "?")} {c.get("path", "?")}"' for c in unlinked[:3]
            )
            suffix = f" and {len(unlinked) - 3} more" if len(unlinked) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked)} API contract(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({previews}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each API contract via "
                f"okto_pulse_link_task(target_type='contract', "
                f"target_id=<contract_id>, card_id=<card_id>, spec_id=<spec_id>). "
                f"Alternatively, enable 'skip contract coverage' on the spec or board."
            )

    async def check_ir_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every active integration requirement has a linked task."""
        skip_global = (
            (board.settings or {}).get("skip_ir_coverage_global", False)
            if board
            else False
        )
        if getattr(spec, "skip_ir_coverage", False) or skip_global:
            return
        requirements = [
            ir
            for ir in (getattr(spec, "integration_requirements", None) or [])
            if isinstance(ir, dict) and ir.get("status", "active") == "active"
        ]
        if not requirements:
            return
        unlinked = [ir for ir in requirements if not ir.get("linked_task_ids")]
        if unlinked:
            titles = ", ".join(
                f'"{ir.get("title", ir.get("id", "?"))}"' for ir in unlinked[:3]
            )
            suffix = f" and {len(unlinked) - 3} more" if len(unlinked) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked)} integration requirement(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({titles}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each IR via "
                f"okto_pulse_link_task(target_type='ir', target_id=<ir_id>, "
                f"card_id=<card_id>, spec_id=<spec_id>). "
                f"Alternatively, enable 'skip IR coverage' on the spec or board."
            )

    async def check_or_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every active observability requirement has a linked task."""
        skip_global = (
            (board.settings or {}).get("skip_or_coverage_global", False)
            if board
            else False
        )
        if getattr(spec, "skip_or_coverage", False) or skip_global:
            return
        requirements = [
            req
            for req in (getattr(spec, "observability_requirements", None) or [])
            if isinstance(req, dict) and req.get("status", "active") == "active"
        ]
        if not requirements:
            return
        unlinked = [req for req in requirements if not req.get("linked_task_ids")]
        if unlinked:
            titles = ", ".join(
                f'"{req.get("title", req.get("id", "?"))}"' for req in unlinked[:3]
            )
            suffix = f" and {len(unlinked) - 3} more" if len(unlinked) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked)} observability requirement(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({titles}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each OR via "
                f"okto_pulse_link_task(target_type='or', target_id=<or_id>, "
                f"card_id=<card_id>, spec_id=<spec_id>). "
                f"Alternatively, enable 'skip OR coverage' on the spec or board."
            )

    @staticmethod
    def _requirement_link_item_is_active(item: dict) -> bool:
        return str(item.get("status", "active")).lower() not in {
            "cancelled",
            "canceled",
            "revoked",
            "superseded",
        }

    @staticmethod
    def _board_skips_task_requirement_link_gate(board: "Board | None") -> bool:
        if board is None:
            return False
        settings = board.settings or {}
        if "skip_task_requirement_link_gate_global" not in settings:
            return True
        return bool(settings.get("skip_task_requirement_link_gate_global", False))

    @classmethod
    def _card_has_direct_requirement_link(cls, spec: "Spec", card_id: str) -> bool:
        """Return True when card_id is linked directly to FR/TR/BR/IR/OR."""
        requirement_fields = (
            "functional_requirements",
            "technical_requirements",
            "business_rules",
            "integration_requirements",
            "observability_requirements",
        )
        for field_name in requirement_fields:
            for item in getattr(spec, field_name, None) or []:
                if not isinstance(
                    item, dict
                ) or not cls._requirement_link_item_is_active(item):
                    continue
                linked_ids = {
                    str(value) for value in (item.get("linked_task_ids") or [])
                }
                if card_id in linked_ids:
                    return True
        return False

    async def check_card_requirement_link_gate(
        self,
        card: "Card",
        spec: "Spec | None",
        board: "Board | None",
    ) -> None:
        """Block normal task execution without a direct FR/TR/BR/IR/OR link."""
        if getattr(card, "card_type", CardType.NORMAL) != CardType.NORMAL:
            return
        if not getattr(card, "spec_id", None):
            return
        if self._board_skips_task_requirement_link_gate(board):
            return
        if getattr(card, "skip_task_requirement_link_gate", False):
            return
        if spec is None:
            raise CardOperationError(
                "task_requirement_link_required",
                "Cannot start task card: the card must be linked to a spec and "
                "to at least one FR/TR/BR/IR/OR requirement first.",
                remediation="link_task_to_requirement",
                facts={
                    "card_id": card.id,
                    "spec_id": getattr(card, "spec_id", None),
                    "required_link_types": ["fr", "tr", "rule", "ir", "or"],
                    "skip_allowed_surface": "ui_or_human_rest",
                },
            )
        if self._card_has_direct_requirement_link(spec, card.id):
            return
        raise CardOperationError(
            "task_requirement_link_required",
            "Cannot start task card: link it directly to at least one "
            "FR/TR/BR/IR/OR requirement, or use the human-only card/board skip.",
            remediation="link_task_to_requirement",
            facts={
                "card_id": card.id,
                "spec_id": spec.id,
                "required_link_types": ["fr", "tr", "rule", "ir", "or"],
                "skip_allowed_surface": "ui_or_human_rest",
            },
        )

    async def check_task_requirement_links_for_spec(
        self,
        spec: "Spec",
        board: "Board | None",
    ) -> None:
        """Block spec validation when active normal task cards are orphaned."""
        if self._board_skips_task_requirement_link_gate(board):
            return

        cards = await _application_list(
            self.db,
            "card",
            filters=(
                _apf("spec_id", "eq", spec.id),
                _apf("archived", "is_false"),
                _apf("card_type", "eq", CardType.NORMAL),
                _apf("status", "ne", CardStatus.CANCELLED),
            ),
        )
        orphaned: list[ApplicationRecord] = []
        for card in cards:
            if getattr(card, "skip_task_requirement_link_gate", False):
                continue
            if not self._card_has_direct_requirement_link(spec, card.id):
                orphaned.append(card)

        if orphaned:
            previews = ", ".join(f'"{card.title or card.id}"' for card in orphaned[:3])
            suffix = f" and {len(orphaned) - 3} more" if len(orphaned) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(orphaned)} normal task card(s) "
                f"in spec '{spec.title}' have no direct FR/TR/BR/IR/OR link "
                f"({previews}{suffix}). REQUIRED ACTION: Link each task via "
                f"okto_pulse_link_task(target_type='fr'|'tr'|'rule'|'ir'|'or', ...), "
                f"or use the human-only card/board skip in the UI."
            )

    async def check_decision_presence(self, spec: "Spec") -> None:
        """Require at least one active Decision before spec validation/progress."""
        decisions = list(spec.decisions or [])
        active = [
            d
            for d in decisions
            if isinstance(d, dict)
            and str(d.get("status", "active")).lower() == "active"
        ]
        if active:
            return
        raise ValueError(
            f"Cannot validate spec: spec '{spec.title}' must include at least "
            f"one active Decision. REQUIRED ACTION: add a Decision with "
            f"okto_pulse_add_decision before validating the spec."
        )

    async def check_decisions_coverage(
        self, spec: "Spec", board: "Board | None"
    ) -> None:
        """Check that every active Decision has a linked task unless skipped.

        New and legacy specs default to enforcing this gate. Only `active`
        decisions are checked — `superseded` and `revoked` are historical and
        do not need linkage.
        """
        skip_global = (
            (board.settings or {}).get("skip_decisions_coverage_global", False)
            if board
            else False
        )
        skip_spec = getattr(spec, "skip_decisions_coverage", False)
        if skip_spec or skip_global:
            return
        decisions = list(spec.decisions or [])
        active = [
            d
            for d in decisions
            if isinstance(d, dict) and d.get("status", "active") == "active"
        ]
        if not active:
            return
        unlinked = [d for d in active if not d.get("linked_task_ids")]
        if unlinked:
            titles = ", ".join(
                f'"{d.get("title", d.get("id", "?"))}"' for d in unlinked[:3]
            )
            suffix = f" and {len(unlinked) - 3} more" if len(unlinked) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked)} Decision(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({titles}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each Decision via "
                f"okto_pulse_link_task(target_type='decision', "
                f"target_id=<decision_id>, card_id=<card_id>, spec_id=<spec_id>). "
                f"Alternatively, enable 'skip decisions coverage' on the spec or board."
            )

    async def resequence_columns(
        self,
        board_id: str,
        ops: list[ColumnResequenceOp],
        *,
        extra_columns: tuple[CardStatus, ...] = (),
        records: dict[str, ApplicationRecord] | None = None,
    ) -> int:
        """Atomically place cards and rewrite the affected columns densely.

        Thin domain facade over :data:`CardRepositoryPort.resequence_columns`
        (``okto_pulse.core.ports.card_repository``) — the architecture's batch
        contract (refinement v17 item 7 + matriz v13 item 5) lives behind the
        port; the Core default implementation is :class:`CoreCardResequencer`.
        See the port module for the full pre-validation and determinism rules.
        """
        return await get_card_repository_port().resequence_columns(
            self.db,
            board_id,
            ops,
            extra_columns=extra_columns,
            records=records,
        )

    async def move_card(
        self, card_id: str, user_id: str, data: CardMove, actor_name: str | None = None
    ) -> ApplicationRecord | None:
        """Move a card to a different column/position. Blocks if dependencies not met.

        Moving execution work to 'validation' or 'done' requires an execution
        report. The report is appended to the card's conclusions list so
        reviewers can validate the executor's claim before approving it.
        """
        requested_position = data.position
        if requested_position is not None and requested_position < -1:
            # Authorized contract narrowing (QA 6afdc547): reject the legacy
            # negative sentinels BEFORE any read, mutation or event — the REST
            # boundary 422s first; this is service-level defense in depth.
            raise ValueError(
                "position_out_of_range: position must be None, -1 (end of column) or >= 0"
            )
        card = await self.get_card(card_id)
        if not card:
            return None

        archived_block = archived_card_block(
            CardTransitionFacts(
                card_id=card.id,
                old_status=card.status,
                new_status=data.status,
                archived=bool(getattr(card, "archived", False)),
            )
        )
        if archived_block is not None:
            raise ValueError(archived_block.detail)

        old_status = card.status
        card_type_value = getattr(card.card_type, "value", card.card_type or "normal")
        if old_status != data.status and not is_transition_allowed(
            "card",
            old_status.value,
            data.status.value,
            card_type=str(card_type_value),
        ):
            allowed_values = [
                edge.to_status
                for edge in transition_contracts("card", old_status.value)
                if edge.visibility == "public"
                and (not edge.card_types or str(card_type_value) in edge.card_types)
            ]
            raise CardOperationError(
                "card_transition_not_allowed",
                (
                    f"Cannot move {card_type_value} card from '{old_status.value}' "
                    f"to '{data.status.value}'. Allowed: {allowed_values}."
                ),
                remediation=(
                    "move_card_to_started_first"
                    if old_status == CardStatus.NOT_STARTED
                    and data.status == CardStatus.IN_PROGRESS
                    and card_type_value == CardType.NORMAL.value
                    else "choose_allowed_card_transition"
                ),
                facts={
                    "card_id": card.id,
                    "card_type": str(card_type_value),
                    "from_status": old_status.value,
                    "to_status": data.status.value,
                    "allowed_statuses": allowed_values,
                },
            )
        if (
            old_status is CardStatus.REJECTED
            and data.status is CardStatus.IN_PROGRESS
            and current_rejection_cause(card) is None
        ):
            raise CardOperationError(
                "current_rejection_cause_missing",
                "Rejected cards require a sealed Current cause before rework can start.",
                remediation="repair_rejected_card_cause_before_rework",
                facts={"card_id": card.id},
            )
        old_position = card.position

        # Load board settings for governance
        board = await _application_get(self.db, "board", card.board_id)
        board_settings = board.settings or {} if board else {}
        skip_global = board_settings.get("skip_test_coverage_global", False)

        # Block forward moves based on card_type and spec status.
        # Uses level comparison: spec must have reached the minimum required status.
        # Once a spec reaches IN_PROGRESS or DONE, cards can advance freely.
        old_level = self._STATUS_ORDER.get(old_status, 0)
        new_level = self._STATUS_ORDER.get(data.status, 0)
        precedence_expected_edition: int | None = None
        precedence_expected_status: SpecStatus | None = None
        precedence_expected_archived: bool | None = None
        if transition_starts_card_execution(old_status, data.status) and card.spec_id:
            spec_for_precedence = await _application_get(self.db, "spec", card.spec_id)
            if spec_for_precedence is not None:
                # Capture the optimistic lifecycle identity without taking the
                # graph fence. Expensive sprint, regression, traceability,
                # policy and cognitive gates run before the lock; readiness is
                # re-read under the fence immediately before mutation.
                precedence_expected_edition = int(
                    getattr(spec_for_precedence, "edition", 1) or 1
                )
                precedence_expected_status = spec_for_precedence.status
                precedence_expected_archived = bool(
                    getattr(spec_for_precedence, "archived", False)
                )
        if new_level > old_level and card.spec_id:
            spec_for_status = await _application_get(self.db, "spec", card.spec_id)
            if spec_for_status:
                card_type = getattr(card, "card_type", CardType.NORMAL)
                maturity_block = spec_maturity_block(
                    CardTransitionFacts(
                        card_id=card.id,
                        old_status=old_status,
                        new_status=data.status,
                        card_type=card_type,
                        spec_id=card.spec_id,
                        spec_title=spec_for_status.title,
                        spec_status=spec_for_status.status,
                    )
                )
                if maturity_block is not None:
                    raise ValueError(maturity_block.detail)

        # Sprint gate: if spec has sprints, card must have sprint_id and sprint must be active
        if new_level > old_level and card.spec_id:
            spec_for_sprint = await _application_get(self.db, "spec", card.spec_id)
            if spec_for_sprint:
                spec_sprints = await _application_list(
                    self.db,
                    "sprint",
                    filters=(
                        _apf("spec_id", "eq", card.spec_id),
                        _apf("archived", "is_false"),
                    ),
                )
                sprint_count = len(spec_sprints)
                if sprint_count > 0:
                    hotfix_count = sum(
                        1
                        for sprint in spec_sprints
                        if sprint.lane_type == SprintLaneType.HOTFIX
                    )
                    sprint_obj = (
                        await _application_get(self.db, "sprint", card.sprint_id)
                        if card.sprint_id
                        else None
                    )
                    transition_facts = CardTransitionFacts(
                        card_id=card.id,
                        old_status=old_status,
                        new_status=data.status,
                        card_type=getattr(card, "card_type", CardType.NORMAL),
                        spec_id=card.spec_id,
                        spec_status=spec_for_sprint.status,
                        sprint_count=sprint_count,
                        sprint_id=card.sprint_id,
                        sprint_exists=sprint_obj is not None
                        if card.sprint_id
                        else True,
                        sprint_status=sprint_obj.status if sprint_obj else None,
                        sprint_title=sprint_obj.title if sprint_obj else None,
                        sprint_is_hotfix=(
                            sprint_obj.lane_type == SprintLaneType.HOTFIX
                            if sprint_obj
                            else False
                        ),
                        hotfix_count=hotfix_count,
                    )
                    sprint_block = sprint_assignment_block(transition_facts)
                    if sprint_block is not None:
                        lane_type = (
                            sprint_obj.lane_type.value
                            if sprint_obj
                            else (
                                "hotfix"
                                if sprint_block.remediation == "assign_hotfix_lane"
                                else None
                            )
                        )
                        error_facts = {
                            "card_id": card.id,
                            "spec_id": card.spec_id,
                            "spec_status": spec_for_sprint.status.value,
                            "lane_type": lane_type,
                            "next_action": sprint_block.remediation,
                        }
                        if card.sprint_id:
                            error_facts["sprint_id"] = card.sprint_id
                        if sprint_obj:
                            error_facts["sprint_status"] = sprint_obj.status.value
                        workflow_message = (
                            f"Card's sprint '{sprint_obj.title}' is not active "
                            f"(status: '{sprint_obj.status.value}')."
                            if sprint_block.code == "sprint_not_active" and sprint_obj
                            else None
                        )
                        raise CardOperationError(
                            sprint_block.code,
                            sprint_block.detail,
                            remediation=sprint_block.remediation,
                            facts=error_facts,
                            workflow_remediation=(
                                BugWorkflowRemediationMessageBuilder().build_from_sprint_lane_block(
                                    code=sprint_block.code,
                                    remediation=sprint_block.remediation
                                    or "assign_sprint",
                                    facts=error_facts,
                                    message=workflow_message,
                                )
                                if getattr(card, "card_type", CardType.NORMAL)
                                == CardType.BUG
                                else None
                            ),
                        )

        # Task requirement link gate: normal task cards must be traceable to at
        # least one FR/TR/BR/IR/OR before execution starts. Human-only skips are
        # stored on the board/card and are intentionally not exposed via MCP.
        execution_start_level = self._STATUS_ORDER.get(CardStatus.IN_PROGRESS, 2)
        is_normal_task = getattr(card, "card_type", CardType.NORMAL) == CardType.NORMAL
        starts_execution = (
            old_status == CardStatus.NOT_STARTED
            and data.status in (CardStatus.STARTED, CardStatus.IN_PROGRESS)
        ) or (new_level >= execution_start_level and old_level < execution_start_level)
        if (
            is_normal_task
            and data.status != CardStatus.CANCELLED
            and new_level > old_level
            and starts_execution
        ):
            spec_for_requirement_gate = (
                await _application_get(self.db, "spec", card.spec_id)
                if card.spec_id
                else None
            )
            await self.check_card_requirement_link_gate(
                card, spec_for_requirement_gate, board
            )

        # --- Task Validation Gate: block in_progress→done when gate active ---
        if (
            data.status == CardStatus.DONE
            and old_status
            in (
                CardStatus.IN_PROGRESS,
                CardStatus.STARTED,
                CardStatus.NOT_STARTED,
                CardStatus.VALIDATION,
            )
            and getattr(card, "card_type", CardType.NORMAL) != CardType.TEST
        ):
            spec_for_gate = (
                await _application_get(self.db, "spec", card.spec_id)
                if card.spec_id
                else None
            )
            sprint_for_gate = (
                await _application_get(self.db, "sprint", card.sprint_id)
                if card.sprint_id
                else None
            )
            gate_config = self._resolve_validation_config(
                card, spec_for_gate, sprint_for_gate, board_settings
            )
            validation_block = validation_gate_block(
                CardTransitionFacts(
                    card_id=card.id,
                    old_status=old_status,
                    new_status=data.status,
                    card_type=getattr(card, "card_type", CardType.NORMAL),
                    validation_required=bool(gate_config["required"]),
                )
            )
            if validation_block is not None:
                raise ValueError(validation_block.detail)

        # Test-card completion is fail-closed over the persisted reference set.
        # An empty set, a missing spec, or a dangling scenario id is an
        # inconsistency — never evidence that all linked scenarios passed.
        if (
            data.status == CardStatus.DONE
            and getattr(card, "card_type", CardType.NORMAL) == CardType.TEST
            and not skip_global
        ):
            scenario_ids = [str(value) for value in (card.test_scenario_ids or [])]
            spec_for_test_scenarios = (
                await _application_get(self.db, "spec", card.spec_id)
                if card.spec_id
                else None
            )
            stale: list[dict[str, Any]] = []
            if not scenario_ids:
                stale.append(
                    {
                        "id": None,
                        "title": "No test scenario is linked to this test card",
                        "status": "missing",
                    }
                )
            elif spec_for_test_scenarios is None:
                stale.extend(
                    {
                        "id": scenario_id,
                        "title": f"Unresolved test scenario {scenario_id}",
                        "status": "missing",
                    }
                    for scenario_id in scenario_ids
                )
            else:
                all_scenarios = {
                    str(s["id"]): s
                    for s in (spec_for_test_scenarios.test_scenarios or [])
                    if isinstance(s, dict) and s.get("id") is not None
                }
                for scenario_id in scenario_ids:
                    scenario = all_scenarios.get(scenario_id)
                    if scenario is None:
                        stale.append(
                            {
                                "id": scenario_id,
                                "title": f"Unresolved test scenario {scenario_id}",
                                "status": "missing",
                            }
                        )
                        continue
                    if scenario.get("status") in (
                        "draft",
                        "ready",
                    ) or not scenario_has_authenticated_required_evidence(
                        board_id=card.board_id,
                        spec_id=card.spec_id,
                        scenario=scenario,
                        acceptance_criteria=list(
                            spec_for_test_scenarios.acceptance_criteria or []
                        ),
                    ):
                        stale.append(
                            {
                                "id": scenario_id,
                                "title": scenario.get("title", scenario_id),
                                "status": scenario.get("status"),
                            }
                        )
            pending_scenarios = tuple(
                PendingScenario(
                    scenario_id=str(scenario.get("id") or "__unlinked__"),
                    title=str(scenario["title"]),
                    status=str(scenario.get("status") or "missing"),
                )
                for scenario in stale
            )
            completion_block = test_completion_block(
                CardTransitionFacts(
                    card_id=card.id,
                    old_status=old_status,
                    new_status=data.status,
                    card_type=CardType.TEST,
                    spec_id=card.spec_id,
                    pending_scenarios=pending_scenarios,
                )
            )
            if completion_block is not None:
                from okto_pulse.core.services.gate_contracts import (
                    incomplete_test_card_completion_error,
                )

                raise incomplete_test_card_completion_error(
                    card_id=card.id,
                    current_status=old_status.value if old_status else None,
                    pending_scenarios=stale,
                    board_id=card.board_id,
                    spec_id=card.spec_id,
                )

        # --- Bug card: block execution-level moves without linked regression tests ---
        # Gate triggers when the bug first crosses into execution level or beyond
        # (in_progress, validation, on_hold, done) from a status before in_progress.
        # Once in_progress is reached, the gate was already passed.
        # NC-6 fix: gate is now conditional on board settings:
        #   - require_test_task_for_bug=False → gate desligado (qualquer bug avança)
        #   - bug_test_gate_min_severity controla qual severity entra no gate
        #     ("minor"=default, sempre exige; "major"=pula minor; "critical"=só critical)
        # Severity ordering (lower → higher): minor < major < critical
        _board_settings = (board.settings or {}) if board else {}
        _bug_gate_enabled = _board_settings.get("require_test_task_for_bug", True)
        _bug_gate_min_sev = _board_settings.get("bug_test_gate_min_severity", "minor")
        _card_severity = getattr(card, "severity", None) or "minor"
        bug_transition_facts = CardTransitionFacts(
            card_id=card.id,
            old_status=old_status,
            new_status=data.status,
            card_type=getattr(card, "card_type", CardType.NORMAL),
            spec_id=card.spec_id,
            require_test_task_for_bug=bool(_bug_gate_enabled),
            bug_test_gate_min_severity=str(_bug_gate_min_sev),
            severity=str(_card_severity),
        )

        if bug_regression_gate_applies(bug_transition_facts):
            bug_gate_started = time.perf_counter()
            linked_tests = list(card.linked_test_task_ids or [])
            # Path B (spec 62cf2d36, fr_646e69d2): when the bug's original spec is
            # content-locked there is no direct path to add a test card, so an
            # eligible AmendmentHotfixRevision formally linked to the bug
            # contributes its regression test tasks as an ADDITIVE fallback source.
            # They are validated below EXACTLY like directly-linked tasks and the
            # deep coverage/lineage decision stays in BugRegressionGateValidator
            # (fail-closed) — this never disables require_test_task_for_bug nor
            # relaxes validator-only coverage. Loaded once here and reused below.
            amendment_rows = (
                await AmendmentRevisionService(self.db).list_for_bug(
                    board_id=card.board_id,
                    original_spec_id=card.spec_id,
                    origin_bug_id=card.id,
                )
                if card.spec_id
                else []
            )
            amendment_facts = [
                AmendmentLineageFact.from_row(row) for row in amendment_rows
            ]
            effective_linked_tests = (
                linked_tests or _amendment_regression_test_task_ids(amendment_rows)
            )
            spec_for_bug = (
                await _application_get(self.db, "spec", card.spec_id)
                if card.spec_id
                else None
            )
            origin_task = (
                await _application_get(self.db, "card", card.origin_task_id)
                if card.origin_task_id
                else None
            )
            regression_block = bug_regression_evidence_block(
                CardTransitionFacts(
                    card_id=card.id,
                    old_status=old_status,
                    new_status=data.status,
                    card_type=getattr(card, "card_type", CardType.NORMAL),
                    spec_id=card.spec_id,
                    require_test_task_for_bug=bool(_bug_gate_enabled),
                    bug_test_gate_min_severity=str(_bug_gate_min_sev),
                    severity=str(_card_severity),
                    has_regression_test_evidence=bool(effective_linked_tests),
                )
            )
            if regression_block is not None:
                eligibility = (
                    BugRegressionScenarioEligibilityResolver().resolve(
                        bug_card=card,
                        spec=spec_for_bug,
                        origin_task=origin_task,
                        amendment_facts=amendment_facts,
                    )
                    if spec_for_bug is not None and origin_task is not None
                    else None
                )
                eligible_scenarios_count = (
                    len(eligibility.eligible_scenarios) if eligibility else 0
                )
                workflow_remediation = BugWorkflowRemediationMessageBuilder().build_missing_regression_test_task(
                    eligible_scenarios_count=eligible_scenarios_count,
                )
                if eligible_scenarios_count:
                    message = (
                        "Bug card requires at least 1 new test task linked before "
                        "moving to in_progress. REQUIRED STEPS: (1) Create a "
                        "regression test card with card_type='test', spec_id, and "
                        "test_scenario_ids using okto_pulse_create_card. The referenced scenario may be an existing scenario on a "
                        "validated/locked "
                        "spec; leave spec content unchanged for Path A regression "
                        "evidence. (2) Link the test task to this bug using "
                        "okto_pulse_update_card with "
                        "linked_test_task_ids. (3) Retry moving this bug card to "
                        "in_progress."
                    )
                else:
                    message = (
                        "Bug card requires regression coverage before moving to "
                        "in_progress, but its canonical task lineage has zero "
                        "eligible existing scenarios. Do not create an unrelated "
                        "test card. Use Path B: create an amendment, refinement, "
                        "spec revision, or hotfix spec for the semantic gap."
                    )
                raise CardOperationError(
                    regression_block.code,
                    message,
                    remediation=workflow_remediation.next_action.value,
                    facts={
                        "card_id": card.id,
                        "spec_id": card.spec_id,
                        "next_action": workflow_remediation.next_action.value,
                        "eligible_scenarios_count": eligible_scenarios_count,
                    },
                    workflow_remediation=workflow_remediation,
                )

            # Validate each linked test task (directly-linked OR contributed by an
            # eligible Path B amendment, computed above). Amendments formally linked
            # to THIS bug+spec feed the shared Path A/B predicate so a cross-spec
            # regression artifact is admissible ONLY with valid amendment lineage;
            # coverage stays validator-only/fail-closed — no production path confirms
            # coverage before card c9cf9781 (ADJ-B/ADJ-C).
            bug_created = card.created_at
            all_scenarios = (
                {
                    str(s["id"]): s
                    for s in (spec_for_bug.test_scenarios or [])
                    if isinstance(s, dict) and s.get("id") is not None
                }
                if spec_for_bug
                else {}
            )
            validated_test_tasks: list[ApplicationRecord] = []
            candidate_scenario_ids: list[str] = []

            for test_task_id in effective_linked_tests:
                test_task = await _application_get(self.db, "card", test_task_id)
                if not test_task:
                    raise ValueError(
                        f"Linked test task '{test_task_id}' not found. "
                        f"Remove it from linked_test_task_ids using okto_pulse_update_card "
                        f"and link a valid test task instead."
                    )

                # Validate test task is of type TEST
                if getattr(test_task, "card_type", "normal") != CardType.TEST:
                    raise ValueError(
                        f"Linked test task '{test_task.title}' is not a test card "
                        f"(type: {getattr(test_task, 'card_type', 'normal')}). "
                        f"Bug cards require linked test cards of type 'test'."
                    )

                # Validate test task has test_scenario_ids
                if not test_task.test_scenario_ids:
                    raise ValueError(
                        f"Linked test task '{test_task.title}' has no test_scenario_ids. "
                        f"A test task must be linked to at least one test scenario. "
                        f"Use okto_pulse_link_task(target_type='scenario', "
                        f"target_id=<scenario_id>, card_id=<test_card_id>, "
                        f"spec_id=<spec_id>) to link the test task to a scenario, "
                        f"or create a new test task with test_scenario_ids set."
                    )

                # Validate test task belongs to the same spec (Path A). A
                # cross-spec test task is admissible ONLY via Path B: when an
                # amendment formally links this bug we defer the decision to the
                # shared predicate (which fail-closes); with no amendment context
                # the cross-spec test task stays blocked (ADJ-C).
                if test_task.spec_id != card.spec_id and not amendment_facts:
                    raise ValueError(
                        f"Linked test task '{test_task.title}' belongs to spec '{test_task.spec_id}' "
                        f"but this bug belongs to spec '{card.spec_id}'. "
                        f"Test tasks must belong to the same spec as the bug card."
                    )

                if (
                    bug_created
                    and test_task.created_at
                    and test_task.created_at.isoformat() < bug_created.isoformat()
                ):
                    raise ValueError(
                        f"Linked test task '{test_task.title}' was created before this bug card. "
                        "Create or link a regression test task created after the bug so the bug has "
                        "fresh validation coverage without editing a locked spec."
                    )

                validated_test_tasks.append(test_task)
                candidate_scenario_ids.extend(
                    str(sid) for sid in (test_task.test_scenario_ids or [])
                )

            missing_scenario_ids = {
                sid for sid in candidate_scenario_ids if sid not in all_scenarios
            }
            candidate_spec_ids_by_scenario_id: dict[str, str] = {}
            if missing_scenario_ids:
                other_specs = await _application_list(
                    self.db,
                    "spec",
                    filters=(
                        _apf("board_id", "eq", card.board_id),
                        _apf("id", "ne", card.spec_id),
                    ),
                )
                for other_spec in other_specs:
                    for scenario in other_spec.test_scenarios or []:
                        if not isinstance(scenario, dict) or scenario.get("id") is None:
                            continue
                        scenario_id = str(scenario["id"])
                        if scenario_id in missing_scenario_ids:
                            candidate_spec_ids_by_scenario_id.setdefault(
                                scenario_id,
                                other_spec.id,
                            )

            for test_task in validated_test_tasks:
                # Validate scenarios exist in spec. Regression test cards may
                # reference existing scenarios even when the spec is locked.
                for sid in test_task.test_scenario_ids:
                    scenario_id = str(sid)
                    sc = all_scenarios.get(scenario_id)
                    if not sc:
                        other_spec_id = candidate_spec_ids_by_scenario_id.get(
                            scenario_id
                        )
                        if other_spec_id:
                            # TR1: cross-spec evidence is admissible ONLY via Path
                            # B. Always defer to the shared predicate
                            # (validate_linked_test_tasks below) — it fail-closes
                            # with a stable Path B reason (missing_amendment_revision
                            # when no formal amendment links this bug), replacing
                            # the old direct same-spec equality reject.
                            continue
                        observe_bug_regression_resolution(
                            board_id=card.board_id,
                            result=None,
                            duration_ms=(time.perf_counter() - bug_gate_started) * 1000,
                            spec_id=card.spec_id,
                            error_code="scenario_not_found",
                        )
                        await record_bug_regression_decision(
                            board_id=card.board_id,
                            bug_id=card.id,
                            spec_id=card.spec_id,
                            decision="semantic_gap",
                            reason_code="scenario_not_found",
                            scenario_count=len(candidate_scenario_ids),
                            test_task_count=len(validated_test_tasks),
                            actor_id=user_id,
                            session=self.db,
                        )
                        workflow_remediation = (
                            BugWorkflowRemediationMessageBuilder().build_semantic_gap(
                                reason_code="scenario_not_found"
                            )
                        )
                        raise CardOperationError(
                            "scenario_not_found",
                            f"Test scenario '{scenario_id}' referenced by test task '{test_task.title}' "
                            f"does not exist in spec '{spec_for_bug.title if spec_for_bug else card.spec_id}'. "
                            f"The scenario may have been deleted. Link the test task to an existing scenario, "
                            "or create an amendment/refinement/spec revision/hotfix spec if new canonical "
                            "coverage is truly required. reason=scenario_not_found; "
                            "next_action=escalate_semantic_gap.",
                            remediation="escalate_semantic_gap",
                            facts={
                                "card_id": card.id,
                                "spec_id": card.spec_id,
                                "next_action": workflow_remediation.next_action.value,
                            },
                            workflow_remediation=workflow_remediation,
                        )

            if not origin_task:
                observe_bug_regression_resolution(
                    board_id=card.board_id,
                    result=None,
                    duration_ms=(time.perf_counter() - bug_gate_started) * 1000,
                    spec_id=card.spec_id,
                    error_code="origin_task_missing",
                )
                await record_bug_regression_decision(
                    board_id=card.board_id,
                    bug_id=card.id,
                    spec_id=card.spec_id,
                    decision="semantic_gap",
                    reason_code="origin_task_missing",
                    scenario_count=len(candidate_scenario_ids),
                    test_task_count=len(validated_test_tasks),
                    actor_id=user_id,
                    session=self.db,
                )
                workflow_remediation = (
                    BugWorkflowRemediationMessageBuilder().build_semantic_gap(
                        reason_code="origin_task_missing"
                    )
                )
                raise CardOperationError(
                    "origin_task_missing",
                    "Bug card requires a valid origin_task_id before regression scenario eligibility "
                    "can be evaluated. reason=origin_task_missing; next_action=escalate_semantic_gap.",
                    remediation="escalate_semantic_gap",
                    facts={
                        "card_id": card.id,
                        "spec_id": card.spec_id,
                        "next_action": workflow_remediation.next_action.value,
                    },
                    workflow_remediation=workflow_remediation,
                )

            gate_result = BugRegressionGateValidator().validate_linked_test_tasks(
                bug_card=card,
                linked_test_tasks=validated_test_tasks,
                spec=spec_for_bug,
                origin_task=origin_task,
                candidate_spec_ids_by_scenario_id=candidate_spec_ids_by_scenario_id,
                # G2 (c9cf9781): coverage is NOT passed in (a bool would be
                # forgeable). It is derived from the persisted, artifact-bound
                # validator attestation carried on each amendment fact
                # (validation_metadata.coverage_confirmation) — fail-closed.
                amendment_facts=amendment_facts,
            )
            eligibility = gate_result.eligibility
            observe_bug_regression_resolution(
                board_id=card.board_id,
                result=eligibility,
                duration_ms=(time.perf_counter() - bug_gate_started) * 1000,
            )
            if eligibility.rejected_scenarios:
                primary_reason = eligibility.rejected_scenarios[0].reason.value
            elif eligibility.eligible_scenarios:
                primary_reason = eligibility.eligible_scenarios[0].reason.value
            elif (
                eligibility.coverage_state
                is BugRegressionCoverageState.COVERAGE_PENDING
            ):
                primary_reason = "coverage_pending"
            else:
                primary_reason = "no_eligible_scenarios"

            await record_bug_regression_decision(
                board_id=card.board_id,
                bug_id=card.id,
                spec_id=card.spec_id,
                # The bounded decision vocabulary (eligible/rejected/semantic_gap)
                # is owned by the observability schema; extending it belongs to
                # 966c7e7c. coverage_pending is a non-allow block -> recorded as
                # "rejected"; the precise signal travels in reason_code below.
                decision=(
                    "eligible"
                    if gate_result.allowed
                    else (
                        "semantic_gap"
                        if eligibility.semantic_gap_required
                        else "rejected"
                    )
                ),
                reason_code=primary_reason,
                coverage_state=eligibility.coverage_state.value,
                scenario_count=len(candidate_scenario_ids),
                test_task_count=len(validated_test_tasks),
                actor_id=user_id,
                session=self.db,
            )
            if not gate_result.allowed:
                rejected = (
                    ", ".join(
                        f"{item.scenario_id}:{item.reason.value}"
                        + (f"({item.detail})" if item.detail else "")
                        for item in eligibility.rejected_scenarios
                    )
                    or "none"
                )
                eligible_ids = (
                    ", ".join(
                        item.scenario_id for item in eligibility.eligible_scenarios
                    )
                    or "none"
                )
                workflow_remediation = (
                    BugWorkflowRemediationMessageBuilder().build_from_eligibility(
                        eligibility
                    )
                )
                raise CardOperationError(
                    gate_result.decision.value,
                    "Bug linked test task scenarios do not satisfy regression eligibility. "
                    f"decision={gate_result.decision.value}; "
                    f"eligible_scenario_ids=[{eligible_ids}]; "
                    f"rejected_scenarios=[{rejected}]; "
                    f"semantic_gap_required={str(eligibility.semantic_gap_required).lower()}; "
                    f"spec_mutation_required={str(eligibility.spec_mutation_required).lower()}; "
                    f"next_action={eligibility.next_action.value}. "
                    "Reuse only scenarios linked to the bug origin task or explicit affected tasks. "
                    "If expected behavior changed or no eligible scenario exists, create an "
                    "amendment/refinement/spec revision/hotfix spec instead of editing the current spec.",
                    remediation=workflow_remediation.next_action.value,
                    facts={
                        "card_id": card.id,
                        "spec_id": card.spec_id,
                        "decision": gate_result.decision.value,
                        "next_action": workflow_remediation.next_action.value,
                        "eligible_scenarios_count": len(eligibility.eligible_scenarios),
                    },
                    workflow_remediation=workflow_remediation,
                )
            emit_no_unlock_invariant(
                board_id=card.board_id,
                spec_id=card.spec_id,
            )

        await evaluate_code_traceability_transition(
            self.db,
            board=board,
            subject=card,
            subject_type=CodeTraceabilitySubjectType.CARD,
            from_status=old_status.value,
            to_status=data.status.value,
            enforce=True,
        )

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=card.board_id,
            actor_id=user_id,
            entity_type="card",
            entity_id=card.id,
            critical_action=_critical_card_move_action(data.status),
            surface="service",
            actor_type="user",
            actor_name=actor_name,
            card_id=card.id,
        )

        if data.status == CardStatus.DONE:
            await self._validate_cognitive_done(card, board)

        # Policy Compliance is a forward aggregate gate.  Recompute it in this
        # mutation UoW before appending the execution report, publishing its
        # event, or resequencing the card.  Free recovery/cancellation edges
        # short-circuit inside the service without touching the resolver.
        if old_status != data.status:
            await GuidelineService(self.db).enforce_policy_transition(
                board_id=card.board_id,
                entity_type="card",
                subject_id=card.id,
                from_status=old_status.value,
                to_status=data.status.value,
            )

        report_target = None
        pending_conclusion_entry: dict[str, Any] | None = None
        pending_missing_impact_advisory = False
        if data.status == CardStatus.DONE:
            report_target = "Done"
        elif (
            data.status == CardStatus.VALIDATION
            and old_status
            in (
                CardStatus.NOT_STARTED,
                CardStatus.STARTED,
                CardStatus.IN_PROGRESS,
                CardStatus.ON_HOLD,
            )
            and getattr(card, "card_type", CardType.NORMAL) != CardType.TEST
        ):
            report_target = "Validation"

        # Require an execution report before handoff to Validation/Done.
        if report_target:
            if not data.conclusion or not data.conclusion.strip():
                raise ValueError(
                    f"A conclusion is required when moving a card to {report_target}. "
                    "The conclusion must be the executor's detailed claim including: "
                    "(1) what was done — specific changes and files modified, "
                    "(2) technical decisions and reasoning, "
                    "(3) what was tested and results, "
                    "(4) any side effects or follow-ups. "
                    "Provide the conclusion in the 'conclusion' parameter."
                )
            # Validate completeness (0-100)
            if data.completeness is None:
                raise ValueError(
                    f"completeness (0-100) is required when moving a card to {report_target}. "
                    "It indicates how much of the planned work was actually implemented. "
                    "100 = fully complete, 0 = nothing delivered."
                )
            if not (0 <= data.completeness <= 100):
                raise ValueError("completeness must be between 0 and 100.")
            if (
                not data.completeness_justification
                or not data.completeness_justification.strip()
            ):
                raise ValueError(
                    f"completeness_justification is required when moving a card to {report_target}. "
                    "Explain why the completeness score is what it is."
                )
            # Validate drift (0-100)
            if data.drift is None:
                raise ValueError(
                    f"drift (0-100) is required when moving a card to {report_target}. "
                    "It indicates how much the implementation deviated from the original plan. "
                    "0 = no deviation, 100 = completely different from plan."
                )
            if not (0 <= data.drift <= 100):
                raise ValueError("drift must be between 0 and 100.")
            if not data.drift_justification or not data.drift_justification.strip():
                raise ValueError(
                    f"drift_justification is required when moving a card to {report_target}. "
                    "Explain what caused the deviation from the original plan."
                )

            # SK-B2-S1 (FR-6): impact_evidence enforcement lives EXACTLY
            # inside the existing report_target block, inheriting its real
            # exemptions (test cards; submit_task_validation->DONE never
            # passes through here).
            from okto_pulse.core.services.impact_evidence import (
                resolve_impact_evidence_mode,
            )

            impact_mode, _impact_mode_source = resolve_impact_evidence_mode(board)
            impact_block = data.impact_evidence
            impact_populated = (
                impact_block is not None and impact_block.is_minimally_populated()
            )
            if impact_mode == "require" and not impact_populated:
                raise CardOperationError(
                    "impact_evidence_required",
                    "This board requires declared impact evidence on the "
                    f"execution report before moving to {report_target}: "
                    "include impact_evidence with at least one populated "
                    "section (files, symbols, surfaces or tests).",
                    remediation=(
                        "Re-enumerate what the execution touched and resubmit "
                        "the move with impact_evidence (schema_version=1): "
                        "changed files (repo+path+change_kind), key symbols "
                        "(name+kind+action+file), affected surfaces "
                        "(kind+identifier) and authored tests. The block is a "
                        "claim - the validator still diffs reality."
                    ),
                    facts={
                        "card_id": card_id,
                        "impact_evidence_mode": impact_mode,
                        "target_status": data.status.value,
                    },
                )
            pending_missing_impact_advisory = (
                impact_mode == "advisory" and not impact_populated
            )

            report_source = (
                "move_to_validation"
                if data.status == CardStatus.VALIDATION
                else "move_to_done"
            )
            conclusion_entry: dict[str, Any] = {
                "text": data.conclusion.strip(),
                "author_id": user_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "completeness": data.completeness,
                "completeness_justification": data.completeness_justification.strip(),
                "drift": data.drift,
                "drift_justification": data.drift_justification.strip(),
                "source": report_source,
            }
            if impact_block is not None:
                # FR-1: the block persists next to the conclusion in the
                # append-only JSON; omitted optional fields stay omitted so
                # the stored shape round-trips the submitted one (AC-2).
                conclusion_entry["impact_evidence"] = impact_block.model_dump(
                    mode="json", exclude_none=True
                )
            pending_conclusion_entry = conclusion_entry

        # Block forward moves if dependencies not met
        if new_level > old_level and data.status != CardStatus.CANCELLED:
            deps_met, blocking = await self.check_dependencies_met(card_id)
            if not deps_met:
                raise CardOperationError(
                    "dependencies_incomplete",
                    "Card dependencies must be done or cancelled before advancing.",
                    remediation="complete_blocking_dependencies",
                    facts={
                        "card_id": card_id,
                        "blocking_dependencies": blocking,
                    },
                )

        if data.status == CardStatus.DONE:
            await ResourceGateService(self.db).validate_or_raise_entity_completion(
                card.board_id,
                "card",
                card.id,
                phase="card_done",
            )

        if precedence_expected_edition is not None and card.spec_id:
            from okto_pulse.core.ports.relational_application import (
                require_relational_application_adapter,
            )
            from okto_pulse.core.services.spec_dependency import SpecDependencyService

            dependency_service = SpecDependencyService(
                require_relational_application_adapter().spec_dependencies(self.db),
                self.db,
            )
            await dependency_service.require_ready_for_execution(
                board_id=card.board_id,
                spec_id=card.spec_id,
                mark_started=True,
                expected_edition=precedence_expected_edition,
                expected_status=precedence_expected_status,
                expected_archived=precedence_expected_archived,
            )

        # All report validation and potentially blocking dependency/resource
        # reads run before staging the report.  On an execution-start edge the
        # precedence graph fence above is therefore acquired first, so a
        # concurrent prerequisite edit cannot leave a conclusion/event queued
        # for an execution start that must fail.
        if pending_conclusion_entry is not None:
            if pending_missing_impact_advisory:
                # AC-17: exact advisory payload - action name, card scope and
                # {mode, target_status, author_id} details; no entry in the
                # off/require modes.
                await self._log_activity(
                    board_id=card.board_id,
                    action="impact_evidence_missing",
                    actor_type="user",
                    actor_id=user_id,
                    actor_name=actor_name
                    or await resolve_actor_name(self.db, user_id, card.board_id),
                    card_id=card.id,
                    details={
                        "mode": "advisory",
                        "target_status": data.status.value,
                        "author_id": user_id,
                    },
                )

            conclusions = list(card.conclusions or [])
            conclusions.append(pending_conclusion_entry)
            card.conclusions = conclusions
            card.mark_dirty("conclusions")

            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import CardConclusionAdded

            await event_publish(
                CardConclusionAdded(
                    board_id=card.board_id,
                    actor_id=user_id,
                    card_id=card_id,
                    spec_id=card.spec_id,
                    conclusion_excerpt=pending_conclusion_entry["text"][:280],
                    added_by=user_id,
                ),
                session=self.db,
            )

        spec_for_auto_rollback = None
        if data.status == CardStatus.CANCELLED and card.spec_id:
            candidate = await _application_get(self.db, "spec", card.spec_id)
            if candidate is not None and candidate.status == SpecStatus.VALIDATED:
                from okto_pulse.core.ports.relational_application import (
                    require_relational_application_adapter,
                )
                from okto_pulse.core.services.spec_dependency import (
                    SpecDependencyService,
                )

                await SpecDependencyService(
                    require_relational_application_adapter().spec_dependencies(self.db),
                    self.db,
                ).acquire_lifecycle_write_fence(board_id=card.board_id)
                if not await _application_fence(
                    self.db,
                    "spec",
                    candidate.id,
                    expected_values={
                        "status": candidate.status,
                        "edition": int(getattr(candidate, "edition", 1) or 1),
                        "version": int(candidate.version),
                        "archived": bool(getattr(candidate, "archived", False)),
                        "current_validation_id": candidate.current_validation_id,
                    },
                ):
                    raise LifecycleTransitionConflictError("spec", candidate.id)
                spec_for_auto_rollback = candidate

        # Cancellation justification (ITEM 17): cancel requires a reason
        # (replacing any previous one); reopening clears it.
        apply_cancellation_policy(
            card,
            entity_type="card",
            from_status=old_status,
            to_status=data.status,
            reason=getattr(data, "cancellation_reason", None),
            actor_id=user_id,
        )

        if old_status is CardStatus.REJECTED and data.status is CardStatus.IN_PROGRESS:
            # Preserve append-only rejection_records/validation history while
            # ending the bounded Current projection for this rework handoff.
            for field_name in (
                "current_rejection_kind",
                "current_rejection_id",
                "current_rejection_code",
                "current_rejection_summary",
            ):
                setattr(card, field_name, None)
                card.mark_dirty(field_name)

        # position < -1 was rejected at the top of this method, before any
        # read, mutation or event (authorized narrowing, QA 6afdc547). All
        # selectors are forwarded; the resequencer enforces their mutual
        # exclusivity (resequence_conflicting_placement) and anchor validity.
        await self.resequence_columns(
            card.board_id,
            [
                ColumnResequenceOp(
                    card_id=card.id,
                    from_status=old_status,
                    to_status=data.status,
                    target_index=(
                        None
                        if requested_position is None or requested_position == -1
                        else requested_position
                    ),
                    before_id=getattr(data, "before_id", None),
                    after_id=getattr(data, "after_id", None),
                    placement=getattr(data, "placement", None),
                )
            ],
            records={card.id: card},
        )

        # Auto-rollback: if card cancelled and spec is validated → revert to approved
        if spec_for_auto_rollback is not None:
            spec_for_auto_rollback.status = SpecStatus.APPROVED
            if spec_for_auto_rollback.evaluations:
                for ev in spec_for_auto_rollback.evaluations:
                    ev["stale"] = True
                spec_for_auto_rollback.mark_dirty("evaluations")
            rollback_name = actor_name or await resolve_actor_name(
                self.db, user_id, card.board_id
            )
            spec_service = SpecService(self.db)
            await spec_service._record_history(
                spec_id=card.spec_id,
                action="status_changed",
                actor_id=user_id,
                actor_name=rollback_name,
                changes=[{"field": "status", "old": "validated", "new": "approved"}],
                summary=f"Auto-rollback: card '{card.title}' cancelled — spec reverted for revalidation",
                version=spec_for_auto_rollback.version,
            )

        # Application records are detached from adapter-specific identity maps.
        # Synchronize the transition before another service reads it in this UoW.
        await _application_flush(self.db)

        resolved_name = actor_name or await resolve_actor_name(
            self.db, user_id, card.board_id
        )

        # Emit CardMoved + optional CardCancelled / CardRestored so downstream
        # handlers (e.g. KG decay on cancel) can react.
        if old_status != data.status:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import (
                CardCancelled,
                CardMoved,
                CardRestored,
            )

            await event_publish(
                CardMoved(
                    board_id=card.board_id,
                    actor_id=user_id,
                    card_id=card.id,
                    from_status=old_status.value,
                    to_status=data.status.value,
                    spec_id=card.spec_id,
                    moved_by=user_id,
                ),
                session=self.db,
            )
            if data.status == CardStatus.CANCELLED:
                await event_publish(
                    CardCancelled(
                        board_id=card.board_id,
                        actor_id=user_id,
                        card_id=card.id,
                        previous_status=old_status.value,
                    ),
                    session=self.db,
                )
            elif old_status == CardStatus.CANCELLED:
                await event_publish(
                    CardRestored(
                        board_id=card.board_id,
                        actor_id=user_id,
                        card_id=card.id,
                        to_status=data.status.value,
                    ),
                    session=self.db,
                )

        await self._log_activity(
            board_id=card.board_id,
            card_id=card_id,
            action="card_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "from_status": old_status.value,
                "to_status": data.status.value,
                "from_position": old_position,
                "to_position": card.position,
            },
        )
        return card

    async def delete_card(
        self,
        card_id: str,
        user_id: str,
        *,
        return_receipt: bool = False,
        actor_type: str = "user",
        actor_name: str | None = None,
        activity_details: Mapping[str, Any] | None = None,
    ) -> bool | GovernedArtifactDeletionReceipt:
        """Delete a card.

        Cascade-cleans orphan references before the row delete so the next
        update_spec/create_card on the same spec doesn't trip
        _validate_spec_linked_refs. Cleans 5 JSON containers on the parent
        spec and the linked_test_task_ids column on any bug card that pointed
        at this one. Same transaction as the delete.
        """
        card = await self.get_card(card_id)
        if not card:
            return False
        require_card_operational_mutation_allowed(card, operation="delete_card")

        board_id = card.board_id

        # A hotfix lane requires origin_bug_id for its entire persisted lifetime.
        # Guard in the application writer rather than relying on schema FKs: fresh
        # databases use ON DELETE SET NULL, while upgraded legacy schemas may have
        # no FK at all. Both would otherwise destroy/dangle mandatory lineage.
        origin_references = await _application_list(
            self.db,
            "sprint",
            filters=(_apf("origin_bug_id", "eq", card_id),),
        )
        if origin_references:
            same_board_sprint_ids = [
                sprint.id for sprint in origin_references if sprint.board_id == board_id
            ]
            raise CardOperationError(
                "hotfix_origin_bug_delete_conflict",
                "Cannot delete this bug while a hotfix sprint references it as "
                "origin_bug_id.",
                remediation=(
                    "Complete/close the hotfix workflow, then remove the sprint or "
                    "relineage it to a valid same-spec bug before deleting this card."
                ),
                facts={
                    "card_id": card_id,
                    "board_id": board_id,
                    "referencing_sprint_count": len(origin_references),
                    "referencing_sprint_ids": same_board_sprint_ids,
                    "next_action": "remove_or_relineage_hotfix_before_bug_delete",
                },
            )

        # This delete may also rewrite Bug regression links. Resolve and guard
        # every affected Bug before staging any parent-Spec or Card mutation.
        referencing_bugs: list[ApplicationRecord] = []
        if getattr(card, "card_type", CardType.NORMAL) != CardType.BUG:
            bugs = await _application_list(
                self.db,
                "card",
                filters=(
                    _apf("board_id", "eq", board_id),
                    _apf("card_type", "eq", CardType.BUG),
                ),
            )
            referencing_bugs = [
                bug
                for bug in bugs
                if card_id in (getattr(bug, "linked_test_task_ids", None) or [])
            ]
            for bug in referencing_bugs:
                require_card_operational_mutation_allowed(
                    bug,
                    operation="delete_linked_regression_test_card",
                )

        # Cascade cleanup: strip card_id from every reference list on the
        # parent spec. Must run BEFORE db.delete(card) so any validator
        # running on the same session sees a consistent state.
        if card.spec_id:
            spec = await _application_get(self.db, "spec", card.spec_id)
            if spec is not None:
                _SPEC_LINK_CONTAINERS = (
                    "test_scenarios",
                    "business_rules",
                    "api_contracts",
                    "integration_requirements",
                    "observability_requirements",
                    "technical_requirements",
                    "decisions",
                )
                for container_name in _SPEC_LINK_CONTAINERS:
                    items = getattr(spec, container_name, None) or []
                    changed = False
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        linked = item.get("linked_task_ids") or []
                        if card_id in linked:
                            item["linked_task_ids"] = [
                                tid for tid in linked if tid != card_id
                            ]
                            changed = True
                    if changed:
                        spec.mark_dirty(container_name)

        # Cascade cleanup: bug cards on the same board may reference this
        # card via their columnar linked_test_task_ids. Non-bug cards only —
        # deleting a bug card doesn't leave references elsewhere.
        for bug in referencing_bugs:
            linked = bug.linked_test_task_ids or []
            bug.linked_test_task_ids = [tid for tid in linked if tid != card_id]
            bug.mark_dirty("linked_test_task_ids")

        resolved_actor_name = actor_name or await resolve_actor_name(
            self.db,
            user_id,
            board_id,
        )
        extra_activity_details = {
            field: activity_log_value(value)
            for field, value in (activity_details or {}).items()
        }
        attachment_receipts: list[AttachmentDeletionReceipt] = []
        attachment_service = AttachmentService(self.db)
        attachments = await _application_list(
            self.db,
            "attachment",
            filters=(_apf("card_id", "eq", card_id),),
        )
        try:
            for attachment in attachments:
                deletion = await attachment_service.delete_attachment_object(attachment)
                attachment_receipts.append(deletion)

            takedown_receipt = await _prepare_governed_artifact_deletion(
                self.db,
                board_id=board_id,
                artifact_type="card",
                artifact_id=card_id,
            )
            await _application_delete(self.db, card)

            await self._log_activity(
                board_id=board_id,
                card_id=card_id,
                action="card_deleted",
                actor_type=actor_type,
                actor_id=user_id,
                actor_name=resolved_actor_name,
                details={**extra_activity_details, "title": card.title},
            )
        except BaseException:
            for receipt in reversed(attachment_receipts):
                await attachment_service.restore_deleted_attachment(receipt)
            raise
        takedown_receipt = replace(
            takedown_receipt,
            attachment_deletions=tuple(attachment_receipts),
        )
        return takedown_receipt if return_receipt else True

    @staticmethod
    async def restore_deleted_card_attachments(
        receipt: GovernedArtifactDeletionReceipt,
    ) -> None:
        """Compensate attachment objects when the card UoW does not commit."""

        storage = get_storage_provider()
        for attachment in reversed(receipt.attachment_deletions):
            await storage.restore(attachment.path, attachment.content)

    async def _log_activity(self, **kwargs: Any) -> None:
        """Log an activity."""
        await _application_add(
            self.db,
            _new_application_record("activity_log", **kwargs),
        )


class AgentService:
    """Service for agent operations."""

    def __init__(self, db: Any):
        self.db = db

    @staticmethod
    def generate_api_key() -> str:
        """Generate a secure API key."""
        return f"dash_{secrets.token_hex(24)}"

    @staticmethod
    def hash_api_key(key: str) -> str:
        """Hash an API key for storage."""
        return hashlib.sha256(key.encode()).hexdigest()

    @staticmethod
    def credential_marker(key_hash: str) -> str:
        """Return a non-recoverable value for the legacy NOT NULL api_key column."""
        return f"sha256:{key_hash[:57]}"

    async def create_agent(
        self, user_id: str, data: AgentCreate
    ) -> tuple[ApplicationRecord, str]:
        """Create a new global agent (no board_id).

        ``permission_flags`` is the direct override layer, never a materialized
        preset snapshot.  Selecting a preset with no explicit overrides stores
        an empty delta; no preset and no direct layer stores ``None``, the
        trusted Full Control sentinel.
        """
        import copy

        reveal_once_secret = self.generate_api_key()
        key_hash = self.hash_api_key(reveal_once_secret)

        flags: dict | None = (
            copy.deepcopy(data.permission_flags)
            if data.permission_flags is not None
            else None
        )
        preset_id = data.preset_id
        if preset_id and flags is None:
            flags = {}

        agent = _new_application_record(
            "agent",
            name=data.name,
            description=data.description,
            objective=data.objective,
            api_key=self.credential_marker(key_hash),
            api_key_hash=key_hash,
            permissions=data.permissions,
            preset_id=preset_id,
            permission_flags=flags,
            created_by=user_id,
        )
        await _application_add(self.db, agent)
        return agent, reveal_once_secret

    async def get_agent(self, agent_id: str) -> ApplicationRecord | None:
        """Get an agent by ID."""
        return await _application_get(self.db, "agent", agent_id)

    async def get_agent_by_key(
        self,
        api_key: str,
        *,
        touch_last_used_at: bool = True,
    ) -> ApplicationRecord | None:
        """Get an agent by API key, optionally recording credential usage."""
        key_hash = self.hash_api_key(api_key)
        agents = await _application_list(
            self.db,
            "agent",
            filters=(
                _apf("api_key_hash", "eq", key_hash),
                _apf("is_active", "is_true"),
            ),
            limit=1,
        )
        agent = agents[0] if agents else None
        if agent and touch_last_used_at:
            agent.last_used_at = datetime.now(timezone.utc)
        return agent

    async def touch_agent_last_used_at(self, agent_id: str) -> None:
        """Explicitly record API-key usage without overloading read auth paths."""
        agent = await self.get_agent(agent_id)
        if agent:
            agent.last_used_at = datetime.now(timezone.utc)

    async def list_agents_for_user(self, user_id: str) -> list[ApplicationRecord]:
        """List all agents owned by a user."""
        return await _application_list(
            self.db,
            "agent",
            filters=(_apf("created_by", "eq", user_id),),
            order_by=(("created_at", False),),
        )

    async def list_agents_for_board(self, board_id: str) -> list[ApplicationRecord]:
        """List all agents that have access to a board (via junction)."""
        grants = await _application_list(
            self.db,
            "agent_board",
            filters=(_apf("board_id", "eq", board_id),),
        )
        agents = (
            await _application_list(
                self.db,
                "agent",
                filters=(_apf("id", "in", [grant.agent_id for grant in grants]),),
                order_by=(("created_at", False),),
            )
            if grants
            else []
        )
        grants_by_agent = {grant.agent_id: grant for grant in grants}
        for agent in agents:
            grant = grants_by_agent.get(agent.id)
            agent.attach(
                "permission_overrides",
                getattr(grant, "permission_overrides", None),
            )
        return agents

    async def list_agents(self, board_id: str) -> list[ApplicationRecord]:
        """Backward-compat alias for list_agents_for_board."""
        return await self.list_agents_for_board(board_id)

    async def agent_has_board_access(self, agent_id: str, board_id: str) -> bool:
        """Check if an agent has access to a board."""
        rows = await _application_list(
            self.db,
            "agent_board",
            filters=(
                _apf("agent_id", "eq", agent_id),
                _apf("board_id", "eq", board_id),
            ),
            limit=1,
        )
        return bool(rows)

    async def grant_board_access(
        self, agent_id: str, board_id: str, granted_by: str
    ) -> ApplicationRecord:
        """Grant an agent access to a board."""
        grant = _new_application_record(
            "agent_board",
            agent_id=agent_id,
            board_id=board_id,
            granted_by=granted_by,
        )
        await _application_add(self.db, grant)
        return grant

    async def revoke_board_access(self, agent_id: str, board_id: str) -> bool:
        """Revoke an agent's access to a board."""
        rows = await _application_list(
            self.db,
            "agent_board",
            filters=(
                _apf("agent_id", "eq", agent_id),
                _apf("board_id", "eq", board_id),
            ),
        )
        for row in rows:
            await _application_delete(self.db, row)
        return bool(rows)

    async def update_board_overrides(
        self, agent_id: str, board_id: str, permission_overrides: dict | None
    ) -> ApplicationRecord | None:
        """Update permission overrides for an agent on a specific board."""
        rows = await _application_list(
            self.db,
            "agent_board",
            filters=(
                _apf("agent_id", "eq", agent_id),
                _apf("board_id", "eq", board_id),
            ),
            limit=1,
        )
        ab = rows[0] if rows else None
        if not ab:
            return None
        ab.permission_overrides = permission_overrides
        return ab

    async def list_boards_for_agent(self, agent_id: str) -> list[ApplicationRecord]:
        """List all boards an agent has access to."""
        grants = await _application_list(
            self.db,
            "agent_board",
            filters=(_apf("agent_id", "eq", agent_id),),
        )
        return (
            await _application_list(
                self.db,
                "board",
                filters=(_apf("id", "in", [grant.board_id for grant in grants]),),
                order_by=(("name", False),),
            )
            if grants
            else []
        )

    async def update_agent(
        self, agent_id: str, data: AgentUpdate
    ) -> ApplicationRecord | None:
        """Update an agent.

        Special handling:
        - If `preset_id` is set (and `permission_flags` is NOT in the same
          payload), agent.permission_flags becomes an empty direct delta.
        - If `preset_id` is explicitly cleared, permission_flags becomes
          ``None`` — the trusted Full Control sentinel.
        """
        agent = await self.get_agent(agent_id)
        if not agent:
            return None

        update_data = data.model_dump(exclude_unset=True)

        preset_id_in_payload = "preset_id" in update_data
        flags_in_payload = "permission_flags" in update_data

        for key, value in update_data.items():
            setattr(agent, key, value)

        if preset_id_in_payload and not flags_in_payload:
            new_preset_id = update_data.get("preset_id")
            if new_preset_id:
                agent.permission_flags = {}
            else:
                agent.permission_flags = None
            agent.mark_dirty("permission_flags")
        elif flags_in_payload:
            agent.mark_dirty("permission_flags")

        return agent

    async def regenerate_key(
        self, agent_id: str
    ) -> tuple[ApplicationRecord | None, str | None]:
        """Regenerate an agent's API key."""
        agent = await self.get_agent(agent_id)
        if not agent:
            return None, None

        reveal_once_secret = self.generate_api_key()
        key_hash = self.hash_api_key(reveal_once_secret)
        agent.api_key = self.credential_marker(key_hash)
        agent.api_key_hash = key_hash
        agent.mark_dirty("api_key")
        agent.mark_dirty("api_key_hash")
        await _application_flush(self.db)
        return agent, reveal_once_secret

    async def delete_agent(self, agent_id: str) -> bool:
        """Delete an agent."""
        agent = await self.get_agent(agent_id)
        if not agent:
            return False
        await _application_delete(self.db, agent)
        return True


@dataclass(frozen=True, slots=True)
class AttachmentDeletionReceipt:
    """Physical bytes retained until the relational delete is committed."""

    attachment_id: str
    path: str
    content: bytes


class AttachmentService:
    """Service for attachment operations."""

    def __init__(self, db: Any):
        self.db = db

    async def upload_attachment(
        self,
        card_id: str,
        user_id: str,
        filename: str,
        content: bytes,
        mime_type: str,
    ) -> ApplicationRecord | None:
        """Upload a file attachment."""
        # Verify card exists
        card = await _application_get(self.db, "card", card_id)
        if not card:
            return None
        require_card_operational_mutation_allowed(
            card,
            operation="upload_attachment",
        )

        # Delegate to the registered storage provider
        storage = get_storage_provider()
        file_path = await storage.save(card.board_id, filename, content)
        try:
            unique_name = Path(file_path).name
            attachment = _new_application_record(
                "attachment",
                card_id=card_id,
                filename=unique_name,
                original_filename=filename,
                mime_type=mime_type,
                size=len(content),
                path=file_path,
                uploaded_by=user_id,
            )
            await _application_add(self.db, attachment)
        except BaseException:
            # The object write precedes relational staging. If staging fails,
            # remove the unowned object before propagating the original error.
            await storage.delete(file_path)
            raise
        return attachment

    async def discard_uploaded_attachment(self, attachment: object) -> None:
        """Compensate a file save when the owning relational UoW rolls back."""

        path = str(getattr(attachment, "path"))
        await get_storage_provider().delete(path)

    async def get_attachment(self, attachment_id: str) -> ApplicationRecord | None:
        """Get an attachment by ID."""
        return await _application_get(self.db, "attachment", attachment_id)

    async def delete_attachment(
        self,
        attachment_id: str,
    ) -> AttachmentDeletionReceipt | bool:
        """Stage attachment deletion while retaining an exact restore receipt."""
        attachment = await self.get_attachment(attachment_id)
        if not attachment:
            return False
        card = await _application_get(self.db, "card", attachment.card_id)
        if card is not None:
            require_card_operational_mutation_allowed(
                card,
                operation="delete_attachment",
            )

        receipt = await self.delete_attachment_object(attachment)
        try:
            await _application_delete(self.db, attachment)
        except BaseException:
            await get_storage_provider().restore(receipt.path, receipt.content)
            raise
        return receipt

    @staticmethod
    async def delete_attachment_object(
        attachment: object,
    ) -> AttachmentDeletionReceipt:
        """Delete only physical bytes; the parent cascade owns the row delete."""

        attachment_id = str(getattr(attachment, "id"))
        path = str(getattr(attachment, "path"))
        storage = get_storage_provider()
        content = await storage.load(path)
        deleted = await storage.delete(path)
        if not deleted:
            raise RuntimeError(
                f"attachment object disappeared during delete: {attachment_id}"
            )
        return AttachmentDeletionReceipt(
            attachment_id=attachment_id,
            path=path,
            content=content,
        )

    async def restore_deleted_attachment(
        self,
        receipt: AttachmentDeletionReceipt,
    ) -> None:
        """Compensate physical deletion after a relational commit failure."""

        await get_storage_provider().restore(receipt.path, receipt.content)


class QAService:
    """Service for Q&A operations."""

    def __init__(self, db: Any):
        self.db = db

    async def get_question(self, qa_id: str) -> ApplicationRecord | None:
        """Get a card Q&A item by ID without mutating it."""
        return await _application_get(self.db, "qa_item", qa_id)

    async def create_question(
        self, card_id: str, user_id: str, data: QACreate
    ) -> ApplicationRecord | None:
        """Create a Q&A question."""
        card = await _application_get(self.db, "card", card_id)
        if not card:
            return None

        qa = _new_application_record(
            "qa_item",
            card_id=card_id,
            question=data.question,
            asked_by=user_id,
        )
        await _application_add(self.db, qa)
        return qa

    async def answer_question(
        self,
        qa_id: str,
        user_id: str,
        data: QAAnswer,
        *,
        actor_type: str = "user",
        surface: str = "service",
    ) -> ApplicationRecord | None:
        """Answer a Q&A question."""
        qa = await _application_get(self.db, "qa_item", qa_id)
        if not qa:
            return None

        card = await _application_get(self.db, "card", qa.card_id)
        board = (
            await _application_get(self.db, "board", card.board_id) if card else None
        )
        await _authorize_qa_answer_or_raise(
            self.db,
            board=board,
            qa=qa,
            user_id=user_id,
            entity_type="card",
            question_id=qa_id,
            card_id=card.id if card else None,
            actor_type=actor_type,
            surface=surface,
        )

        qa.answer = data.answer
        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        return qa

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await _application_get(self.db, "qa_item", qa_id)
        if not qa:
            return False
        await _application_delete(self.db, qa)
        return True


class CommentService:
    """Service for comment operations."""

    def __init__(self, db: Any):
        self.db = db

    async def get_comment(self, comment_id: str) -> ApplicationRecord | None:
        return await _application_get(self.db, "comment", comment_id)

    async def create_comment(
        self, card_id: str, user_id: str, data: CommentCreate
    ) -> ApplicationRecord | None:
        """Create a comment (text or choice board)."""
        card = await _application_get(self.db, "card", card_id)
        if not card:
            return None

        comment = _new_application_record(
            "comment",
            card_id=card_id,
            content=data.content,
            author_id=user_id,
            comment_type=data.comment_type or "text",
            choices=[c.model_dump() for c in data.choices] if data.choices else None,
            responses=[],
            allow_free_text=data.allow_free_text,
        )
        await _application_add(self.db, comment)
        return comment

    async def respond_to_choice(
        self,
        comment_id: str,
        responder_id: str,
        responder_name: str,
        selected: list[str],
        free_text: str | None = None,
    ) -> ApplicationRecord | None:
        """Add a response to a choice board comment."""
        comment = await _application_get(self.db, "comment", comment_id)
        if not comment or comment.comment_type == "text":
            return None

        selected = validate_choice_selection(
            comment.comment_type, selected, comment.choices
        )

        responses = list(comment.responses or [])
        # Replace existing response from same responder
        responses = [r for r in responses if r.get("responder_id") != responder_id]
        responses.append(
            {
                "responder_id": responder_id,
                "responder_name": responder_name,
                "selected": selected,
                "free_text": free_text,
            }
        )
        comment.responses = responses
        await _application_flush(self.db)
        return comment

    async def update_comment(
        self, comment_id: str, user_id: str, data: CommentUpdate
    ) -> ApplicationRecord | None:
        """Update a comment."""
        comment = await _application_get(self.db, "comment", comment_id)
        if not comment or comment.author_id != user_id:
            return None

        comment.content = data.content
        return comment

    async def delete_comment(self, comment_id: str, user_id: str) -> bool:
        """Delete a comment."""
        comment = await _application_get(self.db, "comment", comment_id)
        if not comment or comment.author_id != user_id:
            return False
        await _application_delete(self.db, comment)
        return True


async def _validate_spec_linked_refs(
    db: Any,
    current_spec: Any,
    update_data: dict[str, Any],
) -> None:
    """Reject orphan references in linked_* fields before they hit the DB.

    Computes the *final* state of each spec collection (incoming value when
    the field is in `update_data`, otherwise the current persisted value)
    and validates that every `linked_*` reference points to an existing
    target:

    - linked_criteria (test_scenarios → AC):
        Must be a 0-based string index "0".."N-1" OR the exact AC text.
        AC labels like "AC1" are rejected — the SpecModal coverage widget
        does not recognise them and they would silently appear uncovered.

    - linked_requirements:
        business_rules → FR; api_contracts + IR + OR → FR/TR.
        Same rule — index "0".."N-1" OR exact requirement text/id.
        Labels like "FR1" are rejected.

    - linked_rules (api_contracts → BR):
        Must match an existing business_rule.id in the same spec.

    - linked_api_contracts (IR → API contract):
        Must match an existing api_contract.id in the same spec.

    - linked_integration_requirements (OR → IR):
        Must match an existing integration_requirement.id in the same spec.

    - linked_task_ids (test_scenarios + business_rules + api_contracts +
      IR + OR + structured_trs → Card):
        Each id must resolve to an existing Card row in the DB.

    Raises ValueError with all offenders enumerated so the caller can fix
    them in one round-trip instead of one-by-one.
    """

    def _final(field: str, default: Any):
        if field in update_data:
            return update_data[field] if update_data[field] is not None else default
        return getattr(current_spec, field, None) or default

    def _child_text(item: Any) -> str:
        if isinstance(item, dict):
            return str(
                item.get("text") or item.get("title") or item.get("description") or ""
            )
        return str(item)

    def _child_id(item: Any) -> str | None:
        if isinstance(item, dict):
            raw = item.get("id")
            return str(raw) if raw not in (None, "") else None
        return None

    final_frs_raw: list[Any] = list(_final("functional_requirements", []) or [])
    final_acs_raw: list[Any] = list(_final("acceptance_criteria", []) or [])
    final_frs: list[str] = [_child_text(item) for item in final_frs_raw]
    final_acs: list[str] = [_child_text(item) for item in final_acs_raw]
    final_brs: list[dict] = [
        b if isinstance(b, dict) else b.model_dump()
        for b in (_final("business_rules", []) or [])
    ]
    final_contracts: list[dict] = [
        c if isinstance(c, dict) else c.model_dump()
        for c in (_final("api_contracts", []) or [])
    ]
    final_irs: list[dict] = [
        ir if isinstance(ir, dict) else ir.model_dump()
        for ir in (_final("integration_requirements", []) or [])
    ]
    final_ors: list[dict] = [
        req if isinstance(req, dict) else req.model_dump()
        for req in (_final("observability_requirements", []) or [])
    ]
    final_scenarios: list[dict] = [
        s if isinstance(s, dict) else s.model_dump()
        for s in (_final("test_scenarios", []) or [])
    ]
    final_decisions: list[dict] = [
        d if isinstance(d, dict) else d.model_dump()
        for d in (_final("decisions", []) or [])
    ]
    final_trs_raw: list = list(_final("technical_requirements", []) or [])
    final_trs_structured: list[dict] = []
    for tr in final_trs_raw:
        if isinstance(tr, dict) and tr.get("id"):
            final_trs_structured.append(tr)
        elif hasattr(tr, "model_dump") and getattr(tr, "id", None):
            final_trs_structured.append(tr.model_dump())

    valid_fr_indices = {str(i) for i in range(len(final_frs))}
    valid_ac_indices = {str(i) for i in range(len(final_acs))}
    valid_fr_texts = {text for text in final_frs if text}
    valid_ac_texts = {text for text in final_acs if text}
    valid_fr_ids = {child_id for item in final_frs_raw if (child_id := _child_id(item))}
    valid_ac_ids = {child_id for item in final_acs_raw if (child_id := _child_id(item))}
    valid_tr_texts = {
        _child_text(item) for item in final_trs_structured if _child_text(item)
    }
    valid_tr_ids = {
        child_id for item in final_trs_structured if (child_id := _child_id(item))
    }
    valid_br_ids = {br.get("id") for br in final_brs if br.get("id")}
    valid_contract_ids = {ct.get("id") for ct in final_contracts if ct.get("id")}
    valid_ir_ids = {ir.get("id") for ir in final_irs if ir.get("id")}

    errors: list[str] = []

    _DIM_TARGET = {"requirements": "FR", "criteria": "AC"}

    def _check_index_text_or_id(
        refs: list[str],
        valid_indices: set,
        valid_texts: set,
        valid_ids: set,
        dim: str,
        owner_label: str,
        target_label: str | None = None,
    ):
        target = target_label or _DIM_TARGET.get(dim, dim.upper()[:2])
        for ref in refs or []:
            ref_str = str(ref)
            if (
                ref_str in valid_indices
                or ref_str in valid_texts
                or ref_str in valid_ids
            ):
                continue
            max_idx = max(0, len(valid_indices) - 1)
            errors.append(
                f"{owner_label}: linked_{dim} reference '{ref_str}' is not a valid 0-based index "
                f"(0..{max_idx}), existing {target} text, or structured {target} id."
            )

    # business_rules.linked_requirements → FR
    for br in final_brs:
        owner = f"BR '{br.get('id') or br.get('title') or '?'}'"
        _check_index_text_or_id(
            br.get("linked_requirements") or [],
            valid_fr_indices,
            valid_fr_texts,
            valid_fr_ids,
            "requirements",
            owner,
        )

    # api_contracts.linked_requirements → FR
    # api_contracts.linked_rules → BR.id
    for ct in final_contracts:
        owner = f"Contract '{ct.get('id') or (ct.get('method', '?') + ' ' + ct.get('path', '?'))}'"
        _check_index_text_or_id(
            ct.get("linked_requirements") or [],
            valid_fr_indices,
            valid_fr_texts | valid_tr_texts,
            valid_fr_ids | valid_tr_ids,
            "requirements",
            owner,
            "FR/TR",
        )
        for ref in ct.get("linked_rules") or []:
            if str(ref) not in valid_br_ids:
                errors.append(
                    f"{owner}: linked_rules reference '{ref}' does not match any business_rule.id "
                    f"in the spec (valid: {sorted(valid_br_ids) or 'none'})."
                )

    # integration_requirements.linked_requirements → FR/TR
    # integration_requirements.linked_api_contracts → api_contract.id
    for ir in final_irs:
        owner = f"IR '{ir.get('id') or ir.get('title') or '?'}'"
        _check_index_text_or_id(
            ir.get("linked_requirements") or [],
            valid_fr_indices,
            valid_fr_texts | valid_tr_texts,
            valid_fr_ids | valid_tr_ids,
            "requirements",
            owner,
            "FR/TR",
        )
        for ref in ir.get("linked_api_contracts") or []:
            if str(ref) not in valid_contract_ids:
                errors.append(
                    f"{owner}: linked_api_contracts reference '{ref}' does not match any api_contract.id "
                    f"in the spec (valid: {sorted(valid_contract_ids) or 'none'})."
                )

    # observability_requirements.linked_requirements → FR/TR
    # observability_requirements.linked_integration_requirements → IR.id
    for req in final_ors:
        owner = f"OR '{req.get('id') or req.get('title') or '?'}'"
        _check_index_text_or_id(
            req.get("linked_requirements") or [],
            valid_fr_indices,
            valid_fr_texts | valid_tr_texts,
            valid_fr_ids | valid_tr_ids,
            "requirements",
            owner,
            "FR/TR",
        )
        for ref in req.get("linked_integration_requirements") or []:
            if str(ref) not in valid_ir_ids:
                errors.append(
                    f"{owner}: linked_integration_requirements reference '{ref}' does not match any integration_requirement.id "
                    f"in the spec (valid: {sorted(valid_ir_ids) or 'none'})."
                )

    # test_scenarios.linked_criteria → AC
    for sc in final_scenarios:
        owner = f"Scenario '{sc.get('id') or sc.get('title') or '?'}'"
        _check_index_text_or_id(
            sc.get("linked_criteria") or [],
            valid_ac_indices,
            valid_ac_texts,
            valid_ac_ids,
            "criteria",
            owner,
        )

    # decisions.linked_requirements → FR/TR  +  supersedes_decision_id → Decision.id
    valid_decision_ids = {d.get("id") for d in final_decisions if d.get("id")}
    for dec in final_decisions:
        owner = f"Decision '{dec.get('id') or dec.get('title') or '?'}'"
        _check_index_text_or_id(
            dec.get("linked_requirements") or [],
            valid_fr_indices,
            valid_fr_texts | valid_tr_texts,
            valid_fr_ids | valid_tr_ids,
            "requirements",
            owner,
            "FR/TR",
        )
        sup = dec.get("supersedes_decision_id")
        if sup and sup not in valid_decision_ids:
            errors.append(
                f"{owner}: supersedes_decision_id '{sup}' does not match any decision.id "
                f"in the spec (valid: {sorted(valid_decision_ids) or 'none'})."
            )

    # linked_task_ids → Card.id (DB existence check). Collect all in one batch.
    all_task_ids: set[str] = set()
    task_owners: dict[str, list[str]] = {}
    for sc in final_scenarios:
        owner = f"Scenario '{sc.get('id') or sc.get('title') or '?'}'"
        for tid in sc.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for idx, fr in enumerate(final_frs_raw):
        if not isinstance(fr, dict):
            continue
        owner = f"FR '{fr.get('id') or fr.get('text') or idx}'"
        for tid in fr.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for br in final_brs:
        owner = f"BR '{br.get('id') or br.get('title') or '?'}'"
        for tid in br.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for ct in final_contracts:
        owner = f"Contract '{ct.get('id') or '?'}'"
        for tid in ct.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for ir in final_irs:
        owner = f"IR '{ir.get('id') or ir.get('title') or '?'}'"
        for tid in ir.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for req in final_ors:
        owner = f"OR '{req.get('id') or req.get('title') or '?'}'"
        for tid in req.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for tr in final_trs_structured:
        owner = f"TR '{tr.get('id')}'"
        for tid in tr.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for dec in final_decisions:
        owner = f"Decision '{dec.get('id') or dec.get('title') or '?'}'"
        for tid in dec.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)

    if all_task_ids:
        existing_ids: set[str] = set()
        cards = await _application_list(
            db,
            "card",
            filters=(_apf("id", "in", all_task_ids),),
        )
        existing_ids.update(card.id for card in cards)
        for missing in all_task_ids - existing_ids:
            owners = ", ".join(task_owners.get(missing, []))
            errors.append(
                f"linked_task_ids reference card '{missing}' that does not exist in the database. "
                f"Referenced by: {owners}."
            )

    if errors:
        joined = "; ".join(errors[:10])
        more = f" (and {len(errors) - 10} more)" if len(errors) > 10 else ""
        raise ValueError(
            f"Cannot update spec: {len(errors)} orphan link reference(s) found. {joined}{more}. "
            f'Use 0-based string indices ("0", "1", ...) for FR/AC; '
            f"TR id/text is accepted for API contracts, IR/OR, and decisions; the BR.id for linked_rules, "
            f"the api_contract.id / integration_requirement.id for cross-resource links, "
            f"and an existing Card.id for linked_task_ids."
        )


class SpecLineagePreflightError(ValueError):
    """Stable pre-mutation error for invalid Spec parent lifecycle lineage."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        facts: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.facts = facts or {}

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.facts:
            payload["facts"] = self.facts
        return payload

    def to_error_dict(self) -> dict[str, Any]:
        """Return the stable error envelope expected by MCP create adapters."""
        return {"error": self.code, **self.to_dict()}


class SpecService:
    """Service for spec operations."""

    def __init__(
        self,
        db: Any,
        *,
        knowledge_propagation_port: KnowledgePropagationPort | None = None,
    ):
        self.db = db
        self._knowledge_propagation_port = knowledge_propagation_port
        self._cognitive_closeout_gate_factory: Callable[[], Any] = (
            _build_default_cognitive_closeout_gate
        )
        self._cognitive_readiness_service_factory: Callable[[], Any] = (
            _build_default_cognitive_readiness_service
        )

    async def _validate_test_scenario_subject_identities(
        self,
        *,
        board_id: str,
        scenarios: list[dict[str, Any]],
        current_spec_id: str | None = None,
    ) -> None:
        """Enforce the board-scoped identity used by Policy Compliance.

        A test scenario is a first-class policy subject, so its public id must
        identify exactly one scenario inside a board.  Keep this invariant at
        the aggregate write boundary (including import/create and whole-list
        updates) instead of letting the transition resolver pick an arbitrary
        duplicate later.
        """

        candidate_ids = [
            str(item.get("id")).strip()
            for item in scenarios
            if isinstance(item, dict)
            and item.get("id") is not None
            and str(item.get("id")).strip()
        ]
        duplicate_ids = sorted(
            {
                scenario_id
                for scenario_id in candidate_ids
                if candidate_ids.count(scenario_id) > 1
            }
        )
        if duplicate_ids:
            raise ValueError(
                "test_scenario_identity_conflict: duplicate scenario id(s) "
                f"inside the spec: {', '.join(duplicate_ids)}"
            )
        if not candidate_ids:
            return

        candidate_set = set(candidate_ids)
        board_specs = await _application_list(
            self.db,
            "spec",
            filters=(_apf("board_id", "eq", board_id),),
        )
        conflicts: set[str] = set()
        for existing_spec in board_specs:
            if current_spec_id is not None and existing_spec.id == current_spec_id:
                continue
            for existing in existing_spec.test_scenarios or ():
                if not isinstance(existing, dict):
                    continue
                existing_id = existing.get("id")
                if existing_id is not None and str(existing_id) in candidate_set:
                    conflicts.add(str(existing_id))
        if conflicts:
            raise ValueError(
                "test_scenario_identity_conflict: scenario id(s) already exist "
                f"in this board: {', '.join(sorted(conflicts))}"
            )

    async def _validate_lineage(
        self,
        board_id: str,
        *,
        ideation_id: str | None,
        refinement_id: str | None,
    ) -> None:
        """Validate explicit parent lifecycle lineage before any Spec mutation.

        This is the single parent-lifecycle predicate used by direct create,
        relink, and both authoritative ``derive_spec`` workflows.
        """
        if not ideation_id and not refinement_id:
            return

        ideation = None
        refinement = None
        if ideation_id:
            ideation = await _application_get(self.db, "ideation", ideation_id)
            if ideation is None:
                raise SpecLineagePreflightError(
                    "spec_ideation_not_found",
                    "The requested parent ideation does not exist.",
                    facts={"board_id": board_id, "ideation_id": ideation_id},
                )
            if ideation.board_id != board_id:
                raise SpecLineagePreflightError(
                    "spec_ideation_board_mismatch",
                    "The requested parent ideation belongs to another board.",
                    facts={
                        "board_id": board_id,
                        "ideation_id": ideation_id,
                        "parent_board_id": ideation.board_id,
                    },
                )

        if refinement_id:
            refinement = await _application_get(
                self.db,
                "refinement",
                refinement_id,
            )
            if refinement is None:
                raise SpecLineagePreflightError(
                    "spec_refinement_not_found",
                    "The requested parent refinement does not exist.",
                    facts={"board_id": board_id, "refinement_id": refinement_id},
                )
            if refinement.board_id != board_id:
                raise SpecLineagePreflightError(
                    "spec_refinement_board_mismatch",
                    "The requested parent refinement belongs to another board.",
                    facts={
                        "board_id": board_id,
                        "refinement_id": refinement_id,
                        "parent_board_id": refinement.board_id,
                    },
                )
            ancestor_ideation_id = getattr(refinement, "ideation_id", None)
            if ideation is not None:
                if ancestor_ideation_id != ideation.id:
                    raise SpecLineagePreflightError(
                        "spec_parent_lineage_mismatch",
                        (
                            "The requested refinement does not belong to the "
                            "requested ideation."
                        ),
                        facts={
                            "ideation_id": ideation.id,
                            "refinement_id": refinement.id,
                            "refinement_ideation_id": ancestor_ideation_id,
                        },
                    )
            else:
                # A refinement-only request still carries an implicit ideation
                # parent. Resolve that ancestor explicitly so direct create and
                # relink callers cannot bypass the lifecycle checks enforced by
                # the authoritative derive flow, which supplies both ids.
                if ancestor_ideation_id:
                    ideation = await _application_get(
                        self.db,
                        "ideation",
                        ancestor_ideation_id,
                    )
                if ideation is None:
                    raise SpecLineagePreflightError(
                        "spec_ideation_not_found",
                        ("The requested refinement's parent ideation does not exist."),
                        facts={
                            "board_id": board_id,
                            "ideation_id": ancestor_ideation_id,
                            "refinement_id": refinement.id,
                        },
                    )
                if ideation.board_id != board_id:
                    raise SpecLineagePreflightError(
                        "spec_ideation_board_mismatch",
                        (
                            "The requested refinement's parent ideation "
                            "belongs to another board."
                        ),
                        facts={
                            "board_id": board_id,
                            "ideation_id": ideation.id,
                            "refinement_id": refinement.id,
                            "parent_board_id": ideation.board_id,
                        },
                    )

        if ideation is not None and ideation.status != IdeationStatus.DONE:
            raise SpecLineagePreflightError(
                "spec_ideation_not_done",
                "A Spec can only be created from an ideation in status 'done'.",
                facts={
                    "ideation_id": ideation.id,
                    "ideation_status": getattr(
                        ideation.status,
                        "value",
                        str(ideation.status),
                    ),
                },
            )

        if refinement is not None and refinement.status != RefinementStatus.DONE:
            raise SpecLineagePreflightError(
                "spec_refinement_not_done",
                "A Spec can only be created from a refinement in status 'done'.",
                facts={
                    "refinement_id": refinement.id,
                    "refinement_status": getattr(
                        refinement.status,
                        "value",
                        str(refinement.status),
                    ),
                },
            )

        complexity = (
            getattr(ideation.complexity, "value", ideation.complexity)
            if ideation is not None
            else None
        )
        if (
            complexity
            in {
                IdeationComplexity.MEDIUM.value,
                IdeationComplexity.LARGE.value,
            }
            and refinement is None
        ):
            raise SpecLineagePreflightError(
                "spec_refinement_required",
                (
                    "Ideations with complexity 'medium' or 'large' require a "
                    "completed refinement before Spec creation."
                ),
                facts={
                    "ideation_id": ideation.id,
                    "ideation_complexity": complexity,
                },
            )

    async def _validate_create_lineage(
        self,
        board_id: str,
        data: SpecCreate,
    ) -> None:
        """Validate explicit lineage supplied by a Spec create request."""

        await self._validate_lineage(
            board_id,
            ideation_id=data.ideation_id,
            refinement_id=data.refinement_id,
        )

    async def _validate_cognitive_done(
        self,
        spec: ApplicationRecord,
        board: ApplicationRecord | None = None,
        *,
        read_only_preview: bool = False,
    ) -> None:
        if board is None:
            board = await _application_get(self.db, "board", spec.board_id)
        await _evaluate_entity_cognitive_done_or_raise(
            db=self.db,
            gate_factory=self._cognitive_closeout_gate_factory,
            readiness_service_factory=self._cognitive_readiness_service_factory,
            board=board,
            board_id=spec.board_id,
            entity_type="spec",
            entity_id=spec.id,
            entity=spec,
            target_label="spec",
            resolve_graph_state=True,
        )

    # ---- Status progression order ----
    _STATUS_ORDER = {
        SpecStatus.DRAFT: 0,
        SpecStatus.REVIEW: 1,
        SpecStatus.APPROVED: 2,
        SpecStatus.VALIDATED: 3,
        SpecStatus.IN_PROGRESS: 4,
        SpecStatus.DONE: 5,
        SpecStatus.CANCELLED: 5,
    }

    async def _record_history(
        self,
        spec_id: str,
        action: str,
        actor_id: str,
        actor_name: str,
        actor_type: str = "user",
        changes: list[dict] | None = None,
        summary: str | None = None,
        version: int | None = None,
    ) -> None:
        """Record a history entry for a spec."""
        entry = _new_application_record(
            "spec_history",
            spec_id=spec_id,
            action=action,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            changes=changes,
            summary=summary,
            version=version,
        )
        await _application_add(self.db, entry)

    async def list_history(
        self,
        spec_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> list[ApplicationRecord]:
        """List history entries for a spec, newest first."""
        window = validate_history_window(limit, offset)
        return await _application_list(
            self.db,
            "spec_history",
            filters=(_apf("spec_id", "eq", spec_id),),
            order_by=(("created_at", True), ("id", True)),
            offset=window.offset,
            limit=window.limit,
        )

    async def count_history(self, spec_id: str) -> int:
        """Count every history entry for a spec independently of a page."""
        return await _application_count(
            self.db,
            "spec_history",
            filters=(_apf("spec_id", "eq", spec_id),),
        )

    @staticmethod
    def _compute_diff(old_data: dict, new_data: dict, fields: list[str]) -> list[dict]:
        """Compute field-level diffs between old and new data."""
        changes = []
        for field in fields:
            old_val = old_data.get(field)
            new_val = new_data.get(field)
            # Normalize enum values
            if hasattr(old_val, "value"):
                old_val = old_val.value
            if hasattr(new_val, "value"):
                new_val = new_val.value
            if old_val != new_val:
                changes.append({"field": field, "old": old_val, "new": new_val})
        return changes

    async def create_spec(
        self,
        board_id: str,
        user_id: str,
        data: SpecCreate,
        skip_ownership_check: bool = False,
        *,
        query_scope: QueryScope | None = None,
        target_id: str | None = None,
        knowledge_propagation_v2: bool = False,
    ) -> ApplicationRecord | None:
        """Create a new spec in a board."""
        if (target_id is None) != (not knowledge_propagation_v2):
            raise ValueError(
                "knowledge_propagation_v2 requires an explicit deterministic target_id"
            )
        require_ownership = (
            query_scope.require_ownership
            if query_scope is not None
            else not skip_ownership_check
        )
        board_query = _board_scope_select(
            board_id=board_id,
            user_id=user_id,
            query_scope=None if skip_ownership_check else query_scope,
            require_ownership=require_ownership,
        )
        if board_query is None:
            return None
        if not await _application_run(self.db, board_query):
            return None

        await self._validate_create_lineage(board_id, data)

        # Fail-closed scenario_type (spec ac16b3c9): every scenario in a NEW spec
        # is a new write — reject an unsupported type before insert/flush, never
        # normalize.
        if data.test_scenarios:
            initial_scenarios = [s.model_dump() for s in data.test_scenarios]
            validate_scenario_types_for_write(
                initial_scenarios,
                None,
            )
            for scenario in initial_scenarios:
                initial_status = str(scenario.get("status") or "draft")
                if initial_status in GATED_STATUSES:
                    raise ValueError(
                        "test_scenario_status_requires_scoped_update: "
                        f"new scenario {scenario.get('id') or '(new)'} cannot "
                        f"be created as '{initial_status}' with its parent spec; "
                        "create it as draft/ready, then use "
                        "set_test_scenario_status."
                    )
            await self._validate_test_scenario_subject_identities(
                board_id=board_id,
                scenarios=initial_scenarios,
            )

        canonical_requirements = canonicalize_spec_requirement_fields(
            {
                "functional_requirements": data.functional_requirements,
                "technical_requirements": data.technical_requirements,
                "acceptance_criteria": data.acceptance_criteria,
            }
        )
        spec = _new_application_record(
            "spec",
            **({"id": target_id} if target_id is not None else {}),
            board_id=board_id,
            title=data.title,
            description=data.description,
            context=data.context,
            functional_requirements=canonical_requirements["functional_requirements"],
            technical_requirements=canonical_requirements["technical_requirements"],
            acceptance_criteria=canonical_requirements["acceptance_criteria"],
            test_scenarios=[s.model_dump() for s in data.test_scenarios]
            if data.test_scenarios
            else None,
            screen_mockups=None,  # assigned after the Design System gate (below)
            business_rules=[r.model_dump() for r in data.business_rules]
            if data.business_rules
            else None,
            api_contracts=[c.model_dump() for c in data.api_contracts]
            if data.api_contracts
            else None,
            integration_requirements=[
                ir.model_dump() for ir in data.integration_requirements
            ]
            if data.integration_requirements
            else None,
            observability_requirements=[
                req.model_dump() for req in data.observability_requirements
            ]
            if data.observability_requirements
            else None,
            decisions=[d.model_dump() for d in data.decisions]
            if data.decisions
            else None,
            status=data.status,
            edition=1,
            assignee_id=data.assignee_id,
            created_by=user_id,
            labels=data.labels,
            ideation_id=data.ideation_id,
            refinement_id=data.refinement_id,
        )
        # MockupDesignSystemGate (spec 3a006f65 / card 0192f58d): gate mockups submitted
        # at creation BEFORE persistence — the create twin of the update_spec gate. The
        # baseline is the entity's (empty) mockups, so every submitted screen is
        # evaluated; assign only if the gate does not raise.
        _submitted_mockups = (
            [s.model_dump() for s in data.screen_mockups]
            if data.screen_mockups
            else None
        )
        if _submitted_mockups:
            from okto_pulse.core.services.design_system import (
                gate_entity_screen_mockups,
            )

            await gate_entity_screen_mockups(
                self.db, spec, _submitted_mockups, entity_type="spec"
            )
            spec.screen_mockups = _submitted_mockups
        await _application_add(
            self.db,
            spec,
            conflict_error=(
                ApplicationRecordConflictError("spec", spec.id)
                if knowledge_propagation_v2
                else None
            ),
        )

        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import SpecCreated

        spec_source: str = "manual"
        origin_id: str | None = None
        if data.refinement_id:
            spec_source = "derived_refinement"
            origin_id = data.refinement_id
        elif data.ideation_id:
            spec_source = "derived_ideation"
            origin_id = data.ideation_id

        await event_publish(
            SpecCreated(
                board_id=board_id,
                actor_id=user_id,
                spec_id=spec.id,
                source=spec_source,
                origin_id=origin_id,
            ),
            session=self.db,
        )

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="spec_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "title": data.title,
                "spec_id": spec.id,
                "edition": 1,
                "technical_revision": 1,
            },
        )
        await self._record_history(
            spec_id=spec.id,
            action="created",
            actor_id=user_id,
            actor_name=actor_name,
            summary=f"Spec created: {data.title}",
            version=1,
            changes=[
                {"field": "title", "old": None, "new": data.title},
                {"field": "status", "old": None, "new": data.status.value},
                *(
                    [
                        {
                            "field": "functional_requirements",
                            "old": None,
                            "new": data.functional_requirements,
                        }
                    ]
                    if data.functional_requirements
                    else []
                ),
                *(
                    [
                        {
                            "field": "technical_requirements",
                            "old": None,
                            "new": data.technical_requirements,
                        }
                    ]
                    if data.technical_requirements
                    else []
                ),
                *(
                    [
                        {
                            "field": "acceptance_criteria",
                            "old": None,
                            "new": data.acceptance_criteria,
                        }
                    ]
                    if data.acceptance_criteria
                    else []
                ),
                *(
                    [
                        {
                            "field": "integration_requirements",
                            "old": None,
                            "new": [
                                ir.model_dump() for ir in data.integration_requirements
                            ],
                        }
                    ]
                    if data.integration_requirements
                    else []
                ),
                *(
                    [
                        {
                            "field": "observability_requirements",
                            "old": None,
                            "new": [
                                req.model_dump()
                                for req in data.observability_requirements
                            ],
                        }
                    ]
                    if data.observability_requirements
                    else []
                ),
            ],
        )
        return spec

    async def get_spec(self, spec_id: str) -> ApplicationRecord | None:
        """Get a spec by ID with its cards and knowledge bases."""
        return await _application_get(
            self.db,
            "spec",
            spec_id,
            includes=("cards", "knowledge_bases", "qa_items", "architecture_designs"),
        )

    async def list_specs(
        self,
        board_id: str,
        status_filter: str | None = None,
        include_archived: bool = False,
        *,
        query_scope: QueryScope | None = None,
    ) -> list[ApplicationRecord]:
        """List specs for a board, optionally filtered by status."""
        if query_scope is not None and (
            query_scope.target_board_id != board_id
            or not query_scope.allows_board_id(board_id)
        ):
            return []
        filters = [_apf("board_id", "eq", board_id)]
        if status_filter:
            filters.append(_apf("status", "eq", SpecStatus(status_filter)))
        if not include_archived:
            filters.append(_apf("archived", "is_false"))
        rows = await _application_list(
            self.db,
            "spec",
            filters=tuple(filters),
            order_by=(("updated_at", True),),
            includes=("architecture_designs",),
        )
        await _attach_open_qa_counts(self.db, rows, "spec_qa_item", "spec_id")
        return rows

    async def update_test_scenario(
        self,
        spec_id: str,
        user_id: str,
        scenario_id: str,
        *,
        title: str | None = None,
        given: str | None = None,
        when: str | None = None,
        then: str | None = None,
        scenario_type: str | None = None,
        linked_criteria: list[str] | None = None,
        notes: str | None = None,
        clear: list[str] | None = None,
    ) -> dict:
        """Edit the BODY of a test scenario (spec 6f1e75bf, FR2/FR5).

        ``None`` means "leave unchanged"; a non-None value sets the field.
        ``clear`` lists field names (``notes``/``linked_criteria``) to reset to
        empty — this is how a caller distinguishes "omitted" from "cleared".
        ``status`` is NOT accepted (that stays exclusive to the status path so no
        second NC-9 bypass is created). Editing any SEMANTIC field
        (given/when/then/scenario_type/linked_criteria) of a scenario that holds
        evidence invalidates it (status→ready, evidence dropped); cosmetic edits
        (title/notes) preserve status and evidence. Respects the content-lock.
        """
        await _require_spec_unlocked(self.db, spec_id)
        spec = await self.get_spec(spec_id)
        if not spec:
            raise ValueError("scenario_not_found: spec not found")

        scenarios = [
            dict(s) for s in (spec.test_scenarios or []) if isinstance(s, dict)
        ]
        target = next((s for s in scenarios if s.get("id") == scenario_id), None)
        if target is None:
            raise ValueError(f"scenario_not_found: {scenario_id}")

        # Fail-closed scenario_type (spec ac16b3c9): an explicit new value on the
        # body-edit path must be a supported type — reject before mutation, never
        # normalize. ``None`` means "leave unchanged" and is not validated.
        if scenario_type is not None:
            validate_scenario_type(scenario_type)

        clearable = {"notes", "linked_criteria"}
        clear_set = set(clear or [])
        bad_clear = clear_set - clearable
        if bad_clear:
            raise ValueError(
                f"clear only supports {sorted(clearable)}; got {sorted(bad_clear)}"
            )

        changed_fields: list[str] = []

        # Resolve linked_criteria against the spec's ACs (reuse #2 resolver,
        # fail-closed on unresolved tokens).
        if linked_criteria is not None:
            resolved, unresolved = resolve_linked_criteria_to_ids(
                linked_criteria, list(spec.acceptance_criteria or [])
            )
            if unresolved:
                raise ValueError(f"unresolved_criteria: {', '.join(unresolved)}")
            if target.get("linked_criteria") != resolved:
                target["linked_criteria"] = resolved
                changed_fields.append("linked_criteria")

        for field, value in (
            ("title", title),
            ("given", given),
            ("when", when),
            ("then", then),
            ("scenario_type", scenario_type),
            ("notes", notes),
        ):
            if value is not None and target.get(field) != value:
                target[field] = value
                changed_fields.append(field)

        # Explicit clears (distinguish "omitted" from "emptied").
        for field in clear_set:
            empty: object = [] if field == "linked_criteria" else ""
            if target.get(field) != empty:
                target[field] = empty
                if field not in changed_fields:
                    changed_fields.append(field)

        # Evidence invalidation on semantic edit (spec FR5/BR6): if a SEMANTIC
        # field changed and the scenario currently holds evidence, the old
        # evidence no longer proves the new behaviour — reset to ready + drop it.
        evidence_invalidated = False
        if evidence_invalidated_by_semantic_edit(changed_fields) and (
            target.get("evidence") or target.get("latest_evidence")
        ):
            target["status"] = "ready"
            target["evidence"] = None
            target.pop("latest_evidence", None)
            evidence_invalidated = True

        if not changed_fields:
            return {
                "scenario_id": scenario_id,
                "updated_fields": [],
                "evidence_invalidated": False,
                "scenario": target,
            }

        updated = await self.update_spec(
            spec_id,
            user_id,
            PersistedTestScenarioSpecUpdate.from_iterable(scenarios),
        )
        new_target = next(
            (
                s
                for s in (updated.test_scenarios or [])
                if isinstance(s, dict) and s.get("id") == scenario_id
            ),
            target,
        )
        await _application_add(
            self.db,
            _new_application_record(
                "activity_log",
                board_id=spec.board_id,
                action="test_scenario_body_changed",
                actor_type="agent",
                actor_id=user_id,
                actor_name=user_id,
                details={
                    "spec_id": spec_id,
                    "scenario_id": scenario_id,
                    "updated_fields": changed_fields,
                    "evidence_invalidated": evidence_invalidated,
                },
            ),
        )
        await _application_commit(self.db)
        logging.getLogger("okto_pulse.spec.test_scenario").info(
            "test_scenario.body_changed scenario=%s spec=%s fields=%s invalidated=%s",
            scenario_id,
            spec_id,
            changed_fields,
            evidence_invalidated,
            extra={
                "event": "test_scenario.body_changed",
                "scenario_id": scenario_id,
                "spec_id": spec_id,
                "board_id": spec.board_id,
                "actor_id": user_id,
                "updated_fields": changed_fields,
                "evidence_invalidated": evidence_invalidated,
            },
        )
        return {
            "scenario_id": scenario_id,
            "updated_fields": changed_fields,
            "evidence_invalidated": evidence_invalidated,
            "scenario": new_target,
        }

    async def delete_test_scenario(
        self, spec_id: str, user_id: str, scenario_id: str
    ) -> dict:
        """Delete a test scenario and clean ``Card.test_scenario_ids`` in cascade
        (spec 6f1e75bf, FR3/BR4).

        Atomic: the spec's ``test_scenarios`` and every referencing card are
        mutated in a single transaction (all-or-nothing). Does not block on
        existing links — the cascade removes them. Respects the content-lock.
        """
        await _require_spec_unlocked(self.db, spec_id)
        spec = await _application_get(self.db, "spec", spec_id)
        if not spec:
            raise ValueError("scenario_not_found: spec not found")

        scenarios = [s for s in (spec.test_scenarios or []) if isinstance(s, dict)]
        remaining = [s for s in scenarios if s.get("id") != scenario_id]
        if len(remaining) == len(scenarios):
            raise ValueError(f"scenario_not_found: {scenario_id}")

        # Preflight every affected card before update_spec stages the first
        # mutation. Removing a scenario also rewrites Card traceability and is
        # forbidden while any referencing card is in the Rejected handoff.
        cards = await _application_list(
            self.db,
            "card",
            filters=(_apf("spec_id", "eq", spec_id),),
        )
        referencing_cards = [
            card
            for card in cards
            if scenario_id in (getattr(card, "test_scenario_ids", None) or [])
        ]
        for card in referencing_cards:
            require_card_operational_mutation_allowed(
                card,
                operation="delete_linked_test_scenario",
            )

        updated_spec = await self.update_spec(
            spec_id,
            user_id,
            PersistedTestScenarioSpecUpdate.from_iterable(remaining),
        )
        if updated_spec is None:  # defensive: the Spec was resolved above
            raise ValueError("scenario_not_found: spec not found")
        spec = updated_spec

        # Cascade: drop the scenario id from every card that references it, in
        # the SAME transaction → all-or-nothing, no orphan in Card.test_scenario_ids.
        cards_unlinked: list[str] = []
        for card in referencing_cards:
            ids = list(card.test_scenario_ids or [])
            card.test_scenario_ids = [i for i in ids if i != scenario_id]
            card.mark_dirty("test_scenario_ids")
            cards_unlinked.append(card.id)

        await _application_add(
            self.db,
            _new_application_record(
                "activity_log",
                board_id=spec.board_id,
                action="test_scenario_deleted",
                actor_type="agent",
                actor_id=user_id,
                actor_name=user_id,
                details={
                    "spec_id": spec_id,
                    "scenario_id": scenario_id,
                    "cards_unlinked": cards_unlinked,
                },
            ),
        )
        await _application_commit(self.db)

        logging.getLogger("okto_pulse.spec.test_scenario").info(
            "test_scenario.deleted scenario=%s spec=%s cards_unlinked=%s",
            scenario_id,
            spec_id,
            len(cards_unlinked),
            extra={
                "event": "test_scenario.deleted",
                "scenario_id": scenario_id,
                "spec_id": spec_id,
                "board_id": spec.board_id,
                "actor_id": user_id,
                "cards_unlinked": cards_unlinked,
            },
        )
        return {"scenario_id": scenario_id, "cards_unlinked": cards_unlinked}

    async def set_test_scenario_status(
        self,
        spec_id: str,
        user_id: str,
        scenario_id: str,
        status: str,
        evidence: dict | None = None,
    ) -> dict:
        """Scoped operational status mutation for ONE test scenario (spec
        6f1e75bf, FR4/FR6) — the single helper shared by the MCP status tool and
        the REST status endpoint.

        - Guards by spec STATUS (require_test_scenario_status_mutable): blocks
          arbitrary ``validated``/``done`` status edits, permits
          ``in_progress``. Does NOT use the content-lock (which would wrongly
          block in_progress). Narrow exception: a ``validated``/``done`` spec
          may receive operational evidence/status for a scenario that is already
          linked to an executable test card.
        - Applies the NC-9 evidence gate (validate_test_scenario_evidence) unless
          ``skip_test_evidence_global`` is set (then allows + emits a forensic log).
        - Mutates ONLY the target scenario (status + inline evidence) and persists
          narrow — it does NOT go through update_spec, does NOT bump version and
          does NOT replace the full list, so every other scenario is preserved.

        Returns ``{scenario_id, old_status, new_status, evidence_provided,
        evidence_gate_skipped}``. Raises :class:`StatusNotMutableError` and
        ``ValueError`` (``status_not_valid`` / ``evidence_required`` /
        ``scenario_not_found``).
        """
        if status not in VALID_SCENARIO_STATUSES:
            raise ValueError(
                f"status_not_valid: must be one of {list(VALID_SCENARIO_STATUSES)}"
            )

        spec = await self.get_spec(spec_id)
        if not spec:
            raise ValueError("scenario_not_found: spec not found")

        scenarios = [
            dict(s) for s in (spec.test_scenarios or []) if isinstance(s, dict)
        ]
        target_scenario = next(
            (item for item in scenarios if item.get("id") == scenario_id),
            None,
        )
        if target_scenario is None:
            raise ValueError(f"scenario_not_found: {scenario_id}")
        existing_status = str(target_scenario.get("status") or "draft")
        require_test_scenario_status_transition(existing_status, status)
        scenario_sha256 = compute_test_scenario_semantic_sha256(
            board_id=spec.board_id,
            spec_id=spec_id,
            scenario=target_scenario,
            acceptance_criteria=list(spec.acceptance_criteria or []),
        )

        # A board-level evidence bypass may relax NC-9 completeness, but it must
        # never turn a caller-authored SHA/boolean into a trusted V2 execution.
        # Authenticate the installation receipt before any mutation or audit.
        if evidence is not None:
            _require_trusted_test_evidence_v2_write(
                board_id=spec.board_id,
                spec_id=spec_id,
                scenario_id=scenario_id,
                scenario_sha256=scenario_sha256,
                status=status,
                actor_id=user_id,
                evidence=evidence,
            )

        # Guard by STATUS (NOT the content-lock): blocks validated/done, allows
        # in_progress — the execution phase where scenarios become passed.
        # Post-lock exception: a done/validated spec may still receive
        # operational test evidence when the target scenario is already tied to
        # a real test card in an execution state. This preserves content lock
        # semantics while making the documented "fresh post-bug/regression test
        # card on a locked spec" flow reachable.
        try:
            require_test_scenario_status_mutable(getattr(spec, "status", None))
        except StatusNotMutableError:
            if not await self._has_executable_test_card_for_scenario(spec, scenario_id):
                raise

        board = await _application_get(self.db, "board", spec.board_id)
        skip = (
            bool((board.settings or {}).get("skip_test_evidence_global", False))
            if board
            else False
        )
        evidence_verification_status = (
            "bypassed" if skip and status in GATED_STATUSES else "not_required"
        )
        if not skip:
            # for_write: a NEW gated write must satisfy the re-executable
            # evidence contract (spec 9e0bf979) — explicit evidence_class is
            # strict, and an unclassed run-log-like payload is rejected before
            # persisting (only a direct test pointer is grandfathered).
            ok, missing = validate_test_scenario_evidence(
                status, evidence, for_write=True, scenario_id=scenario_id
            )
            if not ok:
                raise ValueError(f"evidence_required: {', '.join(missing)}")
            if status in GATED_STATUSES:
                evidence_verification_status = "verified"

        # Exact operational replay is a semantic no-op.  Avoid marking the
        # JSON column dirty so the internal test-scenario policy epoch remains
        # stable and existing evidence is not needlessly rewritten.
        existing_evidence = target_scenario.get("evidence")
        if existing_status == status and (
            evidence is None or existing_evidence == evidence
        ):
            return {
                "scenario_id": scenario_id,
                "old_status": existing_status,
                "new_status": status,
                "evidence_provided": evidence is not None,
                "evidence_gate_skipped": skip,
                "evidence_verification_status": (evidence_verification_status),
            }

        await GuidelineService(self.db).enforce_policy_transition(
            board_id=spec.board_id,
            entity_type="test_scenario",
            subject_id=scenario_id,
            from_status=existing_status,
            to_status=status,
        )

        old_status = None
        found = False
        for s in scenarios:
            if s.get("id") == scenario_id:
                old_status = s.get("status")
                s["status"] = status
                if evidence is not None:
                    s["evidence"] = evidence
                found = True
                break
        if not found:  # defensive: target_scenario was resolved above
            raise ValueError(f"scenario_not_found: {scenario_id}")

        # Narrow persist: write only the test_scenarios column, no version bump,
        # no content-lock. The other scenarios in the list are untouched.
        spec.test_scenarios = scenarios
        spec.mark_dirty("test_scenarios")
        await _application_add(
            self.db,
            _new_application_record(
                "activity_log",
                board_id=spec.board_id,
                action="test_scenario_status_changed",
                actor_type="agent",
                actor_id=user_id,
                actor_name=user_id,
                details={
                    "spec_id": spec_id,
                    "scenario_id": scenario_id,
                    "from_status": old_status,
                    "to_status": status,
                    "evidence_provided": evidence is not None,
                    "evidence_gate_skipped": skip,
                    "evidence_verification_status": evidence_verification_status,
                },
            ),
        )
        await _application_commit(self.db)

        logger = logging.getLogger("okto_pulse.spec.test_scenario")
        logger.info(
            "test_scenario.status_changed scenario=%s board=%s from=%s to=%s "
            "evidence=%s skip=%s",
            scenario_id,
            spec.board_id,
            old_status,
            status,
            evidence is not None,
            skip,
            extra={
                "event": "test_scenario.status_changed",
                "scenario_id": scenario_id,
                "board_id": spec.board_id,
                "spec_id": spec_id,
                "from_status": old_status,
                "to_status": status,
                "evidence_provided": evidence is not None,
                "evidence_gate_skipped": skip,
                "evidence_verification_status": evidence_verification_status,
                "changed_by_agent_id": user_id,
            },
        )
        if skip and status in GATED_STATUSES:
            logger.info(
                "test_scenario.evidence_gate_skipped scenario=%s board=%s status=%s",
                scenario_id,
                spec.board_id,
                status,
                extra={
                    "event": "test_scenario.evidence_gate_skipped",
                    "scenario_id": scenario_id,
                    "board_id": spec.board_id,
                    "spec_id": spec_id,
                    "status": status,
                    "skip": True,
                    "agent_id": user_id,
                },
            )

        return {
            "scenario_id": scenario_id,
            "old_status": old_status,
            "new_status": status,
            "evidence_provided": evidence is not None,
            "evidence_gate_skipped": skip,
            "evidence_verification_status": evidence_verification_status,
        }

    async def _has_executable_test_card_for_scenario(
        self, spec: ApplicationRecord, scenario_id: str
    ) -> bool:
        """Return True when a locked/done spec scenario has a concrete test card
        that can legitimately carry post-lock evidence.

        This is intentionally narrower than "scenario exists": the status path
        remains blocked for arbitrary scenario mutation on locked specs. The
        exception only applies after a fresh/existing test card is linked and has
        entered the execution/review lifecycle.
        """

        rows = await _application_list(
            self.db,
            "card",
            filters=(
                _apf("spec_id", "eq", spec.id),
                _apf("card_type", "eq", CardType.TEST),
                _apf(
                    "status",
                    "in",
                    [
                        CardStatus.STARTED,
                        CardStatus.IN_PROGRESS,
                        CardStatus.VALIDATION,
                        CardStatus.DONE,
                    ],
                ),
            ),
        )
        for card in rows:
            if scenario_id in (card.test_scenario_ids or []):
                return True
        return False

    async def _enforce_test_scenario_evidence_gate(
        self,
        spec: "Spec",
        new_scenarios: list,
        user_id: str,
        *,
        acceptance_criteria: list[object] | None = None,
    ) -> None:
        """NC-9 service gate (spec 6f1e75bf, FR1/BR2).

        Reject any test scenario whose FINAL status is gated
        (passed/automated/failed) without valid structured evidence when the
        scenario is NEW, its status CHANGED, or its previously-valid evidence was
        removed/invalidated. Old vs new are matched by scenario id. Respects
        ``skip_test_evidence_global`` (allows but emits a forensic audit log so
        reactivation analytics can flag boards that bypass the gate).
        """
        board = await _application_get(self.db, "board", spec.board_id)
        skip = (
            bool((board.settings or {}).get("skip_test_evidence_global", False))
            if board
            else False
        )

        old_by_id = {
            s.get("id"): s for s in (spec.test_scenarios or []) if isinstance(s, dict)
        }
        offenders: list[str] = []
        for s in new_scenarios:
            if not isinstance(s, dict):
                continue
            status = s.get("status")
            if status not in GATED_STATUSES:
                continue
            sid = s.get("id")
            old = old_by_id.get(sid)
            is_new = old is None
            status_changed = (old or {}).get("status") != status
            evidence = s.get("evidence") or s.get("latest_evidence")
            old_evidence = (old or {}).get("evidence") or (old or {}).get(
                "latest_evidence"
            )
            evidence_changed = bool(old and old_evidence != evidence)
            semantic_changed = False
            if old is not None:
                try:
                    old_semantic_sha256 = compute_test_scenario_semantic_sha256(
                        board_id=spec.board_id,
                        spec_id=spec.id,
                        scenario=old,
                        acceptance_criteria=list(spec.acceptance_criteria or []),
                    )
                    new_semantic_sha256 = compute_test_scenario_semantic_sha256(
                        board_id=spec.board_id,
                        spec_id=spec.id,
                        scenario=s,
                        acceptance_criteria=(
                            acceptance_criteria
                            if acceptance_criteria is not None
                            else list(spec.acceptance_criteria or [])
                        ),
                    )
                    semantic_changed = old_semantic_sha256 != new_semantic_sha256
                except (TypeError, ValueError):
                    # A malformed semantic shape cannot inherit old evidence.
                    semantic_changed = True
            # Whole-spec writes are allowed to round-trip untouched historical
            # rows. A new/changed V2 claim, however, must carry an authentic
            # receipt bound to this exact board/spec/scenario/status.
            if is_new or status_changed or evidence_changed or semantic_changed:
                scenario_sha256 = compute_test_scenario_semantic_sha256(
                    board_id=spec.board_id,
                    spec_id=spec.id,
                    scenario=s,
                    acceptance_criteria=(
                        acceptance_criteria
                        if acceptance_criteria is not None
                        else list(spec.acceptance_criteria or [])
                    ),
                )
                _require_trusted_test_evidence_v2_write(
                    board_id=spec.board_id,
                    spec_id=spec.id,
                    scenario_id=str(sid or ""),
                    scenario_sha256=scenario_sha256,
                    status=str(status),
                    actor_id=user_id,
                    evidence=evidence,
                )
            if scenario_has_required_evidence(s, for_write=True):
                continue
            old_had_evidence = scenario_has_required_evidence(old) if old else False
            # Enforce on: new scenario already gated, status transition into a
            # gated state, or evidence removed/altered from a previously-valid
            # scenario. A pre-existing gated scenario that was always evidenceless
            # and is left unchanged is NOT newly rejected (legacy data, not
            # introduced by this write).
            if is_new or status_changed or old_had_evidence or evidence_changed:
                offenders.append(str(sid) if sid else "(new)")

        if not offenders:
            return

        logger = logging.getLogger("okto_pulse.spec.test_scenario")
        if not skip:
            raise ValueError(
                "evidence_required: test scenario(s) "
                f"{', '.join(offenders)} marked passed/automated/failed without "
                "structured evidence. Provide evidence via the status tool or "
                "endpoint, or enable skip_test_evidence_global on the board."
            )
        # skip ON — allow but emit a forensic audit record (spec OR or_536eca62).
        for sid in offenders:
            logger.info(
                "test_scenario.evidence_gate_skipped scenario=%s board=%s spec=%s",
                sid,
                spec.board_id,
                spec.id,
                extra={
                    "event": "test_scenario.evidence_gate_skipped",
                    "scenario_id": sid,
                    "board_id": spec.board_id,
                    "spec_id": spec.id,
                    "actor_id": user_id,
                    "source": "update_spec",
                    "skip": True,
                },
            )

    async def update_spec(
        self,
        spec_id: str,
        user_id: str,
        data: SpecUpdate | PersistedTestScenarioSpecUpdate,
    ) -> Spec | None:
        """Update a spec. Bumps version on content changes. Records field-level diffs.

        The primary lifecycle rule is Draft-only mutation. A non-Draft Spec
        raises ``SubjectEditRequiresDraftError`` before any write; reopening to
        Draft starts a new validation edition. The legacy active-validation
        lock remains a defense-in-depth compatibility check and may still raise
        ``SpecLockedError``. All content tools (business rules, contracts,
        scenarios, mockups, knowledge) flow through this method via the public
        ``SpecUpdate`` or the narrow internal persisted-scenario carrier, so the
        shared checks cover the whole surface in one place.

        Also enforces referential integrity for `linked_*` fields: any
        `linked_criteria`/`linked_requirements`/`linked_rules`/`linked_task_ids`
        that points to a non-existent target raises ValueError before any write.
        """
        spec = await self.get_spec(spec_id)
        if not spec:
            return None

        if getattr(spec, "archived", False):
            raise ValueError(
                "This spec is archived. Restore it first before making changes."
            )
        require_draft_mutation(spec, subject_type="spec")
        await _require_spec_unlocked(self.db, spec_id)

        update_data = data.model_dump(exclude_unset=True)
        next_ideation_id = (
            update_data["ideation_id"]
            if "ideation_id" in update_data
            else spec.ideation_id
        )
        next_refinement_id = (
            update_data["refinement_id"]
            if "refinement_id" in update_data
            else spec.refinement_id
        )
        parent_link_changed = (
            next_ideation_id != spec.ideation_id
            or next_refinement_id != spec.refinement_id
        )
        if parent_link_changed:
            await self._validate_lineage(
                spec.board_id,
                ideation_id=next_ideation_id,
                refinement_id=next_refinement_id,
            )
        previous_knowledge_parent = _governed_spec_knowledge_parent(
            ideation_id=spec.ideation_id,
            refinement_id=spec.refinement_id,
        )
        next_knowledge_parent = _governed_spec_knowledge_parent(
            ideation_id=next_ideation_id,
            refinement_id=next_refinement_id,
        )
        content_fields = {
            "title",
            "functional_requirements",
            "technical_requirements",
            "acceptance_criteria",
            "test_scenarios",
            "context",
            "description",
            # Legacy bulk writers participate in the same optimistic
            # concurrency contract as their structured-entity counterparts.
            "business_rules",
            "api_contracts",
            "integration_requirements",
            "observability_requirements",
            "decisions",
            "require_task_validation",
            "validation_min_confidence",
            "validation_min_completeness",
            "validation_max_drift",
        }
        # Spec eaf78891 (Ideação #2): semantic_fields are KG-relevant fields.
        # Some also bump version through content_fields so bulk and structured
        # writers share one CAS boundary; all still emit SpecSemanticChanged
        # so ConsolidationEnqueuer re-extracts the spec into the KG. Parent
        # lineage is semantic too: the deterministic worker emits a different
        # belongs_to edge when either parent changes.
        lineage_fields = {"ideation_id", "refinement_id"}
        changed_lineage_fields = {
            field
            for field, current, next_value in (
                ("ideation_id", spec.ideation_id, next_ideation_id),
                ("refinement_id", spec.refinement_id, next_refinement_id),
            )
            if current != next_value
        }
        semantic_fields = {
            "decisions",
            "business_rules",
            "api_contracts",
            "integration_requirements",
            "observability_requirements",
            "test_scenarios",
            "screen_mockups",
            *lineage_fields,
        }

        def _semantic_changed_fields() -> set[str]:
            # Keep the established re-extraction behavior for explicit
            # non-lineage semantic writes, but report lineage fields only when
            # their persisted value actually changes. This prevents an
            # idempotent parent resend from producing a false lineage event.
            explicit_non_lineage = (
                semantic_fields & update_data.keys()
            ) - lineage_fields
            return explicit_non_lineage | changed_lineage_fields

        bumps_version = bool(content_fields & update_data.keys())
        bumps_semantic = bool(_semantic_changed_fields())

        # Capture old values for diff
        old_data = {k: getattr(spec, k) for k in update_data.keys()}

        # Serialize structured JSON list fields if present.
        for json_list_field in (
            "test_scenarios",
            "screen_mockups",
            "business_rules",
            "api_contracts",
            "integration_requirements",
            "observability_requirements",
            "decisions",
        ):
            if (
                json_list_field in update_data
                and update_data[json_list_field] is not None
            ):
                update_data[json_list_field] = [
                    s.model_dump() if hasattr(s, "model_dump") else s
                    for s in update_data[json_list_field]
                ]

        if update_data.get("test_scenarios") is not None:
            update_data["test_scenarios"] = resolve_scenario_types_for_whole_list_write(
                update_data["test_scenarios"],
                spec.test_scenarios,
            )
            # B10 policy-transition fence: a public whole-list replacement is
            # a content API, never an alternate scenario lifecycle writer.
            # Existing status changes and newly-imported terminal/execution
            # states must use ``set_test_scenario_status`` so transition
            # legality, evidence and Policy Compliance are evaluated against
            # exactly one board-scoped subject under the same transaction.
            #
            # The narrow internal carrier remains available to trusted
            # read-modify-write workflows (for example semantic edits that
            # invalidate evidence and regress one scenario to ``ready``).
            if isinstance(data, SpecUpdate):
                old_scenarios_by_id = {
                    str(item.get("id")): item
                    for item in (spec.test_scenarios or ())
                    if isinstance(item, dict) and item.get("id") is not None
                }
                for scenario in update_data["test_scenarios"]:
                    if not isinstance(scenario, dict):
                        continue
                    scenario_id = scenario.get("id")
                    raw_next_status = scenario.get("status") or "draft"
                    next_status = str(
                        getattr(raw_next_status, "value", raw_next_status)
                    ).lower()
                    previous = (
                        old_scenarios_by_id.get(str(scenario_id))
                        if scenario_id is not None
                        else None
                    )
                    if previous is not None:
                        raw_previous_status = previous.get("status") or "draft"
                        previous_status = str(
                            getattr(
                                raw_previous_status,
                                "value",
                                raw_previous_status,
                            )
                        ).lower()
                        if next_status != previous_status:
                            raise ValueError(
                                "test_scenario_status_requires_scoped_update: "
                                f"scenario {scenario_id} cannot move from "
                                f"'{previous_status}' to '{next_status}' through "
                                "update_spec; use set_test_scenario_status."
                            )
                    elif next_status in GATED_STATUSES:
                        raise ValueError(
                            "test_scenario_status_requires_scoped_update: "
                            f"new scenario {scenario_id or '(new)'} cannot be "
                            f"created as '{next_status}' through update_spec; "
                            "create it as draft/ready, then use "
                            "set_test_scenario_status."
                        )
            await self._validate_test_scenario_subject_identities(
                board_id=spec.board_id,
                scenarios=update_data["test_scenarios"],
                current_spec_id=spec.id,
            )

        # Canonicalize explicitly patched FR/TR/AC collections with stable IDs.
        # The complete final namespace participates in collision validation, but
        # omitted collections remain byte-identical: partial updates must not
        # silently materialize legacy requirements or migrate their linked refs.
        # Runs BEFORE _validate_spec_linked_refs so explicit requirement writes
        # are validated against their canonical ids while title/description and
        # other unrelated patches preserve lossless PATCH semantics.
        _explicit_requirement_fields = {
            field_name
            for field_name in (
                "functional_requirements",
                "technical_requirements",
                "acceptance_criteria",
            )
            if field_name in update_data
        }
        if _explicit_requirement_fields:
            _existing_requirement_fields = {
                "functional_requirements": spec.functional_requirements,
                "technical_requirements": spec.technical_requirements,
                "acceptance_criteria": spec.acceptance_criteria,
            }
            _final_requirement_fields = {
                field_name: (
                    update_data[field_name]
                    if field_name in update_data
                    else current_value
                )
                for field_name, current_value in _existing_requirement_fields.items()
            }
            _canonical_requirement_fields = canonicalize_spec_requirement_fields(
                _final_requirement_fields,
                existing_fields=_existing_requirement_fields,
            )
            for _field_name in _explicit_requirement_fields:
                update_data[_field_name] = _canonical_requirement_fields[_field_name]

        # FR5 — lazy ref migration (spec c61569b2, IMPL-4).
        # When explicit FR/AC lists are materialised by canonicalization above,
        # rewrite any index/text refs in downstream fields to the newly
        # assigned fr_/ac_ ids.  This runs on-touch only: specs not passed
        # through update_spec keep resolving via the permanent read-tolerant
        # resolvers (resolve_linked_fr_indices / resolve_linked_criteria_*).
        # No batch sweep; no one-shot migration tool.
        if (
            "functional_requirements" in update_data
            and update_data["functional_requirements"]
        ):
            from okto_pulse.core.services.spec_structured_entities import (  # noqa: PLC0415
                migrate_legacy_fr_refs,
            )

            old_frs = list(spec.functional_requirements or [])
            new_frs = list(update_data["functional_requirements"] or [])
            _fr_dep_collections = {
                field: list(
                    update_data[field]
                    if field in update_data and update_data[field] is not None
                    else getattr(spec, field, None) or []
                )
                for field in (
                    "business_rules",
                    "api_contracts",
                    "integration_requirements",
                    "observability_requirements",
                    "decisions",
                )
            }
            _fr_migration_updates = migrate_legacy_fr_refs(
                old_frs, new_frs, _fr_dep_collections
            )
            # Apply migration results unconditionally: the collections dict was
            # already built from update_data (if present) or spec, so _updated
            # already reflects the caller's new data with refs rewritten.
            for _field, _updated in _fr_migration_updates.items():
                update_data[_field] = _updated

        if "acceptance_criteria" in update_data and update_data["acceptance_criteria"]:
            from okto_pulse.core.services.spec_structured_entities import (  # noqa: PLC0415
                migrate_legacy_ac_refs,
            )

            old_acs = list(spec.acceptance_criteria or [])
            new_acs = list(update_data["acceptance_criteria"] or [])
            _current_scenarios = list(
                update_data["test_scenarios"]
                if "test_scenarios" in update_data
                and update_data["test_scenarios"] is not None
                else getattr(spec, "test_scenarios", None) or []
            )
            _migrated_scenarios = migrate_legacy_ac_refs(
                old_acs, new_acs, _current_scenarios
            )
            if _migrated_scenarios is not None:
                update_data["test_scenarios"] = _migrated_scenarios

        # Re-evaluate bumps_semantic after FR5 migration may have added
        # semantic fields (e.g. business_rules) to update_data.
        bumps_semantic = bool(_semantic_changed_fields())
        # Capture old values for any fields added to update_data by FR5 migration
        # (these were absent from the original update_data so old_data missed them).
        for _migrated_field in update_data:
            if _migrated_field not in old_data:
                old_data[_migrated_field] = getattr(spec, _migrated_field, None)

        # Validate referential integrity of all `linked_*` fields BEFORE
        # mutating the spec. The validator computes the final state of each
        # collection (incoming value OR current state if untouched) and
        # rejects orphan references with a precise error message.
        await _validate_spec_linked_refs(self.db, spec, update_data)

        # Fail-closed scenario_type service gate — defense in depth (spec
        # ac16b3c9, FR2/IR). Closes the same whole-list bypass for scenario_type:
        # any caller (UI full-list, REST PUT or MCP) replacing test_scenarios must
        # not introduce a new/changed invalid scenario_type. Grandfathers unchanged
        # historical values (matched by id) so legacy data keeps re-serializing;
        # runs BEFORE any mutation/flush and never normalizes.
        if update_data.get("test_scenarios") is not None:
            validate_scenario_types_for_write(
                update_data["test_scenarios"], spec.test_scenarios
            )

        # NC-9 (test-theater) service gate — defense in depth (spec 6f1e75bf,
        # FR1/BR2). Closes the bypass where any caller (UI full-list, REST or
        # MCP) could replace test_scenarios with a gated status and no evidence;
        # the evidence rule previously ran only in the MCP status tool. Runs on
        # the incoming list, comparing against the current persisted scenarios.
        if update_data.get("test_scenarios") is not None:
            await self._enforce_test_scenario_evidence_gate(
                spec,
                update_data["test_scenarios"],
                user_id,
                acceptance_criteria=list(
                    update_data.get("acceptance_criteria")
                    if update_data.get("acceptance_criteria") is not None
                    else spec.acceptance_criteria or []
                ),
            )

        # MockupDesignSystemGate (spec 3a006f65, card 0192f58d) — defense in depth:
        # gate the bulk screen_mockups write (UI full-list / REST) the same way the MCP
        # tool does, BEFORE persistence. Delta-only: only new/changed mockups; legacy
        # untouched mockups are skipped; screens already gated by the MCP tool in this
        # transaction are skipped. Blocking raises pre-persist; advisory audits.
        if update_data.get("screen_mockups") is not None:
            from okto_pulse.core.services.design_system import (
                gate_entity_screen_mockups,
            )

            await gate_entity_screen_mockups(
                self.db, spec, update_data["screen_mockups"], entity_type="spec"
            )

        json_fields = {
            "test_scenarios",
            "screen_mockups",
            "business_rules",
            "api_contracts",
            "integration_requirements",
            "observability_requirements",
            "decisions",
            "functional_requirements",
            "technical_requirements",
            "acceptance_criteria",
            "labels",
        }
        if previous_knowledge_parent != next_knowledge_parent:
            await _reset_v2_knowledge_for_relink(
                self.db,
                board_id=spec.board_id,
                target_type="spec",
                target_id=spec.id,
                previous_parent=previous_knowledge_parent,
                next_parent=next_knowledge_parent,
                actor_id=user_id,
                port=self._knowledge_propagation_port,
            )
        for key, value in update_data.items():
            setattr(spec, key, value)
            if key in json_fields:
                spec.mark_dirty(key)

        old_version = spec.version
        if bumps_version:
            spec.version += 1

        # Application records are detached from adapter-specific identity maps.
        # Synchronize before downstream event/activity handlers inspect the row.
        await _application_flush(self.db)

        # Compute diffs
        changes = self._compute_diff(old_data, update_data, list(update_data.keys()))

        if bumps_version:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import SpecVersionBumped

            changed_struct_fields = sorted(content_fields & update_data.keys())
            await event_publish(
                SpecVersionBumped(
                    board_id=spec.board_id,
                    actor_id=user_id,
                    spec_id=spec.id,
                    old_version=old_version,
                    new_version=spec.version,
                    changed_fields=changed_struct_fields,
                ),
                session=self.db,
            )

        # Spec eaf78891 (Ideação #2): emit SpecSemanticChanged whenever
        # KG-relevant non-content fields are mutated, INDEPENDENTLY of whether
        # SpecVersionBumped also fired. Both events are recorded in the
        # outbox for audit completeness; ConsolidationEnqueuer's dedup
        # collapses them into a single ConsolidationQueue row anyway.
        if bumps_semantic:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import SpecSemanticChanged

            changed_semantic = sorted(_semantic_changed_fields())
            await event_publish(
                SpecSemanticChanged(
                    board_id=spec.board_id,
                    actor_id=user_id,
                    spec_id=spec.id,
                    changed_fields=changed_semantic,
                ),
                session=self.db,
            )

        actor_name = await resolve_actor_name(self.db, user_id, spec.board_id)
        await self._log_activity(
            board_id=spec.board_id,
            action="spec_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "spec_id": spec_id,
                "edition": int(getattr(spec, "edition", 1) or 1),
                "version": spec.version,
                "technical_revision": spec.version,
                "fields": list(update_data.keys()),
            },
        )
        if changes:
            changed_fields = ", ".join(c["field"] for c in changes)
            await self._record_history(
                spec_id=spec_id,
                action="updated",
                actor_id=user_id,
                actor_name=actor_name,
                changes=changes,
                version=spec.version,
                summary=f"Updated: {changed_fields}",
            )
        if "screen_mockups" in update_data:
            await SpecResourcePropagationService(self.db).propagate_for_spec(
                board_id=spec.board_id,
                spec_id=spec.id,
                actor_id=user_id,
                trigger="spec_mockups_changed",
            )
        return spec

    async def append_locked_traceability_task_link(
        self,
        spec_id: str,
        user_id: str,
        *,
        target_field: str,
        target_id: str,
        card_id: str,
    ) -> tuple[Spec | None, bool, list[str]]:
        """Append an allowed traceability-only task link on a content-locked spec.

        This narrow path does not unlock ``update_spec``. It only appends
        ``linked_task_ids`` on FR/TR/Decision records, does not bump version, and
        refuses archived/cancelled/cross-spec cards.
        """
        allowed_fields = {
            "functional_requirements": "functional_requirement",
            "technical_requirements": "technical_requirement",
            "decisions": "decision",
        }
        if target_field not in allowed_fields:
            raise ValueError(
                "Traceability-only links are limited to FR, TR, and Decision targets"
            )

        spec = await self.get_spec(spec_id)
        if not spec:
            return None, False, []
        if not spec_is_content_locked(spec):
            raise ValueError(
                "Traceability-only path is only available for content-locked specs"
            )
        if getattr(spec, "archived", False):
            raise ValueError(
                "This spec is archived. Restore it first before linking tasks."
            )

        card = await _application_get(self.db, "card", card_id)
        if card is None:
            raise ValueError("Card not found")
        require_card_operational_mutation_allowed(
            card,
            operation="link_card_traceability",
        )
        card_status = getattr(
            getattr(card, "status", None), "value", getattr(card, "status", None)
        )
        if getattr(card, "spec_id", None) != spec.id:
            raise ValueError("Traceability-only links require a card on the same spec")
        if getattr(card, "archived", False) or card_status == "cancelled":
            raise ValueError(
                "Traceability-only links require a non-archived, non-cancelled card"
            )

        collection = list(getattr(spec, target_field, None) or [])
        target = next(
            (
                item
                for item in collection
                if isinstance(item, dict) and item.get("id") == target_id
            ),
            None,
        )
        if target is None:
            raise ValueError(
                f"{allowed_fields[target_field]} '{target_id}' not found in spec"
            )
        if target_field == "decisions" and target.get("status", "active") != "active":
            raise ValueError(
                "Traceability-only decision links require an active decision"
            )

        task_ids = list(target.get("linked_task_ids") or [])
        old_task_ids = list(task_ids)
        changed = card_id not in task_ids
        if changed:
            task_ids.append(card_id)
            target["linked_task_ids"] = task_ids
            setattr(spec, target_field, collection)
            spec.mark_dirty(target_field)

            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import SpecSemanticChanged

            await event_publish(
                SpecSemanticChanged(
                    board_id=spec.board_id,
                    actor_id=user_id,
                    spec_id=spec.id,
                    changed_fields=[target_field],
                ),
                session=self.db,
            )

        actor_name = await resolve_actor_name(self.db, user_id, spec.board_id)
        await self._log_activity(
            board_id=spec.board_id,
            action="spec_traceability_linked",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "spec_id": spec.id,
                "target_field": target_field,
                "target_id": target_id,
                "card_id": card_id,
                "traceability_only": True,
                "changed": changed,
            },
        )
        if changed:
            await self._record_history(
                spec_id=spec.id,
                action="traceability_linked",
                actor_id=user_id,
                actor_name=actor_name,
                changes=[
                    {
                        "field": f"{target_field}.linked_task_ids",
                        "old": old_task_ids,
                        "new": task_ids,
                    }
                ],
                version=spec.version,
                summary=(
                    f"Traceability-only task link appended to "
                    f"{allowed_fields[target_field]} {target_id}"
                ),
            )
        return spec, changed, task_ids

    async def remove_scenario_traceability_task_link(
        self,
        spec_id: str,
        user_id: str,
        *,
        scenario_id: str,
        card_id: str,
    ) -> tuple[Spec | None, bool, list[str]]:
        """Remove one scenario backlink without treating it as authored content.

        ``linked_task_ids`` is traceability metadata excluded from the SK-A
        semantic snapshot.  This narrow path deliberately preserves the Spec
        version and does not invoke the requirement-lint writer, while retaining
        the same event/history/audit visibility as the locked append path.
        """

        spec = await self.get_spec(spec_id)
        if not spec:
            return None, False, []
        if getattr(spec, "archived", False):
            raise ValueError(
                "This spec is archived. Restore it first before unlinking tasks."
            )

        card = await _application_get(self.db, "card", card_id)
        if card is None or getattr(card, "board_id", None) != spec.board_id:
            raise ValueError("Card not found")
        require_card_operational_mutation_allowed(
            card,
            operation="unlink_card_traceability",
        )

        scenarios = [
            dict(item) if isinstance(item, dict) else item
            for item in (spec.test_scenarios or [])
        ]
        target = next(
            (
                item
                for item in scenarios
                if isinstance(item, dict) and item.get("id") == scenario_id
            ),
            None,
        )
        if target is None:
            raise ValueError(f"test_scenario '{scenario_id}' not found in spec")

        task_ids = list(target.get("linked_task_ids") or [])
        old_task_ids = list(task_ids)
        changed = card_id in task_ids
        if changed:
            task_ids.remove(card_id)
            target["linked_task_ids"] = task_ids
            spec.test_scenarios = scenarios
            spec.mark_dirty("test_scenarios")

            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import SpecSemanticChanged

            await event_publish(
                SpecSemanticChanged(
                    board_id=spec.board_id,
                    actor_id=user_id,
                    spec_id=spec.id,
                    changed_fields=["test_scenarios"],
                ),
                session=self.db,
            )

        actor_name = await resolve_actor_name(self.db, user_id, spec.board_id)
        await self._log_activity(
            board_id=spec.board_id,
            action="spec_traceability_unlinked",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "spec_id": spec.id,
                "target_field": "test_scenarios",
                "target_id": scenario_id,
                "card_id": card_id,
                "traceability_only": True,
                "changed": changed,
            },
        )
        if changed:
            await self._record_history(
                spec_id=spec.id,
                action="traceability_unlinked",
                actor_id=user_id,
                actor_name=actor_name,
                changes=[
                    {
                        "field": "test_scenarios.linked_task_ids",
                        "old": old_task_ids,
                        "new": task_ids,
                    }
                ],
                version=spec.version,
                summary=(
                    "Traceability-only task link removed from "
                    f"test scenario {scenario_id}"
                ),
            )
        return spec, changed, task_ids

    # ---- Spec state machine ----
    # Direct non-Draft→Draft transitions open one new human lifecycle edition.
    # Draft is the sole editable status; the same UoW clears current projections
    # while immutable validation history remains available as Previous.
    _SPEC_TRANSITIONS = transition_map("spec")

    async def _enforce_spec_checklist_gate(
        self,
        spec: Spec,
        *,
        surface: str,
    ) -> None:
        """Apply the shared A3 predicate without transport-specific shortcuts."""

        import logging

        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )
        from okto_pulse.core.services.checklist import ChecklistService

        persistence = require_relational_application_adapter().checklists(self.db)
        decision = await ChecklistService().evaluate_spec_gate(
            board_id=spec.board_id,
            spec_id=spec.id,
            persistence=persistence,
        )
        stale_reasons = (
            tuple(reason.value for reason in decision.currentness.stale_reasons)
            if decision.currentness is not None
            else ()
        )
        # This structured operational record survives a transaction rollback,
        # unlike a staged Activity row on a blocking attempt. It contains only
        # bounded identities and canonical reasons, never checklist bodies.
        logging.getLogger("okto_pulse.core.spec_checklist_gate").info(
            "spec_checklist_gate_attempt board_id=%s spec_id=%s "
            "surface=%s mode=%s allowed=%s reason=%s stale_reasons=%s",
            spec.board_id,
            spec.id,
            surface,
            decision.mode.value,
            decision.allowed,
            decision.reason,
            ",".join(stale_reasons),
        )
        if not decision.allowed:
            from okto_pulse.core.services.gate_contracts import (
                spec_checklist_gate_error,
            )

            raise spec_checklist_gate_error(
                spec_id=spec.id,
                current_status=spec.status.value,
                reason=decision.reason,
                mode=decision.mode.value,
                stale_reasons=stale_reasons,
            )

    async def _enforce_spec_requirement_lint_gate(self, spec: Spec) -> None:
        """Require accepted external lint evidence for this lifecycle edition."""

        from okto_pulse.core.domain.quality_assessment import (
            AssessmentKind,
            AssessmentSubjectType,
        )
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        persistence = require_relational_application_adapter().quality_assessments(
            self.db
        )
        current = await persistence.get_current(
            board_id=spec.board_id,
            subject_type=AssessmentSubjectType.SPEC,
            subject_id=spec.id,
            assessment_kind=AssessmentKind.REQUIREMENT_LINT,
            subject_edition=int(spec.edition),
        )
        if current is None:
            raise RequirementLintRequired(
                "Requirement Lint is required for the current Spec edition.",
                details={"spec_edition": int(spec.edition)},
            )

    async def move_spec(
        self, spec_id: str, user_id: str, data: SpecMove, actor_name: str | None = None
    ) -> Spec | None:
        """Move a spec to a different status.

        Enforces a strict state machine. Coverage gates run on approved→validated.
        Qualitative validation runs on validated→in_progress.
        Moving to 'done' requires full test coverage and task completion.
        """
        await _application_flush(self.db)
        spec = await self.get_spec(spec_id)
        if not spec:
            return None

        if getattr(spec, "archived", False):
            raise ValueError(
                "This spec is archived. Restore it first before changing status."
            )

        lifecycle_fence = {
            "status": spec.status,
            "edition": int(getattr(spec, "edition", 1) or 1),
            "version": int(spec.version),
            "archived": bool(getattr(spec, "archived", False)),
            "current_validation_id": spec.current_validation_id,
        }

        # Enforce state machine transitions
        allowed = self._SPEC_TRANSITIONS.get(spec.status, [])
        if data.status not in allowed:
            allowed_values = [s.value for s in allowed]
            raise ValueError(
                f"Cannot move spec from '{spec.status.value}' to '{data.status.value}'. "
                f"Allowed transitions: {allowed_values}"
            )

        # Load board for settings
        board = await _application_get(self.db, "board", spec.board_id)

        await evaluate_code_traceability_transition(
            self.db,
            board=board,
            subject=spec,
            subject_type=CodeTraceabilitySubjectType.SPEC,
            from_status=spec.status.value,
            to_status=data.status.value,
            enforce=True,
        )

        # A done spec is one of the two eligibility anchors for a hotfix lane.
        # Reopening it must not silently invalidate lanes that have no valid,
        # closed same-spec origin sprint.  This preflight deliberately runs
        # before authorization/audit and before any entity mutation.
        if spec.status == SpecStatus.DONE and data.status == SpecStatus.DRAFT:
            hotfix_sprints = await _application_list(
                self.db,
                "sprint",
                filters=(
                    _apf("spec_id", "eq", spec.id),
                    _apf("lane_type", "eq", SprintLaneType.HOTFIX),
                ),
            )
            ineligible_sprint_ids: list[str] = []
            for hotfix in hotfix_sprints:
                origin_sprint = (
                    await _application_get(self.db, "sprint", hotfix.origin_sprint_id)
                    if hotfix.origin_sprint_id
                    else None
                )
                has_closed_origin = bool(
                    origin_sprint
                    and origin_sprint.id != hotfix.id
                    and origin_sprint.board_id == hotfix.board_id
                    and origin_sprint.spec_id == hotfix.spec_id
                    and origin_sprint.status == SprintStatus.CLOSED
                )
                if not has_closed_origin:
                    ineligible_sprint_ids.append(hotfix.id)

            if ineligible_sprint_ids:
                raise SprintOperationError(
                    "hotfix_spec_reopen_conflict",
                    "Cannot reopen the spec: one or more hotfix lanes rely on "
                    "the spec remaining done.",
                    remediation="close_and_link_same_spec_origin_sprint_before_reopening_spec",
                    facts={
                        "spec_id": spec.id,
                        "target_status": data.status.value,
                        "dependent_sprint_ids": sorted(ineligible_sprint_ids)[:20],
                        "dependent_sprint_count": len(ineligible_sprint_ids),
                    },
                )

        critical_actor_name = actor_name or await resolve_actor_name(
            self.db, user_id, spec.board_id
        )
        critical_context_decision = await _authorize_critical_context_or_raise(
            self.db,
            board_id=spec.board_id,
            actor_id=user_id,
            entity_type="spec",
            entity_id=spec.id,
            critical_action=_critical_spec_move_action(data.status),
            surface="service",
            actor_type="user",
            actor_name=critical_actor_name,
            defer_success_audit=True,
        )

        # Enforce coverage gates when moving to validated
        if data.status == SpecStatus.VALIDATED:
            card_service = CardService(self.db)
            await card_service.check_test_coverage(spec, board)
            await card_service.check_rules_coverage(spec, board)
            await card_service.check_trs_coverage(spec, board)
            await card_service.check_contract_coverage(spec, board)
            await card_service.check_ir_coverage(spec, board)
            await card_service.check_or_coverage(spec, board)
            await card_service.check_task_requirement_links_for_spec(spec, board)
            await card_service.check_decision_presence(spec)
            await card_service.check_decisions_coverage(spec, board)
            await card_service.check_code_evidence_coverage(spec, board)

            # Spec Validation Gate: when enabled, the only path to validated is via
            # submit_spec_validation (which runs the semantic gate). Direct move_spec
            # from approved→validated is blocked so users/agents cannot bypass the
            # quality check. Reopening a validated/in_progress/done Spec to Draft
            # starts the next editable validation edition.
            board_settings = (board.settings or {}) if board else {}
            if spec.status == SpecStatus.APPROVED and board_settings.get(
                "require_spec_validation", True
            ):
                # R4-IMP1: same block, normalized operational contract (GateContractError
                # subclasses ValueError — no state-machine change, no auto-promotion).
                from okto_pulse.core.services.gate_contracts import (
                    spec_validation_gate_error,
                )

                raise spec_validation_gate_error(
                    spec_id=spec.id,
                    current_status=spec.status.value,
                )

            await self._enforce_spec_checklist_gate(
                spec,
                surface="move_spec",
            )

            resource_gate = ResourceGateService(self.db)
            await resource_gate.validate_or_raise_spec_architecture_validation_resource(
                spec.board_id,
                spec.id,
                board=board,
                phase="spec_validation",
            )

        # Re-execute coverage gates + qualitative validation when moving to in_progress
        if (
            data.status == SpecStatus.IN_PROGRESS
            and spec.status == SpecStatus.VALIDATED
        ):
            card_service = CardService(self.db)
            await card_service.check_test_coverage(spec, board)
            await card_service.check_rules_coverage(spec, board)
            await card_service.check_trs_coverage(spec, board)
            await card_service.check_contract_coverage(spec, board)
            await card_service.check_ir_coverage(spec, board)
            await card_service.check_or_coverage(spec, board)
            await card_service.check_task_requirement_links_for_spec(spec, board)
            await card_service.check_decision_presence(spec)
            await card_service.check_decisions_coverage(spec, board)
            await card_service.check_code_evidence_coverage(spec, board)

            # Qualitative validation gate
            auto_validate = (
                (board.settings or {}).get("auto_validate", False) if board else False
            )
            skip_qualitative = getattr(spec, "skip_qualitative_validation", False)
            if not auto_validate and not skip_qualitative:
                evaluations = [
                    e for e in (spec.evaluations or []) if not e.get("stale")
                ]
                approvals = [
                    e for e in evaluations if e.get("recommendation") == "approve"
                ]
                rejections = [
                    e for e in evaluations if e.get("recommendation") == "reject"
                ]
                if rejections:
                    reject_names = ", ".join(
                        e.get("evaluator_name", e.get("evaluator_id", "?"))
                        for e in rejections
                    )
                    raise ValueError(
                        f"Cannot move spec to 'in_progress': {len(rejections)} evaluation(s) "
                        f"with 'reject' recommendation exist (by: {reject_names}). "
                        f"Remove or replace the rejecting evaluations before proceeding."
                    )
                if not approvals:
                    raise ValueError(
                        "Cannot move spec to 'in_progress': no evaluation with "
                        "'approve' recommendation found. At least one approval is required. "
                        "Submit an evaluation via okto_pulse_submit_spec_evaluation."
                    )
                threshold = (
                    getattr(spec, "validation_threshold", None)
                    or (board.settings or {}).get("validation_threshold_global", 70)
                    if board
                    else 70
                )
                avg_score = sum(e.get("overall_score", 0) for e in approvals) / len(
                    approvals
                )
                if avg_score < threshold:
                    raise ValueError(
                        f"Cannot move spec to 'in_progress': average approval score "
                        f"({avg_score:.0f}) is below threshold ({threshold}). "
                        f"Submit additional evaluations with higher scores or lower the threshold."
                    )

        # Enforce test coverage when moving to Done
        skip_global = (
            (board.settings or {}).get("skip_test_coverage_global", False)
            if board
            else False
        )
        if (
            data.status == SpecStatus.DONE
            and not spec.skip_test_coverage
            and not skip_global
        ):
            criteria = spec.acceptance_criteria or []
            scenarios = spec.test_scenarios or []
            if criteria:
                covered_indices: set[int] = set()
                for scenario in scenarios:
                    covered_indices |= resolve_linked_criteria_to_indices(
                        scenario.get("linked_criteria"),
                        criteria,
                    )
                uncovered = [
                    f"[{i}] {_structured_ref_text(criterion)[:80]}..."
                    for i, criterion in enumerate(criteria)
                    if i not in covered_indices
                ]
                if uncovered:
                    raise ValueError(
                        f"Cannot move spec to 'done': {len(uncovered)} acceptance criteria lack test scenarios. "
                        f"Uncovered: {'; '.join(uncovered[:5])}"
                        f"{f' (and {len(uncovered) - 5} more)' if len(uncovered) > 5 else ''}. "
                        f"Create test scenarios for all criteria, or set skip_test_coverage flag in the spec."
                    )

        # Sprint done gate: all sprints must be closed|cancelled (min 1 closed)
        if data.status == SpecStatus.DONE:
            spec_sprints = await _application_list(
                self.db,
                "sprint",
                filters=(
                    _apf("spec_id", "eq", spec_id),
                    _apf("archived", "is_false"),
                ),
            )
            if spec_sprints:
                pending = [
                    s
                    for s in spec_sprints
                    if s.status not in (SprintStatus.CLOSED, SprintStatus.CANCELLED)
                ]
                has_closed = any(s.status == SprintStatus.CLOSED for s in spec_sprints)
                if pending:
                    sprint_list = "; ".join(
                        f"'{s.title}' ({s.status.value})" for s in pending[:5]
                    )
                    raise ValueError(
                        f"Cannot move spec to 'done': {len(pending)} sprint(s) are not closed or cancelled. "
                        f"Pending: {sprint_list}. Close or cancel all sprints first."
                    )
                if not has_closed:
                    raise ValueError(
                        "Cannot move spec to 'done': at least 1 sprint must be closed "
                        "(all are cancelled). Close at least one sprint."
                    )

        # Enforce all linked tasks (non-bug) must be done/cancelled before spec can be done
        if data.status == SpecStatus.DONE:
            pending_tasks = await _application_list(
                self.db,
                "card",
                filters=(
                    _apf("spec_id", "eq", spec_id),
                    _apf("card_type", "eq", CardType.NORMAL),
                    _apf("archived", "is_false"),
                    _apf(
                        "status",
                        "not_in",
                        [CardStatus.DONE, CardStatus.CANCELLED],
                    ),
                ),
            )
            if pending_tasks:
                task_list = "; ".join(
                    f"'{t.title}' ({t.status.value})" for t in pending_tasks[:5]
                )
                extra = (
                    f" (and {len(pending_tasks) - 5} more)"
                    if len(pending_tasks) > 5
                    else ""
                )
                raise ValueError(
                    f"Cannot move spec to 'done': {len(pending_tasks)} linked task(s) are not yet done or cancelled. "
                    f"Pending: {task_list}{extra}. "
                    f"Complete or cancel all linked tasks before finalizing the spec."
                )

            await self._validate_cognitive_done(spec, board)

            resource_gate = ResourceGateService(self.db)
            await resource_gate.validate_or_raise_spec_architecture_validation_resource(
                spec.board_id,
                spec.id,
                board=board,
                phase="spec_done",
            )
            await resource_gate.validate_or_raise_spec_resource_task_coverage(
                spec.board_id,
                spec.id,
                phase="spec_done",
                enabled=resource_gate.is_spec_resource_task_coverage_required(board),
            )
            # AFG na spec (investigacao 2026-06-10): specs com findings de
            # arquitetura ativos completavam - o finding gate so rodava em
            # card/ideation/refinement via entity_completion.
            await resource_gate.validate_or_raise_architecture_findings(
                spec.board_id,
                "spec",
                spec.id,
                phase="spec_done",
            )

        await GuidelineService(self.db).enforce_policy_transition(
            board_id=spec.board_id,
            entity_type="spec",
            subject_id=spec.id,
            from_status=spec.status.value,
            to_status=data.status.value,
        )

        # Every Spec lifecycle write shares the board dependency-graph fence.
        # This prevents a prerequisite Done→Draft transition from racing a
        # dependent's readiness check. The caller-owned transaction keeps the
        # fence held through the row fence, status write and commit.
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )
        from okto_pulse.core.services.spec_dependency import SpecDependencyService

        dependency_service = SpecDependencyService(
            require_relational_application_adapter().spec_dependencies(self.db),
            self.db,
        )
        await dependency_service.acquire_lifecycle_write_fence(board_id=spec.board_id)

        if transition_starts_spec_execution(spec.status, data.status):
            await dependency_service.require_ready_for_execution(
                board_id=spec.board_id,
                spec_id=spec.id,
                mark_started=True,
                expected_edition=int(getattr(spec, "edition", 1) or 1),
                acquire_graph_lock=False,
            )

        # Run expensive read-only gates before acquiring the write lock, then
        # atomically recheck the originally loaded lifecycle authority.  The
        # persistence adapter implements this as a conditional no-op UPDATE,
        # which serializes writers until the caller-owned transaction commits.
        if not await _application_fence(
            self.db,
            "spec",
            spec.id,
            expected_values=lifecycle_fence,
        ):
            raise LifecycleTransitionConflictError("spec", spec.id)

        # Assessment writers serialize on the same subject row.  Re-evaluate
        # only the cheap mutable heads after acquiring the lifecycle fence so
        # a concurrent PASS -> FAIL replacement cannot be promoted.
        if data.status == SpecStatus.VALIDATED:
            await self._enforce_spec_checklist_gate(spec, surface="move_spec")
        await _record_critical_context_decision(
            self.db,
            decision=critical_context_decision,
            actor_name=critical_actor_name,
            actor_type="user",
        )

        old_status = spec.status
        old_edition = int(getattr(spec, "edition", 1) or 1)
        old_version = spec.version

        # ``edition`` is the human-facing lifecycle counter. It advances only
        # when a Spec enters draft from a non-draft state; content mutations
        # continue to advance the independent technical ``version`` token.
        spec.edition = next_lifecycle_edition(
            old_edition,
            from_status=old_status,
            to_status=data.status,
        )
        opened_new_edition = int(spec.edition) != old_edition

        # Reopening a terminal Spec starts a fresh editable iteration, matching
        # the lifecycle registry contract and the ideation/refinement behavior.
        if data.status == SpecStatus.DRAFT and old_status in (
            SpecStatus.DONE,
            SpecStatus.CANCELLED,
        ):
            spec.version += 1

        # Cancellation justification (ITEM 17): cancel requires a reason
        # (replacing any previous one); reopening clears it.
        apply_cancellation_policy(
            spec,
            entity_type="spec",
            from_status=old_status,
            to_status=data.status,
            reason=getattr(data, "cancellation_reason", None),
            actor_id=user_id,
        )

        spec.status = data.status

        # Opening Draft starts a new human edition. Its current projection is
        # empty; immutable validation attempts remain in ``validations``.
        if opened_new_edition:
            spec.current_validation_id = None
            await _application_flush(self.db)

        lifecycle_action = (
            "cancel"
            if data.status == SpecStatus.CANCELLED
            else "reopen"
            if (data.status == SpecStatus.DRAFT and old_status != SpecStatus.DRAFT)
            else "admit_validation"
            if (
                data.status == SpecStatus.APPROVED and old_status != SpecStatus.APPROVED
            )
            else None
        )
        if lifecycle_action is not None:
            await _apply_quality_assessment_lifecycle_transition(
                self.db,
                board_id=spec.board_id,
                subject_type="spec",
                subject_id=spec.id,
                before_version=old_version,
                before_status=old_status.value,
                before_archived=False,
                after_version=spec.version,
                after_status=spec.status.value,
                after_archived=False,
                action=lifecycle_action,
                actor_id=user_id,
                before_edition=old_edition,
                after_edition=int(spec.edition),
            )

        if old_status != data.status:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import SpecMoved, SpecVersionBumped

            if spec.version != old_version:
                await event_publish(
                    SpecVersionBumped(
                        board_id=spec.board_id,
                        actor_id=user_id,
                        spec_id=spec.id,
                        old_version=old_version,
                        new_version=spec.version,
                        changed_fields=["status"],
                    ),
                    session=self.db,
                )

            await event_publish(
                SpecMoved(
                    board_id=spec.board_id,
                    actor_id=user_id,
                    spec_id=spec.id,
                    from_status=old_status.value,
                    to_status=data.status.value,
                ),
                session=self.db,
            )

        resolved_name = actor_name or await resolve_actor_name(
            self.db, user_id, spec.board_id
        )
        await self._log_activity(
            board_id=spec.board_id,
            action="spec_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "spec_id": spec_id,
                "from_status": old_status.value,
                "to_status": data.status.value,
                "edition": int(getattr(spec, "edition", old_edition)),
                "technical_revision": spec.version,
            },
        )
        history_changes = [
            {"field": "status", "old": old_status.value, "new": data.status.value}
        ]
        if int(getattr(spec, "edition", old_edition)) != old_edition:
            history_changes.append(
                {
                    "field": "edition",
                    "old": old_edition,
                    "new": int(spec.edition),
                }
            )
        await self._record_history(
            spec_id=spec_id,
            action="status_changed",
            actor_id=user_id,
            actor_name=resolved_name,
            changes=history_changes,
            summary=f"Status: {old_status.value} → {data.status.value}",
            version=spec.version,
        )
        return spec

    async def delete_spec(
        self,
        spec_id: str,
        user_id: str,
        *,
        return_receipt: bool = False,
    ) -> bool | GovernedArtifactDeletionReceipt:
        """Delete a spec. Unlinks cards but doesn't delete them."""
        spec = await self.get_spec(spec_id)
        if not spec:
            return False

        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )
        from okto_pulse.core.services.spec_dependency import SpecDependencyService

        await SpecDependencyService(
            require_relational_application_adapter().spec_dependencies(self.db),
            self.db,
        ).require_no_incoming_active(
            board_id=spec.board_id,
            target_spec_ids=(spec.id,),
            operation="delete Spec",
        )

        # Unlink cards. A deleted spec also owns the scenario ids stored on
        # those cards; retaining them would leave dangling references that can
        # make later coverage gates appear satisfied. Remove only ids that
        # belonged to this spec so any independently valid references survive.
        deleted_scenario_ids = {
            str(scenario["id"])
            for scenario in (spec.test_scenarios or [])
            if isinstance(scenario, dict) and scenario.get("id")
        }
        linked_cards = await _application_list(
            self.db,
            "card",
            filters=(_apf("spec_id", "eq", spec_id),),
        )
        for linked_card in linked_cards:
            require_card_operational_mutation_allowed(
                linked_card,
                operation="delete_spec_unlink_card",
            )
        for linked_card in linked_cards:
            await _reset_v2_knowledge_for_relink(
                self.db,
                board_id=spec.board_id,
                target_type="card",
                target_id=linked_card.id,
                previous_parent=("spec", spec_id),
                next_parent=None,
                actor_id=user_id,
                port=self._knowledge_propagation_port,
            )
        for linked_card in linked_cards:
            existing_scenario_ids = list(linked_card.test_scenario_ids or [])
            remaining_scenario_ids = [
                scenario_id
                for scenario_id in existing_scenario_ids
                if str(scenario_id) not in deleted_scenario_ids
            ]
            if remaining_scenario_ids != existing_scenario_ids:
                linked_card.test_scenario_ids = remaining_scenario_ids
                linked_card.mark_dirty("test_scenario_ids")
            linked_card.spec_id = None
        await _application_flush(self.db)

        board_id = spec.board_id
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        # SQL cascade physically removes every sprint owned by this spec.
        # Mint each child takedown before deleting the parent so canonical
        # ``sprint:{id}`` nodes cannot survive until a later catch-up sweep.
        linked_sprints = await _application_list(
            self.db,
            "sprint",
            filters=(_apf("spec_id", "eq", spec_id),),
        )
        descendant_deletions: list[GovernedArtifactDeletionReceipt] = []
        for linked_sprint in linked_sprints:
            descendant_deletions.append(
                await _prepare_governed_artifact_deletion(
                    self.db,
                    board_id=board_id,
                    artifact_type="sprint",
                    artifact_id=linked_sprint.id,
                )
            )
        takedown_receipt = replace(
            await _prepare_governed_artifact_deletion(
                self.db,
                board_id=board_id,
                artifact_type="spec",
                artifact_id=spec_id,
            ),
            descendant_deletions=tuple(descendant_deletions),
        )
        await _purge_quality_assessment_subject(
            self.db,
            board_id=board_id,
            subject_type="spec",
            subject_id=spec_id,
        )
        await _application_delete(self.db, spec)

        await self._log_activity(
            board_id=board_id,
            action="spec_deleted",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"spec_id": spec_id},
        )
        return takedown_receipt if return_receipt else True

    async def link_card(
        self, spec_id: str, card_id: str, user_id: str | None = None
    ) -> bool:
        """Link an existing card to a spec. Spec must be in 'approved', 'in_progress', or 'done' status.

        Spec eaf78891 (Ideação #2): emits CardLinkedToSpec on success so the
        ConsolidationEnqueuer re-enqueues the SPEC (not the card) — the spec
        extractor reflects the updated cards list while the card extractor
        does not reference spec_id.
        """
        spec = await _application_get(self.db, "spec", spec_id)
        if not spec:
            return False
        if spec.status not in (
            SpecStatus.APPROVED,
            SpecStatus.VALIDATED,
            SpecStatus.IN_PROGRESS,
            SpecStatus.DONE,
        ):
            raise ValueError(
                f"Cards can only be linked to a spec in 'approved', 'validated', 'in_progress', or 'done' status (current: '{spec.status.value}')"
            )
        card = await _application_get(self.db, "card", card_id)
        if not card or card.board_id != spec.board_id:
            return False
        require_card_operational_mutation_allowed(card, operation="link_card_to_spec")
        old_spec_id = card.spec_id
        actor_id = user_id or card.created_by
        knowledge_v2_relinked = await _reset_v2_knowledge_for_relink(
            self.db,
            board_id=spec.board_id,
            target_type="card",
            target_id=card.id,
            previous_parent=(None if old_spec_id is None else ("spec", old_spec_id)),
            next_parent=("spec", spec_id),
            actor_id=actor_id,
            port=self._knowledge_propagation_port,
        )
        card.spec_id = spec_id
        await _application_flush(self.db)

        await SpecResourcePropagationService(self.db).propagate_for_card(
            board_id=spec.board_id,
            spec_id=spec_id,
            card_id=card_id,
            actor_id=actor_id,
            trigger="card_linked_to_spec",
            excluded_resource_types=(
                {"knowledge_base"} if knowledge_v2_relinked else None
            ),
        )

        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import CardLinkedToSpec

        await event_publish(
            CardLinkedToSpec(
                board_id=spec.board_id,
                actor_id=user_id,
                card_id=card_id,
                spec_id=spec_id,
            ),
            session=self.db,
        )
        return True

    async def unlink_card(self, card_id: str, user_id: str | None = None) -> bool:
        """Unlink a card from its spec.

        Spec eaf78891 (Ideação #2): emits CardUnlinkedFromSpec so the
        ConsolidationEnqueuer re-enqueues the (now-orphaned) spec for
        re-extraction.
        """
        card = await _application_get(self.db, "card", card_id)
        if not card or not card.spec_id:
            return False
        require_card_operational_mutation_allowed(
            card,
            operation="unlink_card_from_spec",
        )
        old_spec_id = card.spec_id
        await _reset_v2_knowledge_for_relink(
            self.db,
            board_id=card.board_id,
            target_type="card",
            target_id=card.id,
            previous_parent=("spec", old_spec_id),
            next_parent=None,
            actor_id=user_id or card.created_by,
            port=self._knowledge_propagation_port,
        )
        card.spec_id = None

        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import CardUnlinkedFromSpec

        await event_publish(
            CardUnlinkedFromSpec(
                board_id=card.board_id,
                actor_id=user_id,
                card_id=card_id,
                spec_id=old_spec_id,
            ),
            session=self.db,
        )
        return True

    # ---- Spec Validation Gate ----

    @staticmethod
    def _resolve_spec_validation_config(board: Board | None) -> dict[str, Any]:
        """Resolve Spec Validation Gate thresholds from board settings.

        Defaults are more rigorous than the Task Validation Gate (70/80/50)
        because poor spec quality has amplified downstream cost.
        """
        settings = (board.settings if board else None) or {}
        return {
            "require_spec_validation": bool(
                settings.get("require_spec_validation", True)
            ),
            "min_spec_confidence": int(settings.get("min_spec_confidence", 70)),
            "min_spec_clarity": int(settings.get("min_spec_clarity", 80)),
            "min_spec_completeness": int(settings.get("min_spec_completeness", 80)),
            "min_spec_assertiveness": int(settings.get("min_spec_assertiveness", 80)),
            "min_spec_decidability": int(settings.get("min_spec_decidability", 80)),
            "max_spec_ambiguity": int(settings.get("max_spec_ambiguity", 30)),
        }

    async def submit_spec_validation(
        self,
        spec_id: str,
        reviewer_id: str,
        reviewer_name: str,
        data: dict,
    ) -> dict:
        """Submit a Spec Validation Gate record for a spec in 'approved' status.

        Mirrors CardService.submit_task_validation: runs coverage gates as
        pre-requisite, computes outcome atomically, appends to spec.validations
        array (append-only history), sets current_validation_id, and on success
        atomically moves spec.status to validated.

        Outcome rule: failed if any threshold violated OR recommendation=reject;
        success only if ALL thresholds ok AND recommendation=approve.
        """
        import uuid as _uuid

        # Preserve read-your-writes when callers intentionally compose more
        # than one service command in the same UoW. The late fence must compare
        # against this transaction's latest authoritative projection.
        await _application_flush(self.db)
        spec = await self.get_spec(spec_id)
        if not spec:
            raise ValueError("Spec not found")

        lifecycle_fence = {
            "status": spec.status,
            "edition": int(getattr(spec, "edition", 1) or 1),
            "version": int(spec.version),
            "archived": bool(getattr(spec, "archived", False)),
            "current_validation_id": spec.current_validation_id,
        }

        validation_edition = int(getattr(spec, "edition", 1) or 1)
        previous_head_revision = 0
        for previous_validation in list(spec.validations or []):
            if previous_validation.get("edition") != validation_edition:
                continue
            candidate_revision = previous_validation.get("head_revision")
            if (
                isinstance(candidate_revision, int)
                and not isinstance(candidate_revision, bool)
                and candidate_revision > previous_head_revision
            ):
                previous_head_revision = candidate_revision

        expected_edition = data.get("expected_validation_edition")
        expected_version = data.get("expected_spec_version")
        expected_head_revision = data.get("expected_head_revision")
        if expected_edition != validation_edition:
            raise SpecValidationEditionConflict(
                "Spec validation edition changed; refresh the validation cycle.",
                details={"expected": expected_edition, "current": validation_edition},
            )
        if expected_version != spec.version:
            raise SpecValidationVersionConflict(
                "Spec version changed; refresh the validation cycle.",
                details={"expected": expected_version, "current": spec.version},
            )
        if expected_head_revision != previous_head_revision:
            raise SpecValidationGateNotReady(
                "Spec validation head changed; refresh the validation cycle.",
                details={
                    "reason": "head_revision_conflict",
                    "expected": expected_head_revision,
                    "current": previous_head_revision,
                },
            )

        if spec.status != SpecStatus.APPROVED:
            raise SpecValidationGateNotReady(
                "Spec must be approved before validation "
                f"(currently '{spec.status.value}').",
                details={"reason": "subject_not_approved", "status": spec.status.value},
            )

        board = await _application_get(self.db, "board", spec.board_id)
        config = self._resolve_spec_validation_config(board)
        if not config["require_spec_validation"]:
            raise SpecValidationGateNotReady(
                "This board does not require spec validation. "
                "To advance the spec without the gate: call "
                "move_spec(spec_id, status='validated'). "
                "To enforce the gate first: enable 'require_spec_validation' "
                "in board settings, then re-submit.",
                details={"reason": "gate_disabled"},
            )

        critical_actor_type = (
            "agent" if reviewer_name and "agent" in reviewer_name.lower() else "user"
        )
        critical_actor_name = reviewer_name or await resolve_actor_name(
            self.db, reviewer_id, spec.board_id
        )
        critical_context_decision = await _authorize_critical_context_or_raise(
            self.db,
            board_id=spec.board_id,
            actor_id=reviewer_id,
            entity_type="spec",
            entity_id=spec.id,
            critical_action=CriticalAction.SPEC_SUBMIT_VALIDATION,
            surface="service",
            actor_type=critical_actor_type,
            actor_name=critical_actor_name,
            defer_success_audit=True,
        )

        # Finding count is advisory, but one externally accepted lint result
        # is mandatory for the exact human lifecycle edition.
        await self._enforce_spec_requirement_lint_gate(spec)

        # Run coverage gates as pre-requisite — reuses existing CardService checks.
        # AC→Scenario coverage must run FIRST so uncovered ACs are caught before
        # the spec gets locked by a successful validation (the move→done gate
        # checks the same thing, but by then the spec is already locked).
        try:
            card_service = CardService(self.db)
            await card_service.check_ac_scenario_coverage(spec, board)
            await card_service.check_test_coverage(spec, board)
            await card_service.check_rules_coverage(spec, board)
            await card_service.check_trs_coverage(spec, board)
            await card_service.check_contract_coverage(spec, board)
            await card_service.check_ir_coverage(spec, board)
            await card_service.check_or_coverage(spec, board)
            await card_service.check_task_requirement_links_for_spec(spec, board)
            await card_service.check_decision_presence(spec)
            await card_service.check_decisions_coverage(spec, board)
            await card_service.check_code_evidence_coverage(spec, board)
            resource_gate = ResourceGateService(self.db)
            await resource_gate.validate_or_raise_spec_architecture_validation_resource(
                spec.board_id,
                spec.id,
                board=board,
                phase="spec_validation",
            )
            await resource_gate.validate_or_raise_spec_resource_task_coverage(
                spec.board_id,
                spec.id,
                phase="spec_validation",
                enabled=resource_gate.is_spec_resource_task_coverage_required(board),
            )
            await self._enforce_spec_checklist_gate(
                spec,
                surface="submit_spec_validation",
            )
        except SpecValidationGateNotReady:
            raise
        except CodeTraceabilityContractError as exc:
            traceability_details = dict(exc.details)
            detail_reason = traceability_details.pop("reason", None)
            if detail_reason is not None:
                traceability_details["technical_reason"] = detail_reason
            raise SpecValidationGateNotReady(
                str(exc) or "Code Evidence Matrix coverage is not ready.",
                details={"reason": exc.code, **traceability_details},
            ) from exc
        except Exception as exc:
            raise SpecValidationGateNotReady(
                str(exc) or "Spec validation prerequisites are not ready.",
                details={"reason": type(exc).__name__},
            ) from exc

        # Five evaluator-supplied dimensions are the canonical contract. The
        # score/summary and completeness shapes remain compatibility inputs so
        # immutable records created by older clients stay readable/replayable.
        canonical_validation_fields = (
            "confidence",
            "confidence_justification",
            "clarity",
            "clarity_justification",
            "assertiveness",
            "assertiveness_justification",
            "decidability",
            "decidability_justification",
            "ambiguity",
            "ambiguity_justification",
            "recommendation",
        )
        canonical_marker_fields = (
            "confidence",
            "confidence_justification",
            "clarity",
            "clarity_justification",
            "decidability",
            "decidability_justification",
            "pinpoints",
        )
        legacy_validation_fields = (
            "completeness",
            "completeness_justification",
            "assertiveness",
            "assertiveness_justification",
            "ambiguity",
            "ambiguity_justification",
            "general_justification",
            "recommendation",
        )
        formal_submission = (
            data.get("score") is not None or data.get("summary") is not None
        )
        canonical_submission = any(
            data.get(field) is not None for field in canonical_marker_fields
        )
        legacy_submission = any(
            data.get(field) is not None for field in legacy_validation_fields
        )
        if formal_submission and (canonical_submission or legacy_submission):
            raise ValueError(
                "formal and legacy validation shapes are mutually exclusive"
            )
        if canonical_submission and any(
            data.get(field) is not None
            for field in (
                "completeness",
                "completeness_justification",
                "general_justification",
            )
        ):
            raise ValueError(
                "canonical and legacy validation shapes are mutually exclusive"
            )
        score: float | None = None
        human_summary: str | None = None
        confidence: int | None = None
        clarity: int | None = None
        completeness: int | None = None
        assertiveness: int | None = None
        decidability: int | None = None
        ambiguity: int | None = None
        pinpoints: list[dict[str, Any]] = []
        recommendation: str | None = None
        violations: list[str] = []
        if formal_submission:
            if data.get("score") is None or data.get("summary") is None:
                raise ValueError("score and summary are required together")
            score = float(data["score"])
            human_summary = str(data["summary"]).strip()
            outcome = "success"
        elif canonical_submission:
            missing = [
                field
                for field in canonical_validation_fields
                if data.get(field) is None
            ]
            if missing:
                raise ValueError("Missing required fields: " + ", ".join(missing))
            for name in (
                "confidence",
                "clarity",
                "assertiveness",
                "decidability",
                "ambiguity",
            ):
                raw_score = data[name]
                if not isinstance(raw_score, int) or isinstance(raw_score, bool):
                    raise ValueError(f"{name} must be between 0 and 100")
            confidence = int(data["confidence"])
            clarity = int(data["clarity"])
            assertiveness = int(data["assertiveness"])
            decidability = int(data["decidability"])
            ambiguity = int(data["ambiguity"])
            recommendation = data["recommendation"]
            if recommendation not in ("approve", "reject"):
                raise ValueError("recommendation must be 'approve' or 'reject'")
            for name, dimension_score in (
                ("confidence", confidence),
                ("clarity", clarity),
                ("assertiveness", assertiveness),
                ("decidability", decidability),
                ("ambiguity", ambiguity),
            ):
                if not (0 <= dimension_score <= 100):
                    raise ValueError(f"{name} must be between 0 and 100")
                justification = data.get(f"{name}_justification")
                if (
                    not isinstance(justification, str)
                    or len(justification.strip()) < 10
                ):
                    raise ValueError(
                        f"{name}_justification must be at least 10 characters"
                    )
            from okto_pulse.core.domain.spec_validation import (
                SpecValidationPinpoint,
            )

            raw_pinpoints = data.get("pinpoints") or []
            if not isinstance(raw_pinpoints, list):
                raise ValueError("pinpoints must be a list")
            pinpoint_identities: set[tuple[str | None, ...]] = set()
            for raw_pinpoint in raw_pinpoints:
                required_pinpoint_fields = {"metric", "anchor_type", "detail"}
                allowed_pinpoint_fields = {
                    *required_pinpoint_fields,
                    "anchor_ref",
                    "anchor_snapshot",
                }
                if (
                    not isinstance(raw_pinpoint, dict)
                    or not required_pinpoint_fields.issubset(raw_pinpoint)
                    or not set(raw_pinpoint).issubset(allowed_pinpoint_fields)
                ):
                    raise ValueError("spec_validation_pinpoint_invalid")
                pinpoint = SpecValidationPinpoint.from_dict(raw_pinpoint)
                projected_pinpoint = pinpoint.to_dict()
                pinpoint_identity = (
                    projected_pinpoint["metric"],
                    projected_pinpoint["anchor_type"],
                    projected_pinpoint.get("anchor_ref"),
                    projected_pinpoint["detail"],
                )
                if pinpoint_identity in pinpoint_identities:
                    raise ValueError("spec_validation_pinpoint_duplicate")
                pinpoint_identities.add(pinpoint_identity)
                pinpoints.append(projected_pinpoint)
            if confidence < config["min_spec_confidence"]:
                violations.append(
                    f"confidence {confidence} < min {config['min_spec_confidence']}"
                )
            if clarity < config["min_spec_clarity"]:
                violations.append(
                    f"clarity {clarity} < min {config['min_spec_clarity']}"
                )
            if assertiveness < config["min_spec_assertiveness"]:
                violations.append(
                    "assertiveness "
                    f"{assertiveness} < min {config['min_spec_assertiveness']}"
                )
            if decidability < config["min_spec_decidability"]:
                violations.append(
                    "decidability "
                    f"{decidability} < min {config['min_spec_decidability']}"
                )
            if ambiguity > config["max_spec_ambiguity"]:
                violations.append(
                    f"ambiguity {ambiguity} > max {config['max_spec_ambiguity']}"
                )
            outcome = (
                "failed" if violations or recommendation == "reject" else "success"
            )
        else:
            completeness = int(data["completeness"])
            assertiveness = int(data["assertiveness"])
            ambiguity = int(data["ambiguity"])
            recommendation = data["recommendation"]
            if recommendation not in ("approve", "reject"):
                raise ValueError("recommendation must be 'approve' or 'reject'")
            for name, dimension_score in (
                ("completeness", completeness),
                ("assertiveness", assertiveness),
                ("ambiguity", ambiguity),
            ):
                if not (0 <= dimension_score <= 100):
                    raise ValueError(f"{name} must be between 0 and 100")
            if completeness < config["min_spec_completeness"]:
                violations.append(
                    f"completeness {completeness} < min {config['min_spec_completeness']}"
                )
            if assertiveness < config["min_spec_assertiveness"]:
                violations.append(
                    f"assertiveness {assertiveness} < min {config['min_spec_assertiveness']}"
                )
            if ambiguity > config["max_spec_ambiguity"]:
                violations.append(
                    f"ambiguity {ambiguity} > max {config['max_spec_ambiguity']}"
                )
            outcome = (
                "failed" if violations or recommendation == "reject" else "success"
            )

        if outcome == "success":
            try:
                await GuidelineService(self.db).enforce_policy_transition(
                    board_id=spec.board_id,
                    entity_type="spec",
                    subject_id=spec.id,
                    from_status=spec.status.value,
                    to_status=SpecStatus.VALIDATED.value,
                )
            except Exception as exc:
                raise SpecValidationGateNotReady(
                    "Spec policy compliance is not ready.",
                    details={"reason": type(exc).__name__},
                ) from exc

        if outcome == "success":
            from okto_pulse.core.ports.relational_application import (
                require_relational_application_adapter,
            )
            from okto_pulse.core.services.spec_dependency import SpecDependencyService

            await SpecDependencyService(
                require_relational_application_adapter().spec_dependencies(self.db),
                self.db,
            ).acquire_lifecycle_write_fence(board_id=spec.board_id)

        # Keep database write-lock time short: evaluate every prerequisite
        # first, then serialize the immutable head append immediately before
        # mutation.  A concurrent validation changes current_validation_id;
        # returning to Draft also changes status/edition, so the loser writes
        # neither history nor events and can safely refresh/retry.
        if not await _application_fence(
            self.db,
            "spec",
            spec.id,
            expected_values=lifecycle_fence,
        ):
            raise SpecValidationGateNotReady(
                "Spec changed while validation was being evaluated; refresh the "
                "validation cycle.",
                details={"reason": "lifecycle_fence_conflict"},
            )

        # Quality heads are independent append-only authorities.  Their
        # writers lock the Spec row, so after this fence they cannot change
        # until commit; recheck only those cheap heads before promotion.
        await self._enforce_spec_requirement_lint_gate(spec)
        try:
            await self._enforce_spec_checklist_gate(
                spec,
                surface="submit_spec_validation",
            )
        except SpecValidationGateNotReady:
            raise
        except Exception as exc:
            raise SpecValidationGateNotReady(
                str(exc) or "Spec validation prerequisites are not ready.",
                details={"reason": type(exc).__name__},
            ) from exc
        await _record_critical_context_decision(
            self.db,
            decision=critical_context_decision,
            actor_name=critical_actor_name,
            actor_type=critical_actor_type,
        )

        # Build the immutable validation record. Human edition is independent
        # from the technical subject version, while head_revision advances for
        # each attempt within the same edition.
        validation_id = f"val_{_uuid.uuid4().hex[:8]}"
        subject_version = int(spec.version)
        head_revision = previous_head_revision + 1
        resolved_thresholds = (
            {
                "min_spec_confidence": config["min_spec_confidence"],
                "min_spec_clarity": config["min_spec_clarity"],
                "min_spec_assertiveness": config["min_spec_assertiveness"],
                "min_spec_decidability": config["min_spec_decidability"],
                "max_spec_ambiguity": config["max_spec_ambiguity"],
            }
            if canonical_submission
            else {
                "min_spec_completeness": config["min_spec_completeness"],
                "min_spec_assertiveness": config["min_spec_assertiveness"],
                "max_spec_ambiguity": config["max_spec_ambiguity"],
            }
        )
        validation: dict[str, Any] = {
            "id": validation_id,
            "validation_id": validation_id,
            "spec_id": spec_id,
            "board_id": spec.board_id,
            "reviewer_id": reviewer_id,
            "reviewer_name": reviewer_name,
            "outcome": outcome,
            "edition": validation_edition,
            "validation_edition": validation_edition,
            "is_current": True,
            "receipt_id": validation_id,
            "subject_version": subject_version,
            "head_revision": head_revision,
            "digests": {},
            "threshold_violations": violations,
            "resolved_thresholds": resolved_thresholds,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if formal_submission:
            validation.update({"score": score, "summary": human_summary})
        elif canonical_submission:
            validation.update(
                {
                    "confidence": confidence,
                    "confidence_justification": data[
                        "confidence_justification"
                    ].strip(),
                    "clarity": clarity,
                    "clarity_justification": data["clarity_justification"].strip(),
                    "assertiveness": assertiveness,
                    "assertiveness_justification": data[
                        "assertiveness_justification"
                    ].strip(),
                    "decidability": decidability,
                    "decidability_justification": data[
                        "decidability_justification"
                    ].strip(),
                    "ambiguity": ambiguity,
                    "ambiguity_justification": data["ambiguity_justification"].strip(),
                    "pinpoints": pinpoints,
                    "recommendation": recommendation,
                }
            )
        else:
            validation.update(
                {
                    "completeness": completeness,
                    "completeness_justification": data[
                        "completeness_justification"
                    ].strip(),
                    "assertiveness": assertiveness,
                    "assertiveness_justification": data[
                        "assertiveness_justification"
                    ].strip(),
                    "ambiguity": ambiguity,
                    "ambiguity_justification": data["ambiguity_justification"].strip(),
                    "general_justification": data["general_justification"].strip(),
                    "recommendation": recommendation,
                }
            )

        # Append-only: never overwrite history. flag_modified is required for JSONB.
        old_current_validation_id = spec.current_validation_id
        validations = list(spec.validations or [])
        validations.append(validation)
        spec.validations = validations
        spec.mark_dirty("validations")
        spec.current_validation_id = validation_id

        # Atomic state transition on success — same transaction as the persist.
        old_status = spec.status
        if outcome == "success":
            spec.status = SpecStatus.VALIDATED
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import SpecMoved, SpecSemanticChanged

            await event_publish(
                SpecMoved(
                    board_id=spec.board_id,
                    actor_id=reviewer_id,
                    spec_id=spec.id,
                    from_status=old_status.value,
                    to_status=spec.status.value,
                ),
                session=self.db,
            )
            await event_publish(
                SpecSemanticChanged(
                    board_id=spec.board_id,
                    actor_id=reviewer_id,
                    spec_id=spec.id,
                    changed_fields=["status"],
                ),
                session=self.db,
            )

        # Activity log
        await self._log_activity(
            board_id=spec.board_id,
            action="spec_validation_submitted",
            actor_type="agent"
            if reviewer_name and "agent" in reviewer_name.lower()
            else "user",
            actor_id=reviewer_id,
            actor_name=reviewer_name,
            details={
                "spec_id": spec_id,
                "validation_id": validation_id,
                "outcome": outcome,
                **(
                    {"score": score}
                    if formal_submission
                    else (
                        {
                            "recommendation": recommendation,
                            "confidence": confidence,
                            "clarity": clarity,
                            "assertiveness": assertiveness,
                            "decidability": decidability,
                            "ambiguity": ambiguity,
                            "pinpoint_count": len(pinpoints),
                        }
                        if canonical_submission
                        else {
                            "recommendation": recommendation,
                            "completeness": completeness,
                            "assertiveness": assertiveness,
                            "ambiguity": ambiguity,
                        }
                    )
                ),
                "threshold_violations": violations,
                "edition": spec.edition,
                "subject_version": subject_version,
                "head_revision": head_revision,
                "from_status": old_status.value,
                "to_status": spec.status.value,
            },
        )
        history_changes = [
            {
                "field": "current_validation_id",
                "old": old_current_validation_id,
                "new": validation_id,
            }
        ]
        if old_status != spec.status:
            history_changes.append(
                {
                    "field": "status",
                    "old": old_status.value,
                    "new": spec.status.value,
                }
            )
        await self._record_history(
            spec_id=spec_id,
            action="validation_submitted",
            actor_id=reviewer_id,
            actor_name=reviewer_name,
            actor_type=(
                "agent"
                if reviewer_name and "agent" in reviewer_name.lower()
                else "user"
            ),
            changes=history_changes,
            summary=(
                f"Validation submitted: {outcome} ({score})"
                if formal_submission
                else (
                    (
                        f"Validation submitted: {outcome} "
                        f"({recommendation}; {confidence}/{clarity}/"
                        f"{assertiveness}/{decidability}/{ambiguity})"
                    )
                    if canonical_submission
                    else (
                        f"Validation submitted: {outcome} "
                        f"({recommendation}; "
                        f"{completeness}/{assertiveness}/{ambiguity})"
                    )
                )
            ),
            version=spec.version,
        )

        return {
            **validation,
            "spec_status": spec.status.value,
            "active": True,
            "lifecycle_state": "current",
        }

    async def list_spec_validations(
        self,
        spec_id: str,
        *,
        limit: int = 50,
        offset: int = 0,
        lifecycle_state: str = "all",
    ) -> dict[str, Any]:
        """List all spec validations in reverse chronological order.

        Returns a dict with current_validation_id and validations list where
        each record has an 'active' flag indicating if it's the current pointer.
        """
        spec = await self.get_spec(spec_id)
        if not spec:
            raise ValueError("Spec not found")

        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
        ):
            raise ValueError("limit must be between 1 and 100")
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise ValueError("offset must be greater than or equal to 0")
        if lifecycle_state not in {"all", "current", "previous", "history_only"}:
            raise ValueError(
                "lifecycle_state must be one of: all, current, previous, history_only"
            )

        validations = list(spec.validations or [])
        pointer_id = getattr(spec, "current_validation_id", None)
        pointer = next((v for v in validations if v.get("id") == pointer_id), None)
        current_id = (
            pointer_id
            if pointer is not None
            and is_current_edition(pointer.get("edition"), spec.edition)
            else None
        )

        # Reverse chronological order; legacy NULL editions are intentionally
        # visible only as history and can never become the current validation.
        projected = []
        for v in reversed(validations):
            active = v.get("id") == current_id
            item_lifecycle_state = (
                "history_only"
                if v.get("edition") is None
                else ("current" if active else "previous")
            )
            projected.append(
                {
                    **v,
                    # Stored rows are immutable attempts. Currentness is a
                    # projection of the live pointer, never a historical fact.
                    "is_current": active,
                    "active": active,
                    "lifecycle_state": item_lifecycle_state,
                }
            )

        current_validation = next(
            (item for item in projected if item["lifecycle_state"] == "current"),
            None,
        )
        if lifecycle_state == "all":
            filtered = projected
        elif lifecycle_state == "previous":
            # Legacy NULL-edition rows are immutable history and belong to the
            # human-facing previous-results collection.
            filtered = [
                item
                for item in projected
                if item["lifecycle_state"] in {"previous", "history_only"}
            ]
        else:
            filtered = [
                item for item in projected if item["lifecycle_state"] == lifecycle_state
            ]
        total = len(filtered)
        result_list = filtered[offset : offset + limit]

        return {
            "current_validation_id": current_id,
            "current_edition": spec.edition,
            "current_validation": current_validation,
            "previous_count": sum(
                1 for item in projected if item["lifecycle_state"] != "current"
            ),
            "total": total,
            "limit": limit,
            "offset": offset,
            "lifecycle_state": lifecycle_state,
            "has_more": offset + len(result_list) < total,
            "validations": result_list,
        }

    # Dimensões qualitativas da spec evaluation — fonte única compartilhada
    # entre o endpoint REST e o MCP tool okto_pulse_submit_spec_evaluation.
    SPEC_EVALUATION_DIMENSIONS: tuple[tuple[str, str], ...] = (
        ("breakdown_completeness", "breakdown_justification"),
        ("granularity", "granularity_justification"),
        ("dependency_coherence", "dependency_justification"),
        ("test_coverage_quality", "test_coverage_justification"),
    )
    SPEC_EVALUATION_RECOMMENDATIONS: tuple[str, ...] = (
        "approve",
        "request_changes",
        "reject",
    )

    async def submit_spec_evaluation(
        self,
        spec_id: str,
        actor_id: str,
        actor_name: str,
        data: dict,
        *,
        actor_type: str = "user",
        surface: str = "service",
    ) -> dict:
        """Submit a qualitative evaluation for a spec in 'validated' status.

        Caminho de escrita ÚNICO da spec evaluation — consumido pelo endpoint
        REST ``POST /specs/{id}/evaluations`` e pelo MCP tool
        ``okto_pulse_submit_spec_evaluation`` (paridade REST/MCP; antes o
        tool era o único caminho e usuários UI/REST ficavam presos no gate
        validated→in_progress sem como satisfazê-lo).

        Multiple evaluators can submit independent evaluations (append-only).
        Caller owns the commit. Raises ValueError on status/input problems.
        """
        spec = await self.get_spec(spec_id)
        if not spec:
            raise ValueError("Spec not found")
        if spec.status != SpecStatus.VALIDATED:
            raise ValueError(
                f"Spec must be in 'validated' status to submit evaluations "
                f"(currently '{spec.status.value}')"
            )

        recommendation = data.get("recommendation")
        if recommendation not in self.SPEC_EVALUATION_RECOMMENDATIONS:
            raise ValueError(
                "Recommendation must be one of: "
                + ", ".join(self.SPEC_EVALUATION_RECOMMENDATIONS)
            )
        score_fields = [name for name, _ in self.SPEC_EVALUATION_DIMENSIONS]
        score_fields.append("overall_score")
        for name in score_fields:
            score = int(data[name])
            if not (0 <= score <= 100):
                raise ValueError(f"{name} must be between 0 and 100")

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=spec.board_id,
            actor_id=actor_id,
            entity_type="spec",
            entity_id=spec.id,
            critical_action=CriticalAction.SPEC_SUBMIT_EVALUATION,
            surface=surface,
            actor_type=actor_type,
            actor_name=actor_name,
        )

        import uuid as _uuid

        evaluation = {
            "id": f"eval_{_uuid.uuid4().hex[:8]}",
            "spec_id": spec_id,
            "evaluator_id": actor_id,
            "evaluator_name": actor_name,
            "evaluator_type": actor_type,
            "dimensions": {
                name: {
                    "score": int(data[name]),
                    "justification": data[justification],
                }
                for name, justification in self.SPEC_EVALUATION_DIMENSIONS
            },
            "overall_score": int(data["overall_score"]),
            "overall_justification": data["overall_justification"],
            "recommendation": recommendation,
            "stale": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        evaluations = list(spec.evaluations or [])
        evaluations.append(evaluation)
        spec.evaluations = evaluations
        spec.mark_dirty("evaluations")

        await self._log_activity(
            board_id=spec.board_id,
            action="spec_evaluation_submitted",
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            details={
                "spec_id": spec_id,
                "evaluation_id": evaluation["id"],
                "overall_score": evaluation["overall_score"],
                "recommendation": recommendation,
            },
        )
        return evaluation

    async def list_spec_evaluations(self, spec_id: str) -> dict[str, Any]:
        """List spec evaluations (newest first) with an active (non-stale) count."""
        spec = await self.get_spec(spec_id)
        if not spec:
            raise ValueError("Spec not found")
        evaluations = list(spec.evaluations or [])
        return {
            "spec_id": spec_id,
            "spec_status": spec.status.value,
            "evaluations": list(reversed(evaluations)),
            "active_count": len([e for e in evaluations if not e.get("stale")]),
        }

    async def _log_activity(self, **kwargs: Any) -> None:
        """Log an activity."""
        await _application_add(
            self.db,
            _new_application_record("activity_log", **kwargs),
        )


class SpecQAService:
    """Service for spec Q&A operations."""

    def __init__(self, db: Any):
        self.db = db

    async def get_question(self, qa_id: str) -> ApplicationRecord | None:
        """Get one Spec Q&A item by ID for parent-scope validation."""
        return await _application_get(self.db, "spec_qa_item", qa_id)

    async def create_question(
        self, spec_id: str, user_id: str, data: SpecQACreate
    ) -> ApplicationRecord | None:
        """Create a question on a spec (text or choice)."""
        spec = await _application_get(self.db, "spec", spec_id)
        if not spec:
            return None
        require_draft_mutation(spec, subject_type="spec")
        qa = _new_application_record(
            "spec_qa_item",
            spec_id=spec_id,
            question=data.question,
            question_type=data.question_type or "text",
            choices=[c.model_dump() for c in data.choices] if data.choices else None,
            allow_free_text=data.allow_free_text,
            asked_by=user_id,
        )
        await _application_add(self.db, qa)
        await _publish_quality_clarification_changed(
            self.db,
            subject=spec,
            subject_type="spec",
            qa_id=getattr(qa, "id", None),
            operation="created",
            actor_id=user_id,
        )
        return qa

    async def answer_question(
        self,
        qa_id: str,
        user_id: str,
        data: SpecQAAnswer,
        *,
        actor_type: str = "user",
        surface: str = "service",
    ) -> ApplicationRecord | None:
        """Answer a spec Q&A question (text or choice selection).
        Mirrors IdeationQAService.answer_question — accepts `single_choice`
        as alias of `choice`, and only commits when something was persisted.
        """
        qa = await _application_get(self.db, "spec_qa_item", qa_id)
        if not qa:
            return None

        spec = await _application_get(self.db, "spec", qa.spec_id)
        if spec is None:
            raise RuntimeError("quality_clarification_subject_missing")
        require_draft_mutation(spec, subject_type="spec")
        board = await _application_get(self.db, "board", spec.board_id)
        await _authorize_qa_answer_or_raise(
            self.db,
            board=board,
            qa=qa,
            user_id=user_id,
            entity_type="spec",
            question_id=qa_id,
            actor_type=actor_type,
            surface=surface,
        )

        saved_something = False
        choice_types = ("choice", "single_choice", "multi_choice")
        if qa.question_type in choice_types and data.selected:
            data.selected = validate_choice_selection(
                qa.question_type, data.selected, qa.choices
            )
            qa.selected = data.selected
            saved_something = True

        if data.answer:
            qa.answer = data.answer
            saved_something = True

        if not saved_something:
            return None

        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        await _publish_quality_clarification_changed(
            self.db,
            subject=spec,
            subject_type="spec",
            qa_id=qa.id,
            operation="answered",
            actor_id=user_id,
            actor_type=actor_type,
        )
        return qa

    async def list_qa(self, spec_id: str) -> list[ApplicationRecord]:
        """List all Q&A items for a spec."""
        return await _application_list(
            self.db,
            "spec_qa_item",
            filters=(_apf("spec_id", "eq", spec_id),),
            order_by=(("created_at", False),),
        )

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await _application_get(self.db, "spec_qa_item", qa_id)
        if not qa:
            return False
        spec = await _application_get(self.db, "spec", qa.spec_id)
        if spec is None:
            raise RuntimeError("quality_clarification_subject_missing")
        require_draft_mutation(spec, subject_type="spec")
        await _application_delete(self.db, qa)
        await _publish_quality_clarification_changed(
            self.db,
            subject=spec,
            subject_type="spec",
            qa_id=qa_id,
            operation="deleted",
            actor_id=None,
        )
        return True


class SpecKnowledgeService:
    """Service for spec knowledge base operations."""

    def __init__(self, db: Any):
        self.db = db

    async def create_knowledge(
        self, spec_id: str, user_id: str, data: SpecKnowledgeCreate
    ) -> ApplicationRecord | None:
        """Create a knowledge base item on a spec."""
        governance_metadata = normalize_knowledge_governance_metadata(
            data.governance_metadata
        )
        spec = await _application_get(self.db, "spec", spec_id)
        if not spec:
            return None
        require_draft_mutation(spec, subject_type="spec")
        kb = _new_knowledge_application_record(
            "spec_knowledge_base",
            parent_field="spec_id",
            parent_id=spec_id,
            parent_version=getattr(spec, "version", None),
            title=data.title,
            description=data.description,
            content=data.content,
            mime_type=data.mime_type,
            governance_metadata=governance_metadata,
            created_by=user_id,
        )
        await _application_add(self.db, kb)
        await SpecResourcePropagationService(self.db).propagate_for_spec(
            board_id=spec.board_id,
            spec_id=spec_id,
            actor_id=user_id,
            trigger="spec_knowledge_created",
        )
        return kb

    async def get_knowledge(self, knowledge_id: str) -> ApplicationRecord | None:
        """Get a knowledge base item by ID."""
        return await _application_get(self.db, "spec_knowledge_base", knowledge_id)

    async def list_knowledge(self, spec_id: str) -> list[ApplicationRecord]:
        """List all knowledge base items for a spec."""
        return await _application_list(
            self.db,
            "spec_knowledge_base",
            filters=(_apf("spec_id", "eq", spec_id),),
            order_by=(("created_at", False),),
        )

    async def update_knowledge(
        self, knowledge_id: str, data: SpecKnowledgeUpdate
    ) -> ApplicationRecord | None:
        """Update a knowledge base item."""
        update_data = data.model_dump(exclude_unset=True)
        if "governance_metadata" in update_data:
            update_data["governance_metadata"] = (
                normalize_knowledge_governance_metadata(
                    update_data["governance_metadata"]
                )
            )
        kb = await self.get_knowledge(knowledge_id)
        if not kb:
            return None
        spec = await _application_get(self.db, "spec", kb.spec_id)
        if spec is None:
            raise RuntimeError("knowledge_subject_missing")
        require_draft_mutation(spec, subject_type="spec")
        for key, value in update_data.items():
            setattr(kb, key, value)
        _refresh_knowledge_content_hash(kb)
        await _application_flush(self.db)
        await SpecResourcePropagationService(self.db).propagate_for_spec(
            board_id=spec.board_id,
            spec_id=kb.spec_id,
            actor_id=kb.created_by or "system",
            trigger="spec_knowledge_updated",
        )
        return kb

    async def delete_knowledge(self, knowledge_id: str) -> bool:
        """Delete a knowledge base item."""
        kb = await self.get_knowledge(knowledge_id)
        if not kb:
            return False
        spec_id = kb.spec_id
        kb_id = kb.id
        actor_id = kb.created_by or "system"
        spec = await _application_get(self.db, "spec", spec_id)
        if spec is None:
            raise RuntimeError("knowledge_subject_missing")
        require_draft_mutation(spec, subject_type="spec")
        await _application_delete(self.db, kb)
        await _application_flush(self.db)
        await SpecResourcePropagationService(self.db).propagate_for_spec(
            board_id=spec.board_id,
            spec_id=spec_id,
            actor_id=actor_id,
            trigger="spec_knowledge_deleted",
            removed_kb_ids={kb_id},
        )
        return True


class IdeationKnowledgeService:
    """Service for ideation knowledge base operations."""

    def __init__(self, db: Any):
        self.db = db

    async def create_knowledge(
        self,
        ideation_id: str,
        user_id: str,
        data: IdeationKnowledgeCreate,
    ) -> ApplicationRecord | None:
        """Create a knowledge base item on an ideation."""
        governance_metadata = normalize_knowledge_governance_metadata(
            data.governance_metadata
        )
        ideation = await _application_get(self.db, "ideation", ideation_id)
        if not ideation:
            return None
        require_draft_mutation(ideation, subject_type="ideation")
        kb = _new_knowledge_application_record(
            "ideation_knowledge_base",
            parent_field="ideation_id",
            parent_id=ideation_id,
            parent_version=getattr(ideation, "version", None),
            title=data.title,
            description=data.description,
            content=data.content,
            mime_type=data.mime_type,
            governance_metadata=governance_metadata,
            created_by=user_id,
        )
        await _application_add(self.db, kb)
        return kb

    async def get_knowledge(self, knowledge_id: str) -> ApplicationRecord | None:
        """Get a knowledge base item by ID."""
        return await _application_get(self.db, "ideation_knowledge_base", knowledge_id)

    async def list_knowledge(self, ideation_id: str) -> list[ApplicationRecord]:
        """List all knowledge base items for an ideation."""
        return await _application_list(
            self.db,
            "ideation_knowledge_base",
            filters=(_apf("ideation_id", "eq", ideation_id),),
            order_by=(("created_at", False),),
        )

    async def update_knowledge(
        self,
        knowledge_id: str,
        data: IdeationKnowledgeUpdate,
    ) -> ApplicationRecord | None:
        """Update a knowledge base item."""
        update_data = data.model_dump(exclude_unset=True)
        if "governance_metadata" in update_data:
            update_data["governance_metadata"] = (
                normalize_knowledge_governance_metadata(
                    update_data["governance_metadata"]
                )
            )
        kb = await self.get_knowledge(knowledge_id)
        if not kb:
            return None
        ideation = await _application_get(self.db, "ideation", kb.ideation_id)
        if ideation is None:
            raise RuntimeError("knowledge_subject_missing")
        require_draft_mutation(ideation, subject_type="ideation")
        for key, value in update_data.items():
            setattr(kb, key, value)
        _refresh_knowledge_content_hash(kb)
        return kb

    async def delete_knowledge(self, knowledge_id: str) -> bool:
        """Delete a knowledge base item."""
        kb = await self.get_knowledge(knowledge_id)
        if not kb:
            return False
        ideation = await _application_get(self.db, "ideation", kb.ideation_id)
        if ideation is None:
            raise RuntimeError("knowledge_subject_missing")
        require_draft_mutation(ideation, subject_type="ideation")
        await _application_delete(self.db, kb)
        return True


class ShareService:
    """Service for board sharing operations."""

    VALID_PERMISSIONS = ("viewer", "editor", "admin")

    def __init__(self, db: Any):
        self.db = db

    async def share_board(
        self,
        board_id: str,
        owner_id: str,
        realm_id: str,
        data: BoardShareCreate,
        *,
        query_scope: QueryScope | None = None,
    ) -> ApplicationRecord | None:
        """Share a board with another user. Only owner/admin can share."""
        # Check board exists and caller is owner or admin
        if not await self._can_manage_shares(
            board_id, owner_id, query_scope=query_scope
        ):
            return None

        scoped_owner_id = _scope_actor_id(owner_id, query_scope) or owner_id
        if data.user_id == scoped_owner_id:
            return None  # Can't share with yourself

        share = _new_application_record(
            "board_share",
            board_id=board_id,
            user_id=data.user_id,
            realm_id=realm_id,
            permission=data.permission,
            shared_by=scoped_owner_id,
        )
        await _application_add(self.db, share)
        return share

    async def list_shares(self, board_id: str) -> list[ApplicationRecord]:
        """List all shares for a board."""
        return await _application_list(
            self.db,
            "board_share",
            filters=(_apf("board_id", "eq", board_id),),
            order_by=(("created_at", False),),
        )

    async def update_share(
        self,
        share_id: str,
        caller_id: str,
        data: BoardShareUpdate,
        *,
        query_scope: QueryScope | None = None,
    ) -> ApplicationRecord | None:
        """Update a share permission. Only owner/admin can update."""
        share = await _application_get(self.db, "board_share", share_id)
        if not share:
            return None

        if not await self._can_manage_shares(
            share.board_id, caller_id, query_scope=query_scope
        ):
            return None

        share.permission = data.permission
        return share

    async def revoke_share(
        self,
        share_id: str,
        caller_id: str,
        *,
        query_scope: QueryScope | None = None,
    ) -> bool:
        """Revoke a share. Owner/admin can revoke, or user can leave."""
        share = await _application_get(self.db, "board_share", share_id)
        if not share:
            return False

        # Allow if caller is the shared user (leaving) or can manage shares
        scoped_caller_id = _scope_actor_id(caller_id, query_scope) or caller_id
        if share.user_id != scoped_caller_id and not await self._can_manage_shares(
            share.board_id,
            caller_id,
            query_scope=query_scope,
        ):
            return False

        await _application_delete(self.db, share)
        return True

    async def get_user_permission(
        self,
        board_id: str,
        user_id: str,
        *,
        query_scope: QueryScope | None = None,
    ) -> str | None:
        """Get a user's permission level for a board. Returns None if no access."""
        # Check if owner
        board = await _application_get(self.db, "board", board_id)
        if not board:
            return None
        if _board_owner_matches(board, user_id, query_scope):
            return "owner"

        return await self.get_share_permission(
            board_id,
            user_id,
            query_scope=query_scope,
        )

    async def get_share_permission(
        self,
        board_id: str,
        user_id: str,
        *,
        query_scope: QueryScope | None = None,
    ) -> str | None:
        """Read only the share row when board existence/ownership is known."""
        scoped_user_id = _scope_actor_id(user_id, query_scope) or user_id
        shares = await _application_list(
            self.db,
            "board_share",
            filters=(
                _apf("board_id", "eq", board_id),
                _apf("user_id", "eq", scoped_user_id),
            ),
            limit=1,
        )
        share = shares[0] if shares else None
        return share.permission if share else None

    async def _can_manage_shares(
        self,
        board_id: str,
        user_id: str,
        *,
        query_scope: QueryScope | None = None,
    ) -> bool:
        """Check if user is owner or admin of the board."""
        board = await _application_get(self.db, "board", board_id)
        if not board:
            return False
        if _board_owner_matches(board, user_id, query_scope):
            return True

        # Check if admin via share
        scoped_user_id = _scope_actor_id(user_id, query_scope) or user_id
        shares = await _application_list(
            self.db,
            "board_share",
            filters=(
                _apf("board_id", "eq", board_id),
                _apf("user_id", "eq", scoped_user_id),
                _apf("permission", "eq", "admin"),
            ),
            limit=1,
        )
        return bool(shares)


class TopicOperationError(ValueError):
    """Domain error with a stable code for Topic operations."""

    def __init__(
        self, message: str, *, code: str, details: dict[str, Any] | None = None
    ):
        super().__init__(message)
        self.code = code
        self.details = details or {}


class TopicNameConflictError(TopicOperationError):
    def __init__(self, message: str = "Topic name already exists in this board"):
        super().__init__(message, code="topic_name_conflict")


class TopicNotEmptyError(TopicOperationError):
    def __init__(self, *, active_count: int, archived_count: int):
        total_count = active_count + archived_count
        super().__init__(
            "Topic has associated Stories and cannot be deleted",
            code="topic_not_empty",
            details={
                "active_count": active_count,
                "archived_count": archived_count,
                "total_associated_count": total_count,
                "suggested_actions": ["merge", "move_stories", "archive"],
            },
        )


class InvalidTopicMergeError(TopicOperationError):
    def __init__(self, message: str):
        super().__init__(message, code="invalid_merge")


class StoryService:
    """Service for Topic and Story operations."""

    def __init__(self, db: Any):
        self.db = db

    _STORY_TRANSITIONS: dict[StoryStatus, list[StoryStatus]] = transition_map("story")
    _EDITABLE_IDEATION_STATUSES = (
        IdeationStatus.DRAFT,
        IdeationStatus.REVIEW,
        IdeationStatus.APPROVED,
        IdeationStatus.EVALUATING,
    )

    async def _ensure_board(
        self,
        board_id: str,
        user_id: str,
        skip_ownership_check: bool = False,
        *,
        query_scope: QueryScope | None = None,
    ) -> ApplicationRecord | None:
        query = _board_scope_select(
            board_id=board_id,
            user_id=user_id,
            query_scope=None if skip_ownership_check else query_scope,
            require_ownership=not skip_ownership_check,
        )
        if query is None:
            return None
        rows = await _application_run(self.db, query)
        return rows[0] if rows else None

    async def _topic_for_board(
        self, topic_id: str, board_id: str
    ) -> ApplicationRecord | None:
        rows = await _application_list(
            self.db,
            "topic",
            filters=(
                _apf("id", "eq", topic_id),
                _apf("board_id", "eq", board_id),
            ),
            limit=1,
        )
        return rows[0] if rows else None

    async def get_topic(self, topic_id: str) -> ApplicationRecord | None:
        """Transport-free PK load of a Topic (spec R01A REST-FU6-S2 rework): the
        update/delete/merge use cases resolve the topic's ``board_id`` for the
        ownership + permission gate here instead of the adapter issuing a
        ``db.get(Topic, …)`` (keeps the REST adapter free of direct ORM)."""
        return await _application_get(self.db, "topic", topic_id)

    async def _log_activity(self, **kwargs: Any) -> None:
        await _application_add(
            self.db,
            _new_application_record("activity_log", **kwargs),
        )

    @staticmethod
    def _archived_topic_name(name: str, topic_id: str) -> str:
        suffix = f" [archived {topic_id[:8]}]"
        return f"{name[: max(1, 255 - len(suffix))]}{suffix}"

    async def _topic_story_counts(
        self, topic_id: str, *, board_id: str
    ) -> dict[str, int]:
        rows = await _application_group_count(
            self.db,
            GroupCountRequest(
                surface="topic_story_counts",
                scope=(
                    _apf("board_id", "eq", board_id),
                    _apf("topic_id", "eq", topic_id),
                ),
                group_by=("archived",),
            ),
        )
        counts = {"active_count": 0, "archived_count": 0}
        for row in rows:
            key = "archived_count" if bool(row.values[0]) else "active_count"
            counts[key] += row.count
        counts["total_associated_count"] = (
            counts["active_count"] + counts["archived_count"]
        )
        return counts

    async def _attach_topic_counts(self, topic: ApplicationRecord) -> ApplicationRecord:
        counts = await self._topic_story_counts(topic.id, board_id=topic.board_id)
        topic.attach("story_count", counts["active_count"])
        topic.attach("active_count", counts["active_count"])
        topic.attach("archived_count", counts["archived_count"])
        topic.attach("total_associated_count", counts["total_associated_count"])
        return topic

    async def _ensure_active_topic_name_available(
        self,
        board_id: str,
        name: str,
        *,
        exclude_topic_id: str | None = None,
    ) -> None:
        filters = [
            _apf("board_id", "eq", board_id),
            _apf("archived", "is_false"),
            _apf("name", "ilike", name),
        ]
        if exclude_topic_id:
            filters.append(_apf("id", "ne", exclude_topic_id))
        existing = await _application_list(
            self.db,
            "topic",
            filters=tuple(filters),
            limit=1,
        )
        if existing:
            raise TopicNameConflictError()

    async def _free_archived_exact_name(
        self,
        board_id: str,
        name: str,
        *,
        exclude_topic_id: str | None = None,
    ) -> list[str]:
        filters = [
            _apf("board_id", "eq", board_id),
            _apf("archived", "is_true"),
            _apf("name", "eq", name),
        ]
        if exclude_topic_id:
            filters.append(_apf("id", "ne", exclude_topic_id))
        archived_topics = await _application_list(
            self.db,
            "topic",
            filters=tuple(filters),
        )
        renamed: list[str] = []
        for archived_topic in archived_topics:
            archived_topic.name = self._archived_topic_name(
                archived_topic.name, archived_topic.id
            )
            renamed.append(archived_topic.id)
        return renamed

    async def create_topic(
        self,
        board_id: str,
        user_id: str,
        data: TopicCreate,
        skip_ownership_check: bool = False,
    ) -> ApplicationRecord | None:
        if not await self._ensure_board(board_id, user_id, skip_ownership_check):
            return None
        name = data.name.strip()
        await self._ensure_active_topic_name_available(board_id, name)
        renamed_archived_topics = await self._free_archived_exact_name(board_id, name)
        if renamed_archived_topics:
            await _application_flush(self.db)
        topic = _new_application_record(
            "topic",
            board_id=board_id,
            name=name,
            description=data.description,
            created_by=user_id,
        )
        await _application_add(self.db, topic)
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="topic_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "topic_id": topic.id,
                "name": topic.name,
                "renamed_archived_topics": renamed_archived_topics,
            },
        )
        return await self._attach_topic_counts(topic)

    async def list_topics(
        self, board_id: str, include_archived: bool = False
    ) -> list[ApplicationRecord]:
        filters = [_apf("board_id", "eq", board_id)]
        if not include_archived:
            filters.append(_apf("archived", "is_false"))
        topics = await _application_list(
            self.db,
            "topic",
            filters=tuple(filters),
            order_by=(("name", False),),
        )
        count_rows = await _application_group_count(
            self.db,
            GroupCountRequest(
                surface="topic_story_counts",
                scope=(_apf("board_id", "eq", board_id),),
                group_by=("topic_id", "archived"),
            ),
        )
        counts: dict[str, dict[str, int]] = {}
        for row in count_rows:
            topic_id, archived = row.values
            if not topic_id:
                continue
            bucket = counts.setdefault(
                topic_id, {"active_count": 0, "archived_count": 0}
            )
            key = "archived_count" if bool(archived) else "active_count"
            bucket[key] += row.count
        for topic in topics:
            topic_counts = counts.get(
                topic.id, {"active_count": 0, "archived_count": 0}
            )
            total_count = topic_counts["active_count"] + topic_counts["archived_count"]
            topic.attach("story_count", topic_counts["active_count"])
            topic.attach("active_count", topic_counts["active_count"])
            topic.attach("archived_count", topic_counts["archived_count"])
            topic.attach("total_associated_count", total_count)
        return topics

    async def update_topic(
        self, topic_id: str, user_id: str, data: TopicUpdate
    ) -> ApplicationRecord | None:
        topic = await _application_get(self.db, "topic", topic_id)
        if not topic:
            return None
        original_archived = bool(topic.archived)
        original_name = topic.name
        update_data = data.model_dump(exclude_unset=True)
        target_archived = bool(update_data.get("archived", topic.archived))
        if "name" in update_data and update_data["name"] is not None:
            name = update_data.pop("name").strip()
            if not target_archived:
                await self._ensure_active_topic_name_available(
                    topic.board_id, name, exclude_topic_id=topic.id
                )
                await self._free_archived_exact_name(
                    topic.board_id, name, exclude_topic_id=topic.id
                )
            topic.name = name
        elif original_archived and not target_archived:
            await self._ensure_active_topic_name_available(
                topic.board_id, topic.name, exclude_topic_id=topic.id
            )
            await self._free_archived_exact_name(
                topic.board_id, topic.name, exclude_topic_id=topic.id
            )
        for key, value in update_data.items():
            setattr(topic, key, value)
        counts = await self._topic_story_counts(topic.id, board_id=topic.board_id)
        if original_archived != bool(topic.archived):
            action = "topic_restored" if original_archived else "topic_archived"
        else:
            action = "topic_updated"
        actor_name = await resolve_actor_name(self.db, user_id, topic.board_id)
        await self._log_activity(
            board_id=topic.board_id,
            action=action,
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "topic_id": topic.id,
                "fields": list(data.model_dump(exclude_unset=True).keys()),
                "previous_name": original_name,
                "name": topic.name,
                "previous_archived": original_archived,
                "archived": bool(topic.archived),
                **counts,
            },
        )
        await _application_flush(self.db)
        await _application_refresh(self.db, topic)
        return await self._attach_topic_counts(topic)

    async def delete_topic(
        self, topic_id: str, user_id: str
    ) -> ApplicationRecord | None:
        topic = await _application_get(self.db, "topic", topic_id)
        if not topic:
            return None
        counts = await self._topic_story_counts(topic.id, board_id=topic.board_id)
        if counts["total_associated_count"] > 0:
            raise TopicNotEmptyError(
                active_count=counts["active_count"],
                archived_count=counts["archived_count"],
            )
        actor_name = await resolve_actor_name(self.db, user_id, topic.board_id)
        await self._log_activity(
            board_id=topic.board_id,
            action="topic_deleted",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"topic_id": topic.id, "name": topic.name, **counts},
        )
        await _application_delete(self.db, topic)
        await _application_flush(self.db)
        return topic

    async def merge_topics(
        self, source_topic_id: str, target_topic_id: str, user_id: str
    ) -> dict[str, Any] | None:
        if source_topic_id == target_topic_id:
            raise InvalidTopicMergeError("Source and target Topics must be different")
        source_topic = await _application_get(self.db, "topic", source_topic_id)
        target_topic = await _application_get(self.db, "topic", target_topic_id)
        if not source_topic or not target_topic:
            return None
        if source_topic.board_id != target_topic.board_id:
            raise InvalidTopicMergeError(
                "Source and target Topics must belong to the same board"
            )
        if target_topic.archived:
            raise InvalidTopicMergeError("Target Topic must be active")

        source_counts = await self._topic_story_counts(
            source_topic.id, board_id=source_topic.board_id
        )
        target_counts_before = await self._topic_story_counts(
            target_topic.id, board_id=target_topic.board_id
        )
        stories = await _application_list(
            self.db,
            "story",
            filters=(_apf("topic_id", "eq", source_topic.id),),
        )
        for story in stories:
            story.topic_id = target_topic.id
        source_topic.archived = True
        await _application_flush(self.db)
        target_counts_after = await self._topic_story_counts(
            target_topic.id, board_id=target_topic.board_id
        )
        actor_name = await resolve_actor_name(self.db, user_id, source_topic.board_id)
        await self._log_activity(
            board_id=source_topic.board_id,
            action="topic_merged",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "source_topic_id": source_topic.id,
                "source_topic_name": source_topic.name,
                "target_topic_id": target_topic.id,
                "target_topic_name": target_topic.name,
                "moved_count": source_counts["total_associated_count"],
                "active_count": source_counts["active_count"],
                "archived_count": source_counts["archived_count"],
                "target_total_before": target_counts_before["total_associated_count"],
                "target_total_after": target_counts_after["total_associated_count"],
            },
        )
        await _application_flush(self.db)
        await _application_refresh(self.db, source_topic)
        await _application_refresh(self.db, target_topic)
        await self._attach_topic_counts(source_topic)
        await self._attach_topic_counts(target_topic)
        return {
            "success": True,
            "source": source_topic,
            "target": target_topic,
            "moved_count": source_counts["total_associated_count"],
            "active_count": source_counts["active_count"],
            "archived_count": source_counts["archived_count"],
            "target_total_before": target_counts_before["total_associated_count"],
            "target_total_after": target_counts_after["total_associated_count"],
        }

    async def create_story(
        self,
        board_id: str,
        user_id: str,
        data: StoryCreate,
        skip_ownership_check: bool = False,
    ) -> ApplicationRecord | None:
        if not await self._ensure_board(board_id, user_id, skip_ownership_check):
            return None
        topic = await self._topic_for_board(data.topic_id, board_id)
        if not topic or topic.archived:
            raise ValueError("Topic not found in this board")
        story = _new_application_record(
            "story",
            board_id=board_id,
            topic_id=data.topic_id,
            title=data.title.strip(),
            description=data.description,
            actor=data.actor,
            goal=data.goal,
            benefit=data.benefit,
            labels=data.labels,
            status=data.status,
            assignee_id=data.assignee_id,
            created_by=user_id,
            screen_mockups=None,  # assigned after the Design System gate (below)
        )
        # MockupDesignSystemGate (spec 3a006f65 / card 0192f58d): gate mockups submitted
        # at creation BEFORE persistence (old=[] baseline so every screen is evaluated).
        _submitted_mockups = [
            item.model_dump() if hasattr(item, "model_dump") else item
            for item in (data.screen_mockups or [])
        ] or None
        if _submitted_mockups:
            from okto_pulse.core.services.design_system import (
                gate_entity_screen_mockups,
            )

            await gate_entity_screen_mockups(
                self.db, story, _submitted_mockups, entity_type="story"
            )
            story.screen_mockups = _submitted_mockups
        await _application_add(self.db, story)
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="story_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "story_id": story.id,
                "topic_id": story.topic_id,
                "title": story.title,
            },
        )
        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import StoryCreated

        await event_publish(
            StoryCreated(
                board_id=board_id,
                actor_id=user_id,
                story_id=story.id,
                topic_id=story.topic_id,
                status=story.status.value,
            ),
            session=self.db,
        )
        return await self.get_story(story.id)

    async def get_story(self, story_id: str) -> ApplicationRecord | None:
        return await _application_get(
            self.db,
            "story",
            story_id,
            includes=("topic", "ideation_links"),
        )

    async def list_stories(
        self,
        board_id: str,
        *,
        status_filter: str | None = None,
        topic_id: str | None = None,
        search: str | None = None,
        linked: bool | None = None,
        converted: bool | None = None,
        include_archived: bool = False,
    ) -> list[ApplicationRecord]:
        filters = [_apf("board_id", "eq", board_id)]
        any_filters: tuple[ApplicationFilter, ...] = ()
        if status_filter:
            filters.append(_apf("status", "eq", StoryStatus(status_filter)))
        if topic_id:
            filters.append(_apf("topic_id", "eq", topic_id))
        if search:
            pattern = f"%{search}%"
            any_filters = tuple(
                _apf(field, "ilike", pattern)
                for field in ("title", "description", "actor", "goal", "benefit")
            )
        if converted is not None:
            filters.append(
                _apf(
                    "status",
                    "eq" if converted else "ne",
                    StoryStatus.CONVERTED,
                )
            )
        if not include_archived:
            filters.append(_apf("archived", "is_false"))
        stories = await _application_list(
            self.db,
            "story",
            filters=tuple(filters),
            any_filters=any_filters,
            order_by=(("updated_at", True),),
            includes=("topic", "ideation_links"),
        )
        if linked is None:
            return stories
        return [
            story
            for story in stories
            if (len(story.ideation_links or []) > 0) is linked
        ]

    async def update_story(
        self, story_id: str, user_id: str, data: StoryUpdate
    ) -> ApplicationRecord | None:
        story = await self.get_story(story_id)
        if not story:
            return None
        if story.archived:
            raise ValueError("This story is archived. Restore it before editing.")
        update_data = data.model_dump(exclude_unset=True)
        if "topic_id" in update_data and update_data["topic_id"] is not None:
            topic = await self._topic_for_board(update_data["topic_id"], story.board_id)
            if not topic or topic.archived:
                raise ValueError("Topic not found in this board")
        if (
            "screen_mockups" in update_data
            and update_data["screen_mockups"] is not None
        ):
            update_data["screen_mockups"] = [
                item.model_dump() if hasattr(item, "model_dump") else item
                for item in update_data["screen_mockups"]
            ]
            # MockupDesignSystemGate (spec 3a006f65) — defense in depth pre-persist.
            from okto_pulse.core.services.design_system import (
                gate_entity_screen_mockups,
            )

            await gate_entity_screen_mockups(
                self.db, story, update_data["screen_mockups"], entity_type="story"
            )
        for key, value in update_data.items():
            setattr(story, key, value)
            if key in {"labels", "screen_mockups"}:
                story.mark_dirty(key)
        actor_name = await resolve_actor_name(self.db, user_id, story.board_id)
        await self._log_activity(
            board_id=story.board_id,
            action="story_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"story_id": story.id, "fields": list(update_data.keys())},
        )
        if update_data:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import StoryUpdated

            await event_publish(
                StoryUpdated(
                    board_id=story.board_id,
                    actor_id=user_id,
                    story_id=story.id,
                    changed_fields=list(update_data.keys()),
                ),
                session=self.db,
            )
        await _application_flush(self.db)
        return await self.get_story(story_id)

    async def move_story(
        self, story_id: str, user_id: str, data: StoryMove
    ) -> ApplicationRecord | None:
        story = await self.get_story(story_id)
        if not story:
            return None
        if story.archived:
            raise ValueError(
                "This story is archived. Restore it before changing status."
            )
        old_status = story.status
        # A lifecycle no-op is a read, not a mutation. Returning before the
        # activity/event path prevents callers from manufacturing audit rows
        # without holding any ``story.move.*`` capability.
        if data.status == old_status:
            return story
        allowed = self._STORY_TRANSITIONS.get(old_status, [])
        if data.status not in allowed:
            allowed_str = (
                ", ".join(status.value for status in allowed) if allowed else "none"
            )
            raise ValueError(
                f"Cannot move story from '{old_status.value}' to '{data.status.value}'. "
                f"Allowed transitions: {allowed_str}."
            )
        story.status = data.status
        actor_name = await resolve_actor_name(self.db, user_id, story.board_id)
        await self._log_activity(
            board_id=story.board_id,
            action="story_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "story_id": story.id,
                "from_status": old_status.value,
                "to_status": data.status.value,
            },
        )
        if old_status != data.status:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import StoryMoved

            await event_publish(
                StoryMoved(
                    board_id=story.board_id,
                    actor_id=user_id,
                    story_id=story.id,
                    from_status=old_status.value,
                    to_status=data.status.value,
                ),
                session=self.db,
            )
        await _application_flush(self.db)
        return await self.get_story(story_id)

    async def archive_story(
        self, story_id: str, user_id: str, archived: bool = True
    ) -> ApplicationRecord | None:
        story = await self.get_story(story_id)
        if not story:
            return None
        archive_changed = bool(story.archived) != bool(archived)
        story.archived = archived
        story.pre_archive_status = story.status.value if archived else None
        actor_name = await resolve_actor_name(self.db, user_id, story.board_id)
        await self._log_activity(
            board_id=story.board_id,
            action="story_archived" if archived else "story_restored",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"story_id": story.id},
        )
        if archive_changed:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import ArtifactArchiveChanged

            await event_publish(
                ArtifactArchiveChanged(
                    board_id=story.board_id,
                    actor_id=user_id,
                    artifact_type="story",
                    artifact_id=story.id,
                    archived=archived,
                ),
                session=self.db,
            )
        await _application_flush(self.db)
        return await self.get_story(story_id)

    async def link_story_to_ideation(
        self,
        story_id: str,
        ideation_id: str,
        user_id: str,
        *,
        mark_converted: bool = True,
    ) -> ApplicationRecord | None:
        story = await self.get_story(story_id)
        ideation = await _application_get(self.db, "ideation", ideation_id)
        if not story or not ideation or story.board_id != ideation.board_id:
            return None
        if ideation.status not in self._EDITABLE_IDEATION_STATUSES:
            allowed = ", ".join(
                status.value for status in self._EDITABLE_IDEATION_STATUSES
            )
            raise ValueError(
                f"Story can only be linked to editable Ideations. "
                f"Current ideation status is '{ideation.status.value}'. Allowed statuses: {allowed}."
            )
        links = await _application_list(
            self.db,
            "story_ideation_link",
            filters=(_apf("story_id", "eq", story_id),),
            limit=1,
        )
        link = links[0] if links else None
        if link:
            if link.ideation_id == ideation_id:
                raise ValueError("Story is already linked to this Ideation.")
            raise ValueError(
                "Story is already linked to another Ideation. A Story can only link to one Ideation."
            )
        if story.status != StoryStatus.READY:
            raise ValueError(
                "Only ready Stories can be converted to Ideation. "
                f"Current Story status is '{story.status.value}'. "
                "Move the Story to 'ready' before linking it."
            )
        link = _new_application_record(
            "story_ideation_link",
            board_id=story.board_id,
            story_id=story_id,
            ideation_id=ideation_id,
            created_by=user_id,
        )
        await _application_add(self.db, link)
        # mark_converted remains accepted for API compatibility; successful links now always convert.
        if story.status != StoryStatus.CONVERTED:
            story.status = StoryStatus.CONVERTED
            await _application_flush(self.db)
        actor_name = await resolve_actor_name(self.db, user_id, story.board_id)
        await self._log_activity(
            board_id=story.board_id,
            action="story_linked_to_ideation",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"story_id": story_id, "ideation_id": ideation_id},
        )
        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import StoryLinkedToIdeation

        await event_publish(
            StoryLinkedToIdeation(
                board_id=story.board_id,
                actor_id=user_id,
                story_id=story_id,
                ideation_id=ideation_id,
            ),
            session=self.db,
        )
        return link

    async def convert_stories(
        self,
        board_id: str,
        user_id: str,
        data: StoryConversionRequest,
        *,
        skip_ownership_check: bool = False,
        query_scope: QueryScope | None = None,
    ) -> tuple[ApplicationRecord, list[ApplicationRecord], int] | None:
        if not await self._ensure_board(
            board_id,
            user_id,
            skip_ownership_check,
            query_scope=query_scope,
        ):
            return None
        stories = await _application_list(
            self.db,
            "story",
            filters=(
                _apf("board_id", "eq", board_id),
                _apf("id", "in", data.story_ids),
                _apf("archived", "is_false"),
            ),
            includes=("topic", "ideation_links"),
        )
        if len(stories) != len(set(data.story_ids)):
            raise ValueError("One or more Stories were not found in this board")
        not_ready = [
            story.title
            for story in stories
            if story.status not in (StoryStatus.READY, StoryStatus.CONVERTED)
        ]
        if not_ready:
            raise ValueError("Only ready Stories can be converted to Ideation")

        if data.ideation_id:
            ideation = await _application_get(self.db, "ideation", data.ideation_id)
            if not ideation or ideation.board_id != board_id:
                raise ValueError("Ideation not found in this board")
        else:
            story_lines = []
            for story in stories:
                topic_name = story.topic.name if story.topic else story.topic_id
                story_lines.append(
                    f"- {story.title} (topic: {topic_name})"
                    f"{f'; actor: {story.actor}' if story.actor else ''}"
                    f"{f'; goal: {story.goal}' if story.goal else ''}"
                    f"{f'; benefit: {story.benefit}' if story.benefit else ''}"
                )
            ideation = await IdeationService(self.db).create_ideation(
                board_id,
                user_id,
                IdeationCreate(
                    title=data.title or stories[0].title,
                    description=data.description
                    or "Ideation created from selected Stories.",
                    problem_statement=data.problem_statement
                    or "Selected Stories:\n" + "\n".join(story_lines),
                    proposed_approach=data.proposed_approach,
                    labels=sorted(
                        {label for story in stories for label in (story.labels or [])}
                    )
                    or None,
                ),
                skip_ownership_check=skip_ownership_check,
                query_scope=query_scope,
            )
            if not ideation:
                return None

        links: list[ApplicationRecord] = []
        for story in stories:
            link = await self.link_story_to_ideation(
                story.id, ideation.id, user_id, mark_converted=data.mark_converted
            )
            if link:
                links.append(link)

        _old_ideation_mockups = list(ideation.screen_mockups or [])
        propagated = self._propagate_story_mockups(stories, ideation, data.mockup_ids)
        if propagated:
            ideation.mark_dirty("screen_mockups")
            # MockupDesignSystemGate (spec 3a006f65 / card 0192f58d): convert_stories
            # rewrites story mockups with FRESH ids onto the ideation — gate the new
            # entries (delta vs the pre-propagation set) BEFORE flush so a non-compliant
            # legacy mockup can't be laundered onto a blocking board.
            from okto_pulse.core.services.design_system import MockupDesignSystemGate

            await MockupDesignSystemGate(self.db).gate_delta(
                ideation.board_id,
                _old_ideation_mockups,
                list(ideation.screen_mockups or []),
                entity_type="ideation",
                entity_id=ideation.id,
            )
        await _application_flush(self.db)
        await _application_refresh(self.db, ideation)
        for link in links:
            await _application_refresh(self.db, link)
        return ideation, links, propagated

    def _propagate_story_mockups(
        self,
        stories: list[ApplicationRecord],
        ideation: ApplicationRecord,
        mockup_ids: list[str] | None,
    ) -> int:
        selected = set(mockup_ids) if mockup_ids is not None else None
        target = list(ideation.screen_mockups or [])
        propagated = 0
        for story in stories:
            for mockup in story.screen_mockups or []:
                if not isinstance(mockup, dict):
                    continue
                mockup_id = mockup.get("id")
                if selected is not None and mockup_id not in selected:
                    continue
                copied = dict(mockup)
                copied["id"] = f"story_mockup_{secrets.token_hex(8)}"
                copied["origin_id"] = mockup_id
                copied["origin_story_id"] = story.id
                copied["origin_entity_type"] = "story"
                copied["order"] = len(target)
                target.append(copied)
                propagated += 1
        if propagated:
            ideation.screen_mockups = target
        return propagated


def _build_default_ambiguity_gate_service(db: Any) -> AmbiguityGateService:
    """Resolve the edition-owned assessment source of truth for one UoW."""

    from okto_pulse.core.ports.relational_application import (
        require_relational_application_adapter,
    )

    persistence = require_relational_application_adapter().quality_assessments(db)
    return AmbiguityGateService(persistence)


@dataclass(frozen=True, slots=True)
class RefinementAmbiguityGateSkipResult:
    refinement: ApplicationRecord
    activity_id: str
    skipped: bool
    version: int
    edition: int


class IdeationService:
    """Service for ideation operations."""

    def __init__(self, db: Any):
        self.db = db
        self._ambiguity_gate_service_factory: Callable[[Any], AmbiguityGateService] = (
            _build_default_ambiguity_gate_service
        )

    _STATUS_ORDER = {
        IdeationStatus.DRAFT: 0,
        IdeationStatus.REVIEW: 1,
        IdeationStatus.APPROVED: 2,
        IdeationStatus.EVALUATING: 3,
        IdeationStatus.DONE: 4,
        IdeationStatus.CANCELLED: 4,
    }

    async def _record_history(
        self,
        ideation_id: str,
        action: str,
        actor_id: str,
        actor_name: str,
        actor_type: str = "user",
        changes: list[dict] | None = None,
        summary: str | None = None,
        version: int | None = None,
    ) -> None:
        """Record a history entry for an ideation."""
        entry = _new_application_record(
            "ideation_history",
            ideation_id=ideation_id,
            action=action,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            changes=changes,
            summary=summary,
            version=version,
        )
        await _application_add(self.db, entry)

    async def list_history(
        self,
        ideation_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> list[IdeationHistory]:
        """List history entries for an ideation, newest first."""
        window = validate_history_window(limit, offset)
        return await _application_list(
            self.db,
            "ideation_history",
            filters=(_apf("ideation_id", "eq", ideation_id),),
            order_by=(("created_at", True), ("id", True)),
            offset=window.offset,
            limit=window.limit,
        )

    async def count_history(self, ideation_id: str) -> int:
        """Count every history entry for an ideation independently of a page."""
        return await _application_count(
            self.db,
            "ideation_history",
            filters=(_apf("ideation_id", "eq", ideation_id),),
        )

    @staticmethod
    def _compute_diff(old_data: dict, new_data: dict, fields: list[str]) -> list[dict]:
        """Compute field-level diffs between old and new data."""
        changes = []
        for field in fields:
            old_val = old_data.get(field)
            new_val = new_data.get(field)
            if hasattr(old_val, "value"):
                old_val = old_val.value
            if hasattr(new_val, "value"):
                new_val = new_val.value
            if old_val != new_val:
                changes.append({"field": field, "old": old_val, "new": new_val})
        return changes

    async def create_ideation(
        self,
        board_id: str,
        user_id: str,
        data: IdeationCreate,
        skip_ownership_check: bool = False,
        *,
        query_scope: QueryScope | None = None,
    ) -> Ideation | None:
        """Create a new ideation in a board."""
        board_query = _board_scope_select(
            board_id=board_id,
            user_id=user_id,
            query_scope=None if skip_ownership_check else query_scope,
            require_ownership=not skip_ownership_check,
        )
        if board_query is None:
            return None
        if not await _application_run(self.db, board_query):
            return None

        ideation = _new_application_record(
            "ideation",
            board_id=board_id,
            title=data.title,
            description=data.description,
            problem_statement=data.problem_statement,
            proposed_approach=data.proposed_approach,
            scope_assessment=data.scope_assessment,
            complexity=IdeationComplexity(data.complexity) if data.complexity else None,
            assignee_id=data.assignee_id,
            created_by=user_id,
            labels=data.labels,
            edition=1,
        )
        await _application_add(self.db, ideation)

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="ideation_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"title": data.title, "ideation_id": ideation.id},
        )
        await self._record_history(
            ideation_id=ideation.id,
            action="created",
            actor_id=user_id,
            actor_name=actor_name,
            summary=f"Ideation created: {data.title}",
            version=1,
            changes=[
                {"field": "title", "old": None, "new": data.title},
                {"field": "status", "old": None, "new": IdeationStatus.DRAFT.value},
                *(
                    [
                        {
                            "field": "problem_statement",
                            "old": None,
                            "new": data.problem_statement,
                        }
                    ]
                    if data.problem_statement
                    else []
                ),
                *(
                    [
                        {
                            "field": "proposed_approach",
                            "old": None,
                            "new": data.proposed_approach,
                        }
                    ]
                    if data.proposed_approach
                    else []
                ),
            ],
        )
        return ideation

    async def get_ideation(self, ideation_id: str) -> Ideation | None:
        """Get an ideation by ID with refinements, specs, and qa_items."""
        ideation = await _application_get(
            self.db,
            "ideation",
            ideation_id,
            includes=(
                "refinements.architecture_designs",
                "specs.architecture_designs",
                "story_links.story",
                "knowledge_bases",
                "qa_items",
                "architecture_designs",
            ),
        )
        if ideation:
            stories: list[ApplicationRecord] = []
            for link in ideation.story_links or []:
                story = getattr(link, "story", None)
                if story is None:
                    continue
                story.attach(
                    "ideation_links",
                    [
                        candidate
                        for candidate in ideation.story_links
                        if candidate.story_id == story.id
                    ],
                )
                stories.append(story)
            ideation.attach(
                "stories",
                stories,
            )
            await _attach_active_refinement_counts(self.db, [ideation])
            await _attach_active_direct_spec_counts(self.db, [ideation])
            await _attach_active_spec_counts(self.db, list(ideation.refinements or []))
        return ideation

    async def list_ideations(
        self,
        board_id: str,
        status_filter: str | None = None,
        include_archived: bool = False,
    ) -> list[Ideation]:
        """List ideations for a board, optionally filtered by status."""
        filters = [_apf("board_id", "eq", board_id)]
        if status_filter:
            filters.append(_apf("status", "eq", IdeationStatus(status_filter)))
        if not include_archived:
            filters.append(_apf("archived", "is_false"))
        rows = await _application_list(
            self.db,
            "ideation",
            filters=tuple(filters),
            order_by=(("updated_at", True),),
            includes=("architecture_designs",),
        )
        await _attach_open_qa_counts(self.db, rows, "ideation_qa_item", "ideation_id")
        await _attach_active_refinement_counts(self.db, rows)
        await _attach_active_direct_spec_counts(self.db, rows)
        return rows

    async def update_ideation(
        self, ideation_id: str, user_id: str, data: IdeationUpdate
    ) -> Ideation | None:
        """Update an ideation. Bumps version on content changes. Records field-level diffs.

        Only allowed in Draft status — all other statuses are read-only.
        """
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return None

        if getattr(ideation, "archived", False):
            raise ValueError(
                "This ideation is archived. Restore it first before making changes."
            )

        require_draft_mutation(ideation, subject_type="ideation")

        update_data = data.model_dump(exclude_unset=True)
        content_fields = {
            "title",
            "description",
            "problem_statement",
            "proposed_approach",
            "scope_assessment",
        }
        bumps_version = bool(content_fields & update_data.keys())

        old_data = {k: getattr(ideation, k) for k in update_data.keys()}

        # Serialize screen_mockups if present
        if (
            "screen_mockups" in update_data
            and update_data["screen_mockups"] is not None
        ):
            update_data["screen_mockups"] = [
                s.model_dump() if hasattr(s, "model_dump") else s
                for s in update_data["screen_mockups"]
            ]
            # MockupDesignSystemGate (spec 3a006f65) — defense in depth pre-persist.
            from okto_pulse.core.services.design_system import (
                gate_entity_screen_mockups,
            )

            await gate_entity_screen_mockups(
                self.db, ideation, update_data["screen_mockups"], entity_type="ideation"
            )

        ideation_json_fields = {"scope_assessment", "labels", "screen_mockups"}
        for key, value in update_data.items():
            if key == "complexity" and value is not None:
                setattr(ideation, key, IdeationComplexity(value))
            else:
                setattr(ideation, key, value)
            if key in ideation_json_fields:
                ideation.mark_dirty(key)

        if bumps_version:
            ideation.version += 1

        changes = self._compute_diff(old_data, update_data, list(update_data.keys()))

        actor_name = await resolve_actor_name(self.db, user_id, ideation.board_id)
        await self._log_activity(
            board_id=ideation.board_id,
            action="ideation_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "ideation_id": ideation_id,
                "version": ideation.version,
                "fields": list(update_data.keys()),
            },
        )
        if changes:
            changed_fields = ", ".join(c["field"] for c in changes)
            await self._record_history(
                ideation_id=ideation_id,
                action="updated",
                actor_id=user_id,
                actor_name=actor_name,
                changes=changes,
                version=ideation.version,
                summary=f"Updated: {changed_fields}",
            )
        return ideation

    # Allowed ideation transitions:
    # Draft → Review, Cancelled
    # Review → Draft, Approved, Cancelled
    # Approved → Review, Evaluating, Cancelled
    # Evaluating → Approved, Done, Cancelled
    # Done → Draft (new version)
    _IDEATION_TRANSITIONS: dict[IdeationStatus, list[IdeationStatus]] = transition_map(
        "ideation"
    )

    @staticmethod
    def _resolve_ideation_ambiguity_config(board: Board | None) -> dict[str, Any]:
        """Resolve the Max ambiguity gate config from board settings (spec 2485780b).

        Reads through the same ``settings.get(key, default)`` normalization path
        used by other governance settings, so missing legacy settings resolve to
        defaults (gate disabled, threshold 3). The threshold is clamped to 1-5
        defensively in case a legacy row persisted an out-of-range value before
        BoardSettings validation existed.
        """
        settings = (board.settings if board else None) or {}
        threshold = int(settings.get("max_ideation_ambiguity", 3))
        threshold = max(1, min(5, threshold))
        return {
            "require_ideation_ambiguity_gate": bool(
                settings.get("require_ideation_ambiguity_gate", False)
            ),
            "max_ideation_ambiguity": threshold,
        }

    async def set_ambiguity_gate_skip(
        self,
        ideation_id: str,
        user_id: str,
        skip: bool,
        *,
        reason: str,
        expected_ideation_version: int,
        expected_ideation_edition: int,
        source: str,
        actor_name: str | None = None,
    ) -> Ideation | None:
        """Dedicated write path for the per-ideation skip_ambiguity_gate flag (spec 2485780b).

        Works while the ideation is in evaluating status (or any non-draft
        status) WITHOUT routing through the generic update_ideation draft-only
        guard — so it cannot be used to smuggle other non-draft edits past that
        guard. Rejects archived ideations. Emits an auditable activity entry
        (ideation.ambiguity_gate_skip_updated) carrying actor, source path and
        the old -> new skip value. Both the REST endpoint and the MCP mirror
        call THIS method, so their behavior, validation and audit trail are
        identical (BR7 / FR5 / FR14 / FR15).
        """
        if source not in {"rest", "ui"}:
            raise ValueError("human_actor_required")
        normalized_reason = reason.strip() if isinstance(reason, str) else ""
        if not normalized_reason:
            raise ValueError("ambiguity_gate_skip_reason_required")
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return None

        if getattr(ideation, "archived", False):
            raise ValueError("Cannot update ambiguity gate skip for archived ideation.")
        if ideation.status != IdeationStatus.EVALUATING:
            raise ValueError("ideation_ambiguity_skip_status_conflict")
        if int(ideation.version) != expected_ideation_version:
            raise ValueError("version_conflict")
        current_edition = int(getattr(ideation, "edition", 1) or 1)
        if current_edition != expected_ideation_edition:
            raise ValueError("assessment_subject_edition_conflict")

        old_value = bool(ideation.skip_ambiguity_gate)
        new_value = bool(skip)
        ideation.skip_ambiguity_gate = new_value
        ideation.skip_ambiguity_gate_edition = current_edition if new_value else None

        resolved_name = actor_name or await resolve_actor_name(
            self.db, user_id, ideation.board_id
        )
        await self._log_activity(
            board_id=ideation.board_id,
            action="ideation.ambiguity_gate_skip_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "ideation_id": ideation_id,
                "source": source,
                "old_value": old_value,
                "new_value": new_value,
                "reason": normalized_reason,
                "edition": current_edition,
            },
        )
        return ideation

    @staticmethod
    def _parse_ambiguity_score(raw: Any) -> int | None:
        """Parse scope_assessment.ambiguity as an integer 1-5 (spec 2485780b TR8).

        Returns None when the value is missing, non-numeric, or outside the
        1-5 range so the gate treats it as 'not properly evaluated' and
        fails closed. ``bool`` is rejected explicitly (it is an ``int``
        subclass but never a valid ambiguity score).
        """
        if isinstance(raw, bool):
            return None
        if isinstance(raw, int):
            value = raw
        elif isinstance(raw, float) and raw.is_integer():
            value = int(raw)
        elif isinstance(raw, str) and raw.strip().lstrip("-").isdigit():
            value = int(raw.strip())
        else:
            return None
        if not 1 <= value <= 5:
            return None
        return value

    async def _enforce_ambiguity_gate(self, ideation: Ideation) -> None:
        """Enforce the canonical receipt-backed Ideation ambiguity predicate."""

        board = await _application_get(self.db, "board", ideation.board_id)
        board_settings = getattr(board, "settings", None) or {}
        configuration = resolve_ambiguity_gate_configuration(
            "ideation",
            board_settings,
        )
        skipped = bool(
            getattr(ideation, "skip_ambiguity_gate", False)
        ) and is_current_edition(
            getattr(ideation, "skip_ambiguity_gate_edition", None),
            getattr(ideation, "edition", 1),
        )
        if not configuration.required or skipped:
            return
        await self._ambiguity_gate_service_factory(self.db).evaluate(
            board_id=ideation.board_id,
            subject_type="ideation",
            subject=ideation,
            board_settings=board_settings,
            qa_items=list(getattr(ideation, "qa_items", None) or ()),
            skipped=skipped,
        )

    async def move_ideation(
        self,
        ideation_id: str,
        user_id: str,
        data: IdeationMove,
        actor_name: str | None = None,
    ) -> Ideation | None:
        """Move an ideation to a different status.

        Enforces transition rules:
        - Draft → Review → Approved → Evaluating → Done
        - Done → Draft (creates new version)
        - Any (except Done) → Cancelled
        - Evaluation can only happen in Evaluating status
        - Editing only allowed in Draft
        """
        await _application_flush(self.db)
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return None

        if getattr(ideation, "archived", False):
            raise ValueError(
                "This ideation is archived. Restore it first before changing status."
            )

        old_status = ideation.status
        old_version = int(ideation.version)
        old_edition = int(getattr(ideation, "edition", 1) or 1)
        allowed = self._IDEATION_TRANSITIONS.get(old_status, [])
        if data.status not in allowed:
            allowed_str = ", ".join(s.value for s in allowed) if allowed else "none"
            raise ValueError(
                f"Cannot move ideation from '{old_status.value}' to '{data.status.value}'. "
                f"Allowed transitions: {allowed_str}."
            )

        resolved_name = actor_name or await resolve_actor_name(
            self.db, user_id, ideation.board_id
        )

        critical_context_decision = await _authorize_critical_context_or_raise(
            self.db,
            board_id=ideation.board_id,
            actor_id=user_id,
            entity_type="ideation",
            entity_id=ideation.id,
            critical_action=_critical_ideation_move_action(data.status),
            surface="service",
            actor_type="user",
            actor_name=resolved_name,
            defer_success_audit=True,
        )

        # Snapshot on done
        if data.status == IdeationStatus.DONE:
            # Max ambiguity gate (spec 2485780b): only evaluating -> done, and
            # BEFORE ResourceGate so ambiguity errors take precedence (BR4).
            if old_status == IdeationStatus.EVALUATING:
                await self._enforce_ambiguity_gate(ideation)
            await ResourceGateService(self.db).validate_or_raise_entity_completion(
                ideation.board_id,
                "ideation",
                ideation.id,
                phase="ideation_done",
            )
            await GuidelineService(self.db).enforce_policy_transition(
                board_id=ideation.board_id,
                entity_type="ideation",
                subject_id=ideation.id,
                from_status=old_status.value,
                to_status=data.status.value,
            )

        if not await _application_fence(
            self.db,
            "ideation",
            ideation.id,
            expected_values={
                "status": old_status,
                "edition": old_edition,
                "version": old_version,
                "archived": bool(getattr(ideation, "archived", False)),
            },
        ):
            raise LifecycleTransitionConflictError("ideation", ideation.id)

        if data.status == IdeationStatus.DONE:
            if old_status == IdeationStatus.EVALUATING:
                await self._enforce_ambiguity_gate(ideation)
        await _record_critical_context_decision(
            self.db,
            decision=critical_context_decision,
            actor_name=resolved_name,
            actor_type="user",
        )

        # Reopening a terminal ideation starts a fresh editable iteration.
        if data.status == IdeationStatus.DRAFT and old_status in (
            IdeationStatus.DONE,
            IdeationStatus.CANCELLED,
        ):
            ideation.version += 1

        ideation.edition = next_lifecycle_edition(
            old_edition,
            from_status=old_status,
            to_status=data.status,
        )
        opened_new_edition = ideation.edition != old_edition
        if opened_new_edition:
            ideation.skip_ambiguity_gate = False
            ideation.skip_ambiguity_gate_edition = None
        # Cancellation justification (ITEM 17): cancel requires a reason
        # (replacing any previous one); reopening clears it.
        apply_cancellation_policy(
            ideation,
            entity_type="ideation",
            from_status=old_status,
            to_status=data.status,
            reason=getattr(data, "cancellation_reason", None),
            actor_id=user_id,
        )

        ideation.status = data.status

        # Finalize adapter-owned technical status versioning before freezing a
        # successful completion snapshot or applying edition-scoped CAS plans.
        if data.status == IdeationStatus.DONE or opened_new_edition:
            await _application_flush(self.db)
        if data.status == IdeationStatus.DONE:
            await self._create_snapshot(ideation, user_id)

        lifecycle_action = (
            "cancel"
            if data.status == IdeationStatus.CANCELLED
            else "reopen"
            if (
                data.status == IdeationStatus.DRAFT
                and old_status != IdeationStatus.DRAFT
            )
            else "admit_validation"
            if (
                data.status == IdeationStatus.EVALUATING
                and old_status != IdeationStatus.EVALUATING
            )
            else None
        )
        if lifecycle_action is not None:
            await _apply_quality_assessment_lifecycle_transition(
                self.db,
                board_id=ideation.board_id,
                subject_type="ideation",
                subject_id=ideation.id,
                before_version=old_version,
                before_status=old_status.value,
                before_archived=False,
                after_version=ideation.version,
                after_status=ideation.status.value,
                after_archived=False,
                action=lifecycle_action,
                actor_id=user_id,
                before_edition=old_edition,
                after_edition=int(ideation.edition),
            )

        # Persist the transition and publish its durable outbox event in the
        # same unit of work.  ConsolidationEnqueuer consumes this event so
        # cancelled/restored ideations cannot leave a stale KG projection.
        await _application_flush(self.db)
        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import IdeationMoved

        await event_publish(
            IdeationMoved(
                board_id=ideation.board_id,
                actor_id=user_id,
                ideation_id=ideation.id,
                from_status=old_status.value,
                to_status=data.status.value,
            ),
            session=self.db,
        )

        await self._log_activity(
            board_id=ideation.board_id,
            action="ideation_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "ideation_id": ideation_id,
                "from_status": old_status.value,
                "to_status": data.status.value,
                "version": ideation.version,
                "edition": int(ideation.edition),
            },
        )
        summary = f"Status: {old_status.value} → {data.status.value}"
        if data.status == IdeationStatus.DONE:
            summary += f" (snapshot v{ideation.version} created)"
        elif data.status == IdeationStatus.DRAFT and old_status in (
            IdeationStatus.DONE,
            IdeationStatus.CANCELLED,
        ):
            summary += f" (new iteration v{ideation.version})"

        await self._record_history(
            ideation_id=ideation_id,
            action="status_changed",
            actor_id=user_id,
            actor_name=resolved_name,
            changes=[
                {"field": "status", "old": old_status.value, "new": data.status.value},
                *(
                    [
                        {
                            "field": "edition",
                            "old": old_edition,
                            "new": int(ideation.edition),
                        }
                    ]
                    if opened_new_edition
                    else []
                ),
            ],
            summary=summary,
            version=ideation.version,
        )
        return ideation

    async def _create_snapshot(self, ideation: "Ideation", user_id: str) -> Any:
        """Create an immutable snapshot of the ideation's current state."""
        qa_snapshot = []
        for qa in ideation.qa_items or []:
            qa_snapshot.append(
                {
                    "question": qa.question,
                    "question_type": qa.question_type,
                    "choices": qa.choices,
                    "answer": qa.answer,
                    "selected": qa.selected,
                    "asked_by": qa.asked_by,
                    "answered_by": qa.answered_by,
                }
            )

        snapshot = _new_application_record(
            "ideation_snapshot",
            ideation_id=ideation.id,
            version=ideation.version,
            title=ideation.title,
            description=ideation.description,
            problem_statement=ideation.problem_statement,
            proposed_approach=ideation.proposed_approach,
            scope_assessment=ideation.scope_assessment,
            complexity=ideation.complexity.value if ideation.complexity else None,
            labels=ideation.labels,
            qa_snapshot=qa_snapshot if qa_snapshot else None,
            created_by=user_id,
        )
        await _application_add(self.db, snapshot)
        return snapshot

    async def list_snapshots(self, ideation_id: str) -> list:
        """List all snapshots for an ideation."""
        return await _application_list(
            self.db,
            "ideation_snapshot",
            filters=(_apf("ideation_id", "eq", ideation_id),),
            order_by=(("version", True),),
        )

    async def get_snapshot(self, ideation_id: str, version: int):
        """Get a specific version snapshot."""
        version = validate_snapshot_version(version)
        rows = await _application_list(
            self.db,
            "ideation_snapshot",
            filters=(
                _apf("ideation_id", "eq", ideation_id),
                _apf("version", "eq", version),
            ),
            limit=1,
        )
        return rows[0] if rows else None

    async def delete_ideation(
        self,
        ideation_id: str,
        user_id: str,
        *,
        return_receipt: bool = False,
    ) -> bool | GovernedArtifactDeletionReceipt:
        """Delete an ideation and every refinement/spec in its subtree."""
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return False

        board_id = ideation.board_id
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        descendant_deletions: list[GovernedArtifactDeletionReceipt] = []

        specs = await _application_list(
            self.db,
            "spec",
            filters=(_apf("ideation_id", "eq", ideation_id),),
        )
        refinements = await _application_list(
            self.db,
            "refinement",
            filters=(_apf("ideation_id", "eq", ideation_id),),
        )
        refinement_ids = {refinement.id for refinement in refinements}
        for refinement in refinements:
            receipt = await RefinementService(self.db).delete_refinement(
                refinement.id,
                user_id,
                return_receipt=True,
            )
            if not isinstance(receipt, GovernedArtifactDeletionReceipt):
                raise RuntimeError("governed_delete_descendant_receipt_missing")
            descendant_deletions.append(receipt)

        # A derived Spec carries both ideation_id and refinement_id. Its
        # refinement deletion already minted the governed receipt and removed
        # the row, so only delete Specs that are direct Ideation children here.
        # This avoids relying on an adapter's autoflush/query visibility and
        # preserves the actual parent/child receipt hierarchy.
        for spec in specs:
            if getattr(spec, "refinement_id", None) in refinement_ids:
                continue
            receipt = await SpecService(self.db).delete_spec(
                spec.id,
                user_id,
                return_receipt=True,
            )
            if not isinstance(receipt, GovernedArtifactDeletionReceipt):
                raise RuntimeError("governed_delete_descendant_receipt_missing")
            descendant_deletions.append(receipt)

        takedown_receipt = replace(
            await _prepare_governed_artifact_deletion(
                self.db,
                board_id=board_id,
                artifact_type="ideation",
                artifact_id=ideation_id,
            ),
            descendant_deletions=tuple(descendant_deletions),
        )
        await _purge_quality_assessment_subject(
            self.db,
            board_id=board_id,
            subject_type="ideation",
            subject_id=ideation_id,
        )
        await _application_delete(self.db, ideation)

        await self._log_activity(
            board_id=board_id,
            action="ideation_deleted",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"ideation_id": ideation_id},
        )
        return takedown_receipt if return_receipt else True

    async def evaluate_complexity(
        self,
        ideation_id: str,
        user_id: str,
        *,
        previous_scope_assessment: dict[str, Any] | None = None,
    ) -> Ideation | None:
        """Evaluate and set complexity based on scope_assessment.

        Only allowed in Evaluating status.

        Rules:
        - domains >= 3 OR ambiguity >= 3 OR dependencies >= 3 -> large
        - any >= 2 -> medium
        - else -> small
        """
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return None

        if ideation.status != IdeationStatus.EVALUATING:
            raise ValueError(
                f"Evaluation can only be performed in 'evaluating' status (current: '{ideation.status.value}'). "
                f"Move the ideation to 'evaluating' first."
            )

        from okto_pulse.core.application.ideation_scope import (
            SCOPE_SCORE_FIELDS,
            validate_scope_assessment,
        )

        scope = validate_scope_assessment(ideation.scope_assessment)
        ideation.scope_assessment = scope
        domains = scope.get("domains", 1)
        ambiguity = scope.get("ambiguity", 1)
        dependencies = scope.get("dependencies", 1)

        if domains >= 3 or ambiguity >= 3 or dependencies >= 3:
            new_complexity = IdeationComplexity.LARGE
        elif domains >= 2 or ambiguity >= 2 or dependencies >= 2:
            new_complexity = IdeationComplexity.MEDIUM
        else:
            new_complexity = IdeationComplexity.SMALL

        old_complexity = ideation.complexity
        ideation.complexity = new_complexity

        actor_name = await resolve_actor_name(self.db, user_id, ideation.board_id)
        changes = []
        if previous_scope_assessment is not None:
            previous_scope = dict(previous_scope_assessment)
            for field in SCOPE_SCORE_FIELDS:
                for candidate in (field, f"{field}_justification"):
                    old_value = previous_scope.get(candidate)
                    new_value = scope.get(candidate)
                    if old_value != new_value:
                        changes.append(
                            {
                                "field": f"scope_assessment.{candidate}",
                                "old": old_value,
                                "new": new_value,
                            }
                        )
        if old_complexity != new_complexity:
            changes.append(
                {
                    "field": "complexity",
                    "old": old_complexity.value if old_complexity else None,
                    "new": new_complexity.value,
                }
            )
        if not changes:
            return ideation
        await self._record_history(
            ideation_id=ideation_id,
            action="complexity_evaluated",
            actor_id=user_id,
            actor_name=actor_name,
            changes=changes,
            summary=f"Complexity evaluated: {new_complexity.value}",
            version=ideation.version,
        )
        await self._log_activity(
            board_id=ideation.board_id,
            action="ideation_complexity_evaluated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "ideation_id": ideation_id,
                "scope_assessment": dict(scope),
                "complexity": new_complexity.value,
                "changes": changes,
            },
        )
        return ideation

    async def derive_spec(
        self,
        ideation_id: str,
        user_id: str,
        skip_ownership_check: bool = False,
        mockup_ids: list[str] | None = None,
        kb_ids: list[str] | None = None,
        architecture_design_ids: list[str] | None = None,
        architecture_propagation_mode: str = "copy",
        query_scope: QueryScope | None = None,
    ) -> Spec | None:
        """Create a Spec draft linked to an ideation.

        Compiles context from the ideation's problem statement, proposed approach,
        scope assessment, and Q&A history. Artifacts (mockups, KBs) are automatically
        propagated. Use mockup_ids/kb_ids to select specific ones.

        Only allowed when ideation status is 'done'.
        """
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return None

        spec_service = SpecService(self.db)
        await spec_service._validate_lineage(
            ideation.board_id,
            ideation_id=ideation.id,
            refinement_id=None,
        )

        # Compile rich context from ideation data
        context_parts: list[str] = []
        if ideation.problem_statement:
            context_parts.append(f"## Problem Statement\n{ideation.problem_statement}")
        if ideation.proposed_approach:
            context_parts.append(f"## Proposed Approach\n{ideation.proposed_approach}")
        if ideation.scope_assessment:
            sa = ideation.scope_assessment
            context_parts.append(
                f"## Scope Assessment\n"
                f"- Domains: {sa.get('domains', '?')}/5\n"
                f"- Ambiguity: {sa.get('ambiguity', '?')}/5\n"
                f"- Dependencies: {sa.get('dependencies', '?')}/5\n"
                f"- Complexity: {ideation.complexity.value if ideation.complexity else 'not evaluated'}"
            )
        context = "\n\n".join(context_parts) if context_parts else ideation.description

        # Snapshot parent collections before flush: eager-loaded collections can
        # expire after create_spec flushes the new child entity.
        snapshot_qa = list(ideation.qa_items or [])
        snapshot_kbs = list(ideation.knowledge_bases or [])

        validate_artifact_selections(
            source_mockups=list(ideation.screen_mockups or []),
            source_knowledge_bases=snapshot_kbs,
            mockup_ids=mockup_ids,
            kb_ids=kb_ids,
            source_type="ideation",
            source_id=ideation_id,
        )
        await preflight_architecture_designs(
            self.db,
            source_parent_type="ideation",
            source_parent_id=ideation_id,
            mode=architecture_propagation_mode,
            design_ids=architecture_design_ids,
        )

        spec_data = SpecCreate(
            title=ideation.title,
            description=ideation.description,
            context=context,
            ideation_id=ideation_id,
            labels=ideation.labels,
        )
        spec = await spec_service.create_spec(
            ideation.board_id,
            user_id,
            spec_data,
            skip_ownership_check=skip_ownership_check,
            query_scope=query_scope,
        )
        if spec:
            # Propagate mockups and Q&A from ideation to spec
            artifact_counts = await propagate_artifacts(
                db=self.db,
                source_mockups=ideation.screen_mockups,
                source_qa_items=snapshot_qa,
                source_knowledge_bases=snapshot_kbs,
                target_entity=spec,
                target_kb_entity="spec_knowledge_base",
                user_id=user_id,
                mockup_ids=mockup_ids,
                kb_ids=kb_ids,
                source_type="ideation",
                source_id=ideation.id,
                source_title=ideation.title,
                source_version=ideation.version,
            )
            architecture_designs = await propagate_architecture_designs(
                self.db,
                source_parent_type="ideation",
                source_parent_id=ideation_id,
                target_parent_type="spec",
                target_parent_id=spec.id,
                actor_id=user_id,
                mode=architecture_propagation_mode,
                design_ids=architecture_design_ids,
            )
            spec.attach(
                "resource_propagation",
                _resource_propagation_summary(
                    source_parent_type="ideation",
                    source_parent_id=ideation_id,
                    target_parent_type="spec",
                    target_parent_id=spec.id,
                    architecture_mode=architecture_propagation_mode,
                    architecture_requested_ids=architecture_design_ids,
                    architecture_designs=architecture_designs,
                    artifact_counts=artifact_counts,
                ),
            )

            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import IdeationDerivedToSpec

            await event_publish(
                IdeationDerivedToSpec(
                    board_id=ideation.board_id,
                    actor_id=user_id,
                    ideation_id=ideation_id,
                    spec_id=spec.id,
                ),
                session=self.db,
            )

            actor_name = await resolve_actor_name(self.db, user_id, ideation.board_id)
            await self._record_history(
                ideation_id=ideation_id,
                action="spec_draft_created",
                actor_id=user_id,
                actor_name=actor_name,
                changes=[{"field": "spec", "old": None, "new": spec.id}],
                summary=f"Spec draft created: {spec.title} (requirements to be defined)",
                version=ideation.version,
            )
        return spec

    async def _log_activity(self, **kwargs: Any) -> None:
        """Log an activity."""
        await _application_add(
            self.db,
            _new_application_record("activity_log", **kwargs),
        )


class IdeationQAService:
    """Service for ideation Q&A operations."""

    def __init__(self, db: Any):
        self.db = db

    async def get_question(self, qa_id: str) -> IdeationQAItem | None:
        """Load a Q&A item so callers can authorize its canonical parent."""
        return await _application_get(self.db, "ideation_qa_item", qa_id)

    async def create_question(
        self, ideation_id: str, user_id: str, data: IdeationQACreate
    ) -> IdeationQAItem | None:
        """Create a question on an ideation (text or choice)."""
        ideation = await _application_get(self.db, "ideation", ideation_id)
        if not ideation:
            return None
        require_draft_mutation(ideation, subject_type="ideation")
        qa = _new_application_record(
            "ideation_qa_item",
            ideation_id=ideation_id,
            question=data.question,
            question_type=data.question_type or "text",
            choices=[c.model_dump() for c in data.choices] if data.choices else None,
            allow_free_text=data.allow_free_text,
            asked_by=user_id,
        )
        await _application_add(self.db, qa)
        await _publish_quality_clarification_changed(
            self.db,
            subject=ideation,
            subject_type="ideation",
            qa_id=getattr(qa, "id", None),
            operation="created",
            actor_id=user_id,
        )
        return qa

    async def answer_question(
        self,
        qa_id: str,
        user_id: str,
        data: IdeationQAAnswer,
        *,
        actor_type: str = "user",
        surface: str = "service",
    ) -> IdeationQAItem | None:
        """Answer an ideation Q&A question (text or choice selection).

        Accepts `question_type in {"choice","single_choice","multi_choice"}`
        — `single_choice` is treated as an alias of `choice`. Only commits
        `answered_at`/`answered_by` when something was actually persisted,
        otherwise returns None so the route surfaces a 404 instead of a
        false-positive 200 (which caused the "toast says saved but the
        question flips back to unanswered" UX bug).
        """
        qa = await _application_get(self.db, "ideation_qa_item", qa_id)
        if not qa:
            return None

        ideation = await _application_get(self.db, "ideation", qa.ideation_id)
        if ideation is None:
            raise RuntimeError("quality_clarification_subject_missing")
        require_draft_mutation(ideation, subject_type="ideation")
        board = await _application_get(self.db, "board", ideation.board_id)
        await _authorize_qa_answer_or_raise(
            self.db,
            board=board,
            qa=qa,
            user_id=user_id,
            entity_type="ideation",
            question_id=qa_id,
            actor_type=actor_type,
            surface=surface,
        )

        saved_something = False
        choice_types = ("choice", "single_choice", "multi_choice")
        if qa.question_type in choice_types and data.selected:
            data.selected = validate_choice_selection(
                qa.question_type, data.selected, qa.choices
            )
            qa.selected = data.selected
            saved_something = True

        if data.answer:
            qa.answer = data.answer
            saved_something = True
        elif qa.question_type not in choice_types and data.answer == "":
            # Explicit clear of a free-text answer.
            qa.answer = None

        if not saved_something:
            return None

        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        await _publish_quality_clarification_changed(
            self.db,
            subject=ideation,
            subject_type="ideation",
            qa_id=qa.id,
            operation="answered",
            actor_id=user_id,
            actor_type=actor_type,
        )
        return qa

    async def list_qa(self, ideation_id: str) -> list[IdeationQAItem]:
        """List all Q&A items for an ideation."""
        return await _application_list(
            self.db,
            "ideation_qa_item",
            filters=(_apf("ideation_id", "eq", ideation_id),),
            order_by=(("created_at", False),),
        )

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await _application_get(self.db, "ideation_qa_item", qa_id)
        if not qa:
            return False
        ideation = await _application_get(
            self.db,
            "ideation",
            qa.ideation_id,
        )
        if ideation is None:
            raise RuntimeError("quality_clarification_subject_missing")
        require_draft_mutation(ideation, subject_type="ideation")
        await _application_delete(self.db, qa)
        await _publish_quality_clarification_changed(
            self.db,
            subject=ideation,
            subject_type="ideation",
            qa_id=qa_id,
            operation="deleted",
            actor_id=None,
        )
        return True


def _build_default_refinement_cognitive_done_guard() -> Any:
    """Backward-compatible alias for the shared closeout gate factory."""

    return _build_default_cognitive_closeout_gate()


class RefinementService:
    """Service for refinement operations."""

    def __init__(self, db: Any):
        self.db = db
        self._ambiguity_gate_service_factory: Callable[[Any], AmbiguityGateService] = (
            _build_default_ambiguity_gate_service
        )
        # Cognitive closeout must run BEFORE snapshot/status mutation.
        # Keep the historical attribute name so existing tests and callers
        # that inject a fake guard continue to work.
        self._cognitive_done_guard_factory: Callable[[], Any] = (
            _build_default_refinement_cognitive_done_guard
        )
        self._cognitive_readiness_service_factory: Callable[[], Any] = (
            _build_default_cognitive_readiness_service
        )

    async def _validate_cognitive_done(
        self,
        refinement: ApplicationRecord,
        board: ApplicationRecord | None = None,
        *,
        read_only_preview: bool = False,
    ) -> None:
        if board is None:
            board = await _application_get(self.db, "board", refinement.board_id)
        await _evaluate_entity_cognitive_done_or_raise(
            db=self.db,
            gate_factory=self._cognitive_done_guard_factory,
            readiness_service_factory=self._cognitive_readiness_service_factory,
            board=board,
            board_id=refinement.board_id,
            entity_type="refinement",
            entity_id=refinement.id,
            entity=refinement,
            target_label="refinement",
            resolve_graph_state=True,
        )

    async def _enforce_ambiguity_gate(
        self,
        refinement: ApplicationRecord,
        board: ApplicationRecord | None = None,
    ) -> None:
        """Evaluate the same receipt-backed predicate used by readiness."""

        if board is None:
            board = await _application_get(self.db, "board", refinement.board_id)
        board_settings = getattr(board, "settings", None) or {}
        configuration = resolve_ambiguity_gate_configuration(
            "refinement",
            board_settings,
        )
        skipped = bool(
            getattr(refinement, "skip_ambiguity_gate", False)
        ) and is_current_edition(
            getattr(refinement, "skip_ambiguity_gate_edition", None),
            getattr(refinement, "edition", 1),
        )
        if not configuration.required or skipped:
            return
        await self._ambiguity_gate_service_factory(self.db).evaluate(
            board_id=refinement.board_id,
            subject_type="refinement",
            subject=refinement,
            board_settings=board_settings,
            qa_items=list(getattr(refinement, "qa_items", None) or ()),
            skipped=skipped,
        )

    _STATUS_ORDER = {
        RefinementStatus.DRAFT: 0,
        RefinementStatus.REVIEW: 1,
        RefinementStatus.APPROVED: 2,
        RefinementStatus.DONE: 3,
        RefinementStatus.CANCELLED: 3,
    }

    async def _record_history(
        self,
        refinement_id: str,
        action: str,
        actor_id: str,
        actor_name: str,
        actor_type: str = "user",
        changes: list[dict] | None = None,
        summary: str | None = None,
        version: int | None = None,
    ) -> None:
        """Record a history entry for a refinement."""
        entry = _new_application_record(
            "refinement_history",
            refinement_id=refinement_id,
            action=action,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            changes=changes,
            summary=summary,
            version=version,
        )
        await _application_add(self.db, entry)

    async def list_history(
        self,
        refinement_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> list[RefinementHistory]:
        """List history entries for a refinement, newest first."""
        window = validate_history_window(limit, offset)
        return await _application_list(
            self.db,
            "refinement_history",
            filters=(_apf("refinement_id", "eq", refinement_id),),
            order_by=(("created_at", True), ("id", True)),
            offset=window.offset,
            limit=window.limit,
        )

    async def count_history(self, refinement_id: str) -> int:
        """Count every history entry for a refinement independently of a page."""
        return await _application_count(
            self.db,
            "refinement_history",
            filters=(_apf("refinement_id", "eq", refinement_id),),
        )

    @staticmethod
    def _compute_diff(old_data: dict, new_data: dict, fields: list[str]) -> list[dict]:
        """Compute field-level diffs between old and new data."""
        changes = []
        for field in fields:
            old_val = old_data.get(field)
            new_val = new_data.get(field)
            if hasattr(old_val, "value"):
                old_val = old_val.value
            if hasattr(new_val, "value"):
                new_val = new_val.value
            if old_val != new_val:
                changes.append({"field": field, "old": old_val, "new": new_val})
        return changes

    async def create_refinement(
        self,
        ideation_id: str,
        user_id: str,
        data: RefinementCreate,
        skip_ownership_check: bool = False,
        *,
        query_scope: QueryScope | None = None,
    ) -> Refinement | None:
        """Create a new refinement for a done ideation.

        The ideation must be in 'done' status (snapshotted) before refinements
        can be created from it — same governance as spec derivation.

        Always preserves the parent ideation's structured context as a
        derivation snapshot. If a custom description is provided, the inherited
        context is appended instead of being skipped.
        """
        ideation_service = IdeationService(self.db)
        ideation = await ideation_service.get_ideation(ideation_id)
        if not ideation:
            return None

        if ideation.status != IdeationStatus.DONE:
            raise ValueError("Refinements can only be created from a 'done' ideation")

        board_id = ideation.board_id
        if not skip_ownership_check:
            board_query = _board_scope_select(
                board_id=board_id,
                user_id=user_id,
                query_scope=query_scope,
                require_ownership=(
                    query_scope.require_ownership if query_scope is not None else True
                ),
            )
            if board_query is None:
                return None
            if not await _application_run(self.db, board_query):
                return None

        description = data.description.strip() if data.description else None
        parent_context = compile_ideation_parent_context(ideation)
        if parent_context:
            if description:
                if "## Parent Ideation Context" not in description:
                    description = f"{description}\n\n{parent_context}"
            else:
                description = parent_context

        # Parse optional mockup/kb filters from data (if present)
        prop_mockup_ids = getattr(data, "mockup_ids", None)
        prop_kb_ids = getattr(data, "kb_ids", None)
        prop_architecture_ids = getattr(data, "architecture_design_ids", None)
        architecture_mode = getattr(data, "architecture_propagation_mode", "copy")

        validate_artifact_selections(
            source_mockups=list(ideation.screen_mockups or []),
            source_knowledge_bases=list(ideation.knowledge_bases or []),
            mockup_ids=prop_mockup_ids,
            kb_ids=prop_kb_ids,
            source_type="ideation",
            source_id=ideation_id,
        )
        await preflight_architecture_designs(
            self.db,
            source_parent_type="ideation",
            source_parent_id=ideation_id,
            mode=architecture_mode,
            design_ids=prop_architecture_ids,
        )

        refinement = _new_application_record(
            "refinement",
            ideation_id=ideation_id,
            board_id=board_id,
            title=data.title,
            description=description,
            in_scope=data.in_scope,
            out_of_scope=data.out_of_scope,
            analysis=data.analysis,
            decisions=data.decisions,
            screen_mockups=None,  # assigned after the Design System gate (below)
            assignee_id=data.assignee_id,
            created_by=user_id,
            labels=data.labels or ideation.labels,
            edition=1,
        )
        # MockupDesignSystemGate (spec 3a006f65 / card 0192f58d): gate the MANUAL mockups
        # submitted at creation BEFORE persistence (old=[] baseline). Propagated mockups
        # added below by propagate_artifacts are gated inside that helper.
        _submitted_mockups = [
            item.model_dump() if hasattr(item, "model_dump") else item
            for item in (data.screen_mockups or [])
        ] or None
        if _submitted_mockups:
            from okto_pulse.core.services.design_system import (
                gate_entity_screen_mockups,
            )

            await gate_entity_screen_mockups(
                self.db, refinement, _submitted_mockups, entity_type="refinement"
            )
            refinement.screen_mockups = _submitted_mockups
        await _application_add(self.db, refinement)

        # Propagate artifacts from ideation (mockups, KBs, Q&A)
        artifact_counts = await propagate_artifacts(
            db=self.db,
            source_mockups=ideation.screen_mockups,
            source_qa_items=ideation.qa_items,
            source_knowledge_bases=ideation.knowledge_bases,
            target_entity=refinement,
            target_kb_entity="refinement_knowledge_base",
            user_id=user_id,
            mockup_ids=prop_mockup_ids,
            kb_ids=prop_kb_ids,
            source_type="ideation",
            source_id=ideation.id,
            source_title=ideation.title,
            source_version=ideation.version,
        )
        architecture_designs = await propagate_architecture_designs(
            self.db,
            source_parent_type="ideation",
            source_parent_id=ideation_id,
            target_parent_type="refinement",
            target_parent_id=refinement.id,
            actor_id=user_id,
            mode=architecture_mode,
            design_ids=prop_architecture_ids,
        )
        refinement.attach(
            "resource_propagation",
            _resource_propagation_summary(
                source_parent_type="ideation",
                source_parent_id=ideation_id,
                target_parent_type="refinement",
                target_parent_id=refinement.id,
                architecture_mode=architecture_mode,
                architecture_requested_ids=prop_architecture_ids,
                architecture_designs=architecture_designs,
                artifact_counts=artifact_counts,
            ),
        )

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="refinement_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "title": data.title,
                "refinement_id": refinement.id,
                "ideation_id": ideation_id,
            },
        )
        await self._record_history(
            refinement_id=refinement.id,
            action="created",
            actor_id=user_id,
            actor_name=actor_name,
            summary=f"Refinement created: {data.title}",
            version=1,
            changes=[
                {"field": "title", "old": None, "new": data.title},
                {"field": "status", "old": None, "new": RefinementStatus.DRAFT.value},
                *(
                    [{"field": "in_scope", "old": None, "new": data.in_scope}]
                    if data.in_scope
                    else []
                ),
                *(
                    [{"field": "out_of_scope", "old": None, "new": data.out_of_scope}]
                    if data.out_of_scope
                    else []
                ),
                *(
                    [{"field": "analysis", "old": None, "new": data.analysis}]
                    if data.analysis
                    else []
                ),
                *(
                    [{"field": "decisions", "old": None, "new": data.decisions}]
                    if data.decisions
                    else []
                ),
            ],
        )
        return refinement

    async def get_refinement(self, refinement_id: str) -> Refinement | None:
        """Get a refinement by ID with specs, knowledge_bases, and qa_items."""
        refinement = await _application_get(
            self.db,
            "refinement",
            refinement_id,
            includes=(
                "ideation.qa_items",
                "ideation.knowledge_bases",
                "ideation.architecture_designs",
                "specs.architecture_designs",
                "knowledge_bases",
                "qa_items",
                "architecture_designs",
            ),
        )
        if refinement:
            await _attach_active_spec_counts(self.db, [refinement])
        return refinement

    async def list_refinements(
        self,
        ideation_id: str,
        status_filter: str | None = None,
        include_archived: bool = False,
    ) -> list[Refinement]:
        """List refinements for an ideation, optionally filtered by status."""
        filters = [_apf("ideation_id", "eq", ideation_id)]
        if status_filter:
            filters.append(_apf("status", "eq", RefinementStatus(status_filter)))
        if not include_archived:
            filters.append(_apf("archived", "is_false"))
        rows = await _application_list(
            self.db,
            "refinement",
            filters=tuple(filters),
            order_by=(("updated_at", True),),
            includes=("architecture_designs",),
        )
        await _attach_open_qa_counts(
            self.db,
            rows,
            "refinement_qa_item",
            "refinement_id",
        )
        await _attach_active_spec_counts(self.db, rows)
        return rows

    async def update_refinement(
        self, refinement_id: str, user_id: str, data: RefinementUpdate
    ) -> Refinement | None:
        """Update a refinement. Bumps version on content changes. Records field-level diffs.

        Only allowed in Draft status — all other statuses are read-only.
        """
        refinement = await self.get_refinement(refinement_id)
        if not refinement:
            return None

        if getattr(refinement, "archived", False):
            raise ValueError(
                "This refinement is archived. Restore it first before making changes."
            )

        require_draft_mutation(refinement, subject_type="refinement")

        update_data = data.model_dump(exclude_unset=True)
        content_fields = {
            "title",
            "description",
            "in_scope",
            "out_of_scope",
            "analysis",
            "decisions",
        }
        # Spec eaf78891 (Ideação #2): refinement_semantic_fields cover all
        # update_data keys that affect KG extraction. Refinements have a much
        # smaller surface than specs, so any update is treated as semantic.
        bumps_version = bool(content_fields & update_data.keys())
        bumps_semantic = bool(update_data)

        old_data = {k: getattr(refinement, k) for k in update_data.keys()}

        # Serialize screen_mockups if present
        if (
            "screen_mockups" in update_data
            and update_data["screen_mockups"] is not None
        ):
            update_data["screen_mockups"] = [
                s.model_dump() if hasattr(s, "model_dump") else s
                for s in update_data["screen_mockups"]
            ]
            # MockupDesignSystemGate (spec 3a006f65) — defense in depth pre-persist.
            from okto_pulse.core.services.design_system import (
                gate_entity_screen_mockups,
            )

            await gate_entity_screen_mockups(
                self.db,
                refinement,
                update_data["screen_mockups"],
                entity_type="refinement",
            )

        refinement_json_fields = {
            "in_scope",
            "out_of_scope",
            "labels",
            "screen_mockups",
        }
        for key, value in update_data.items():
            setattr(refinement, key, value)
            if key in refinement_json_fields:
                refinement.mark_dirty(key)

        if bumps_version:
            refinement.version += 1

        if bumps_semantic:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import RefinementSemanticChanged

            await event_publish(
                RefinementSemanticChanged(
                    board_id=refinement.board_id,
                    actor_id=user_id,
                    refinement_id=refinement.id,
                    changed_fields=sorted(update_data.keys()),
                ),
                session=self.db,
            )

        changes = self._compute_diff(old_data, update_data, list(update_data.keys()))

        actor_name = await resolve_actor_name(self.db, user_id, refinement.board_id)
        await self._log_activity(
            board_id=refinement.board_id,
            action="refinement_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "refinement_id": refinement_id,
                "version": refinement.version,
                "fields": list(update_data.keys()),
            },
        )
        if changes:
            changed_fields = ", ".join(c["field"] for c in changes)
            await self._record_history(
                refinement_id=refinement_id,
                action="updated",
                actor_id=user_id,
                actor_name=actor_name,
                changes=changes,
                version=refinement.version,
                summary=f"Updated: {changed_fields}",
            )
        return refinement

    async def set_ambiguity_gate_skip(
        self,
        refinement_id: str,
        user_id: str,
        skip: bool,
        *,
        reason: str,
        expected_refinement_version: int,
        expected_refinement_edition: int,
        source: str,
        actor_name: str | None = None,
    ) -> RefinementAmbiguityGateSkipResult | None:
        """Apply/remove the human-only Refinement ambiguity skip.

        The override is governance metadata, not semantic content: it is
        version-fenced but does not bump the Refinement version or stale an
        otherwise current assessment.  It bypasses only the ambiguity
        predicate; Resource and Cognitive gates still execute.
        """

        if source not in {"rest", "ui"}:
            raise ValueError("human_actor_required")
        if not isinstance(skip, bool):
            raise ValueError("skip_ambiguity_gate_invalid")
        normalized_reason = reason.strip() if isinstance(reason, str) else ""
        if not normalized_reason:
            raise ValueError("ambiguity_gate_skip_reason_required")
        if (
            not isinstance(expected_refinement_version, int)
            or isinstance(expected_refinement_version, bool)
            or expected_refinement_version < 1
        ):
            raise ValueError("expected_refinement_version_invalid")

        refinement = await self.get_refinement(refinement_id)
        if refinement is None:
            return None
        if bool(getattr(refinement, "archived", False)):
            raise ValueError("refinement_archived")
        if refinement.status != RefinementStatus.APPROVED:
            raise ValueError("refinement_ambiguity_skip_status_conflict")
        if refinement.version != expected_refinement_version:
            raise ValueError("version_conflict")
        current_edition = int(getattr(refinement, "edition", 1) or 1)
        if current_edition != expected_refinement_edition:
            raise ValueError("assessment_subject_edition_conflict")

        old_value = bool(getattr(refinement, "skip_ambiguity_gate", False))
        refinement.skip_ambiguity_gate = skip
        refinement.skip_ambiguity_gate_edition = current_edition if skip else None
        resolved_name = actor_name or await resolve_actor_name(
            self.db,
            user_id,
            refinement.board_id,
        )
        activity = _new_application_record(
            "activity_log",
            board_id=refinement.board_id,
            action="refinement.ambiguity_gate_skip_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "refinement_id": refinement.id,
                "source": source,
                "reason": normalized_reason,
                "old_value": old_value,
                "new_value": skip,
                "state_changed": old_value != skip,
                "expected_refinement_version": expected_refinement_version,
                "refinement_version": refinement.version,
                "edition": current_edition,
            },
        )
        await _application_add(self.db, activity)
        return RefinementAmbiguityGateSkipResult(
            refinement=refinement,
            activity_id=activity.id,
            skipped=skip,
            version=refinement.version,
            edition=current_edition,
        )

    # Allowed refinement transitions:
    # Draft → Review, Cancelled
    # Review → Draft, Approved, Cancelled
    # Approved → Review, Done, Cancelled
    # Done → Draft (new version)
    _REFINEMENT_TRANSITIONS: dict[RefinementStatus, list[RefinementStatus]] = (
        transition_map("refinement")
    )

    async def move_refinement(
        self,
        refinement_id: str,
        user_id: str,
        data: RefinementMove,
        actor_name: str | None = None,
    ) -> Refinement | None:
        """Move a refinement to a different status.

        Enforces transition rules:
        - Draft → Review → Approved → Done
        - Done → Draft (creates new version)
        - Any (except Done) → Cancelled
        - Editing only allowed in Draft
        """
        await _application_flush(self.db)
        refinement = await self.get_refinement(refinement_id)
        if not refinement:
            return None

        if getattr(refinement, "archived", False):
            raise ValueError(
                "This refinement is archived. Restore it first before changing status."
            )

        old_status = refinement.status
        old_version = int(refinement.version)
        old_edition = int(getattr(refinement, "edition", 1) or 1)
        allowed = self._REFINEMENT_TRANSITIONS.get(old_status, [])
        if data.status not in allowed:
            allowed_str = ", ".join(s.value for s in allowed) if allowed else "none"
            raise ValueError(
                f"Cannot move refinement from '{old_status.value}' to '{data.status.value}'. "
                f"Allowed transitions: {allowed_str}."
            )

        # Content gate — draft→review requires at least one non-empty in_scope
        # entry. Prevents stub refinements (no design intent captured) from
        # leaking into review / approved / done where downstream tools
        # (derive_spec, get_refinement_context) would operate on them.
        if (
            old_status == RefinementStatus.DRAFT
            and data.status == RefinementStatus.REVIEW
        ):
            in_scope_items = refinement.in_scope or []
            if not any(isinstance(s, str) and s.strip() for s in in_scope_items):
                raise ValueError(
                    "Refinement must have at least one in_scope item before "
                    "moving to review.",
                )

        resolved_name = actor_name or await resolve_actor_name(
            self.db, user_id, refinement.board_id
        )
        board = await _application_get(self.db, "board", refinement.board_id)
        await evaluate_code_traceability_transition(
            self.db,
            board=board,
            subject=refinement,
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
            from_status=old_status.value,
            to_status=data.status.value,
            enforce=True,
        )

        critical_context_decision = await _authorize_critical_context_or_raise(
            self.db,
            board_id=refinement.board_id,
            actor_id=user_id,
            entity_type="refinement",
            entity_id=refinement.id,
            critical_action=_critical_refinement_move_action(data.status),
            surface="service",
            actor_type="user",
            actor_name=resolved_name,
            defer_success_audit=True,
        )

        # One ordered completion predicate across preview and mutation:
        # Ambiguity -> Resource -> Cognitive.  Every gate runs before snapshot,
        # history, activity, event or status mutation.
        if data.status == RefinementStatus.DONE:
            await self._enforce_ambiguity_gate(refinement, board)
            await ResourceGateService(self.db).validate_or_raise_entity_completion(
                refinement.board_id,
                "refinement",
                refinement.id,
                phase="refinement_done",
            )
            await self._validate_cognitive_done(refinement, board)
            await GuidelineService(self.db).enforce_policy_transition(
                board_id=refinement.board_id,
                entity_type="refinement",
                subject_id=refinement.id,
                from_status=old_status.value,
                to_status=data.status.value,
            )

        if not await _application_fence(
            self.db,
            "refinement",
            refinement.id,
            expected_values={
                "status": old_status,
                "edition": old_edition,
                "version": old_version,
                "archived": bool(getattr(refinement, "archived", False)),
            },
        ):
            raise LifecycleTransitionConflictError("refinement", refinement.id)

        if data.status == RefinementStatus.DONE:
            await self._enforce_ambiguity_gate(refinement, board)
        await _record_critical_context_decision(
            self.db,
            decision=critical_context_decision,
            actor_name=resolved_name,
            actor_type="user",
        )

        # Reopening a terminal refinement starts a fresh editable iteration.
        if data.status == RefinementStatus.DRAFT and old_status in (
            RefinementStatus.DONE,
            RefinementStatus.CANCELLED,
        ):
            refinement.version += 1

        refinement.edition = next_lifecycle_edition(
            old_edition,
            from_status=old_status,
            to_status=data.status,
        )
        opened_new_edition = refinement.edition != old_edition
        if opened_new_edition:
            refinement.skip_ambiguity_gate = False
            refinement.skip_ambiguity_gate_edition = None

        # Cancellation justification (ITEM 17): cancel requires a reason
        # (replacing any previous one); reopening clears it.
        apply_cancellation_policy(
            refinement,
            entity_type="refinement",
            from_status=old_status,
            to_status=data.status,
            reason=getattr(data, "cancellation_reason", None),
            actor_id=user_id,
        )

        refinement.status = data.status
        if data.status == RefinementStatus.DONE or opened_new_edition:
            await _application_flush(self.db)
        if data.status == RefinementStatus.DONE:
            await self._create_snapshot(refinement, user_id)
        lifecycle_action = (
            "cancel"
            if data.status == RefinementStatus.CANCELLED
            else "reopen"
            if (
                data.status == RefinementStatus.DRAFT
                and old_status != RefinementStatus.DRAFT
            )
            else "admit_validation"
            if (
                data.status == RefinementStatus.APPROVED
                and old_status != RefinementStatus.APPROVED
            )
            else None
        )
        if lifecycle_action is not None:
            await _apply_quality_assessment_lifecycle_transition(
                self.db,
                board_id=refinement.board_id,
                subject_type="refinement",
                subject_id=refinement.id,
                before_version=old_version,
                before_status=old_status.value,
                before_archived=False,
                after_version=refinement.version,
                after_status=refinement.status.value,
                after_archived=False,
                action=lifecycle_action,
                actor_id=user_id,
                before_edition=old_edition,
                after_edition=int(refinement.edition),
            )
        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import (
            RefinementMoved,
            RefinementSemanticChanged,
        )

        await event_publish(
            RefinementSemanticChanged(
                board_id=refinement.board_id,
                actor_id=user_id,
                refinement_id=refinement.id,
                changed_fields=["status"],
            ),
            session=self.db,
        )
        await event_publish(
            RefinementMoved(
                board_id=refinement.board_id,
                actor_id=user_id,
                refinement_id=refinement.id,
                from_status=old_status.value,
                to_status=data.status.value,
            ),
            session=self.db,
        )

        await self._log_activity(
            board_id=refinement.board_id,
            action="refinement_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "refinement_id": refinement_id,
                "from_status": old_status.value,
                "to_status": data.status.value,
                "version": refinement.version,
                "edition": int(refinement.edition),
            },
        )
        summary = f"Status: {old_status.value} \u2192 {data.status.value}"
        if data.status == RefinementStatus.DONE:
            summary += f" (snapshot v{refinement.version} created)"
        elif (
            data.status == RefinementStatus.DRAFT
            and old_status == RefinementStatus.DONE
        ):
            summary += f" (new iteration v{refinement.version})"

        await self._record_history(
            refinement_id=refinement_id,
            action="status_changed",
            actor_id=user_id,
            actor_name=resolved_name,
            changes=[
                {"field": "status", "old": old_status.value, "new": data.status.value},
                *(
                    [
                        {
                            "field": "edition",
                            "old": old_edition,
                            "new": int(refinement.edition),
                        }
                    ]
                    if opened_new_edition
                    else []
                ),
            ],
            summary=summary,
            version=refinement.version,
        )
        return refinement

    async def _create_snapshot(
        self, refinement: "Refinement", user_id: str
    ) -> "RefinementSnapshot":
        """Create an immutable snapshot of the refinement's current state."""
        qa_snapshot = []
        for qa in refinement.qa_items or []:
            qa_snapshot.append(
                {
                    "question": qa.question,
                    "question_type": qa.question_type,
                    "choices": qa.choices,
                    "answer": qa.answer,
                    "selected": qa.selected,
                    "asked_by": qa.asked_by,
                    "answered_by": qa.answered_by,
                }
            )

        code_evidence_manifest: list[dict[str, str]] = []
        try:
            from okto_pulse.core.ports.relational_application import (
                RelationalApplicationAdapterMissing,
                require_relational_application_adapter,
            )

            relational_adapter = require_relational_application_adapter()
            traceability_factory = getattr(
                relational_adapter,
                "code_traceability",
                None,
            )
            if not callable(traceability_factory):
                raise RelationalApplicationAdapterMissing(
                    "The composed relational adapter does not expose the "
                    "code-traceability store."
                )
            traceability_store = traceability_factory(self.db)
            cursor = None
            evidence_count = 0
            while True:
                page = await traceability_store.list_evidence(
                    CodeEvidenceQuery(
                        board_id=refinement.board_id,
                        parent_type=CodeTraceabilitySubjectType.REFINEMENT,
                        parent_id=refinement.id,
                        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
                        cursor=cursor,
                        limit=200,
                    )
                )
                for evidence in page.items:
                    if evidence.parent_version <= refinement.version:
                        code_evidence_manifest.append(
                            {
                                "evidence_id": evidence.id,
                                "content_sha256": evidence.content_sha256,
                                "lifecycle_status": evidence.lifecycle_status.value,
                            }
                        )
                evidence_count += len(page.items)
                if evidence_count > 2_000:
                    raise CodeInvestigationCurrentnessUnknown(
                        details={"reason": "snapshot_evidence_manifest_limit"}
                    )
                cursor = page.next_cursor
                if cursor is None:
                    break
        except (
            CodeTraceabilityAdapterMissing,
            RelationalApplicationAdapterMissing,
        ) as exc:
            # Agent-mediated traceability is always evaluated. Never seal a
            # falsely empty manifest when its authoritative store is absent.
            raise CodeInvestigationCurrentnessUnknown(
                details={"reason": "snapshot_traceability_adapter_unavailable"}
            ) from exc
        code_evidence_manifest.sort(key=lambda item: item["evidence_id"])

        snapshot = _new_application_record(
            "refinement_snapshot",
            refinement_id=refinement.id,
            version=refinement.version,
            title=refinement.title,
            description=refinement.description,
            in_scope=refinement.in_scope,
            out_of_scope=refinement.out_of_scope,
            analysis=refinement.analysis,
            decisions=refinement.decisions,
            labels=refinement.labels,
            qa_snapshot=qa_snapshot if qa_snapshot else None,
            code_evidence_manifest=code_evidence_manifest,
            created_by=user_id,
        )
        await _application_add(self.db, snapshot)
        return snapshot

    async def list_snapshots(self, refinement_id: str) -> list:
        """List all snapshots for a refinement."""
        return await _application_list(
            self.db,
            "refinement_snapshot",
            filters=(_apf("refinement_id", "eq", refinement_id),),
            order_by=(("version", True),),
        )

    async def get_snapshot(self, refinement_id: str, version: int):
        """Get a specific version snapshot."""
        version = validate_snapshot_version(version)
        rows = await _application_list(
            self.db,
            "refinement_snapshot",
            filters=(
                _apf("refinement_id", "eq", refinement_id),
                _apf("version", "eq", version),
            ),
            limit=1,
        )
        return rows[0] if rows else None

    async def resolve_completed_snapshot(
        self,
        refinement: "Refinement",
    ) -> "RefinementSnapshot":
        """Resolve the immutable Done source without synthesizing history.

        Current writers snapshot the post-flush version. Legacy records can
        expose completed content and its status-only proof at ``vN`` while the
        live aggregate is already ``vN+1``. That one compatibility shape is
        accepted only when history proves the completed snapshot changed
        status from Approved to Done and nothing else.
        """

        if refinement.status != RefinementStatus.DONE:
            raise SpecLineagePreflightError(
                "spec_refinement_not_done",
                "A Spec can only be derived from a completed Refinement.",
                facts={
                    "refinement_id": refinement.id,
                    "refinement_status": refinement.status.value,
                },
            )
        live_version = int(refinement.version)
        exact = await self.get_snapshot(refinement.id, live_version)
        if exact is not None:
            return exact
        if live_version <= 1:
            previous = None
        else:
            previous = await self.get_snapshot(refinement.id, live_version - 1)
        if previous is not None:
            # A compatible legacy record can expose the immutable completed
            # snapshot and its strict status-only proof at N while the live
            # aggregate is already N+1.  The proof is therefore keyed by the
            # snapshot version, never inferred from the live row alone.
            rows = await _application_list(
                self.db,
                "refinement_history",
                filters=(
                    _apf("refinement_id", "eq", refinement.id),
                    _apf("action", "eq", "status_changed"),
                    _apf("version", "eq", live_version - 1),
                ),
                order_by=(("created_at", True), ("id", True)),
                # Two rows are enough to reject an ambiguous/additional
                # history proof without loading unbounded history.
                limit=2,
            )
            change_set = list(getattr(rows[0], "changes", None) or ()) if rows else []
            status_only_done = (
                len(rows) == 1
                and len(change_set) == 1
                and isinstance(change_set[0], dict)
                and change_set[0].get("field") == "status"
                and str(change_set[0].get("old")) == RefinementStatus.APPROVED.value
                and str(change_set[0].get("new")) == RefinementStatus.DONE.value
            )
            if status_only_done:
                return previous
        raise SpecLineagePreflightError(
            "spec_refinement_snapshot_required",
            (
                "A Spec derived from a Refinement must pin its immutable "
                "completed snapshot. No compatible Done snapshot was found."
            ),
            facts={
                "refinement_id": refinement.id,
                "refinement_version": live_version,
            },
        )

    async def delete_refinement(
        self,
        refinement_id: str,
        user_id: str,
        *,
        return_receipt: bool = False,
    ) -> bool | GovernedArtifactDeletionReceipt:
        """Delete a refinement and every spec derived from it."""
        refinement = await self.get_refinement(refinement_id)
        if not refinement:
            return False

        board_id = refinement.board_id
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        descendant_deletions: list[GovernedArtifactDeletionReceipt] = []

        specs = await _application_list(
            self.db,
            "spec",
            filters=(_apf("refinement_id", "eq", refinement_id),),
        )
        for spec in specs:
            receipt = await SpecService(self.db).delete_spec(
                spec.id,
                user_id,
                return_receipt=True,
            )
            if not isinstance(receipt, GovernedArtifactDeletionReceipt):
                raise RuntimeError("governed_delete_descendant_receipt_missing")
            descendant_deletions.append(receipt)

        takedown_receipt = replace(
            await _prepare_governed_artifact_deletion(
                self.db,
                board_id=board_id,
                artifact_type="refinement",
                artifact_id=refinement_id,
            ),
            descendant_deletions=tuple(descendant_deletions),
        )
        await _purge_quality_assessment_subject(
            self.db,
            board_id=board_id,
            subject_type="refinement",
            subject_id=refinement_id,
        )
        await _application_delete(self.db, refinement)

        await self._log_activity(
            board_id=board_id,
            action="refinement_deleted",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"refinement_id": refinement_id},
        )
        return takedown_receipt if return_receipt else True

    async def derive_spec(
        self,
        refinement_id: str,
        user_id: str,
        skip_ownership_check: bool = False,
        mockup_ids: list[str] | None = None,
        kb_ids: list[str] | None = None,
        architecture_design_ids: list[str] | None = None,
        architecture_propagation_mode: str = "copy",
        query_scope: QueryScope | None = None,
        *,
        target_id: str | None = None,
        knowledge_propagation_v2: bool = False,
    ) -> Spec | None:
        """Create a Spec draft linked to a refinement.

        Artifacts (mockups, KBs) are automatically propagated. Use mockup_ids/kb_ids
        to select specific ones. Compiles context from the refinement's scope, analysis, decisions,
        technical_requirements, acceptance_criteria) are left empty — they must be
        filled by the agent or human through deliberate analysis.

        Only allowed when refinement status is 'done'.
        """
        if (target_id is None) != (not knowledge_propagation_v2):
            raise ValueError(
                "knowledge_propagation_v2 requires an explicit deterministic target_id"
            )
        refinement = await self.get_refinement(refinement_id)
        if not refinement:
            return None

        spec_service = SpecService(self.db)
        await spec_service._validate_lineage(
            refinement.board_id,
            ideation_id=refinement.ideation_id,
            refinement_id=refinement.id,
        )
        source_snapshot = await self.resolve_completed_snapshot(refinement)
        source_snapshot_id = source_snapshot.id
        source_snapshot_version = source_snapshot.version

        # Compile rich context from refinement data plus the parent ideation
        # intent. Existing refinements created before parent context was
        # appended to description still carry the original idea into specs.
        context_parts: list[str] = []
        if source_snapshot.description:
            context_parts.append(
                f"## Refinement Description\n{source_snapshot.description}"
            )
        if source_snapshot.in_scope:
            scope_text = "\n".join(f"- {s}" for s in source_snapshot.in_scope)
            context_parts.append(f"## In Scope\n{scope_text}")
        if source_snapshot.out_of_scope:
            out_text = "\n".join(f"- {s}" for s in source_snapshot.out_of_scope)
            context_parts.append(f"## Out of Scope\n{out_text}")
        if source_snapshot.analysis:
            context_parts.append(f"## Analysis\n{source_snapshot.analysis}")
        if source_snapshot.decisions:
            decisions_text = "\n".join(f"- {d}" for d in source_snapshot.decisions)
            context_parts.append(f"## Decisions\n{decisions_text}")
        parent_context = compile_ideation_parent_context(
            getattr(refinement, "ideation", None)
        )
        if parent_context and not (
            source_snapshot.description
            and "## Parent Ideation Context" in source_snapshot.description
        ):
            context_parts.append(parent_context)
        context = (
            "\n\n".join(context_parts) if context_parts else source_snapshot.description
        )

        # Snapshot artifact data BEFORE create_spec — flush() in create_spec
        # expires all session objects, making eagerly-loaded collections inaccessible.
        snapshot_qa = list(source_snapshot.qa_snapshot or [])
        snapshot_mockups = list(refinement.screen_mockups or [])
        snapshot_kbs = [
            {
                "title": kb.title,
                "description": kb.description,
                "content": kb.content,
                "mime_type": getattr(kb, "mime_type", "text/markdown"),
                "id": kb.id,
                "source_type": getattr(kb, "source_type", None),
                "source_id": getattr(kb, "source_id", None),
                "source_title": getattr(kb, "source_title", None),
                "source_version": getattr(kb, "source_version", None),
                "source_kb_id": getattr(kb, "source_kb_id", None),
                "root_source_kb_id": getattr(kb, "root_source_kb_id", None),
                "immediate_parent_kb_id": getattr(kb, "immediate_parent_kb_id", None),
                "governance_metadata": getattr(kb, "governance_metadata", None),
            }
            for kb in (refinement.knowledge_bases or [])
        ]

        validate_artifact_selections(
            source_mockups=snapshot_mockups,
            source_knowledge_bases=([] if knowledge_propagation_v2 else snapshot_kbs),
            mockup_ids=mockup_ids,
            kb_ids=(None if knowledge_propagation_v2 else kb_ids),
            source_type="refinement",
            source_id=refinement_id,
        )
        await preflight_architecture_designs(
            self.db,
            source_parent_type="refinement",
            source_parent_id=refinement_id,
            mode=architecture_propagation_mode,
            design_ids=architecture_design_ids,
        )

        spec_data = SpecCreate(
            title=source_snapshot.title,
            description=source_snapshot.description,
            context=context,
            ideation_id=refinement.ideation_id,
            refinement_id=refinement_id,
            labels=source_snapshot.labels,
        )
        spec = await spec_service.create_spec(
            refinement.board_id,
            user_id,
            spec_data,
            skip_ownership_check=skip_ownership_check,
            query_scope=query_scope,
            target_id=target_id,
            knowledge_propagation_v2=knowledge_propagation_v2,
        )
        if spec:
            spec.source_refinement_snapshot_id = source_snapshot_id
            spec.source_refinement_version = source_snapshot_version
            # Propagate artifacts using pre-flush snapshots
            artifact_counts = await propagate_artifacts(
                db=self.db,
                source_mockups=snapshot_mockups,
                source_qa_items=snapshot_qa,
                source_knowledge_bases=(
                    [] if knowledge_propagation_v2 else snapshot_kbs
                ),
                target_entity=spec,
                target_kb_entity="spec_knowledge_base",
                user_id=user_id,
                mockup_ids=mockup_ids,
                kb_ids=None if knowledge_propagation_v2 else kb_ids,
                source_type="refinement",
                source_id=refinement.id,
                source_title=refinement.title,
                source_version=source_snapshot_version,
            )
            architecture_designs = await propagate_architecture_designs(
                self.db,
                source_parent_type="refinement",
                source_parent_id=refinement_id,
                target_parent_type="spec",
                target_parent_id=spec.id,
                actor_id=user_id,
                mode=architecture_propagation_mode,
                design_ids=architecture_design_ids,
            )
            spec.attach(
                "resource_propagation",
                _resource_propagation_summary(
                    source_parent_type="refinement",
                    source_parent_id=refinement_id,
                    target_parent_type="spec",
                    target_parent_id=spec.id,
                    architecture_mode=architecture_propagation_mode,
                    architecture_requested_ids=architecture_design_ids,
                    architecture_designs=architecture_designs,
                    artifact_counts=artifact_counts,
                ),
            )

            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import RefinementDerivedToSpec

            await event_publish(
                RefinementDerivedToSpec(
                    board_id=refinement.board_id,
                    actor_id=user_id,
                    refinement_id=refinement_id,
                    spec_id=spec.id,
                ),
                session=self.db,
            )

            actor_name = await resolve_actor_name(self.db, user_id, refinement.board_id)
            await self._record_history(
                refinement_id=refinement_id,
                action="spec_draft_created",
                actor_id=user_id,
                actor_name=actor_name,
                changes=[{"field": "spec", "old": None, "new": spec.id}],
                summary=f"Spec draft created: {spec.title} (requirements to be defined)",
                version=refinement.version,
            )
        return spec

    async def _log_activity(self, **kwargs: Any) -> None:
        """Log an activity."""
        await _application_add(
            self.db,
            _new_application_record("activity_log", **kwargs),
        )


class RefinementQAService:
    """Service for refinement Q&A operations."""

    def __init__(self, db: Any):
        self.db = db

    async def get_question(self, qa_id: str) -> RefinementQAItem | None:
        """Load a Q&A item so callers can authorize its canonical parent."""
        return await _application_get(self.db, "refinement_qa_item", qa_id)

    async def create_question(
        self, refinement_id: str, user_id: str, data: RefinementQACreate
    ) -> RefinementQAItem | None:
        """Create a question on a refinement (text or choice)."""
        refinement = await _application_get(self.db, "refinement", refinement_id)
        if not refinement:
            return None
        require_draft_mutation(refinement, subject_type="refinement")
        qa = _new_application_record(
            "refinement_qa_item",
            refinement_id=refinement_id,
            question=data.question,
            question_type=data.question_type or "text",
            choices=[c.model_dump() for c in data.choices] if data.choices else None,
            allow_free_text=data.allow_free_text,
            asked_by=user_id,
        )
        await _application_add(self.db, qa)
        await _publish_quality_clarification_changed(
            self.db,
            subject=refinement,
            subject_type="refinement",
            qa_id=getattr(qa, "id", None),
            operation="created",
            actor_id=user_id,
        )
        return qa

    async def answer_question(
        self,
        qa_id: str,
        user_id: str,
        data: RefinementQAAnswer,
        *,
        actor_type: str = "user",
        surface: str = "service",
    ) -> RefinementQAItem | None:
        """Answer a refinement Q&A question (text or choice selection).
        Mirrors IdeationQAService.answer_question — accepts `single_choice`
        as alias of `choice`, and only commits when something was persisted.
        """
        qa = await _application_get(self.db, "refinement_qa_item", qa_id)
        if not qa:
            return None

        refinement = await _application_get(self.db, "refinement", qa.refinement_id)
        if refinement is None:
            raise RuntimeError("quality_clarification_subject_missing")
        require_draft_mutation(refinement, subject_type="refinement")
        board = await _application_get(self.db, "board", refinement.board_id)
        await _authorize_qa_answer_or_raise(
            self.db,
            board=board,
            qa=qa,
            user_id=user_id,
            entity_type="refinement",
            question_id=qa_id,
            actor_type=actor_type,
            surface=surface,
        )

        saved_something = False
        choice_types = ("choice", "single_choice", "multi_choice")
        if qa.question_type in choice_types and data.selected:
            data.selected = validate_choice_selection(
                qa.question_type, data.selected, qa.choices
            )
            qa.selected = data.selected
            saved_something = True

        if data.answer:
            qa.answer = data.answer
            saved_something = True

        if not saved_something:
            return None

        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        await _publish_quality_clarification_changed(
            self.db,
            subject=refinement,
            subject_type="refinement",
            qa_id=qa.id,
            operation="answered",
            actor_id=user_id,
            actor_type=actor_type,
        )
        return qa

    async def list_qa(self, refinement_id: str) -> list[RefinementQAItem]:
        """List all Q&A items for a refinement."""
        return await _application_list(
            self.db,
            "refinement_qa_item",
            filters=(_apf("refinement_id", "eq", refinement_id),),
            order_by=(("created_at", False),),
        )

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await _application_get(self.db, "refinement_qa_item", qa_id)
        if not qa:
            return False
        refinement = await _application_get(
            self.db,
            "refinement",
            qa.refinement_id,
        )
        if refinement is None:
            raise RuntimeError("quality_clarification_subject_missing")
        require_draft_mutation(refinement, subject_type="refinement")
        await _application_delete(self.db, qa)
        await _publish_quality_clarification_changed(
            self.db,
            subject=refinement,
            subject_type="refinement",
            qa_id=qa_id,
            operation="deleted",
            actor_id=None,
        )
        return True


class RefinementKnowledgeService:
    """Service for refinement knowledge base operations."""

    def __init__(self, db: Any):
        self.db = db

    async def create_knowledge(
        self, refinement_id: str, user_id: str, data: RefinementKnowledgeCreate
    ) -> RefinementKnowledgeBase | None:
        """Create a knowledge base item on a refinement."""
        governance_metadata = normalize_knowledge_governance_metadata(
            data.governance_metadata
        )
        refinement = await _application_get(self.db, "refinement", refinement_id)
        if not refinement:
            return None
        require_draft_mutation(refinement, subject_type="refinement")
        kb = _new_knowledge_application_record(
            "refinement_knowledge_base",
            parent_field="refinement_id",
            parent_id=refinement_id,
            parent_version=getattr(refinement, "version", None),
            title=data.title,
            description=data.description,
            content=data.content,
            mime_type=data.mime_type,
            governance_metadata=governance_metadata,
            created_by=user_id,
        )
        await _application_add(self.db, kb)
        return kb

    async def get_knowledge(self, knowledge_id: str) -> RefinementKnowledgeBase | None:
        """Get a knowledge base item by ID."""
        return await _application_get(
            self.db, "refinement_knowledge_base", knowledge_id
        )

    async def list_knowledge(self, refinement_id: str) -> list[RefinementKnowledgeBase]:
        """List all knowledge base items for a refinement."""
        return await _application_list(
            self.db,
            "refinement_knowledge_base",
            filters=(_apf("refinement_id", "eq", refinement_id),),
            order_by=(("created_at", False),),
        )

    async def update_knowledge(
        self,
        knowledge_id: str,
        data: RefinementKnowledgeUpdate,
    ) -> RefinementKnowledgeBase | None:
        """Update a refinement knowledge base item."""
        update_data = data.model_dump(exclude_unset=True)
        if "governance_metadata" in update_data:
            update_data["governance_metadata"] = (
                normalize_knowledge_governance_metadata(
                    update_data["governance_metadata"]
                )
            )
        kb = await self.get_knowledge(knowledge_id)
        if not kb:
            return None
        refinement = await _application_get(self.db, "refinement", kb.refinement_id)
        if refinement is None:
            raise RuntimeError("knowledge_subject_missing")
        require_draft_mutation(refinement, subject_type="refinement")
        for key, value in update_data.items():
            setattr(kb, key, value)
        _refresh_knowledge_content_hash(kb)
        return kb

    async def delete_knowledge(self, knowledge_id: str) -> bool:
        """Delete a knowledge base item."""
        kb = await self.get_knowledge(knowledge_id)
        if not kb:
            return False
        refinement = await _application_get(self.db, "refinement", kb.refinement_id)
        if refinement is None:
            raise RuntimeError("knowledge_subject_missing")
        require_draft_mutation(refinement, subject_type="refinement")
        await _application_delete(self.db, kb)
        return True


class GuidelineService:
    """Service for guideline operations."""

    def __init__(self, db: Any):
        self.db = db

    def policy_persistence(self):
        """Expose the transaction-bound immutable policy authority to use cases.

        Import/export, REST and MCP must share the same persistence contract.
        Keeping this resolver on the Core-owned service catalog lets those
        transport-free use cases obtain the edition adapter without extracting
        or depending on the concrete relational session.
        """

        return self._policy()

    def _policy(self):
        """Resolve the edition-owned immutable guideline authority."""

        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        return require_relational_application_adapter().guideline_policy(self.db)

    def semantic_policy_persistence(self):
        """Expose the transaction-bound semantic evidence authority."""

        return self._semantic_policy()

    def _semantic_policy(self):
        """Resolve the edition-owned append-only semantic evidence authority."""

        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        return require_relational_application_adapter().semantic_guideline_assessments(
            self.db
        )

    async def preview_policy_transition(
        self,
        *,
        board_id: str,
        entity_type: str,
        subject_id: str,
        from_status: str,
        to_status: str,
    ) -> Any | None:
        """Evaluate one frozen Policy Compliance edge without mutating status.

        Lifecycle legality remains owned by the canonical SDLC registry.  Free
        recovery/cancellation/regression edges return before the persistence
        adapter is even resolved, so an unavailable evaluator can never strand
        a subject outside a forward enforcement point.
        """

        normalized_entity_type = str(entity_type).strip().lower()
        normalized_from_status = str(from_status).strip()
        normalized_to_status = str(to_status).strip()
        if not transition_requires_policy_compliance(
            normalized_entity_type,
            normalized_from_status,
            normalized_to_status,
        ):
            return None

        from okto_pulse.core.domain.guideline_policy import PolicyEntityType
        from okto_pulse.core.domain.guideline_policy_transition import (
            evaluate_policy_transition,
        )

        policy = self._policy()
        snapshot = await policy.resolve_transition_snapshot(
            board_id=board_id,
            entity_type=PolicyEntityType(normalized_entity_type),
            subject_id=subject_id,
            expected_from_status=normalized_from_status,
        )
        return evaluate_policy_transition(snapshot, normalized_to_status)

    async def enforce_policy_transition(
        self,
        *,
        board_id: str,
        entity_type: str,
        subject_id: str,
        from_status: str,
        to_status: str,
    ) -> Any | None:
        """Recompute and enforce the canonical gate in the mutation UoW."""

        decision = await self.preview_policy_transition(
            board_id=board_id,
            entity_type=entity_type,
            subject_id=subject_id,
            from_status=from_status,
            to_status=to_status,
        )
        if decision is None:
            return None

        from okto_pulse.core.domain.guideline_policy_transition import (
            raise_for_policy_transition,
        )

        raise_for_policy_transition(decision)
        return decision

    @staticmethod
    def _guideline_projection(
        identity: Any,
        revision: Any,
        *,
        updated_at: datetime,
    ) -> ApplicationRecord:
        """Project the authoritative immutable snapshot on the legacy shape."""

        return _new_application_record(
            "guideline",
            id=identity.guideline_id,
            title=revision.title,
            content=revision.content,
            tags=list(revision.tags),
            scope=identity.scope.value,
            board_id=identity.board_id,
            owner_id=identity.owner_id,
            created_at=identity.created_at,
            updated_at=updated_at,
            version=revision.revision_number,
            semantic_version=revision.semantic_version,
            revision_id=revision.revision_id,
            revision_digest=revision.revision_digest,
            context_scope=identity.context_scope.value,
        )

    @staticmethod
    def _binding_projection(
        binding: Any,
        *,
        template_id: str | None = None,
        template_version: int | None = None,
        guideline_version: int | None = None,
    ) -> ApplicationRecord:
        """Project an append-only binding revision on the legacy link shape."""

        return _new_application_record(
            "board_guideline",
            id=binding.binding_id,
            board_id=binding.board_id,
            guideline_id=binding.guideline_id,
            priority=binding.priority,
            guideline_version=guideline_version,
            template_id=template_id,
            template_version=template_version,
            binding_revision=binding.binding_revision,
            revision_id=binding.revision_id,
            semantic_version=binding.semantic_version,
            revision_digest=binding.revision_digest,
            enforcement=binding.enforcement.value,
            minimum_confidence=binding.minimum_confidence,
            metric_threshold_overrides=dict(binding.metric_threshold_overrides),
            state=binding.state.value,
        )

    async def _authoritative_snapshot(
        self,
        guideline_id: str,
        *,
        include_retired: bool = False,
    ) -> tuple[Any, Any, Any, Any | None] | None:
        """Load identity, head and exact revision, failing closed on drift."""

        policy = self._policy()
        identity = await policy.get_guideline(guideline_id=guideline_id)
        if identity is None:
            return None
        retirement = await policy.get_retirement(guideline_id=guideline_id)
        if retirement is not None and not include_retired:
            return None
        head = await policy.get_head(guideline_id=guideline_id)
        if head is None:
            raise RuntimeError("guideline_authority_head_missing")
        revision = await policy.get_revision(
            guideline_id=guideline_id,
            revision_id=head.revision_id,
        )
        if (
            revision is None
            or revision.revision_number != head.revision_number
            or revision.semantic_version != head.semantic_version
        ):
            raise RuntimeError("guideline_authority_revision_mismatch")
        return identity, head, revision, retirement

    @staticmethod
    def _next_event_time(previous: datetime | None = None) -> datetime:
        """Return a UTC event time strictly after an optional prior event."""

        occurred_at = datetime.now(timezone.utc)
        if previous is not None and occurred_at <= previous:
            occurred_at = previous + timedelta(microseconds=1)
        return occurred_at

    async def _board_visible(
        self,
        board_id: str,
        owner_id: str | None,
        query_scope: QueryScope | None,
    ) -> bool:
        scoped_owner_id = _scope_actor_id(owner_id, query_scope) or owner_id
        query = _board_scope_select(
            board_id=board_id,
            user_id=scoped_owner_id,
            query_scope=query_scope,
            require_ownership=query_scope.require_ownership if query_scope else True,
        )
        if query is None:
            return False
        return bool(await _application_run(self.db, query))

    async def _build_guideline_impact_plan(
        self,
        *,
        board_id: str,
        guideline_id: str,
        impact_receipt_id: str,
        proposed_priority: int,
        proposed_enforcement: Any,
        proposed_minimum_confidence: int,
        proposed_metric_threshold_overrides: Any,
        requested_by: str,
        requested_at: datetime,
        idempotency_key: str,
        to_revision_id: str | None = None,
    ) -> Any:
        """Resolve every server-owned fence used by a B08 impact preview."""

        from okto_pulse.core.domain.guideline_impact import (
            GuidelineImpactPreviewCommand,
            plan_guideline_impact_preview,
        )
        from okto_pulse.core.domain.guideline_policy import (
            GuidelineEnforcement,
            GuidelineScope,
        )

        if not isinstance(
            proposed_enforcement,
            GuidelineEnforcement,
        ):
            raise ValueError("guideline_impact_enforcement_invalid")
        policy = self._policy()
        identity = await policy.get_guideline(guideline_id=guideline_id)
        if (
            identity is None
            or identity.scope is not GuidelineScope.GLOBAL
            or identity.board_id is not None
        ):
            raise ValueError("guideline_impact_global_guideline_required")
        head = await policy.get_head(guideline_id=guideline_id)
        if head is None:
            raise RuntimeError("guideline_authority_head_missing")
        target_id = to_revision_id or head.revision_id
        target = await policy.get_revision(
            guideline_id=guideline_id,
            revision_id=target_id,
        )
        if target is None:
            raise ValueError("guideline_impact_target_revision_not_found")
        current = await policy.get_binding(
            board_id=board_id,
            guideline_id=guideline_id,
        )
        before = (
            await policy.get_revision(
                guideline_id=guideline_id,
                revision_id=current.revision_id,
            )
            if current is not None
            else None
        )
        if current is not None and before is None:
            raise RuntimeError("guideline_binding_revision_mismatch")
        active_bindings = await policy.list_bindings(board_id=board_id)
        active_revisions = []
        for binding in active_bindings:
            revision = await policy.get_revision(
                guideline_id=binding.guideline_id,
                revision_id=binding.revision_id,
            )
            if revision is None:
                raise RuntimeError("guideline_binding_revision_mismatch")
            active_revisions.append(revision)
        semantic_policy = self._semantic_policy()
        semantic_waivers = []
        waiver_cursor = None
        seen_waiver_cursors = set()
        while True:
            (
                waiver_page,
                next_waiver_cursor,
            ) = await semantic_policy.list_board_semantic_waivers(
                board_id=board_id,
                evaluated_at=requested_at,
                guideline_id=guideline_id,
                after=waiver_cursor,
                limit=50,
            )
            semantic_waivers.extend(waiver_page)
            if next_waiver_cursor is None:
                break
            if next_waiver_cursor in seen_waiver_cursors:
                raise RuntimeError("semantic_waiver_cursor_repeated")
            seen_waiver_cursors.add(next_waiver_cursor)
            waiver_cursor = next_waiver_cursor

        return plan_guideline_impact_preview(
            GuidelineImpactPreviewCommand(
                impact_receipt_id=impact_receipt_id,
                board_id=board_id,
                guideline_id=guideline_id,
                head=head,
                to_revision=target,
                current_binding=current,
                from_revision=before,
                active_bindings=active_bindings,
                active_revisions=tuple(active_revisions),
                subjects=await policy.list_policy_subjects(board_id=board_id),
                waivers=tuple(semantic_waivers),
                proposed_priority=proposed_priority,
                proposed_enforcement=proposed_enforcement,
                proposed_minimum_confidence=proposed_minimum_confidence,
                proposed_metric_threshold_overrides=(
                    proposed_metric_threshold_overrides
                ),
                requested_by=requested_by,
                created_at=requested_at,
                idempotency_key=idempotency_key,
                requested_to_revision_id=to_revision_id,
            ),
            retirement=await policy.get_retirement(guideline_id=guideline_id),
        )

    async def preview_guideline_revision_impact(
        self,
        *,
        board_id: str,
        guideline_id: str,
        proposed_priority: int,
        proposed_enforcement: Any,
        proposed_minimum_confidence: int,
        proposed_metric_threshold_overrides: Any,
        requested_by: str,
        idempotency_key: str,
        to_revision_id: str | None = None,
        requested_at: datetime | None = None,
        owner_id: str | None = None,
        query_scope: QueryScope | None = None,
    ) -> Any:
        """Persist an effect-free, sealed and replayable impact receipt."""

        from okto_pulse.core.ports.guideline_policy import (
            GuidelineImpactPreviewReplay,
            GuidelinePolicyIdempotencyConflict,
        )
        from okto_pulse.core.domain.guideline_impact import (
            guideline_impact_preview_request_digest_v1,
        )

        if query_scope is not None and not await self._board_visible(
            board_id,
            owner_id,
            query_scope,
        ):
            raise ValueError("guideline_impact_board_not_visible")
        policy = self._policy()
        replay = await policy.get_impact_receipt_by_idempotency(
            board_id=board_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            expected_request_digest = guideline_impact_preview_request_digest_v1(
                board_id=board_id,
                guideline_id=guideline_id,
                proposed_priority=proposed_priority,
                proposed_enforcement=proposed_enforcement,
                proposed_minimum_confidence=proposed_minimum_confidence,
                proposed_metric_threshold_overrides=(
                    proposed_metric_threshold_overrides
                ),
                requested_by=requested_by,
                requested_to_revision_id=to_revision_id,
            )
            if not isinstance(replay, GuidelineImpactPreviewReplay):
                raise GuidelinePolicyIdempotencyConflict(
                    "guideline_impact_idempotency_evidence_missing"
                )
            if replay.request_digest != expected_request_digest:
                raise GuidelinePolicyIdempotencyConflict(
                    "guideline_impact_idempotency_payload_mismatch"
                )
            return replay.receipt
        impact_receipt_id = str(
            uuid.uuid5(
                uuid.UUID("ba53bf34-b3fe-5e18-a8fb-32eff21d454c"),
                f"{board_id}:{idempotency_key}",
            )
        )
        plan = await self._build_guideline_impact_plan(
            board_id=board_id,
            guideline_id=guideline_id,
            impact_receipt_id=impact_receipt_id,
            proposed_priority=proposed_priority,
            proposed_enforcement=proposed_enforcement,
            proposed_minimum_confidence=proposed_minimum_confidence,
            proposed_metric_threshold_overrides=(proposed_metric_threshold_overrides),
            requested_by=requested_by,
            requested_at=requested_at or self._next_event_time(),
            idempotency_key=idempotency_key,
            to_revision_id=to_revision_id,
        )
        return await policy.save_impact_preview(plan=plan)

    async def adopt_guideline_revision(
        self,
        *,
        board_id: str,
        guideline_id: str,
        impact_receipt_id: str,
        impact_digest: str,
        actor_id: str,
        actor_type: str,
        idempotency_key: str,
        occurred_at: datetime | None = None,
        owner_id: str | None = None,
        query_scope: QueryScope | None = None,
    ) -> tuple[Any, Any]:
        """Consume one current receipt into exactly one audited adoption."""

        from okto_pulse.core.domain.guideline_impact import (
            GuidelineImpactError,
            impact_fence_from_receipt,
            plan_guideline_adoption,
        )
        from okto_pulse.core.ports.guideline_policy import (
            GuidelinePolicyCasConflict,
            GuidelinePolicyDigestConflict,
            GuidelinePolicyIdempotencyConflict,
        )

        normalized_text: dict[str, str] = {}
        for field_name, value in (
            ("board_id", board_id),
            ("guideline_id", guideline_id),
            ("impact_receipt_id", impact_receipt_id),
            ("actor_id", actor_id),
            ("actor_type", actor_type),
            ("idempotency_key", idempotency_key),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"guideline_adoption_{field_name}_required")
            normalized_text[field_name] = value.strip()
        board_id = normalized_text["board_id"]
        guideline_id = normalized_text["guideline_id"]
        impact_receipt_id = normalized_text["impact_receipt_id"]
        actor_id = normalized_text["actor_id"]
        actor_type = normalized_text["actor_type"]
        idempotency_key = normalized_text["idempotency_key"]
        if actor_type not in {"agent", "user", "system"}:
            raise ValueError("guideline_adoption_actor_type_invalid")
        if (
            not isinstance(impact_digest, str)
            or len(impact_digest.strip()) != 64
            or any(
                character not in "0123456789abcdef"
                for character in impact_digest.strip().lower()
            )
        ):
            raise ValueError("guideline_impact_digest_invalid")
        impact_digest = impact_digest.strip().lower()
        if query_scope is not None and not await self._board_visible(
            board_id,
            owner_id,
            query_scope,
        ):
            raise ValueError("guideline_adoption_board_not_visible")
        policy = self._policy()
        replay = await policy.get_adoption_result_by_idempotency(
            board_id=board_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            if (
                replay.receipt.guideline_id != guideline_id
                or replay.receipt.impact_receipt_id != impact_receipt_id
                or replay.receipt.impact_digest != impact_digest
                or replay.binding.adopted_by != actor_id
                or replay.actor_type != actor_type
            ):
                raise GuidelinePolicyIdempotencyConflict(
                    "guideline_adoption_idempotency_payload_mismatch"
                )
            return replay.binding, replay.receipt
        receipt = await policy.get_impact_receipt(
            board_id=board_id,
            impact_receipt_id=impact_receipt_id,
        )
        if receipt is None or receipt.guideline_id != guideline_id:
            raise ValueError("guideline_impact_receipt_not_found")
        if receipt.impact_digest != impact_digest:
            raise GuidelinePolicyDigestConflict("guideline_impact_digest_mismatch")
        event_at = occurred_at or self._next_event_time()
        event_id = str(
            uuid.uuid5(
                uuid.UUID("6a16175a-35b5-545e-aecc-73fecaf970b3"),
                f"{board_id}:{idempotency_key}",
            )
        )
        try:
            current_plan = await self._build_guideline_impact_plan(
                board_id=board_id,
                guideline_id=guideline_id,
                impact_receipt_id=impact_receipt_id,
                proposed_priority=receipt.proposed_priority,
                proposed_enforcement=receipt.proposed_enforcement,
                proposed_minimum_confidence=(receipt.proposed_minimum_confidence),
                proposed_metric_threshold_overrides=(
                    receipt.proposed_metric_threshold_overrides
                ),
                requested_by=receipt.requested_by,
                requested_at=event_at,
                idempotency_key=f"currentness:{idempotency_key}",
                to_revision_id=receipt.to_revision_id,
            )
            current_binding = await policy.get_binding(
                board_id=board_id,
                guideline_id=guideline_id,
            )
            mutation = plan_guideline_adoption(
                receipt=receipt,
                current_snapshot=impact_fence_from_receipt(current_plan.receipt),
                current_binding=current_binding,
                retirement=await policy.get_retirement(guideline_id=guideline_id),
                actor_id=actor_id,
                actor_type=actor_type,
                occurred_at=event_at,
                event_id=event_id,
                idempotency_key=idempotency_key,
            )
        except GuidelineImpactError as exc:
            details = (
                (
                    (
                        "stale_reasons",
                        ",".join(reason.value for reason in exc.currentness_reasons),
                    ),
                )
                if exc.currentness_reasons
                else ()
            )
            raise GuidelinePolicyCasConflict(
                exc.code,
                details=details,
            ) from exc
        return await policy.adopt_revision_cas(mutation=mutation)

    async def create_guideline(
        self,
        owner_id: str,
        data: GuidelineCreate,
        *,
        query_scope: QueryScope | None = None,
        actor_type: str = "user",
    ) -> Guideline:
        """Create identity, ``1.0.0`` revision and head atomically.

        Inline guidelines additionally receive a real exact-revision binding
        in the same caller-owned transaction.  The legacy mutable tables are
        never a second writer after the identity row is established.
        """
        from okto_pulse.core.domain.guideline_lifecycle import (
            GuidelineBindingTransitionCommand,
            GuidelineCreateCommand,
            GuidelineBindingApplied,
            plan_guideline_binding_transition,
            plan_guideline_creation,
        )
        from okto_pulse.core.domain.guideline_policy import (
            GuidelineBindingState,
            GuidelineEnforcement,
            GuidelineScope,
        )

        scoped_owner_id = _scope_actor_id(owner_id, query_scope) or owner_id
        try:
            scope = GuidelineScope(str(data.scope))
        except ValueError as exc:
            raise ValueError("guideline_scope_invalid") from exc
        if scope is GuidelineScope.INLINE and (
            not data.board_id
            or not await self._board_visible(
                data.board_id,
                scoped_owner_id,
                query_scope,
            )
        ):
            raise ValueError("inline_guideline_board_not_visible")

        guideline_id = str(uuid.uuid4())
        revision_id = str(uuid.uuid4())
        occurred_at = self._next_event_time()
        create = plan_guideline_creation(
            GuidelineCreateCommand(
                guideline_id=guideline_id,
                revision_id=revision_id,
                owner_id=scoped_owner_id,
                scope=scope,
                board_id=data.board_id,
                title=data.title,
                content=data.content,
                tags=tuple(data.tags or ()),
                metrics=(),
                created_by=scoped_owner_id,
                created_at=occurred_at,
                idempotency_key=f"legacy:create:{guideline_id}",
            )
        )
        policy = self._policy()
        identity, revision, head = await policy.create_guideline(
            guideline=create.guideline,
            initial_revision=create.revision,
            initial_head=create.head,
            idempotency_key=create.idempotency_key,
            request_digest=create.request_digest,
        )

        if scope is GuidelineScope.INLINE:
            binding_plan = plan_guideline_binding_transition(
                GuidelineBindingTransitionCommand(
                    binding_id=str(uuid.uuid4()),
                    board_id=identity.board_id,
                    guideline_id=identity.guideline_id,
                    state=GuidelineBindingState.ACTIVE,
                    revision_id=revision.revision_id,
                    semantic_version=revision.semantic_version,
                    revision_digest=revision.revision_digest,
                    priority=int(getattr(data, "priority", 0) or 0),
                    enforcement=GuidelineEnforcement.ADVISORY,
                    minimum_confidence=70,
                    metric_threshold_overrides={},
                    actor_id=scoped_owner_id,
                    occurred_at=occurred_at,
                    idempotency_key=(f"legacy:inline-binding:{identity.guideline_id}"),
                    expected_binding_revision=None,
                ),
                current=None,
            )
            if not isinstance(binding_plan, GuidelineBindingApplied):
                raise RuntimeError("inline_guideline_binding_not_applied")
            await policy.append_binding_cas(
                binding=binding_plan.binding,
                expected_binding_revision=None,
                idempotency_key=binding_plan.idempotency_key,
                request_digest=binding_plan.request_digest,
                actor_type=actor_type,
            )
        return self._guideline_projection(
            identity,
            revision,
            updated_at=head.updated_at,
        )

    async def get_guideline(
        self,
        guideline_id: str,
        *,
        owner_id: str | None = None,
        query_scope: QueryScope | None = None,
    ) -> Guideline | None:
        """Get a guideline by ID, optionally constrained to the scoped actor owner.

        Calls with both ``owner_id`` and ``query_scope`` omitted are trusted
        internal lookups only; inbound adapters must pass an explicit scope.
        """
        scoped_owner_id = _scope_actor_id(owner_id, query_scope)
        snapshot = await self._authoritative_snapshot(guideline_id)
        if snapshot is None:
            return None
        identity, head, revision, _retirement = snapshot
        if scoped_owner_id is not None and identity.owner_id != scoped_owner_id:
            return None
        return self._guideline_projection(
            identity,
            revision,
            updated_at=head.updated_at,
        )

    async def list_guidelines(
        self,
        owner_id: str,
        offset: int = 0,
        limit: int = 50,
        tag: str | None = None,
        *,
        query_scope: QueryScope | None = None,
    ) -> list[Guideline]:
        """List authoritative global heads, optionally filtered by current tag."""
        scoped_owner_id = _scope_actor_id(owner_id, query_scope) or owner_id
        identities = await _application_list(
            self.db,
            "guideline",
            filters=(
                _apf("owner_id", "eq", scoped_owner_id),
                _apf("scope", "eq", "global"),
            ),
            order_by=(("created_at", True),),
            limit=None,
        )
        policy = self._policy()
        projected: list[Guideline] = []
        for legacy_identity in identities:
            retirement = await policy.get_retirement(guideline_id=legacy_identity.id)
            if retirement is not None:
                continue
            head = await policy.get_head(guideline_id=legacy_identity.id)
            identity = await policy.get_guideline(guideline_id=legacy_identity.id)
            if head is None or identity is None:
                raise RuntimeError("guideline_authority_head_missing")
            revision = await policy.get_revision(
                guideline_id=identity.guideline_id,
                revision_id=head.revision_id,
            )
            if revision is None:
                raise RuntimeError("guideline_authority_revision_mismatch")
            if tag is not None and tag not in revision.tags:
                continue
            projected.append(
                self._guideline_projection(
                    identity,
                    revision,
                    updated_at=head.updated_at,
                )
            )
        return projected[offset : offset + limit]

    async def update_guideline(
        self,
        guideline_id: str,
        owner_id: str,
        data: GuidelineUpdate,
        *,
        query_scope: QueryScope | None = None,
    ) -> Guideline | None:
        """Append a canonical immutable revision and advance the head by CAS."""
        from okto_pulse.core.domain.guideline_lifecycle import (
            GuidelinePatchApplied,
            GuidelinePatchCommand,
            GuidelinePatchNoop,
            GuidelineRevisionPatch,
            execute_guideline_patch,
        )

        scoped_owner_id = _scope_actor_id(owner_id, query_scope) or owner_id
        snapshot = await self._authoritative_snapshot(guideline_id)
        if snapshot is None:
            return None
        identity, head, current, retirement = snapshot
        if identity.owner_id != scoped_owner_id:
            return None
        patch_id = str(uuid.uuid4())
        result = execute_guideline_patch(
            GuidelinePatchCommand(
                current_revision=current,
                current_head=head,
                patch=GuidelineRevisionPatch(
                    title=data.title,
                    content=data.content,
                    tags=(tuple(data.tags) if data.tags is not None else None),
                ),
                next_revision_id=patch_id,
                actor_id=scoped_owner_id,
                occurred_at=self._next_event_time(head.updated_at),
                idempotency_key=f"legacy:patch:{patch_id}",
            ),
            retirement=retirement,
        )
        if isinstance(result, GuidelinePatchNoop):
            return self._guideline_projection(
                identity,
                current,
                updated_at=head.updated_at,
            )
        if not isinstance(result, GuidelinePatchApplied):
            raise ValueError(result.code)
        revision, next_head = await self._policy().append_revision_cas(
            revision=result.revision,
            next_head=result.head,
            expected_head_revision=result.expected_head_revision,
            idempotency_key=result.idempotency_key,
            request_digest=result.request_digest,
        )
        return self._guideline_projection(
            identity,
            revision,
            updated_at=next_head.updated_at,
        )

    async def delete_guideline(
        self,
        guideline_id: str,
        owner_id: str,
        *,
        actor_type: str = "user",
        query_scope: QueryScope | None = None,
    ) -> bool:
        """Logically retire a guideline while retaining its complete history."""
        from okto_pulse.core.domain.guideline_lifecycle import (
            GuidelineRetirementCommand,
            plan_guideline_retirement,
        )
        from okto_pulse.core.domain.guideline_policy import (
            GuidelineLifecycleStatus,
        )

        scoped_owner_id = _scope_actor_id(owner_id, query_scope) or owner_id
        snapshot = await self._authoritative_snapshot(
            guideline_id,
            include_retired=True,
        )
        if snapshot is None:
            return False
        identity, head, revision, retirement = snapshot
        if identity.owner_id != scoped_owner_id or retirement is not None:
            return False
        retirement_id = str(uuid.uuid4())
        plan = plan_guideline_retirement(
            GuidelineRetirementCommand(
                current_revision=revision,
                current_head=head,
                retirement_id=retirement_id,
                status=GuidelineLifecycleStatus.RETIRED,
                reason="Retired through the legacy guideline façade.",
                actor_id=scoped_owner_id,
                occurred_at=self._next_event_time(head.updated_at),
                idempotency_key=f"legacy:retire:{retirement_id}",
            ),
            current_retirement=None,
        )
        await self._policy().retire_guideline_cas(
            retirement=plan.retirement,
            expected_head_revision=plan.expected_head_revision,
            idempotency_key=plan.idempotency_key,
            request_digest=plan.request_digest,
            actor_type=actor_type,
        )
        return True

    async def get_board_guidelines(
        self,
        board_id: str,
        *,
        surface: str = "service",
        owner_id: str | None = None,
        query_scope: QueryScope | None = None,
    ) -> list[dict]:
        """Project active exact-revision bindings for a board."""
        if query_scope is not None and not await self._board_visible(
            board_id,
            owner_id,
            query_scope,
        ):
            return []
        policy = self._policy()
        bindings = await policy.list_bindings(board_id=board_id)
        items: list[dict] = []
        for binding in bindings:
            identity = await policy.get_guideline(guideline_id=binding.guideline_id)
            retirement = await policy.get_retirement(guideline_id=binding.guideline_id)
            revision = await policy.get_revision(
                guideline_id=binding.guideline_id,
                revision_id=binding.revision_id,
            )
            if identity is None or retirement is not None:
                continue
            if (
                revision is None
                or revision.semantic_version != binding.semantic_version
                or revision.revision_digest != binding.revision_digest
            ):
                raise RuntimeError("guideline_binding_revision_mismatch")
            items.append(
                {
                    "id": identity.guideline_id,
                    "guideline": {
                        "id": identity.guideline_id,
                        "title": revision.title,
                        "content": revision.content,
                        "tags": list(revision.tags),
                        "scope": identity.scope.value,
                        "board_id": identity.board_id,
                        "owner_id": identity.owner_id,
                        "created_at": identity.created_at.isoformat(),
                        "version": revision.revision_number,
                        "semantic_version": revision.semantic_version,
                        "revision_id": revision.revision_id,
                        "revision_digest": revision.revision_digest,
                        "updated_at": revision.created_at.isoformat(),
                    },
                    "priority": binding.priority,
                    "scope": identity.scope.value,
                    "binding_id": binding.binding_id,
                    "binding_revision": binding.binding_revision,
                    "enforcement": binding.enforcement.value,
                    "minimum_confidence": binding.minimum_confidence,
                    "metric_threshold_overrides": dict(
                        binding.metric_threshold_overrides
                    ),
                    "binding_state": binding.state.value,
                    "source_kind": binding.source_kind.value,
                }
            )

        items.sort(key=lambda item: (item["priority"], item["id"]))
        if not items:
            details = build_board_missing_context_warning_details(
                board_id=board_id,
                warning_code="board_rules_missing",
                surface=surface,
            )
            emit_governance_metric(details, raise_on_violation=False)
        return items

    async def link_guideline_to_board(
        self,
        board_id: str,
        guideline_id: str,
        priority: int = 0,
        *,
        owner_id: str | None = None,
        query_scope: QueryScope | None = None,
    ) -> BoardGuideline | None:
        """Fail closed: global links require preview then explicit adoption."""
        from okto_pulse.core.ports.guideline_policy import (
            GuidelinePolicyBindingConflict,
        )

        if query_scope is not None and not await self._board_visible(
            board_id,
            owner_id,
            query_scope,
        ):
            return None
        snapshot = await self._authoritative_snapshot(guideline_id)
        if snapshot is None:
            return None
        del priority
        raise GuidelinePolicyBindingConflict(
            "guideline_impact_preview_required",
            details=(
                ("board_id", board_id),
                ("guideline_id", guideline_id),
                ("remediation", "preview_then_adopt"),
            ),
        )

    async def unlink_guideline_from_board(
        self,
        board_id: str,
        guideline_id: str,
        *,
        actor_type: str = "user",
        idempotency_key: str | None = None,
        owner_id: str | None = None,
        query_scope: QueryScope | None = None,
    ) -> bool:
        """Append one audited UNLINKED revision without deleting history."""
        from okto_pulse.core.domain.guideline_impact import (
            plan_guideline_unlink,
        )
        from okto_pulse.core.domain.guideline_policy import (
            GuidelineBindingState,
        )

        if query_scope is not None and not await self._board_visible(
            board_id,
            owner_id,
            query_scope,
        ):
            return False
        policy = self._policy()
        current = await policy.get_binding(
            board_id=board_id,
            guideline_id=guideline_id,
        )
        if current is None or current.state is not GuidelineBindingState.ACTIVE:
            return False
        retirement = await policy.get_retirement(guideline_id=guideline_id)
        current_revision = await policy.get_revision(
            guideline_id=guideline_id,
            revision_id=current.revision_id,
        )
        if current_revision is None:
            raise RuntimeError("guideline_binding_revision_mismatch")
        active_bindings = await policy.list_bindings(board_id=board_id)
        active_revisions = []
        for active_binding in active_bindings:
            active_revision = await policy.get_revision(
                guideline_id=active_binding.guideline_id,
                revision_id=active_binding.revision_id,
            )
            if active_revision is None:
                raise RuntimeError("guideline_binding_revision_mismatch")
            active_revisions.append(active_revision)
        resolved_key = (
            idempotency_key.strip()
            if isinstance(idempotency_key, str) and idempotency_key.strip()
            else f"legacy:unlink:{uuid.uuid4()}"
        )
        event_id = str(
            uuid.uuid5(
                uuid.UUID("18c3f17c-b1a5-5f2a-bdd4-3c84390fd347"),
                f"{board_id}:{resolved_key}",
            )
        )
        mutation = plan_guideline_unlink(
            current_binding=current,
            current_revision=current_revision,
            active_bindings=active_bindings,
            active_revisions=tuple(active_revisions),
            retirement=retirement,
            actor_id=owner_id or current.adopted_by,
            actor_type=actor_type,
            occurred_at=self._next_event_time(current.adopted_at),
            event_id=event_id,
            idempotency_key=resolved_key,
        )
        await policy.unlink_binding_cas(mutation=mutation)
        return True

    async def update_priority(
        self,
        board_id: str,
        guideline_id: str,
        priority: int,
        *,
        owner_id: str | None = None,
        query_scope: QueryScope | None = None,
    ) -> bool:
        """Fail closed: priority changes alter policy and require a preview."""
        from okto_pulse.core.ports.guideline_policy import (
            GuidelinePolicyBindingConflict,
        )

        if query_scope is not None and not await self._board_visible(
            board_id,
            owner_id,
            query_scope,
        ):
            return False
        del priority
        raise GuidelinePolicyBindingConflict(
            "guideline_impact_preview_required",
            details=(
                ("board_id", board_id),
                ("guideline_id", guideline_id),
                ("remediation", "preview_then_adopt"),
            ),
        )

    async def apply_default_guidelines(
        self,
        board_id: str,
        refs: list,
        *,
        template_id: str,
        template_version: int,
        actor: str = "system",
        owner_id: str | None = None,
        query_scope: QueryScope | None = None,
    ) -> list[BoardGuideline]:
        """Materialize exact immutable pins from a default template.

        The caller owns the transaction. Existing binding lineages are left
        untouched and intra-template duplicates remain first-wins.
        """
        from okto_pulse.core.domain.guideline_lifecycle import (
            GuidelineBindingApplied,
            GuidelineBindingTransitionCommand,
            plan_guideline_binding_transition,
        )
        from okto_pulse.core.domain.guideline_policy import (
            GuidelineBindingProvenance,
            GuidelineBindingState,
            GuidelineEnforcement,
            GuidelineScope,
        )
        from okto_pulse.core.ports.guideline_policy import (
            GuidelineDefaultMaterializationProof,
        )
        from okto_pulse.core.ports.guideline_policy import (
            GuidelineRevisionListQuery,
        )

        if query_scope is not None and not await self._board_visible(
            board_id,
            owner_id,
            query_scope,
        ):
            return []
        policy = self._policy()
        created: list[BoardGuideline] = []
        seen: set[str] = set()
        for ref in refs or []:
            guideline_id = ref["guideline_id"]
            priority = ref.get("priority", 0)
            if type(priority) is not int or priority < 0:
                raise ValueError("default_guideline_priority_invalid")
            if guideline_id in seen:
                continue
            seen.add(guideline_id)
            current = await policy.get_binding(
                board_id=board_id,
                guideline_id=guideline_id,
            )
            if current is not None:
                continue
            identity = await policy.get_guideline(guideline_id=guideline_id)
            retirement = await policy.get_retirement(guideline_id=guideline_id)
            if identity is None:
                raise ValueError("default_guideline_not_found")
            if retirement is not None:
                raise ValueError("default_guideline_retired")
            if (
                identity.scope is not GuidelineScope.GLOBAL
                or identity.board_id is not None
            ):
                raise ValueError("default_guideline_not_global")
            revision_id = ref.get("revision_id")
            requested_number = ref.get("revision_number")
            if requested_number is None and revision_id is None:
                requested_number = ref.get("guideline_version")
            if revision_id:
                revision = await policy.get_revision(
                    guideline_id=guideline_id,
                    revision_id=revision_id,
                )
            else:
                revision = None
                cursor = None
                if requested_number is not None:
                    if type(requested_number) is not int:
                        raise ValueError("default_guideline_revision_invalid")
                    if requested_number < 1:
                        raise ValueError("default_guideline_revision_invalid")
                while requested_number is not None:
                    page = await policy.list_revisions(
                        GuidelineRevisionListQuery(
                            guideline_id=guideline_id,
                            limit=200,
                            cursor=cursor,
                        )
                    )
                    revision = next(
                        (
                            candidate
                            for candidate in page.items
                            if candidate.revision_number == int(requested_number)
                        ),
                        None,
                    )
                    if revision is not None or not page.has_more:
                        break
                    cursor = page.next_cursor
                if revision is None and requested_number is None:
                    head = await policy.get_head(guideline_id=guideline_id)
                    revision = (
                        await policy.get_revision(
                            guideline_id=guideline_id,
                            revision_id=head.revision_id,
                        )
                        if head is not None
                        else None
                    )
            if revision is None:
                raise ValueError("default_guideline_revision_not_found")
            declared_semver = ref.get("semantic_version")
            declared_digest = ref.get("revision_digest")
            declared_number = ref.get("revision_number")
            legacy_number = ref.get("guideline_version")
            legacy_unresolvable = ref.get("legacy_version_unresolvable", False)
            if not isinstance(legacy_unresolvable, bool):
                raise ValueError("default_guideline_revision_invalid")
            if (
                isinstance(declared_number, bool)
                or isinstance(legacy_number, bool)
                or (declared_number is not None and type(declared_number) is not int)
                or (legacy_number is not None and type(legacy_number) is not int)
            ):
                raise ValueError("default_guideline_revision_invalid")
            normalized_declared_number = declared_number
            normalized_legacy_number = legacy_number
            complete_legacy_pin = bool(
                legacy_unresolvable
                and revision_id
                and declared_semver
                and declared_digest
                and legacy_number is not None
                and ref.get("legacy_version") is not None
            )
            legacy_number_exempt = bool(
                complete_legacy_pin and ref.get("legacy_version") == legacy_number
            )
            if (
                (
                    declared_semver is not None
                    and declared_semver != revision.semantic_version
                )
                or (
                    declared_digest is not None
                    and declared_digest != revision.revision_digest
                )
                or (
                    normalized_declared_number is not None
                    and normalized_declared_number != revision.revision_number
                )
                or (
                    normalized_legacy_number is not None
                    and normalized_legacy_number != revision.revision_number
                    and not legacy_number_exempt
                )
                or (legacy_unresolvable and not complete_legacy_pin)
            ):
                raise ValueError("default_guideline_pin_mismatch")
            event_id = str(uuid.uuid4())
            plan = plan_guideline_binding_transition(
                GuidelineBindingTransitionCommand(
                    binding_id=str(uuid.uuid4()),
                    board_id=board_id,
                    guideline_id=guideline_id,
                    state=GuidelineBindingState.ACTIVE,
                    revision_id=revision.revision_id,
                    semantic_version=revision.semantic_version,
                    revision_digest=revision.revision_digest,
                    priority=priority,
                    enforcement=GuidelineEnforcement.ADVISORY,
                    minimum_confidence=70,
                    metric_threshold_overrides={},
                    source_kind=(GuidelineBindingProvenance.DEFAULT_MATERIALIZATION),
                    actor_id=actor,
                    occurred_at=self._next_event_time(),
                    idempotency_key=f"default-binding:{event_id}",
                    expected_binding_revision=None,
                ),
                current=None,
                retirement=retirement,
            )
            if not isinstance(plan, GuidelineBindingApplied):
                raise RuntimeError("default_guideline_binding_not_applied")
            binding = await policy.append_binding_cas(
                binding=plan.binding,
                expected_binding_revision=None,
                idempotency_key=plan.idempotency_key,
                request_digest=plan.request_digest,
                materialization_proof=(
                    GuidelineDefaultMaterializationProof(
                        template_id=template_id,
                        template_version=template_version,
                        guideline_revision_number=(revision.revision_number),
                    )
                ),
                actor_type="system",
            )
            created.append(
                self._binding_projection(
                    binding,
                    template_id=template_id,
                    template_version=template_version,
                    guideline_version=revision.revision_number,
                )
            )
        return created


# ============================================================================
# Archive Service
# ============================================================================


def _tree_cards_structural_preorder(cards: list[Any]) -> list[Any]:
    """Deterministic STRUCTURAL preorder for tree card ops (matriz v13).

    True DFS PREORDER (FR11): roots in canonical lane order — (status,
    position ASC, id DESC) — and each card is IMMEDIATELY followed by its
    whole bug subtree (``origin_task_id`` children in lane order, then their
    own bugs, depth-first). ``A, A1(→A), A2(→A1), B, B1(→B)`` — never the
    breadth-first ``A, B, A1, B1, A2``. Bug-of-bug chains are product-legal
    and traversed. Cycle-safe: an origin loop's residue is emitted in lane
    order via the trailing sweep.
    """
    ordered = sorted(cards, key=lambda item: item.id, reverse=True)
    ordered.sort(
        key=lambda item: (
            getattr(item.status, "value", str(item.status)),
            item.position if isinstance(item.position, int) else 0,
        )
    )
    ids_in_tree = {card.id for card in ordered}
    children: dict[str, list[Any]] = {}
    roots: list[Any] = []
    for card in ordered:
        origin = getattr(card, "origin_task_id", None)
        if origin and origin != card.id and origin in ids_in_tree:
            children.setdefault(origin, []).append(card)
        else:
            roots.append(card)

    result: list[Any] = []
    visited: set[str] = set()

    def _visit(card: Any) -> None:
        stack = [card]
        while stack:
            current = stack.pop()
            if current.id in visited:
                continue
            visited.add(current.id)
            result.append(current)
            # Reversed push keeps lane order across siblings under DFS.
            stack.extend(reversed(children.get(current.id, [])))

    for root in roots:
        _visit(root)
    for card in ordered:  # cycle residue (origin loops): lane order
        if card.id not in visited:
            _visit(card)
    return result


class ArchiveService:
    """Service for archiving and restoring entity trees."""

    def __init__(self, db: Any):
        self.db = db

    async def _resolve_tree(self, entity_type: str, entity_id: str) -> dict[str, list]:
        """Resolve the full descendant tree from a given entity.
        Returns {ideations: [...], refinements: [...], specs: [...], sprints: [...],
        cards: [...]}.
        """
        tree: dict[str, list] = {
            "ideations": [],
            "refinements": [],
            "specs": [],
            "sprints": [],
            "cards": [],
        }

        if entity_type == "ideation":
            ideation = await _application_get(self.db, "ideation", entity_id)
            if not ideation:
                raise ValueError("Ideation not found")
            tree["ideations"].append(ideation)

            # Refinements from this ideation
            refinements = await _application_list(
                self.db,
                "refinement",
                filters=(_apf("ideation_id", "eq", entity_id),),
            )
            tree["refinements"].extend(refinements)

            # Specs from refinements + direct from ideation
            ref_ids = [r.id for r in refinements]
            spec_filters = [_apf("ideation_id", "eq", entity_id)]
            if ref_ids:
                spec_filters.append(_apf("refinement_id", "in", ref_ids))
            specs = await _application_list(
                self.db,
                "spec",
                any_filters=tuple(spec_filters),
            )
            tree["specs"].extend(specs)

        elif entity_type == "refinement":
            refinement = await _application_get(self.db, "refinement", entity_id)
            if not refinement:
                raise ValueError("Refinement not found")
            tree["refinements"].append(refinement)

            specs = await _application_list(
                self.db,
                "spec",
                filters=(_apf("refinement_id", "eq", entity_id),),
            )
            tree["specs"].extend(specs)

        elif entity_type == "spec":
            spec = await _application_get(self.db, "spec", entity_id)
            if not spec:
                raise ValueError("Spec not found")
            tree["specs"].append(spec)

        else:
            raise ValueError(
                f"Invalid entity_type: {entity_type}. Must be ideation, refinement, or spec."
            )

        # Cards from all specs in tree
        spec_ids = [s.id for s in tree["specs"]]
        if spec_ids:
            cards = await _application_list(
                self.db,
                "card",
                filters=(_apf("spec_id", "in", spec_ids),),
            )
            tree["cards"].extend(cards)

            # Bug cards linked to these cards via origin_task_id
            card_ids = [c.id for c in cards]
            if card_ids:
                bugs = await _application_list(
                    self.db,
                    "card",
                    filters=(
                        _apf("origin_task_id", "in", card_ids),
                        _apf("id", "not_in", card_ids),
                    ),
                )
                tree["cards"].extend(bugs)

        # Sprints are first-class descendants of Spec (each sprint belongs to one
        # spec). The sprint's cards are already captured above via spec_id, so only
        # the sprint rows themselves are added here.
        if spec_ids:
            sprints = await _application_list(
                self.db,
                "sprint",
                filters=(_apf("spec_id", "in", spec_ids),),
            )
            tree["sprints"].extend(sprints)

        return tree

    async def archive_tree(self, entity_type: str, entity_id: str) -> dict[str, int]:
        """Archive an entity and all its descendants."""
        tree = await self._resolve_tree(entity_type, entity_id)
        for card in tree["cards"]:
            require_card_operational_mutation_allowed(
                card,
                operation="archive_tree",
            )

        spec_ids = tuple(str(spec.id) for spec in tree["specs"])
        if spec_ids:
            from okto_pulse.core.ports.relational_application import (
                require_relational_application_adapter,
            )
            from okto_pulse.core.services.spec_dependency import SpecDependencyService

            board_ids = sorted({str(spec.board_id) for spec in tree["specs"]})
            for board_id in board_ids:
                board_spec_ids = tuple(
                    str(spec.id)
                    for spec in tree["specs"]
                    if str(spec.board_id) == board_id
                )
                await SpecDependencyService(
                    require_relational_application_adapter().spec_dependencies(self.db),
                    self.db,
                ).require_no_incoming_active(
                    board_id=board_id,
                    target_spec_ids=board_spec_ids,
                    # Edges wholly inside the same atomic archive tree do not
                    # prevent the tree operation; external dependents do.
                    exclude_source_spec_ids=board_spec_ids,
                    operation="archive Spec tree",
                )

        counts = {
            "ideations": 0,
            "refinements": 0,
            "specs": 0,
            "sprints": 0,
            "cards": 0,
        }
        changed_artifacts: list[tuple[str, Any]] = []
        quality_lifecycle_changes: list[tuple[str, Any, int, str, bool]] = []

        for ideation in tree["ideations"]:
            if not ideation.archived:
                before_status = (
                    ideation.status.value
                    if hasattr(ideation.status, "value")
                    else str(ideation.status)
                )
                before_version = int(ideation.version)
                ideation.pre_archive_status = before_status
                ideation.archived = True
                counts["ideations"] += 1
                changed_artifacts.append(("ideation", ideation))
                quality_lifecycle_changes.append(
                    (
                        "ideation",
                        ideation,
                        before_version,
                        before_status,
                        False,
                    )
                )

        for refinement in tree["refinements"]:
            if not refinement.archived:
                before_status = (
                    refinement.status.value
                    if hasattr(refinement.status, "value")
                    else str(refinement.status)
                )
                before_version = int(refinement.version)
                refinement.pre_archive_status = before_status
                refinement.archived = True
                counts["refinements"] += 1
                changed_artifacts.append(("refinement", refinement))
                quality_lifecycle_changes.append(
                    (
                        "refinement",
                        refinement,
                        before_version,
                        before_status,
                        False,
                    )
                )

        for spec in tree["specs"]:
            if not spec.archived:
                before_status = (
                    spec.status.value
                    if hasattr(spec.status, "value")
                    else str(spec.status)
                )
                before_version = int(spec.version)
                spec.pre_archive_status = before_status
                spec.archived = True
                counts["specs"] += 1
                changed_artifacts.append(("spec", spec))
                quality_lifecycle_changes.append(
                    (
                        "spec",
                        spec,
                        before_version,
                        before_status,
                        False,
                    )
                )

        for sprint in tree["sprints"]:
            if not sprint.archived:
                sprint.pre_archive_status = (
                    sprint.status.value
                    if hasattr(sprint.status, "value")
                    else str(sprint.status)
                )
                sprint.archived = True
                counts["sprints"] += 1
                changed_artifacts.append(("sprint", sprint))

        # Cards are archived as resequence OPS (matriz v13, item 5): each op
        # flips archived and relocates the card to the archived range n..m
        # preserving batch (tree-preorder) relative order, while the actives
        # stay dense 0..n-1.
        card_ops: dict[str, list[ColumnResequenceOp]] = {}
        card_records: dict[str, dict[str, ApplicationRecord]] = {}
        # _resolve_tree lists without order_by (DB order): apply the
        # deterministic STRUCTURAL preorder — canonical column order
        # (position ASC, id DESC) plus parent-before-bug topology.
        tree_cards = _tree_cards_structural_preorder(tree["cards"])
        for card in tree_cards:
            if not card.archived:
                card.pre_archive_status = (
                    card.status.value
                    if hasattr(card.status, "value")
                    else str(card.status)
                )
                counts["cards"] += 1
                card_ops.setdefault(card.board_id, []).append(
                    ColumnResequenceOp(
                        card_id=card.id,
                        from_status=card.status,
                        to_status=card.status,
                        from_archived=False,
                        to_archived=True,
                    )
                )
                card_records.setdefault(card.board_id, {})[card.id] = card
                changed_artifacts.append(("card", card))

        await _application_flush(self.db)
        card_service = CardService(self.db)
        for affected_board_id, ops in card_ops.items():
            await card_service.resequence_columns(
                affected_board_id,
                ops,
                records=card_records[affected_board_id],
            )
        for (
            artifact_type,
            artifact,
            before_version,
            before_status,
            before_archived,
        ) in quality_lifecycle_changes:
            status = (
                artifact.status.value
                if hasattr(artifact.status, "value")
                else str(artifact.status)
            )
            await _apply_quality_assessment_lifecycle_transition(
                self.db,
                board_id=artifact.board_id,
                subject_type=artifact_type,
                subject_id=artifact.id,
                before_version=before_version,
                before_status=before_status,
                before_archived=before_archived,
                after_version=int(artifact.version),
                after_status=status,
                after_archived=True,
                action="archive",
                actor_id="system:archive-tree",
            )
        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import ArtifactArchiveChanged

        for artifact_type, artifact in changed_artifacts:
            await event_publish(
                ArtifactArchiveChanged(
                    board_id=artifact.board_id,
                    artifact_type=artifact_type,
                    artifact_id=artifact.id,
                    archived=True,
                ),
                session=self.db,
            )
        return counts

    async def restore_tree(self, entity_type: str, entity_id: str) -> dict[str, int]:
        """Restore an archived entity and all its descendants."""
        from okto_pulse.core.domain.enums import (
            CardStatus,
            IdeationStatus,
            RefinementStatus,
            SpecStatus,
            SprintStatus,
        )

        tree = await self._resolve_tree(entity_type, entity_id)
        for card in tree["cards"]:
            require_card_operational_mutation_allowed(
                card,
                operation="restore_tree",
            )

        spec_board_ids = sorted(
            {str(spec.board_id) for spec in tree["specs"] if spec.archived}
        )
        if spec_board_ids:
            from okto_pulse.core.ports.relational_application import (
                require_relational_application_adapter,
            )
            from okto_pulse.core.services.spec_dependency import SpecDependencyService

            dependency_service = SpecDependencyService(
                require_relational_application_adapter().spec_dependencies(self.db),
                self.db,
            )
            for board_id in spec_board_ids:
                await dependency_service.acquire_lifecycle_write_fence(
                    board_id=board_id
                )

        counts = {
            "ideations": 0,
            "refinements": 0,
            "specs": 0,
            "sprints": 0,
            "cards": 0,
        }
        changed_artifacts: list[tuple[str, Any]] = []
        quality_lifecycle_changes: list[tuple[str, Any, int, str, bool]] = []

        for ideation in tree["ideations"]:
            if ideation.archived:
                before_status = (
                    ideation.status.value
                    if hasattr(ideation.status, "value")
                    else str(ideation.status)
                )
                before_version = int(ideation.version)
                if ideation.pre_archive_status:
                    try:
                        ideation.status = IdeationStatus(ideation.pre_archive_status)
                    except (ValueError, KeyError):
                        pass
                ideation.archived = False
                ideation.pre_archive_status = None
                counts["ideations"] += 1
                changed_artifacts.append(("ideation", ideation))
                quality_lifecycle_changes.append(
                    (
                        "ideation",
                        ideation,
                        before_version,
                        before_status,
                        True,
                    )
                )

        for refinement in tree["refinements"]:
            if refinement.archived:
                before_status = (
                    refinement.status.value
                    if hasattr(refinement.status, "value")
                    else str(refinement.status)
                )
                before_version = int(refinement.version)
                if refinement.pre_archive_status:
                    try:
                        refinement.status = RefinementStatus(
                            refinement.pre_archive_status
                        )
                    except (ValueError, KeyError):
                        pass
                refinement.archived = False
                refinement.pre_archive_status = None
                counts["refinements"] += 1
                changed_artifacts.append(("refinement", refinement))
                quality_lifecycle_changes.append(
                    (
                        "refinement",
                        refinement,
                        before_version,
                        before_status,
                        True,
                    )
                )

        for spec in tree["specs"]:
            if spec.archived:
                before_status = (
                    spec.status.value
                    if hasattr(spec.status, "value")
                    else str(spec.status)
                )
                before_version = int(spec.version)
                if spec.pre_archive_status:
                    try:
                        spec.status = SpecStatus(spec.pre_archive_status)
                    except (ValueError, KeyError):
                        pass
                spec.archived = False
                spec.pre_archive_status = None
                counts["specs"] += 1
                changed_artifacts.append(("spec", spec))
                quality_lifecycle_changes.append(
                    (
                        "spec",
                        spec,
                        before_version,
                        before_status,
                        True,
                    )
                )

        for sprint in tree["sprints"]:
            if sprint.archived:
                if sprint.pre_archive_status:
                    try:
                        sprint.status = SprintStatus(sprint.pre_archive_status)
                    except (ValueError, KeyError):
                        pass
                sprint.archived = False
                sprint.pre_archive_status = None
                counts["sprints"] += 1
                changed_artifacts.append(("sprint", sprint))

        # Cards are restored as resequence OPS with placement="end" (matriz
        # v13, item 5): the landing at the END of the active range is EXPLICIT
        # — never inferred from the stored position, which legacy data may
        # hold as -1 or any other corrupt value (ts_b2e972e7).
        card_ops: dict[str, list[ColumnResequenceOp]] = {}
        card_records: dict[str, dict[str, ApplicationRecord]] = {}
        # Deterministic STRUCTURAL preorder (see archive_tree): canonical
        # column order plus parent-before-bug topology — restored cards land
        # at the end of the active range in this stable order.
        tree_cards = _tree_cards_structural_preorder(tree["cards"])
        for card in tree_cards:
            if card.archived:
                stored_status = card.status
                restored_status = stored_status
                if card.pre_archive_status:
                    try:
                        restored_status = CardStatus(card.pre_archive_status)
                    except (ValueError, KeyError):
                        restored_status = stored_status
                card.pre_archive_status = None
                counts["cards"] += 1
                card_ops.setdefault(card.board_id, []).append(
                    ColumnResequenceOp(
                        card_id=card.id,
                        from_status=stored_status,
                        to_status=restored_status,
                        from_archived=True,
                        to_archived=False,
                        placement="end",
                    )
                )
                card_records.setdefault(card.board_id, {})[card.id] = card
                changed_artifacts.append(("card", card))

        await _application_flush(self.db)
        card_service = CardService(self.db)
        for affected_board_id, ops in card_ops.items():
            await card_service.resequence_columns(
                affected_board_id,
                ops,
                records=card_records[affected_board_id],
            )
        for (
            artifact_type,
            artifact,
            before_version,
            before_status,
            before_archived,
        ) in quality_lifecycle_changes:
            status = (
                artifact.status.value
                if hasattr(artifact.status, "value")
                else str(artifact.status)
            )
            await _apply_quality_assessment_lifecycle_transition(
                self.db,
                board_id=artifact.board_id,
                subject_type=artifact_type,
                subject_id=artifact.id,
                before_version=before_version,
                before_status=before_status,
                before_archived=before_archived,
                after_version=int(artifact.version),
                after_status=status,
                after_archived=False,
                action="restore",
                actor_id="system:restore-tree",
            )
        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import ArtifactArchiveChanged

        for artifact_type, artifact in changed_artifacts:
            await event_publish(
                ArtifactArchiveChanged(
                    board_id=artifact.board_id,
                    artifact_type=artifact_type,
                    artifact_id=artifact.id,
                    archived=False,
                ),
                session=self.db,
            )
        return counts


# ============================================================================
# SPRINT SERVICE
# ============================================================================


class SprintOperationError(ValueError):
    """Typed sprint workflow error for API/MCP callers."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        remediation: str | None = None,
        facts: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.remediation = remediation
        self.facts = facts or {}

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.remediation:
            payload["remediation"] = self.remediation
        if self.facts:
            payload["facts"] = self.facts
        return payload


class SprintService:
    """Service for sprint operations."""

    def __init__(self, db: Any):
        self.db = db

    _SPRINT_TRANSITIONS = transition_map("sprint")

    async def _record_history(
        self,
        sprint_id: str,
        action: str,
        actor_id: str,
        actor_name: str,
        actor_type: str = "user",
        changes: list[dict] | None = None,
        summary: str | None = None,
        version: int | None = None,
    ) -> None:
        entry = _new_application_record(
            "sprint_history",
            sprint_id=sprint_id,
            action=action,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            changes=changes,
            summary=summary,
            version=version,
        )
        await _application_add(self.db, entry)

    async def _log_activity(self, **kwargs: Any) -> None:
        await _application_add(
            self.db,
            _new_application_record("activity_log", **kwargs),
        )

    @staticmethod
    def _attach_derived_fields(sprint: ApplicationRecord) -> ApplicationRecord:
        sprint.attach(
            "normal_sprint_created",
            sprint.lane_type == SprintLaneType.NORMAL,
        )
        return sprint

    @staticmethod
    def _lane_activity_details(sprint: Sprint) -> dict[str, Any]:
        lane_type = (
            sprint.lane_type.value
            if getattr(sprint.lane_type, "value", None)
            else str(sprint.lane_type or SprintLaneType.NORMAL.value)
        )
        return {
            "lane_type": lane_type,
            "origin_sprint_id": sprint.origin_sprint_id,
            "origin_bug_id": sprint.origin_bug_id,
            "normal_sprint_created": lane_type == SprintLaneType.NORMAL.value,
        }

    async def _validate_sprint_lane_lineage(
        self,
        *,
        board_id: str,
        spec: Spec,
        lane_type: SprintLaneType,
        origin_sprint_id: str | None,
        origin_bug_id: str | None,
        current_sprint_id: str | None = None,
    ) -> tuple[Sprint | None, Card | None]:
        """Validate the complete resulting lane state before any write.

        Normal lanes cannot carry hotfix lineage. Hotfix lanes require a valid
        same-board/spec bug; an origin sprint is optional but, when supplied, must
        resolve inside the same board/spec and cannot point at the sprint being
        updated. The eligibility rule remains a done spec OR a closed origin sprint.
        """
        if lane_type == SprintLaneType.NORMAL:
            if origin_sprint_id is not None or origin_bug_id is not None:
                raise SprintOperationError(
                    "normal_lane_lineage_forbidden",
                    "Normal lanes cannot declare hotfix origin lineage.",
                    remediation="remove_origin_lineage_or_use_hotfix_lane",
                    facts={
                        "board_id": board_id,
                        "spec_id": spec.id,
                        "origin_sprint_id": origin_sprint_id,
                        "origin_bug_id": origin_bug_id,
                    },
                )
            return None, None
        if lane_type != SprintLaneType.HOTFIX:
            raise SprintOperationError(
                "invalid_lane_type",
                "Sprint lane_type must be normal or hotfix.",
                remediation="use_supported_sprint_lane_type",
                facts={
                    "board_id": board_id,
                    "spec_id": spec.id,
                    "lane_type": getattr(lane_type, "value", lane_type),
                    "accepted_values": [lane.value for lane in SprintLaneType],
                },
            )

        origin_sprint: Sprint | None = None
        if origin_sprint_id is not None:
            origin_sprint = await _application_get(self.db, "sprint", origin_sprint_id)
            if (
                not origin_sprint
                or origin_sprint.board_id != board_id
                or origin_sprint.spec_id != spec.id
                or origin_sprint.id == current_sprint_id
            ):
                raise SprintOperationError(
                    "origin_sprint_not_found",
                    "origin_sprint_id does not reference a sprint in this board/spec.",
                    remediation="provide_same_spec_origin_sprint",
                    facts={
                        "origin_sprint_id": origin_sprint_id,
                        "spec_id": spec.id,
                        "board_id": board_id,
                    },
                )

        origin_bug: Card | None = None
        if origin_bug_id:
            origin_bug = await _application_get(self.db, "card", origin_bug_id)
            if (
                not origin_bug
                or origin_bug.board_id != board_id
                or origin_bug.spec_id != spec.id
                or origin_bug.card_type != CardType.BUG
            ):
                raise SprintOperationError(
                    "origin_bug_not_found",
                    "origin_bug_id does not reference a bug in this board/spec.",
                    remediation="provide_same_spec_bug_card",
                    facts={
                        "origin_bug_id": origin_bug_id,
                        "spec_id": spec.id,
                        "board_id": board_id,
                    },
                )

        spec_done = spec.status == SpecStatus.DONE
        origin_sprint_closed = bool(
            origin_sprint and origin_sprint.status == SprintStatus.CLOSED
        )
        if not spec_done and not origin_sprint_closed:
            raise SprintOperationError(
                "hotfix_lane_not_eligible",
                "Hotfix lane requires a done spec or a closed same-spec origin sprint.",
                remediation="assign_hotfix_lane_after_done_spec_or_closed_origin_sprint",
                facts={
                    "spec_id": spec.id,
                    "spec_status": spec.status.value,
                    "origin_sprint_id": origin_sprint_id,
                    "origin_sprint_status": (
                        origin_sprint.status.value if origin_sprint else None
                    ),
                },
            )

        if origin_bug is None:
            raise SprintOperationError(
                "hotfix_lineage_required",
                "Hotfix lanes require an explicit same-spec origin_bug_id.",
                remediation="create_or_select_same_spec_bug_and_retry",
                facts={
                    "spec_id": spec.id,
                    "board_id": board_id,
                    "origin_sprint_id": origin_sprint_id,
                    "required_lineage": ["origin_bug_id"],
                },
            )

        return origin_sprint, origin_bug

    async def _validate_hotfix_lane_create(
        self,
        board_id: str,
        spec: Spec,
        data: SprintCreate,
    ) -> tuple[Sprint | None, Card | None]:
        """Compatibility wrapper around the canonical resulting-state validator."""
        return await self._validate_sprint_lane_lineage(
            board_id=board_id,
            spec=spec,
            lane_type=data.lane_type,
            origin_sprint_id=data.origin_sprint_id,
            origin_bug_id=data.origin_bug_id,
        )

    async def _is_confirmed_path_b_hotfix_test(
        self,
        *,
        sprint: Sprint,
        card: Card,
    ) -> bool:
        """Return whether a cross-spec test card is safe in this hotfix lane.

        Path B intentionally permits a regression test task on a revision spec
        to provide evidence for a bug on the original spec.  Path C then asks
        callers to put that bug *and its regression test card* in a hotfix lane
        on the original spec.  The normal same-spec sprint invariant therefore
        needs one narrow exception.  It is granted only when the persisted,
        validator-owned coverage attestation binds all of these identities:

        * this hotfix lane's origin bug;
        * this exact regression test task and one of its scenarios;
        * a non-blocking, complete amendment for the original spec; and
        * the test task's revision spec.

        Mere linkage on the bug, an unconfirmed amendment, or a same-board
        cross-spec card is insufficient.  Unknown/malformed state fails closed.
        """
        if (
            sprint.lane_type != SprintLaneType.HOTFIX
            or card.card_type != CardType.TEST
            or not sprint.origin_bug_id
            or not sprint.spec_id
            or card.board_id != sprint.board_id
            or card.spec_id == sprint.spec_id
            or not card.spec_id
        ):
            return False

        origin_bug = await _application_get(self.db, "card", sprint.origin_bug_id)
        if (
            origin_bug is None
            or origin_bug.board_id != sprint.board_id
            or origin_bug.spec_id != sprint.spec_id
            or origin_bug.card_type != CardType.BUG
            or card.id not in set(origin_bug.linked_test_task_ids or [])
        ):
            return False

        amendments = await AmendmentRevisionService(self.db).list_for_bug(
            board_id=sprint.board_id,
            original_spec_id=sprint.spec_id,
            origin_bug_id=origin_bug.id,
        )
        card_scenarios = {str(value) for value in (card.test_scenario_ids or [])}
        for amendment in amendments:
            fact = AmendmentLineageFact.from_row(amendment)
            eligibility = evaluate_amendment_eligibility(
                fact.status,
                fact.lineage_state,
            )
            confirmation = fact.coverage_confirmation
            if (
                not eligibility.lineage_eligible
                or str(getattr(amendment, "revision_spec_id", "") or "")
                != str(card.spec_id)
                or card.id not in set(fact.regression_test_task_ids)
                or confirmation is None
                or confirmation.amendment_revision_id != fact.amendment_revision_id
                or confirmation.regression_test_task_id != card.id
                or confirmation.regression_scenario_id not in card_scenarios
                or confirmation.regression_scenario_id
                not in set(fact.regression_scenario_ids)
                or not confirmation.validator_id
                or not confirmation.evidence_ref
            ):
                continue
            return True
        return False

    async def create_sprint(
        self,
        board_id: str,
        user_id: str,
        data: SprintCreate,
        skip_ownership_check: bool = False,
        *,
        query_scope: QueryScope | None = None,
    ) -> Sprint | None:
        """Create a new sprint for a spec."""
        spec = await _application_get(self.db, "spec", data.spec_id)
        if not spec or spec.board_id != board_id:
            return None
        if not skip_ownership_check:
            board_query = _board_scope_select(
                board_id=board_id,
                user_id=user_id,
                query_scope=query_scope,
                require_ownership=(
                    query_scope.require_ownership if query_scope is not None else True
                ),
            )
            if board_query is None:
                return None
            if not await _application_run(self.db, board_query):
                return None

        await self._validate_hotfix_lane_create(board_id, spec, data)

        # Validate scoped IDs exist in spec
        if data.test_scenario_ids:
            spec_ts_ids = {s.get("id") for s in (spec.test_scenarios or [])}
            invalid = set(data.test_scenario_ids) - spec_ts_ids
            if invalid:
                raise ValueError(f"Test scenario IDs not found in spec: {invalid}")
        if data.business_rule_ids:
            spec_br_ids = {r.get("id") for r in (spec.business_rules or [])}
            invalid = set(data.business_rule_ids) - spec_br_ids
            if invalid:
                raise ValueError(f"Business rule IDs not found in spec: {invalid}")

        sprint = _new_application_record(
            "sprint",
            board_id=board_id,
            spec_id=data.spec_id,
            title=data.title,
            description=data.description,
            objective=data.objective,
            expected_outcome=data.expected_outcome,
            spec_version=spec.version,
            lane_type=data.lane_type or SprintLaneType.NORMAL,
            origin_sprint_id=data.origin_sprint_id,
            origin_bug_id=data.origin_bug_id,
            test_scenario_ids=data.test_scenario_ids,
            business_rule_ids=data.business_rule_ids,
            start_date=data.start_date,
            end_date=data.end_date,
            labels=data.labels,
            created_by=user_id,
        )
        await _application_add(self.db, sprint)
        self._attach_derived_fields(sprint)

        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import SprintCreated as SprintCreatedEvent

        await event_publish(
            SprintCreatedEvent(
                board_id=board_id,
                actor_id=user_id,
                sprint_id=sprint.id,
                spec_id=data.spec_id,
            ),
            session=self.db,
        )

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="sprint_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "title": data.title,
                "sprint_id": sprint.id,
                "spec_id": data.spec_id,
                "lane_type": sprint.lane_type.value if sprint.lane_type else "normal",
                "origin_sprint_id": sprint.origin_sprint_id,
                "origin_bug_id": sprint.origin_bug_id,
                "normal_sprint_created": sprint.normal_sprint_created,
            },
        )
        await self._record_history(
            sprint_id=sprint.id,
            action="created",
            actor_id=user_id,
            actor_name=actor_name,
            changes=[
                {
                    "field": "lane",
                    **self._lane_activity_details(sprint),
                }
            ],
            summary=(
                f"Hotfix lane created: {data.title}"
                if sprint.lane_type == SprintLaneType.HOTFIX
                else f"Sprint created: {data.title}"
            ),
            version=1,
        )
        return sprint

    async def get_sprint(self, sprint_id: str) -> Sprint | None:
        """Get a sprint by ID with cards, Q&A, and history."""
        sprint = await _application_get(
            self.db,
            "sprint",
            sprint_id,
            includes=("cards", "qa_items", "history"),
        )
        return self._attach_derived_fields(sprint) if sprint else None

    async def list_sprints(
        self,
        spec_id: str,
        include_archived: bool = False,
    ) -> list[Sprint]:
        """List sprints for a spec."""
        filters = [_apf("spec_id", "eq", spec_id)]
        if not include_archived:
            filters.append(_apf("archived", "is_false"))
        rows = await _application_list(
            self.db,
            "sprint",
            filters=tuple(filters),
            order_by=(("created_at", False),),
        )
        for sprint in rows:
            self._attach_derived_fields(sprint)
        await _attach_open_qa_counts(self.db, rows, "sprint_qa_item", "sprint_id")
        return rows

    async def list_board_sprints(
        self,
        board_id: str,
        status_filter: str | None = None,
        spec_id: str | None = None,
        include_archived: bool = False,
    ) -> list[Sprint]:
        """List all sprints for a board, optionally filtered by status and/or spec."""
        filters = [_apf("board_id", "eq", board_id)]
        if status_filter:
            filters.append(_apf("status", "eq", SprintStatus(status_filter)))
        if spec_id:
            filters.append(_apf("spec_id", "eq", spec_id))
        if not include_archived:
            filters.append(_apf("archived", "is_false"))
        rows = await _application_list(
            self.db,
            "sprint",
            filters=tuple(filters),
            order_by=(("updated_at", True),),
            includes=("spec",),
        )
        for sprint in rows:
            self._attach_derived_fields(sprint)
        await _attach_open_qa_counts(self.db, rows, "sprint_qa_item", "sprint_id")
        return rows

    async def list_assigned_cards(self, sprint_id: str) -> list[Card]:
        """Read the canonical assignment set independently of relationship caches."""

        return await _application_list(
            self.db,
            "card",
            filters=(
                _apf("sprint_id", "eq", sprint_id),
                _apf("archived", "is_false"),
            ),
            order_by=(("created_at", False), ("title", False)),
        )

    async def update_sprint(
        self,
        sprint_id: str,
        user_id: str,
        data: SprintUpdate,
    ) -> Sprint | None:
        """Update a sprint. Bumps version on content changes."""
        sprint = await self.get_sprint(sprint_id)
        if not sprint:
            return None
        if sprint.archived:
            raise ValueError("This sprint is archived. Restore it first.")

        update_data = data.model_dump(exclude_unset=True)
        expected_version = update_data.pop("expected_version", None)
        if expected_version is not None and expected_version != sprint.version:
            raise SprintOperationError(
                "sprint_version_conflict",
                "Sprint was changed after it was read.",
                remediation="reload_sprint_and_retry",
                facts={
                    "sprint_id": sprint_id,
                    "expected_version": expected_version,
                    "actual_version": sprint.version,
                },
            )
        if update_data.get("lane_type") == SprintLaneType.NORMAL:
            # Switching out of a hotfix is one atomic resulting-state change. MCP
            # represents omitted optional strings as empty input, so callers should
            # not need a second transport-specific operation to clear stale lineage.
            update_data.setdefault("origin_sprint_id", None)
            update_data.setdefault("origin_bug_id", None)
        update_data = {
            key: value
            for key, value in update_data.items()
            if getattr(sprint, key) != value
        }
        if not update_data:
            return sprint
        lane_fields = {"lane_type", "origin_sprint_id", "origin_bug_id"}
        if lane_fields & update_data.keys() and sprint.status != SprintStatus.DRAFT:
            raise ValueError(
                "Sprint lane metadata can only be updated while the sprint is draft"
            )

        # Always validate the complete resulting lineage, not only when a lane
        # field is present.  Otherwise a legacy-invalid row could be mutated by
        # changing an unrelated field and remain silently corrupt.
        spec = await _application_get(self.db, "spec", sprint.spec_id)
        if not spec or spec.board_id != sprint.board_id:
            raise SprintOperationError(
                "sprint_spec_not_found",
                "Sprint does not reference a spec in its board.",
                remediation="repair_sprint_spec_lineage",
                facts={
                    "sprint_id": sprint.id,
                    "spec_id": sprint.spec_id,
                    "board_id": sprint.board_id,
                },
            )
        await self._validate_sprint_lane_lineage(
            board_id=sprint.board_id,
            spec=spec,
            lane_type=update_data.get("lane_type", sprint.lane_type),
            origin_sprint_id=update_data.get(
                "origin_sprint_id", sprint.origin_sprint_id
            ),
            origin_bug_id=update_data.get("origin_bug_id", sprint.origin_bug_id),
            current_sprint_id=sprint.id,
        )

        old_data = {k: getattr(sprint, k) for k in update_data.keys()}

        # Validate scoped IDs if changed
        if (
            "test_scenario_ids" in update_data
            and update_data["test_scenario_ids"] is not None
        ):
            spec = spec or await _application_get(self.db, "spec", sprint.spec_id)
            if spec:
                spec_ts_ids = {s.get("id") for s in (spec.test_scenarios or [])}
                invalid = set(update_data["test_scenario_ids"]) - spec_ts_ids
                if invalid:
                    raise ValueError(f"Test scenario IDs not found in spec: {invalid}")
        if (
            "business_rule_ids" in update_data
            and update_data["business_rule_ids"] is not None
        ):
            spec = (
                spec
                if "test_scenario_ids" in update_data
                else await _application_get(self.db, "spec", sprint.spec_id)
            )
            if spec:
                spec_br_ids = {r.get("id") for r in (spec.business_rules or [])}
                invalid = set(update_data["business_rule_ids"]) - spec_br_ids
                if invalid:
                    raise ValueError(f"Business rule IDs not found in spec: {invalid}")

        content_fields = {
            "title",
            "description",
            "objective",
            "expected_outcome",
            "test_scenario_ids",
            "business_rule_ids",
            "lane_type",
            "origin_sprint_id",
            "origin_bug_id",
            "start_date",
            "end_date",
            "labels",
            "skip_test_coverage",
            "skip_rules_coverage",
            "skip_qualitative_validation",
            "validation_threshold",
            "require_task_validation",
            "validation_min_confidence",
            "validation_min_completeness",
            "validation_max_drift",
        }
        bumps_version = bool(content_fields & update_data.keys())

        json_fields = {"test_scenario_ids", "business_rule_ids", "labels"}
        for key, value in update_data.items():
            setattr(sprint, key, value)
            if key in json_fields:
                sprint.mark_dirty(key)

        if bumps_version:
            sprint.version += 1
            SprintScopeResolver.invalidate(sprint.id)

        actor_name = await resolve_actor_name(self.db, user_id, sprint.board_id)
        await self._log_activity(
            board_id=sprint.board_id,
            action="sprint_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "sprint_id": sprint_id,
                "version": sprint.version,
                "fields": list(update_data.keys()),
            },
        )
        changes = SpecService._compute_diff(
            old_data, update_data, list(update_data.keys())
        )
        if changes:
            await self._record_history(
                sprint_id=sprint_id,
                action="updated",
                actor_id=user_id,
                actor_name=actor_name,
                changes=changes,
                version=sprint.version,
                summary=f"Updated: {', '.join(c['field'] for c in changes)}",
            )
        await _application_commit(self.db)
        return sprint

    async def move_sprint(
        self,
        sprint_id: str,
        user_id: str,
        data: SprintMove,
        actor_name: str | None = None,
    ) -> Sprint | None:
        """Move a sprint to a different status with gates."""
        sprint = await self.get_sprint(sprint_id)
        if not sprint:
            return None
        if sprint.archived:
            raise ValueError("This sprint is archived. Restore it first.")

        expected_version = getattr(data, "expected_version", None)
        if expected_version is not None and expected_version != sprint.version:
            raise SprintOperationError(
                "sprint_version_conflict",
                "Sprint was changed after it was read.",
                remediation="reload_sprint_and_retry",
                facts={
                    "sprint_id": sprint_id,
                    "expected_version": expected_version,
                    "actual_version": sprint.version,
                },
            )

        allowed = self._SPRINT_TRANSITIONS.get(sprint.status, [])
        if data.status not in allowed:
            allowed_values = [s.value for s in allowed]
            raise ValueError(
                f"Cannot move sprint from '{sprint.status.value}' to '{data.status.value}'. "
                f"Allowed: {allowed_values}"
            )

        spec = await _application_get(self.db, "spec", sprint.spec_id)
        if not spec or spec.board_id != sprint.board_id:
            raise SprintOperationError(
                "sprint_spec_not_found",
                "Sprint does not reference a spec in its board.",
                remediation="repair_sprint_spec_lineage",
                facts={
                    "sprint_id": sprint.id,
                    "spec_id": sprint.spec_id,
                    "board_id": sprint.board_id,
                },
            )
        await self._validate_sprint_lane_lineage(
            board_id=sprint.board_id,
            spec=spec,
            lane_type=sprint.lane_type,
            origin_sprint_id=sprint.origin_sprint_id,
            origin_bug_id=sprint.origin_bug_id,
            current_sprint_id=sprint.id,
        )

        # A closed origin sprint is the alternative eligibility anchor for
        # hotfix lanes whose spec is not done.  Reopening it would make those
        # dependents invalid, so reject before authorization/audit/mutation.
        if sprint.status == SprintStatus.CLOSED and data.status == SprintStatus.DRAFT:
            dependents = await _application_list(
                self.db,
                "sprint",
                filters=(_apf("origin_sprint_id", "eq", sprint.id),),
            )
            blocked_ids: list[str] = []
            for dependent in dependents:
                dependent_spec = await _application_get(
                    self.db, "spec", dependent.spec_id
                )
                if (
                    not dependent_spec
                    or dependent_spec.board_id != dependent.board_id
                    or dependent_spec.status != SpecStatus.DONE
                ):
                    blocked_ids.append(dependent.id)
            if blocked_ids:
                raise SprintOperationError(
                    "origin_sprint_reopen_conflict",
                    "Cannot reopen the sprint: one or more hotfix lanes require "
                    "this closed origin to remain eligible.",
                    remediation="complete_dependent_specs_or_relineage_hotfix_lanes",
                    facts={
                        "origin_sprint_id": sprint.id,
                        "target_status": data.status.value,
                        "dependent_sprint_ids": sorted(blocked_ids)[:20],
                        "dependent_sprint_count": len(blocked_ids),
                    },
                )
        board = (
            await _application_get(self.db, "board", sprint.board_id) if spec else None
        )

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=sprint.board_id,
            actor_id=user_id,
            entity_type="sprint",
            entity_id=sprint.id,
            critical_action=_critical_sprint_move_action(data.status),
            surface="service",
            actor_type="user",
            actor_name=actor_name,
        )

        # Gate: draft → active requires at least 1 card assigned
        if data.status == SprintStatus.ACTIVE:
            assigned_cards = await _application_list(
                self.db,
                "card",
                filters=(
                    _apf("sprint_id", "eq", sprint_id),
                    _apf("archived", "is_false"),
                ),
            )
            if not assigned_cards:
                raise ValueError(
                    "Cannot activate sprint: no cards assigned. "
                    "Assign at least one card to this sprint before activating."
                )
            if sprint.lane_type == SprintLaneType.HOTFIX:
                bug_ids = {
                    card.id for card in assigned_cards if card.card_type == CardType.BUG
                }
                test_ids = {
                    card.id
                    for card in assigned_cards
                    if card.card_type == CardType.TEST
                }
                origin_bug = next(
                    (
                        card
                        for card in assigned_cards
                        if card.id == sprint.origin_bug_id
                        and card.card_type == CardType.BUG
                    ),
                    None,
                )
                linked_regression_ids = (
                    set(getattr(origin_bug, "linked_test_task_ids", None) or [])
                    if origin_bug
                    else set()
                )
                if (
                    not sprint.origin_bug_id
                    or sprint.origin_bug_id not in bug_ids
                    or not test_ids
                    or not linked_regression_ids.intersection(test_ids)
                ):
                    raise SprintOperationError(
                        "hotfix_regression_lineage_required",
                        "Hotfix activation requires its origin bug and an explicitly "
                        "linked regression test card in the lane.",
                        remediation="assign_origin_bug_and_linked_regression_test",
                        facts={
                            "sprint_id": sprint.id,
                            "origin_bug_id": sprint.origin_bug_id,
                            "assigned_bug_ids": sorted(bug_ids),
                            "assigned_test_ids": sorted(test_ids),
                            "origin_bug_linked_test_ids": sorted(linked_regression_ids),
                        },
                    )

        # Gate: active → review requires scoped test coverage check
        if data.status == SprintStatus.REVIEW:
            skip_tc = sprint.skip_test_coverage or (
                (board.settings or {}).get("skip_test_coverage_global", False)
                if board
                else False
            )
            if not skip_tc and spec:
                assigned_cards = await self.list_assigned_cards(sprint_id)
                scope = SprintScopeResolver.resolve(
                    sprint=sprint,
                    spec=spec,
                    cards=assigned_cards,
                )
                scoped = list(scope.items.get("test_scenarios", ()))
                not_covered = [s for s in scoped if s.get("status") != "passed"]
                if not_covered:
                    names = "; ".join(
                        s.get("title", s.get("id", "?"))[:60] for s in not_covered[:5]
                    )
                    raise ValueError(
                        f"Cannot submit sprint for review: {len(not_covered)} scoped test scenario(s) "
                        f"not passed. Pending: {names}"
                        f"{f' (and {len(not_covered) - 5} more)' if len(not_covered) > 5 else ''}."
                    )

        # Gate: review → closed defesa em profundidade do test theater
        # prevention (Wave 2 NC-9, spec 873e98cc). Itera test cards do sprint,
        # checa se scenarios linked com status passed/automated têm evidence
        # persisted. Honra board.settings.skip_test_evidence_global.
        if data.status == SprintStatus.CLOSED and spec is not None:
            open_cards = await _application_list(
                self.db,
                "card",
                filters=(
                    _apf("sprint_id", "eq", sprint_id),
                    _apf("archived", "is_false"),
                    _apf(
                        "status",
                        "not_in",
                        [CardStatus.DONE, CardStatus.CANCELLED],
                    ),
                ),
                order_by=(("created_at", False), ("title", False)),
            )
            if open_cards:
                preview = ", ".join(
                    f"{card.title} ({card.status.value})" for card in open_cards[:5]
                )
                suffix = (
                    f" and {len(open_cards) - 5} more" if len(open_cards) > 5 else ""
                )
                raise SprintOperationError(
                    "sprint_has_incomplete_cards",
                    (
                        f"Cannot close sprint: {len(open_cards)} assigned card(s) "
                        f"are not terminal: {preview}{suffix}. Move each card to "
                        "'done' or 'cancelled', or remove it from the sprint before closing."
                    ),
                    remediation="complete_or_unassign_sprint_cards",
                    facts={
                        "sprint_id": sprint_id,
                        "open_cards": [
                            {
                                "id": card.id,
                                "title": card.title,
                                "status": card.status.value,
                            }
                            for card in open_cards[:20]
                        ],
                        "terminal_statuses": [
                            CardStatus.DONE.value,
                            CardStatus.CANCELLED.value,
                        ],
                    },
                )

            skip_evidence = bool(
                (board.settings or {}).get("skip_test_evidence_global", False)
                if board
                else False
            )
            scope = SprintScopeResolver.resolve(
                sprint=sprint,
                spec=spec,
                cards=sprint.cards,
            )
            scope_blockers = completion_blockers(
                scope,
                skip_test_coverage=bool(
                    sprint.skip_test_coverage
                    or (
                        (board.settings or {}).get("skip_test_coverage_global", False)
                        if board
                        else False
                    )
                ),
                skip_test_evidence=skip_evidence,
                skip_rules_coverage=bool(
                    sprint.skip_rules_coverage
                    or (
                        (board.settings or {}).get("skip_rules_coverage_global", False)
                        if board
                        else False
                    )
                ),
                evidence_validator=lambda scenario: (
                    scenario_has_authenticated_required_evidence(
                        board_id=sprint.board_id,
                        spec_id=spec.id,
                        scenario=scenario,
                        acceptance_criteria=list(spec.acceptance_criteria or []),
                    )
                ),
            )
            if scope_blockers:
                raise SprintOperationError(
                    "sprint_scope_gate_blocked",
                    f"Cannot close sprint: {len(scope_blockers)} scoped gate blocker(s).",
                    remediation="resolve_sprint_scope_blockers",
                    facts={
                        "sprint_id": sprint_id,
                        "sprint_version": sprint.version,
                        "spec_version": spec.version,
                        "blockers": [blocker.to_dict() for blocker in scope_blockers],
                    },
                )
            if skip_evidence:
                # Skip ON — log forensics record so reactivation analytics
                # can flag boards that bypass the gate at sprint close.
                import logging as _logging

                _ev_logger = _logging.getLogger("okto_pulse.spec.test_scenario")
                _ev_logger.info(
                    "sprint.evidence_gate_skipped sprint=%s board=%s",
                    sprint_id,
                    sprint.board_id,
                    extra={
                        "event": "sprint.evidence_gate_skipped",
                        "sprint_id": sprint_id,
                        "board_id": sprint.board_id,
                        "skip": True,
                    },
                )

        # Gate: review → closed requires evaluation
        if data.status == SprintStatus.CLOSED:
            skip_qual = sprint.skip_qualitative_validation
            if not skip_qual:
                evaluations = [
                    e for e in (sprint.evaluations or []) if not e.get("stale")
                ]
                approvals = [
                    e for e in evaluations if e.get("recommendation") == "approve"
                ]
                rejections = [
                    e for e in evaluations if e.get("recommendation") == "reject"
                ]
                if rejections:
                    names = ", ".join(e.get("evaluator_name", "?") for e in rejections)
                    raise SprintOperationError(
                        "sprint_evaluation_rejected",
                        f"Cannot close sprint: {len(rejections)} evaluation(s) with 'reject' "
                        f"recommendation (by: {names}). Remove or replace rejections.",
                        remediation="replace_rejected_sprint_evaluation",
                        facts={"sprint_id": sprint_id, "rejected_by": names},
                    )
                if not approvals:
                    raise SprintOperationError(
                        "sprint_evaluation_required",
                        "Cannot close sprint: no evaluation with 'approve' recommendation. "
                        "Submit an evaluation before closing.",
                        remediation="submit_sprint_evaluation",
                        facts={"sprint_id": sprint_id},
                    )
                threshold = (
                    sprint.validation_threshold
                    or (board.settings or {}).get("validation_threshold_global", 70)
                    if board
                    else 70
                )
                avg_score = sum(e.get("overall_score", 0) for e in approvals) / len(
                    approvals
                )
                if avg_score < threshold:
                    raise SprintOperationError(
                        "sprint_evaluation_below_threshold",
                        f"Cannot close sprint: average approval score ({avg_score:.0f}) "
                        f"is below threshold ({threshold}).",
                        remediation="improve_delivery_and_resubmit_evaluation",
                        facts={
                            "sprint_id": sprint_id,
                            "average_score": avg_score,
                            "threshold": threshold,
                        },
                    )

            await GuidelineService(self.db).enforce_policy_transition(
                board_id=sprint.board_id,
                entity_type="sprint",
                subject_id=sprint.id,
                from_status=sprint.status.value,
                to_status=data.status.value,
            )

        old_status = sprint.status

        # Cancellation justification (ITEM 17): cancel requires a reason
        # (replacing any previous one); reopening clears it.
        apply_cancellation_policy(
            sprint,
            entity_type="sprint",
            from_status=old_status,
            to_status=data.status,
            reason=getattr(data, "cancellation_reason", None),
            actor_id=user_id,
        )

        sprint.status = data.status
        if old_status != data.status:
            sprint.version += 1

        if old_status != data.status:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import (
                SprintClosed as SprintClosedEvent,
            )
            from okto_pulse.core.events.types import (
                SprintMoved as SprintMovedEvent,
            )

            await event_publish(
                SprintMovedEvent(
                    board_id=sprint.board_id,
                    actor_id=user_id,
                    sprint_id=sprint.id,
                    from_status=old_status.value,
                    to_status=data.status.value,
                ),
                session=self.db,
            )
            if data.status == SprintStatus.CLOSED:
                await event_publish(
                    SprintClosedEvent(
                        board_id=sprint.board_id,
                        actor_id=user_id,
                        sprint_id=sprint.id,
                    ),
                    session=self.db,
                )

        resolved_name = actor_name or await resolve_actor_name(
            self.db, user_id, sprint.board_id
        )
        await self._log_activity(
            board_id=sprint.board_id,
            action="sprint_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "sprint_id": sprint_id,
                "spec_id": sprint.spec_id,
                "from_status": old_status.value,
                "to_status": data.status.value,
                "version": sprint.version,
                "cancellation_reason": sprint.cancellation_reason,
                "cancelled_by": sprint.cancelled_by,
                "cancelled_at": (
                    sprint.cancelled_at.isoformat() if sprint.cancelled_at else None
                ),
                **self._lane_activity_details(sprint),
            },
        )
        await self._record_history(
            sprint_id=sprint_id,
            action="status_changed",
            actor_id=user_id,
            actor_name=resolved_name,
            changes=[
                {
                    "field": "status",
                    "old": old_status.value,
                    "new": data.status.value,
                    "cancellation_reason": sprint.cancellation_reason,
                    "cancelled_by": sprint.cancelled_by,
                    "cancelled_at": (
                        sprint.cancelled_at.isoformat() if sprint.cancelled_at else None
                    ),
                    **self._lane_activity_details(sprint),
                }
            ],
            summary=f"Status: {old_status.value} → {data.status.value}",
            version=sprint.version,
        )
        await _application_commit(self.db)
        return sprint

    async def delete_sprint(
        self,
        sprint_id: str,
        user_id: str,
        *,
        return_receipt: bool = False,
    ) -> bool | GovernedArtifactDeletionReceipt:
        """Delete a sprint. Unlinks cards but doesn't delete them."""
        sprint = await self.get_sprint(sprint_id)
        if not sprint:
            return False

        # Resolve origin-sprint dependents explicitly so fresh schemas (where
        # the FK has ON DELETE SET NULL) and upgraded legacy schemas (where the
        # column may have no FK) have identical application behavior.  A hotfix
        # whose spec is not done would lose its only eligibility anchor, so the
        # entire delete fails before any mutation, flush, activity, or history.
        origin_dependents = await _application_list(
            self.db,
            "sprint",
            filters=(_apf("origin_sprint_id", "eq", sprint_id),),
        )
        blocked_ids: list[str] = []
        for dependent in origin_dependents:
            dependent_spec = await _application_get(self.db, "spec", dependent.spec_id)
            if (
                not dependent_spec
                or dependent_spec.board_id != dependent.board_id
                or dependent_spec.status != SpecStatus.DONE
            ):
                blocked_ids.append(dependent.id)

        if blocked_ids:
            raise SprintOperationError(
                "origin_sprint_delete_conflict",
                "Cannot delete the sprint: one or more dependent hotfix lanes "
                "would become ineligible.",
                remediation="complete_dependent_specs_or_relineage_hotfix_lanes",
                facts={
                    "origin_sprint_id": sprint_id,
                    "dependent_sprint_ids": sorted(blocked_ids)[:20],
                    "dependent_sprint_count": len(blocked_ids),
                },
            )

        assigned_cards = await _application_list(
            self.db,
            "card",
            filters=(_apf("sprint_id", "eq", sprint_id),),
        )
        for card in assigned_cards:
            require_card_operational_mutation_allowed(
                card,
                operation="delete_sprint_unassign_card",
            )

        for dependent in origin_dependents:
            dependent.origin_sprint_id = None
        for card in assigned_cards:
            card.sprint_id = None
        await _application_flush(self.db)
        board_id = sprint.board_id
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        takedown_receipt = await _prepare_governed_artifact_deletion(
            self.db,
            board_id=board_id,
            artifact_type="sprint",
            artifact_id=sprint_id,
        )
        await _application_delete(self.db, sprint)
        await self._log_activity(
            board_id=board_id,
            action="sprint_deleted",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"sprint_id": sprint_id},
        )
        await _application_commit(self.db)
        return takedown_receipt if return_receipt else True

    async def assign_tasks(
        self,
        sprint_id: str,
        card_ids: list[str],
        user_id: str,
    ) -> int:
        """Assign cards to a sprint.

        Cards normally belong to the sprint spec.  A hotfix lane additionally
        accepts its origin bug's exact, validator-confirmed Path B regression
        test task from the amendment revision spec.
        """
        sprint = await _application_get(self.db, "sprint", sprint_id)
        if not sprint:
            raise SprintOperationError(
                "sprint_not_found",
                "Sprint not found.",
                remediation="Refresh the sprint list and retry assignment with an existing sprint.",
                facts={"sprint_id": sprint_id},
            )
        cards_to_assign: list[Card] = []
        cross_spec_path_b_test_ids: list[str] = []
        for card_id in dict.fromkeys(card_ids):
            card = await _application_get(self.db, "card", card_id)
            if not card or card.board_id != sprint.board_id:
                raise SprintOperationError(
                    "card_not_found",
                    "Card not found.",
                    remediation=(
                        "Refresh the board and retry assignment with existing card IDs."
                    ),
                    facts={"sprint_id": sprint_id, "card_id": card_id},
                )
            cross_spec_path_b_test = False
            if card.spec_id != sprint.spec_id:
                cross_spec_path_b_test = await self._is_confirmed_path_b_hotfix_test(
                    sprint=sprint,
                    card=card,
                )
            if card.spec_id != sprint.spec_id and not cross_spec_path_b_test:
                raise ValueError(
                    f"Card '{card.title}' belongs to a different spec. "
                    f"Sprint spec: {sprint.spec_id}, card spec: {card.spec_id}"
                )
            if cross_spec_path_b_test:
                cross_spec_path_b_test_ids.append(card.id)
            if sprint.lane_type == SprintLaneType.HOTFIX and card.card_type not in {
                CardType.BUG,
                CardType.TEST,
            }:
                raise SprintOperationError(
                    "hotfix_lane_card_type_forbidden",
                    "Hotfix lanes accept only bug and test cards.",
                    remediation=(
                        "Assign only bug cards and regression test cards to the hotfix lane. "
                        "Use a normal sprint for implementation cards."
                    ),
                    facts={
                        "sprint_id": sprint_id,
                        "lane_type": sprint.lane_type.value,
                        "card_id": card.id,
                        "card_type": card.card_type.value,
                        "allowed_card_types": [CardType.BUG.value, CardType.TEST.value],
                    },
                )
            cards_to_assign.append(card)

        moved_cards = [card for card in cards_to_assign if card.sprint_id != sprint_id]
        for card in moved_cards:
            require_card_operational_mutation_allowed(
                card,
                operation="assign_card_to_sprint",
            )
        source_cards: dict[str, list[Card]] = {}
        for card in moved_cards:
            if card.sprint_id:
                source_cards.setdefault(card.sprint_id, []).append(card)
            card.sprint_id = sprint_id
        assigned = len(moved_cards)
        if assigned:
            actor_name = await resolve_actor_name(self.db, user_id, sprint.board_id)
            affected_sprint_ids = {sprint_id, *source_cards.keys()}
            for source_sprint_id, removed_cards in source_cards.items():
                source_sprint = await _application_get(
                    self.db, "sprint", source_sprint_id
                )
                if source_sprint is None or source_sprint.board_id != sprint.board_id:
                    continue
                source_sprint.version += 1
                removed_ids = [card.id for card in removed_cards]
                await self._log_activity(
                    board_id=source_sprint.board_id,
                    action="sprint_tasks_unassigned",
                    actor_type="user",
                    actor_id=user_id,
                    actor_name=actor_name,
                    details={
                        "sprint_id": source_sprint_id,
                        "card_ids": removed_ids,
                        "count": len(removed_ids),
                        "version": source_sprint.version,
                        "reassigned_to_sprint_id": sprint_id,
                        **self._lane_activity_details(source_sprint),
                    },
                )
                await self._record_history(
                    sprint_id=source_sprint_id,
                    action="tasks_unassigned",
                    actor_id=user_id,
                    actor_name=actor_name,
                    changes=[
                        {
                            "field": "cards",
                            "removed": removed_ids,
                            "reassigned_to_sprint_id": sprint_id,
                        }
                    ],
                    summary=f"Reassigned {len(removed_ids)} card(s) to another sprint",
                    version=source_sprint.version,
                )
            sprint.version += 1
            SprintScopeResolver.invalidate(*affected_sprint_ids)
            await self._log_activity(
                board_id=sprint.board_id,
                action="sprint_tasks_assigned",
                actor_type="user",
                actor_id=user_id,
                actor_name=actor_name,
                details={
                    "sprint_id": sprint_id,
                    "card_ids": [card.id for card in moved_cards],
                    "count": assigned,
                    "cross_spec_path_b_test_ids": cross_spec_path_b_test_ids,
                    **self._lane_activity_details(sprint),
                    "accepted_card_types": (
                        [CardType.BUG.value, CardType.TEST.value]
                        if sprint.lane_type == SprintLaneType.HOTFIX
                        else [
                            CardType.NORMAL.value,
                            CardType.TEST.value,
                            CardType.BUG.value,
                        ]
                    ),
                },
            )
            await self._record_history(
                sprint_id=sprint_id,
                action="tasks_assigned",
                actor_id=user_id,
                actor_name=actor_name,
                changes=[
                    {
                        "field": "cards",
                        "added": [card.id for card in moved_cards],
                        "count": assigned,
                        "cross_spec_path_b_test_ids": cross_spec_path_b_test_ids,
                        **self._lane_activity_details(sprint),
                    }
                ],
                summary=(
                    f"Assigned {assigned} card(s) to hotfix lane"
                    if sprint.lane_type == SprintLaneType.HOTFIX
                    else f"Assigned {assigned} card(s) to sprint"
                ),
                version=sprint.version,
            )
        await _application_commit(self.db)
        return assigned

    async def unassign_tasks(
        self,
        sprint_id: str,
        card_ids: list[str],
        user_id: str,
    ) -> int:
        """Atomically unassign cards and invalidate version-keyed scope caches."""

        sprint = await _application_get(self.db, "sprint", sprint_id)
        if not sprint:
            raise SprintOperationError(
                "sprint_not_found",
                "Sprint not found.",
                remediation="refresh_sprint_list",
                facts={"sprint_id": sprint_id},
            )
        cards: list[Card] = []
        for card_id in dict.fromkeys(card_ids):
            card = await _application_get(self.db, "card", card_id)
            if (
                card is not None
                and card.board_id == sprint.board_id
                and card.sprint_id == sprint_id
            ):
                cards.append(card)
        for card in cards:
            require_card_operational_mutation_allowed(
                card,
                operation="unassign_card_from_sprint",
            )
        for card in cards:
            card.sprint_id = None
        if cards:
            sprint.version += 1
            SprintScopeResolver.invalidate(sprint.id)
            actor_name = await resolve_actor_name(self.db, user_id, sprint.board_id)
            details = {
                "sprint_id": sprint_id,
                "card_ids": [card.id for card in cards],
                "count": len(cards),
                "version": sprint.version,
                **self._lane_activity_details(sprint),
            }
            await self._log_activity(
                board_id=sprint.board_id,
                action="sprint_tasks_unassigned",
                actor_type="user",
                actor_id=user_id,
                actor_name=actor_name,
                details=details,
            )
            await self._record_history(
                sprint_id=sprint_id,
                action="tasks_unassigned",
                actor_id=user_id,
                actor_name=actor_name,
                changes=[{"field": "cards", "removed": details["card_ids"]}],
                summary=f"Unassigned {len(cards)} card(s) from sprint",
                version=sprint.version,
            )
        await _application_commit(self.db)
        return len(cards)

    async def submit_evaluation(
        self,
        sprint_id: str,
        user_id: str,
        evaluation: dict,
    ) -> Sprint | None:
        """Submit a qualitative evaluation for a sprint."""
        sprint = await _application_get(self.db, "sprint", sprint_id)
        if not sprint:
            return None
        if sprint.status != SprintStatus.REVIEW:
            raise ValueError(
                f"Evaluations can only be submitted for sprints in 'review' status "
                f"(current: '{sprint.status.value}')"
            )
        board = await _application_get(self.db, "board", sprint.board_id)
        cards = await _application_list(
            self.db,
            "card",
            filters=(
                _apf("sprint_id", "eq", sprint_id),
                _apf("archived", "is_false"),
            ),
        )
        separation = evaluate_reviewer_separation(
            board=board,
            reviewer_id=user_id,
            sprint=sprint,
            cards=cards,
        )
        if not separation.allowed:
            raise SprintOperationError(
                "reviewer_separation_required",
                "Sprint reviewer must be independent from its creator/executors.",
                remediation="request_independent_sprint_reviewer",
                facts={"sprint_id": sprint_id, **separation.to_dict()},
            )
        evaluator_name = await resolve_actor_name(self.db, user_id, sprint.board_id)
        await _authorize_critical_context_or_raise(
            self.db,
            board_id=sprint.board_id,
            actor_id=user_id,
            entity_type="sprint",
            entity_id=sprint.id,
            critical_action=CriticalAction.SPRINT_SUBMIT_EVALUATION,
            surface="service",
            actor_type="user",
            actor_name=evaluator_name,
        )
        import uuid as _uuid

        eval_entry = {
            **evaluation,
            "id": f"eval_{_uuid.uuid4().hex[:8]}",
            "evaluator_id": user_id,
            "evaluator_name": evaluator_name,
            "evaluator_type": "user",
            "reviewer_separation": separation.to_dict(),
            "stale": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        evals = list(sprint.evaluations or [])
        evals.append(eval_entry)
        sprint.evaluations = evals
        sprint.mark_dirty("evaluations")
        sprint.version += 1

        await self._log_activity(
            board_id=sprint.board_id,
            action="sprint_evaluation_submitted",
            actor_type="user",
            actor_id=user_id,
            actor_name=eval_entry["evaluator_name"],
            details={
                "sprint_id": sprint_id,
                "evaluation_id": eval_entry["id"],
                "score": evaluation.get("overall_score"),
                "reviewer_separation": separation.to_dict(),
                **self._lane_activity_details(sprint),
            },
        )
        await self._record_history(
            sprint_id=sprint_id,
            action="evaluation_submitted",
            actor_id=user_id,
            actor_name=eval_entry["evaluator_name"],
            changes=[
                {
                    "field": "evaluations",
                    "evaluation_id": eval_entry["id"],
                    "recommendation": evaluation.get("recommendation"),
                    "overall_score": evaluation.get("overall_score"),
                    **self._lane_activity_details(sprint),
                }
            ],
            summary=f"Evaluation submitted: {evaluation.get('recommendation')} (score: {evaluation.get('overall_score')})",
            version=sprint.version,
        )
        await _application_commit(self.db)
        return sprint

    async def delete_evaluation(
        self,
        sprint_id: str,
        evaluator_id: str,
        evaluation_id: str,
    ) -> str:
        """Delete a caller-owned evaluation from the ``Sprint.evaluations`` JSON column.

        MCP-FU6 (sprint, delete_sprint_evaluation, option A): the load, the ownership
        gate (``evaluator_id``) and the JSON mutation live HERE in the service
        (persistence mutation must not leak into the use-case
        layer). This does NOT commit; the caller (the MCP use case) owns the UoW commit
        so an unauthorized/not-found attempt persists nothing. Returns a status:
        ``"sprint_not_found"`` | ``"eval_not_found"`` | ``"not_owner"`` | ``"deleted"``.
        """
        sprint = await _application_get(self.db, "sprint", sprint_id)
        if not sprint:
            return "sprint_not_found"
        evaluations = list(sprint.evaluations or [])
        target = None
        for entry in evaluations:
            if entry.get("id") == evaluation_id:
                target = entry
                break
        if not target:
            return "eval_not_found"
        if target.get("evaluator_id") != evaluator_id:
            return "not_owner"
        evaluations.remove(target)
        sprint.evaluations = evaluations
        sprint.mark_dirty("evaluations")
        return "deleted"

    async def list_history(
        self, sprint_id: str, limit: int = 50
    ) -> list[SprintHistory]:
        return await _application_list(
            self.db,
            "sprint_history",
            filters=(_apf("sprint_id", "eq", sprint_id),),
            order_by=(("created_at", True),),
            limit=limit,
        )

    async def suggest_sprints(
        self,
        spec_id: str,
        threshold: int = 8,
    ) -> list[dict]:
        """Suggest sprint breakdown for a spec based on FRs, test scenarios, and dependencies.

        Algorithm:
        1. Group cards by linked FRs (via test_scenario_ids → linked_criteria).
        2. Consider card dependencies (dependent cards in same or later sprint).
        3. Distribute into N sprints where N = ceil(total_cards / threshold).
        4. Each sprint gets the test_scenario_ids and business_rule_ids for its cards.
        Returns suggestions without creating anything.
        """
        import math

        spec = await _application_get(self.db, "spec", spec_id)
        if not spec:
            raise ValueError("Spec not found")

        cards = await _application_list(
            self.db,
            "card",
            filters=(
                _apf("spec_id", "eq", spec_id),
                _apf("archived", "is_false"),
                _apf(
                    "status",
                    "not_in",
                    [CardStatus.DONE, CardStatus.CANCELLED],
                ),
            ),
        )

        if not cards:
            return []

        # Build FR→cards mapping via test_scenario_ids → linked_criteria
        scenarios = {s.get("id"): s for s in (spec.test_scenarios or [])}
        fr_groups: dict[str, list[Card]] = {}
        ungrouped: list[Card] = []

        for card in cards:
            linked_frs: set[str] = set()
            for ts_id in card.test_scenario_ids or []:
                sc = scenarios.get(ts_id)
                if sc:
                    for crit in sc.get("linked_criteria") or []:
                        linked_frs.add(crit)
            if linked_frs:
                primary_fr = sorted(linked_frs)[0]
                fr_groups.setdefault(primary_fr, []).append(card)
            else:
                ungrouped.append(card)

        # Build dependency graph
        dependencies = await _application_list(
            self.db,
            "card_dependency",
            filters=(_apf("card_id", "in", [c.id for c in cards]),),
        )
        dep_map: dict[str, set[str]] = {}
        for d in dependencies:
            dep_map.setdefault(d.card_id, set()).add(d.depends_on_id)

        # Flatten groups into ordered buckets
        all_groups = list(fr_groups.values())
        if ungrouped:
            all_groups.append(ungrouped)

        # Determine number of sprints
        total = len(cards)
        n_sprints = max(1, math.ceil(total / threshold))

        # Distribute groups across sprints
        suggested: list[list[Card]] = [[] for _ in range(n_sprints)]
        group_idx = 0
        for group in all_groups:
            target = group_idx % n_sprints
            suggested[target].extend(group)
            group_idx += 1

        # Ensure dependency ordering: if card A depends on B, B must be in same or earlier sprint
        card_sprint_map: dict[str, int] = {}
        for si, sprint_cards in enumerate(suggested):
            for c in sprint_cards:
                card_sprint_map[c.id] = si

        # Adjust: move cards earlier if their dependencies are in later sprints
        changed = True
        iterations = 0
        while changed and iterations < 10:
            changed = False
            iterations += 1
            for card_id, card_deps in dep_map.items():
                if card_id not in card_sprint_map:
                    continue
                card_si = card_sprint_map[card_id]
                for dep_id in card_deps:
                    dep_si = card_sprint_map.get(dep_id)
                    if dep_si is not None and dep_si > card_si:
                        # Move dependency to same sprint as dependent card
                        card_sprint_map[dep_id] = card_si
                        changed = True

        # Rebuild sprints from adjusted map
        final: list[list[Card]] = [[] for _ in range(n_sprints)]
        for card in cards:
            si = card_sprint_map.get(card.id, 0)
            final[si].append(card)

        # Build suggestion output
        suggestions = []
        for i, sprint_cards in enumerate(final):
            if not sprint_cards:
                continue
            # Collect scoped test scenario and BR IDs
            ts_ids: set[str] = set()
            br_ids: set[str] = set()
            for c in sprint_cards:
                for ts_id in c.test_scenario_ids or []:
                    ts_ids.add(ts_id)
                    sc = scenarios.get(ts_id)
                    if sc:
                        for linked in sc.get("linked_criteria") or []:
                            # Find BRs that reference this FR
                            for r in spec.business_rules or []:
                                if linked in (r.get("linked_requirements") or []):
                                    br_ids.add(r.get("id"))

            suggestions.append(
                {
                    "title": f"Sprint {i + 1}",
                    "description": f"Auto-suggested sprint ({len(sprint_cards)} tasks)",
                    "card_ids": [c.id for c in sprint_cards],
                    "card_titles": [c.title for c in sprint_cards],
                    "test_scenario_ids": sorted(ts_ids) if ts_ids else None,
                    "business_rule_ids": sorted(br_ids) if br_ids else None,
                }
            )

        return suggestions


class SprintQAService:
    """Service for sprint Q&A operations."""

    def __init__(self, db: Any):
        self.db = db

    async def get_question(self, qa_id: str) -> SprintQAItem | None:
        """Load a Sprint Q&A item so callers can authorize its parent first."""
        return await _application_get(self.db, "sprint_qa_item", qa_id)

    async def create_question(
        self,
        sprint_id: str,
        user_id: str,
        question: str,
        question_type: str = "text",
        choices: list | None = None,
        allow_free_text: bool = False,
    ) -> SprintQAItem | None:
        sprint = await _application_get(self.db, "sprint", sprint_id)
        if not sprint:
            return None
        qa = _new_application_record(
            "sprint_qa_item",
            sprint_id=sprint_id,
            question=question,
            question_type=question_type or "text",
            choices=choices,
            allow_free_text=allow_free_text,
            asked_by=user_id,
        )
        await _application_add(self.db, qa)
        return qa

    async def answer_question(
        self,
        qa_id: str,
        user_id: str,
        answer: str | None = None,
        selected: list[str] | None = None,
        *,
        actor_type: str = "user",
        surface: str = "service",
    ) -> SprintQAItem | None:
        qa = await _application_get(self.db, "sprint_qa_item", qa_id)
        if not qa:
            return None

        sprint = await _application_get(self.db, "sprint", qa.sprint_id)
        board = (
            await _application_get(self.db, "board", sprint.board_id)
            if sprint
            else None
        )
        await _authorize_qa_answer_or_raise(
            self.db,
            board=board,
            qa=qa,
            user_id=user_id,
            entity_type="sprint",
            question_id=qa_id,
            actor_type=actor_type,
            surface=surface,
        )

        qa.answer = answer
        qa.selected = selected
        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        if selected is not None:
            qa.mark_dirty("selected")
        return qa

    async def list_qa(self, sprint_id: str) -> list[SprintQAItem]:
        return await _application_list(
            self.db,
            "sprint_qa_item",
            filters=(_apf("sprint_id", "eq", sprint_id),),
            order_by=(("created_at", False),),
        )

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await _application_get(self.db, "sprint_qa_item", qa_id)
        if not qa:
            return False
        await _application_delete(self.db, qa)
        return True


async def mcp_list_my_mentions(
    db: Any,
    *,
    board_id: str,
    agent_id: str,
    agent_name: str | None,
    include_seen: bool = False,
) -> tuple[list[dict[str, Any]], bool]:
    if not agent_name or not agent_name.strip():
        return [], include_seen

    mention_pattern = f"%@{agent_name or ''}%"
    show_all = include_seen

    seen = await _application_list(
        db,
        "agent_seen_item",
        filters=(
            _apf("board_id", "eq", board_id),
            _apf("agent_id", "eq", agent_id),
        ),
    )
    seen_ids = {item.item_id for item in seen}

    cards = await _application_list(
        db,
        "card",
        filters=(_apf("board_id", "eq", board_id),),
    )
    specs = await _application_list(
        db,
        "spec",
        filters=(_apf("board_id", "eq", board_id),),
    )
    ideations = await _application_list(
        db,
        "ideation",
        filters=(_apf("board_id", "eq", board_id),),
    )
    refinements = await _application_list(
        db,
        "refinement",
        filters=(_apf("board_id", "eq", board_id),),
    )

    card_titles = {item.id: item.title for item in cards}
    spec_titles = {item.id: item.title for item in specs}
    ideation_titles = {item.id: item.title for item in ideations}
    refinement_titles = {item.id: item.title for item in refinements}

    comments = (
        await _application_list(
            db,
            "comment",
            filters=(
                _apf("card_id", "in", list(card_titles)),
                _apf("content", "ilike", mention_pattern),
            ),
            order_by=(("created_at", True),),
        )
        if card_titles
        else []
    )
    comment_results = [(item, card_titles[item.card_id]) for item in comments]

    qa_items = (
        await _application_list(
            db,
            "qa_item",
            filters=(_apf("card_id", "in", list(card_titles)),),
            any_filters=(
                _apf("question", "ilike", mention_pattern),
                _apf("answer", "ilike", mention_pattern),
            ),
            order_by=(("created_at", True),),
        )
        if card_titles
        else []
    )
    qa_results = [(item, card_titles[item.card_id]) for item in qa_items]

    spec_qa_items = (
        await _application_list(
            db,
            "spec_qa_item",
            filters=(_apf("spec_id", "in", list(spec_titles)),),
            any_filters=(
                _apf("question", "ilike", mention_pattern),
                _apf("answer", "ilike", mention_pattern),
            ),
            order_by=(("created_at", True),),
        )
        if spec_titles
        else []
    )
    spec_qa_results = [(item, spec_titles[item.spec_id]) for item in spec_qa_items]

    ideation_qa_items = (
        await _application_list(
            db,
            "ideation_qa_item",
            filters=(_apf("ideation_id", "in", list(ideation_titles)),),
            any_filters=(
                _apf("question", "ilike", mention_pattern),
                _apf("answer", "ilike", mention_pattern),
            ),
            order_by=(("created_at", True),),
        )
        if ideation_titles
        else []
    )
    ideation_qa_results = [
        (item, ideation_titles[item.ideation_id]) for item in ideation_qa_items
    ]

    refinement_qa_items = (
        await _application_list(
            db,
            "refinement_qa_item",
            filters=(_apf("refinement_id", "in", list(refinement_titles)),),
            any_filters=(
                _apf("question", "ilike", mention_pattern),
                _apf("answer", "ilike", mention_pattern),
            ),
            order_by=(("created_at", True),),
        )
        if refinement_titles
        else []
    )
    refinement_qa_results = [
        (item, refinement_titles[item.refinement_id]) for item in refinement_qa_items
    ]

    mentions: list[dict[str, Any]] = []
    for comment, card_title in comment_results:
        if not show_all and comment.id in seen_ids:
            continue
        mentions.append(
            {
                "type": "comment",
                "item_id": comment.id,
                "card_id": comment.card_id,
                "card_title": card_title,
                "content": comment.content,
                "author": comment.author_id,
                "created_at": comment.created_at.isoformat(),
                "seen": comment.id in seen_ids,
            }
        )
    for qa, card_title in qa_results:
        if not show_all and qa.id in seen_ids:
            continue
        mentions.append(
            {
                "type": "qa",
                "item_id": qa.id,
                "card_id": qa.card_id,
                "card_title": card_title,
                "question": qa.question,
                "answer": qa.answer,
                "asked_by": qa.asked_by,
                "created_at": qa.created_at.isoformat(),
                "seen": qa.id in seen_ids,
            }
        )
    for spec_qa, spec_title in spec_qa_results:
        if not show_all and spec_qa.id in seen_ids:
            continue
        mentions.append(
            {
                "type": "spec_qa",
                "item_id": spec_qa.id,
                "spec_id": spec_qa.spec_id,
                "spec_title": spec_title,
                "question": spec_qa.question,
                "question_type": spec_qa.question_type,
                "choices": spec_qa.choices,
                "answer": spec_qa.answer,
                "selected": spec_qa.selected,
                "asked_by": spec_qa.asked_by,
                "created_at": spec_qa.created_at.isoformat(),
                "seen": spec_qa.id in seen_ids,
            }
        )
    for ideation_qa, ideation_title in ideation_qa_results:
        if not show_all and ideation_qa.id in seen_ids:
            continue
        mentions.append(
            {
                "type": "ideation_qa",
                "item_id": ideation_qa.id,
                "ideation_id": ideation_qa.ideation_id,
                "ideation_title": ideation_title,
                "question": ideation_qa.question,
                "question_type": ideation_qa.question_type,
                "choices": ideation_qa.choices,
                "answer": ideation_qa.answer,
                "selected": ideation_qa.selected,
                "asked_by": ideation_qa.asked_by,
                "created_at": ideation_qa.created_at.isoformat(),
                "seen": ideation_qa.id in seen_ids,
            }
        )
    for refinement_qa, refinement_title in refinement_qa_results:
        if not show_all and refinement_qa.id in seen_ids:
            continue
        mentions.append(
            {
                "type": "refinement_qa",
                "item_id": refinement_qa.id,
                "refinement_id": refinement_qa.refinement_id,
                "refinement_title": refinement_title,
                "question": refinement_qa.question,
                "question_type": refinement_qa.question_type,
                "choices": refinement_qa.choices,
                "answer": refinement_qa.answer,
                "selected": refinement_qa.selected,
                "asked_by": refinement_qa.asked_by,
                "created_at": refinement_qa.created_at.isoformat(),
                "seen": refinement_qa.id in seen_ids,
            }
        )
    mentions.sort(key=lambda m: m["created_at"], reverse=True)
    return mentions, show_all


async def mcp_mark_mentions_seen(
    db: Any,
    *,
    board_id: str,
    agent_id: str,
    agent_name: str | None,
    item_ids: list[str],
) -> tuple[int, int]:
    requested_ids = list(dict.fromkeys(item_ids))
    mentions, _ = await mcp_list_my_mentions(
        db,
        board_id=board_id,
        agent_id=agent_id,
        agent_name=agent_name,
        include_seen=True,
    )
    mention_ids = {str(item["item_id"]) for item in mentions}
    eligible_ids = [item_id for item_id in requested_ids if item_id in mention_ids]

    existing_rows = (
        await _application_list(
            db,
            "agent_seen_item",
            filters=(
                _apf("board_id", "eq", board_id),
                _apf("agent_id", "eq", agent_id),
                _apf("item_id", "in", eligible_ids),
            ),
        )
        if eligible_ids
        else []
    )
    existing_ids = {item.item_id for item in existing_rows}
    marked = 0
    for item_id in eligible_ids:
        if item_id in existing_ids:
            continue
        await _application_add(
            db,
            _new_application_record(
                "agent_seen_item",
                board_id=board_id,
                agent_id=agent_id,
                item_type="mention",
                item_id=item_id,
            ),
        )
        existing_ids.add(item_id)
        marked += 1

    if marked > 0:
        comments = await _application_list(
            db,
            "comment",
            filters=(_apf("id", "in", eligible_ids),),
        )
        qa_items = await _application_list(
            db,
            "qa_item",
            filters=(_apf("id", "in", eligible_ids),),
        )
        card_ids = {item.card_id for item in comments} | {
            item.card_id for item in qa_items
        }
        board_service = BoardService(db)
        for card_id in card_ids:
            await board_service._log_activity(
                board_id=board_id,
                card_id=card_id,
                action="items_seen",
                actor_type="agent",
                actor_id=agent_id,
                actor_name=agent_name,
                details={"item_count": marked},
            )

        spec_qa_items = await _application_list(
            db,
            "spec_qa_item",
            filters=(_apf("id", "in", eligible_ids),),
        )
        for spec_id in {item.spec_id for item in spec_qa_items}:
            await board_service._log_activity(
                board_id=board_id,
                action="spec_qa_seen",
                actor_type="agent",
                actor_id=agent_id,
                actor_name=agent_name,
                details={"spec_id": spec_id, "item_count": marked},
            )

        ideation_qa_items = await _application_list(
            db,
            "ideation_qa_item",
            filters=(_apf("id", "in", eligible_ids),),
        )
        for ideation_id in {item.ideation_id for item in ideation_qa_items}:
            await board_service._log_activity(
                board_id=board_id,
                action="ideation_qa_seen",
                actor_type="agent",
                actor_id=agent_id,
                actor_name=agent_name,
                details={"ideation_id": ideation_id, "item_count": marked},
            )

        refinement_qa_items = await _application_list(
            db,
            "refinement_qa_item",
            filters=(_apf("id", "in", eligible_ids),),
        )
        for refinement_id in {item.refinement_id for item in refinement_qa_items}:
            await board_service._log_activity(
                board_id=board_id,
                action="refinement_qa_seen",
                actor_type="agent",
                actor_id=agent_id,
                actor_name=agent_name,
                details={"refinement_id": refinement_id, "item_count": marked},
            )
    return marked, len(requested_ids)


async def mcp_get_unseen_summary(
    db: Any,
    *,
    board_id: str,
    agent_id: str,
    agent_name: str | None,
) -> dict[str, Any]:
    mentions, _ = await mcp_list_my_mentions(
        db,
        board_id=board_id,
        agent_id=agent_id,
        agent_name=agent_name,
        include_seen=True,
    )
    mention_ids = {item["item_id"] for item in mentions}
    seen_rows = (
        await _application_list(
            db,
            "agent_seen_item",
            filters=(
                _apf("board_id", "eq", board_id),
                _apf("agent_id", "eq", agent_id),
                _apf("item_id", "in", list(mention_ids)),
            ),
        )
        if mention_ids
        else []
    )
    seen_ids = {item.item_id for item in seen_rows}

    recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    recent_activity = await _application_list(
        db,
        "activity_log",
        filters=(
            _apf("board_id", "eq", board_id),
            _apf("created_at", "gte", recent_cutoff),
        ),
    )
    total_mentions = len(mentions)
    unseen_mentions = total_mentions - len(seen_ids)
    return {
        "board_id": board_id,
        "unseen_mentions": unseen_mentions,
        "total_mentions": total_mentions,
        "seen_count": len(seen_ids),
        "recent_activity_24h": len(recent_activity),
    }


async def mcp_get_activity_log_rows(
    db: Any,
    *,
    board_id: str,
    limit: int,
    cursor_pair: tuple[datetime, str] | None,
    effective_offset: int,
    action: str = "",
    card_id: str = "",
    include_details: bool = False,
) -> tuple[list[dict[str, Any]], tuple[datetime, str] | None]:
    from okto_pulse.core.services.activity_log import (
        activity_log_summary,
        sanitize_activity_details,
    )
    from okto_pulse.core.services.analytics_contract import (
        normalize_activity_timestamp,
    )

    filters = [_apf("board_id", "eq", board_id)]
    if action:
        filters.append(_apf("action", "eq", action))
    if card_id:
        filters.append(_apf("card_id", "eq", card_id))
    cursor_groups: tuple[tuple[ApplicationFilter, ...], ...] = ()
    if cursor_pair is not None:
        ts, rid = cursor_pair
        cursor_groups = (
            (_apf("created_at", "lt", ts),),
            (
                _apf("created_at", "eq", ts),
                _apf("id", "lt", rid),
            ),
        )
    logs = await _application_list(
        db,
        "activity_log",
        filters=tuple(filters),
        any_groups=cursor_groups,
        order_by=(("created_at", True), ("id", True)),
        offset=effective_offset,
        limit=limit + 1,
    )

    has_more = len(logs) > limit
    if has_more:
        logs = logs[:limit]

    rows: list[dict[str, Any]] = []
    for log in logs:
        row: dict[str, Any] = {
            "id": log.id,
            "action": log.action,
            "card_id": log.card_id,
            "created_at": normalize_activity_timestamp(log.created_at).isoformat(),
            "trigger": (
                (log.details or {}).get("trigger")
                if isinstance(log.details, dict)
                else None
            ),
            "summary": activity_log_summary(log.action, log.details),
        }
        if include_details:
            row["actor_type"] = log.actor_type
            row["actor_id"] = log.actor_id
            row["actor_name"] = log.actor_name
            row["details"] = sanitize_activity_details(log.details)
        rows.append(row)

    next_cursor_pair = (logs[-1].created_at, logs[-1].id) if has_more and logs else None
    return rows, next_cursor_pair
