"""Stable, lazily resolved exports for Core services.

Keeping package initialization side-effect free is an import-boundary invariant:
ports may depend on leaf service helpers without pulling in ``services.main``
while the port itself is still being initialized.
"""

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from types import MappingProxyType
from typing import Any


_ExportTarget = tuple[str, str]


def _register_exports(
    module_name: str,
    names: tuple[str, ...],
    *,
    aliases: dict[str, str] | None = None,
) -> None:
    pending = getattr(_register_exports, "_pending", None)
    if pending is None:
        pending = {}
        setattr(_register_exports, "_pending", pending)
    for public_name in names:
        if public_name in pending:
            raise RuntimeError(f"duplicate service export: {public_name}")
        pending[public_name] = (module_name, public_name)
    for public_name, target_name in (aliases or {}).items():
        if public_name in pending:
            raise RuntimeError(f"duplicate service export: {public_name}")
        pending[public_name] = (module_name, target_name)


_register_exports(
    "okto_pulse.core.services.architecture",
    (
        "ArchitectureDesignRepository",
        "ArchitectureFindingGate",
        "ArchitecturePropagationService",
        "ArchitectureDiagramAdapter",
        "ArchitectureDiagramAdapterRegistry",
        "ArchitectureDiagramStore",
        "ExcalidrawArchitectureDiagramAdapter",
        "RawArchitectureDiagramAdapter",
    ),
)
_register_exports(
    "okto_pulse.core.services.bug_regression_scenarios",
    (
        "BugRegressionEligibilityReason",
        "BugRegressionGateDecision",
        "BugRegressionGateValidationResult",
        "BugRegressionGateValidator",
        "BugRegressionNextAction",
        "BugRegressionRejectionReason",
        "BugRegressionScenarioEligibilityResolver",
        "BugRegressionScenarioEligibilityResult",
        "EligibleBugRegressionScenario",
        "RejectedBugRegressionScenario",
    ),
)
_register_exports(
    "okto_pulse.core.services.bug_workflow_remediation",
    (
        "BugWorkflowHotfixLaneStatus",
        "BugWorkflowNextAction",
        "BugWorkflowRemediationAction",
        "BugWorkflowRemediationMessage",
        "BugWorkflowRemediationMessageBuilder",
        "BugWorkflowRemediationPath",
        "bug_workflow_remediation_safe_labels",
        "serialize_bug_workflow_remediation",
    ),
)
_register_exports(
    "okto_pulse.core.services.board_governance",
    (
        "BoardGovernanceService",
        "BoardGovernanceSettings",
        "GOVERNANCE_SETTING_KEYS",
        "QA_SELF_ANSWER_DENIED_ACTION",
        "QA_SELF_ANSWER_DENIED_METRIC",
        "QASelfAnsweringNotAllowedError",
        "SELF_ANSWERING_NOT_ALLOWED_REASON",
        "build_qa_self_answer_denied_details",
    ),
)
_register_exports(
    "okto_pulse.core.services.qa_selection",
    ("QASelectionError", "validate_choice_selection"),
)
_register_exports(
    "okto_pulse.core.services.critical_context_guard",
    (
        "CONTEXT_FINGERPRINT_ALG",
        "CRITICAL_ACTION_REGISTRY",
        "CRITICAL_CONTEXT_DECISION_ACTION",
        "CRITICAL_CONTEXT_DECISION_METRIC",
        "CRITICAL_CONTEXT_RESOLUTION_FAILURE_ACTION",
        "CRITICAL_CONTEXT_RESOLUTION_FAILURE_METRIC",
        "ContextFingerprintProvider",
        "CriticalAction",
        "CriticalActionDefinition",
        "CriticalContextDecision",
        "CriticalMutationGuardCoverage",
        "CRITICAL_MUTATION_GUARD_COVERAGE",
        "DatabaseFullContextResolver",
        "FullContextCriticalActionGuard",
        "FullContextGuardError",
        "FullContextRequiredError",
        "FullContextResolver",
        "FullContextUnavailableError",
        "NON_CRITICAL_MUTATION_EXCLUSIONS",
        "NonCriticalMutationExclusion",
        "build_default_full_context_resolvers",
        "coerce_critical_action",
        "critical_actions_for_entity",
        "get_critical_action_definition",
    ),
)
_register_exports(
    "okto_pulse.core.services.cancellation",
    ("CancellationReasonRequiredError", "apply_cancellation_policy"),
)
_register_exports(
    "okto_pulse.core.services.governance_observability",
    (
        "GovernanceAuditPayloadError",
        "GovernanceMetricEvent",
        "GovernanceMetricsSink",
        "build_board_governance_setting_changed_details",
        "build_board_missing_context_warning_details",
        "build_governance_safe_label_violation_details",
        "emit_governance_metric",
        "get_governance_metric_samples",
        "reset_governance_metric_samples",
        "sanitize_governance_metric_event",
        "validate_governance_audit_details",
    ),
    aliases={
        "build_safe_qa_self_answer_denied_details": (
            "build_qa_self_answer_denied_details"
        )
    },
)
_register_exports(
    "okto_pulse.core.services.main",
    (
        "AgentService",
        "AmbiguityGateError",
        "ArchiveService",
        "AttachmentService",
        "BoardService",
        "CardOperationError",
        "CardResourceReadOnlyError",
        "CardService",
        "CARD_RESOURCE_READ_ONLY_MESSAGE",
        "CommentService",
        "GuidelineService",
        "IdeationKnowledgeService",
        "IdeationQAService",
        "IdeationService",
        "QAService",
        "RefinementKnowledgeService",
        "RefinementQAService",
        "RefinementService",
        "ShareService",
        "SpecKnowledgeService",
        "SpecQAService",
        "SpecService",
        "StoryService",
        "TopicOperationError",
    ),
)
_register_exports(
    "okto_pulse.core.services.resource_gate",
    (
        "ResourceGateError",
        "ResourceGateJustificationRequired",
        "ResourceGateNotFound",
        "ResourceGateService",
        "ResourceGateViolation",
    ),
)
_register_exports(
    "okto_pulse.core.services.resource_lineage",
    (
        "ResourceRevisionStamp",
        "ResolvedResourceLineageProjection",
        "ResolvedResourceLineageService",
        "get_resource_lineage_metric_samples",
        "reset_resource_lineage_observability_for_tests",
    ),
)
_register_exports(
    "okto_pulse.core.domain.knowledge_fingerprint",
    (
        "KNOWLEDGE_CONTENT_HASH_FIELDS",
        "compute_knowledge_content_sha256",
        "knowledge_content_bytes",
        "knowledge_content_sha256",
        "resolve_knowledge_content_sha256",
    ),
)
_register_exports(
    "okto_pulse.core.services.knowledge_propagation",
    (
        "KnowledgeCreationPreflightCommand",
        "KnowledgeGrandfatherAttachment",
        "KnowledgeGrandfatherCommand",
        "KnowledgeGrandfatherEvidence",
        "KnowledgeMutationCommand",
        "KnowledgeMutationPreparation",
        "KnowledgeMutationResultV2",
        "KnowledgeMutationResultV2Projector",
        "KnowledgePropagationReadResult",
        "KnowledgePropagationService",
        "KnowledgePropagationServiceError",
        "KnowledgeRelinkResetCommand",
        "KnowledgeRefreshCommand",
        "KnowledgeRefreshByKnowledgeIdsCommand",
        "ResolvedKnowledgeAssignment",
        "classify_legacy_origin",
        "deterministic_knowledge_target_id",
    ),
)
_register_exports(
    "okto_pulse.core.services.spec_resource_propagation",
    ("SpecResourcePropagationService",),
)
_register_exports(
    "okto_pulse.core.services.spec_structured_entities",
    (
        "InMemoryStructuredSpecEntityMetricsSink",
        "StructuredSpecEntityCommand",
        "StructuredSpecEntityErrorCode",
        "StructuredSpecEntityMetricEvent",
        "StructuredSpecEntityMetricsSink",
        "StructuredSpecEntityResult",
        "StructuredSpecEntityService",
    ),
)

_EXPORTS: Mapping[str, _ExportTarget] = MappingProxyType(
    dict(getattr(_register_exports, "_pending"))
)
delattr(_register_exports, "_pending")


def __getattr__(name: str) -> Any:
    try:
        module_name, target_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    return getattr(import_module(module_name), target_name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS))


__all__ = [
    "ArchitectureDesignRepository",
    "ArchitectureFindingGate",
    "ArchitecturePropagationService",
    "ArchitectureDiagramAdapter",
    "ArchitectureDiagramAdapterRegistry",
    "ArchitectureDiagramStore",
    "ExcalidrawArchitectureDiagramAdapter",
    "RawArchitectureDiagramAdapter",
    "BugRegressionEligibilityReason",
    "BugRegressionGateDecision",
    "BugRegressionGateValidationResult",
    "BugRegressionGateValidator",
    "BugRegressionNextAction",
    "BugRegressionRejectionReason",
    "BugRegressionScenarioEligibilityResolver",
    "BugRegressionScenarioEligibilityResult",
    "EligibleBugRegressionScenario",
    "RejectedBugRegressionScenario",
    "BugWorkflowHotfixLaneStatus",
    "BugWorkflowNextAction",
    "BugWorkflowRemediationAction",
    "BugWorkflowRemediationMessage",
    "BugWorkflowRemediationMessageBuilder",
    "BugWorkflowRemediationPath",
    "bug_workflow_remediation_safe_labels",
    "serialize_bug_workflow_remediation",
    "BoardGovernanceService",
    "BoardGovernanceSettings",
    "GOVERNANCE_SETTING_KEYS",
    "QA_SELF_ANSWER_DENIED_ACTION",
    "QA_SELF_ANSWER_DENIED_METRIC",
    "QASelfAnsweringNotAllowedError",
    "SELF_ANSWERING_NOT_ALLOWED_REASON",
    "build_qa_self_answer_denied_details",
    "QASelectionError",
    "validate_choice_selection",
    "CONTEXT_FINGERPRINT_ALG",
    "CRITICAL_ACTION_REGISTRY",
    "CRITICAL_CONTEXT_DECISION_ACTION",
    "CRITICAL_CONTEXT_DECISION_METRIC",
    "CRITICAL_CONTEXT_RESOLUTION_FAILURE_ACTION",
    "CRITICAL_CONTEXT_RESOLUTION_FAILURE_METRIC",
    "ContextFingerprintProvider",
    "CriticalAction",
    "CriticalActionDefinition",
    "CriticalContextDecision",
    "CriticalMutationGuardCoverage",
    "CRITICAL_MUTATION_GUARD_COVERAGE",
    "DatabaseFullContextResolver",
    "FullContextCriticalActionGuard",
    "FullContextGuardError",
    "FullContextRequiredError",
    "FullContextResolver",
    "FullContextUnavailableError",
    "NON_CRITICAL_MUTATION_EXCLUSIONS",
    "NonCriticalMutationExclusion",
    "build_default_full_context_resolvers",
    "coerce_critical_action",
    "critical_actions_for_entity",
    "get_critical_action_definition",
    "GovernanceAuditPayloadError",
    "GovernanceMetricEvent",
    "GovernanceMetricsSink",
    "build_board_governance_setting_changed_details",
    "build_board_missing_context_warning_details",
    "build_governance_safe_label_violation_details",
    "build_safe_qa_self_answer_denied_details",
    "emit_governance_metric",
    "get_governance_metric_samples",
    "reset_governance_metric_samples",
    "sanitize_governance_metric_event",
    "validate_governance_audit_details",
    "AgentService",
    "AmbiguityGateError",
    "ArchiveService",
    "AttachmentService",
    "BoardService",
    "CancellationReasonRequiredError",
    "apply_cancellation_policy",
    "CardOperationError",
    "CardResourceReadOnlyError",
    "CardService",
    "CARD_RESOURCE_READ_ONLY_MESSAGE",
    "CommentService",
    "GuidelineService",
    "IdeationKnowledgeService",
    "IdeationQAService",
    "IdeationService",
    "QAService",
    "RefinementKnowledgeService",
    "RefinementQAService",
    "RefinementService",
    "ShareService",
    "SpecKnowledgeService",
    "SpecQAService",
    "SpecService",
    "StoryService",
    "TopicOperationError",
    "ResourceGateError",
    "ResourceGateJustificationRequired",
    "ResourceGateNotFound",
    "ResourceGateService",
    "ResourceGateViolation",
    "ResourceRevisionStamp",
    "ResolvedResourceLineageProjection",
    "ResolvedResourceLineageService",
    "get_resource_lineage_metric_samples",
    "reset_resource_lineage_observability_for_tests",
    "KNOWLEDGE_CONTENT_HASH_FIELDS",
    "compute_knowledge_content_sha256",
    "knowledge_content_bytes",
    "knowledge_content_sha256",
    "resolve_knowledge_content_sha256",
    "KnowledgeCreationPreflightCommand",
    "KnowledgeGrandfatherAttachment",
    "KnowledgeGrandfatherCommand",
    "KnowledgeGrandfatherEvidence",
    "KnowledgeMutationCommand",
    "KnowledgeMutationPreparation",
    "KnowledgeMutationResultV2",
    "KnowledgeMutationResultV2Projector",
    "KnowledgePropagationReadResult",
    "KnowledgePropagationService",
    "KnowledgePropagationServiceError",
    "KnowledgeRelinkResetCommand",
    "KnowledgeRefreshCommand",
    "KnowledgeRefreshByKnowledgeIdsCommand",
    "ResolvedKnowledgeAssignment",
    "classify_legacy_origin",
    "deterministic_knowledge_target_id",
    "SpecResourcePropagationService",
    "InMemoryStructuredSpecEntityMetricsSink",
    "StructuredSpecEntityCommand",
    "StructuredSpecEntityErrorCode",
    "StructuredSpecEntityMetricEvent",
    "StructuredSpecEntityMetricsSink",
    "StructuredSpecEntityResult",
    "StructuredSpecEntityService",
]

if len(__all__) != len(set(__all__)):
    raise RuntimeError("duplicate names in public service exports")
if set(__all__) != set(_EXPORTS):
    missing = sorted(set(__all__) - set(_EXPORTS))
    unexpected = sorted(set(_EXPORTS) - set(__all__))
    raise RuntimeError(
        f"service export map mismatch: missing={missing}, unexpected={unexpected}"
    )
