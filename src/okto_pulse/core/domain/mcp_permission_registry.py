"""Exact, transport-free permission inventory for the MCP command surface.

The manifest in this module is deliberately boring: every command name is
written out and bound to one or more permission leaves.  There is no prefix,
name-shape, or default-policy inference, so adding a command cannot silently
inherit authority from an existing family.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any

from okto_pulse.core.domain.sdlc_registry import transition_permission_flags


# The cap intentionally equals the reviewed baseline.  Adding an exception must
# therefore change both the explicit record and this conspicuous budget.
MAX_HUMAN_ONLY_TOOL_EXEMPTIONS = 3


class McpAdmissionClass(str, Enum):
    """Closed execution-effect classes published with every Core MCP tool."""

    READER = "reader"
    WRITER = "writer"


# Exact allowlist reviewed for concurrent read admission.  This is deliberately
# independent from permission-leaf spelling: read-authorized tools that persist
# counters, acquire semantic mutexes, render, preview, or otherwise produce
# effects remain writers unless they are explicitly present here.
MCP_READER_TOOL_NAMES = frozenset(
    {
        "okto_pulse_get_active_default_board_config",
        "okto_pulse_get_amendment_revision",
        "okto_pulse_get_architecture_design_schema",
        "okto_pulse_get_board",
        "okto_pulse_get_board_design_system",
        "okto_pulse_get_board_guidelines",
        "okto_pulse_get_card",
        "okto_pulse_get_card_dependencies",
        "okto_pulse_get_checklist_binding",
        "okto_pulse_get_checklist_receipt",
        "okto_pulse_get_code_evidence",
        "okto_pulse_get_code_investigation_receipt",
        "okto_pulse_get_current_quality_assessment",
        "okto_pulse_get_current_semantic_guideline_assessment",
        "okto_pulse_get_design_system",
        "okto_pulse_get_guideline_revision",
        "okto_pulse_get_ideation",
        "okto_pulse_get_ideation_context",
        "okto_pulse_get_ideation_knowledge",
        "okto_pulse_get_implementation_overlaps",
        "okto_pulse_get_requirement_lint_preflight",
        "okto_pulse_get_my_profile",
        "okto_pulse_get_publish_health",
        "okto_pulse_get_quality_assessment_receipt",
        "okto_pulse_get_refinement",
        "okto_pulse_get_refinement_context",
        "okto_pulse_get_refinement_knowledge",
        "okto_pulse_get_resource_gate_summary",
        "okto_pulse_get_semantic_guideline_assessment",
        "okto_pulse_get_semantic_guideline_waiver",
        "okto_pulse_get_spec",
        "okto_pulse_get_spec_context",
        "okto_pulse_get_spec_evaluation",
        "okto_pulse_get_spec_knowledge",
        "okto_pulse_get_sprint",
        "okto_pulse_get_sprint_context",
        "okto_pulse_get_sprint_evaluation",
        "okto_pulse_get_task_conclusions",
        "okto_pulse_get_task_validation",
        "okto_pulse_get_traceability_report",
        "okto_pulse_kg_canonical_debt_list",
        "okto_pulse_kg_canonical_partition_integrity_list",
        "okto_pulse_kg_connectivity_dlq_diagnose",
        "okto_pulse_kg_connectivity_dlq_verify",
        "okto_pulse_kg_dead_letter_list",
        "okto_pulse_kg_digest_layer_mismatch_list",
        "okto_pulse_kg_evaluate_bug_cognitive_closure",
        "okto_pulse_kg_evaluate_cognitive_readiness",
        "okto_pulse_kg_global_discovery_recovery_status",
        "okto_pulse_kg_global_outbox_dead_letter_list",
        "okto_pulse_kg_health",
        "okto_pulse_kg_health_readiness",
        "okto_pulse_kg_list_cognitive_dlq",
        "okto_pulse_kg_list_cognitive_pending_items",
        "okto_pulse_kg_list_cognitive_readiness_items",
        "okto_pulse_kg_originates_from_contract_audit",
        "okto_pulse_kg_orphan_report",
        "okto_pulse_kg_provenance_drift",
        "okto_pulse_kg_queue_drilldown",
        "okto_pulse_kg_stale_canonical_parity_list",
        "okto_pulse_kg_takedown_status",
        "okto_pulse_kg_verify_grounding",
        "okto_pulse_list_agents",
        "okto_pulse_list_amendment_revisions",
        "okto_pulse_list_api_contracts",
        "okto_pulse_list_architecture_propagation_legacy",
        "okto_pulse_list_attachments",
        "okto_pulse_list_blockers",
        "okto_pulse_list_board_members",
        "okto_pulse_list_business_rules",
        "okto_pulse_list_by_board",
        "okto_pulse_list_cards_by_status",
        "okto_pulse_list_code_evidence",
        "okto_pulse_list_comments",
        "okto_pulse_list_default_board_config_versions",
        "okto_pulse_list_design_systems",
        "okto_pulse_list_guideline_revisions",
        "okto_pulse_list_guidelines",
        "okto_pulse_list_implementation_targets",
        "okto_pulse_list_integration_requirements",
        "okto_pulse_list_my_boards",
        "okto_pulse_list_observability_requirements",
        "okto_pulse_list_qa",
        "okto_pulse_list_quality_assessments",
        "okto_pulse_list_quality_findings",
        "okto_pulse_list_research_decisions",
        "okto_pulse_list_screen_mockups",
        "okto_pulse_list_semantic_guideline_assessments",
        "okto_pulse_list_semantic_guideline_findings",
        "okto_pulse_list_semantic_guideline_waiver_events",
        "okto_pulse_list_semantic_guideline_waivers",
        "okto_pulse_list_spec_dependencies",
        "okto_pulse_list_spec_evaluations",
        "okto_pulse_list_spec_validations",
        "okto_pulse_list_sprint_evaluations",
        "okto_pulse_list_task_validations",
        "okto_pulse_list_test_scenarios",
        "okto_pulse_resolve_bug_regression_scenarios",
        "okto_pulse_suggest_sprints",
    }
)


@dataclass(frozen=True, order=True)
class McpToolPermissionPolicy:
    """Canonical permission leaves that an exact MCP command may exercise."""

    tool_name: str
    permission_flags: tuple[str, ...]
    admission_class: McpAdmissionClass = McpAdmissionClass.WRITER


@dataclass(frozen=True, order=True)
class HumanOnlyToolExemption:
    """Audited exception for an MCP command that always refuses agent mutation."""

    tool_name: str
    reason: str
    admission_class: McpAdmissionClass = McpAdmissionClass.WRITER


def _policy(tool_name: str, *permission_flags: str) -> McpToolPermissionPolicy:
    admission_class = (
        McpAdmissionClass.READER
        if tool_name in MCP_READER_TOOL_NAMES
        else McpAdmissionClass.WRITER
    )
    return McpToolPermissionPolicy(
        tool_name,
        tuple(permission_flags),
        admission_class,
    )


# Keep this tuple sorted by exact tool name.  Multiple flags describe a command
# whose explicit arguments select among several permission leaves; they are not
# alternatives inferred from the command name.
MCP_TOOL_PERMISSION_POLICIES: tuple[McpToolPermissionPolicy, ...] = (
    _policy(
        "okto_pulse_acknowledge_implementation_overlap",
        "code_traceability.overlap.acknowledge",
    ),
    _policy(
        "okto_pulse_activate_default_board_config_version",
        "default_board_config.activate",
    ),
    _policy("okto_pulse_add_api_contract", "spec.contracts.create"),
    _policy(
        "okto_pulse_add_architecture_design",
        "ideation.architecture.create",
        "refinement.architecture.create",
        "spec.architecture.create",
        "card.architecture.create",
    ),
    _policy("okto_pulse_add_business_rule", "spec.rules.create"),
    _policy("okto_pulse_add_card_dependency", "card.entity.manage_dependencies"),
    _policy("okto_pulse_add_card_knowledge", "card.copy_from_spec.knowledge"),
    _policy("okto_pulse_add_choice_comment", "card.comments.create_choice"),
    _policy("okto_pulse_add_comment", "card.comments.create"),
    _policy(
        "okto_pulse_add_decision",
        "spec.structured_entity.decision.create",
        "spec.structured_entity.decision.supersede",
    ),
    _policy("okto_pulse_add_ideation_knowledge", "ideation.knowledge.create"),
    _policy(
        "okto_pulse_add_integration_requirement",
        "spec.integration_requirements.create",
    ),
    _policy(
        "okto_pulse_add_observability_requirement",
        "spec.observability_requirements.create",
    ),
    _policy("okto_pulse_add_refinement_knowledge", "refinement.knowledge.create"),
    _policy(
        "okto_pulse_add_screen_mockup",
        "story.mockups.create",
        "ideation.mockups.create",
        "refinement.mockups.create",
        "spec.mockups.create",
        "card.mockups.create",
    ),
    _policy(
        "okto_pulse_add_spec_dependency",
        "spec.entity.manage_dependencies",
        "spec.entity.read",
    ),
    _policy("okto_pulse_add_spec_knowledge", "spec.knowledge.create"),
    _policy("okto_pulse_add_test_scenario", "spec.tests.create"),
    _policy("okto_pulse_adopt_guideline_revision", "guidelines.adoption.manage"),
    _policy(
        "okto_pulse_annotate_mockup",
        "story.mockups.annotate",
        "ideation.mockups.annotate",
        "refinement.mockups.annotate",
        "spec.mockups.annotate",
        "card.mockups.annotate",
    ),
    _policy("okto_pulse_answer_ideation_question", "ideation.qa.answer"),
    _policy(
        "okto_pulse_answer_question",
        "ideation.qa.answer",
        "refinement.qa.answer",
        "spec.qa.answer",
        "sprint.qa.answer",
        "card.qa.answer",
    ),
    _policy("okto_pulse_answer_refinement_question", "refinement.qa.answer"),
    _policy("okto_pulse_answer_spec_question", "spec.qa.answer"),
    _policy("okto_pulse_answer_sprint_question", "sprint.qa.answer"),
    _policy(
        "okto_pulse_append_research_decision",
        "refinement.research_decisions.append",
    ),
    _policy("okto_pulse_archive_story", "story.entity.archive"),
    _policy("okto_pulse_archive_topic", "topic.entity.archive"),
    _policy(
        "okto_pulse_archive_tree",
        "ideation.entity.archive",
        "refinement.entity.archive",
        "spec.entity.archive",
    ),
    _policy(
        "okto_pulse_ask",
        "ideation.qa.ask",
        "refinement.qa.ask",
        "spec.qa.ask",
        "sprint.qa.ask",
        "card.qa.ask",
    ),
    _policy("okto_pulse_ask_ideation_choice_question", "ideation.qa.ask_choice"),
    _policy("okto_pulse_ask_ideation_question", "ideation.qa.ask"),
    _policy(
        "okto_pulse_ask_question",
        "ideation.qa.ask",
        "refinement.qa.ask",
        "spec.qa.ask",
        "sprint.qa.ask",
        "card.qa.ask",
    ),
    _policy(
        "okto_pulse_ask_refinement_choice_question",
        "refinement.qa.ask_choice",
    ),
    _policy("okto_pulse_ask_refinement_question", "refinement.qa.ask"),
    _policy("okto_pulse_ask_spec_choice_question", "spec.qa.ask_choice"),
    _policy("okto_pulse_ask_spec_question", "spec.qa.ask"),
    _policy("okto_pulse_ask_sprint_question", "sprint.qa.ask"),
    _policy("okto_pulse_assign_tasks_to_sprint", "sprint.tasks.assign"),
    _policy(
        "okto_pulse_associate_amendment_revision_artifacts",
        "amendment.revision.associate",
    ),
    _policy(
        "okto_pulse_clear_code_traceability_not_applicable",
        "code_traceability.waiver.clear",
    ),
    _policy(
        "okto_pulse_clear_resource_not_applicable",
        "ideation.entity.edit_fields",
        "refinement.entity.edit_fields",
        "spec.entity.edit_coverage_flags",
        "sprint.entity.edit_coverage_flags",
        "card.entity.edit_fields",
    ),
    _policy("okto_pulse_confirm_amendment_coverage", "amendment.coverage.confirm"),
    _policy("okto_pulse_convert_stories_to_ideation", "story.conversion.to_ideation"),
    _policy("okto_pulse_copy_architecture_to_card", "card.copy_from_spec.architecture"),
    _policy("okto_pulse_copy_knowledge_to_card", "card.copy_from_spec.knowledge"),
    _policy("okto_pulse_copy_mockups_to_card", "card.copy_from_spec.mockups"),
    _policy("okto_pulse_copy_qa_to_card", "card.copy_from_spec.qa"),
    _policy("okto_pulse_create_amendment_revision", "amendment.revision.create"),
    _policy(
        "okto_pulse_create_card",
        "card.entity.create",
        "card.entity.create_test",
    ),
    _policy(
        "okto_pulse_create_default_board_config_version",
        "default_board_config.create",
    ),
    _policy("okto_pulse_create_design_system", "design_system.entity.create"),
    _policy("okto_pulse_create_guideline", "guidelines.create"),
    _policy("okto_pulse_create_guideline_revision", "guidelines.revisions.create"),
    _policy("okto_pulse_create_ideation", "ideation.entity.create"),
    _policy(
        "okto_pulse_create_implementation_target",
        "code_traceability.target.suggest",
    ),
    _policy("okto_pulse_create_refinement", "refinement.entity.create"),
    _policy("okto_pulse_create_spec", "spec.entity.create"),
    _policy("okto_pulse_create_sprint", "sprint.entity.create"),
    _policy("okto_pulse_create_story", "story.entity.create"),
    _policy("okto_pulse_create_topic", "topic.entity.create"),
    _policy(
        "okto_pulse_deactivate_default_board_config_version",
        "default_board_config.deactivate",
    ),
    _policy(
        "okto_pulse_delete_architecture_design",
        "ideation.architecture.delete",
        "refinement.architecture.delete",
        "spec.architecture.delete",
        "card.architecture.delete",
    ),
    _policy("okto_pulse_delete_attachment", "card.attachments.delete"),
    _policy("okto_pulse_delete_card", "card.entity.delete"),
    _policy("okto_pulse_delete_card_knowledge", "card.copy_from_spec.knowledge"),
    _policy("okto_pulse_delete_comment", "card.comments.delete"),
    _policy("okto_pulse_delete_design_system", "design_system.entity.delete"),
    _policy("okto_pulse_delete_guideline", "guidelines.delete"),
    _policy("okto_pulse_delete_ideation", "ideation.entity.delete"),
    _policy("okto_pulse_delete_ideation_knowledge", "ideation.knowledge.delete"),
    _policy("okto_pulse_delete_ideation_question", "ideation.qa.delete"),
    _policy(
        "okto_pulse_delete_question",
        "ideation.qa.delete",
        "refinement.qa.delete",
        "spec.qa.delete",
        "sprint.qa.delete",
        "card.qa.delete",
    ),
    _policy("okto_pulse_delete_refinement", "refinement.entity.delete"),
    _policy("okto_pulse_delete_refinement_knowledge", "refinement.knowledge.delete"),
    _policy("okto_pulse_delete_refinement_question", "refinement.qa.delete"),
    _policy(
        "okto_pulse_delete_screen_mockup",
        "story.mockups.delete",
        "ideation.mockups.delete",
        "refinement.mockups.delete",
        "spec.mockups.delete",
        "card.mockups.delete",
    ),
    _policy("okto_pulse_delete_spec", "spec.entity.delete"),
    _policy("okto_pulse_delete_spec_evaluation", "spec.evaluations.delete"),
    _policy("okto_pulse_delete_spec_knowledge", "spec.knowledge.delete"),
    _policy("okto_pulse_delete_spec_question", "spec.qa.delete"),
    _policy("okto_pulse_delete_sprint_evaluation", "sprint.evaluations.delete"),
    _policy("okto_pulse_delete_sprint_question", "sprint.qa.delete"),
    _policy("okto_pulse_delete_test_scenario", "spec.tests.delete"),
    _policy("okto_pulse_delete_topic", "topic.entity.delete"),
    _policy("okto_pulse_derive_spec_from_ideation", "ideation.specs_derive"),
    _policy("okto_pulse_derive_spec_from_refinement", "refinement.specs_derive"),
    _policy(
        "okto_pulse_drop_card_knowledge_assignments", "card.copy_from_spec.knowledge"
    ),
    _policy(
        "okto_pulse_dump_architecture_diagram",
        "ideation.architecture.render",
        "refinement.architecture.render",
        "spec.architecture.render",
        "card.architecture.render",
    ),
    _policy("okto_pulse_evaluate_ideation", "ideation.entity.evaluate"),
    _policy("okto_pulse_execute_test_scenario_evidence", "spec.tests.execute"),
    _policy("okto_pulse_get_active_default_board_config", "default_board_config.read"),
    _policy("okto_pulse_get_activity_log", "board.activity_read"),
    _policy("okto_pulse_get_allowed_transitions", "board.read"),
    _policy("okto_pulse_get_amendment_revision", "amendment.revision.read"),
    _policy("okto_pulse_get_analytics", "board.analytics_read"),
    _policy(
        "okto_pulse_get_architecture_design",
        "ideation.architecture.read",
        "refinement.architecture.read",
        "spec.architecture.read",
        "card.architecture.read",
        "ideation.architecture.render",
        "refinement.architecture.render",
        "spec.architecture.render",
        "card.architecture.render",
    ),
    _policy("okto_pulse_get_architecture_design_schema", "board.read"),
    _policy("okto_pulse_get_board", "board.read"),
    _policy(
        "okto_pulse_get_board_default_config_diff", "default_board_config.diff_read"
    ),
    _policy("okto_pulse_get_board_design_system", "design_system.board_link.read"),
    _policy("okto_pulse_get_board_guidelines", "guidelines.read"),
    _policy("okto_pulse_get_card", "card.entity.read"),
    _policy("okto_pulse_get_card_dependencies", "card.entity.read"),
    _policy("okto_pulse_get_card_knowledge", "card.entity.context_read"),
    _policy("okto_pulse_get_card_knowledge_propagation", "card.entity.context_read"),
    _policy("okto_pulse_get_checklist_binding", "spec.checklist.read"),
    _policy("okto_pulse_get_checklist_receipt", "spec.checklist.read"),
    _policy("okto_pulse_get_choice_responses", "card.comments.get_responses"),
    _policy(
        "okto_pulse_get_code_evidence",
        "code_traceability.evidence.read",
    ),
    _policy(
        "okto_pulse_get_code_investigation_receipt",
        "code_traceability.investigation.read",
    ),
    _policy(
        "okto_pulse_get_current_quality_assessment",
        "ideation.quality.read",
        "refinement.quality.read",
        "spec.quality.read",
    ),
    _policy(
        "okto_pulse_get_current_semantic_guideline_assessment",
        "guidelines.assessments.read",
    ),
    _policy("okto_pulse_get_design_system", "design_system.entity.read"),
    _policy("okto_pulse_get_guideline_impact", "guidelines.impact.preview"),
    _policy("okto_pulse_get_guideline_revision", "guidelines.revisions.read"),
    _policy("okto_pulse_get_ideation", "ideation.entity.read"),
    _policy("okto_pulse_get_ideation_context", "ideation.entity.read"),
    _policy("okto_pulse_get_ideation_history", "ideation.history_read"),
    _policy("okto_pulse_get_ideation_knowledge", "ideation.knowledge.read"),
    _policy("okto_pulse_get_ideation_snapshot", "ideation.versions_read"),
    _policy(
        "okto_pulse_get_implementation_overlaps",
        "code_traceability.overlap.read",
    ),
    _policy("okto_pulse_get_my_profile", "agent.entity.read"),
    _policy("okto_pulse_get_publish_health", "metrics.publish_health.read"),
    _policy(
        "okto_pulse_get_quality_assessment_receipt",
        "ideation.quality.read",
        "refinement.quality.read",
        "spec.quality.read",
    ),
    _policy("okto_pulse_get_refinement", "refinement.entity.read"),
    _policy("okto_pulse_get_refinement_context", "refinement.entity.read"),
    _policy("okto_pulse_get_refinement_history", "refinement.history_read"),
    _policy("okto_pulse_get_refinement_knowledge", "refinement.knowledge.read"),
    _policy("okto_pulse_get_refinement_snapshot", "refinement.versions_read"),
    _policy(
        "okto_pulse_get_requirement_lint_preflight",
        "spec.quality.read",
    ),
    _policy(
        "okto_pulse_get_resource_gate_summary",
        "ideation.entity.read",
        "refinement.entity.read",
        "spec.entity.read",
        "sprint.entity.read",
        "card.entity.read",
    ),
    _policy(
        "okto_pulse_get_semantic_guideline_assessment", "guidelines.assessments.read"
    ),
    _policy("okto_pulse_get_semantic_guideline_waiver", "guidelines.waiver.read"),
    _policy(
        "okto_pulse_get_spec",
        "spec.entity.read",
        "spec.integration_requirements.read",
        "spec.observability_requirements.read",
    ),
    _policy(
        "okto_pulse_get_spec_context",
        "spec.entity.read",
        "spec.integration_requirements.read",
        "spec.observability_requirements.read",
        "spec.checklist.read",
    ),
    _policy("okto_pulse_get_spec_evaluation", "spec.evaluations.read"),
    _policy("okto_pulse_get_spec_history", "spec.history_read"),
    _policy("okto_pulse_get_spec_knowledge", "spec.knowledge.read"),
    _policy("okto_pulse_get_sprint", "sprint.entity.read"),
    _policy("okto_pulse_get_sprint_context", "sprint.entity.read"),
    _policy("okto_pulse_get_sprint_evaluation", "sprint.evaluations.read"),
    _policy("okto_pulse_get_task_conclusions", "card.conclusion.read"),
    _policy(
        "okto_pulse_get_task_context",
        "card.entity.context_read",
        "card.validation.read",
        "spec.integration_requirements.read",
        "spec.observability_requirements.read",
    ),
    _policy(
        "okto_pulse_get_task_validation",
        "card.entity.read",
        "card.validation.read",
    ),
    _policy("okto_pulse_get_traceability_report", "spec.entity.read"),
    _policy("okto_pulse_get_unseen_summary", "board.mentions_read"),
    _policy(
        "okto_pulse_import_excalidraw_architecture_diagram",
        "ideation.architecture.import",
        "refinement.architecture.import",
        "spec.architecture.import",
        "card.architecture.import",
    ),
    _policy("okto_pulse_kg_abort_consolidation", "kg.session.abort"),
    _policy("okto_pulse_kg_add_edge_candidate", "kg.session.add_edge"),
    _policy("okto_pulse_kg_add_node_candidate", "kg.session.add_node"),
    _policy("okto_pulse_kg_begin_consolidation", "kg.session.begin"),
    _policy("okto_pulse_kg_canonical_debt_list", "kg.operations.integrity.read"),
    _policy(
        "okto_pulse_kg_canonical_partition_integrity_list",
        "kg.operations.integrity.read",
    ),
    _policy("okto_pulse_kg_commit_consolidation", "kg.session.commit"),
    _policy("okto_pulse_kg_connectivity_dlq_diagnose", "kg.operations.queue.read"),
    _policy(
        "okto_pulse_kg_connectivity_dlq_reprocess",
        "kg.operations.queue.reprocess",
    ),
    _policy("okto_pulse_kg_connectivity_dlq_verify", "kg.operations.queue.read"),
    _policy("okto_pulse_kg_dead_letter_list", "kg.operations.queue.read"),
    _policy("okto_pulse_kg_dead_letter_reprocess", "kg.operations.queue.reprocess"),
    _policy("okto_pulse_kg_digest_layer_mismatch_list", "kg.operations.integrity.read"),
    _policy(
        "okto_pulse_kg_digest_layer_reconcile",
        "kg.operations.integrity.reconcile",
    ),
    _policy(
        "okto_pulse_kg_evaluate_bug_cognitive_closure",
        "kg.operations.cognitive.read",
    ),
    _policy(
        "okto_pulse_kg_evaluate_cognitive_readiness",
        "kg.operations.cognitive.read",
    ),
    _policy("okto_pulse_kg_explain_constraint", "kg.query.constraint_explain"),
    _policy("okto_pulse_kg_export_jsonld", "kg.query.global"),
    _policy("okto_pulse_kg_find_contradictions", "kg.query.contradictions"),
    _policy("okto_pulse_kg_find_similar_decisions", "kg.query.similar_decisions"),
    _policy("okto_pulse_kg_get_decision_history", "kg.query.decision_history"),
    _policy("okto_pulse_kg_get_learning_from_bugs", "kg.query.learning_from_bugs"),
    _policy("okto_pulse_kg_get_related_context", "kg.query.related_context"),
    _policy("okto_pulse_kg_get_similar_nodes", "kg.session.get_similar"),
    _policy("okto_pulse_kg_get_supersedence_chain", "kg.query.supersedence_chain"),
    _policy(
        "okto_pulse_kg_global_discovery_recovery_cancel",
        "kg.operations.global_recovery.cancel",
    ),
    _policy(
        "okto_pulse_kg_global_discovery_recovery_confirm",
        "kg.operations.global_recovery.confirm",
    ),
    _policy(
        "okto_pulse_kg_global_discovery_recovery_preflight",
        "kg.operations.global_recovery.preflight",
    ),
    _policy(
        "okto_pulse_kg_global_discovery_recovery_resume",
        "kg.operations.global_recovery.resume",
    ),
    _policy(
        "okto_pulse_kg_global_discovery_recovery_run",
        "kg.operations.global_recovery.run",
    ),
    _policy(
        "okto_pulse_kg_global_discovery_recovery_status",
        "kg.operations.global_recovery.read",
    ),
    _policy(
        "okto_pulse_kg_global_outbox_dead_letter_list",
        "kg.operations.global_outbox.read",
    ),
    _policy(
        "okto_pulse_kg_global_outbox_dead_letter_reprocess",
        "kg.operations.global_outbox.reprocess",
    ),
    _policy(
        "okto_pulse_kg_global_outbox_dead_letter_verify",
        "kg.operations.global_outbox.verify",
    ),
    _policy("okto_pulse_kg_health", "kg.operations.health.read"),
    _policy("okto_pulse_kg_health_readiness", "kg.operations.health.read"),
    _policy("okto_pulse_kg_list_alternatives", "kg.query.alternatives"),
    _policy("okto_pulse_kg_list_cognitive_dlq", "kg.operations.cognitive.read"),
    _policy(
        "okto_pulse_kg_list_cognitive_pending_items",
        "kg.operations.cognitive.read",
    ),
    _policy(
        "okto_pulse_kg_list_cognitive_readiness_items",
        "kg.operations.cognitive.read",
    ),
    _policy("okto_pulse_kg_migrate_schema", "kg.operations.schema.migrate"),
    _policy(
        "okto_pulse_kg_originates_from_contract_audit",
        "kg.operations.audit.read",
    ),
    _policy("okto_pulse_kg_orphan_backfill", "kg.operations.integrity.backfill"),
    _policy("okto_pulse_kg_orphan_report", "kg.operations.integrity.read"),
    _policy("okto_pulse_kg_propose_reconciliation", "kg.session.propose"),
    _policy("okto_pulse_kg_provenance_drift", "kg.operations.audit.read"),
    _policy(
        "okto_pulse_kg_quarantine_restore",
        "kg.operations.quarantine.restore",
    ),
    _policy("okto_pulse_kg_query_cypher", "kg.power.cypher"),
    _policy("okto_pulse_kg_query_global", "kg.query.global"),
    _policy("okto_pulse_kg_query_natural", "kg.power.natural"),
    _policy("okto_pulse_kg_query_reflective", "kg.power.natural"),
    _policy("okto_pulse_kg_queue_drilldown", "kg.operations.queue.read"),
    _policy("okto_pulse_kg_rebuild_confirm", "kg.operations.rebuild.confirm"),
    _policy("okto_pulse_kg_rebuild_preflight", "kg.operations.rebuild.preflight"),
    _policy("okto_pulse_kg_rebuild_run", "kg.operations.rebuild.run"),
    _policy("okto_pulse_kg_schema_info", "kg.power.schema_info"),
    _policy(
        "okto_pulse_kg_stale_canonical_parity_list",
        "kg.operations.integrity.read",
    ),
    _policy("okto_pulse_kg_takedown_status", "kg.operations.audit.read"),
    _policy("okto_pulse_kg_tick_run_now", "kg.operations.tick.run"),
    _policy("okto_pulse_kg_update_cognitive_pending_item", "kg.session.commit"),
    _policy("okto_pulse_kg_verify_grounding", "board.read"),
    _policy("okto_pulse_link_board_design_system", "design_system.board_link.create"),
    _policy(
        "okto_pulse_link_code_evidence",
        "code_traceability.spec_link.create",
    ),
    _policy("okto_pulse_link_guideline_to_board", "guidelines.link"),
    _policy("okto_pulse_link_story_to_ideation", "story.links.ideation"),
    _policy(
        "okto_pulse_link_task",
        "card.entity.link_spec",
        "card.link_to.scenario",
        "spec.structured_entity.functional_requirement.link_task",
        "card.link_to.rule",
        "spec.structured_entity.decision.link_task",
        "card.link_to.tr",
        "card.link_to.contract",
        "spec.integration_requirements.link_task",
        "card.link_to.ir",
        "spec.observability_requirements.link_task",
        "card.link_to.or",
    ),
    _policy("okto_pulse_list_agents", "agent.entity.read"),
    _policy("okto_pulse_list_amendment_revisions", "amendment.revision.read"),
    _policy("okto_pulse_list_api_contracts", "spec.contracts.read"),
    _policy(
        "okto_pulse_list_architecture_designs",
        "ideation.architecture.read",
        "refinement.architecture.read",
        "spec.architecture.read",
        "card.architecture.read",
        "ideation.architecture.render",
        "refinement.architecture.render",
        "spec.architecture.render",
        "card.architecture.render",
    ),
    _policy(
        "okto_pulse_list_architecture_propagation_legacy",
        "spec.architecture.read",
    ),
    _policy("okto_pulse_list_attachments", "card.attachments.read"),
    _policy("okto_pulse_list_blockers", "card.entity.read"),
    _policy("okto_pulse_list_board_members", "board.share.read"),
    _policy("okto_pulse_list_business_rules", "spec.rules.read"),
    _policy("okto_pulse_list_by_board", "board.read"),
    _policy("okto_pulse_list_cards_by_status", "card.entity.read"),
    _policy(
        "okto_pulse_list_code_evidence",
        "code_traceability.evidence.read",
    ),
    _policy("okto_pulse_list_comments", "card.comments.read"),
    _policy(
        "okto_pulse_list_default_board_config_versions",
        "default_board_config.read",
    ),
    _policy(
        "okto_pulse_list_default_guideline_candidates",
        "default_board_config.candidates_read",
    ),
    _policy("okto_pulse_list_design_systems", "design_system.entity.read"),
    _policy("okto_pulse_list_guideline_impact_items", "guidelines.impact.preview"),
    _policy("okto_pulse_list_guideline_revisions", "guidelines.revisions.read"),
    _policy("okto_pulse_list_guidelines", "guidelines.read"),
    _policy(
        "okto_pulse_list_implementation_targets",
        "code_traceability.target.read",
    ),
    _policy(
        "okto_pulse_list_integration_requirements",
        "spec.integration_requirements.read",
    ),
    _policy(
        "okto_pulse_list_knowledge",
        "ideation.knowledge.read",
        "refinement.knowledge.read",
        "spec.knowledge.read",
        "card.entity.context_read",
    ),
    _policy("okto_pulse_list_my_boards", "board.read"),
    _policy("okto_pulse_list_my_mentions", "board.mentions_read"),
    _policy(
        "okto_pulse_list_observability_requirements",
        "spec.observability_requirements.read",
    ),
    _policy(
        "okto_pulse_list_qa",
        "ideation.qa.read",
        "refinement.qa.read",
        "spec.qa.read",
        "sprint.qa.read",
        "card.qa.read",
    ),
    _policy(
        "okto_pulse_list_quality_assessments",
        "ideation.quality.read",
        "refinement.quality.read",
        "spec.quality.read",
    ),
    _policy(
        "okto_pulse_list_quality_findings",
        "ideation.quality.read",
        "refinement.quality.read",
        "spec.quality.read",
    ),
    _policy(
        "okto_pulse_list_research_decisions",
        "refinement.research_decisions.read",
    ),
    _policy(
        "okto_pulse_list_screen_mockups",
        "story.mockups.read",
        "ideation.mockups.read",
        "refinement.mockups.read",
        "spec.mockups.read",
        "card.mockups.read",
    ),
    _policy(
        "okto_pulse_list_semantic_guideline_assessments",
        "guidelines.assessments.read",
    ),
    _policy(
        "okto_pulse_list_semantic_guideline_findings",
        "guidelines.assessments.read",
    ),
    _policy(
        "okto_pulse_list_semantic_guideline_waiver_events",
        "guidelines.waiver.read",
    ),
    _policy(
        "okto_pulse_list_semantic_guideline_waivers",
        "guidelines.waiver.read",
    ),
    _policy(
        "okto_pulse_list_snapshots",
        "ideation.versions_read",
        "refinement.versions_read",
    ),
    _policy("okto_pulse_list_spec_dependencies", "spec.entity.read"),
    _policy("okto_pulse_list_spec_evaluations", "spec.evaluations.read"),
    _policy("okto_pulse_list_spec_validations", "spec.validation.read"),
    _policy("okto_pulse_list_sprint_evaluations", "sprint.evaluations.read"),
    _policy(
        "okto_pulse_list_task_validations",
        "card.entity.read",
        "card.validation.read",
    ),
    _policy("okto_pulse_list_test_scenarios", "spec.tests.read"),
    _policy("okto_pulse_mark_as_seen", "board.mentions_mark_seen"),
    _policy(
        "okto_pulse_mark_code_traceability_not_applicable",
        "code_traceability.waiver.create",
    ),
    _policy(
        "okto_pulse_mark_resource_not_applicable",
        "ideation.entity.edit_fields",
        "refinement.entity.edit_fields",
        "spec.entity.edit_coverage_flags",
        "sprint.entity.edit_coverage_flags",
        "card.entity.edit_fields",
    ),
    _policy("okto_pulse_merge_topics", "topic.entity.merge"),
    _policy(
        "okto_pulse_migrate_spec_decisions",
        "spec.structured_entity.decision.create",
    ),
    _policy("okto_pulse_move_card", *transition_permission_flags("card")),
    _policy("okto_pulse_move_ideation", *transition_permission_flags("ideation")),
    _policy("okto_pulse_move_refinement", *transition_permission_flags("refinement")),
    _policy("okto_pulse_move_spec", *transition_permission_flags("spec")),
    _policy("okto_pulse_move_sprint", *transition_permission_flags("sprint")),
    _policy("okto_pulse_move_story", *transition_permission_flags("story")),
    _policy("okto_pulse_preview_guideline_impact", "guidelines.impact.preview"),
    _policy(
        "okto_pulse_record_ambiguity_assessment",
        "ideation.quality.assess",
        "refinement.quality.assess",
    ),
    _policy(
        "okto_pulse_record_requirement_lint",
        "spec.quality.assess",
    ),
    _policy(
        "okto_pulse_record_semantic_guideline_assessment",
        "guidelines.assessments.record",
    ),
    _policy(
        "okto_pulse_record_semantic_guideline_assessment_v2",
        "guidelines.assessments.record",
    ),
    _policy(
        "okto_pulse_refresh_card_knowledge_assignments",
        "card.copy_from_spec.knowledge",
    ),
    _policy("okto_pulse_remove_api_contract", "spec.contracts.delete"),
    _policy("okto_pulse_remove_business_rule", "spec.rules.delete"),
    _policy("okto_pulse_remove_card_dependency", "card.entity.manage_dependencies"),
    _policy(
        "okto_pulse_remove_decision",
        "spec.structured_entity.decision.revoke",
    ),
    _policy(
        "okto_pulse_remove_spec_dependency",
        "spec.entity.manage_dependencies",
        "spec.entity.read",
    ),
    _policy(
        "okto_pulse_remove_spec_entity",
        "spec.rules.delete",
        "spec.contracts.delete",
        "spec.structured_entity.decision.revoke",
    ),
    _policy(
        "okto_pulse_replace_card_knowledge_assignments",
        "card.copy_from_spec.knowledge",
    ),
    _policy(
        "okto_pulse_request_semantic_guideline_waiver", "guidelines.waiver.request"
    ),
    _policy("okto_pulse_resolve_bug_regression_scenarios", "board.read"),
    _policy("okto_pulse_respond_to_choice", "card.comments.respond_choice"),
    _policy("okto_pulse_restore_story", "story.entity.restore"),
    _policy("okto_pulse_restore_topic", "topic.entity.restore"),
    _policy(
        "okto_pulse_restore_tree",
        "ideation.entity.restore",
        "refinement.entity.restore",
        "spec.entity.restore",
    ),
    _policy("okto_pulse_retire_guideline", "guidelines.revisions.retire"),
    _policy(
        "okto_pulse_revalidate_semantic_guideline_waiver",
        "guidelines.waiver.revalidate",
    ),
    _policy(
        "okto_pulse_review_semantic_guideline_waiver",
        "guidelines.waiver.review",
    ),
    _policy(
        "okto_pulse_revoke_semantic_guideline_waiver",
        "guidelines.waiver.revoke",
    ),
    _policy(
        "okto_pulse_set_code_evidence_disposition",
        "code_traceability.spec_link.set_disposition",
    ),
    _policy(
        "okto_pulse_set_default_design_system",
        "default_board_config.set_design_system",
    ),
    _policy("okto_pulse_start_checklist_execution", "spec.checklist.execute"),
    _policy(
        "okto_pulse_start_code_investigation",
        "code_traceability.investigation.start",
    ),
    _policy("okto_pulse_submit_checklist_execution", "spec.checklist.execute"),
    _policy(
        "okto_pulse_submit_code_evidence",
        "code_traceability.evidence.submit",
    ),
    _policy(
        "okto_pulse_submit_code_investigation_receipt",
        "code_traceability.investigation.receipt_submit",
    ),
    _policy(
        "okto_pulse_submit_implementation_target_execution_receipt",
        "code_traceability.target.execution_submit",
    ),
    _policy(
        "okto_pulse_submit_implementation_target_resolution",
        "code_traceability.target.resolution_submit",
    ),
    _policy("okto_pulse_submit_spec_evaluation", "spec.evaluations.submit"),
    _policy("okto_pulse_submit_spec_validation", "spec.validation.submit"),
    _policy("okto_pulse_submit_sprint_evaluation", "sprint.evaluations.submit"),
    _policy(
        "okto_pulse_submit_task_validation",
        "card.validation.submit",
        "card.validation.read",
    ),
    _policy("okto_pulse_suggest_sprints", "board.read"),
    _policy(
        "okto_pulse_supersede_code_evidence",
        "code_traceability.evidence.supersede",
    ),
    _policy(
        "okto_pulse_transition_amendment_revision",
        "amendment.revision.transition",
    ),
    _policy(
        "okto_pulse_unlink_board_design_system",
        "design_system.board_link.delete",
    ),
    _policy(
        "okto_pulse_unlink_code_evidence",
        "code_traceability.spec_link.delete",
    ),
    _policy("okto_pulse_unlink_guideline_from_board", "guidelines.unlink"),
    _policy("okto_pulse_update_api_contract", "spec.contracts.edit"),
    _policy(
        "okto_pulse_update_architecture_design",
        "ideation.architecture.edit",
        "refinement.architecture.edit",
        "spec.architecture.edit",
        "card.architecture.edit",
    ),
    _policy("okto_pulse_update_board_guideline_priority", "guidelines.link"),
    _policy("okto_pulse_update_business_rule", "spec.rules.edit"),
    _policy("okto_pulse_update_card", "card.entity.edit_fields"),
    _policy("okto_pulse_update_card_knowledge", "card.copy_from_spec.knowledge"),
    _policy("okto_pulse_update_comment", "card.comments.edit"),
    _policy(
        "okto_pulse_update_decision",
        "spec.structured_entity.decision.update",
        "spec.structured_entity.decision.revoke",
        "spec.structured_entity.decision.supersede",
        "spec.structured_entity.decision.restore",
    ),
    _policy(
        "okto_pulse_update_default_guideline_refs",
        "default_board_config.guidelines.edit",
    ),
    _policy("okto_pulse_update_design_system", "design_system.entity.edit"),
    _policy("okto_pulse_update_guideline", "guidelines.edit"),
    _policy(
        "okto_pulse_update_ideation",
        "ideation.entity.edit_fields",
        "ideation.entity.assign",
        "ideation.entity.label",
    ),
    _policy(
        "okto_pulse_update_implementation_target",
        "code_traceability.target.edit",
    ),
    _policy("okto_pulse_update_my_profile", "profile.update"),
    _policy(
        "okto_pulse_update_refinement",
        "refinement.entity.edit_fields",
        "refinement.entity.assign",
        "refinement.entity.label",
    ),
    _policy(
        "okto_pulse_update_screen_mockup",
        "story.mockups.edit",
        "ideation.mockups.edit",
        "refinement.mockups.edit",
        "spec.mockups.edit",
        "card.mockups.edit",
    ),
    _policy(
        "okto_pulse_update_spec",
        "spec.entity.edit_fields",
        "spec.entity.assign",
        "spec.entity.label",
    ),
    _policy("okto_pulse_update_spec_api_contract", "spec.contracts.edit"),
    _policy("okto_pulse_update_spec_entity", "spec.entity.edit_fields"),
    _policy(
        "okto_pulse_update_sprint",
        "sprint.entity.edit_fields",
        "sprint.entity.edit_coverage_flags",
        "sprint.entity.assign",
        "sprint.entity.label",
    ),
    _policy(
        "okto_pulse_update_story",
        "story.entity.edit_fields",
        "story.entity.assign",
        "story.entity.label",
    ),
    _policy("okto_pulse_update_test_scenario", "spec.tests.edit"),
    _policy(
        "okto_pulse_update_test_scenario_status",
        "spec.tests.execute",
        *transition_permission_flags("test_scenario"),
    ),
    _policy("okto_pulse_update_topic", "topic.entity.edit_fields"),
    _policy("okto_pulse_upload_attachment", "card.attachments.upload"),
    _policy(
        "okto_pulse_validate_architecture_design_payload",
        "ideation.architecture.create",
        "refinement.architecture.create",
        "spec.architecture.create",
        "card.architecture.create",
        "ideation.architecture.edit",
        "refinement.architecture.edit",
        "spec.architecture.edit",
        "card.architecture.edit",
    ),
)


HUMAN_ONLY_MCP_TOOL_EXEMPTIONS: tuple[HumanOnlyToolExemption, ...] = (
    HumanOnlyToolExemption(
        "okto_pulse_kg_clear_cognitive_skip",
        "The agent-facing handler always fails closed; clearing an audited cognitive skip requires a human control plane.",
    ),
    HumanOnlyToolExemption(
        "okto_pulse_kg_record_cognitive_skip",
        "The agent-facing handler always fails closed; recording skip/no-action debt requires a human decision.",
    ),
    HumanOnlyToolExemption(
        "okto_pulse_set_ideation_ambiguity_gate_skip",
        "The agent-facing handler always fails closed; bypassing the ambiguity gate is reserved for a human decision.",
    ),
)


_MCP_POLICY_TOOL_NAMES = frozenset(
    policy.tool_name for policy in MCP_TOOL_PERMISSION_POLICIES
)
_UNBOUND_READER_TOOL_NAMES = MCP_READER_TOOL_NAMES - _MCP_POLICY_TOOL_NAMES
if _UNBOUND_READER_TOOL_NAMES:
    raise RuntimeError(
        "MCP reader admission allowlist references tools without a permission "
        "policy: " + ",".join(sorted(_UNBOUND_READER_TOOL_NAMES))
    )


MCP_TOOL_ADMISSION_CLASSES = MappingProxyType(
    {
        **{
            policy.tool_name: policy.admission_class
            for policy in MCP_TOOL_PERMISSION_POLICIES
        },
        **{
            exemption.tool_name: exemption.admission_class
            for exemption in HUMAN_ONLY_MCP_TOOL_EXEMPTIONS
        },
    }
)


def resolve_mcp_tool_admission_class(tool_name: object) -> McpAdmissionClass:
    """Resolve exact Core metadata; unknown tools fail closed as writers."""

    return MCP_TOOL_ADMISSION_CLASSES.get(
        str(tool_name or ""),
        McpAdmissionClass.WRITER,
    )


@dataclass(frozen=True)
class McpPermissionRegistryReport:
    """Deterministic result of comparing the live catalog with the manifest."""

    live_tools: tuple[str, ...]
    policy_tools: tuple[str, ...]
    exempt_tools: tuple[str, ...]
    new_tools: tuple[str, ...]
    missing_tools: tuple[str, ...]
    duplicate_live_tools: tuple[str, ...]
    duplicate_policy_tools: tuple[str, ...]
    duplicate_exemption_tools: tuple[str, ...]
    conflicting_tools: tuple[str, ...]
    invalid_policy_flags: tuple[tuple[str, str], ...]
    invalid_policy_records: tuple[str, ...]
    invalid_exemption_records: tuple[str, ...]
    exemption_count: int
    exemption_limit: int

    @property
    def exemption_limit_exceeded(self) -> bool:
        return self.exemption_count > self.exemption_limit

    @property
    def is_valid(self) -> bool:
        return not any(
            (
                self.new_tools,
                self.missing_tools,
                self.duplicate_live_tools,
                self.duplicate_policy_tools,
                self.duplicate_exemption_tools,
                self.conflicting_tools,
                self.invalid_policy_flags,
                self.invalid_policy_records,
                self.invalid_exemption_records,
                self.exemption_limit_exceeded,
            )
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "valid": self.is_valid,
            "counts": {
                "live_tools": len(self.live_tools),
                "policies": len(self.policy_tools),
                "human_only_exemptions": self.exemption_count,
                "human_only_exemption_limit": self.exemption_limit,
            },
            "new_tools": list(self.new_tools),
            "missing_tools": list(self.missing_tools),
            "duplicate_live_tools": list(self.duplicate_live_tools),
            "duplicate_policy_tools": list(self.duplicate_policy_tools),
            "duplicate_exemption_tools": list(self.duplicate_exemption_tools),
            "conflicting_tools": list(self.conflicting_tools),
            "invalid_policy_flags": [list(item) for item in self.invalid_policy_flags],
            "invalid_policy_records": list(self.invalid_policy_records),
            "invalid_exemption_records": list(self.invalid_exemption_records),
            "exemption_limit_exceeded": self.exemption_limit_exceeded,
        }

    def render(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))

    def assert_valid(self) -> None:
        if not self.is_valid:
            raise McpPermissionRegistryError(self)


class McpPermissionRegistryError(RuntimeError):
    """Raised when strict validation finds catalog/permission drift."""

    def __init__(self, report: McpPermissionRegistryReport) -> None:
        self.report = report
        super().__init__(f"MCP permission registry drift: {report.render()}")


def _duplicates(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(value for value, count in Counter(values).items() if count > 1))


def build_mcp_permission_registry_report(
    tool_names: Iterable[str],
    *,
    all_flags: Collection[str],
    policies: Sequence[McpToolPermissionPolicy] = MCP_TOOL_PERMISSION_POLICIES,
    exemptions: Sequence[HumanOnlyToolExemption] = HUMAN_ONLY_MCP_TOOL_EXEMPTIONS,
    exemption_limit: int = MAX_HUMAN_ONLY_TOOL_EXEMPTIONS,
) -> McpPermissionRegistryReport:
    """Compare exact live names with exact policy/exemption records.

    This function is pure: it neither logs nor raises.  Call ``assert_valid``
    on its result at strict boundaries such as CI.
    """

    live = tuple(tool_names)
    policy_names = tuple(policy.tool_name for policy in policies)
    exemption_names = tuple(exemption.tool_name for exemption in exemptions)
    live_set = set(live)
    policy_set = set(policy_names)
    exemption_set = set(exemption_names)
    inventory = policy_set | exemption_set
    known_flags = set(all_flags)

    invalid_policy_records: list[str] = []
    invalid_policy_flags: list[tuple[str, str]] = []
    for index, policy in enumerate(policies):
        if (
            not policy.tool_name
            or policy.tool_name != policy.tool_name.strip()
            or not policy.permission_flags
            or len(policy.permission_flags) != len(set(policy.permission_flags))
        ):
            invalid_policy_records.append(f"policy[{index}]")
        for flag in policy.permission_flags:
            if not flag or flag != flag.strip() or flag not in known_flags:
                invalid_policy_flags.append((policy.tool_name, flag))

    invalid_exemption_records: list[str] = []
    for index, exemption in enumerate(exemptions):
        if (
            not exemption.tool_name
            or exemption.tool_name != exemption.tool_name.strip()
            or not exemption.reason.strip()
        ):
            invalid_exemption_records.append(f"exemption[{index}]")

    return McpPermissionRegistryReport(
        live_tools=tuple(sorted(live)),
        policy_tools=tuple(sorted(policy_names)),
        exempt_tools=tuple(sorted(exemption_names)),
        new_tools=tuple(sorted(live_set - inventory)),
        missing_tools=tuple(sorted(inventory - live_set)),
        duplicate_live_tools=_duplicates(live),
        duplicate_policy_tools=_duplicates(policy_names),
        duplicate_exemption_tools=_duplicates(exemption_names),
        conflicting_tools=tuple(sorted(policy_set & exemption_set)),
        invalid_policy_flags=tuple(sorted(invalid_policy_flags)),
        invalid_policy_records=tuple(invalid_policy_records),
        invalid_exemption_records=tuple(invalid_exemption_records),
        exemption_count=len(exemptions),
        exemption_limit=exemption_limit,
    )


__all__ = [
    "HUMAN_ONLY_MCP_TOOL_EXEMPTIONS",
    "MAX_HUMAN_ONLY_TOOL_EXEMPTIONS",
    "MCP_READER_TOOL_NAMES",
    "MCP_TOOL_ADMISSION_CLASSES",
    "MCP_TOOL_PERMISSION_POLICIES",
    "HumanOnlyToolExemption",
    "McpAdmissionClass",
    "McpPermissionRegistryError",
    "McpPermissionRegistryReport",
    "McpToolPermissionPolicy",
    "build_mcp_permission_registry_report",
    "resolve_mcp_tool_admission_class",
]
