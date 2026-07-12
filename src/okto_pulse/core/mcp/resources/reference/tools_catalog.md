---
version: "1.0"
generated_by: okto_pulse.core.mcp.tools_catalog_generator
---
# Okto Pulse — MCP Tools Catalog

GENERATED FILE — do not edit by hand. Regenerate with
`python -m okto_pulse.core.mcp.tools_catalog_generator`
(guarded by tests/test_mcp_tools_catalog_drift.py).

Tool schemas arrive lazily via MCP `tools/list`. Each section links its
long-form docs (args/returns/examples): `okto-pulse://reference/tool-docs/{family}`.
Required filters for the consolidated `list_*` tools:
`okto-pulse://reference/list_tools`.

## KG — Consolidation (write path) — docs: `okto-pulse://reference/tool-docs/kg`
`okto_pulse_kg_abort_consolidation`, `okto_pulse_kg_add_edge_candidate`, `okto_pulse_kg_add_node_candidate`, `okto_pulse_kg_begin_consolidation`, `okto_pulse_kg_commit_consolidation`, `okto_pulse_kg_propose_reconciliation`

## KG — Query (Recall) — docs: `okto-pulse://reference/tool-docs/kg`
`okto_pulse_kg_explain_constraint`, `okto_pulse_kg_find_contradictions`, `okto_pulse_kg_find_similar_decisions`, `okto_pulse_kg_get_decision_history`, `okto_pulse_kg_get_learning_from_bugs`, `okto_pulse_kg_get_related_context`, `okto_pulse_kg_get_similar_nodes`, `okto_pulse_kg_get_supersedence_chain`, `okto_pulse_kg_list_alternatives`, `okto_pulse_kg_query_global`

## KG — Query (Power) — docs: `okto-pulse://reference/tool-docs/kg`
`okto_pulse_kg_export_jsonld`, `okto_pulse_kg_provenance_drift`, `okto_pulse_kg_query_cypher`, `okto_pulse_kg_query_natural`, `okto_pulse_kg_query_reflective`, `okto_pulse_kg_schema_info`, `okto_pulse_kg_verify_grounding`

## KG — Cognitive readiness & closeout — docs: `okto-pulse://reference/tool-docs/kg`
`okto_pulse_kg_clear_cognitive_skip`, `okto_pulse_kg_evaluate_bug_cognitive_closure`, `okto_pulse_kg_evaluate_cognitive_readiness`, `okto_pulse_kg_list_cognitive_dlq`, `okto_pulse_kg_list_cognitive_pending_items`, `okto_pulse_kg_list_cognitive_readiness_items`, `okto_pulse_kg_record_cognitive_skip`, `okto_pulse_kg_update_cognitive_pending_item`

## KG — Rebuild & recovery — docs: `okto-pulse://reference/tool-docs/kg`
`okto_pulse_kg_quarantine_restore`, `okto_pulse_kg_rebuild_confirm`, `okto_pulse_kg_rebuild_preflight`, `okto_pulse_kg_rebuild_run`

## KG — Operational & health — docs: `okto-pulse://reference/tool-docs/kg`
`okto_pulse_kg_canonical_debt_list`, `okto_pulse_kg_canonical_partition_integrity_list`, `okto_pulse_kg_connectivity_dlq_diagnose`, `okto_pulse_kg_connectivity_dlq_reprocess`, `okto_pulse_kg_connectivity_dlq_verify`, `okto_pulse_kg_dead_letter_list`, `okto_pulse_kg_dead_letter_reprocess`, `okto_pulse_kg_digest_layer_mismatch_list`, `okto_pulse_kg_health`, `okto_pulse_kg_health_readiness`, `okto_pulse_kg_migrate_schema`, `okto_pulse_kg_originates_from_contract_audit`, `okto_pulse_kg_orphan_backfill`, `okto_pulse_kg_orphan_report`, `okto_pulse_kg_queue_drilldown`, `okto_pulse_kg_stale_canonical_parity_list`, `okto_pulse_kg_tick_run_now`

## Session & Agents — docs: `okto-pulse://reference/tool-docs/agent`
`okto_pulse_get_my_profile`, `okto_pulse_get_unseen_summary`, `okto_pulse_list_agents`, `okto_pulse_list_my_boards`, `okto_pulse_list_my_mentions`, `okto_pulse_mark_as_seen`, `okto_pulse_update_my_profile`

## Boards & Governance — docs: `okto-pulse://reference/tool-docs/board`
`okto_pulse_clear_resource_not_applicable`, `okto_pulse_get_activity_log`, `okto_pulse_get_allowed_transitions`, `okto_pulse_get_analytics`, `okto_pulse_get_board`, `okto_pulse_get_publish_health`, `okto_pulse_get_resource_gate_summary`, `okto_pulse_list_board_members`, `okto_pulse_mark_resource_not_applicable`

## Default Board Config — docs: `okto-pulse://reference/tool-docs/board`
`okto_pulse_activate_default_board_config_version`, `okto_pulse_create_default_board_config_version`, `okto_pulse_deactivate_default_board_config_version`, `okto_pulse_get_active_default_board_config`, `okto_pulse_get_board_default_config_diff`, `okto_pulse_list_default_board_config_versions`

## Design Systems — docs: `okto-pulse://reference/tool-docs/board`
`okto_pulse_create_design_system`, `okto_pulse_delete_design_system`, `okto_pulse_get_board_design_system`, `okto_pulse_get_design_system`, `okto_pulse_link_board_design_system`, `okto_pulse_list_design_systems`, `okto_pulse_set_default_design_system`, `okto_pulse_unlink_board_design_system`, `okto_pulse_update_design_system`

## Guidelines — docs: `okto-pulse://reference/tool-docs/guideline`
`okto_pulse_create_guideline`, `okto_pulse_delete_guideline`, `okto_pulse_get_board_guidelines`, `okto_pulse_link_guideline_to_board`, `okto_pulse_list_default_guideline_candidates`, `okto_pulse_list_guidelines`, `okto_pulse_unlink_guideline_from_board`, `okto_pulse_update_board_guideline_priority`, `okto_pulse_update_default_guideline_refs`, `okto_pulse_update_guideline`

## Stories & Topics — docs: `okto-pulse://reference/tool-docs/story`
`okto_pulse_archive_story`, `okto_pulse_archive_topic`, `okto_pulse_convert_stories_to_ideation`, `okto_pulse_create_story`, `okto_pulse_create_topic`, `okto_pulse_delete_topic`, `okto_pulse_get_ideation_history`, `okto_pulse_get_refinement_history`, `okto_pulse_get_spec_history`, `okto_pulse_merge_topics`, `okto_pulse_move_story`, `okto_pulse_restore_story`, `okto_pulse_restore_topic`, `okto_pulse_update_story`, `okto_pulse_update_topic`

## Ideations — docs: `okto-pulse://reference/tool-docs/ideation`
`okto_pulse_add_ideation_knowledge`, `okto_pulse_answer_ideation_question`, `okto_pulse_ask_ideation_choice_question`, `okto_pulse_ask_ideation_question`, `okto_pulse_create_ideation`, `okto_pulse_delete_ideation`, `okto_pulse_delete_ideation_knowledge`, `okto_pulse_delete_ideation_question`, `okto_pulse_derive_spec_from_ideation`, `okto_pulse_evaluate_ideation`, `okto_pulse_get_ideation`, `okto_pulse_get_ideation_context`, `okto_pulse_get_ideation_knowledge`, `okto_pulse_get_ideation_snapshot`, `okto_pulse_link_story_to_ideation`, `okto_pulse_move_ideation`, `okto_pulse_set_ideation_ambiguity_gate_skip`, `okto_pulse_update_ideation`

## Refinements — docs: `okto-pulse://reference/tool-docs/refinement`
`okto_pulse_add_refinement_knowledge`, `okto_pulse_answer_refinement_question`, `okto_pulse_ask_refinement_choice_question`, `okto_pulse_ask_refinement_question`, `okto_pulse_create_refinement`, `okto_pulse_delete_refinement`, `okto_pulse_delete_refinement_knowledge`, `okto_pulse_delete_refinement_question`, `okto_pulse_derive_spec_from_refinement`, `okto_pulse_get_refinement`, `okto_pulse_get_refinement_context`, `okto_pulse_get_refinement_knowledge`, `okto_pulse_get_refinement_snapshot`, `okto_pulse_move_refinement`, `okto_pulse_update_refinement`

## Specs — lifecycle & gates — docs: `okto-pulse://reference/tool-docs/spec`
`okto_pulse_add_spec_knowledge`, `okto_pulse_create_spec`, `okto_pulse_delete_spec`, `okto_pulse_delete_spec_evaluation`, `okto_pulse_delete_spec_knowledge`, `okto_pulse_get_spec`, `okto_pulse_get_spec_context`, `okto_pulse_get_spec_evaluation`, `okto_pulse_get_spec_knowledge`, `okto_pulse_list_spec_evaluations`, `okto_pulse_list_spec_validations`, `okto_pulse_migrate_spec_decisions`, `okto_pulse_move_spec`, `okto_pulse_remove_spec_entity`, `okto_pulse_submit_spec_evaluation`, `okto_pulse_submit_spec_validation`, `okto_pulse_update_spec`, `okto_pulse_update_spec_api_contract`, `okto_pulse_update_spec_entity`

## Specs — entities (rules, decisions, contracts, requirements) — docs: `okto-pulse://reference/tool-docs/spec`
`okto_pulse_add_api_contract`, `okto_pulse_add_business_rule`, `okto_pulse_add_decision`, `okto_pulse_add_integration_requirement`, `okto_pulse_add_observability_requirement`, `okto_pulse_list_api_contracts`, `okto_pulse_list_business_rules`, `okto_pulse_list_integration_requirements`, `okto_pulse_list_observability_requirements`, `okto_pulse_remove_api_contract`, `okto_pulse_remove_business_rule`, `okto_pulse_remove_decision`, `okto_pulse_update_api_contract`, `okto_pulse_update_business_rule`, `okto_pulse_update_decision`

## Test Scenarios — docs: `okto-pulse://reference/tool-docs/test-scenario`
`okto_pulse_add_test_scenario`, `okto_pulse_delete_test_scenario`, `okto_pulse_list_test_scenarios`, `okto_pulse_resolve_bug_regression_scenarios`, `okto_pulse_update_test_scenario`, `okto_pulse_update_test_scenario_status`

## Cards & Tasks — docs: `okto-pulse://reference/tool-docs/card`
`okto_pulse_add_card_dependency`, `okto_pulse_add_card_knowledge`, `okto_pulse_copy_architecture_to_card`, `okto_pulse_copy_knowledge_to_card`, `okto_pulse_copy_mockups_to_card`, `okto_pulse_copy_qa_to_card`, `okto_pulse_create_card`, `okto_pulse_delete_card`, `okto_pulse_delete_card_knowledge`, `okto_pulse_get_card`, `okto_pulse_get_card_dependencies`, `okto_pulse_get_card_knowledge`, `okto_pulse_get_task_conclusions`, `okto_pulse_get_task_context`, `okto_pulse_get_task_validation`, `okto_pulse_link_task`, `okto_pulse_list_blockers`, `okto_pulse_list_cards_by_status`, `okto_pulse_list_task_validations`, `okto_pulse_move_card`, `okto_pulse_remove_card_dependency`, `okto_pulse_submit_task_validation`, `okto_pulse_update_card`, `okto_pulse_update_card_knowledge`

## Amendments — docs: `okto-pulse://reference/tool-docs/card`
`okto_pulse_associate_amendment_revision_artifacts`, `okto_pulse_confirm_amendment_coverage`, `okto_pulse_create_amendment_revision`, `okto_pulse_get_amendment_revision`, `okto_pulse_list_amendment_revisions`, `okto_pulse_transition_amendment_revision`

## Sprints — docs: `okto-pulse://reference/tool-docs/sprint`
`okto_pulse_answer_sprint_question`, `okto_pulse_ask_sprint_question`, `okto_pulse_assign_tasks_to_sprint`, `okto_pulse_create_sprint`, `okto_pulse_delete_sprint_evaluation`, `okto_pulse_delete_sprint_question`, `okto_pulse_get_sprint`, `okto_pulse_get_sprint_context`, `okto_pulse_get_sprint_evaluation`, `okto_pulse_list_sprint_evaluations`, `okto_pulse_move_sprint`, `okto_pulse_submit_sprint_evaluation`, `okto_pulse_suggest_sprints`, `okto_pulse_update_sprint`

## Q&A — docs: `okto-pulse://reference/tool-docs/qa`
`okto_pulse_add_choice_comment`, `okto_pulse_answer_question`, `okto_pulse_answer_spec_question`, `okto_pulse_ask`, `okto_pulse_ask_question`, `okto_pulse_ask_spec_choice_question`, `okto_pulse_ask_spec_question`, `okto_pulse_delete_question`, `okto_pulse_delete_spec_question`, `okto_pulse_get_choice_responses`, `okto_pulse_list_qa`, `okto_pulse_respond_to_choice`

## Knowledge — docs: `okto-pulse://reference/tool-docs/knowledge`
`okto_pulse_list_knowledge`

## Mockups — docs: `okto-pulse://reference/tool-docs/mockup`
`okto_pulse_add_screen_mockup`, `okto_pulse_annotate_mockup`, `okto_pulse_delete_screen_mockup`, `okto_pulse_list_screen_mockups`, `okto_pulse_update_screen_mockup`

## Architecture — docs: `okto-pulse://reference/tool-docs/architecture`
`okto_pulse_add_architecture_design`, `okto_pulse_delete_architecture_design`, `okto_pulse_dump_architecture_diagram`, `okto_pulse_get_architecture_design`, `okto_pulse_get_architecture_design_schema`, `okto_pulse_import_excalidraw_architecture_diagram`, `okto_pulse_list_architecture_designs`, `okto_pulse_list_architecture_propagation_legacy`, `okto_pulse_update_architecture_design`, `okto_pulse_validate_architecture_design_payload`

## Comments & Attachments — docs: `okto-pulse://reference/tool-docs/comment`
`okto_pulse_add_comment`, `okto_pulse_delete_attachment`, `okto_pulse_delete_comment`, `okto_pulse_list_attachments`, `okto_pulse_list_comments`, `okto_pulse_update_comment`, `okto_pulse_upload_attachment`

## Listing & Traceability — docs: `okto-pulse://reference/tool-docs/misc`
`okto_pulse_archive_tree`, `okto_pulse_get_traceability_report`, `okto_pulse_list_by_board`, `okto_pulse_list_snapshots`, `okto_pulse_restore_tree`
