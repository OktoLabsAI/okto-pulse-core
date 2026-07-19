---
version: "1.0"
generated_by: okto_pulse.core.mcp.tools_catalog_generator
---
# Okto Pulse — MCP Tools Catalog

GENERATED FILE — do not edit by hand. Regenerate with
`python -m okto_pulse.core.mcp.tools_catalog_generator`
(guarded by tests/test_mcp_tools_catalog_drift.py).

Tool schemas arrive lazily via MCP `tools/list`. Every entry links the
exact resource containing that tool's args, returns and examples.
Required filters for the consolidated `list_*` tools:
`okto-pulse://reference/list_tools`.

## KG — Consolidation (write path)
- `okto_pulse_kg_abort_consolidation` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_add_edge_candidate` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_add_node_candidate` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_begin_consolidation` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_commit_consolidation` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_propose_reconciliation` — docs: `okto-pulse://reference/tool-docs/kg`

## KG — Query (Recall)
- `okto_pulse_kg_explain_constraint` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_find_contradictions` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_find_similar_decisions` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_get_decision_history` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_get_learning_from_bugs` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_get_related_context` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_get_similar_nodes` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_get_supersedence_chain` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_list_alternatives` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_query_global` — docs: `okto-pulse://reference/tool-docs/kg`

## KG — Query (Power)
- `okto_pulse_kg_export_jsonld` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_provenance_drift` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_query_cypher` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_query_natural` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_query_reflective` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_schema_info` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_verify_grounding` — docs: `okto-pulse://reference/tool-docs/kg`

## KG — Cognitive readiness & closeout
- `okto_pulse_kg_clear_cognitive_skip` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_evaluate_bug_cognitive_closure` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_evaluate_cognitive_readiness` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_list_cognitive_dlq` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_list_cognitive_pending_items` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_list_cognitive_readiness_items` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_record_cognitive_skip` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_update_cognitive_pending_item` — docs: `okto-pulse://reference/tool-docs/kg`

## KG — Rebuild & recovery
- `okto_pulse_kg_global_discovery_recovery_confirm` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_global_discovery_recovery_preflight` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_global_discovery_recovery_run` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_quarantine_restore` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_rebuild_confirm` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_rebuild_preflight` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_rebuild_run` — docs: `okto-pulse://reference/tool-docs/kg`

## KG — Operational & health
- `okto_pulse_kg_canonical_debt_list` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_canonical_partition_integrity_list` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_connectivity_dlq_diagnose` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_connectivity_dlq_reprocess` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_connectivity_dlq_verify` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_dead_letter_list` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_dead_letter_reprocess` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_digest_layer_mismatch_list` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_digest_layer_reconcile` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_global_discovery_recovery_cancel` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_global_discovery_recovery_resume` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_global_discovery_recovery_status` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_global_outbox_dead_letter_list` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_global_outbox_dead_letter_reprocess` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_global_outbox_dead_letter_verify` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_health` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_health_readiness` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_migrate_schema` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_originates_from_contract_audit` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_orphan_backfill` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_orphan_report` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_queue_drilldown` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_stale_canonical_parity_list` — docs: `okto-pulse://reference/tool-docs/kg`
- `okto_pulse_kg_tick_run_now` — docs: `okto-pulse://reference/tool-docs/kg`

## Session & Agents
- `okto_pulse_get_my_profile` — docs: `okto-pulse://reference/tool-docs/agent`
- `okto_pulse_get_unseen_summary` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_list_agents` — docs: `okto-pulse://reference/tool-docs/agent`
- `okto_pulse_list_my_boards` — docs: `okto-pulse://reference/tool-docs/board`
- `okto_pulse_list_my_mentions` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_mark_as_seen` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_update_my_profile` — docs: `okto-pulse://reference/tool-docs/agent`

## Boards & Governance
- `okto_pulse_clear_resource_not_applicable` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_get_activity_log` — docs: `okto-pulse://reference/tool-docs/activity`
- `okto_pulse_get_allowed_transitions` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_get_analytics` — docs: `okto-pulse://reference/tool-docs/analytics`
- `okto_pulse_get_board` — docs: `okto-pulse://reference/tool-docs/board`
- `okto_pulse_get_publish_health` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_get_resource_gate_summary` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_list_board_members` — docs: `okto-pulse://reference/tool-docs/board`
- `okto_pulse_mark_resource_not_applicable` — docs: `okto-pulse://reference/tool-docs/misc`

## Default Board Config
- `okto_pulse_activate_default_board_config_version` — docs: `okto-pulse://reference/tool-docs/board`
- `okto_pulse_create_default_board_config_version` — docs: `okto-pulse://reference/tool-docs/board`
- `okto_pulse_deactivate_default_board_config_version` — docs: `okto-pulse://reference/tool-docs/board`
- `okto_pulse_get_active_default_board_config` — docs: `okto-pulse://reference/tool-docs/board`
- `okto_pulse_get_board_default_config_diff` — docs: `okto-pulse://reference/tool-docs/board`
- `okto_pulse_list_default_board_config_versions` — docs: `okto-pulse://reference/tool-docs/board`

## Design Systems
- `okto_pulse_create_design_system` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_delete_design_system` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_get_board_design_system` — docs: `okto-pulse://reference/tool-docs/board`
- `okto_pulse_get_design_system` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_link_board_design_system` — docs: `okto-pulse://reference/tool-docs/board`
- `okto_pulse_list_design_systems` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_set_default_design_system` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_unlink_board_design_system` — docs: `okto-pulse://reference/tool-docs/board`
- `okto_pulse_update_design_system` — docs: `okto-pulse://reference/tool-docs/misc`

## Guidelines
- `okto_pulse_create_guideline` — docs: `okto-pulse://reference/tool-docs/guideline`
- `okto_pulse_delete_guideline` — docs: `okto-pulse://reference/tool-docs/guideline`
- `okto_pulse_get_board_guidelines` — docs: `okto-pulse://reference/tool-docs/guideline`
- `okto_pulse_link_guideline_to_board` — docs: `okto-pulse://reference/tool-docs/guideline`
- `okto_pulse_list_default_guideline_candidates` — docs: `okto-pulse://reference/tool-docs/guideline`
- `okto_pulse_list_guidelines` — docs: `okto-pulse://reference/tool-docs/guideline`
- `okto_pulse_unlink_guideline_from_board` — docs: `okto-pulse://reference/tool-docs/guideline`
- `okto_pulse_update_board_guideline_priority` — docs: `okto-pulse://reference/tool-docs/guideline`
- `okto_pulse_update_default_guideline_refs` — docs: `okto-pulse://reference/tool-docs/guideline`
- `okto_pulse_update_guideline` — docs: `okto-pulse://reference/tool-docs/guideline`

## Stories & Topics
- `okto_pulse_archive_story` — docs: `okto-pulse://reference/tool-docs/story`
- `okto_pulse_archive_topic` — docs: `okto-pulse://reference/tool-docs/topic`
- `okto_pulse_convert_stories_to_ideation` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_create_story` — docs: `okto-pulse://reference/tool-docs/story`
- `okto_pulse_create_topic` — docs: `okto-pulse://reference/tool-docs/topic`
- `okto_pulse_delete_topic` — docs: `okto-pulse://reference/tool-docs/topic`
- `okto_pulse_get_ideation_history` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_get_refinement_history` — docs: `okto-pulse://reference/tool-docs/refinement`
- `okto_pulse_get_spec_history` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_merge_topics` — docs: `okto-pulse://reference/tool-docs/topic`
- `okto_pulse_move_story` — docs: `okto-pulse://reference/tool-docs/story`
- `okto_pulse_restore_story` — docs: `okto-pulse://reference/tool-docs/story`
- `okto_pulse_restore_topic` — docs: `okto-pulse://reference/tool-docs/topic`
- `okto_pulse_update_story` — docs: `okto-pulse://reference/tool-docs/story`
- `okto_pulse_update_topic` — docs: `okto-pulse://reference/tool-docs/topic`

## Ideations
- `okto_pulse_add_ideation_knowledge` — docs: `okto-pulse://reference/tool-docs/knowledge`
- `okto_pulse_answer_ideation_question` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_ask_ideation_choice_question` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_ask_ideation_question` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_create_ideation` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_delete_ideation` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_delete_ideation_knowledge` — docs: `okto-pulse://reference/tool-docs/knowledge`
- `okto_pulse_delete_ideation_question` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_derive_spec_from_ideation` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_evaluate_ideation` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_get_ideation` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_get_ideation_context` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_get_ideation_knowledge` — docs: `okto-pulse://reference/tool-docs/knowledge`
- `okto_pulse_get_ideation_snapshot` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_link_story_to_ideation` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_move_ideation` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_set_ideation_ambiguity_gate_skip` — docs: `okto-pulse://reference/tool-docs/ideation`
- `okto_pulse_update_ideation` — docs: `okto-pulse://reference/tool-docs/ideation`

## Refinements
- `okto_pulse_add_refinement_knowledge` — docs: `okto-pulse://reference/tool-docs/knowledge`
- `okto_pulse_answer_refinement_question` — docs: `okto-pulse://reference/tool-docs/refinement`
- `okto_pulse_ask_refinement_choice_question` — docs: `okto-pulse://reference/tool-docs/refinement`
- `okto_pulse_ask_refinement_question` — docs: `okto-pulse://reference/tool-docs/refinement`
- `okto_pulse_create_refinement` — docs: `okto-pulse://reference/tool-docs/refinement`
- `okto_pulse_delete_refinement` — docs: `okto-pulse://reference/tool-docs/refinement`
- `okto_pulse_delete_refinement_knowledge` — docs: `okto-pulse://reference/tool-docs/knowledge`
- `okto_pulse_delete_refinement_question` — docs: `okto-pulse://reference/tool-docs/refinement`
- `okto_pulse_derive_spec_from_refinement` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_get_refinement` — docs: `okto-pulse://reference/tool-docs/refinement`
- `okto_pulse_get_refinement_context` — docs: `okto-pulse://reference/tool-docs/refinement`
- `okto_pulse_get_refinement_knowledge` — docs: `okto-pulse://reference/tool-docs/knowledge`
- `okto_pulse_get_refinement_snapshot` — docs: `okto-pulse://reference/tool-docs/refinement`
- `okto_pulse_move_refinement` — docs: `okto-pulse://reference/tool-docs/refinement`
- `okto_pulse_update_refinement` — docs: `okto-pulse://reference/tool-docs/refinement`

## Specs — lifecycle & gates
- `okto_pulse_add_spec_knowledge` — docs: `okto-pulse://reference/tool-docs/knowledge`
- `okto_pulse_create_spec` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_delete_spec` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_delete_spec_evaluation` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_delete_spec_knowledge` — docs: `okto-pulse://reference/tool-docs/knowledge`
- `okto_pulse_get_spec` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_get_spec_context` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_get_spec_evaluation` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_get_spec_knowledge` — docs: `okto-pulse://reference/tool-docs/knowledge`
- `okto_pulse_list_spec_evaluations` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_list_spec_validations` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_migrate_spec_decisions` — docs: `okto-pulse://reference/tool-docs/decision`
- `okto_pulse_move_spec` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_remove_spec_entity` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_submit_spec_evaluation` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_submit_spec_validation` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_update_spec` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_update_spec_api_contract` — docs: `okto-pulse://reference/tool-docs/api-contract`
- `okto_pulse_update_spec_entity` — docs: `okto-pulse://reference/tool-docs/spec`

## Specs — entities (rules, decisions, contracts, requirements)
- `okto_pulse_add_api_contract` — docs: `okto-pulse://reference/tool-docs/api-contract`
- `okto_pulse_add_business_rule` — docs: `okto-pulse://reference/tool-docs/business-rule`
- `okto_pulse_add_decision` — docs: `okto-pulse://reference/tool-docs/decision`
- `okto_pulse_add_integration_requirement` — docs: `okto-pulse://reference/tool-docs/integration-requirement`
- `okto_pulse_add_observability_requirement` — docs: `okto-pulse://reference/tool-docs/observability-requirement`
- `okto_pulse_list_api_contracts` — docs: `okto-pulse://reference/tool-docs/api-contract`
- `okto_pulse_list_business_rules` — docs: `okto-pulse://reference/tool-docs/business-rule`
- `okto_pulse_list_integration_requirements` — docs: `okto-pulse://reference/tool-docs/integration-requirement`
- `okto_pulse_list_observability_requirements` — docs: `okto-pulse://reference/tool-docs/observability-requirement`
- `okto_pulse_remove_api_contract` — docs: `okto-pulse://reference/tool-docs/api-contract`
- `okto_pulse_remove_business_rule` — docs: `okto-pulse://reference/tool-docs/business-rule`
- `okto_pulse_remove_decision` — docs: `okto-pulse://reference/tool-docs/decision`
- `okto_pulse_update_api_contract` — docs: `okto-pulse://reference/tool-docs/api-contract`
- `okto_pulse_update_business_rule` — docs: `okto-pulse://reference/tool-docs/business-rule`
- `okto_pulse_update_decision` — docs: `okto-pulse://reference/tool-docs/decision`

## Test Scenarios
- `okto_pulse_add_test_scenario` — docs: `okto-pulse://reference/tool-docs/test-scenario`
- `okto_pulse_delete_test_scenario` — docs: `okto-pulse://reference/tool-docs/test-scenario`
- `okto_pulse_execute_test_scenario_evidence` — docs: `okto-pulse://reference/tool-docs/test-scenario`
- `okto_pulse_list_test_scenarios` — docs: `okto-pulse://reference/tool-docs/test-scenario`
- `okto_pulse_resolve_bug_regression_scenarios` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_update_test_scenario` — docs: `okto-pulse://reference/tool-docs/test-scenario`
- `okto_pulse_update_test_scenario_status` — docs: `okto-pulse://reference/tool-docs/test-scenario`

## Cards & Tasks
- `okto_pulse_add_card_dependency` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_add_card_knowledge` — docs: `okto-pulse://reference/tool-docs/knowledge`
- `okto_pulse_copy_architecture_to_card` — docs: `okto-pulse://reference/tool-docs/architecture`
- `okto_pulse_copy_knowledge_to_card` — docs: `okto-pulse://reference/tool-docs/knowledge`
- `okto_pulse_copy_mockups_to_card` — docs: `okto-pulse://reference/tool-docs/mockup`
- `okto_pulse_copy_qa_to_card` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_create_card` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_delete_card` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_delete_card_knowledge` — docs: `okto-pulse://reference/tool-docs/knowledge`
- `okto_pulse_get_card` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_get_card_dependencies` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_get_card_knowledge` — docs: `okto-pulse://reference/tool-docs/knowledge`
- `okto_pulse_get_task_conclusions` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_get_task_context` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_get_task_validation` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_link_task` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_list_blockers` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_list_cards_by_status` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_list_task_validations` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_move_card` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_remove_card_dependency` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_submit_task_validation` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_update_card` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_update_card_knowledge` — docs: `okto-pulse://reference/tool-docs/knowledge`

## Amendments
- `okto_pulse_associate_amendment_revision_artifacts` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_confirm_amendment_coverage` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_create_amendment_revision` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_get_amendment_revision` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_list_amendment_revisions` — docs: `okto-pulse://reference/tool-docs/card`
- `okto_pulse_transition_amendment_revision` — docs: `okto-pulse://reference/tool-docs/card`

## Sprints
- `okto_pulse_answer_sprint_question` — docs: `okto-pulse://reference/tool-docs/sprint`
- `okto_pulse_ask_sprint_question` — docs: `okto-pulse://reference/tool-docs/sprint`
- `okto_pulse_assign_tasks_to_sprint` — docs: `okto-pulse://reference/tool-docs/sprint`
- `okto_pulse_create_sprint` — docs: `okto-pulse://reference/tool-docs/sprint`
- `okto_pulse_delete_sprint_evaluation` — docs: `okto-pulse://reference/tool-docs/sprint`
- `okto_pulse_delete_sprint_question` — docs: `okto-pulse://reference/tool-docs/sprint`
- `okto_pulse_get_sprint` — docs: `okto-pulse://reference/tool-docs/sprint`
- `okto_pulse_get_sprint_context` — docs: `okto-pulse://reference/tool-docs/sprint`
- `okto_pulse_get_sprint_evaluation` — docs: `okto-pulse://reference/tool-docs/sprint`
- `okto_pulse_list_sprint_evaluations` — docs: `okto-pulse://reference/tool-docs/sprint`
- `okto_pulse_move_sprint` — docs: `okto-pulse://reference/tool-docs/sprint`
- `okto_pulse_submit_sprint_evaluation` — docs: `okto-pulse://reference/tool-docs/sprint`
- `okto_pulse_suggest_sprints` — docs: `okto-pulse://reference/tool-docs/sprint`
- `okto_pulse_update_sprint` — docs: `okto-pulse://reference/tool-docs/sprint`

## Q&A
- `okto_pulse_add_choice_comment` — docs: `okto-pulse://reference/tool-docs/comment`
- `okto_pulse_answer_question` — docs: `okto-pulse://reference/tool-docs/qa`
- `okto_pulse_answer_spec_question` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_ask` — docs: `okto-pulse://reference/tool-docs/qa`
- `okto_pulse_ask_question` — docs: `okto-pulse://reference/tool-docs/qa`
- `okto_pulse_ask_spec_choice_question` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_ask_spec_question` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_delete_question` — docs: `okto-pulse://reference/tool-docs/qa`
- `okto_pulse_delete_spec_question` — docs: `okto-pulse://reference/tool-docs/spec`
- `okto_pulse_get_choice_responses` — docs: `okto-pulse://reference/tool-docs/qa`
- `okto_pulse_list_qa` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_respond_to_choice` — docs: `okto-pulse://reference/tool-docs/qa`

## Knowledge
- `okto_pulse_list_knowledge` — docs: `okto-pulse://reference/tool-docs/knowledge`

## Mockups
- `okto_pulse_add_screen_mockup` — docs: `okto-pulse://reference/tool-docs/mockup`
- `okto_pulse_annotate_mockup` — docs: `okto-pulse://reference/tool-docs/mockup`
- `okto_pulse_delete_screen_mockup` — docs: `okto-pulse://reference/tool-docs/mockup`
- `okto_pulse_list_screen_mockups` — docs: `okto-pulse://reference/tool-docs/mockup`
- `okto_pulse_update_screen_mockup` — docs: `okto-pulse://reference/tool-docs/mockup`

## Architecture
- `okto_pulse_add_architecture_design` — docs: `okto-pulse://reference/tool-docs/architecture`
- `okto_pulse_delete_architecture_design` — docs: `okto-pulse://reference/tool-docs/architecture`
- `okto_pulse_dump_architecture_diagram` — docs: `okto-pulse://reference/tool-docs/architecture`
- `okto_pulse_get_architecture_design` — docs: `okto-pulse://reference/tool-docs/architecture`
- `okto_pulse_get_architecture_design_schema` — docs: `okto-pulse://reference/tool-docs/architecture`
- `okto_pulse_import_excalidraw_architecture_diagram` — docs: `okto-pulse://reference/tool-docs/architecture`
- `okto_pulse_list_architecture_designs` — docs: `okto-pulse://reference/tool-docs/architecture`
- `okto_pulse_list_architecture_propagation_legacy` — docs: `okto-pulse://reference/tool-docs/architecture`
- `okto_pulse_update_architecture_design` — docs: `okto-pulse://reference/tool-docs/architecture`
- `okto_pulse_validate_architecture_design_payload` — docs: `okto-pulse://reference/tool-docs/architecture`

## Comments & Attachments
- `okto_pulse_add_comment` — docs: `okto-pulse://reference/tool-docs/comment`
- `okto_pulse_delete_attachment` — docs: `okto-pulse://reference/tool-docs/attachment`
- `okto_pulse_delete_comment` — docs: `okto-pulse://reference/tool-docs/comment`
- `okto_pulse_list_attachments` — docs: `okto-pulse://reference/tool-docs/attachment`
- `okto_pulse_list_comments` — docs: `okto-pulse://reference/tool-docs/comment`
- `okto_pulse_update_comment` — docs: `okto-pulse://reference/tool-docs/comment`
- `okto_pulse_upload_attachment` — docs: `okto-pulse://reference/tool-docs/attachment`

## Listing & Traceability
- `okto_pulse_archive_tree` — docs: `okto-pulse://reference/tool-docs/misc`
- `okto_pulse_get_traceability_report` — docs: `okto-pulse://reference/tool-docs/traceability`
- `okto_pulse_list_by_board` — docs: `okto-pulse://reference/tool-docs/board`
- `okto_pulse_list_snapshots` — docs: `okto-pulse://reference/tool-docs/snapshot`
- `okto_pulse_restore_tree` — docs: `okto-pulse://reference/tool-docs/misc`
