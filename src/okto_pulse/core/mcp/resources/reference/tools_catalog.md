# Available Tools — Full Catalog

Lazy reference. Most agents can discover tools via the MCP `tools/list` protocol; this file groups them by domain for human-readable lookup.

## Identity & Context
`okto_pulse_get_my_profile`, `okto_pulse_update_my_profile`, `okto_pulse_list_my_boards`

## Board & Members
`okto_pulse_get_board`, `okto_pulse_list_agents`, `okto_pulse_list_board_members`, `okto_pulse_get_activity_log`

## Cards
`okto_pulse_create_card`, `okto_pulse_get_card`, `okto_pulse_get_task_context`, `okto_pulse_get_task_conclusions`, `okto_pulse_update_card`, `okto_pulse_move_card`, `okto_pulse_delete_card`, `okto_pulse_list_cards_by_status`

## Dependencies
`okto_pulse_add_card_dependency`, `okto_pulse_remove_card_dependency`, `okto_pulse_get_card_dependencies`

## Q&A
`okto_pulse_ask_question`, `okto_pulse_answer_question`, `okto_pulse_delete_question`

## Comments
`okto_pulse_add_comment`, `okto_pulse_add_choice_comment`, `okto_pulse_respond_to_choice`, `okto_pulse_get_choice_responses`, `okto_pulse_list_comments`, `okto_pulse_update_comment`, `okto_pulse_delete_comment`

## Specs
`okto_pulse_create_spec`, `okto_pulse_get_spec`, **`okto_pulse_list_by_board`** (`entity_type='spec'`), `okto_pulse_update_spec`, `okto_pulse_move_spec`, `okto_pulse_delete_spec`, `okto_pulse_link_card_to_spec`

## Sprints
`okto_pulse_create_sprint`, `okto_pulse_get_sprint`, **`okto_pulse_list_by_board`** (`entity_type='sprint'`, `filters={'spec_id':...}`), `okto_pulse_update_sprint`, `okto_pulse_move_sprint`, `okto_pulse_assign_tasks_to_sprint`, `okto_pulse_submit_sprint_evaluation`, `okto_pulse_suggest_sprints`

## Ideations
`okto_pulse_create_ideation`, `okto_pulse_get_ideation`, **`okto_pulse_list_by_board`** (`entity_type='ideation'`), `okto_pulse_update_ideation`, `okto_pulse_move_ideation`, `okto_pulse_evaluate_ideation`, `okto_pulse_delete_ideation`

## Refinements
`okto_pulse_create_refinement`, `okto_pulse_get_refinement`, **`okto_pulse_list_by_board`** (`entity_type='refinement'`, `filters={'ideation_id':...}`), `okto_pulse_update_refinement`, `okto_pulse_move_refinement`, `okto_pulse_delete_refinement`, `okto_pulse_derive_spec_from_ideation`, `okto_pulse_derive_spec_from_refinement`

## Stories & Topics
**`okto_pulse_list_by_board`** (`entity_type='topic'`), `okto_pulse_create_topic`, `okto_pulse_update_topic`, `okto_pulse_archive_topic`, `okto_pulse_restore_topic`, `okto_pulse_delete_topic`, `okto_pulse_merge_topics`, **`okto_pulse_list_by_board`** (`entity_type='story'`), `okto_pulse_create_story`, `okto_pulse_update_story`, `okto_pulse_move_story`, `okto_pulse_archive_story`, `okto_pulse_restore_story`, `okto_pulse_link_story_to_ideation`, `okto_pulse_convert_stories_to_ideation`

## Test Scenarios
`okto_pulse_add_test_scenario`, `okto_pulse_list_test_scenarios`, `okto_pulse_update_test_scenario_status`, `okto_pulse_link_task_to_scenario`, `okto_pulse_link_task_to_rule`, `okto_pulse_link_task_to_contract`, `okto_pulse_link_task_to_tr`, `okto_pulse_link_task_to_decision`

## Business Rules, Contracts, Mockups, Architecture, Knowledge
`okto_pulse_add_business_rule`, `okto_pulse_update_business_rule`, `okto_pulse_remove_business_rule`, `okto_pulse_list_business_rules`; `okto_pulse_add_api_contract`, `okto_pulse_update_api_contract`, `okto_pulse_remove_api_contract`, `okto_pulse_list_api_contracts`; `okto_pulse_add_screen_mockup`, `okto_pulse_update_screen_mockup`, `okto_pulse_delete_screen_mockup`, `okto_pulse_annotate_mockup`, `okto_pulse_list_screen_mockups`; `okto_pulse_get_architecture_design_schema`, `okto_pulse_validate_architecture_design_payload`, `okto_pulse_add_architecture_design`, `okto_pulse_update_architecture_design`, `okto_pulse_delete_architecture_design`, `okto_pulse_list_architecture_designs`, `okto_pulse_get_architecture_design`, `okto_pulse_import_excalidraw_architecture_diagram`, `okto_pulse_dump_architecture_diagram`, `okto_pulse_copy_architecture_to_card`; `okto_pulse_get_resource_gate_summary`, `okto_pulse_mark_resource_not_applicable`, `okto_pulse_clear_resource_not_applicable`; `okto_pulse_add_spec_knowledge`, **`okto_pulse_list_knowledge`** (`entity_type='spec'`), `okto_pulse_get_spec_knowledge`, `okto_pulse_delete_spec_knowledge`; `okto_pulse_add_card_knowledge`, **`okto_pulse_list_knowledge`** (`entity_type='card'`), `okto_pulse_get_card_knowledge`, `okto_pulse_update_card_knowledge`, `okto_pulse_delete_card_knowledge`; `okto_pulse_copy_mockups_to_card`, `okto_pulse_copy_knowledge_to_card`, `okto_pulse_copy_qa_to_card`

## Decisions
`okto_pulse_add_decision`, `okto_pulse_update_decision`, `okto_pulse_remove_decision`, `okto_pulse_migrate_spec_decisions`

## Evaluations & Validations
`okto_pulse_submit_spec_validation`, `okto_pulse_submit_spec_evaluation`, `okto_pulse_submit_task_validation`, `okto_pulse_list_spec_validations`, `okto_pulse_list_spec_evaluations`, `okto_pulse_list_task_validations`, `okto_pulse_get_task_validation`

## Archive & Restore
`okto_pulse_archive_tree`, `okto_pulse_restore_tree`

## Guidelines
`okto_pulse_get_board_guidelines`, `okto_pulse_list_guidelines`, `okto_pulse_create_guideline`, `okto_pulse_update_guideline`, `okto_pulse_delete_guideline`, `okto_pulse_link_guideline_to_board`, `okto_pulse_unlink_guideline_from_board`

## Mentions & Seen Tracking
`okto_pulse_get_unseen_summary`, `okto_pulse_list_my_mentions`, `okto_pulse_mark_as_seen`

## Consolidated Context Retrieval (MANDATORY before any validation/move)
`okto_pulse_get_task_context`, `okto_pulse_get_ideation_context`, `okto_pulse_get_refinement_context`, `okto_pulse_get_spec_context`, `okto_pulse_get_sprint_context`, `okto_pulse_get_traceability_report`

## Attachments
`okto_pulse_upload_attachment`, `okto_pulse_list_attachments`, `okto_pulse_delete_attachment`

## Analytics
`okto_pulse_get_analytics`

## KG — Consolidation
`okto_pulse_kg_begin_consolidation`, `okto_pulse_kg_add_node_candidate`, `okto_pulse_kg_add_edge_candidate`, `okto_pulse_kg_get_similar_nodes`, `okto_pulse_kg_propose_reconciliation`, `okto_pulse_kg_commit_consolidation`, `okto_pulse_kg_abort_consolidation`

## KG — Query (Primary)
`okto_pulse_kg_get_decision_history`, `okto_pulse_kg_get_related_context`, `okto_pulse_kg_get_supersedence_chain`, `okto_pulse_kg_find_contradictions`, `okto_pulse_kg_find_similar_decisions`, `okto_pulse_kg_explain_constraint`, `okto_pulse_kg_list_alternatives`, `okto_pulse_kg_get_learning_from_bugs`, `okto_pulse_kg_query_global`

## KG — Query (Power)
`okto_pulse_kg_query_cypher`, `okto_pulse_kg_query_natural`, `okto_pulse_kg_schema_info`

## KG — Operational
`okto_pulse_kg_health`, `okto_pulse_kg_dead_letter_list`, `okto_pulse_kg_dead_letter_reprocess`, `okto_pulse_kg_migrate_schema`, `okto_pulse_kg_tick_run_now`
