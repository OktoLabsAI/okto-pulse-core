---
version: "1.0"
---

# Tool docs — `misc`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_archive_tree`

Archive an entity and all its descendants in cascade.
Saves pre_archive_status before setting archived=true.

Args:
    board_id: Board ID
    entity_type: Type of entity — one of: ideation, refinement, spec
    entity_id: Entity ID to archive

Returns:
    JSON with archived_count per entity type

## `okto_pulse_clear_resource_not_applicable`

Clear an active Resource Gate N/A mark.

Use this when the resource becomes applicable after all, or when the real
Architecture, Mockup, or Knowledge Base has been attached.

Args:
    board_id: Board ID
    entity_type: One of ideation, refinement, spec, card
    entity_id: Target entity ID
    resource_type: One of architecture, mockup, knowledge_base
    reason: Optional audit reason for clearing the N/A mark

Returns:
    JSON with the updated Resource Gate summary.

## `okto_pulse_get_resource_gate_summary`

Get the Resource Gate state for an SDLC entity.

Args:
    board_id: Board ID
    entity_type: One of ideation, refinement, spec, card
    entity_id: Target entity ID

Returns:
    JSON with Architecture, Mockup, and Knowledge Base states:
    provided, not_applicable, or missing.

## `okto_pulse_get_task_conclusions`

Get the conclusions of a completed task card. Conclusions describe what was done,
the root cause (for bugs), decisions made, and any relevant notes.

Useful for:
- Understanding what was done in a previous task before starting related work
- Bug triage — understanding root cause and fix approach
- Knowledge transfer between agents or team members

Args:
    board_id: Board ID
    card_id: Card ID

Returns:
    JSON with card title, status, conclusions, and bug details if applicable

## `okto_pulse_get_task_context`

Get the FULL execution context for a task card. Aggregates the card data with
all relevant spec information: functional requirements, technical requirements,
acceptance criteria, test scenarios, business rules, API contracts, integration
requirements, observability requirements, knowledge base entries, screen
mockups, Q&A, and comments.

**Always call this before starting work on a task** — it provides everything
an agent needs to understand what to build, how to test it, and what rules apply.

Args:
    board_id: Board ID
    card_id: Card ID
    include_knowledge: Include spec knowledge base entries (default "true")
    include_mockups: Include screen mockups from card and spec (default "true")
    include_qa: Include Q&A items from card and spec (default "true")
    include_comments: Include card comments (default "true")
    include_architecture: Include Architecture Designs from card and spec (default "true")

Returns:
    JSON with complete task context: card details + spec requirements + linked artifacts

## `okto_pulse_get_task_validation`

Get full details of a specific task validation entry.

Args:
    board_id: Board ID
    card_id: Card ID
    validation_id: Validation ID (e.g. "val_abc12345")

Returns:
    JSON with full validation details including scores, justifications, outcome, and threshold violations

## `okto_pulse_get_unseen_summary`

Quick summary of unseen mentions and activity for the agent on this board.
Use this to check if there's anything new without fetching full details.

Args:
    board_id: Board ID

Returns:
    JSON with counts of unseen mentions and recent activity

## `okto_pulse_link_task`

Generic task-linking tool — dispatches on `target_type`. Equivalent to the
per-type tools, each annotated with the `target_type` short code it maps to:
`okto_pulse_link_task_to_rule` (target_type="rule"), `…_to_decision`
(target_type="decision"), `…_to_tr` (target_type="tr"),
`…_to_integration_requirement` (target_type="ir"),
`…_to_observability_requirement` (target_type="or"), `…_to_scenario`
(target_type="scenario"), `…_to_contract` (target_type="contract"), and
`okto_pulse_link_card_to_spec` (target_type="spec"). It exposes a single entry
point so agents don't have to pre-load eight near-identical tool schemas. The
`target_type` Args line below is the single source of truth for accepted codes.

Ideação MCP-token-optimization Story 5.

Args:
    board_id: Board ID.
    target_type: One of: rule, decision, tr, ir, or, scenario, contract, spec.
        Keywords link rule decision tr ir or scenario contract spec.
    target_id: ID of the target artifact (rule_id, decision_id, tr_id,
        ir_id/requirement_id, or_id/requirement_id, scenario_id,
        contract_id, or spec_id when target_type=='spec').
    card_id: ID of the card to link.
    spec_id: Required for every target_type except 'spec'.

Returns:
    JSON identical to the corresponding per-type tool (success + ids +
    saturation envelope).

## `okto_pulse_list_my_mentions`

List comments and Q&A items where you are mentioned via @name.
By default only returns UNSEEN mentions. Use include_seen="true" to get all.

Args:
    board_id: Board ID to search within
    include_seen: "true" to include already-seen mentions (default "false")

Returns:
    JSON with unseen mentions, each with an item_id you can pass to mark_as_seen

## `okto_pulse_list_qa`

List Q&A items for a spec, ideation, or refinement.

    Consolidates: list_spec_qa, list_ideation_qa, list_refinement_qa.

    Args:
        board_id: Board ID
        entity_type: One of: spec, ideation, refinement
        entity_id: ID of the entity (spec_id, ideation_id, or refinement_id)
        filters: Optional filter dict OR JSON string.
            status: filter by answer status
            asked_by: filter by agent/user who asked

    Returns:
        JSON {qa_items: [...], count: int, entity_type: str} or structured error

## `okto_pulse_list_task_validations`

List all validations for a task card in reverse chronological order.

Useful for understanding the validation history of a card, especially
cards that have been through multiple validation cycles (failed → reworked → resubmitted).

Args:
    board_id: Board ID
    card_id: Card ID

Returns:
    JSON with list of validation entries

## `okto_pulse_resolve_bug_regression_scenarios`

Preview reusable regression scenarios for a bug without mutating a locked spec.

Use this before creating or linking a post-bug regression test. It classifies
eligible same-spec scenarios and rejects unrelated or cross-spec candidates so
agents can choose reuse versus amendment/hotfix work explicitly.

Args:
    board_id: Board ID.
    bug_id: Bug card ID.
    affected_task_ids: Optional multi-value task IDs when the incident spans
        additional implementation tasks.
    candidate_scenario_ids: Optional multi-value scenario IDs to classify.

Returns:
    JSON preview with eligible candidates, rejected candidates, and semantic-gap
    guidance. The tool is read-only.

## `okto_pulse_mark_as_seen`

Mark one or more items as seen so they won't appear in list_my_mentions.
Use this after processing mentions to avoid seeing them again.

Args:
    board_id: Board ID (for access verification)
    item_ids: Multi-value item IDs to mark as seen (from list_my_mentions item_id
        field). Preferred native list (e.g. ``["c_a", "qa_b"]``); legacy string
        accepted as JSON array or pipe-separated. Comma-only string is REJECTED.
        See ``okto_pulse.core.mcp.helpers.coerce_to_list_str``.

Returns:
    JSON with count of newly marked items

## `okto_pulse_mark_resource_not_applicable`

Mark a mandatory resource as not applicable through the MCP channel.

Args:
    board_id: Board ID
    entity_type: One of ideation, refinement, spec, card
    entity_id: Target entity ID
    resource_type: One of architecture, mockup, knowledge_base
    justification: REQUIRED. Explain why the resource is not applicable.

Returns:
    JSON with the updated Resource Gate summary and a warning that skipping
    the resource can lead to partial or incorrect solutions if it is needed.

## `okto_pulse_restore_tree`

Restore an archived entity and all its descendants.
Returns each entity to its pre_archive_status.

Args:
    board_id: Board ID
    entity_type: Type of entity — one of: ideation, refinement, spec
    entity_id: Entity ID to restore

Returns:
    JSON with restored_count per entity type

## `okto_pulse_submit_task_validation`

Submit a task validation for a card in 'validation' status.

Evaluates the implementation quality of a completed task against three
dimensions: confidence, completeness, and drift. The system applies
threshold checks (resolved from sprint → spec → board hierarchy) and
automatically routes the card: success → done; failed remains in
validation so the validator feedback stays visible and the executor can
decide whether to move the card back for rework.

Args:
    board_id: Board ID
    card_id: Card ID (must be in 'validation' status)
    confidence: Score 0-100 — how confident is the reviewer that the task was implemented correctly?
    confidence_justification: Why this confidence score
    estimated_completeness: Score 0-100 — how complete is the implementation relative to the spec?
    completeness_justification: Why this completeness score
    estimated_drift: Score 0-100 — how much did the implementation deviate from the spec? (lower is better)
    drift_justification: Why this drift score
    general_justification: Overall assessment of the task implementation
    recommendation: One of: approve, reject

Returns:
    JSON with validation result, outcome, threshold violations, and card routing
