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

## `okto_pulse_get_publish_health`

Read the metrics publication health surface used by Pulse telemetry.

Use this before or after an AWS/S3/Firehose validation to inspect the last
publish attempt, failure state, freshness, and configured availability
sources without mutating local state.

Args:
    board_id: Board ID used for authentication and scoping.

Returns:
    JSON health DTO with source availability, last success/failure details,
    publish mode, and bounded reason codes.

## `okto_pulse_link_task`

Generic task-linking tool — dispatches on `target_type`. Equivalent to the
former per-type tools and accepts short codes/aliases for each link family. It
exposes a single entry point so agents don't have to pre-load near-identical
tool schemas. The `target_type` Args line below is the single source of truth
for accepted codes and long-name aliases.

Ideação MCP-token-optimization Story 5.

Args:
    board_id: Board ID.
    target_type: One of: rule, business_rule, decision, tr,
        technical_requirement, ir, integration_requirement, or,
        observability_requirement, scenario, test_scenario, contract,
        api_contract, spec.
        Keywords link rule decision tr ir or scenario contract spec.
        Legacy target names map to short codes: business_rule uses
        target_type="rule"; decision uses target_type="decision"; technical
        requirement uses target_type="tr"; integration requirement uses
        target_type="ir"; observability requirement uses target_type="or";
        test scenario uses target_type="scenario"; API contract uses
        target_type="contract"; spec links use target_type="spec".
        Direct FR task links are not accepted by this tool; the FR coverage gate
        reads business_rules[].linked_requirements.
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

## `okto_pulse_create_amendment_revision`

Create a formal hotfix/amendment revision for a bug tied to a content-locked spec
(`done`/`validated`, OR `in_progress` still content-locked by an active passed
validation — `current_validation_id` → outcome=success). An `in_progress` spec that
is still editable (no active success validation, or a `failed`/`stale`/`superseded`
one) is rejected — edit it directly instead.

Use this for Path B regression remediation when a same-spec scenario cannot be
reused safely. The revision starts before coverage confirmation; it does not by
itself close the bug gate.

Args:
    board_id: Board ID.
    bug_id: Bug card ID.
    title: Revision title.
    description: Optional summary of the semantic gap or hotfix scope.

Returns:
    JSON with revision identity, status, lineage_state, and required follow-up.

## `okto_pulse_list_amendment_revisions`

List amendment/hotfix revisions for a board or bug.

Use this to inspect Path B readiness before moving a bug forward, especially
when the bug is blocked with coverage_pending or incomplete lineage.

Args:
    board_id: Board ID.
    bug_id: Optional bug card ID filter.
    status: Optional status filter.

Returns:
    JSON list with revision status, lineage_state, artifacts, and coverage flags.

## `okto_pulse_get_amendment_revision`

Read one amendment/hotfix revision in detail.

Args:
    board_id: Board ID.
    revision_id: Amendment revision ID.

Returns:
    JSON with lineage artifacts, associated regression scenario/test task,
    status, and coverage confirmation state.

## `okto_pulse_associate_amendment_revision_artifacts`

Attach or replace the declared artifacts on an amendment revision.

Use this to bind the revision spec, declared regression scenario, regression
test task, and affected/origin task membership before transition.

Args:
    board_id: Board ID.
    revision_id: Amendment revision ID.
    revision_spec_id: Optional spec ID for the amendment.
    regression_scenario_id: Optional test scenario ID.
    regression_test_task_id: Optional test card ID.
    affected_task_ids: Optional multi-value task IDs.

Returns:
    JSON with the updated artifact set and remaining lineage requirements.

## `okto_pulse_transition_amendment_revision`

Move an amendment revision through status and lineage-state transitions.

Use this after artifacts are associated. `lineage_state=complete` requires the
declared scenario, test task, and origin/affected membership. `approved`/`done`
require complete lineage. This still does not confirm bug coverage; use
`okto_pulse_confirm_amendment_coverage` for the validator attestation.

Args:
    board_id: Board ID.
    revision_id: Amendment revision ID.
    status: Optional target status.
    lineage_state: Optional target lineage state.
    note: Optional transition rationale.

Returns:
    JSON with updated status, lineage_state, blockers, and next action.

## `okto_pulse_confirm_amendment_coverage`

Validator-only confirmation that the amendment regression artifacts satisfy the
bug coverage gate.

Use this after the regression test card and declared scenario have fresh
re-executable evidence. This is the non-forgeable coverage signal consumed by
bug movement gates. Binding + validator authorization + reexecutable evidence are
necessary but NOT sufficient: BEFORE persisting, the tool runs the same
gate-consumability preflight the bug regression gate uses, so success implies the
attestation is consumable (same-spec routes through Path A by origin/affected-task
lineage; cross-spec routes through Path B and must reach `path_b_ready`). An inert
tuple fails closed with `coverage_not_gate_consumable` (see `reference/errors.md`).
There is no `bug_id` argument — the amendment carries bug/board/original_spec.

Args:
    board_id: Board ID.
    amendment_id: Path B AmendmentHotfixRevision ID (carries bug/board/spec).
    regression_test_task_id: Declared regression test task ID.
    regression_scenario_id: Declared regression scenario ID.

Returns:
    JSON with confirmation status and the bug coverage gate readiness.

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

## `okto_pulse_list_design_systems`

List global and board-scoped design systems visible to the board.

Args:
    board_id: Board ID.
    scope: Optional filter such as global or inline.

Returns:
    JSON list with design system metadata and board-link/default flags.

## `okto_pulse_get_design_system`

Read a design system, including its content body.

Args:
    board_id: Board ID used for access checks.
    design_system_id: Design system ID.

Returns:
    JSON with title, content, scope, tags, and timestamps.

## `okto_pulse_create_design_system`

Create a design system record and its content.

Global design systems can be linked to boards and used as defaults; inline
design systems belong to the current board.

Args:
    board_id: Board ID.
    title: Design system title.
    content: Markdown instructions or tokens for mockup generation.
    scope: global or inline.
    tags: Optional multi-value tags.

Returns:
    JSON with created design system.

## `okto_pulse_update_design_system`

Update design system title, content, tags, or active state.

Args:
    board_id: Board ID.
    design_system_id: Design system ID.
    title: Optional new title.
    content: Optional new content.
    tags: Optional replacement tags.

Returns:
    JSON with updated design system.

## `okto_pulse_delete_design_system`

Delete or deactivate a design system and remove board links.

Args:
    board_id: Board ID.
    design_system_id: Design system ID.

Returns:
    JSON success payload.

## `okto_pulse_set_default_design_system`

Set or clear the global default design system used by newly created boards.

Only design systems from the global catalog are eligible for default use.

Args:
    board_id: Board ID used for authentication.
    design_system_id: Global design system ID, or empty/CLEAR to remove default.

Returns:
    JSON with the active default design system reference.
