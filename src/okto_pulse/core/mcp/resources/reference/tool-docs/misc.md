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

## `okto_pulse_get_allowed_transitions`

Return the allowed lifecycle transitions for a story, ideation, refinement,
spec, card, or sprint from the Core SDLC registry — the same authority the move
tools/endpoints enforce. Use it to know which `status` values a move will
accept before calling `okto_pulse_move_*`.

Args:
    board_id: Board ID
    entity_type: One of: story, ideation, refinement, spec, card, sprint
    entity_id: Target entity ID
    current_status: Optional status to evaluate from (empty = the entity's
        current status)

Returns:
    JSON with target statuses plus gate, preconditions, capabilities, effects,
    stable reason codes and the registry source identifier.

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

Get execution context for a task card. The default `context_scope="all"`
aggregates card data with all relevant spec requirements and artifacts. The
additive `profile="full", context_scope="gate"` mode returns the bounded
pre-mutation gate/readiness slice plus a content-addressed manifest and
drilldowns.

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
    include_superseded: When "false" (default), superseded/revoked decisions are
        filtered out; set "true" for full decision history.
    profile: Response projection — one of: summary (default), detail, full,
        legacy. Use `summary` for exploration, `detail` plus follow-ups for
        bounded body reads, and `full` + `context_scope="gate"` before
        status-changing moves. See okto-pulse://reference/projection-profiles.
    context_scope: `all` (default, historical complete body) or `gate`
        (`profile="full"` only; bounded mandatory pre-mutation view).

Returns:
    JSON with task context selected by profile/scope.
    `reviewer_separation` projects the current caller's task-validation policy
    decision (`mode`, `allowed`, `warning`, creator/assignee/executor
    `conflicts`, and `source`); inspect it from
    `profile="full", context_scope="gate"` before validating.
    Test cards expose `test_card_operational_flow`; `gate_readiness` mirrors the
    active done-gate and cognitive-readiness verdict without mutating or skipping
    anything.

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

Get the local telemetry publish-health status (R5C-A).

Reports whether this install's anonymous usage publishing is healthy,
degraded, recovering, failing, stale, disabled, or unavailable — plus the
last success/failure timestamps, the scheduled next retry, and freshness.
Agent-facing twin of `GET /metrics/publish-health`; it reads the
install-local failure-state and is NOT board-scoped.

No parameters.

Returns:
    JSON health DTO — the allowlisted, redacted projection only: publish
    state, last success/failure details, and bounded reason codes. The
    install id appears solely as `install_id_redacted`; never a token/secret.

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

Preview reusable regression scenarios for a bug without mutating a content-locked spec.

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

The amendment/hotfix revision tools (`okto_pulse_create_amendment_revision`,
`okto_pulse_list_amendment_revisions`, `okto_pulse_get_amendment_revision`,
`okto_pulse_associate_amendment_revision_artifacts`,
`okto_pulse_transition_amendment_revision`,
`okto_pulse_confirm_amendment_coverage`) are documented in
`okto-pulse://reference/tool-docs/card`.

## `okto_pulse_mark_as_seen`

Mark one or more items as seen so they won't appear in list_my_mentions.
Use this after processing mentions to avoid seeing them again.

Args:
    board_id: Board ID (for access verification)
    item_ids: Multi-value item IDs to mark as seen (from list_my_mentions item_id
        field). Formats: okto-pulse://reference/multivalue.

Returns:
    JSON with count of newly marked items

## `okto_pulse_mark_resource_not_applicable`

Mark a tracked Resource Gate resource as not applicable through the MCP
channel. Architecture and Mockup are blocking resource types; their N/A marks
participate in completion gates. Knowledge Base is advisory: a KB N/A mark can
record applicability intent, but it is never required to unblock completion,
spec validation, or spec done.

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
the board's `reviewer_separation_mode` against task creator, assignee, and
executor conflicts. `enforce` returns the action-required code
`reviewer_separation_required` with remediation
`request_independent_task_validator` before any mutation. `warn` and `off`
continue and persist/return `reviewer_separation`; a legacy board with the
setting absent is explicitly `off` with `source=legacy_absent_compat`. It then
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
    JSON with validation result, outcome, threshold violations,
    `reviewer_separation`, and card routing. Under `enforce`, a conflict returns
    `outcome=action_required` in MCP Outcome V2 with code,
    conflicts/source facts, and remediation; no validation is persisted.

## `okto_pulse_list_design_systems`

List Design Systems (spec 3a006f65 / FR2, admin read). REST twin:
GET /design-systems. Perm: BOARD_READ.

Args:
    board_id: Board ID used for authentication.
    scope: `global` lists the global catalog; `inline` lists THIS board's
        inline Design Systems.

Returns:
    JSON list with design system metadata.

## `okto_pulse_get_design_system`

Get a Design System by id, including its payload (admin read). REST twin:
GET /design-systems/{id}. Perm: BOARD_READ.

Args:
    board_id: Board ID used for access checks.
    design_system_id: Design system ID.

Returns:
    JSON with the Design System (title, scope, payload, status, version, timestamps).

## `okto_pulse_create_design_system`

Create a Design System (spec 3a006f65 / FR1, admin write). REST twin:
POST /design-systems. Perm: SPECS_UPDATE.

`scope="global"` creates a catalog entry that can be linked to any board or
made a template default; `scope="inline"` binds it to THIS board (board_id).
An inline Design System can never be a global default.

Args:
    board_id: Board ID.
    title: Design system title.
    scope: `global` (default) or `inline`.
    payload: Design System payload dict (tokens/instructions).
    status: Lifecycle status (default `active`).

Returns:
    JSON with the created Design System.

## `okto_pulse_update_design_system`

Update a Design System (admin write); a title/payload change bumps the version.
REST twin: PATCH /design-systems/{id}. Perm: SPECS_UPDATE.

Args:
    board_id: Board ID.
    design_system_id: Design system ID.
    title: Optional new title.
    payload: Optional replacement payload dict.
    status: Optional new lifecycle status.

Returns:
    JSON with the updated Design System.

## `okto_pulse_delete_design_system`

Delete a Design System (admin write). REST twin: DELETE /design-systems/{id}.
Perm: SPECS_UPDATE.

Args:
    board_id: Board ID.
    design_system_id: Design system ID.

Returns:
    JSON success payload.

## `okto_pulse_set_default_design_system`

Set the Design System default reference + canonical gate mode on a default
board-configuration TEMPLATE (spec 3a006f65 / FR3, admin write). REST twin:
POST /default-board-configurations/{template_id}/design-system. Perm: SPECS_UPDATE.

The `design_system_id` must be a real, global, active DesignSystem —
inline/synthetic references are rejected fail-closed. An ACTIVE template is
copy-on-write: the change lands on a new template version.

Args:
    board_id: Board ID used for authentication.
    template_id: Default board-configuration template ID.
    design_system_id: Global active Design System ID.
    gate_mode: Mockup Design System gate mode — `off` (default), `advisory`,
        or `blocking`.
    version: Optional Design System version to pin.
    snapshot: Optional snapshot payload.

Returns:
    JSON with the EFFECTIVE template and its design_system_default_ref.
