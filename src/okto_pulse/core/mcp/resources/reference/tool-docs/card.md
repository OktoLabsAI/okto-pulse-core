---
version: "1.0"
---

# Tool docs — `card`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_add_card_dependency`

Add a dependency: card_id cannot advance until depends_on_id is done/cancelled.
Circular dependencies are blocked automatically.

Args:
    board_id: Board ID
    card_id: The card that will be blocked
    depends_on_id: The card it depends on

Returns:
    JSON with success or error

## `okto_pulse_copy_qa_to_card`

Copy answered Q&A items from a spec to a card as a consolidated comment.
Only copies Q&As that have been answered — unanswered questions are skipped.

Args:
    board_id: Board ID
    spec_id: Source spec ID
    card_id: Target card ID

Returns:
    JSON with count of Q&A entries copied

## `okto_pulse_create_card`

Create a new card on the board. Every card MUST be linked to a spec.

Args:
    board_id: Board ID
    title: Card title
    spec_id: REQUIRED — Spec ID to link this card to. Normal/bug cards
        are allowed when the spec is approved, in_progress, or done.
        Test cards are allowed once the spec is approved/validated or
        later, including regression tests for a bug on a locked spec.
        For bug cards, this is auto-resolved from the origin task if not provided.
    description: Card description (optional). Supports Markdown and Mermaid diagrams (```mermaid code blocks).
    details: Card details/rich text (optional). Supports Markdown and Mermaid diagrams.
    status: Card status - one of: not_started, started, in_progress, validation, on_hold, done, cancelled
    priority: Card priority - one of: none, low, medium, high, very_high, critical (default: none)
    assignee_id: User ID to assign (optional)
    labels: Multi-value labels — preferred native list (e.g. ``["bug", "frontend"]``);
        legacy string accepted as JSON array ``'["bug", "frontend"]'`` or
        pipe-separated ``"bug|frontend"``. Comma-only string is REJECTED.
        See ``okto_pulse.core.mcp.helpers.coerce_to_list_str``.
    test_scenario_ids: Multi-value test scenario IDs (e.g. ``["ts_abc", "ts_def"]``)
        — same input shapes as ``labels`` above. For test cards, this is MANDATORY.
        When provided, automatically creates bidirectional links between the
        card and the scenarios. Linking to an existing scenario is a
        traceability update that leaves validated spec content unchanged, but
        for bug regression Path A the scenario must be
        eligible by lineage: same spec and linked to the bug origin task or an
        explicitly supplied affected task.
    card_type: Card type - "normal" (default), "test", or "bug".
        Test cards require test_scenario_ids and do not use
        submit_task_validation; complete them with move_card(..., done)
        plus conclusion/evidence. Bug regression coverage may reuse an
        eligible existing scenario after spec validation, but the linked test
        card itself must be created after the bug. On a validated/done spec,
        update the scenario status/evidence only after that test card is
        executable (`started`, `in_progress`, `validation`, or `done`) so the
        update is treated as operational evidence rather than semantic spec
        editing. If no eligible scenario
        exists or expected behavior changed, treat it as a semantic gap and
        route to amendment, refinement, spec revision, or hotfix spec instead
        of editing the current spec. Bug cards require origin_task_id,
        severity, expected_behavior, and observed_behavior.
    origin_task_id: REQUIRED for bug cards — ID of the task that originated the bug. The spec is auto-resolved from this task.
    severity: REQUIRED for bug cards — one of: critical, major, minor
    expected_behavior: REQUIRED for bug cards — what should happen
    observed_behavior: REQUIRED for bug cards — what actually happens
    steps_to_reproduce: Steps to reproduce the bug (optional)
    action_plan: Plan for fixing the bug (optional)

Returns:
    JSON with created card details

Bug regression decision rule:
    Path A is traceability-only reuse. Use
    okto_pulse_resolve_bug_regression_scenarios before creating/linking the
    regression test card when eligibility is not obvious. The response exposes
    eligible_scenarios, rejected_scenarios, semantic_gap_required,
    spec_mutation_required, and next_action. Only eligible scenarios may satisfy
    require_test_task_for_bug.

    Path B is semantic gap remediation. Same-spec membership by itself, title
    similarity, cross-spec scenarios, or scenario_not_found do not satisfy the
    bug gate. Escalate with next_action=escalate_semantic_gap by creating an
    amendment, refinement, spec revision, or hotfix spec. Do not move a spec directly from in_progress to approved for the simple Path A reuse case.

    Path C is hotfix execution. When a done spec or closed origin sprint blocks
    bug execution, use the returned next_action (`assign_hotfix_lane` or
    `activate_hotfix_lane`) to put the bug and its regression test card on an
    active `lane_type="hotfix"` sprint. Keep the original closed sprint
    unchanged.

## `okto_pulse_delete_card`

Delete a card from the board. This operation is permanent and cannot be undone.

## `okto_pulse_get_card`

Get detailed card information including attachments, Q&A, and comments.

## `okto_pulse_get_card_dependencies`

List cards that this card depends on and cards that depend on it.

Args:
    board_id: Board ID
    card_id: Card ID

Returns:
    JSON with depends_on (blockers) and dependents (blocked by this card)

## `okto_pulse_list_blockers`

Triage view of everything stalling the funnel, with root-cause classification.

Every returned entry carries a `type` so the agent can act directly:

- `dependency_blocked` — card is active while at least one `depends_on`
  target is not DONE.
- `on_hold` — card is explicitly paused (status=on_hold).
- `stale` — card is started/in_progress/validation and hasn't been
  touched for more than `stale_hours`.
- `spec_pending_validation` — spec is approved but has no 'approve'
  evaluation yet, blocking promotion to in_progress.
- `spec_no_cards` — spec is validated/in_progress but has zero linked
  cards (implementation hasn't started).
- `uncovered_scenario` — test scenario has no linked test card, so the
  test-coverage gate will fail.

Args:
    board_id: Board ID
    stale_hours: Cards unchanged longer than this while active are flagged
        as stale (default 72, ≥1).
    filter_type: Optional — return only blockers of this type. Empty returns all.

Returns:
    JSON ``{summary: {<type>: count}, total: int, blockers: [...]}``

## `okto_pulse_list_cards_by_status`

List cards on the board with optional filters and pagination.

status: empty = all, or one of not_started/started/in_progress/validation/on_hold/done/cancelled/open.
Use 'open' for all cards NOT in done/cancelled. Max limit is 200.

## `okto_pulse_move_card`

Move a card to a different column/position on the board.

Moving to 'validation' or 'done' REQUIRES conclusion, completeness (0-100),
completeness_justification, drift (0-100), and drift_justification so the
reviewer can validate the claim. Use -1 for completeness/drift when no
execution report is required (e.g. moving to on_hold or started).

## `okto_pulse_remove_card_dependency`

Remove a dependency between two cards.

Args:
    board_id: Board ID
    card_id: The card that has the dependency
    depends_on_id: The card it depended on

Returns:
    JSON with success status

## `okto_pulse_update_card`

Update card details. Pass only the fields you want to change; omit the rest.

Multi-value fields (labels, test_scenario_ids, linked_test_task_ids): prefer
native list; legacy pipe-separated string is also accepted. Comma-only strings
are REJECTED. For bidirectional scenario linking, use okto_pulse_link_task_to_scenario.
