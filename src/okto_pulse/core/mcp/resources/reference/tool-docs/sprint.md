---
version: "1.0"
---

# Tool docs — `sprint`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_answer_sprint_question`

Answer a question on a sprint.

Args:
    board_id: Board ID
    sprint_id: Sprint ID
    qa_id: Q&A item ID
    answer: Answer text

Returns:
    JSON with updated Q&A item

## `okto_pulse_ask_sprint_question`

Ask a question on a sprint.

Args:
    board_id: Board ID
    sprint_id: Sprint ID
    question: Question text

Returns:
    JSON with created Q&A item

## `okto_pulse_assign_tasks_to_sprint`

Assign cards to a sprint. Cards must belong to the same spec as the sprint.

Args:
    board_id: Board ID
    sprint_id: Sprint ID
    card_ids: Multi-value card IDs to assign. Preferred native list (e.g.
        ``["uuid_a", "uuid_b"]``); legacy string accepted as JSON array or
        pipe-separated. Comma-only string is REJECTED. See
        ``okto_pulse.core.mcp.helpers.coerce_to_list_str``.

Returns:
    JSON with number of cards assigned

## `okto_pulse_create_sprint`

Create a new sprint for a spec. Sprints break specs into incremental deliverables.

Args:
    board_id: Board ID
    spec_id: Spec ID this sprint belongs to
    title: Sprint title
    description: Sprint description with scope and deliverables (optional)
    objective: What this sprint aims to achieve (optional but recommended)
    expected_outcome: What success looks like when this sprint is done (optional but recommended)
    test_scenario_ids: Multi-value spec test scenario IDs scoped to this sprint —
        preferred native list; legacy string accepted as JSON array or pipe-separated.
        Comma-only string is REJECTED. (optional)
    business_rule_ids: Multi-value spec business rule IDs scoped to this sprint —
        preferred native list; legacy string accepted as JSON array or pipe-separated.
        Comma-only string is REJECTED. (optional)
    start_date: ISO date string (optional)
    end_date: ISO date string (optional)
    labels: Multi-value labels — preferred native list (e.g. ``["backend", "api"]``);
        legacy string accepted as JSON array or pipe-separated. Comma-only string
        is REJECTED. See ``okto_pulse.core.mcp.helpers.coerce_to_list_str``. (optional)

Returns:
    JSON with created sprint details

## `okto_pulse_delete_sprint_evaluation`

Delete your own sprint evaluation.

Args:
    board_id: Board ID
    sprint_id: Sprint ID
    evaluation_id: Evaluation ID to delete

Returns:
    JSON with success or error

## `okto_pulse_delete_sprint_question`

Delete a Q&A item from a sprint. Use this to invalidate outdated questions
or remove resolved clarifications that no longer apply.

Args:
    board_id: Board ID
    sprint_id: Sprint ID (for context/logging)
    qa_id: Q&A item ID to delete

Returns:
    JSON with success status

## `okto_pulse_get_sprint`

Get full sprint details including cards, evaluations, and Q&A.

Args:
    board_id: Board ID
    sprint_id: Sprint ID

Returns:
    JSON with full sprint details

## `okto_pulse_get_sprint_context`

Get the FULL consolidated context of a sprint. Returns sprint data plus
the parent spec's structured sections (requirements, test scenarios, BRs,
contracts) for scope resolution and evaluation.

**Always call this before evaluating, moving, or reviewing a sprint.**

Args:
    board_id: Board ID
    sprint_id: Sprint ID
    include_spec: Include parent spec context with all structured data (default "true")

Returns:
    JSON with complete sprint context: details + cards + evaluations + Q&A + parent spec + scope

## `okto_pulse_get_sprint_evaluation`

Get full details of a specific sprint evaluation.

Args:
    board_id: Board ID
    sprint_id: Sprint ID
    evaluation_id: Evaluation ID

Returns:
    JSON with full evaluation details

## `okto_pulse_list_sprint_evaluations`

List all evaluations for a sprint.

Args:
    board_id: Board ID
    sprint_id: Sprint ID

Returns:
    JSON with evaluations list and summary

## `okto_pulse_move_sprint`

Move a sprint to a new status. State machine: draft→active→review→closed.
Gates: draft→active requires cards, active→review requires scoped test coverage, review→closed requires evaluation.

Args:
    board_id: Board ID
    sprint_id: Sprint ID
    status: New status — one of: draft, active, review, closed, cancelled

Returns:
    JSON with updated sprint details

## `okto_pulse_submit_sprint_evaluation`

Submit a qualitative evaluation for a sprint in 'review' status.

Args:
    board_id: Board ID
    sprint_id: Sprint ID (must be in 'review' status)
    breakdown_completeness: Score 0-100 — do tasks cover the sprint scope?
    breakdown_justification: Why this score
    granularity: Score 0-100 — are tasks properly sized?
    granularity_justification: Why this score
    dependency_coherence: Score 0-100 — do task dependencies make sense?
    dependency_justification: Why this score
    test_coverage_quality: Score 0-100 — do tests cover happy path and edge cases?
    test_coverage_justification: Why this score
    overall_score: Overall score 0-100
    overall_justification: Overall assessment
    recommendation: One of: approve, request_changes, reject

Returns:
    JSON with evaluation ID and sprint summary

## `okto_pulse_suggest_sprints`

Suggest a sprint breakdown for a spec based on tasks, FRs, and dependencies.
Does NOT create sprints — returns suggestions for review.

Args:
    board_id: Board ID
    spec_id: Spec ID
    threshold: Max tasks per sprint (default 8)

Returns:
    JSON with list of suggested sprints (title, card_ids, test_scenario_ids, business_rule_ids)

## `okto_pulse_update_sprint`

Update sprint fields.

Args:
    board_id: Board ID
    sprint_id: Sprint ID
    title: New title (optional, empty = no change)
    description: New description (optional)
    test_scenario_ids: Multi-value scoped test scenario IDs — preferred native list;
        legacy string accepted as JSON array or pipe-separated. Comma-only string is
        REJECTED. (optional)
    business_rule_ids: Multi-value scoped business rule IDs — preferred native list;
        legacy string accepted as JSON array or pipe-separated. Comma-only string is
        REJECTED. (optional)
    labels: Multi-value labels — preferred native list (e.g. ``["backend", "api"]``);
        legacy string accepted as JSON array or pipe-separated. Comma-only string
        is REJECTED. See ``okto_pulse.core.mcp.helpers.coerce_to_list_str``. (optional)
    skip_test_coverage: "true" or "false" (optional)
    skip_rules_coverage: "true" or "false" (optional)
    skip_qualitative_validation: "true" or "false" (optional)

Returns:
    JSON with updated sprint details
