---
version: "1.0"
---

# Tool docs — `ideation`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_answer_ideation_question`

Answer a question on an ideation's Q&A board.
For text questions, provide answer. For choice questions, provide selected option IDs.

Args:
    board_id: Board ID
    ideation_id: Ideation ID (for context/validation)
    qa_id: Q&A item ID to answer
    answer: Free-text answer (for text questions, or additional text on choice questions with allow_free_text)
    selected: Option IDs for choice questions — multi-value; formats:
        okto-pulse://reference/multivalue.

Returns:
    JSON with updated Q&A item

## `okto_pulse_ask_ideation_choice_question`

Ask a choice question (poll/form) on an ideation's Q&A board.

Args:
    board_id: Board ID
    ideation_id: Ideation ID
    question: The question text
    options: Option labels — multi-value; formats:
        okto-pulse://reference/multivalue.
    options_json: Preferred structured options. Pass a native array such as
        [{"label":"Safer path","recommended":true,"tradeoff":"More setup"}].
        A JSON-array string is accepted for compatibility. Each item requires
        label; recommended defaults to false and tradeoff to null. A non-empty
        options_json takes precedence over options.
    question_type: "choice" for single-select (default) or "multi_choice" for multi-select
    allow_free_text: "true" to also allow a free-text response alongside selections

Returns:
    JSON with Q&A item including choices

## `okto_pulse_ask_ideation_question`

Ask a question on an ideation's Q&A board. Use @Name to direct the question.

Args:
    board_id: Board ID
    ideation_id: Ideation ID
    question: Question text (use @Name to mention someone)

Returns:
    JSON with Q&A item details

## `okto_pulse_convert_stories_to_ideation`

Create a new Ideation or link an existing Ideation from selected Stories.

## `okto_pulse_create_ideation`

Create a new ideation on the board. Ideations are the starting point — raw ideas that may be
evaluated, refined into refinements, and eventually derived into specs.

Args:
    board_id: Board ID
    title: Ideation title
    description: High-level description of the idea (optional)
    problem_statement: What problem does this idea solve? (optional)
    proposed_approach: How might this be implemented? (optional)
    assignee_id: User/agent ID to assign (optional)
    labels: Multi-value labels — formats: okto-pulse://reference/multivalue. (optional)

Returns:
    JSON with created ideation details

## `okto_pulse_delete_ideation`

Delete an ideation. Linked refinements and Q&A are also deleted (cascade).

Args:
    board_id: Board ID
    ideation_id: Ideation ID

Returns:
    JSON with success status and a governed `takedown` receipt. Cascaded
    refinement/spec/sprint receipts are exposed recursively under
    `takedown.descendant_deletions`.

## `okto_pulse_delete_ideation_question`

Delete a Q&A item from an ideation. Use this to invalidate outdated questions
or remove resolved clarifications that no longer apply.

Args:
    board_id: Board ID
    ideation_id: Ideation ID (for context/logging)
    qa_id: Q&A item ID to delete

Returns:
    JSON with success status

## `okto_pulse_evaluate_ideation`

Evaluate an ideation's scope and compute its complexity (small/medium/large).
Set scope assessment scores (1-5) for each dimension WITH justification, then the system computes complexity.
- Any score >= 3 -> large (needs refinements before spec)
- Any score >= 2 -> medium
- All scores 1 -> small (can derive spec directly)

Each score MUST include a justification explaining why that score was given.

PRE-REQUISITE — ideation status MUST be 'evaluating' before calling this tool.
The full transition flow is: draft → review → approved → evaluating → (this tool) → done.
If the ideation is in any other status this call fails with:
    "Evaluation can only be performed in 'evaluating' status (current: '<status>'). ..."
Use okto_pulse_move_ideation(..., status='review'/'approved'/'evaluating') to advance through
the states first. The tool does NOT auto-promote because each transition is an explicit
gate decision (review = ready for stakeholder approval; approved = ready to score; evaluating
= scoring in progress).

Args:
    board_id: Board ID
    ideation_id: Ideation ID
    domains: Number of domains/systems affected, 1-5
    domains_justification: Why this score — which systems are impacted
    ambiguity: Level of requirement ambiguity, 1-5
    ambiguity_justification: Why this score — what is unclear or well-defined
    dependencies: External dependencies/coordination needed, 1-5
    dependencies_justification: Why this score — what dependencies exist

Returns:
    JSON with the computed complexity and scope assessment

## `okto_pulse_get_ideation`

Get full details of an ideation including its refinements, specs, and Q&A items.

Args:
    board_id: Board ID
    ideation_id: Ideation ID

Returns:
    JSON with ideation details and linked entities

## `okto_pulse_get_ideation_context`

Get the FULL consolidated context of an ideation. Returns all data needed
to evaluate, review, or derive refinements/specs from this ideation.

**Always call this before evaluating, moving, or deriving from an ideation.**

Args:
    board_id: Board ID
    ideation_id: Ideation ID
    include_knowledge: Include knowledge base entries (default "true")
    include_mockups: Include screen mockups (default "true")
    include_qa: Include Q&A items (default "true")
    include_architecture: Include Architecture Designs (default "true")

Returns:
    JSON with complete ideation context: details + Q&A + mockups + KBs + refinements + specs + evaluation

## `okto_pulse_get_ideation_history`

Get the detailed change history of an ideation. Shows every modification with field-level diffs,
who made the change, and when.

Args:
    board_id: Board ID
    ideation_id: Ideation ID
    limit: Maximum number of history entries to return (default 30)

Returns:
    JSON with list of history entries, newest first

## `okto_pulse_get_ideation_snapshot`

Get the full immutable snapshot of an ideation at a specific version.
Includes all fields as they were when the ideation was marked 'done',
plus a snapshot of all Q&A at that point.

Args:
    board_id: Board ID
    ideation_id: Ideation ID
    version: Version number to retrieve

Returns:
    JSON with complete snapshot including Q&A history

## `okto_pulse_link_story_to_ideation`

Link a Story to one Ideation; multiple Stories may feed the same Ideation.

## `okto_pulse_move_ideation`

Change an ideation's status (draft -> review -> approved -> evaluating -> done).

Allowed transitions:
- draft → review, cancelled
- review → draft, approved, cancelled
- approved → review, evaluating, cancelled
- evaluating → approved, done, cancelled
- done → draft (new version)
- cancelled → draft (new version; cancellation record cleared)

Args:
    board_id: Board ID
    ideation_id: Ideation ID
    status: New status — one of: draft, review, approved, evaluating, done, cancelled
    cancellation_reason: REQUIRED when status=cancelled; reopening clears it.

Returns:
    JSON with updated ideation status

## `okto_pulse_set_ideation_ambiguity_gate_skip`

Human-only control (R5-IMP1). Applying or removing the per-ideation Max ambiguity
gate skip is a human decision and is NOT available from this agent-facing tool: it
fails closed with `human_control_required` (mutation_allowed=false,
state_changed=false) and never changes `skip_ambiguity_gate`.

When the board-level Max ambiguity gate blocks an `evaluating` -> `done`
transition, an agent should surface the read-only state and request a human
decision. A human operator applies the skip through the authorized UI control or
the human REST endpoint (`PATCH /api/v1/ideations/{ideation_id}/ambiguity-gate-skip`).

Args:
    board_id: Board ID.
    ideation_id: Ideation ID.
    skip_ambiguity_gate: Ignored on the agent surface (human-only control).

Returns:
    JSON `human_control_required` envelope (read-only; no mutation, no state change).

## `okto_pulse_update_ideation`

Update an ideation's fields. Content changes bump the version. Only non-empty fields are updated.

Args:
    board_id: Board ID
    ideation_id: Ideation ID
    title: New title (optional, empty = no change)
    description: New description (optional, empty = no change)
    problem_statement: New problem statement (optional, empty = no change)
    proposed_approach: New proposed approach (optional, empty = no change)
    assignee_id: New assignee (optional, empty = no change)
    labels: Multi-value labels — formats: okto-pulse://reference/multivalue. (optional, empty = no change)

Returns:
    JSON with updated ideation details
