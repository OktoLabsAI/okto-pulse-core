---
version: "1.0"
---

# Tool docs — `refinement`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_answer_refinement_question`

Answer a question on a refinement's Q&A board.
For text questions, provide answer. For choice questions, provide selected option IDs.

Args:
    board_id: Board ID
    refinement_id: Refinement ID (for context/validation)
    qa_id: Q&A item ID to answer
    answer: Free-text answer (for text questions, or additional text on choice questions with allow_free_text)
    selected: Option IDs for choice questions — multi-value; formats:
        okto-pulse://reference/multivalue.

Returns:
    JSON with updated Q&A item

## `okto_pulse_ask_refinement_choice_question`

Ask a choice question (poll/form) on a refinement's Q&A board.

Args:
    board_id: Board ID
    refinement_id: Refinement ID
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

## `okto_pulse_ask_refinement_question`

Ask a question on a refinement's Q&A board. Use @Name to direct the question.

Args:
    board_id: Board ID
    refinement_id: Refinement ID
    question: Question text (use @Name to mention someone)

Returns:
    JSON with Q&A item details

## `okto_pulse_create_refinement`

Create a new refinement for a DONE ideation. The ideation must be in 'done' status
(snapshotted) before refinements can be created. The parent ideation context
is always preserved; when description is provided, inherited context is appended.

Artifacts (mockups, KBs, Architecture Designs) from the ideation are
automatically propagated. Use mockup_ids/kb_ids/architecture_design_ids
to select specific ones (default: all).

Args:
    board_id: Board ID
    ideation_id: Ideation ID (must be in 'done' status)
    title: Refinement title
    description: Description of this refinement aspect (optional; parent ideation context is appended)
    in_scope: Pipe-separated list of what IS in scope (e.g. "Auth flow|Token refresh|Session management")
    out_of_scope: Pipe-separated list of what is NOT in scope (e.g. "UI changes|Email notifications")
    analysis: Detailed analysis text (optional)
    decisions: Pipe-separated list of decisions made (e.g. "Use REST API|Cache with Redis") (optional)
    assignee_id: User/agent ID to assign (optional)
    labels: Multi-value labels — formats: okto-pulse://reference/multivalue. (optional)
    mockup_ids: Pipe-separated mockup IDs to propagate from ideation (optional, empty = all)
    kb_ids: Pipe-separated KB IDs to propagate from ideation (optional, empty = all)
    architecture_design_ids: Multi-value Architecture Design IDs to propagate (optional, empty = all)
    architecture_propagation_mode: one of copy, derive, reference_only, none.
        "snapshot" is not accepted; copy/derive are the snapshot-copy modes,
        while reference_only/none keep only parent linkage.

Returns:
    JSON with created refinement details

## `okto_pulse_delete_refinement`

Delete a refinement. Linked Q&A items are also deleted (cascade).

Args:
    board_id: Board ID
    refinement_id: Refinement ID

Returns:
    JSON with success status

## `okto_pulse_delete_refinement_question`

Delete a Q&A item from a refinement. Use this to invalidate outdated questions
or remove resolved clarifications that no longer apply.

Args:
    board_id: Board ID
    refinement_id: Refinement ID (for context/logging)
    qa_id: Q&A item ID to delete

Returns:
    JSON with success status

## `okto_pulse_get_refinement`

Get full details of a refinement including its specs and Q&A items.

Args:
    board_id: Board ID
    refinement_id: Refinement ID

Returns:
    JSON with refinement details and linked entities

## `okto_pulse_get_refinement_context`

Get the FULL consolidated context of a refinement. Returns all data needed
to review, derive specs, or evaluate this refinement.

**Always call this before moving, evaluating, or deriving a spec from a refinement.**

Args:
    board_id: Board ID
    refinement_id: Refinement ID
    include_knowledge: Include knowledge base entries (default "true")
    include_mockups: Include screen mockups (default "true")
    include_qa: Include Q&A items (default "true")
    include_architecture: Include Architecture Designs (default "true")

Returns:
    JSON with complete refinement context: details + parent ideation + scope + Q&A + mockups + KBs + derived specs

## `okto_pulse_get_refinement_history`

Get the detailed change history of a refinement. Shows every modification with field-level diffs,
who made the change, and when.

Args:
    board_id: Board ID
    refinement_id: Refinement ID
    limit: Maximum number of history entries to return (default 30)

Returns:
    JSON with list of history entries, newest first

## `okto_pulse_get_refinement_snapshot`

Get the full immutable snapshot of a refinement at a specific version.
Includes all fields as they were when the refinement was marked 'done',
plus a snapshot of all Q&A at that point.

Args:
    board_id: Board ID
    refinement_id: Refinement ID
    version: Version number to retrieve

Returns:
    JSON with complete snapshot including Q&A history

## `okto_pulse_move_refinement`

Change a refinement's status (draft -> review -> approved -> done).

Allowed transitions:
- draft → review, cancelled
- review → draft, approved, cancelled
- approved → review, done, cancelled
- done → draft (new version)

Args:
    board_id: Board ID
    refinement_id: Refinement ID
    status: New status — one of: draft, review, approved, done, cancelled
    cancellation_reason: REQUIRED when status=cancelled; reopening clears it.

Returns:
    JSON with updated refinement status

## `okto_pulse_update_refinement`

Update a refinement's fields. Content changes bump the version. Only non-empty fields are updated.

Args:
    board_id: Board ID
    refinement_id: Refinement ID
    title: New title (optional, empty = no change)
    description: New description (optional, empty = no change)
    in_scope: Pipe-separated list of in-scope items (optional, empty = no change)
    out_of_scope: Pipe-separated list of out-of-scope items (optional, empty = no change)
    analysis: New analysis (optional, empty = no change)
    decisions: Pipe-separated list of decisions (optional, empty = no change)
    assignee_id: New assignee (optional, empty = no change)
    labels: Multi-value labels — formats: okto-pulse://reference/multivalue. (optional, empty = no change)

Returns:
    JSON with updated refinement details
