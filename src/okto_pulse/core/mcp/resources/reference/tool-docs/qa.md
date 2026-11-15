---
version: "1.0"
---

# Tool docs — `qa`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_ask`

Consolidated Q&A "ask a question" tool (R4). Dispatches on `target_type`.

Args:
    board_id: Board ID
    target_type: One of `card` | `ideation` | `refinement` | `spec`
    parent_id: The id of that work item
    question: The question text (use `@Name` to direct it)

Returns:
    JSON `{success, qa:{id, question, asked_by}}`. Unsupported `target_type`
    returns `{error:"unsupported_target_type", allowed:[…]}` (no mutation).

Each target retains its own authorization and activity log.
See `okto-pulse://reference/tool-families/qa_ask`.

## Choice questions and recommendations

For Ideation and Refinement, `answered_at` is the authoritative completion
predicate. A selected choice with no text is answered when that timestamp is
present; a suggested answer without it is still pending. Answers are accepted
in Draft and Review. Entering Review is allowed with pending questions;
approval, finalization and derivation require every question to be answered.
`planning_pending_questions` reports the exact count, at most 50 pending IDs,
`truncated`, and the next action. Read `qa_readiness` from existing context to
avoid extra per-question calls. A historical incomplete source must be reopened
as a new edition before derivation; do not auto-answer questions, synthesize
timestamps, or edit its old snapshots. Propagation retains unanswered questions
and the original answer timestamps, including a missing timestamp.

Use the subject's `ask_*_choice_question` tool for structured alternatives;
`okto_pulse_ask` creates a text question. Send a native `options` array with
mutually exclusive labels, a boolean `recommended`, and a concrete `tradeoff`.
When recommending one option, mark exactly that option `recommended: true`.
Persist the answer separately using the returned option ID and the subject's
answer tool. Neither recommendation nor a label containing “Recommended” is
an answer; never assume generated option IDs or silently select an option.

Example for `okto_pulse_ask_spec_choice_question` (substitute current IDs):

<!-- contract-example: okto_pulse_ask_spec_choice_question -->
```json
{
  "board_id": "board-example",
  "spec_id": "spec-example",
  "question": "When should the agent record progress?",
  "question_type": "choice",
  "allow_free_text": false,
  "options": [
    {"label": "At meaningful milestones", "recommended": true, "tradeoff": "Preserves resumable context without repeated heartbeats."},
    {"label": "After every tool call", "recommended": false, "tradeoff": "Adds repeated records and token cost without new delivery proof."}
  ]
}
```

Ambiguity `proposed_questions` are a different schema: `choices` contains
strings, not these option objects. The assessment creates pending native Q&A
in the same transaction (duplicate text may reuse existing Q&A). Those strings
cannot carry recommendation metadata. When recommendations are required,
create the native choice question above and omit the duplicate proposed
question from the assessment. Answer pending questions before closeout;
do not delete them merely to satisfy a gate.

## `okto_pulse_answer_question`

Answer a question on a card's Q&A board.

Args:
    board_id: Board ID
    qa_id: Q&A item ID
    answer: Answer text

Returns:
    JSON with updated Q&A details

## `okto_pulse_ask` (`target_type="card"`)

Use `okto_pulse_ask` with `target_type="card"` and `parent_id` naming the target.
See `okto-pulse://reference/tool-families/qa_ask` for parameters, authorization and results.

## `okto_pulse_delete_question`

Delete a Q&A item from a card.

Args:
    board_id: Board ID
    qa_id: Q&A item ID

Returns:
    JSON with success status

## `okto_pulse_get_choice_responses`

Get all responses for a choice board comment.

Args:
    board_id: Board ID
    comment_id: Comment ID of the choice board

Returns:
    JSON with the choice options and all responses

## `okto_pulse_respond_to_choice`

Respond to a choice board comment by selecting one or more options.

Args:
    board_id: Board ID
    comment_id: Comment ID of the choice board
    selected: Option IDs to select — multi-value; formats:
        okto-pulse://reference/multivalue.
    free_text: Optional free-text response (only if allow_free_text is enabled)

Returns:
    JSON with the updated comment including all responses
