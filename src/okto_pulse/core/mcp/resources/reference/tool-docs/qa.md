---
version: "1.0"
---

# Tool docs — `qa`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_ask`

Consolidated Q&A "ask a question" tool (R4). Dispatches on `target_type`.

Args:
    board_id: Board ID
    target_type: One of `card` | `ideation` | `refinement` | `spec` | `sprint`
    parent_id: The id of that work item
    question: The question text (use `@Name` to direct it)

Returns:
    JSON `{success, qa:{id, question, asked_by}}`. Unsupported `target_type`
    returns `{error:"unsupported_target_type", allowed:[…]}` (no mutation).

The legacy per-type tools (`okto_pulse_ask_question`, `…_ask_ideation_question`,
`…_ask_refinement_question`, `…_ask_spec_question`, `…_ask_sprint_question`) remain
as aliases and delegate to the same implementation. Note: `sprint` is asymmetric —
no `QA_CREATE` permission gate and no activity-log write. Full family contract:
`okto-pulse://reference/tool-families/qa_ask`.

## `okto_pulse_answer_question`

Answer a question on a card's Q&A board.

Args:
    board_id: Board ID
    qa_id: Q&A item ID
    answer: Answer text

Returns:
    JSON with updated Q&A details

## `okto_pulse_ask_question`

Add a question to a card's Q&A board.

Args:
    board_id: Board ID
    card_id: Card ID
    question: Question text

Returns:
    JSON with Q&A item details

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
    selected: Option IDs to select, accepted in three formats:
        ``'["opt_0", "opt_2"]'`` (JSON array, preferred), ``"opt_0|opt_2"``
        (pipe-separated), or ``"opt_0,opt_2"`` (legacy comma-separated).
        See ``okto_pulse.core.mcp.helpers.parse_multi_value``.
    free_text: Optional free-text response (only if allow_free_text is enabled)

Returns:
    JSON with the updated comment including all responses
