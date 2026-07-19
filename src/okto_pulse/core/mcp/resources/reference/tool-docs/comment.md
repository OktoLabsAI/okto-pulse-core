---
version: "1.0"
---

# Tool docs — `comment`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_add_choice_comment`

Add a choice board (poll) to a card. Responders can select from the options.

Args:
    board_id: Board ID
    card_id: Card ID
    question: The question or prompt text displayed above the options
    options: Option labels — multi-value; formats:
        okto-pulse://reference/multivalue.
    options_json: Preferred structured options. Pass a native array such as
        [{"label":"Safer path","recommended":true,"tradeoff":"More setup"}].
        A JSON-array string is accepted for compatibility. Each item requires
        label; recommended defaults to false and tradeoff to null. A non-empty
        options_json takes precedence over options.
    comment_type: "choice" for single-select (default) or "multi_choice" for multi-select
    allow_free_text: "true" to allow a free-text response in addition to selections

Returns:
    JSON with the created choice comment

## `okto_pulse_add_comment`

Add a comment to a card.

Args:
    board_id: Board ID
    card_id: Card ID
    content: Comment text. Supports Markdown and Mermaid diagrams (```mermaid code blocks).

Returns:
    JSON with comment details

## `okto_pulse_delete_comment`

Delete the agent's own comment.

Args:
    board_id: Board ID
    comment_id: Comment ID

Returns:
    JSON with success status

## `okto_pulse_list_comments`

List all comments on a card.

Args:
    board_id: Board ID
    card_id: Card ID

Returns:
    JSON array of comments

## `okto_pulse_update_comment`

Update the agent's own comment.

Args:
    board_id: Board ID
    comment_id: Comment ID
    content: New comment text

Returns:
    JSON with updated comment
