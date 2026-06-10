---
version: "1.0"
---

# Tool docs — `attachment`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_delete_attachment`

Delete an attachment.

Args:
    board_id: Board ID
    attachment_id: Attachment ID

Returns:
    JSON with success status

## `okto_pulse_list_attachments`

List all attachments on a card.

Args:
    board_id: Board ID
    card_id: Card ID

Returns:
    JSON array of attachments

## `okto_pulse_upload_attachment`

Upload a file attachment to a card.

Provide exactly ONE of: content_base64, file_path, or file_url. Prefer
file_path or file_url for binary files — the bytes are loaded server-side
and never pass through the LLM context, saving tokens.

Args:
    board_id: Board ID
    card_id: Card ID
    filename: Original filename
    content_base64: File content encoded as base64 (use for small files only)
    mime_type: MIME type of the file
    file_path: Absolute path to a local file on the MCP server host
    file_url: HTTP(S) URL of a file to fetch

Returns:
    JSON with attachment details
