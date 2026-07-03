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

Provide exactly ONE of: content_base64 or content_reference.

Args:
    board_id: Board ID
    card_id: Card ID
    filename: Original filename
    content_base64: File content encoded as base64 (use for small files only)
    content_reference: Runtime-specific reference resolved by the active edition
    mime_type: MIME type of the file

Returns:
    JSON with attachment details
