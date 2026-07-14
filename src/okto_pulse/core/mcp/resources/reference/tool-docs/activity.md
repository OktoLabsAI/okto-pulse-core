---
version: "1.0"
---

# Tool docs — `activity`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_get_activity_log`

Get the activity log (history) for the board with optional filtering and pagination.

Ideação MCP-token-optimization Story 3: default response carries id, action,
trigger, card_id, created_at + a deterministic `summary` string built
server-side from details — ~120B per row vs ~1.5KB. Pass include_details=true
to receive the full nested details object (legacy shape).

Cursor-pagination follow-up: pass ``cursor`` (opaque base64 from a prior
``next_cursor``) for O(1) keyset pagination independent of page depth.
Pass ``envelope=true`` to receive ``{items, next_cursor}`` instead of a
raw list (default keeps Story 3 list shape — backward compat). Legacy
``offset`` is silently ignored unless the edition enables ``mcp_legacy_offset``.

Args:
    board_id: Board ID
    limit: Maximum number of entries to return (default 50, max 200)
    cursor: Opaque continuation token from a previous call's ``next_cursor``.
        Empty string = first page. Invalid cursor returns a structured error.
    envelope: When true, response is ``{items: [...], next_cursor: str|null}``.
        Default false returns raw list (preserves Story 3 contract).
    offset: DEPRECATED — silently ignored unless the edition enables
        mcp_legacy_offset.
    action: Filter by action type (optional) — e.g. card_created, card_moved, spec_updated
    card_id: Filter by card ID (optional) — only activities for this card
    include_details: When true, include the full `details` object on each row.
        Default false returns the minimal envelope only.

Returns:
    JSON list (default) or ``{items, next_cursor}`` dict when envelope=true.
    Invalid cursor returns ``{error, error_code: "invalid_cursor"}``.
