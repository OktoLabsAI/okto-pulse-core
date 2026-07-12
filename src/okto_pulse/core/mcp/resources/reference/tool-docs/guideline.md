---
version: "1.0"
---

# Tool docs — `guideline`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_create_guideline`

Create a new guideline. If scope is "global", it goes into the catalog and can be
linked to any board. If scope is "inline", set a board_id to make it board-specific.

Args:
    board_id: Board ID (used for authentication; also used as guideline board_id if scope is "inline")
    title: Guideline title
    content: Guideline content (Markdown supported)
    tags: Pipe-separated tags (e.g. "coding|architecture") — empty = no tags
    scope: "global" (catalog) or "inline" (board-specific)

Returns:
    JSON with created guideline

## `okto_pulse_delete_guideline`

Delete a guideline. Also removes all board links.

Args:
    board_id: Board ID (used for authentication)
    guideline_id: Guideline ID to delete

Returns:
    JSON with success status

## `okto_pulse_get_board_guidelines`

Get all guidelines for a board, ordered by priority. This is the PRIMARY tool
for reading board guidelines — call it BEFORE doing any work on a board.

Returns linked global guidelines and inline board guidelines merged and sorted.

Args:
    board_id: Board ID

Returns:
    JSON with list of guidelines sorted by priority (highest first)

## `okto_pulse_link_guideline_to_board`

Link a global guideline to a board so agents see it when loading board guidelines.

Args:
    board_id: Board ID
    guideline_id: Guideline ID to link
    priority: Priority order (higher = more important, default 0)

Returns:
    JSON with link details

## `okto_pulse_list_guidelines`

List global guidelines from the catalog. Use this to browse available guidelines
that can be linked to boards.

Args:
    board_id: Board ID (used for authentication)
    offset: Pagination offset (default 0)
    limit: Max results (default 50)
    tag: Optional tag filter (empty = all)

Returns:
    JSON with list of global guidelines

## `okto_pulse_list_default_guideline_candidates`

List GLOBAL catalog guidelines with derived eligibility and current default
status from the umbrella template (spec 8a2fad91 / FR1, admin read). REST twin:
GET /guidelines/default-candidates. Perm: BOARD_READ.

Only global guidelines can become defaults; inline board guidelines are never
eligible.

Args:
    board_id: Board ID used for authentication.
    scope: Template scope (default `global`).
    template_id: Optional — inspect a specific template version; empty uses
        the active template.

Returns:
    JSON with candidate guidelines and current default selection state.

## `okto_pulse_update_default_guideline_refs`

Update a template's `guideline_default_refs` using only GLOBAL catalog
guidelines (spec 8a2fad91 / FR1, admin write). REST twin: POST
/default-board-configurations/{template_id}/guidelines. Perm: SPECS_UPDATE.

Inline/missing/non-global refs are rejected fail-closed (structured error).
An ACTIVE template is copy-on-write: a new version is created and activated;
a draft mutates in-place.

Args:
    board_id: Board ID used for authentication.
    template_id: Default board-configuration template ID.
    guideline_default_refs: List of global guideline refs to set (empty list
        clears the defaults).

Returns:
    JSON with the EFFECTIVE template (including its default guideline refs).

## `okto_pulse_update_board_guideline_priority`

Update the priority of a guideline linked to a board. Higher priority means
the guideline sorts first in `okto_pulse_get_board_guidelines`.

Args:
    board_id: Board ID
    guideline_id: Linked guideline ID
    priority: New priority (higher = more important)

Returns:
    JSON with the updated board-guideline link.

## `okto_pulse_unlink_guideline_from_board`

Unlink a guideline from a board. The guideline itself is not deleted.

Args:
    board_id: Board ID
    guideline_id: Guideline ID to unlink

Returns:
    JSON with success status

## `okto_pulse_update_guideline`

Update a guideline's title, content, or tags.

Args:
    board_id: Board ID (used for authentication)
    guideline_id: Guideline ID to update
    title: New title (empty = no change)
    content: New content (empty = no change)
    tags: New pipe-separated tags (empty = no change)

Returns:
    JSON with updated guideline
