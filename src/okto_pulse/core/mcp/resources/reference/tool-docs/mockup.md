---
version: "1.0"
---

# Tool docs — `mockup`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_add_screen_mockup`

Add a screen mockup to a source entity (spec, ideation, refinement, or story).
Card mockups are read-only governed snapshots; use
`okto_pulse_copy_mockups_to_card` to refresh card context.
Screens contain HTML+Tailwind content that renders as visual mockups in the dashboard.

Args:
    board_id: Board ID
    entity_id: Entity ID (spec, ideation, refinement, or story)
    title: Screen title (e.g. "Login Page", "Dashboard", "Settings Modal")
    entity_type: Type of entity — one of: spec, ideation, refinement, story (default: spec)
    description: What this screen does and when it appears (optional). Supports Markdown.
    screen_type: Type of screen — one of: page, modal, drawer, popover, panel (default: page)
    html_content: HTML+Tailwind markup for the screen mockup. Script tags and on* event attributes are stripped for safety.
    design_system_ref: Design System ID this mockup complies with (see the
        board's effective Design System via `okto_pulse_get_board_design_system`).
    design_system_version: Optional Design System version number.
    design_system_evidence: Optional compliance evidence payload.

Design System gate (MockupDesignSystemGate, spec 3a006f65): when the board has
an effective Design System and `design_system_gate_mode=blocking`, an
invalid/missing ref is rejected BEFORE persistence (`design_system_required` /
`design_system_not_found` / `design_system_version_mismatch` /
`design_system_evidence_missing`); `advisory` persists and returns a
`design_system_gate` warning; `off` (or no Design System) does not block.

Returns:
    JSON with created screen including its generated ID

## `okto_pulse_annotate_mockup`

Add a design annotation/note to a screen mockup on a source entity. Card mockups
are read-only governed snapshots.

Args:
    board_id: Board ID
    entity_id: Entity ID (spec, ideation, refinement, or story)
    screen_id: Screen mockup ID
    text: Annotation text (design note, requirement, constraint)
    entity_type: Type of entity — one of: spec, ideation, refinement, story (default: spec)

Returns:
    JSON with created annotation

## `okto_pulse_copy_mockups_to_card`

Copy screen mockups from a spec to a card. Use this when creating implementation
cards to carry the relevant mockups into the card for the implementer's context.

Args:
    board_id: Board ID
    spec_id: Source spec ID
    card_id: Target card ID
    screen_ids: Multi-value screen IDs to copy (empty = copy ALL mockups from the
        spec) — formats: okto-pulse://reference/multivalue.

Returns:
    JSON with count of mockups copied

## `okto_pulse_delete_screen_mockup`

Delete a screen mockup from a source entity. Card mockups are read-only governed
snapshots.

Args:
    board_id: Board ID
    entity_id: Entity ID (spec, ideation, refinement, or story)
    screen_id: Screen mockup ID to delete
    entity_type: Type of entity — one of: spec, ideation, refinement, story (default: spec)

Returns:
    JSON with success status

## `okto_pulse_list_screen_mockups`

List screen mockups for any entity with optional filtering and pagination,
including read-only card snapshots.

Args:
    board_id: Board ID
    entity_id: Entity ID (spec, ideation, refinement, card, or story)
    entity_type: Type of entity — one of: spec, ideation, refinement, card, story (default: spec)
    screen_type: Filter by screen type (optional) — one of: page, modal, drawer, popover, panel
    offset: Skip first N screens (default 0)
    limit: Max screens to return (default 50, max 200)

Returns:
    JSON with filtered/paginated screens

## `okto_pulse_update_screen_mockup`

Update an existing screen mockup's fields on a source entity. Card mockups are
read-only governed snapshots.

Args:
    board_id: Board ID
    entity_id: Entity ID (spec, ideation, refinement, or story)
    screen_id: Screen mockup ID to update
    entity_type: Type of entity — one of: spec, ideation, refinement, story (default: spec)
    title: New title (empty = no change)
    description: New description (empty = no change)
    html_content: New HTML+Tailwind content (empty = no change). Script tags and on* event attributes are stripped.
    screen_type: New screen type (empty = no change) — one of: page, modal, drawer, popover, panel
    design_system_ref: New Design System ID (empty = no change).
    design_system_version: New Design System version (optional).
    design_system_evidence: New compliance evidence payload (optional).

When a gate-relevant field changes (html_content / design_system_ref /
design_system_evidence) the MockupDesignSystemGate re-evaluates this mockup
BEFORE persistence (delta-only): `blocking` rejects an invalid Design System
ref/version/evidence with an actionable error; `advisory` persists and returns
a `design_system_gate` warning; `off` (or no Design System) does not block.

Returns:
    JSON with updated screen
