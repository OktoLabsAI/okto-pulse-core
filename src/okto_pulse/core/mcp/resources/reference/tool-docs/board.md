---
version: "1.0"
---

# Tool docs — `board`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_get_board`

Get board details. Defaults to a minimal overview envelope; pass `include` to
inline collections.

Ideação MCP-token-optimization Story 2: the default response carries id,
name, description, owner_id, settings, counts{} and timestamps — ~200B vs
~10KB on a typical board.

Args:
    board_id: Board ID to retrieve.
    include: Comma-separated list of collections to inline. Accepts any
        subset of `ideations`, `specs`, `cards`, `agents`. Pass `*` to
        inline every collection (legacy shape).

Returns:
    JSON string with the board overview, plus any inlined collections.

## `okto_pulse_list_board_members`

List all members of the board (owner + agents).

Args:
    board_id: Board ID

Returns:
    JSON with owner info and agents list

## `okto_pulse_get_active_default_board_config`

Read the active default board configuration template.

Use this to understand which gates and settings new boards inherit when they are
created from the global default configuration.

Args:
    board_id: Board ID used for authentication.

Returns:
    JSON with active template metadata, version, settings snapshot, or null.

## `okto_pulse_list_default_board_config_versions`

List default board configuration versions.

Args:
    board_id: Board ID used for authentication.
    include_inactive: Optional boolean-like string to include inactive versions.

Returns:
    JSON list with version, active flag, author, and created timestamp.

## `okto_pulse_get_board_default_config_diff`

Compare a board's current settings against the active default board
configuration snapshot applied at creation.

Args:
    board_id: Board ID.

Returns:
    JSON with field-level differences and applied default snapshot metadata.

## `okto_pulse_create_default_board_config_version`

Create a new default board configuration version.

Use this to define the gate/settings defaults that future boards should inherit.
Creating a version does not necessarily activate it.

Args:
    board_id: Board ID used for authentication.
    name: Template name.
    description: Optional description.
    settings_json: JSON settings payload.

Returns:
    JSON with created template version and validation results.

## `okto_pulse_activate_default_board_config_version`

Activate one default board configuration version for new boards.

Args:
    board_id: Board ID used for authentication.
    template_id: Default configuration template ID.
    version: Version to activate.

Returns:
    JSON with the activated version and prior active version, if any.

## `okto_pulse_deactivate_default_board_config_version`

Deactivate the current default board configuration version.

Args:
    board_id: Board ID used for authentication.
    template_id: Default configuration template ID.
    version: Version to deactivate.

Returns:
    JSON with updated active/default state.

## `okto_pulse_link_board_design_system`

Associate a global design system with the current board.

Args:
    board_id: Board ID.
    design_system_id: Global design system ID.
    priority: Optional ordering priority.

Returns:
    JSON with board design-system link details.

## `okto_pulse_unlink_board_design_system`

Remove a design system association from the current board.

Args:
    board_id: Board ID.
    design_system_id: Design system ID to unlink.

Returns:
    JSON success payload and remaining board design-system count.

## `okto_pulse_get_board_design_system`

Read the effective design system for a board.

Resolution prefers an explicit board link, then the global default design
system, and otherwise returns null. Use this before submitting mockups when the
design-system gate is advisory or blocking.

Args:
    board_id: Board ID.

Returns:
    JSON with effective design system, source, version, and gate context.

## `okto_pulse_list_by_board`

List top-level entities of a board by type.

    Consolidates: list_specs, list_ideations, list_refinements,
    list_sprints, list_stories, list_topics.

    Use this single tool instead of the individual list_* tools.

    Args:
        board_id: Board ID
        entity_type: One of: spec, ideation, refinement, sprint, story, topic
        filters: Optional filter dict OR JSON string; validated server-side per entity_type.
            spec: status, labels, assignee_id
            ideation: status, labels
            refinement: status, labels, ideation_id
            sprint: status (requires filters.spec_id to identify parent spec)
            story: status, topic_id, linked, converted, include_archived
            topic: include_archived
        limit: Max results (default 100, max 200)
        offset: Skip first N results (default 0)

    Returns:
        JSON {items: [...], total: int, entity_type: str} or structured error

## `okto_pulse_list_my_boards`

List all boards the authenticated agent has access to.
No parameters needed — the agent is identified by the API key in the MCP connection.

Returns:
    JSON with agent identity and list of boards
