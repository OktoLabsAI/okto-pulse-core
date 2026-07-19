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

List default board-configuration template versions for a scope, plus the
active template id (admin read). REST twin: GET /default-board-config/versions.

Args:
    board_id: Board ID used for authentication.
    scope: Template scope (default `global`).

Returns:
    JSON list of template versions (version, active flag, author, created
    timestamp) and the active template id.

## `okto_pulse_get_board_default_config_diff`

Compare a board's current settings against the active default board
configuration snapshot applied at creation.

Args:
    board_id: Board ID.

Returns:
    JSON with field-level differences and applied default snapshot metadata.

## `okto_pulse_create_default_board_config_version`

Create a new default board-configuration template version (admin write).
REST twin: POST /default-board-config/versions. Perm: SPECS_UPDATE.

Use this to define the gate/settings defaults that future boards should
inherit. Creating a version does not activate it unless `activate=true`
(single-active is enforced). New versions default
`reviewer_separation_mode="enforce"`; pass `warn` or `off` explicitly only when
that is the intended policy. Historical boards/templates with the field absent
are not backfilled and resolve through `legacy_absent_compat`.
The same `enforce` default is materialized when a new board is created before
any active template exists; an explicitly supplied `warn`/`off` is preserved.

Args:
    board_id: Board ID used for authentication.
    settings_payload: Settings dict — validated as BoardSettings.
    scope: Template scope (default `global`).
    guideline_default_refs: Optional list of default guideline refs — must
        reference GLOBAL catalog guidelines.
    design_system_default_ref: Optional default Design System ref dict — its
        gate_mode must be valid.
    activate: When true, activates the new version (default false).

Returns:
    JSON with the created template version and validation results.

## `okto_pulse_activate_default_board_config_version`

Activate a default board-configuration template version (admin write);
deactivates every other active version in the scope. REST twin:
POST /default-board-config/versions/{template_id}/activate. Perm: SPECS_UPDATE.

Args:
    board_id: Board ID used for authentication.
    template_id: Default configuration template ID to activate.

Returns:
    JSON with the activated version and prior active version, if any.

## `okto_pulse_deactivate_default_board_config_version`

Deactivate a default board-configuration template version (admin write).
REST twin: POST /default-board-config/versions/{template_id}/deactivate.
Perm: SPECS_UPDATE.

Args:
    board_id: Board ID used for authentication.
    template_id: Default configuration template ID to deactivate.

Returns:
    JSON with updated active/default state.

## `okto_pulse_link_board_design_system`

Associate a global design system with the current board.

Args:
    board_id: Board ID.
    design_system_id: Active global Design System ID, or an inline Design
        System owned by this same board. The board has one effective link, so
        this operation has no priority argument.

Returns:
    JSON with board design-system link details.

## `okto_pulse_unlink_board_design_system`

Remove a design system association from the current board.

Args:
    board_id: Board ID.
    The board has a single effective link; no design_system_id argument is
    accepted.

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
            ideation: status, labels, derivation_pending
            refinement: status, labels, ideation_id, derivation_pending
            sprint: status (requires filters.spec_id to identify parent spec)
            story: status, topic_id, linked, converted, include_archived
            topic: include_archived
        limit: Max results (default 100, max 200)
        offset: Skip first N results (default 0)

    Returns:
        JSON {items: [...], total: int, entity_type: str} or structured error

    Notes:
        derivation_pending=true is available for ideation and refinement only,
        and is the canonical triage for done work lacking a derived child.
        For ideations it means DONE medium/large ideations with zero active
        child refinements, plus DONE small ideations with zero active direct
        specs. For refinements it means DONE refinements with zero active
        child specs. Archived or cancelled children are not active
        derivations. Follow-up tools: okto_pulse_derive_spec_from_ideation
        (small), okto_pulse_create_refinement (medium/large),
        okto_pulse_derive_spec_from_refinement.
        Full table + examples: okto-pulse://reference/list_tools

## `okto_pulse_list_my_boards`

List all boards the authenticated agent has access to.
No parameters needed — the agent is identified by the API key in the MCP connection.

Returns:
    JSON with agent identity and list of boards
