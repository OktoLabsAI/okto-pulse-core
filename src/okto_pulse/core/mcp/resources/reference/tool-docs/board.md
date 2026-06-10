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
