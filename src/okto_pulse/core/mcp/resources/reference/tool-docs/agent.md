---
version: "1.0"
---

# Tool docs — `agent`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_get_my_profile`

Get the authenticated agent's own profile including identity, description, objective, and permissions.
No parameters needed — the agent is identified by the API key in the MCP connection.

Returns:
    JSON with agent profile details

## `okto_pulse_list_agents`

List all agents registered on the board.

Args:
    board_id: Board ID

Returns:
    JSON array of agents

## `okto_pulse_update_my_profile`

Update or clear the authenticated agent's own description and/or objective.
No board_id needed — this updates the global agent profile.

Args:
    description: New description (optional; null/omitted = no change, empty = clear)
    objective: New objective (optional; null/omitted = no change, empty = clear)

Returns:
    JSON with updated profile
