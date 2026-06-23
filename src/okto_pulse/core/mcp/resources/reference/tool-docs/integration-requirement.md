---
version: "1.0"
---

# Tool docs — `integration-requirement`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_add_integration_requirement`

Add an Integration Requirement (IR) to a spec.

Use IR for APIs, queues, stored procedures, MCP tools, events, files,
external services, and data contracts that need traceability beyond a single
endpoint.

`integration_type` accepted values: `api`, `queue`, `stored_procedure`,
`data_contract`, `event`, `file`, `external_service`, `mcp_tool`, `other`.

`linked_requirements` is pipe-separated and fail-closed. It accepts FR
index/fr_id/text and structured TR id/text, then persists canonical IDs.
Unresolved tokens abort the append before persistence.

## `okto_pulse_list_integration_requirements`

List Integration Requirements (IR) for a spec.
