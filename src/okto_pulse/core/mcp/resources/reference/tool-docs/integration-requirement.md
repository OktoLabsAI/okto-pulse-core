---
version: "1.0"
---

# Tool docs — `integration-requirement`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_add_integration_requirement`

Add an Integration Requirement (IR) to a spec.

Use IR for APIs, queues, stored procedures, events, files, external services,
and data contracts that need traceability beyond a single endpoint.

`linked_requirements` is pipe-separated and fail-closed. It accepts FR
index/fr_id/text and structured TR id/text, then persists canonical IDs.
Unresolved tokens abort the append before persistence.

## `okto_pulse_list_integration_requirements`

List Integration Requirements (IR) for a spec.
