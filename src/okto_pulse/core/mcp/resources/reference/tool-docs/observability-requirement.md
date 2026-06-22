---
version: "1.0"
---

# Tool docs — `observability-requirement`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_add_observability_requirement`

Add an Observability Requirement (OR) to a spec.

`linked_requirements` is pipe-separated and fail-closed. It accepts FR
index/fr_id/text and structured TR id/text, then persists canonical IDs.
Unresolved tokens abort the append before persistence.

## `okto_pulse_list_observability_requirements`

List Observability Requirements (OR) for a spec.
