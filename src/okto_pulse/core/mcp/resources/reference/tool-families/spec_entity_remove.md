---
version: "1.0"
---

# Tool family: `spec_entity_remove` (R4 consolidation)

Consolidated removal of a spec's structured entities through one tool with a
closed `target_type` enum. One of the two assertiveness-gate-eligible families
(the legacy per-type tools have identical `(board_id, spec_id, <id>)` signatures,
so consolidation loses no typed guidance).

## Consolidated tool

`okto_pulse_remove_spec_entity(board_id, spec_id, target_type, entity_id)`

- `target_type` ∈ `business_rule` | `api_contract` | `decision`
- An unsupported `target_type` returns a structured error
  `unsupported_projection`-style `{error:"unsupported_target_type", allowed:[…]}`
  and performs **no mutation**.

## Behavioral asymmetry (preserved by dedicated routing)

| target_type | semantics |
|---|---|
| `business_rule` | HARD remove (filtered out of the list). Returns `{success, removed, remaining, …coverage}`. |
| `api_contract` | HARD remove. Returns `{success, removed, remaining, …coverage}`. |
| `decision` | **SOFT-delete** — `status` becomes `revoked` (restorable via `okto_pulse_update_decision` with `status=active`); KG history is preserved. Returns `{success, revoked, decision}`. |

## Legacy aliases (preserved, additive — not removed)

These continue to work and delegate to the same implementation:

- `okto_pulse_remove_business_rule(board_id, spec_id, rule_id)`
- `okto_pulse_remove_api_contract(board_id, spec_id, contract_id)`
- `okto_pulse_remove_decision(board_id, spec_id, decision_id)`

Registry-only short name: `remove_spec_entity`. It exists for internal family
resolution, collision checks, and bounded telemetry labels; it is **not** an MCP
tool token, does not appear in `tools/list`, and cannot be invoked remotely. The
three `okto_pulse_*` entries above are the live additive MCP aliases.

## Telemetry

Every dispatch (legacy / consolidated / short) emits
`mcp_tool_alias_usage_total` with safe labels `{family_id, alias_kind, tool_name,
operation, target_type, outcome}` — counts only, never a body. An unsupported
`target_type` also emits `mcp_tool_family_registry_violation_total`.
