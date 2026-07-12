---
version: "1.0"
---

# Projection Profiles & the Response Envelope

High-volume Okto Pulse MCP responses can be wrapped by a shared **projection
envelope** (`MCPProjectionEnvelopeHelper`) so agents fetch only as much payload
as a step needs. This resource is the single source of truth for profile
semantics; tool descriptions link here instead of repeating it inline.

## Canonical success key: `outcome`

The envelope's canonical success key is **`outcome`**, with values `ok` or
`error`. `status` may appear only as optional compatibility metadata.

- Success → `outcome: "ok"`.
- Failure → `outcome: "error"` plus standard `error` and `error_code`.

Do **not** rely on `result.get("success")` truthiness in `summary`/`detail`
mode — positive `success` is intentionally omitted there. `success: true` and
`success: false` remain available in `full`/`legacy` mode for existing callers.

## The four profiles

| Profile | Returns | Use it for |
|---|---|---|
| `summary` *(default)* | IDs, counts, minimal state | cheap exploration / listing |
| `detail` | summary + selected bodies | inspecting a few items |
| `full` | complete current modern body (within hard safety caps) | reading one item fully |
| `legacy` | prior compatibility fields, preserved exactly | existing callers mid-migration |

An unsupported profile returns a structured error `unsupported_projection`
with the allowed list under `supported_profiles`.

**Per-family variance:** the `copy_*_to_card` family supports a 3-value
profile set — `summary`/`full`/`legacy`, **no `detail`**. Passing `detail`
to a copy tool that exposes `profile` (e.g. `copy_architecture_to_card`)
returns `unsupported_projection` with the 3-value `supported_profiles` list.

## Envelope metadata (shared contract SC1)

Every projected response carries:

- `profile` — the profile that produced this shape.
- `outcome` — `ok` or `error` (canonical success key).
- `payload_bytes` — deterministic byte size of the returned body.
- `truncated` — whether a hard safety cap trimmed the body.
- `omitted_count` / `deduped_count` — how many fields/items were dropped or
  collapsed.
- `follow_up` — compact, machine-readable next-step affordances, e.g.
  `{ "rel": "read_full_context", "target_ref": "okto_pulse_get_task_context" }`.

## Context dedup in `summary` (`get_task_context` / `get_spec_context`)

Under `summary`/`detail` the two high-frequency context tools deduplicate the
largest repeated blocks. **Nothing is lost** — every body stays reachable via
`profile=full` or the `follow_up` affordance below.

- **`decisions_markdown` is gated.** The structured `decisions[]` and
  `decisions_stats` stay; the redundant rendered markdown is dropped and replaced
  by a follow_up `{ "rel": "render_decisions_markdown", "target_ref":
  "spec:<id>:decisions_markdown" }`. Read it with `profile=full`.
- **Architecture bodies are summarized once.** The heavy fields
  (`global_description`, `entities`, `interfaces`, `diagrams`) are replaced by a
  `counts` drilldown while every identifying field (`id`, `title`, `parent_type`,
  `parent_id`, `version`, `source_ref`) is kept — across the `card`, `spec`,
  top-level and `resolved_references` sections. A single follow_up
  `{ "rel": "read_full_architecture", "target_ref": "<tool>" }` points to the
  full bodies. `include_architecture=false` is honored — the projection layer
  never re-introduces architecture that was excluded upstream.
- **`resolved_references` bodies** (`content`/`text`, plus prose in `summary`)
  are dropped — the same facts live in `spec`/`card`. `deduped_count` totals
  every duplicated body removed.

`full`/`legacy` preserve all of the above bodies exactly for back-compat.

## Invariant: full context before status-changing moves

Summary-first reads are for **exploration only**. The SDLC safety rule is
unchanged: you MUST read full context (e.g. `okto_pulse_get_*_context`) before
any status-changing move (moving a card/spec/sprint, submitting a gate,
deciding a transition). Summary-first discovery never replaces that mandatory
full read before a mutation.
