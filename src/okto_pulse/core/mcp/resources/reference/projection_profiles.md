---
version: "1.0"
---

# Projection Profiles & the Response Envelope

High-volume Okto Pulse MCP responses can be wrapped by a shared **projection
envelope** (`MCPProjectionEnvelopeHelper`) so agents fetch only as much payload
as a step needs. This resource is the single source of truth for profile
semantics; tool descriptions link here instead of repeating it inline.
The governed policy lists intentionally support only `summary|detail`; see
`okto-pulse://reference/policy-compliance`.

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

Entity REST lists preserve their legacy array shape unless the caller opts into
offset/limit PageEnvelope pagination. Only that envelope may add the
permission-gated `quality_summaries` field, whose closed row allowlist is
`receipt_id`, `subject_version`, `currentness`, `score`, `scale`, and
`head_revision`. Dedicated Quality MCP list tools use keyset cursors and are
not projection-profile aliases; see
`okto-pulse://reference/quality-assessments`.

**Per-family variance:** the `copy_*_to_card` family supports a 3-value
profile set — `summary`/`full`/`legacy`, **no `detail`**. Passing `detail`
to a copy tool that exposes `profile` (e.g. `copy_architecture_to_card`)
returns `unsupported_projection` with the 3-value `supported_profiles` list.

## Envelope metadata (shared contract SC1)

`summary` and `detail` responses carry a nested `projection` object with:

- `profile` — the profile that produced this shape.
- `outcome` — `ok` or `error` (canonical success key).
- `payload_bytes` — deterministic byte size of the returned body.
- `truncated` — whether a hard safety cap trimmed the body.
- `omitted_count` / `deduped_count` — how many fields/items were dropped or
  collapsed.
- `follow_up` — compact, machine-readable next-step affordances, e.g.
  `{ "rel": "read_full_context", "target_ref": "okto_pulse_get_task_context" }`.

`full` returns the complete modern payload and `legacy` preserves the prior
payload exactly, so those two profiles normally do not inject the nested
projection object. The additive task-only
`profile="full", context_scope="gate"` view is the exception: it carries
projection metadata because it is explicitly bounded and content-manifested.
At the MCP transport boundary, every non-legacy call is still wrapped once by
the `okto-pulse.mcp-tool-outcome` V2 envelope, whose `meta` identifies the
contract and tool.

## Context tools

`get_task_context` and `get_spec_context` default to `summary` for exploration.
`get_ideation_context`, `get_refinement_context`, and `get_sprint_context`
default to `full` for backward compatibility, but now accept the same four
profiles and return the same `unsupported_projection` error for invalid values.

`get_task_context` enforces deterministic wire-response budgets: 32 KiB for
`summary` and 64 KiB for `detail`. If an assembled projection exceeds its
budget, long strings/collections and optional drill-down sections are bounded,
`projection.truncated` is `true`, and `projection.truncation_reason` is
`profile_payload_budget`. The response also exposes `projection.budget_bytes`.

For card mutation pre-flight, use
`profile="full", context_scope="gate"`. This 32 KiB task-only scope keeps card
and spec workflow identity, validation configuration and recent validation
metadata, reviewer separation, test-card operational flow, cognitive/gate
readiness, and compact Resource Gate state. Artifact/spec bodies and full
lineage are represented by a deterministic `content_manifest` with per-section
counts, byte sizes and SHA-256 digests. Its `follow_up` entries point to bounded
detail and complete drill-down reads.

The historical `profile="full", context_scope="all"` response remains the
default and is byte-compatible for API clients that can accept a large body;
`legacy` is also unchanged. Neither is the required in-band card mutation
pre-flight. Mutation services independently resolve and fingerprint complete
context server-side, so omitting large bodies from the client-side gate slice
does not weaken the critical-context guard.

## Context dedup in `summary`

Under `summary`/`detail` the two high-frequency context tools deduplicate the
largest repeated blocks. **Nothing is lost** — every body stays reachable via
`profile=full, context_scope=all`, a dedicated content tool, or the `follow_up`
affordance below.

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
- **Primary KB/mockup bodies** are also omitted from `summary`/`detail`;
  metadata, governance and provenance remain visible. `detail` additionally
  retains descriptive prose and bounded 2 KiB `*_preview` fields with an
  accompanying `*_preview_truncated` flag. Use `profile=full` for complete
  content or markup.

`full/all` and `legacy` preserve all of the above bodies exactly for back-compat.

## Invariant: full context before status-changing moves

Summary-first reads are for **exploration only**. The SDLC safety rule is
unchanged: you MUST read full gate context before any status-changing move
(moving a card/spec/sprint, submitting a gate, deciding a transition). For a
card, the mandatory in-band call is
`okto_pulse_get_task_context(profile="full", context_scope="gate")`; for the
other entity-context tools it remains `profile="full"`. Summary-first discovery
never replaces that mandatory full gate read before a mutation. Fetch
`detail`/follow-up content separately when implementation or review requires the
artifact bodies.

## Code Traceability projections

Code Traceability uses the same response envelope and profile rules; it does
not introduce a parallel context mechanism.

| Profile/scope | Projected traceability data |
|---|---|
| `compact` | Counts, mode, aggregate readiness, and bounded blocker codes. |
| `summary` | Receipt and lifecycle metadata, currentness, coverage, Target states, and overlap summaries; no excerpts. |
| `detail` | Summary plus bounded rationale, selectors, links, dispositions, and resolution metadata; safe excerpt previews may be omitted. |
| `full`, `context_scope=all` | Complete bounded Evidence, links, disposition history, Target history, and candidates within hard caps. |
| `full`, `context_scope=gate` | Fingerprints, current accepted agent receipt, receipt currentness, Target resolution states, blockers, overlaps, and omission manifest; no excerpts. |

Paths and symbols are attested payload fields, never instructions for Pulse to
open source. Gate projections intentionally exclude excerpts and secrets.
Community projects only records already persisted through Core use cases; it
does not enrich a projection by reading a repository or provider.

Canonical protocol: `okto-pulse://reference/code-traceability`.
