---
version: "1.0"
---

# Tool docs — `traceability`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_get_traceability_report`

Without `query`, the existing SDLC report and filters are preserved.
For Bug clusters pass `query={"view":"bugs","group_by":"proxy"}`. Closed groupings:
`proxy`, `spec` (origin Card's Spec), `learning`, `severity`. Optional `date_from`,
`date_to`, `status`, `severity`, `limit` (default 200, maximum 1000), `cursor`,
`timeout_ms` (cannot exceed the Board policy). Default window: last 15 UTC days.
Date-only upper bounds include that UTC day. For the next page copy response
`window.from`/`window.to` into `query.date_from`/`query.date_to`, preserving filters
and limit. A changed scope returns `bug_clusters_cursor_stale`; restart without
the cursor. Do not combine `query` with `ideation_id`, `spec_id` or
`include_artifacts=true`.

Counts cover distinct authorized Bugs, not join rows or the current page.
Origin associations do not establish a common cause; Learning is a recorded
interpretation whose validity may be unknown. Missing graph checkpoints remain
unknown/incomplete; an empty observation is not proof of absence. These reads
are informational and cannot approve a gate. Permissions are rechecked on every
page: Board/Card read for all groups, Spec read for origin Spec/proxy, existing
related-context grant for proxy and learning-from-bugs grant for Learning.

For contextual Spec coverage pass
`query={"view":"coverage","subject_ref":"spec:<id>"}`. Optional `limit`
(default 200, maximum 1000), `cursor` and `timeout_ms` share the same bounded
read contract. Keep subject and limit on subsequent pages. Source changes,
proof visibility changes or graph generation changes invalidate the cursor;
restart the observation after `spec_coverage_cursor_stale` or
`spec_coverage_source_changed`. Do not combine this variant with SDLC filters.

`structure.summary` comes from the existing authoritative structural resolver;
`delivery` comes from the existing admitted-proof rollup. Neither substitutes
for other gates. A linked Test Card is not a passing run, raw supports is not
delivery, and an authorized waiver is labelled separately from proven work.
`items.kind` distinguishes delivery rows, source nodes and structural relations.
Graph-only and missing observations are diagnostic, never a repair instruction.
Counts describe the full authorized scope, not the current page. Without a full
projection checkpoint, matching observations still have unknown freshness.

The combined source scope requires Board, Spec, Card, scenarios, IR and OR read
grants. Code Traceability evidence and KG related-context grants are independent:
missing optional grants yield restricted sections without hidden counts or reads.
Permissions are checked on every page. In the UI, open Spec → Coverage; source
navigation uses the existing domain editors and does not write graph relations.

okto_pulse_get_traceability_report — return a consolidated SDLC traceability report:
ideation → refinement → spec → card/test/bug → artifacts.

Use this at the end of an E2E flow to verify whether the agent can answer
what was implemented in each flow and whether KBs, mockups, architecture,
tests, bugs, cards, and parent references stayed queryable.

Args:
    board_id: Board ID.
    ideation_id: Optional ideation filter. When provided, returns only
        lineage below that ideation.
    spec_id: Optional spec filter. When provided, returns the spec and its
        parent ideation/refinement lineage when available.
    include_artifacts: defaults to "false" (compact artifact counts) to keep
        the agent-facing report small; pass "true" to expand the full
        KB/mockup/architecture references.

Returns:
    JSON with consolidated lineage, card/test/bug counts, artifacts, and
    orphan_specs that are linked to the selected board but not attached to
    the selected ideation/refinement chain.
