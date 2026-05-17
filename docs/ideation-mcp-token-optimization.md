# Ideação — MCP token-payload optimization (5 opportunities)

**Target board:** Okto Pulse 0.2.1 (`1dad8706-f57d-492e-9f83-85847fbd9850`) — or a new dedicated *Pulse Evolution* board.
**Suggested complexity:** medium (5 independent FRs, each small-to-medium, no breaking contract changes if added as opt-in modes).
**Suggested labels:** `mcp`, `optimization`, `token-cost`, `payload`, `dx`.

---

## Description

Agents interacting with the Okto Pulse MCP server spend a non-trivial share of their
context window on response payloads that carry information the caller did not ask for —
coverage roll-ups, full board snapshots, verbose audit details, duplicated payloads
between dry-run + persist, and repetitive `link_task_to_*` definitions. This ideation
groups five concrete, runtime-observed opportunities to reduce token cost on the hot
paths, ordered by impact, with the tradeoff each one carries.

Each opportunity is independently shippable; together they reduce per-session token
spend in agent-driven flows (saturation, smoke check, e2e) by an estimated 30–60%
without losing any information that agents actually use.

---

## Design principle — *minimal informative default, verbose opt-in*

Every story below follows the same rule:

> **The default response carries the smallest payload that still gives the caller
> a directional signal it can act on. Full detail is available, but only on
> explicit opt-in.**

Concretely:

- For **writes** (Story 1), the default is a tiny envelope conveying *how close
  the artifact is to its gate* (`saturation.pct` + `blocking[]`). Full coverage
  block via `include_coverage=true`.
- For **reads** of aggregates (Stories 2 and 3), the default is an *overview*
  carrying ids + counts + the smallest fact the caller usually wants (settings
  for `get_board`, trigger + summary string for `get_activity_log`). Inline
  collections / nested details via `include=…`.
- For **dual-step flows** (Story 4 — validate + persist), the default unifies
  the two round-trips and returns a minimal post-persist envelope (id, version,
  warnings count). Full design echo via `include_design=true`.
- For **tool inventory** (Story 5), the default is a single generic tool whose
  parameter selects the variant — instead of N explicit tools the agent must
  pre-load. Per-type tools remain available as thin shims for one release.

This consistency matters: agents learn one pattern (*"call with no extras, opt
in when you need verbose"*) and apply it uniformly across the API. Internally,
each implementation reuses the same "minimal envelope builder" idea — the
shape varies, the principle does not.

---

## Context (runtime evidence — session 2026-05-16)

While authoring and validating spec `9af2cf05` (Fix: auto-derive Spec resources),
the following payload patterns repeated in a single workflow:

- **22 `link_task_to_*` calls** during saturation. Every response carried a full
  `coverage{}` block (~1.5 KB each), even though the caller only needs the final
  coverage state before `submit_spec_validation`. Aggregate: ~33 KB of redundant
  rollups in one spec.
- **First `get_board` call** on the E2E board returned 8 ideations + 9 specs + 47 cards
  + 3 agents inline (~10 KB) when the only field needed was `settings` to confirm
  the auto-derive toggle.
- **`get_activity_log`** for a smoke check returned `details.results.{kb, mockup,
  architecture}` for every row, when the caller wanted only the `trigger` string and
  `created_at` to verify which paths fired.
- **`validate_architecture_design_payload` + `add_architecture_design`** sent the
  same ~6 KB payload twice in a row (dry-run, then persist). For larger architectures
  the duplication scales linearly.
- **5 nearly-identical `link_task_to_*` tools** (`rule`, `decision`, `tr`,
  `integration_requirement`, `observability_requirement`) inflate the tool inventory
  and increase the cost of every `ToolSearch` / capability discovery.

Combined, a single agent-led spec saturation easily spends 50–80 KB on payload chrome
that the caller discards.

---

## Story 1 — `coverage` block off the write hot-path

### Situation today

Every MCP write that touches a spec aggregate (`add_business_rule`,
`add_test_scenario`, `add_decision`, `add_integration_requirement`,
`add_observability_requirement`, `link_task_to_rule`, `link_task_to_decision`,
`link_task_to_tr`, `link_task_to_integration_requirement`,
`link_task_to_observability_requirement`) returns the full coverage roll-up:

```json
"coverage": {
  "ac_coverage_pct": 100.0, "ac_covered": 8, "ac_total": 8, "ac_uncovered_indices": [],
  "fr_coverage_pct": 100.0, "fr_covered": 7, "fr_total": 7, "fr_uncovered_indices": [],
  "scenario_task_linkage_pct": 100.0, "scenarios_linked": 8, "scenarios_total": 8,
  "br_task_linkage_pct": 100.0, "brs_linked": 4, "brs_total": 4,
  "contract_task_linkage_pct": 100, "contracts_linked": 0, "contracts_total": 0,
  "tr_task_linkage_pct": 100.0, "trs_linked": 7, "trs_total": 7,
  "decisions_coverage_pct": 100.0, "decisions_linked": 3, "decisions_total": 3,
  "decisions_uncovered_ids": [],
  "ir_task_linkage_pct": 100.0, "irs_linked": 1, "irs_total": 1, "irs_uncovered_ids": [],
  "or_task_linkage_pct": 100.0, "ors_linked": 1, "ors_total": 1, "ors_uncovered_ids": [],
  "skip_test_coverage": false, "skip_rules_coverage": false,
  "skip_decisions_coverage": false, "skip_ir_coverage": false, "skip_or_coverage": false
}
```

This is the same payload `get_resource_gate_summary` already exposes — and which
`submit_spec_validation` consults internally.

### Proposal (preferred — minimal saturation envelope)

Replace the verbose `coverage{}` block with a **two-field envelope** that
preserves the progressive signal while collapsing the bytes:

```json
{
  "success": true,
  "id": "br_61de4741",
  "saturation": {
    "pct": 87.5,
    "blocking": ["decisions", "scenarios"]
  }
}
```

- `saturation.pct` is the aggregate completion across all required dimensions
  (FR/AC/BR/TR/decisions/scenarios/IR/OR) weighted equally, rounded to one
  decimal.
- `saturation.blocking` is the (possibly empty) ordered list of dimension keys
  that still gate `submit_spec_validation` (i.e. percentages < 100 or required
  links missing). Skip-flagged dimensions are excluded.
- Payload size: ~60 bytes vs ~1500 bytes today (25× smaller).
- Agents keep the ongoing-completeness signal that the original design
  carried, AND get a directional hint (`blocking`) so they can focus the next
  call instead of trusting a misleading average.

A fallback `include_coverage=true` flag can still expose the full block for
clients that depend on every dimension count today.

### Alternative (rejected)

Default write responses to `{success, id}` with no progress signal at all,
relying on `get_resource_gate_summary` at the end. Saves more bytes per call
(~30 bytes) but loses the ongoing feedback that turned out to be load-bearing
for agents authoring multi-step saturations. The minimal envelope above is
the better trade.

### Tradeoff

The aggregate `pct` can mask a single low-coverage dimension behind a high
average. Mitigated by `blocking[]`: when the list is non-empty the agent
knows where to focus regardless of the headline number. When `blocking[]` is
empty, the gate will pass — that is the truthful signal agents need.

### Expected impact

**High.** Highest per-session saving observed in this run (≈30 KB per spec
saturation cycle), zero extra round-trip, zero loss of feedback. Pure
additive change behind a default — clients reading `coverage{}` today can opt
in via the legacy flag. Lowest-risk, highest-value optimisation.

---

## Story 2 — `get_board` minimal overview envelope

### Situation today

`okto_pulse_get_board(board_id)` returns every collection inline (ideations,
specs, cards, agents) and their summaries. On the E2E board this was ~10 KB.

Frequent uses in this session needed only `name + settings` (e.g. to verify
which boards have `auto_derive_spec_resources_enabled=true` before reproducing
a bug). The rest of the payload was discarded.

### Proposal (preferred — minimal overview envelope)

Default response becomes an *overview* that always fits in a single screen of
context, regardless of board size:

```json
{
  "id": "1dad8706-…",
  "name": "Okto Pulse 0.2.1",
  "description": "…",
  "owner_id": "local-user",
  "settings": { … },
  "counts": {
    "ideations": 8,
    "specs": 9,
    "cards": 47,
    "agents": 3
  }
}
```

- ~200 bytes vs ~10 KB today (50× smaller on a typical board).
- Agents keep the *scoping* signal — `counts` tells them whether descending
  is worth it; `settings` is the most-frequently-needed field.
- Inline collections come back only when explicitly asked for via
  `include=ideations,specs,cards,agents` (any subset, comma-separated). The
  legacy "everything inline" shape is reproduced by `include=*`.

### Alternative (rejected)

Pure field projection (`fields=settings,name`) without `counts`. Smaller still,
but the loss of `counts` removes the "is there anything here?" signal — agents
end up calling list endpoints just to know the size. The overview envelope
captures the 90% case with one round-trip.

### Tradeoff

Clients reading inline collections today must add `include=…`. The legacy
behaviour stays one query-string away (`?include=*`), so the migration is a
one-line change and a deprecation period covers existing integrations.

### Expected impact

**Medium-high.** Each saving is ~9.8 KB and happens at almost every session
warmup (agents inspect the board first). Compounds across multi-board sessions.

---

## Story 3 — `get_activity_log` row envelope with inline summary

### Situation today

`get_activity_log(action="spec_resources_auto_propagated", limit=10)` returned 10
rows, each with `details.results.{knowledge_base, mockup, architecture}` carrying
nested `{source_count, copied_count, ignored_count, copied_ids, warnings}` for
every resource type. Total ~1.5 KB per row, ~15 KB for the typical `limit=10`
window. In a smoke check, agents only need to confirm *which trigger fired and
what the outcome was* — a single sentence per row.

### Proposal (preferred — row envelope with inline `summary`)

Default rows ship the identifying fields plus a one-line, human-readable
`summary` of the per-type counters:

```json
{
  "id": "1794495a-…",
  "action": "spec_resources_auto_propagated",
  "trigger": "spec_architecture_created",
  "card_id": "5d78c01f-…",
  "created_at": "2026-05-16T22:56:49",
  "summary": "arch.copied=2 arch.ignored=0 kb.copied=0 mockup.copied=0"
}
```

- ~120 bytes per row vs ~1500 bytes (12× smaller); a `limit=10` smoke fits in
  ~1.2 KB instead of ~15 KB.
- The `summary` string is a deterministic projection of `details.results`,
  built server-side from the same dict — no information loss for the smoke
  path, no parsing cost on the agent.
- Full nested `details` available via `include_details=true` (legacy shape).
- For deep dives, pair with `okto_pulse_get_activity_log_entry(id)` returning
  the single row in full detail.

### Alternative (rejected)

`compact=true` flag that drops `details` entirely (no summary). Saves another
~40 bytes per row but loses the "what changed" signal that the smoke flow
actually wants. The summary-string envelope keeps the directional information
at near-zero cost.

### Tradeoff

The summary format is part of the contract; consumers reading it as plain
text must tolerate added counters when new resource types appear. Mitigated
by versioning the summary builder (kept stable within a major API version)
and keeping `include_details=true` as the structured-data path.

### Expected impact

**Medium.** Estimated ≥90 % reduction on audit-log smoke payloads — the
dominant usage in agent reproduction and CI flows.

---

## Story 4 — `validate_and_persist` with minimal post-persist envelope

### Situation today

The current convention is `validate_architecture_design_payload` → review issues
and warnings → `add_architecture_design` *with the same payload* — both round
trips carry the full entities/interfaces/diagrams JSON. For the 7-entity / 9-
interface architecture authored this session, that was ~6 KB sent twice.

Worse, the persist call then echoes the full `ArchitectureDesignResponse`
(~6 KB) back even though the caller already has the source payload in memory.

### Proposal (preferred — unified flow + minimal envelope)

Add `commit=true` to `validate_architecture_design_payload` (or a sibling
`okto_pulse_validate_and_persist_architecture_design`). Two changes at once:

1. **Request dedup** — when `commit=true` and `valid=true` the payload is sent
   exactly once. On `valid=false`, the response carries issues + suggested
   fixes; nothing is persisted.

2. **Minimal post-persist envelope** — successful persist returns:

   ```json
   {
     "success": true,
     "id": "b5b7f593-…",
     "version": 1,
     "warnings_count": 2,
     "normalized": false
   }
   ```

   ~80 bytes vs ~6 KB today. The caller already has the source payload and
   the validate-step warnings list — there's nothing in the echoed full
   response it doesn't already know.

   Full echo available via `include_design=true` for clients (e.g. UIs) that
   need the server-normalised shape.

### Alternative (rejected)

Just deduplicating the request (returning today's full echo). Catches the big
win, misses the smaller-but-cheap one. Combining both keeps the principle
consistent across all stories.

### Tradeoff

Loses the dry-run isolation in the unified call — but agents already pay for
the isolated dry-run when needed (just call `validate` without `commit`).
The unified flow matches what `agent_instructions.md` already recommends as
the *happy path* (validate, then persist).

### Expected impact

**Medium.** Cuts the authoring path from ~12 KB round-trip to ~6 KB + 80 B
(≈50 % total). Larger absolute saving per call than Story 1, lower
frequency.

---

## Story 5 — Unified `link_task` tool (minimal tool surface)

### Situation today

Five tools (`link_task_to_rule`, `link_task_to_decision`, `link_task_to_tr`,
`link_task_to_integration_requirement`, `link_task_to_observability_requirement`)
share the exact same signature and behaviour modulo the target table.

This is the **same principle as the other stories, applied to tool inventory
instead of response payload**: the surface defaults to large (5 tools each
with their own docstring and schema), forcing every agent to pre-load all
of them, when a minimal surface (1 tool + a `target_type` parameter) carries
the same expressivity.

`okto_pulse_link_task_to_scenario`, `link_task_to_contract`, and
`link_card_to_spec` follow the same pattern and could be unified by the same
construct.

### Proposal (preferred — minimal-surface generic tool)

Introduce `okto_pulse_link_task(target_type, target_id, board_id, spec_id, card_id)`
where `target_type ∈ {rule, decision, tr, ir, or, scenario, contract, spec}`.
The single tool replaces eight (5 + 3) and pairs naturally with Story 1's
minimal write envelope — the response is `{success, id, saturation:{pct,
blocking}}` regardless of target type.

The legacy per-type tools remain as thin shims for one release (delegating to
the generic), then are removed once `agent_instructions.md` points to the
unified entry.

### Alternative (rejected)

Keep all per-type tools and just shrink their schema text. Marginal saving,
keeps the discoverability burden on the agent (more entries in
`ToolSearch`).

### Tradeoff

The unified tool is less discoverable to agents that filter by name
(`ToolSearch "link rule"`). Mitigation: keep keyword aliases in the
docstring (e.g. *"link rule decision tr ir or scenario contract spec"*) —
`ToolSearch` ranks on description text. Also: the explicit-per-type model
gave clearer permission auditing if board-permission rules ever differed
per linkage type — today they don't, but the generic tool can still emit a
per-`target_type` permission-check log if needed.

### Expected impact

**Medium.** Per-call gain is small (~hundreds of bytes from the response
envelope alignment), but **the tool inventory itself shrinks by 8 entries**
(~6 KB of schema text always loaded by clients warming up). That cost is
paid once per session — and for short sessions, dominates.

---

## Out of scope (intentionally)

- MCP transport envelope (`{"result": "..."}` with escaped JSON string) — this
  is a constraint of the current FastMCP transport and not worth fighting
  unless a new transport ships.
- Coalescing multiple writes into a batch tool (e.g.
  `add_business_rules(rules: list)`) — useful, but bigger surface change and
  needs separate ideation; the 5 stories above are all opt-in additions.
- Streaming responses — agents in this codebase don't currently consume
  streaming MCP responses.

---

## Suggested next steps (when persisting in Pulse)

1. Create as Ideation on Okto Pulse 0.2.1 with `complexity=medium`.
2. Convert to Refinement to investigate (a) `coverage_response_builder` shared by
   the affected tools, (b) field-projection on `get_board`, (c) compact mode on
   audit-log query path.
3. Each story becomes its own FR on the resulting Spec; tasks split per FR.
4. Backwards-compatibility tests: every new flag defaults to the legacy behaviour
   so existing agents and the 8+6 tests in `tests/test_spec_resource_auto_propagation.py`
   keep passing.
