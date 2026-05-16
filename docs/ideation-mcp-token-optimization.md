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

### Proposal

Default write responses to `{success, id, <small entity echo>}`. Move coverage
behind an opt-in flag (`include_coverage=true`) or rely on the existing
`get_resource_gate_summary` for the one moment that matters (before submitting
the validation gate).

### Tradeoff

Agents that previously used the progressive coverage signal to decide when to
stop saturating would need one extra read at the end. That extra read is a single
small call against a tool that already exists; for free-form authoring agents
the saving is ≈30 KB per spec.

### Expected impact

**High.** Highest per-session saving observed in this run. Closes the most-common
hot path. Pure additive change behind a flag — zero breaking risk.

---

## Story 2 — `get_board` field projection

### Situation today

`okto_pulse_get_board(board_id)` returns every collection inline (ideations,
specs, cards, agents) and their summaries. On the E2E board this was ~10 KB.

Frequent uses in this session needed only `name + settings` (e.g. to verify
which boards have `auto_derive_spec_resources_enabled=true` before reproducing
a bug). The rest of the payload was discarded.

### Proposal

Add a `fields=` (or `include=`) parameter accepting a comma-separated allowlist
(e.g. `fields=settings,name,owner_id`). When omitted, fall back to today's behaviour
for backwards compatibility. Optionally expose a thin sibling `okto_pulse_get_board_settings`
(name aligns with the existing REST `board-settings-api.ts`).

### Tradeoff

Slight increase in tool surface (1 extra parameter or 1 extra tool). Agents that
relied on the full snapshot keep working — no contract change. Field projection
is cheap to implement: skip the relationship-loading code paths when the field
isn't requested.

### Expected impact

**Medium-high.** Each saving is ~9 KB but happens often — every session that
inspects multiple boards repeats this cost. Also reduces noise in the activity
log Q&A flows that only need settings to decide next steps.

---

## Story 3 — `get_activity_log` compact mode

### Situation today

`get_activity_log(action="spec_resources_auto_propagated", limit=10)` returned 10
rows, each with `details.results.{knowledge_base, mockup, architecture}` carrying
nested `{source_count, copied_count, ignored_count, copied_ids, warnings}` for
every resource type. In a smoke check, agents only need:

```
[id, action, trigger, card_id, created_at]
```

Verbose details mattered exactly once — when triaging *why* a propagation row was
created with zeros — and even then, fetching the single offending row in detail
is the right tool.

### Proposal

`okto_pulse_get_activity_log(..., compact=true)` returns the minimal projection
(id, action, trigger, card_id, created_at). `compact=false` is the default and
preserves today's payload. Optionally pair with a dedicated
`okto_pulse_get_activity_log_entry(id)` for the rare "show me the full row"
path.

### Tradeoff

One extra parameter. Default behaviour unchanged — no migration risk. Agents
that explicitly want details opt in.

### Expected impact

**Medium.** Strong win on smoke-test / "did the trigger fire?" flows, which are
the dominant audit-log consumer in agent loops. Estimated 60–70% reduction on
the audit-log payload in those cases.

---

## Story 4 — `validate_and_persist` for architecture design

### Situation today

The current convention is `validate_architecture_design_payload` → review issues
and warnings → `add_architecture_design` *with the same payload* — both round
trips carry the full entities/interfaces/diagrams JSON. For the 7-entity / 9-
interface architecture authored this session, that was ~6 KB sent twice.

### Proposal

Add a `commit=true` parameter to `validate_architecture_design_payload` (or a
sibling `okto_pulse_validate_and_persist_architecture_design`). When `commit=true`
and `valid=true`, the server persists in the same call and returns the same
`ArchitectureDesignResponse` shape; when `valid=false`, it returns issues and
suggested fixes without persisting (today's behaviour).

The same model could apply to `update_architecture_design` if a validation
endpoint exists for updates.

### Tradeoff

Loses the dry-run isolation in the unified call — but agents already pay for the
isolated dry-run when needed (just call `validate` without `commit`). The unified
flow matches what `agent_instructions.md` already recommends as the *happy path*
(validate, then persist).

### Expected impact

**Medium.** Halves the payload cost on the architecture authoring path. Less
frequent than coverage rollups but each saving is larger.

---

## Story 5 — Unified `link_task` tool

### Situation today

Five tools (`link_task_to_rule`, `link_task_to_decision`, `link_task_to_tr`,
`link_task_to_integration_requirement`, `link_task_to_observability_requirement`)
share the exact same signature and behaviour modulo the target table.

This inflates the MCP tool inventory served by FastMCP, increases the cost of
`ToolSearch`, and means agents need 5 separate `select:` lookups when warming
up tool schemas. The current `okto_pulse_link_task_to_scenario`,
`link_task_to_contract`, and `link_card_to_spec` follow the same pattern and
could be unified by the same construct.

### Proposal

Introduce `okto_pulse_link_task(target_type, target_id, board_id, spec_id, card_id)`
where `target_type ∈ {rule, decision, tr, ir, or, scenario, contract, spec}`.
Old tools become thin shims for one release, then are removed once instructions
in `agent_instructions.md` point to the unified entry.

### Tradeoff

The unified tool is less discoverable to agents that filter by name (`ToolSearch
"link rule"` instead of finding it instantly). Mitigation: keep keyword aliases
in the tool docstring (e.g. "link rule decision tr ir or"); `ToolSearch` ranks
on description text too. Also: the explicit-per-type model gives clearer
permission auditing if board-permission rules ever differ per linkage type
(today they don't).

### Expected impact

**Medium.** Smaller per-call gain (a few hundred bytes), but compounding:
shrinks the always-loaded MCP tool description block by ~5 tool entries (~3–4 KB
in schema text).

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
