---
version: "1.0"
---

# Knowledge Graph — Consolidation, Query & Governance

## Architecture Overview

- **Per-board embedded graph database** (per-board graph store) — 11 node types, 10 relationship types, 5 HNSW vector indexes
- **Global discovery meta-graph** (global discovery graph store) — board summaries, topic clusters, canonical entities (digest-only, no sensitive content)
- **Relational operational tables**: `consolidation_queue`, `consolidation_audit`, node back-references for undo, `global_update_outbox`
- **Agent-as-LLM premise**: the platform NEVER invokes LLM. All cognitive work (extraction, reasoning, reconciliation decisions) is done by YOU, the code agent.

### SK-A relational authority and graph projection

Quality receipts, findings, checklist executions/items, and checklist receipts
remain relational source-of-truth records. The graph never creates a
`QualityFinding` or checklist node. Entity roots may carry only the current
Quality summary identity. A resolved Research Decision Ledger head projects
through the existing `Decision`/`Alternative` vocabulary; demoting it to
`open`, `investigating`, or `deferred` removes/tombstones those derived
projections. Incremental reconciliation and rebuild must produce the same
active set. Archive/cancel/demotion must not leave a stale child projection.
Use `okto-pulse://reference/quality-assessments` for Quality currentness and
`okto-pulse://workflows/refinements` for the RDL authoring lifecycle.
For adopted semantic guidelines, assessment receipts, governed exceptions,
projection identity and rebuild behavior, read the single canonical
protocol at `okto-pulse://reference/policy-compliance`.

When KG Health reports `digest_vs_board_layer_mismatch` after all operational
queues are idle, inspect the rows with
`okto_pulse_kg_digest_layer_mismatch_list`. An authorized KG administrator may
then call `okto_pulse_kg_digest_layer_reconcile` with a bounded audit reason and
wait for the outbox to return to idle before verifying the mismatch list again.
This is a parity sync, not a rebuild. The worker keyset-inventories authoritative
publishable board sources with physical duplicate detection, guards stale prune
against derived clustering relationships, repairs identities and Board links,
and backfills missing identities. It revalidates the source inventory before a
board-isolated ACK and requires a post-flush fresh-handle proof of one stable
digest, the correct Board edge, and exactly one total inbound Board edge per
source.

## Consolidation Primitives (7 tools)

> Args/returns for every `okto_pulse_kg_*` tool live in `okto-pulse://reference/tool-docs/kg` — the tables in this workflow list purpose only.

| Tool | Purpose |
|------|---------|
| `okto_pulse_kg_begin_consolidation` | Open a transactional session. Returns session_id + SHA256 dedup. |
| `okto_pulse_kg_add_node_candidate` | Add a node candidate. Not persisted until commit. |
| `okto_pulse_kg_add_edge_candidate` | Add an edge. Endpoints reference in-session candidates or existing nodes via `kg:` prefix. |
| `okto_pulse_kg_get_similar_nodes` | HNSW vector search against existing graph. |
| `okto_pulse_kg_propose_reconciliation` | Server computes deterministic hints: ADD/UPDATE/SUPERSEDE/NOOP. |
| `okto_pulse_kg_commit_consolidation` | Atomically write to the embedded graph database + audit row + outbox event. |
| `okto_pulse_kg_abort_consolidation` | Drop the session without writing. |

**Node types (11):** Decision, Criterion, Constraint, Assumption, Requirement, Entity, APIContract, TestScenario, Bug, Learning, Alternative

**Cognitive edge types (agent-emittable only):** `supersedes`, `contradicts`, `depends_on`, `relates_to`, `validates`

**Reserved deterministic edges** (auto-created by worker, never emit manually): `belongs_to`, `derives_from`, `implements`, `mentions`, `tests`, `violates`, `originates_from`, `covered_by`

**Consolidation workflow:**
```
1. okto_pulse_kg_begin_consolidation(board_id, artifact_type, artifact_id, raw_content, deterministic_candidates=[...])
   → if nothing_changed=true → STOP, abort and move on
2. For every candidate:
     a. okto_pulse_kg_add_node_candidate(session_id, candidate)
     b. okto_pulse_kg_get_similar_nodes(session_id, candidate_id, top_k=5, min_similarity=0.85)
        → if match ≥ 0.95: plan UPDATE; if 0.85..0.95: plan SUPERSEDE; else: plan ADD
3. okto_pulse_kg_add_edge_candidate only for cognitive rels
4. okto_pulse_kg_propose_reconciliation(session_id)
5. okto_pulse_kg_commit_consolidation(session_id, summary_text="<1-2 sentences>", agent_overrides={...})
6. Verify with okto_pulse_kg_health + okto_pulse_kg_query_natural + okto_pulse_kg_query_cypher
7. On any unrecoverable error: okto_pulse_kg_abort_consolidation(session_id, reason=...)
```

`candidate_id` is session-local: calling `get_similar_nodes` before
`add_node_candidate` deterministically returns `candidate_not_found`.

## Global Discovery recovery (component-scoped)

Never infer the remedy from generic `overall_state=recovery_needed`. If
`graph_state` is healthy while `discovery_state=recovery_needed` and
`discovery_recovery_required=true`, use only
`okto_pulse_kg_global_discovery_recovery_preflight` →
`okto_pulse_kg_global_discovery_recovery_confirm` →
`okto_pulse_kg_global_discovery_recovery_run`. Board rebuild preflight refuses
this discovery-only case. The global preflight requires all board graphs to be
healthy; healthy/quarantined discovery is not admitted.

`run` persists integrity-bound worker inputs, creates the durable control row,
dispatches owned background work, and returns `accepted` without waiting for
the native candidate/cutover. Poll
`okto_pulse_kg_global_discovery_recovery_status` for authoritative progress and
the terminal outcome. Retrying the exact confirmation/run binding returns the
existing run; do not issue a new recovery while the existing run is `pending`
or `running`.

### Terminal Global Discovery outbox recovery

Use the global-outbox dead-letter family only after the delivery root cause is
fixed; it is distinct from the board consolidation DLQ family:

1. Page `okto_pulse_kg_global_outbox_dead_letter_list` with `limit<=100` and
   retain the returned immutable IDs. With `classification`, an empty page may
   still carry `next_cursor`; follow it until null.
2. For legacy rows, call `okto_pulse_kg_global_outbox_dead_letter_reprocess`
   with 1-100 explicit, unique IDs and an audit reason.
   Never interpret an empty selection as all. Governed
   `gd_parity:...:attempt:n` rows are immutable
   delivery history: do not rearm them. `kg.tick.daily` starts the automatic
   recovery chain; each `kg.tick.delivery_redrive` event runs one globally
   bounded page, serves one oldest due item per board before repeating a board,
   and advances the persisted round-robin checkpoint in the same transaction
   as fresh `attempt:n+1` rows. Due backlog schedules the next bounded event in
   that transaction. A governed or mixed manual selection fails closed with
   `governed_delivery_attempt_tick_owned`.
   The orphan watchdog is independently board-bounded and resumes from a
   persisted rotating cursor, so an active prefix cannot starve a later
   missing, malformed, terminal, or already-processed attempt.
3. Call `okto_pulse_kg_global_outbox_dead_letter_verify` with those exact IDs.
   Treat `still_dead_lettered`, dangling/cyclic lineage reason codes, or a busy
   response as unresolved; queued/applied are idempotent replay outcomes.

Each operation owns a dedicated relational transaction. Selection validation
and guarded requeue are atomic; `process_now=true` wakes the outbox worker only
after commit.

## Durable stale-canonical catch-up

`kg.tick.daily` stages at most one low-priority `stale_sweep` coordinator per
relational board, and does so only after the board's decay graph transaction has
closed. Its durable payload is `{cursor,budget,attempt}`. The queue worker runs
one global `(artifact_type, artifact_id)` keyset query capped at `budget + 1`,
normalizes card child refs to their owning card, and advances through live-only
inventory pages as well as deleted-source pages.

The inventory requires a complete relational source snapshot. Before creating
catch-up work, the relational adapter rechecks each candidate while holding the
writer slot. A still-absent historical source receives the deterministic key
`catchup:{board}:{type}:{id}:epoch:1`; its tombstone, `stale_reconcile` child and
next checkpoint are committed in one relational unit of work. An existing real
tombstone is reused and never advanced by the sweep.

Graph open/read failures and incomplete source snapshots preserve the cursor
and re-pend the coordinator for the next tick window; they never authorize a
broader scan or a synthetic tombstone. Literal relational board deletion still
cascades its queue rows, so "board unavailable" here means the board remains in
SQL while its graph runtime is absent, locked, quarantined or unreadable. Do not
edit coordinator payloads manually. Use staged telemetry
`kg.stale_sweep.scheduled.staged`, `kg.stale_sweep.page_staged`,
`kg.stale_sweep.rescheduled.staged` and `kg.stale_sweep.completed.staged` to
follow the caller-owned commit boundary.

## Query Timing — MANDATORY at Every Stage

> ### Degraded-KG Fallback Rule — `kg_health`-first (single source of truth)
>
> Before running ANY mandatory KG query set below — and in particular the Stage 1 ideation triad — call `okto_pulse_kg_health(board_id)` **first** and read its `graph_state` field. The graph is **degraded** when `graph_state` is one of exactly `recovery_needed` or `quarantined` (these two values are the `_RISK_STATE_HARD_REJECT` predicate; no other `graph_state` value is a degraded trigger). When the graph is degraded, follow these steps in order:
>
> 1. **Call `okto_pulse_kg_health(board_id)` first** and read `graph_state`.
> 2. **Branch on the degraded `graph_state`** (`recovery_needed` or `quarantined`): the mandatory KG queries are EXPECTED to be unavailable. `okto_pulse_kg_get_learning_from_bugs` in particular returns a structured `graph_unavailable` whose message carries the hint `Use the explicit KG Health recovery flow`; on a degraded board this `graph_unavailable` is the EXPECTED signal — do not retry it in a loop.
> 3. **Record the degraded `graph_state` in the ideation** — a one-line note such as `KG degraded: graph_state=<recovery_needed|quarantined>; Stage 1 triad skipped under the Degraded-KG Fallback Rule.`
> 4. **Proceed past `okto_pulse_evaluate_ideation` with a warning** — do not block on the unavailable triad. Skipping the Stage 1 triad on a degraded graph is expected-and-logged and is **not a protocol violation**.
>
> This rule keys ONLY on the existing `graph_state` field and the existing structured `graph_unavailable`. Recovering a degraded graph is the separate KG Health recovery flow (an operator-driven path, out of scope for this rule), and this rule does **not** define any new error code or response envelope for the degraded case. When `graph_state` is not one of the two degraded values, run the mandatory query sets normally.

**Stage 1 — Ideation (before moving to `evaluating` and before answering any Q&A)**

| Query | Why it's required |
|---|---|
| `okto_pulse_kg_find_similar_decisions(board_id, topic=<ideation problem statement>)` | Discover prior art, prevent re-inventing the wheel |
| `okto_pulse_kg_query_global(nl_query=<problem statement>)` | Cross-board context |
| `okto_pulse_kg_get_learning_from_bugs(board_id, area=<affected area>)` | Past bugs in the affected area |

**Stage 2 — Refinement (before moving to `approved`)**

| Query | Why it's required |
|---|---|
| `okto_pulse_kg_find_similar_decisions(board_id, topic=<refinement topic>)` | Find prior decisions the refinement may extend, supersede, or contradict |
| `okto_pulse_kg_get_related_context(board_id, artifact_id="spec:<uuid>")` or `artifact_id="card:<uuid>"` | Use only when anchored to an existing formalized spec/card; the type discriminator is mandatory |
| `okto_pulse_kg_find_contradictions(board_id, node_id=<relevant decision>)` | Detect contradictions before they reach spec |
| `okto_pulse_kg_list_alternatives(board_id, decision_id=<anchor decision>)` | Surface "why not X" rationale |

**Stage 3 — Spec (before moving out of `draft`)**

| Query | Why it's required |
|---|---|
| `okto_pulse_kg_get_related_context(board_id, artifact_id="spec:<uuid>")` | Final sweep of 2-hop neighbors; a raw spec UUID is rejected |
| `okto_pulse_kg_find_contradictions(board_id)` (board-wide) | Detects contradictions the spec itself may have introduced |
| `okto_pulse_kg_find_similar_decisions(board_id, topic=<each major FR/BR>)` | Check every significant FR/BR for similarity |
| `okto_pulse_kg_explain_constraint(board_id, constraint_id=<each relevant canonical Constraint.id>)` | Fetch origin + related constraints + prior violations. Do not fabricate this id from a TR id or worker candidate id; use the discovery recipe below. |

**Constraint-ID discovery for Stage 3.** `constraint_id` is the canonical graph
node id. For a current draft, resolve it through the read-only MCP boundary with
`okto_pulse_kg_query_cypher(board_id, cypher="MATCH (c:Constraint) WHERE
c.source_artifact_ref STARTS WITH $prefix RETURN c.id AS id,
c.source_artifact_ref AS ref", params={"prefix":"spec:<spec-id>:"},
include_working=true)`. Match the returned `source_artifact_ref` to the spec child
refs, reject missing or duplicate refs, and pass only the returned `c.id` values to
`okto_pulse_kg_explain_constraint`. `include_working=true` is required here because
the mandatory sweep happens before the spec leaves `draft`.

## Tier Primary Query Tools (9 tools)

| Tool | Purpose |
|------|---------|
| `okto_pulse_kg_get_decision_history` | Trace decisions about a topic over time |
| `okto_pulse_kg_get_related_context` | 2-hop neighborhood |
| `okto_pulse_kg_get_supersedence_chain` | Full chain of what superseded what |
| `okto_pulse_kg_find_contradictions` | Contradictory decision pairs |
| `okto_pulse_kg_find_similar_decisions` | Semantic search with hybrid ranking |
| `okto_pulse_kg_explain_constraint` | Origin, related constraints, violations |
| `okto_pulse_kg_list_alternatives` | Alternatives considered and discarded |
| `okto_pulse_kg_get_learning_from_bugs` | Lessons learned from bugs |
| `okto_pulse_kg_query_global` | Cross-board semantic search |

## Tier Power Escape Hatch (3 tools)

| Tool | Purpose |
|------|---------|
| `okto_pulse_kg_query_cypher` | Read-only Cypher directly on the embedded graph database. Defaults to canonical-only rows; pass `include_working=true` when validating working graph ingestion. |
| `okto_pulse_kg_query_natural` | Natural language search via embedding + HNSW |
| `okto_pulse_kg_schema_info` | Schema introspection: node types, rel types, vector indexes |

**Safety rails:** Timeout: 5s default, 30s max. Max rows: 50 by default and 1000 hard cap. Rate limit: **30 queries/min per agent**. Cypher injection: blacklist keywords rejected.

**Layer contract:** Graph nodes expose `graph_layer` (`canonical` or `working`). `kg_layer_counts` is a health payload aggregate, not a node property. `okto_pulse_kg_query_cypher` enforces canonical-only visibility by default and should be called with `include_working=true` for working graph checks, rebuild validation, or E2E ingestion tests.

### Cypher Hit-Counting & RETURN Contract

**Hit-counting parity with `kg_query_natural` (spec 28583299):** `kg_query_cypher` increments the per-node hit counter for every node it returns, the same way `kg_query_natural` does for its top-K. The counter feeds `relevance_score` over time and biases ranking toward nodes the agents actually consult. To get credit for a hit, **shape the RETURN so the result row carries the node's id**:

- `RETURN n.id` — the column named `id` is detected directly.
- `RETURN n.id AS node_id` — alias works too (`node_id`/`*_id`/`*.id`).
- `RETURN n` — when the row carries a UUID-like scalar anywhere, it's recognised as a node id.
- `RETURN labels(n) AS node_type, n.id AS id` — pair labels with the id so the counter tags the right node type instead of `unknown`.

Aggregator queries (`RETURN count(n)`, `RETURN sum(...)`) **do not** increment the counter — there's no row-level node id to attribute to. That's intentional: aggregations are diagnostic, not consumption.

**Last decay tick visibility:** `okto_pulse_kg_health` exposes `last_decay_tick_at`, `nodes_recomputed_in_last_tick`, and `decay_scheduler_diagnostics`, populated from the daily scheduler tick ledger (03:00 UTC). When the diagnostics report `operational_debt=true` but `graph_recovery_required=false`, treat it as scheduler/operations debt, not as a rebuild trigger. Score freshness is still bounded by the on-read `_apply_decay_reorder` until the scheduled tick lands.

## When and How to Consolidate — Mandatory Triggers

**Mandatory triggers — you MUST open a consolidation session:**

| Trigger | Pattern |
|---|---|
| Spec reaches `done` | Begin canonical consolidation on the spec. The **cognitive** candidates you may create are `Decision`, `Assumption`, `Alternative`. `Criterion` (from acceptance criteria) and `Constraint` (from technical requirements / business rules) are **deterministic-only**: the deterministic worker materializes them — reference the existing deterministic nodes, never create `Criterion`/`Constraint` on the cognitive path. `approved` and `validated` remain working/diagnostic only. |
| Sprint closes (moves to `closed`) | Consolidate retrospective Learnings + Bugs + Learning→validates→Bug edges |
| Q&A on an ideation/refinement/spec gets an answer that contains a decision | Carry decision into next formalized spec first, then consolidate from that spec-side formalization |
| Bug card moves to `done` with root cause + fix narrative | Consolidate a Learning node that validates the Bug node |
| Complete SDLC/E2E flow about to be reported as finished | Create a final report consolidation session |

**Anti-triggers — do NOT consolidate:**
- Artifact is still in `draft` or `review` — content is not stable.
- `okto_pulse_kg_begin_consolidation` returned `nothing_changed=true` — abort.
- Q&A answer is a clarification, not a decision.

## Cognitive KG Closeout — Mandatory for Specs and Bugs

**Mandatory closeout sequence:**
```
1. Read complete context: okto_pulse_get_spec_context or okto_pulse_get_task_context
2. Identify cognitive candidates — the cognitive-writable node types are `Decision`, `Assumption`, `Alternative` (spec closeout) and `Learning` (bug closeout). Risk, root cause, rejected alternative, and process gap are *content/classification expressed within those node types*, not new node types. `Criterion` and `Constraint` are **deterministic-only** — reference existing deterministic nodes by id; never add them as cognitive candidates.
3. okto_pulse_kg_begin_consolidation(board_id, artifact_type, artifact_id, raw_content, deterministic_candidates=[])
4. If nothing_changed=true: okto_pulse_kg_abort_consolidation and report attempted closeout
5. okto_pulse_kg_add_node_candidate and okto_pulse_kg_add_edge_candidate for applicable candidates
6. okto_pulse_kg_propose_reconciliation and inspect every hint
7. okto_pulse_kg_commit_consolidation or okto_pulse_kg_abort_consolidation with clear reason
8. Verify health/queryability when closeout committed
```

**Decision closeout rule:** create a cognitive `Decision` only when the
artifact contains a real choice with a valid judgement edge you can model
(`supersedes`/`contradicts`/`depends_on` to another Decision, or `relates_to`
to an Alternative). Do not create a Decision just to satisfy connectivity. If
the closeout only records uncertainty, risk, rejected path, or contextual note,
use `Assumption` or `Alternative` with a precise `source_artifact_ref` such as
`spec:<spec_id>:assumption:<stable_id>` or
`spec:<spec_id>:alternative:<stable_id>`.

**Final response requirement:** include a compact Cognitive KG Closeout line naming one of:
- committed: session_id, nodes_added/updated, edges_added, and verification summary
- nothing_changed: session_id or aborted session, plus evidence that reconciliation found no semantic change
- not_applicable: objective reason no cognitive candidate existed after context review
- blocked: the tool/error that prevented closeout

### Node-type ownership by writer path (allowlist)

KG node types are owned by a specific **writer path**. A consolidation candidate is
rejected when its `node_type` is not permitted for the writer path that proposes it —
this is distinct from a *missing semantic connectivity* failure.

| Node type | Owner | Created by |
|---|---|---|
| `Criterion` (from acceptance criteria), `Constraint` (from technical requirements / business rules) | deterministic | **deterministic worker only** — never the cognitive path |
| `Decision` | dual | cognitive **or** deterministic |
| `Learning` | cognitive | cognitive (bug closeout) |
| `Alternative`, `Assumption` | cognitive | cognitive (spec closeout) |

- The **deterministic worker** materializes `Criterion`/`Constraint` from the spec's
  structured acceptance criteria / technical requirements / business rules. The
  **cognitive** consolidation path may create only `Decision`, `Learning`,
  `Alternative`, `Assumption`.
- When a cognitive decision needs to cite a `Criterion` or `Constraint`, **reference the
  existing deterministic node by id** (or wait for the deterministic worker to
  materialize it) — do **not** recreate the node on the cognitive path.
- A cognitive session proposing a `Criterion` or `Constraint` candidate fails **before
  any graph mutation or session commit** with `status=source_type_not_supported`,
  `reason=writer_not_connectivity_owner`. Remediation: remove the invalid candidate,
  abort and recreate the session without it, or route the materialization through the
  deterministic owner.

## KG Governance — Operator Hygiene

### Query-First Pattern (required before authoring Decisions/Constraints)

Before creating any Decision or Constraint, run:
1. `okto_pulse_kg_query_natural(nl_query="<topic keywords>")` — detect duplicates / near-matches.
2. `okto_pulse_kg_find_contradictions(board_id)` if the topic is contentious.
3. `okto_pulse_kg_get_decision_history(topic="<keyword>")` — inspect any supersedence chain.

### Edge Layer Ownership

| Edge type | Who emits | When |
|---|---|---|
| `mentions`, `derives_from`, `tests`, `implements`, `violates`, `belongs_to` | Layer 1 deterministic worker | Auto on consolidation |
| `supersedes`, `contradicts`, `depends_on`, `relates_to`, `validates` | Cognitive agent (you) | Manual cognitive edges only |

### Cognitive Provenance — Learning Taxonomy (S-KG-01)

Cognitive artifacts (`Learning` / `Alternative` / `Assumption`) prove connectivity through **cognitive provenance** — a resolved `source_artifact_ref` PLUS a cognitive-taxonomy relation — NOT the deterministic `belongs_to`-to-`Entity` backbone the **operational** artifacts (`Requirement` / `Constraint` / `APIContract` / `TestScenario` / `Criterion` / `Bug` / `Entity`) require. The taxonomy reuses the EXISTING edge names; no new edge type is ever introduced (never `learned_from` / `informs` / `constrains` / `refines` / `warns_about`):

| Learning shape | Allowed relation | Endpoint |
|---|---|---|
| bug-derived | `validates` | a **canonical** `Bug` (a `working` Bug never canonizes the Learning) |
| non-bug | `relates_to` | ONE canonical `Entity` \| `Decision` \| `Requirement` \| `Constraint` \| `TestScenario` \| `APIContract` \| `Criterion` |

`relates_to Decision -> Alternative` is unchanged. A cognitive writer that emits `belongs_to` (or any deterministic edge) is rejected fail-closed with `forbidden_deterministic_edge`; a cognitive node whose source resolves but carries no allowed relation is `missing_cognitive_provenance`. Operational artifacts stay on the strict deterministic provenance group even when they carry cognitive metadata.

### KG Health

`okto_pulse_kg_health(board_id)` — returns a JSON health snapshot. It carries the KG-01 contract fields (`board_id`, `graph_state`, `discovery_state`, `overall_state`, `metric_status`, `correlation_id`, `checked_at`, …) alongside the legacy aggregation fields (`queue_depth`, `oldest_pending_age_s`, `dead_letter_count`, `total_nodes`, `default_score_ratio`, `avg_relevance`, `schema_version`, `contradict_warn_count`), the daily-tick fields `last_decay_tick_at` / `nodes_recomputed_in_last_tick`, `decay_scheduler_diagnostics`, and `storage_footprint_proxy`.

**When to consult:** before long consolidation cycles, after flagging contradictions, when debugging stale ranking (`default_score_ratio > 0.7`).

**Storage:** each board's KG persists through the runtime graph-store adapter. The core contract reports `graph_state` for the canonical/working graph and `discovery_state` for the discovery/embedding index without naming a concrete storage engine or filesystem layout. Never delete the graph store; schema migrations self-heal on the hot path. A degraded `graph_state` / `discovery_state` points at the matching operational store owned by the active runtime adapter.

### Operational Signals — Separate Domains + Drill-Down (spec 007d1308)

Cognitive consolidation produces **canonical** knowledge by construction: a cognitive `okto_pulse_kg_add_node_candidate` with `graph_layer=working` is rejected (`cognitive_node_candidates_must_be_canonical`) BEFORE the session is mutated, and accepted candidates are persisted as `canonical` / `maturity_status=canonical_eligible`. Working-layer nodes are the Layer 1 deterministic worker's responsibility only (`source_maturity`), never the cognitive agent's.

KG Health surfaces three **distinct** operational signals — never merged into one bucket (dec_68fd26a2). When a signal is present, its `health_issues[]` row names the correct drill-down MCP tool in `drill_down_tool`:

| Signal (`health_issues[].code`) | Domain | Drill-down tool |
|---|---|---|
| `cognitive_consolidation_pending` | Cognitive items awaiting agent action (pending/in_progress/failed) | `okto_pulse_kg_list_cognitive_pending_items` |
| `dead_letter_backlog` | Consolidation rows that exhausted retries | `okto_pulse_kg_dead_letter_list` → `okto_pulse_kg_dead_letter_reprocess` after fixing the root cause |
| `canonical_debt_open` | Artifacts still outside canonical consolidation | `okto_pulse_kg_canonical_debt_list` |

Each tool lists ONLY its own domain — do NOT infer one signal's backlog from another's counters, and do not reprocess the wrong queue. `okto_pulse_kg_dead_letter_list` exposes both `rows`/`id` (legacy) and the additive `items`/`dead_letter_id` + `last_error`/`error_text` (full `errors[]` history preserved). The three listings emit the bounded `kg_operational_inspection_list_total` counter (labels: `signal`=`cognitive_pending`/`dead_letter`/`canonical_debt`, `surface`, `outcome`) so the **absence** of operational drill-down is itself diagnosable.

### Consolidation Hygiene Checklist

Before `okto_pulse_kg_commit_consolidation`:
- [ ] `okto_pulse_kg_begin_consolidation` was called with `deterministic_candidates` pre-populated
- [ ] `nothing_changed` flag checked — if `true`, abort early
- [ ] `raw_content` includes enough context for SHA256 dedup
- [ ] Edge candidates reference existing nodes via `kg:<existing_node_id>` prefix
- [ ] Only cognitive edges emitted (`supersedes`, `contradicts`, `depends_on`, `relates_to`, `validates`)

**Concurrency:** Server serialises commits per board automatically (per-board lock in `commit_coordinator`), so you may fire `okto_pulse_kg_commit_consolidation` calls in parallel — the handler retries transient file-lock contention with exponential backoff. Distinct boards never contend with each other.

After `okto_pulse_kg_commit_consolidation`:
- [ ] `okto_pulse_kg_health` checked for new dead letters
- [ ] `okto_pulse_kg_query_natural` retrieved the newly consolidated final facts
- [ ] `okto_pulse_kg_query_cypher` validated the new nodes by `source_artifact_ref`
- [ ] Final response includes `session_id`, `nodes_added`, `edges_added`, query verification, and nonconformities
