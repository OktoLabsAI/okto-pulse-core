---
version: "1.0"
---

# Knowledge Graph — Consolidation, Query & Governance

## Architecture Overview

- **Per-board LadybugDB graph** at `~/.okto-pulse/boards/{board_id}/graph.lbug` — 11 node types, 10 relationship types, 5 HNSW vector indexes
- **Global discovery meta-graph** at `~/.okto-pulse/global/discovery.lbug` — board summaries, topic clusters, canonical entities (digest-only, no sensitive content)
- **SQLite operational tables**: `consolidation_queue`, `consolidation_audit`, node back-references for undo, `global_update_outbox`
- **Agent-as-LLM premise**: the platform NEVER invokes LLM. All cognitive work (extraction, reasoning, reconciliation decisions) is done by YOU, the code agent.

## Consolidation Primitives (7 tools)

| Tool | Args | Purpose |
|------|------|---------|
| `okto_pulse_kg_begin_consolidation` | board_id, artifact_type, artifact_id, raw_content, deterministic_candidates? | Open a transactional session. Returns session_id + SHA256 dedup. |
| `okto_pulse_kg_add_node_candidate` | session_id, candidate | Add a node candidate. Not persisted until commit. |
| `okto_pulse_kg_add_edge_candidate` | session_id, candidate | Add an edge. Endpoints reference in-session candidates or existing nodes via `kg:` prefix. |
| `okto_pulse_kg_get_similar_nodes` | session_id, candidate_id, top_k?, min_similarity? | HNSW vector search against existing graph. |
| `okto_pulse_kg_propose_reconciliation` | session_id | Server computes deterministic hints: ADD/UPDATE/SUPERSEDE/NOOP. |
| `okto_pulse_kg_commit_consolidation` | session_id, summary_text?, agent_overrides? | Atomically write to LadybugDB + audit row + outbox event. |
| `okto_pulse_kg_abort_consolidation` | session_id, reason? | Drop the session without writing. |

**Node types (11):** Decision, Criterion, Constraint, Assumption, Requirement, Entity, APIContract, TestScenario, Bug, Learning, Alternative

**Cognitive edge types (agent-emittable only):** `supersedes`, `contradicts`, `depends_on`, `relates_to`, `validates`

**Reserved deterministic edges** (auto-created by worker, never emit manually): `belongs_to`, `derives_from`, `implements`, `mentions`, `tests`, `violates`, `originates_from`, `covered_by`

**Consolidation workflow:**
```
1. okto_pulse_kg_begin_consolidation(board_id, artifact_type, artifact_id, raw_content, deterministic_candidates=[...])
   → if nothing_changed=true → STOP, abort and move on
2. For every candidate:
     a. okto_pulse_kg_get_similar_nodes(session_id, candidate_id, top_k=5, min_similarity=0.85)
        → if match ≥ 0.95: plan UPDATE; if 0.85..0.95: plan SUPERSEDE; else: plan ADD
     b. okto_pulse_kg_add_node_candidate(session_id, candidate)
3. okto_pulse_kg_add_edge_candidate only for cognitive rels
4. okto_pulse_kg_propose_reconciliation(session_id)
5. okto_pulse_kg_commit_consolidation(session_id, summary_text="<1-2 sentences>", agent_overrides={...})
6. Verify with okto_pulse_kg_health + okto_pulse_kg_query_natural + okto_pulse_kg_query_cypher
7. On any unrecoverable error: okto_pulse_kg_abort_consolidation(session_id, reason=...)
```

## Query Timing — MANDATORY at Every Stage

**Stage 1 — Ideation (before moving to `evaluating` or answering any Q&A)**

| Query | Why it's required |
|---|---|
| `okto_pulse_kg_find_similar_decisions(board_id, topic=<ideation problem statement>)` | Discover prior art, prevent re-inventing the wheel |
| `okto_pulse_kg_query_global(nl_query=<problem statement>)` | Cross-board context |
| `okto_pulse_kg_get_learning_from_bugs(board_id, area=<affected area>)` | Past bugs in the affected area |

**Stage 2 — Refinement (before moving to `approved`)**

| Query | Why it's required |
|---|---|
| `okto_pulse_kg_find_similar_decisions(board_id, topic=<refinement topic>)` | Find prior decisions the refinement may extend, supersede, or contradict |
| `okto_pulse_kg_get_related_context(board_id, artifact_id=<formalized_node_or_artifact_id>)` | Use only when anchored to an existing formalized KG node |
| `okto_pulse_kg_find_contradictions(board_id, node_id=<relevant decision>)` | Detect contradictions before they reach spec |
| `okto_pulse_kg_list_alternatives(board_id, decision_id=<anchor decision>)` | Surface "why not X" rationale |

**Stage 3 — Spec (before moving out of `draft`)**

| Query | Why it's required |
|---|---|
| `okto_pulse_kg_get_related_context(board_id, artifact_id=<spec_id>)` | Final sweep of 2-hop neighbors |
| `okto_pulse_kg_find_contradictions(board_id)` (board-wide) | Detects contradictions the spec itself may have introduced |
| `okto_pulse_kg_find_similar_decisions(board_id, topic=<each major FR/BR>)` | Check every significant FR/BR for similarity |
| `okto_pulse_kg_explain_constraint(board_id, constraint_id=<each relevant constraint>)` | Fetch origin + related constraints + prior violations |

## Tier Primary Query Tools (9 tools)

| Tool | Args | Purpose |
|------|------|---------|
| `okto_pulse_kg_get_decision_history` | board_id, topic, min_confidence?, max_rows? | Trace decisions about a topic over time |
| `okto_pulse_kg_get_related_context` | board_id, artifact_id, min_confidence?, max_rows? | 2-hop neighborhood |
| `okto_pulse_kg_get_supersedence_chain` | board_id, decision_id | Full chain of what superseded what |
| `okto_pulse_kg_find_contradictions` | board_id, node_id?, max_rows? | Contradictory decision pairs |
| `okto_pulse_kg_find_similar_decisions` | board_id, topic, top_k?, min_similarity? | Semantic search with hybrid ranking |
| `okto_pulse_kg_explain_constraint` | board_id, constraint_id | Origin, related constraints, violations |
| `okto_pulse_kg_list_alternatives` | board_id, decision_id, max_rows? | Alternatives considered and discarded |
| `okto_pulse_kg_get_learning_from_bugs` | board_id, area, min_confidence?, max_rows? | Lessons learned from bugs |
| `okto_pulse_kg_query_global` | board_id?, nl_query, top_k? | Cross-board semantic search |

## Tier Power Escape Hatch (3 tools)

| Tool | Args | Purpose |
|------|------|---------|
| `okto_pulse_kg_query_cypher` | board_id, cypher, params?, max_rows?, timeout_ms? | Read-only Cypher directly on LadybugDB |
| `okto_pulse_kg_query_natural` | board_id, nl_query, limit?, min_confidence? | Natural language search via embedding + HNSW |
| `okto_pulse_kg_schema_info` | board_id?, include_internal? | Schema introspection: node types, rel types, vector indexes |

**Safety rails:** Timeout: 5s default, 30s max. Max rows: 1000 default, 10000 max. Rate limit: **30 queries/min per agent**. Cypher injection: blacklist keywords rejected.

### Cypher Hit-Counting & RETURN Contract

**Hit-counting parity with `kg_query_natural` (spec 28583299):** `kg_query_cypher` increments the per-node hit counter for every node it returns, the same way `kg_query_natural` does for its top-K. The counter feeds `relevance_score` over time and biases ranking toward nodes the agents actually consult. To get credit for a hit, **shape the RETURN so the result row carries the node's id**:

- `RETURN n.id` — the column named `id` is detected directly.
- `RETURN n.id AS node_id` — alias works too (`node_id`/`*_id`/`*.id`).
- `RETURN n` — when the row carries a UUID-like scalar anywhere, it's recognised as a node id.
- `RETURN labels(n) AS node_type, n.id AS id` — pair labels with the id so the counter tags the right node type instead of `unknown`.

Aggregator queries (`RETURN count(n)`, `RETURN sum(...)`) **do not** increment the counter — there's no row-level node id to attribute to. That's intentional: aggregations are diagnostic, not consumption.

**Last decay tick visibility:** `okto_pulse_kg_health` exposes `last_decay_tick_at` and `nodes_recomputed_in_last_tick`, populated by the daily APScheduler tick (03:00 UTC). When `last_decay_tick_at` is `null` the daily worker hasn't run yet on this deployment — score freshness is bounded by the on-read `_apply_decay_reorder` until the first tick lands.

## When and How to Consolidate — Mandatory Triggers

**Mandatory triggers — you MUST open a consolidation session:**

| Trigger | Pattern |
|---|---|
| Spec reaches `approved`, `validated`, or `done` | Begin consolidation on the spec: extract Decision + Criterion + Constraint + Assumption + Alternative nodes |
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
2. Identify cognitive candidates: Learning, Decision, Assumption, Risk, Alternative, root cause, rejected alternative, or process gap
3. okto_pulse_kg_begin_consolidation(board_id, artifact_type, artifact_id, raw_content, deterministic_candidates=[])
4. If nothing_changed=true: okto_pulse_kg_abort_consolidation and report attempted closeout
5. okto_pulse_kg_add_node_candidate and okto_pulse_kg_add_edge_candidate for applicable candidates
6. okto_pulse_kg_propose_reconciliation and inspect every hint
7. okto_pulse_kg_commit_consolidation or okto_pulse_kg_abort_consolidation with clear reason
8. Verify health/queryability when closeout committed
```

**Final response requirement:** include a compact Cognitive KG Closeout line naming one of:
- committed: session_id, nodes_added/updated, edges_added, and verification summary
- nothing_changed: session_id or aborted session, plus evidence that reconciliation found no semantic change
- not_applicable: objective reason no cognitive candidate existed after context review
- blocked: the tool/error that prevented closeout

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

### KG Health

`okto_pulse_kg_health(board_id)` — returns a JSON health snapshot. It carries the KG-01 contract fields (`board_id`, `graph_state`, `discovery_state`, `overall_state`, `metric_status`, `correlation_id`, `checked_at`, …) alongside the legacy aggregation fields (`queue_depth`, `oldest_pending_age_s`, `dead_letter_count`, `total_nodes`, `default_score_ratio`, `avg_relevance`, `schema_version`, `contradict_warn_count`) and the daily-tick fields `last_decay_tick_at` / `nodes_recomputed_in_last_tick`.

**When to consult:** before long consolidation cycles, after flagging contradictions, when debugging stale ranking (`default_score_ratio > 0.7`).

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
