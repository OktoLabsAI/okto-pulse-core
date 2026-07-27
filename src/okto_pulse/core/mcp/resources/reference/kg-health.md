---
version: "1.0"
---

# KG Health & Operational Signals (full reference)

Spec KG-01 (a7659ba3) ships an explicit KG Health surface so agents stop relying
on heuristics like "graph feels stale" or "let me just rebuild". Read this
**before** you ask a user to run a tick, force a rebuild, or escalate "KG looks
broken". The compact stop-rule lives in `agent_instructions.md`; this resource is
the full contract.

## When to consult

1. You're about to call a KG mutation path (`okto_pulse_kg_commit_consolidation`,
   `okto_pulse_kg_tick_run_now`, anything in the rebuild/reset family). If
   `overall_state == quarantined` you MUST stop and surface the state to the
   user — do not write. `recovery_needed` also stops ordinary mutation. Choose
   an exception only from the component state: board
   `graph_state=recovery_needed` uses board rebuild; healthy board graph plus
   `discovery_state=recovery_needed` and
   `discovery_recovery_required=true` uses the separate global discovery
   preflight → confirm → run trio. Generic `overall_state=recovery_needed` is
   never evidence that board rebuild will fix the failed component.
2. A KG query returned empty/stale results and you're tempted to "fix it" by
   running a tick. Check `metric_status` first; an `unavailable` status means
   telemetry is the problem, not the data.
3. `contradict_penalty` looked off in a recent decision history result, or you're
   investigating why `decay` (relevance score recomputation) seems to have skipped
   a node. Health surfaces `last_decay_tick_at`, `nodes_recomputed_in_last_tick`
   and the `contradict_warn_count` (alias of CONTRADICT_PENALTY warnings).

## How to consult

Prefer the MCP tool over the REST endpoint — it is the agent-facing surface and
uses your session context automatically:

- **MCP**: `okto_pulse_kg_health(board_id=...)` — same payload, no auth dance.
- **REST**: `GET /api/v1/kg/health?board_id=...` exists for the dashboard SPA and
  ad-hoc curl. Use it only when MCP is unavailable.

## Reading the payload

Contract fields you must understand (`api_3ed9037f`):

| Field | Meaning |
|---|---|
| `graph_state` / `discovery_state` | Per-graph state from the 5-state machine (`healthy`, `at_risk`, `backpressure`, `recovery_needed`, `quarantined`). |
| `overall_state` | Worst-case fold of the two above. It gates ordinary writes but does not select recovery. Inspect `graph_state` versus `discovery_state`: board rebuild handles only the board graph; the global trio handles admitted discovery-only failure. |
| `metric_status` | `available` or `unavailable`. **`unavailable` never means "graph is fine, sensor is just off"** — BR br_2a8cdfdc forbids degrading to healthy when telemetry can't be read. Treat the graph as `at_risk` until telemetry recovers. |
| `classification_reason` | Single-string explanation of why the state was assigned (e.g. `graph:metric.unavailable`). |
| `health_schema_version` | Version of the coordinated REST/MCP/model/frontend health contract. Version `1.1` adds the materialization diagnosis while the legacy `schema_version` alias remains `1.0` for backward compatibility. |
| `materialization_state` | Board-scoped diagnosis: `not_materialized`, `materialized`, or fail-closed `unknown`. `not_materialized` requires same-generation confirmed board-store absence plus a successful all-zero relational census; it is never inferred from a timeout, I/O error, provider failure, unreadable path, nonzero census, or generation race. |
| `materialization_generation` | Generation fence shared by the store observations and relational census. It is null when evidence is unknown or raced. |
| `probe_reason_codes` | Stable reasons for `board_graph`, `board_census`, and `global_discovery`. Use these codes to distinguish confirmed absence from timeout/error/provider failure without parsing prose. |
| `global_outbox_dead_letter_count` | Board-scoped terminal Global Discovery delivery failures. It appears exactly once and remains distinct from consolidation `dead_letter_count` and the active retry-window queue. |
| `source_count` | Re-executable board sources. It is exactly `0` only for a confirmed-empty board and null when the source census is unavailable. |
| `oldest_pending_age_s` | Age of the oldest active item. It is null, not synthetic zero, when no pending item exists on a confirmed-empty board. |
| `root_cause` | Structured, bounded recovery root-cause. `categories` distinguishes `wal_or_commit_errors`, `empty_after_materialized_history`, `source_enumeration_failure` and `safe_write_drain_failure` (each `present` + bounded detail), plus `materialized_node_count`, `source_count` (reexecutable rebuild sources), `queue_state` and `last_safe_write_outcome`. When `drilldown_unavailable` is true the source-enumeration recovery drill-down can't be read and the board is forced off `healthy`. |
| Drill-down `next_action` | Rows from `okto_pulse_kg_dead_letter_list`, `okto_pulse_kg_canonical_debt_list` and `okto_pulse_kg_queue_drilldown` carry a bounded `next_action` (e.g. `reprocess_via_okto_pulse_kg_dead_letter_reprocess`, `wait_for_scheduled_retry`, `start_consolidation_worker`) alongside artifact type/id + state/error, so recovery triage is possible from the drill-down alone — no local-file forensics. |
| `correlation_id` | Join key against `recent_events` and against observability logs. Quote this when reporting an issue. |
| `current_kg_generation_id` | Identifies the active graph generation. Changes after a clean rebuild; same generation across health calls means storage hasn't been replaced. |
| `recent_events` | Recent state transitions, WAL/commit failures and memory-pressure samples. Empty when the safe observability path (KG-01.5) hasn't shipped yet. |
| `memory_pressure_status` | `unconfirmed` or `confirmed_primary_cause`. **Only** `confirmed_primary_cause` justifies recommending a memory-pressure mitigation — anything else means the deterministic criterion (>90% in ≥3 samples within 10 min before WAL/commit failure) did NOT match. |
| Legacy fields (`contradict_warn_count`, `last_decay_tick_at`, `nodes_recomputed_in_last_tick`, `default_score_ratio`, …) | Preserved for backward compat with the existing dashboard. |

## What you MUST NOT do

- Never call `okto_pulse_kg_health` thinking it will fix anything — it is
  **read-only** by contract. It does not mutate graph or discovery storage, run a
  tick, clear quarantine, bootstrap an absent store, create directories, or
  automatically retry a failed probe. Board, census, and discovery evidence
  share one bounded request deadline; an exhausted/unavailable probe returns a
  typed `unknown` + `unavailable` response instead of falling through to an
  opening probe.
- Never advise the user to "just rebuild" when `overall_state ∈ {at_risk,
  backpressure}`. The KG-01 hardening flow is: surface state → wait for
  backpressure to drain / sensors to recover → only then consider rebuild via
  KG-02 paths.
- Never claim a board rebuild repairs generic `overall_state=recovery_needed`.
  If only Global Discovery failed, board rebuild returns
  `board_rebuild_wrong_recovery_scope`; use the global recovery trio.
- Never override `metric_status=unavailable` with your own interpretation. The
  conservative default is the BR. Surface the unknown.
