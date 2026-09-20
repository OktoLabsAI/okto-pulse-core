---
version: "1.0"
---

# KG Health & Operational Signals (full reference)

Health reports board-scoped observations, component states and limitations.
It is available when diagnosing an unavailable operation; it is not a required
repair step before ordinary product work. A reading does not grant permission
to mutate storage or bypass a product gate.

## When to consult

Use health to understand a reported failure or missing observation. Quarantined
or unavailable components continue to block affected operations. Missing metrics
do not establish health, corruption or loss of data. Empty query results alone
do not establish that a graph needs rebuilding.

## How to consult

Prefer the MCP tool over the REST endpoint — it is the agent-facing surface and
uses your session context automatically:

- **MCP**: `okto_pulse_kg_health(board_id=...)` — same payload, no auth dance.
- **REST**: `GET /api/v1/kg/health?board_id=...` exists for the dashboard SPA and
  ad-hoc curl. Use it only when MCP is unavailable.

## Recovery boundary

Public board rebuild preflight, confirmation and execution have been removed
from MCP, REST and the installed CLI. There is no replacement maintenance tool.
Health reports the component, reason and limitation. External support or release
procedures may restore an authorized backup; they are not actions an agent can
invoke through Pulse. Historical audit records remain history.

## Reading the payload

Contract fields you must understand (`api_3ed9037f`):

| Field | Meaning |
|---|---|
| `graph_state` / `discovery_state` | Per-graph state from the 5-state machine (`healthy`, `at_risk`, `backpressure`, `recovery_needed`, `quarantined`). |
| `overall_state` | Worst-case fold of the two above. It gates ordinary writes but does not authorize recovery. Inspect `graph_state` versus `discovery_state` to identify the affected component. |
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
| Legacy fields (`contradict_warn_count`, `last_decay_tick_at`, `nodes_recomputed_in_last_tick`, `default_score_ratio`, …) | Preserved for backward compatibility with the dashboard. `contradict_warn_count` counts CONTRADICT_PENALTY warnings; it does not request score recomputation. |

## What you MUST NOT do

- Never call `okto_pulse_kg_health` thinking it will fix anything — it is
  **read-only** by contract. It does not mutate graph or discovery storage, run a
  tick, clear quarantine, bootstrap an absent store, create directories, or
  automatically retry a failed probe. Board, census, and discovery evidence
  share one bounded request deadline; an exhausted/unavailable probe returns a
  typed `unknown` + `unavailable` response instead of falling through to an
  opening probe.
- Do not infer a repair operation from `overall_state` or advise restarting,
  replacing storage or bypassing a fence. Report the component and limitation.
- Never override `metric_status=unavailable` with your own interpretation. The
  conservative default is the BR. Surface the unknown.
