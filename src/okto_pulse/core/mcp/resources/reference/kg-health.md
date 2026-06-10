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
   `overall_state ∈ {recovery_needed, quarantined}` you MUST stop and surface the
   state to the user — do not write.
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
| `overall_state` | Worst-case fold of the two above. This is what gates your decision to write/rebuild. |
| `metric_status` | `available` or `unavailable`. **`unavailable` never means "graph is fine, sensor is just off"** — BR br_2a8cdfdc forbids degrading to healthy when telemetry can't be read. Treat the graph as `at_risk` until telemetry recovers. |
| `classification_reason` | Single-string explanation of why the state was assigned (e.g. `graph:metric.unavailable`). |
| `correlation_id` | Join key against `recent_events` and against observability logs. Quote this when reporting an issue. |
| `current_kg_generation_id` | Identifies the active graph generation. Changes after a clean rebuild; same generation across health calls means storage hasn't been replaced. |
| `recent_events` | Recent state transitions, WAL/commit failures and memory-pressure samples. Empty when the safe observability path (KG-01.5) hasn't shipped yet. |
| `memory_pressure_status` | `unconfirmed` or `confirmed_primary_cause`. **Only** `confirmed_primary_cause` justifies recommending a memory-pressure mitigation — anything else means the deterministic criterion (>90% in ≥3 samples within 10 min before WAL/commit failure) did NOT match. |
| Legacy fields (`contradict_warn_count`, `last_decay_tick_at`, `nodes_recomputed_in_last_tick`, `default_score_ratio`, …) | Preserved for backward compat with the existing dashboard. |

## What you MUST NOT do

- Never call `okto_pulse_kg_health` thinking it will fix anything — it is
  **read-only** by contract. It does not mutate graph or discovery storage, run a
  tick, or clear quarantine.
- Never advise the user to "just rebuild" when `overall_state ∈ {at_risk,
  backpressure}`. The KG-01 hardening flow is: surface state → wait for
  backpressure to drain / sensors to recover → only then consider rebuild via
  KG-02 paths.
- Never override `metric_status=unavailable` with your own interpretation. The
  conservative default is the BR. Surface the unknown.
