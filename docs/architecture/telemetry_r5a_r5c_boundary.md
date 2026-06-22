# Telemetry boundary — R5A producer vs R5C health surface

**Spec**: R5A — *core telemetry instrumentation e failure-state local*
(`6a04b59a`). This is the canonical statement of which layer owns what, so R5C
(and any UI/MCP health surface) consumes the local telemetry state WITHOUT
redefining the schema or re-deriving trust. It is doc-only; the contracts it
points at live in code (the telemetry module docstrings + tests).

## The two layers

```
   R5A — PRODUCER (this repo, src/okto_pulse/core/telemetry/)
   ──────────────────────────────────────────────────────────
   emitters.py   record_event helpers (cli/mcp/kg/lifecycle/pipeline_transition)
   sender.py     aggregates the delta batch + drives the publish lifecycle
   failure_state.py  the publish failure-state schema (R1 base, R5A extension)
   event_contract.py every declared EventType is classified (no phantom schema)
   http_policy.py    which HTTP paths count + a values-free route template
                 │  produces: local state.json + secret-free structured logs
                 ▼
   R5C — CONSUMER / HEALTH SURFACE (MCP/UI)
   ──────────────────────────────────────────────────────────
   reads the COMPOSED public projection and DISPLAYS it.
   MUST NOT redefine the schema, build a parallel DTO, or compute failure-state.
```

- **R5A produces** (local-first, never blocks a real operation):
  - **event aggregates** — the delta-batch metric families in
    `sender.py::_build_delta_batch`: `cli_counts`, `http_route_template_counts`,
    `mcp_tool_counts`, `kg_operation_counts`, `duration_buckets`,
    `error_class_counts`, the seven `product_*_counts`, `guided_help_counts`, and
    (R5A-B) `lifecycle_counts`, `pipeline_transition_counts`,
    `unknown_event_type_counts`. Pinned by `event_contract.LIVE_AGGREGATE_MAPS`.
  - **failure-state** — `failure_state.FailureState`: `status`, `reason_code`,
    `http_status`, `last_success_at`, `last_failure_at`, `next_retry_at`,
    `retry_count`, `recovered_at`, `publish_enabled`, `consent_state`,
    `install_id_redacted`. R1 owns the base schema; R5A EXTENDS it
    (`install_id_redacted`), never a parallel schema (`br_14606103`).
  - **secret-free structured logs** — `metrics.failure_state_transition` (built
    from the allowlisted public projection), `metrics.beacon_outcome`,
    `metrics.token_refresh`, the watermark audit signal.
- **R5C consumes** the composed, allowlisted DTO
  (`failure_state.public_status_projection` / `FailureState.to_public_dict`,
  surfaced today by `TelemetryService.summary`) and renders it on the
  MCP/UI health surface. R5C adds NO producer logic and NO schema.

## Safe vs forbidden fields (the security invariant)

`br_89e39ee6` / `fr_8ead6f5e` / `br_7a6224e3`: every projection and structured log
is built from an **allowlist**, so a secret cannot leak structurally.

- **Safe to surface**: the allowlisted failure-state fields above;
  `install_id_redacted` (a NON-reversible `iid_<sha256-prefix>` — never the raw
  install id); the bounded aggregate maps, whose keys are bounded categoricals
  (cli command name, mcp tool name, kg operation, lifecycle action, pipeline
  phase, the `{placeholder}` route template — never args/input/payload/id).
- **Forbidden everywhere** (state, projection, log, metric key/value):
  `install_token`, `token_hash`, `signature`, a full `nonce`, sensitive payload,
  raw request/response body, and raw ids/PII. `failure_state.is_secret_key` is the
  catch-all guard; `schema.sanitize_payload` enforces the closed event schema.

## HTTP telemetry policy (R5A-C)

`http_policy.should_count_http` counts product/agent usage by EXACT first segment:
`/api` + `/mcp` (and subpaths) are counted; `/health`, `/docs`, `/openapi.json`,
`/redoc` are excluded; a lookalike (`/apiary`, `/api-keys`, `/mcping`, `/healthz`)
is not counted. `safe_route_template` prefers the resolved route PATTERN and, when
the route did not resolve, collapses to a bounded `/api/{unresolved}` —
never a concrete path/id/query.

## Cross-repo state (R5A-F / R5A-G)

The downstream consumer `okto_labs_community_metrics` ingests the published delta
batch. R5A-F + R5A-G closed the consumer compatibility: its
`schema.USAGE_METRIC_KEYS` and the Glue `usage_aggregates_v1` `metrics` struct now
accept + preserve every family the core emits — including `guided_help_counts`,
`lifecycle_counts`, `pipeline_transition_counts`, `unknown_event_type_counts` — so
a batch carrying them is never rejected as `UNKNOWN_FIELDS` nor dropped at Firehose
JSON→Parquet. New families are emitted conditionally (only when non-empty), so old
batches are unaffected.

## What R5C must NOT do

- Redefine or fork the failure-state / event schema (consume the R5A projection).
- Recompute trust or failure classification (R5A already did, secret-free).
- Surface any forbidden field — only the allowlisted projection is renderable.
- Implement producer/ingest logic — that is R5A (this repo) / the consumer repo.
